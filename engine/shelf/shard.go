// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shelf

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const DefaultShardFreeDuration = 600 // Second
const backgroundSyncDuration = time.Millisecond * 100

type ShardInfo struct {
	ident     *meta.ShardIdentifier
	filePath  string
	lock      *string
	fileInfos chan []immutable.FileInfoExtend
	tbStore   immutable.TablesStore
	idx       Index
}

type Shard struct {
	// release background threads for shards that have no data written for a long time
	// and set the value of this attribute to true
	idle   bool
	signal *util.Signal

	walDir     string
	info       *ShardInfo
	idxCreator IndexCreator

	mu sync.RWMutex
	wg sync.WaitGroup

	// List of Wal files waiting to be converted to tssp files
	// key of map is the path of the Wal file
	waitSwitchWal map[string]*Wal

	// List of active wal files for real-time data writing
	// map key is measurement name
	wal map[string]*Wal

	freeDuration       uint64
	lastWriteTimestamp uint64
	tsspConverting     bool

	walSwap []*Wal
}

func NewShardInfo(ident *meta.ShardIdentifier, filePath string, lock *string,
	tbStore immutable.TablesStore, index Index) *ShardInfo {
	return &ShardInfo{
		ident:    ident,
		filePath: filePath,
		lock:     lock,
		tbStore:  tbStore,
		idx:      index,
	}
}

func NewShard(workerID int, info *ShardInfo, freeDuration uint64) *Shard {
	shard := &Shard{
		idle:          true,
		signal:        util.NewSignal(),
		waitSwitchWal: make(map[string]*Wal),
		wal:           make(map[string]*Wal),
		info:          info,
		freeDuration:  freeDuration,
	}
	shard.idxCreator.idx = info.idx
	shard.walDir = filepath.Join(config.GetStoreConfig().WALDir,
		config.WalDirectory,
		info.ident.OwnerDb,
		strconv.FormatUint(uint64(info.ident.OwnerPt), 10),
		info.ident.Policy,
		fmt.Sprintf("%d_%d", info.ident.ShardID, workerID))
	return shard
}

func (s *Shard) Run() {
	s.Load()
	s.openBackgroundProcessor()
}

func (s *Shard) openBackgroundProcessor() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.idle {
		return
	}

	stat.ActiveShardTotal.Incr()
	s.idle = false
	s.signal.ReOpen()
	s.wg.Add(2)
	go s.backgroundProcess()
	go s.backgroundWalProcess()
}

func (s *Shard) backgroundProcess() {
	defer s.wg.Done()

	util.TickerRun(time.Second/2, s.signal.C(), func() {
		s.AsyncConvertToTSSP()
		s.AsyncCreateIndex()
		s.Free()
	}, func() {})
}

func (s *Shard) backgroundWalProcess() {
	defer s.wg.Done()

	var wals []*Wal
	util.TickerRun(backgroundSyncDuration, s.signal.C(), func() {
		wals = s.getWalList(wals[:0], func(wal *Wal) bool {
			return wal.Opened()
		})

		for i := range wals {
			if wals[i].Expired() {
				s.switchWalIfExists(wals[i])
			}
			wals[i].BackgroundSync()
			wals[i] = nil
		}
	}, func() {})
}

func (s *Shard) GetWalReaders(dst []*Wal, mst string, tr *util.TimeRange) []*Wal {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, w := range s.waitSwitchWal {
		if !w.Converted() && w.mst == mst && w.timeRange.Overlaps(tr.Min, tr.Max) {
			w.Ref()
			dst = append(dst, w)
		}
	}

	w, ok := s.wal[mst]
	if ok && w.timeRange.Overlaps(tr.Min, tr.Max) {
		w.Ref()
		dst = append(dst, w)
	}
	return dst
}

func (s *Shard) switchWalIfExists(wal *Wal) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.wal[wal.mst]
	if !ok {
		return
	}
	s.switchWal(wal)
}

func (s *Shard) switchWal(wal *Wal) {
	stat.WALFileCount.Incr()
	stat.WALFileSizeSum.Add(wal.WrittenSize() / 1024)

	delete(s.wal, wal.mst)
	s.waitSwitchWal[wal.Name()] = wal
	stat.WALWaitConvert.Incr()
}

func (s *Shard) CreateWal(mst string) *Wal {
	s.mu.Lock()
	defer s.mu.Unlock()

	wal, ok := s.wal[mst]
	if ok && wal.SizeLimited() {
		ok = false
		s.switchWal(wal)
	}

	if !ok {
		mst = stringinterner.InternSafe(mst)
		wal = NewWal(s.GetWalDir(), s.info.lock, mst)
		s.wal[mst] = wal
	}

	wal.writing = true
	return wal
}

func (s *Shard) GetWalDir() string {
	return s.walDir
}

func (s *Shard) Stop() {
	s.signal.CloseOnce(func() {
		s.Wait()
		for _, wal := range s.wal {
			wal.MustClose()
		}
		for _, wal := range s.waitSwitchWal {
			wal.MustClose()
		}
	})
}

func (s *Shard) ForceFlush() {
	items := s.getWalList(nil, func(wal *Wal) bool {
		return wal.Opened()
	})

	for _, wal := range items {
		s.switchWalIfExists(wal)
	}
}

func (s *Shard) Wait() {
	s.wg.Wait()
}

func (s *Shard) getWalList(dst []*Wal, filter func(*Wal) bool) []*Wal {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, wal := range s.wal {
		if filter(wal) {
			dst = append(dst, wal)
		}
	}
	return dst
}

func (s *Shard) getWaitSwitchWal() *Wal {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, wal := range s.waitSwitchWal {
		return wal
	}

	return nil
}

func (s *Shard) Write(wal *Wal, seriesKey, rec []byte) error {
	if s.idle {
		s.openBackgroundProcessor()
	}

	s.lastWriteTimestamp = fasttime.UnixTimestamp()
	err := s.writeRecord(wal, seriesKey, rec)
	return err
}

func (s *Shard) UpdateWal(wal *Wal, seriesKey []byte, tr *util.TimeRange) (*Wal, error) {
	mst := util.Bytes2str(record.ReadMstFromSeriesKey(seriesKey))

	if wal == nil {
		wal = s.CreateWal(mst)
	} else if wal.mst != mst {
		wal.EndWrite()
		if err := wal.Flush(); err != nil {
			return wal, err
		}
		if err := wal.Sync(); err != nil {
			return wal, err
		}
		wal = s.CreateWal(mst)
	}

	wal.UpdateTimeRange(tr)
	return wal, nil
}

func (s *Shard) writeRecord(wal *Wal, seriesKey []byte, rec []byte) error {
	if len(seriesKey) == 0 {
		return nil
	}

	sid, err := s.info.idx.GetSeriesIdBySeriesKeyFromCache(seriesKey)
	if err != nil {
		return err
	}

	err = wal.WriteRecord(sid, seriesKey, rec)
	if err != nil {
		logger.GetLogger().Error("write record failed", zap.Error(err))
		s.switchWalIfExists(wal)
	}
	return err
}

func (s *Shard) Free() {
	d := fasttime.UnixTimestamp() - s.lastWriteTimestamp
	if d < s.freeDuration || s.lastWriteTimestamp == 0 {
		return
	}

	s.mu.Lock()
	inuse := len(s.waitSwitchWal) > 0 || len(s.wal) > 0
	if !inuse {
		clear(s.waitSwitchWal)
		clear(s.wal)
	}
	s.mu.Unlock()

	if inuse {
		return
	}

	stat.ActiveShardTotal.Decr()
	logger.GetLogger().Info("free shelf shard", zap.String("wal dir", s.walDir))
	s.idle = true
	s.signal.Close()
}

func (s *Shard) AsyncCreateIndex() {
	s.walSwap = s.getWalList(s.walSwap[:0], func(wal *Wal) bool {
		return wal.NeedCreateIndex()
	})
	for i, wal := range s.walSwap {
		s.idxCreator.Create(wal)
		s.walSwap[i] = nil
	}
}

func (s *Shard) AsyncConvertToTSSP() {
	if s.tsspConverting {
		return
	}

	wal := s.getWaitSwitchWal()
	if wal == nil {
		return
	}

	ok, release := AllocTSSPConvertSource()
	if !ok {
		return
	}

	s.tsspConverting = true
	go func() {
		defer func() {
			release()
			s.tsspConverting = false
		}()

		stat.WALWaitConvert.Decr()
		stat.WALConverting.Incr()
		s.convertWalToTSSP(wal)
		stat.WALConverting.Decr()
	}()
}

func (s *Shard) convertWalToTSSP(wal *Wal) {
	defer statistics.MilliTimeUse(stat.TSSPConvertCount, stat.TSSPConvertDurSum)()
	s.idxCreator.Create(wal)

	util.MustRun(wal.sync)

	logger.GetLogger().Info("start convert wal to tssp",
		zap.String("file", wal.Name()),
		zap.Int64("size", wal.WrittenSize()))

	itr := &WalRecordIterator{}
	itr.Init(wal)

	tsMemTable := mutable.NewTsMemTableImpl()
	orderFiles, unorderedFiles := tsMemTable.FlushRecords(s.info.tbStore, itr, wal.mst,
		s.info.filePath, s.info.lock, s.info.fileInfos)

	s.mu.Lock()
	s.info.tbStore.AddBothTSSPFiles(&wal.converted, wal.mst, orderFiles, unorderedFiles)
	delete(s.waitSwitchWal, wal.Name())
	s.mu.Unlock()

	// waiting for the query request to complete
	wal.Wait()
	wal.MustClose()
	wal.Clean()
}

func (s *Shard) Load() {
	walFiles := s.scanWalFiles()

	s.tsspConverting = true
	s.wg.Add(1)
	go func() {
		defer func() {
			s.wg.Done()
			s.tsspConverting = false
		}()

		for mst, files := range walFiles {
			s.convertWalFiles(mst, files)
		}
	}()
}

func (s *Shard) scanWalFiles() map[string][]string {
	walDir := s.GetWalDir()
	var files = make(map[string][]string)
	var mstList []string

	readFiles(walDir, func(item fs.FileInfo) {
		if item.IsDir() {
			mstList = append(mstList, item.Name())
		}
	})

	for _, mst := range mstList {
		readFiles(filepath.Join(walDir, mst), func(item fs.FileInfo) {
			if item.IsDir() || !strings.HasSuffix(item.Name(), walFileSuffixes) {
				return
			}

			files[mst] = append(files[mst], filepath.Join(walDir, mst, item.Name()))
		})
	}
	return files
}

func (s *Shard) convertWalFiles(mst string, files []string) {
	defer func() {
		if err := recover(); err != nil {
			logger.GetLogger().Error("load wal panic", zap.String("mst", mst),
				zap.Strings("files", files), zap.Any("recover", err))
		}
	}()

	for i := range files {
		wal := NewWal(s.GetWalDir(), s.info.lock, mst)
		err := wal.Load(files[i])
		if err != nil {
			logger.GetLogger().Error("failed to load wal file", zap.String("file", files[i]), zap.Error(err))
			continue
		}
		s.convertWalToTSSP(wal)
	}
}

type IndexCreator struct {
	mu   sync.Mutex
	idx  Index
	tags []influx.Tag
}

func (c *IndexCreator) Create(wal *Wal) {
	done := statistics.MicroTimeUse(stat.IndexCreateCount, stat.IndexCreateDurSum)
	defer func() {
		done()
		if e := recover(); e != nil {
			logger.GetLogger().Error("failed to create index", zap.Any("panic", e))
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()

	tags := c.tags[:0]
	var mst []byte

	for {
		seriesKey, ofs := wal.PopSeriesKey()
		if len(seriesKey) == 0 {
			break
		}

		mst, tags = influx.UnsafeParse2Tags(seriesKey, tags)
		sid, err := c.idx.CreateIndexIfNotExistsBySeries(mst, seriesKey, tags)
		if err != nil {
			logger.GetLogger().Error("failed to create index", zap.Error(err))
			continue
		}
		wal.AddSeriesOffsets(sid, ofs)
	}
	c.tags = tags
}

func readFiles(dir string, handler func(item fs.FileInfo)) {
	dirs, err := fileops.ReadDir(dir)
	if err != nil {
		logger.GetLogger().Error("failed to read dir", zap.String("dir", dir), zap.Error(err))
		return
	}

	for _, item := range dirs {
		handler(item)
	}
}
