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
	"container/list"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/indirect/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/pubsub"
	"go.uber.org/zap"
)

const DefaultShardFreeDuration = 600 // Second
const backgroundSyncDuration = time.Millisecond * 100

var inuseWalCount atomic.Int64

func IncrInuseWalCount() {
	inuseWalCount.Add(1)
}

func DecrInuseWalCount() {
	inuseWalCount.Add(-1)
}

type ShardInfo struct {
	ident     *meta.ShardIdentifier
	filePath  string
	walPath   string
	lock      *string
	fileInfos chan []immutable.FileInfoExtend
	tbStore   immutable.TablesStore
	idx       Index
	options   *obs.ObsOptions
}

func NewShardInfo(ident *meta.ShardIdentifier, filePath, walPath string, lock *string,
	tbStore immutable.TablesStore, index Index, options *obs.ObsOptions) *ShardInfo {
	return &ShardInfo{
		ident:    ident,
		filePath: filePath,
		walPath:  walPath,
		lock:     lock,
		tbStore:  tbStore,
		idx:      index,
		options:  options,
	}
}

type Shard struct {
	// release background threads for shards that have no data written for a long time
	// and set the value of this attribute to true
	idle   bool
	signal *util.Signal

	walDir     string
	info       *ShardInfo
	idxCreator *IndexCreator

	mu sync.RWMutex
	wg sync.WaitGroup

	// List of Wal files waiting to be converted to tssp files
	// key of map is the path of the Wal file
	waitSwitchWalList *list.List

	wal *Wal

	freeDuration       uint64
	lastWriteTimestamp uint64
	workerID           int
}

func NewShard(workerID int, info *ShardInfo, freeDuration uint64) *Shard {
	shard := &Shard{
		idle:               true,
		signal:             util.NewSignal(),
		info:               info,
		freeDuration:       freeDuration,
		lastWriteTimestamp: fasttime.UnixTimestamp(),
		workerID:           workerID,
	}
	shard.idxCreator = NewRunner().IndexCreatorManager().Alloc(info.idx)
	shard.walDir = filepath.Join(info.walPath, strconv.FormatUint(uint64(workerID), 10))
	shard.wal = shard.CreateWal()
	shard.waitSwitchWalList = list.New()

	util.MustRun(func() error {
		return fileops.MkdirAll(shard.walDir, 0700)
	})
	return shard
}

func (si *ShardInfo) Index() Index {
	return si.idx
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
	s.wg.Add(3)
	go s.backgroundCreateIndex()
	go s.backgroundConvertToTSSP()
	go s.backgroundWalProcess()
}

func (s *Shard) backgroundConvertToTSSP() {
	defer s.wg.Done()

	util.TickerRun(time.Second, s.signal.C(), func() {
		s.ConvertToTSSP()
		s.Free()
	}, func() {})
}

func (s *Shard) backgroundCreateIndex() {
	defer s.wg.Done()

	util.TickerRun(time.Second/2, s.signal.C(), func() {
		if s.wal.NeedCreateIndex() {
			s.idxCreator.Create(s.wal)
		}
	}, func() {})
}

func (s *Shard) backgroundWalProcess() {
	defer s.wg.Done()

	after := time.Now().UnixNano() % int64(backgroundSyncDuration)
	time.AfterFunc(time.Duration(after), func() {
		util.TickerRun(backgroundSyncDuration, s.signal.C(), func() {
			s.SwitchWalIfNeeded()
			s.wal.BackgroundSync()
		}, func() {})
	})
}

func (s *Shard) GetWalReaders(dst []*Wal, mst string, tr *util.TimeRange) []*Wal {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for e := s.waitSwitchWalList.Front(); e != nil; e = e.Next() {
		w, _ := e.Value.(*Wal)
		if w.HasMeasurement(mst) && w.Overlaps(tr.Min, tr.Max) {
			w.Ref()
			dst = append(dst, w)
		}
	}

	if s.wal.HasMeasurement(mst) && s.wal.Overlaps(tr.Min, tr.Max) {
		s.wal.Ref()
		dst = append(dst, s.wal)
	}
	return dst
}

func (s *Shard) SwitchWalIfNeeded() {
	if wal := s.wal; !wal.NeedSwitch() {
		return
	}

	newWal := s.CreateWal()

	// By opening WAL in the background, resources are pre-allocated, reducing the latency of synchronous writes.
	err := newWal.open()
	if err != nil {
		logger.GetLogger().Error("open WAL fail", zap.Error(err))
		newWal = s.CreateWal()
	}

	oldWal := s.SwitchWal(newWal, false)
	if oldWal == nil {
		// wal switch failed
		newWal.Clean()
		return
	}
	util.MustRun(oldWal.sync)
}

func (s *Shard) SwitchWal(wal *Wal, force bool) *Wal {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !force && !s.wal.NeedSwitch() {
		return nil
	}

	return s.switchWal(wal)
}

func (s *Shard) switchWal(newWal *Wal) *Wal {
	if !s.wal.opened {
		return nil
	}

	oldWal := s.wal
	stat.WALFileCount.Incr()
	stat.WALFileSizeSum.Add(s.wal.WrittenSize() / 1024)

	s.waitSwitchWalList.PushBack(oldWal)
	stat.WALWaitConvert.Incr()

	s.wal = newWal
	oldWal.StateSwitching = true
	return oldWal
}

func (s *Shard) CreateWal() *Wal {
	wal := NewWal(s.GetWalDir(), s.info.lock, s.info.options)

	if config.GetStoreConfig().Consume.ConsumeEnable {
		// Notify registry that a new WAL has been created so the update
		// can be cached and broadcast to any active subscribers.
		pubsub.DefaultCachedPubSub.Publish(BuildWalCreatedEventKey(s.info),
			&WalCreatedEvent{Info: s.info, Wal: wal, WorkId: s.workerID})
	}

	return wal
}

func (s *Shard) GetWalDir() string {
	return s.walDir
}

func (s *Shard) Stop() {
	s.signal.CloseOnce(func() {
		s.Wait()
		s.switchWal(s.CreateWal())
		for e := s.waitSwitchWalList.Front(); e != nil; e = e.Next() {
			wal, _ := e.Value.(*Wal)
			s.convertWalToTSSP(wal)
			util.WaitTimeOut(wal.Wait, wal.ForceUnref, time.Minute)
			wal.Clean()
		}

		NewRunner().IndexCreatorManager().Recycle(s.idxCreator)
		s.idxCreator = nil
	})
}

func (s *Shard) ForceFlush() {
	wal := s.SwitchWal(s.CreateWal(), true)
	if wal != nil {
		util.MustRun(wal.sync)
	}
}

func (s *Shard) Wait() {
	s.wg.Wait()
}

func (s *Shard) Write(wal *Wal, seriesKey, rec []byte) error {
	s.lastWriteTimestamp = fasttime.UnixTimestamp()
	if s.idle {
		s.openBackgroundProcessor()
	}

	err := s.writeRecord(wal, seriesKey, rec)
	return err
}

func (s *Shard) UpdateWal(tr *util.TimeRange) *Wal {
	if !s.wal.writing {
		s.mu.Lock()
		s.wal.writing = true
		s.mu.Unlock()
	}

	s.wal.UpdateTimeRange(tr)
	return s.wal
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
		oldWal := s.SwitchWal(s.CreateWal(), true)
		if oldWal != nil {
			util.MustRun(oldWal.sync)
		}
	}
	return err
}

func (s *Shard) Free() {
	d := fasttime.UnixTimestamp() - s.lastWriteTimestamp
	if d < s.freeDuration || s.lastWriteTimestamp == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inuse := s.waitSwitchWalList.Len() > 0 || s.wal.WrittenSize() > 0 || s.wal.writing
	if inuse {
		return
	}

	s.wal.Clean()
	s.waitSwitchWalList.Init()
	s.wal = s.CreateWal()

	stat.ActiveShardTotal.Decr()
	logger.GetLogger().Info("free shelf shard", zap.String("wal dir", s.walDir))
	s.idle = true
	s.signal.Close()
}

func (s *Shard) ConvertToTSSP() {
	s.mu.RLock()
	e := s.waitSwitchWalList.Front()
	if e == nil {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	wal, _ := e.Value.(*Wal)

	stat.WALWaitConvert.Decr()
	stat.WALConverting.Incr()
	s.convertWalToTSSP(wal)
	stat.WALConverting.Decr()

	s.mu.Lock()
	s.waitSwitchWalList.Remove(e)
	s.mu.Unlock()

	go func() {
		// asynchronous waiting to avoid blocking the thread that converts WAL to TSSP
		// Set a wait timeout to prevent WAL files from being unable to be deleted due to reference count bugs.
		util.WaitTimeOut(wal.Wait, wal.ForceUnref, 15*time.Minute)
		wal.Clean()
	}()
}

func (s *Shard) convertWalToTSSP(wal *Wal) {
	defer statistics.MilliTimeUse(stat.TSSPConvertCount, stat.TSSPConvertDurSum)()

	util.MustRun(wal.sync)

	logger.GetLogger().Info("start convert wal to tssp",
		zap.String("file", wal.Name()),
		zap.Int64("size", wal.WrittenSize()))

	err := wal.LoadFromDisk()
	if err != nil {
		logger.GetLogger().Error("failed to load wal file",
			zap.String("file", wal.Name()), zap.Error(err))
		return
	}

	s.idxCreator.Create(wal)

	mstList := wal.seriesMap.GetAalMst()
	mstNum := len(mstList)
	if mstNum == 0 {
		return
	}

	idx := atomic.Int64{}
	var convert = func() {
		itr, release := NewWalRecordIterator(wal)
		defer release()
		for {
			n := int(idx.Add(1))
			if n > mstNum {
				return
			}
			mst := mstList[n-1]

			itr.SetSeries(wal.seriesMap.GetSeriesIDs(mst))
			tsMemTable := mutable.NewTsMemTableImpl()
			orderFiles, unorderedFiles, err := tsMemTable.FlushRecords(s.info.tbStore, itr, mst, s.info.fileInfos)
			if err != nil {
				logger.GetLogger().Error("failed to flush records", zap.Error(err), zap.String("mst", mst))
				return
			}

			wal.AddTargetTSSPFiles(orderFiles...)
			wal.AddTargetTSSPFiles(unorderedFiles...)
			s.info.tbStore.AddBothTSSPFiles(nil, mst, orderFiles, unorderedFiles)
		}
	}

	var parallel = min(mstNum, max(1, conf.OneTSSPConvertConcurrent))
	wg := sync.WaitGroup{}
	wg.Add(parallel)

	for range parallel {
		if int(idx.Load()) > mstNum {
			wg.Done()
			continue
		}

		tsspConvertLimited.Take()
		stat.ConvertParallel.Incr()

		go func() {
			defer func() {
				wg.Done()
				tsspConvertLimited.Release()
				stat.ConvertParallel.Decr()
			}()
			convert()
		}()
	}

	wg.Wait()
}

func (s *Shard) Load() {
	walDir := s.GetWalDir()

	readFiles(walDir, func(item fs.FileInfo) {
		if item.IsDir() || !strings.HasSuffix(item.Name(), walFileSuffixes) {
			return
		}

		s.loadWalFile(filepath.Join(walDir, item.Name()))
	})
}

func (s *Shard) loadWalFile(file string) {
	defer func() {
		if err := recover(); err != nil {
			logger.GetLogger().Error("load wal panic",
				zap.String("file", file), zap.Any("recover", err))
		}
	}()

	wal := s.CreateWal()
	wal.PreLoad(file)
	stat.WALWaitConvert.Incr()
	s.mu.Lock()
	s.waitSwitchWalList.PushBack(wal)
	s.mu.Unlock()
}

type IndexCreatorManager struct {
	mu       sync.Mutex
	creators map[Index]*IndexCreator
}

func NewIndexCreatorManager() *IndexCreatorManager {
	return &IndexCreatorManager{
		creators: make(map[Index]*IndexCreator),
	}
}

func (icm *IndexCreatorManager) Alloc(idx Index) *IndexCreator {
	icm.mu.Lock()
	defer icm.mu.Unlock()

	creator, ok := icm.creators[idx]
	if !ok {
		creator = &IndexCreator{
			idx:  idx,
			tags: nil,
		}
		icm.creators[idx] = creator
	}
	creator.ref()
	return creator
}

func (icm *IndexCreatorManager) Recycle(creator *IndexCreator) {
	if creator.unref() > 0 {
		return
	}

	icm.mu.Lock()
	defer icm.mu.Unlock()

	if creator.idle() {
		delete(icm.creators, creator.idx)
	}
}

type IndexCreator struct {
	mu   sync.Mutex
	idx  Index
	tags []influx.Tag
	rv   atomic.Int64
}

func (c *IndexCreator) ref() {
	c.rv.Add(1)
}

func (c *IndexCreator) unref() int64 {
	return c.rv.Add(-1)
}

func (c *IndexCreator) idle() bool {
	return c.rv.Load() == 0
}

func (c *IndexCreator) Create(wal *Wal) {
	// Avoid conflicts during index creation
	c.mu.Lock()
	defer c.mu.Unlock()

	if !wal.NeedCreateIndex() {
		return
	}

	var maxLatency int64 = 0
	var createCount int64 = 0
	var begin = time.Now()
	defer func() {
		stat.IndexCreateCount.Add(createCount)
		stat.IndexCreateDurSum.AddSinceMicro(begin)
		stat.IndexLatency.Store(uint64(maxLatency) / 1000)
		if e := recover(); e != nil {
			logger.GetLogger().Error("failed to create index", zap.Any("panic", e))
		}
	}()

	tags := c.tags[:0]
	var mst []byte

	for {
		seriesKey, ofs, ts := wal.PopSeriesKey()
		if len(seriesKey) == 0 {
			break
		}

		mst, tags = influx.UnsafeParse2Tags(seriesKey, tags)
		sid, err := c.idx.CreateIndexIfNotExistsBySeries(mst, seriesKey, tags)
		if err != nil {
			logger.GetLogger().Error("failed to create index", zap.Error(err))
			continue
		}
		wal.MapSeries(util.Bytes2str(mst), sid)
		wal.AddSeriesOffsets(sid, ofs)
		maxLatency = max(maxLatency, time.Now().UnixNano()-ts)
		createCount++
	}
	c.tags = tags
}

func readFiles(dir string, handler func(item fs.FileInfo)) {
	dirs, err := fileops.ReadDir(dir)
	if fileops.DirNotExists(err) {
		return
	}

	if err != nil {
		logger.GetLogger().Error("failed to read dir", zap.String("dir", dir), zap.Error(err))
		return
	}

	for _, item := range dirs {
		handler(item)
	}
}
