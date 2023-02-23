/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immutable

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

const (
	CompactLevels        = 7
	defaultFileSizeLimit = 8 * 1024 * 1024 * 1024
	minFileSizeLimit     = 1 * 1024 * 1024
)

var (
	fullCompactingCount  int64
	maxFullCompactor     = cpu.GetCpuNum() / 2
	maxCompactor         = cpu.GetCpuNum()
	compLimiter          limiter.Fixed
	ErrCompStopped       = errors.New("compact stopped")
	ErrDownSampleStopped = errors.New("downSample stopped")
	ErrDroppingMst       = errors.New("measurement is dropped")
	LevelCompactRule     = []uint16{0, 1, 0, 2, 0, 3, 0, 1, 2, 3, 0, 4, 0, 5, 0, 1, 2, 6}
	LeveLMinGroupFiles   = [CompactLevels]int{8, 4, 4, 4, 4, 4, 2}
	compLogSeq           = uint64(time.Now().UnixNano())

	EnableMergeOutOfOrder       = true
	MaxNumOfFileToMerge         = 256
	MaxSizeOfFileToMerge  int64 = 512 * 1024 * 1024 // 512MB
	log                         = Log.GetLogger()

	compactStat = statistics.NewCompactStatistics()
)

func Init() {
	log = Log.GetLogger()
}

func SetMaxCompactor(n int) {
	log = Log.GetLogger().With(zap.String("service", "compact"))
	maxCompactor = n
	if maxCompactor == 0 {
		maxCompactor = cpu.GetCpuNum()
	}

	if maxCompactor < 2 {
		maxCompactor = 2
	}

	if maxCompactor > 32 {
		maxCompactor = 32
	}

	compLimiter = limiter.NewFixed(maxCompactor)
	log.Info("set maxCompactor", zap.Int("number", maxCompactor))
}

func SetMaxFullCompactor(n int) {
	log = Log.GetLogger().With(zap.String("service", "compact"))
	maxFullCompactor = n
	if maxFullCompactor == 0 {
		maxFullCompactor = cpu.GetCpuNum() / 2
		if maxFullCompactor < 1 {
			maxFullCompactor = 1
		}
	}

	if maxFullCompactor >= maxCompactor {
		maxFullCompactor = maxCompactor / 2
	}

	if maxFullCompactor < 1 {
		maxFullCompactor = 1
	}

	if maxFullCompactor > 32 {
		maxFullCompactor = 32
	}

	log.Info("set maxFullCompactor", zap.Int("number", maxFullCompactor))
}

func (m *MmsTables) refMmsTable(name string, refOutOfOrder bool) (orderWg, outOfOrderWg *sync.WaitGroup) {
	m.mu.RLock()
	fs, ok := m.Order[name]
	if ok {
		fs.wg.Add(1)
		orderWg = &fs.wg
	}
	if refOutOfOrder {
		fs, ok = m.OutOfOrder[name]
		if ok {
			fs.wg.Add(1)
			outOfOrderWg = &fs.wg
		}
	}
	m.mu.RUnlock()

	return
}

func (m *MmsTables) unrefMmsTable(orderWg, outOfOrderWg *sync.WaitGroup) {
	if orderWg != nil {
		orderWg.Done()
	}
	if outOfOrderWg != nil {
		outOfOrderWg.Done()
	}
}

func (m *MmsTables) LevelCompact(level uint16, shid uint64) error {
	plans := m.LevelPlan(level)
	for len(plans) > 0 {
		plan := plans[0]
		plan.shardId = shid
		select {
		case <-m.closed:
			return ErrCompStopped
		case <-m.stopCompMerge:
			log.Info("stop LevelCompact", zap.Uint64("id", shid))
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			if !m.CompactionEnabled() {
				m.wg.Done()
				return nil
			}
			go func(group *CompactGroup) {
				orderWg, inorderWg := m.refMmsTable(group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					m.unrefMmsTable(orderWg, inorderWg)
					compLimiter.Release()
					m.CompactDone(group.group)
					group.release()
				}()

				fi, err := m.NewFileIterators(group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}

				err = m.compactToLevel(fi, false, NonStreamingCompaction(fi))
				if err != nil {
					compactStat.AddErrors(1)
					log.Error("compact error", zap.Error(err))
				}
			}(plan)
		}
		plans = plans[1:]
	}

	return nil
}

func (m *MmsTables) NewChunkIterators(group FilesInfo) (*ChunkIterators, error) {
	compItrs := &ChunkIterators{
		closed:   m.closed,
		dropping: group.dropping,
		name:     group.name,
		itrs:     make([]*ChunkIterator, 0, len(group.compIts)),
		merged:   &record.Record{},
	}

	for _, i := range group.compIts {
		itr := NewChunkIterator(i)
		itr.WithLog(CLog)
		if !itr.Next() {
			itr.Close()
			continue
		}
		compItrs.itrs = append(compItrs.itrs, itr)
	}
	compItrs.maxN = group.maxChunkN
	compItrs.estimateSize = group.estimateSize

	heap.Init(compItrs)

	return compItrs, nil
}

func (m *MmsTables) compactToLevel(group FilesInfo, full, isNonStream bool) error {
	compactStatItem := statistics.NewCompactStatItem(group.name, group.shId)
	compactStatItem.Full = full
	compactStatItem.Level = group.toLevel - 1
	compactStat.AddActive(1)
	defer func() {
		compactStat.AddActive(-1)
		compactStat.PushCompaction(compactStatItem)
	}()

	var cLog *zap.Logger
	var logEnd func()
	if isNonStream {
		cLog, logEnd = logger.NewOperation(log, "Compaction", group.name)
	} else {
		cLog, logEnd = logger.NewOperation(log, "StreamCompaction", group.name)
	}
	defer logEnd()
	lcLog := Log.NewLogger(errno.ModuleCompact).SetZapLogger(cLog)
	start := time.Now()
	lcLog.Debug("start compact file", zap.Uint64("shid", group.shId), zap.Any("seqs", group.oldFids), zap.Time("start", start))
	lcLog.Debug(fmt.Sprintf("compactionGroup: name=%v, groups=%v", group.name, group.oldFids))

	var oldFilesSize int
	var newFiles []TSSPFile
	var compactErr error
	if isNonStream {
		compItrs, err := m.NewChunkIterators(group)
		if err != nil {
			lcLog.Error("new chunk iterators fail", zap.Error(err))
			return err
		}
		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = m.compact(compItrs, group.oldFiles, group.toLevel, true, lcLog)
		compItrs.Close()
	} else {
		compItrs, err := m.NewStreamIterators(group)
		if err != nil {
			lcLog.Error("new stream iterators fail", zap.Error(err))
			return err
		}
		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = compItrs.compact(group.oldFiles, group.toLevel, true)
		compItrs.Close()
	}

	if compactErr != nil {
		lcLog.Error("compact fail", zap.Error(compactErr))
		return compactErr
	}

	if err := m.ReplaceFiles(group.name, group.oldFiles, newFiles, true); err != nil {
		lcLog.Error("replace compacted file error", zap.Error(err))
		return err
	}

	end := time.Now()
	lcLog.Debug("compact file done", zap.Any("files", group.oldFids), zap.Time("end", end), zap.Duration("time used", end.Sub(start)))

	if oldFilesSize != 0 {
		compactStatItem.OriginalFileCount = int64(len(group.oldFiles))
		compactStatItem.CompactedFileCount = int64(len(newFiles))
		compactStatItem.OriginalFileSize = int64(oldFilesSize)
		compactStatItem.CompactedFileSize = SumFilesSize(newFiles)
	}
	return nil
}

func (m *MmsTables) compact(itrs *ChunkIterators, files []TSSPFile, level uint16, isOrder bool, cLog *Log.Logger) ([]TSSPFile, error) {
	_, seq := files[0].LevelAndSequence()
	fileName := NewTSSPFileName(seq, level, 0, 0, isOrder, m.lock)
	tableBuilder := AllocMsBuilder(m.path, itrs.name, m.lock, m.Conf, itrs.maxN, fileName, *m.tier, nil, itrs.estimateSize)
	tableBuilder.WithLog(cLog)
	for {
		select {
		case <-m.closed:
			return nil, ErrCompStopped
		case <-m.stopCompMerge:
			return nil, ErrCompStopped
		default:
		}

		id, rec, err := itrs.Next()
		if err != nil {
			cLog.Error("read data fail", zap.Error(err))
			return nil, err
		}

		if rec == nil || id == 0 {
			break
		}

		record.CheckRecord(rec)
		tableBuilder, err = tableBuilder.WriteRecord(id, rec, func(fn TSSPFileName) (uint64, uint16, uint16, uint16) {
			ext := fn.extent
			ext++
			return fn.seq, fn.level, 0, ext
		})
		if err != nil {
			cLog.Error("write record fail", zap.Error(err))
			return nil, err
		}
	}

	if tableBuilder.Size() > 0 {
		f, err := tableBuilder.NewTSSPFile(true)
		if err != nil {
			cLog.Error("new tssp file fail", zap.Error(err))
			return nil, err
		}
		if f != nil {
			tableBuilder.Files = append(tableBuilder.Files, f)
		}
	} else {
		tableBuilder.removeEmptyFile()
	}

	newFiles := make([]TSSPFile, 0, len(tableBuilder.Files))
	newFiles = append(newFiles, tableBuilder.Files...)
	return newFiles, nil
}

func (m *MmsTables) GetMstFileStat() *statistics.FileStat {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fileStat := statistics.NewFileStat()

	statsFunc := func(mstFiles map[string]*TSSPFiles) {
		for mstName, v := range mstFiles {
			files := v.files
			var totalSize int64
			for _, file := range files {
				size := file.FileSize()
				lv, _ := file.LevelAndSequence()
				fileStat.AddLevel(lv, size)
				totalSize += size
			}

			fileStat.AddMst(mstName, v.Len(), totalSize)
		}
	}

	statsFunc(m.Order)
	statsFunc(m.OutOfOrder)
	return fileStat
}

func (m *MmsTables) fullyCompacted() bool {
	count := 0
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.Order {
		if m.isClosed() || m.isCompMergeStopped() {
			return false
		}
		if v.fullCompacted() {
			count++
		}
	}
	return count == len(m.Order)
}

func (m *MmsTables) FreeSequencer() bool {
	if !m.fullyCompacted() {
		return false
	}
	return m.sequencer.free()
}

func (m *MmsTables) loadIdTimesInLock() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.loadIdTimes()
}

func (m *MmsTables) loadIdTimes() (int64, error) {
	if len(m.Order) == 0 {
		return 0, nil
	}

	loader := newIDTimesLoader(m.sequencer)
	// m.OutOfOrder used for statistics row count
	go loader.Load(m.path, m.Order, m.OutOfOrder)

	select {
	case <-loader.Done():
	case <-m.closed:
		loader.close()
		log.Info("table closed")
		return 0, nil
	}

	return loader.ctx.getRowCount(), loader.Error()
}

func (m *MmsTables) UnRefSequencer() {
	m.sequencer.unRef()
}

func (m *MmsTables) mmsFiles(n int64) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	groups := make([]*CompactGroup, 0, n)
	for k, v := range m.Order {
		if m.isClosed() || m.isCompMergeStopped() {
			return nil
		}

		if v.fullCompacted() {
			continue
		}

		if atomic.LoadInt64(&v.closing) > 0 {
			continue
		}

		group := &CompactGroup{
			dropping: &v.closing,
			name:     k,
			group:    make([]string, 0, v.Len()),
		}

		for _, f := range v.files {
			if f.(*tsspFile).ref == 0 {
				panic("file closed")
			}

			name := f.Path()
			if tmpTsspFileSuffix == name[len(name)-len(tmpTsspFileSuffix):] {
				continue
			}

			lv, _ := f.LevelAndSequence()
			if group.toLevel < lv {
				group.toLevel = lv
			}
			group.group = append(group.group, f.Path())
		}

		if m.acquire(group.group) {
			group.toLevel++
			groups = append(groups, group)
		}

		if len(groups) >= int(n) {
			break
		}
	}

	return groups
}

func (m *MmsTables) SetAddFunc(addFunc func(int64)) {
	m.addFunc = addFunc
}

func (m *MmsTables) GetLastFlushTimeBySid(measurement string, sid uint64) (int64, error) {
	seq, err := m.getSequencer()
	if err != nil {
		return 0, err
	}
	if seq.isLoading {
		m.UnRefSequencer()
		return math.MaxInt64, nil
	}
	lastFlushTime, _ := seq.Get(measurement, sid)
	m.UnRefSequencer()
	return lastFlushTime, nil
}

func (m *MmsTables) GetRowCountsBySid(measurement string, sid uint64) (int64, error) {
	seq, err := m.getSequencer()
	if err != nil {
		return 0, err
	}
	_, rowCounts := seq.Get(measurement, sid)
	m.UnRefSequencer()
	return rowCounts, err
}

func (m *MmsTables) AddRowCountsBySid(measurement string, sid uint64, rowCounts int64) error {
	seq, err := m.getSequencer()
	if err != nil {
		return err
	}
	seq.AddRowCounts(measurement, sid, rowCounts)
	m.UnRefSequencer()
	return err
}

func (m *MmsTables) FullCompact(shid uint64) error {
	n := int64(maxFullCompactor) - atomic.LoadInt64(&fullCompactingCount)
	if n < 1 {
		return nil
	}

	plans := m.mmsFiles(n)
	if len(plans) == 0 {
		return nil
	}

	for _, plan := range plans {
		plan.shardId = shid
		select {
		case <-m.closed:
			return ErrCompStopped
		case <-m.stopCompMerge:
			log.Info("stop FullCompact", zap.Uint64("id", shid))
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			if !m.CompactionEnabled() {
				m.wg.Done()
				return nil
			}
			atomic.AddInt64(&fullCompactingCount, 1)
			go func(group *CompactGroup) {
				orderWg, inorderWg := m.refMmsTable(group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					compLimiter.Release()
					m.unrefMmsTable(orderWg, inorderWg)
					atomic.AddInt64(&fullCompactingCount, -1)
					m.CompactDone(group.group)
				}()

				fi, err := m.NewFileIterators(group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}

				err = m.compactToLevel(fi, true, NonStreamingCompaction(fi))
				if err != nil {
					compactStat.AddErrors(1)
					log.Error("compact error", zap.Error(err))
				}
			}(plan)
		}
	}

	return nil
}

func (m *MmsTables) Wait() {
	m.wg.Wait()
}

func (m *MmsTables) deleteFiles(files ...TSSPFile) error {
	for _, f := range files {
		fname := f.Path()
		if f.Inuse() {
			if err := f.Rename(fname + tmpTsspFileSuffix); err != nil {
				if err == errFileClosed {
					continue
				}
				log.Error("rename old file error", zap.String("name", fname), zap.Error(err))
				return err
			}
			nodeTableStoreGC.Add(false, f)
		} else {
			if err := f.Remove(); err != nil {
				log.Error("remove compacted fail error", zap.String("name", fname), zap.Error(err))
				return err
			}
		}
	}
	return nil
}

var compRecPool = sync.Pool{}

func allocRecord() *record.Record {
	compactStat.AddRecordPoolGetTotal(1)
	v := compRecPool.Get()
	if v != nil {
		return v.(*record.Record)
	}
	compactStat.AddRecordPoolHitTotal(1)
	return &record.Record{}
}

func freeRecord(rec *record.Record) {
	if rec != nil {
		rec.Reset()
		compRecPool.Put(rec)
	}
}

func CompactRecovery(path string, group *CompactGroup) {
	if err := recover(); err != nil {
		panicInfo := fmt.Sprintf("[Compact Panic:err:%s, name:%s, shard:%d, level:%d, group:%v, path:%s] %s",
			err, group.name, group.shardId, group.toLevel, group.group, path, debug.Stack())
		errMsg := errno.NewError(errno.CompactPanicFail)
		log.Error(panicInfo, zap.Error(errMsg))
	}
}
