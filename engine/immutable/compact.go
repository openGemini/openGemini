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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

const (
	CompactLevels    = 7
	minFileSizeLimit = 1 * 1024 * 1024
)

var (
	fullCompactingCount   int64
	maxFullCompactor      = cpu.GetCpuNum() / 2
	maxCompactor          = cpu.GetCpuNum()
	compLimiter           limiter.Fixed
	ErrCompStopped        = errors.New("compact stopped")
	ErrDownSampleStopped  = errors.New("downSample stopped")
	ErrDroppingMst        = errors.New("measurement is dropped")
	LevelCompactRule      = []uint16{0, 1, 0, 2, 0, 3, 0, 1, 2, 3, 0, 4, 0, 5, 0, 1, 2, 6}
	LevelCompactRuleForCs = []uint16{0, 1, 0, 1, 0, 1} // columnStore currently only doing level 0 and level 1 compaction,but the full functionality is available
	LeveLMinGroupFiles    = [CompactLevels]int{8, 4, 4, 4, 4, 4, 2}
	compLogSeq            = uint64(time.Now().UnixNano())

	EnableMergeOutOfOrder = true
	log                   = Log.GetLogger()

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
	orderWg, outOfOrderWg = m.ImmTable.refMmsTable(m, name, refOutOfOrder)
	return
}

func (m *MmsTables) unrefMmsTable(orderWg, outOfOrderWg *sync.WaitGroup) {
	m.ImmTable.unrefMmsTable(orderWg, outOfOrderWg)
}

func (m *MmsTables) LevelCompact(level uint16, shid uint64) error {
	plans := m.ImmTable.LevelPlan(m, level)
	for len(plans) > 0 {
		plan := plans[0]
		plan.shardId = shid
		select {
		case <-m.closed:
			return ErrCompStopped
		case <-m.stopCompMerge:
			log.Info("stop LevelCompact", zap.Uint64("id", shid))
			m.blockCompactStop(plan.name)
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			if !m.CompactionEnabled() {
				m.wg.Done()
				m.blockCompactStop(plan.name)
				return nil
			}
			go func(group *CompactGroup) {
				orderWg, inorderWg := m.ImmTable.refMmsTable(m, group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					m.ImmTable.unrefMmsTable(orderWg, inorderWg)
					compLimiter.Release()
					m.CompactDone(group.group)
					m.blockCompactStop(group.name)
					group.release()
				}()

				fi, err := m.ImmTable.NewFileIterators(m, group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}
				err = m.ImmTable.compactToLevel(m, fi, false, NonStreamingCompaction(fi))
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

func (m *MmsTables) blockCompactStop(name string) {
	if m.ImmTable.GetCompactionType(name) == config.BLOCK {
		m.inBlockCompact.Del(name)
	}
}

func (m *MmsTables) NewChunkIterators(group FilesInfo) *ChunkIterators {
	compItrs := &ChunkIterators{
		closed:        m.closed,
		stopCompMerge: m.stopCompMerge,
		dropping:      group.dropping,
		name:          group.name,
		itrs:          make([]*ChunkIterator, 0, len(group.compIts)),
		merged:        &record.Record{},
	}

	for _, i := range group.compIts {
		if m.isClosed() || m.isCompMergeStopped() {
			return nil
		}
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

	return compItrs
}

func (m *MmsTables) compact(itrs *ChunkIterators, files []TSSPFile, level uint16, isOrder bool, cLog *Log.Logger) ([]TSSPFile, error) {
	_, seq := files[0].LevelAndSequence()
	fileName := NewTSSPFileName(seq, level, 0, 0, isOrder, m.lock)
	tableBuilder := NewMsBuilder(m.path, itrs.name, m.lock, m.Conf, itrs.maxN, fileName, *m.tier, nil, itrs.estimateSize, config.TSSTORE, m.obsOpt, m.GetShardID())
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
		record.CheckTimes(rec.Times())
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

func (m *MmsTables) FullyCompacted() bool {
	return m.ImmTable.FullyCompacted(m)
}

func (m *MmsTables) FreeSequencer() bool {
	if !m.ImmTable.FullyCompacted(m) {
		return false
	}
	return m.sequencer.free()
}

func (m *MmsTables) loadIdTimes() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.Order) == 0 {
		return 0, nil
	}

	seq := m.Sequencer()
	defer seq.UnRef()
	loader := newIDTimesLoader(seq)
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

func (m *MmsTables) mmsFiles(n int64) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	groups := make([]*CompactGroup, 0, n)
	mmsTables := m.ImmTable.getFiles(m, true)
	for k, v := range mmsTables {
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
			if m.isClosed() || m.isCompMergeStopped() {
				return nil
			}
			if f.(*tsspFile).ref == 0 {
				panic("file closed")
			}

			name := f.Path()
			if tmpFileSuffix == name[len(name)-len(tmpFileSuffix):] {
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

func (m *MmsTables) GetRowCountsBySid(measurement string, sid uint64) (int64, error) {
	seq := m.Sequencer()
	m.ReloadSequencer(seq, false)
	_, rowCounts := seq.Get(measurement, sid)
	seq.UnRef()
	return rowCounts, nil
}

func (m *MmsTables) AddRowCountsBySid(measurement string, sid uint64, rowCounts int64) {
	seq := m.Sequencer()
	seq.AddRowCounts(measurement, sid, rowCounts)
	seq.UnRef()
}

func (m *MmsTables) LoadSequencer() {
	if !m.sequencer.isFree || m.sequencer.isLoading {
		return
	}

	m.ReloadSequencer(m.sequencer, true)
}

func (m *MmsTables) ReloadSequencer(seq *Sequencer, async bool) {
	if !seq.SetToInLoading() {
		return
	}

	if async {
		go m.reloadSequencer(seq)
		return
	}

	m.reloadSequencer(seq)
}

func (m *MmsTables) reloadSequencer(seq *Sequencer) {
	count, err := m.loadIdTimes()
	if err == nil && m.addFunc != nil && !m.isAdded {
		// add row count to shard
		m.addFunc(count)
		m.isAdded = true
	}

	if err != nil {
		seq.ResetMmsIdTime()
		m.logger.Error("failed to load id time", zap.Error(err))
	}
	seq.SetStat(err != nil, false)
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

	return m.executeFullCompact(shid, plans)
}

func (m *MmsTables) executeFullCompact(shid uint64, plans []*CompactGroup) error {
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
				orderWg, inorderWg := m.ImmTable.refMmsTable(m, group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					compLimiter.Release()
					m.ImmTable.unrefMmsTable(orderWg, inorderWg)
					atomic.AddInt64(&fullCompactingCount, -1)
					m.CompactDone(group.group)
				}()

				fi, err := m.ImmTable.NewFileIterators(m, group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}

				err = m.ImmTable.compactToLevel(m, fi, true, NonStreamingCompaction(fi))
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
			if err := f.Rename(fname + tmpFileSuffix); err != nil {
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
