// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"path"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	CompactLevels    = 7
	minFileSizeLimit = 1 * 1024 * 1024
)

var (
	fullCompactingCount  int64
	maxFullCompactor     = cpu.GetCpuNum() / 2
	maxCompactor         = cpu.GetCpuNum()
	CompLimiter          = limiter.NewFixed(maxCompactor)
	ErrCompStopped       = errors.New("compact stopped")
	ErrDownSampleStopped = errors.New("downSample stopped")
	ErrDroppingMst       = errors.New("measurement is dropped")
	ErrParquetStopped    = errors.New("parquet task stopped")
	LevelCompactRule     = []uint16{0, 1, 0, 2, 0, 3, 0, 1, 2, 3, 0, 4, 0, 5, 0, 1, 2, 6}
	LeveLMinGroupFiles   = [CompactLevels]int{8, 4, 4, 4, 4, 4, 2}
	compLogSeq           = uint64(time.Now().UnixNano())

	EnableMergeOutOfOrder = true
	log                   = Log.GetLogger()

	compactStat = statistics.NewCompactStatistics()
)

func Init() {
	log = Log.GetLogger()
}

func calculateMaxCompactor() int {
	cpuN := cpu.GetCpuNumWithoutRatio()
	// 2U:2, 4U:2, 8U:4, 16U:6, 32U:8, 64U:10
	return max(2, 2*int(math.Log2(float64(cpuN)))-2)
}

func SetMaxCompactor(n int) {
	log = Log.GetLogger().With(zap.String("service", "compact"))
	maxCompactor = n
	if maxCompactor == 0 {
		maxCompactor = calculateMaxCompactor()
	}
	maxCompactor = min(32, maxCompactor)

	CompLimiter = limiter.NewFixed(maxCompactor)
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
	levelLimited := config.GetStoreConfig().Compact.MaxCompactionLevel
	if levelLimited > 0 && int(level) >= levelLimited {
		return nil
	}

	plans := m.ImmTable.LevelPlan(m, level)

	if len(plans) == 0 {
		return nil
	}

	taskGroups := m.buildCompactTaskGroup(plans, false, shid)
	for _, group := range taskGroups {
		if config.GetCommon().TaskNodeEnabled && m.ImmTable.GetEngineType() == config.TSSTORE {
			m.registerTasks(group.GetTasks())
		} else {
			m.scheduler.ExecuteTaskGroup(group, m.stopCompMerge)
		}
	}
	return nil
}

func (m *MmsTables) registerTasks(srcTasks []scheduler.Task) {
	for _, task := range srcTasks {
		t, ok := task.(*CompactTask)
		if !ok {
			continue
		}
		m.scheduler.RegisterCompactTask(t)
	}
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
	tableBuilder := NewMsBuilder(m.path, itrs.name, m.lock, m.Conf, itrs.maxN, fileName, FilesMergedTire(files),
		nil, itrs.estimateSize, config.TSSTORE, m.obsOpt, m.GetShardID())
	tableBuilder.WithLog(cLog)
	success := false
	defer func() {
		if !success {
			tableBuilder.Clean()
		}
	}()

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

		times := rec.Times()
		if !slices.IsSorted(times) {
			return nil, errno.NewError(errno.CompactTimeDisorder, times[0], times[len(times)-1])
		}

		record.CheckRecord(rec)

		if m.indexMergeSet != nil && m.indexMergeSet.HasDeletedTSID(id) {
			continue
		}

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

	success = true
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

func (m *MmsTables) buildFullCompactPlan(n int64, toLevel uint16) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}

	builder := &CompactGroupBuilder{
		limit:        int(n),
		parquetLevel: config.TSSPToParquetLevel(),
		lowLevelMode: toLevel > 0,
		level:        toLevel,
	}
	defer builder.Release()

	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTables := m.ImmTable.getFiles(m, true)
	for k, v := range mmsTables {
		if m.isClosed() || m.isCompMergeStopped() {
			return nil
		}

		if v.hasUnloadFile() {
			ReloadSpecifiedFiles(m, k, v)
		}

		if m.scheduler.IsRunning(k) || atomic.LoadInt64(&v.closing) > 0 || v.fullCompacted() || v.hasUnloadFile() {
			continue
		}

		builder.Init(k, m.path, &v.closing, v.Len(), m.lock)
		for _, f := range v.files {
			if m.isClosed() || m.isCompMergeStopped() {
				return nil
			}
			if f.(*tsspFile).ref == 0 {
				name := f.FileName()
				m.logger.Warn("file closed",
					zap.String("path", m.path),
					zap.String("mst", k),
					zap.String("file", name.String()))
				return nil
			}

			name := f.Path()
			if len(name) == 0 || strings.HasSuffix(name, tmpFileSuffix) {
				continue
			}

			if !builder.AddFile(f) {
				return nil
			}
		}

		builder.SwitchGroup()
		if builder.Limited() {
			break
		}
	}

	return builder.groups
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

	if preLevel := config.PreFullCompactLevel(); preLevel > 0 {
		plans := m.buildFullCompactPlan(n, preLevel)
		if len(plans) > 0 {
			m.executePlan(plans, shid)
			return nil
		}
	}

	plans := m.buildFullCompactPlan(n, 0)
	if len(plans) == 0 {
		return nil
	}
	m.executePlan(plans, shid)

	return nil
}

func (m *MmsTables) executePlan(plans []*CompactGroup, shid uint64) {
	tasks := m.buildCompactTasks(plans, true, shid)
	if config.GetCommon().TaskNodeEnabled && m.ImmTable.GetEngineType() == config.TSSTORE {
		m.registerTasks(tasks)
	} else {
		m.scheduler.ExecuteBatch(tasks, m.stopCompMerge)
	}
}

func (m *MmsTables) RenameFileToLevel(name string, group []string, toLevel uint16) error {
	files, err := m.getFilesByPath(name, group, true)
	if err != nil {
		return err
	}
	file := files.files[0]
	tsspFileName := file.FileName()
	tsspFileName.level = toLevel
	err = file.Rename(tsspFileName.Path(path.Dir(file.Path()), false))
	if err == nil {
		file.UpdateLevel(toLevel)
	}
	return err
}

func (m *MmsTables) buildCompactTasks(plans []*CompactGroup, full bool, shardId uint64) []scheduler.Task {
	tasks := make([]scheduler.Task, 0, len(plans))
	for _, plan := range plans {
		plan.shardId = shardId

		task := NewCompactTask(m, plan, full)
		tasks = append(tasks, task)
	}
	return tasks
}

func (m *MmsTables) buildCompactTaskGroup(plans []*CompactGroup, full bool, shardId uint64) []*scheduler.TaskGroup {
	var taskGroups []*scheduler.TaskGroup
	var taskGroup *scheduler.TaskGroup

	var appendGroup = func() {
		if taskGroup != nil && taskGroup.Len() > 0 {
			taskGroups = append(taskGroups, taskGroup)
		}
	}

	for _, plan := range plans {
		if taskGroup == nil || taskGroup.Key() != plan.name {
			appendGroup()
			taskGroup = scheduler.NewTaskGroup(plan.name)
		}

		plan.shardId = shardId
		taskGroup.Add(NewCompactTask(m, plan, full))
	}

	appendGroup()
	return taskGroups
}

func (m *MmsTables) Wait() {
	util.WaitTimeOut(m.wg.Wait, m.wg.Done, 10*time.Minute)
	m.scheduler.Wait()
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
			nodeTableStoreGC.Add(f)
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
