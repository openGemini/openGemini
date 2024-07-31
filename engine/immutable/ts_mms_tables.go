/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

type tsImmTableImpl struct {
	mstsInfo map[string]*meta.MeasurementInfo
}

func NewTsImmTable() *tsImmTableImpl {
	return &tsImmTableImpl{
		mstsInfo: make(map[string]*meta.MeasurementInfo),
	}
}

func (c *tsImmTableImpl) GetEngineType() config.EngineType {
	return config.TSSTORE
}

func (c *tsImmTableImpl) GetCompactionType(name string) config.CompactionType {
	return config.ROW
}

func (c *tsImmTableImpl) UpdateAccumulateMetaIndexInfo(name string, index *AccumulateMetaIndex) {
}

func (t *tsImmTableImpl) compactToLevel(m *MmsTables, group FilesInfo, full, isNonStream bool) error {
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

	parquetPlan := NewTSSP2ParquetPlan(group.toLevel)

	var oldFilesSize int
	var newFiles []TSSPFile
	var compactErr error
	if isNonStream {
		compItrs := m.NewChunkIterators(group)
		if compItrs == nil {
			group.compIts.Close()
			return nil
		}
		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = m.compact(compItrs, group.oldFiles, group.toLevel, true, lcLog)
		compItrs.Close()
	} else {
		compItrs := m.NewStreamIterators(group)
		if compItrs == nil {
			group.compIts.Close()
			return nil
		}

		if parquetPlan.Enable() {
			compItrs.SetHook(parquetPlan)
		}

		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = compItrs.compact(group.oldFiles, group.toLevel, true)
		if compactErr != nil {
			compItrs.RemoveTmpFiles()
		}
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

func (t *tsImmTableImpl) LevelPlan(m *MmsTables, level uint16) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}
	var plans []*CompactGroup
	minGroupFileN := LeveLMinGroupFiles[level]

	m.mu.RLock()
	for k, v := range m.Order {
		if m.isClosed() || m.isCompMergeStopped() {
			break
		}
		plans = m.getMmsPlan(k, v, level, minGroupFileN, plans)
	}
	m.mu.RUnlock()
	return plans
}

func (t *tsImmTableImpl) makeTSSPFiles(m *MmsTables, name string, isOrder bool, files []TSSPFile) *TSSPFiles {
	m.mu.RLock()
	tables := m.Order
	if !isOrder {
		tables = m.OutOfOrder
	}
	fs, ok := tables[name]
	m.mu.RUnlock()

	if !ok || fs == nil {
		m.mu.Lock()
		fs, ok = tables[name]
		if !ok {
			fs = NewTSSPFiles()
			tables[name] = fs
		}
		m.mu.Unlock()
	}

	for _, f := range files {
		statistics.IOStat.AddIOSnapshotBytes(f.FileSize())
	}
	return fs
}

// use for flush tsEngine Table
func (t *tsImmTableImpl) AddBothTSSPFiles(flushed *bool, m *MmsTables, name string, orderFiles []TSSPFile, unorderFiles []TSSPFile) {
	var orderFs *TSSPFiles
	var unorderFs *TSSPFiles
	if len(orderFiles) != 0 {
		orderFs = t.makeTSSPFiles(m, name, true, orderFiles)
	}
	if len(unorderFiles) != 0 {
		unorderFs = t.makeTSSPFiles(m, name, false, unorderFiles)
	}
	if orderFs != nil {
		orderFs.lock.Lock()
		defer orderFs.lock.Unlock()
	}
	if unorderFs != nil {
		unorderFs.lock.Lock()
		defer unorderFs.lock.Unlock()
	}
	if orderFs != nil {
		orderFs.files = append(orderFs.files, orderFiles...)
		sort.Sort(orderFs)
	}
	if unorderFs != nil {
		unorderFs.files = append(unorderFs.files, unorderFiles...)
		sort.Sort(unorderFs)
	}
	if flushed != nil {
		*flushed = true
	}
}

func (t *tsImmTableImpl) AddTSSPFiles(m *MmsTables, name string, isOrder bool, files ...TSSPFile) {
	fs := t.makeTSSPFiles(m, name, isOrder, files)

	fs.lock.Lock()
	fs.files = append(fs.files, files...)
	sort.Sort(fs)
	fs.lock.Unlock()
}

func (t *tsImmTableImpl) addTSSPFile(m *MmsTables, isOrder bool, f TSSPFile, nameWithVer string) {
	mmsTbls := m.Order
	if !isOrder {
		mmsTbls = m.OutOfOrder
	}

	v, ok := mmsTbls[nameWithVer]
	if !ok || v == nil {
		v = NewTSSPFiles()
		mmsTbls[nameWithVer] = v
	}
	v.lock.Lock()
	v.files = append(v.files, f)
	v.lock.Unlock()
}

func (t *tsImmTableImpl) getTSSPFiles(m *MmsTables, mstName string, isOrder bool) (*TSSPFiles, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.Order
	if !isOrder {
		mmsTbls = m.OutOfOrder
	}

	files, ok := mmsTbls[mstName]
	return files, ok
}

func (t *tsImmTableImpl) refMmsTable(m *MmsTables, name string, refOutOfOrder bool) (*sync.WaitGroup, *sync.WaitGroup) {
	var orderWg *sync.WaitGroup
	var outOfOrderWg *sync.WaitGroup
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

	return orderWg, outOfOrderWg
}

func (t *tsImmTableImpl) unrefMmsTable(orderWg, outOfOrderWg *sync.WaitGroup) {
	if orderWg != nil {
		orderWg.Done()
	}
	if outOfOrderWg != nil {
		outOfOrderWg.Done()
	}
}

func (t *tsImmTableImpl) NewFileIterators(m *MmsTables, group *CompactGroup) (FilesInfo, error) {
	var fi FilesInfo
	fi.compIts = make(FileIterators, 0, len(group.group))
	fi.oldFiles = make([]TSSPFile, 0, len(group.group))
	for _, fn := range group.group {
		if m.isClosed() || m.isCompMergeStopped() {
			fi.compIts.Close()
			return fi, ErrCompStopped
		}
		if atomic.LoadInt64(group.dropping) > 0 {
			fi.compIts.Close()
			return fi, ErrDroppingMst
		}
		f := m.File(group.name, fn, true)
		if f == nil {
			fi.compIts.Close()
			return fi, fmt.Errorf("table %v, %v, %v not find", group.name, fn, true)
		}
		fi.oldFiles = append(fi.oldFiles, f)
		itr := NewFileIterator(f, CLog)
		if itr.NextChunkMeta() {
			fi.compIts = append(fi.compIts, itr)
		} else {
			continue
		}

		fi.updatingFilesInfo(f, itr)
	}
	if len(fi.compIts) < 2 {
		return fi, fmt.Errorf("no enough files to do compact, iterator size: %d", len(fi.compIts))
	}
	fi.updateFinalFilesInfo(group)
	return fi, nil
}

func (t *tsImmTableImpl) getFiles(m *MmsTables, isOrder bool) map[string]*TSSPFiles {
	mmsTables := m.Order
	if !isOrder {
		mmsTables = m.OutOfOrder
	}
	return mmsTables
}

func (t *tsImmTableImpl) SetMstInfo(name string, mstInfo *meta.MeasurementInfo) {}

func (t *tsImmTableImpl) GetMstInfo(name string) (*meta.MeasurementInfo, bool) {
	return nil, false
}

func (t *tsImmTableImpl) FullyCompacted(m *MmsTables) bool {
	count := 0
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.Order {
		if v.fullCompacted() {
			count++
		}
	}
	return count == len(m.Order)
}
