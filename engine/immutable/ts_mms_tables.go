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

	stats "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

type tsImmTableImpl struct {
}

func (t *tsImmTableImpl) LevelPlan(m *MmsTables, level uint16) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}
	var plans []*CompactGroup
	minGroupFileN := LeveLMinGroupFiles[level]

	m.mu.RLock()
	for k, v := range m.Order {
		plans = m.getMmsPlan(k, v, level, minGroupFileN, plans)
	}
	m.mu.RUnlock()
	return plans
}

func (t *tsImmTableImpl) AddTSSPFiles(m *MmsTables, name string, isOrder bool, files ...TSSPFile) {
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
		stats.IOStat.AddIOSnapshotBytes(f.FileSize())
	}

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

func (t *tsImmTableImpl) unrefMmsTable(m *MmsTables, orderWg, outOfOrderWg *sync.WaitGroup) {
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

func (t *tsImmTableImpl) SetMstInfo(name string, mstInfo *MeasurementInfo) {}
