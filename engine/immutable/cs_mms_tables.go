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
	"container/heap"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/record"
	stats "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type csImmTableImpl struct {
	mu         sync.RWMutex
	primaryKey map[string]record.Schemas   // mst -> primary key
	mstsInfo   map[string]*MeasurementInfo // map[cpu-001]MeasurementInfo
}

func (c *csImmTableImpl) LevelPlan(m *MmsTables, level uint16) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}
	var plans []*CompactGroup
	minGroupFileN := LeveLMinGroupFiles[level]

	m.mu.RLock()
	for k, v := range m.CSFiles {
		plans = m.getMmsPlan(k, v, level, minGroupFileN, plans)
	}
	m.mu.RUnlock()
	return plans
}

func (c *csImmTableImpl) AddTSSPFiles(m *MmsTables, name string, isOrder bool, files ...TSSPFile) {
	m.mu.RLock()
	tables := m.CSFiles
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

func (c *csImmTableImpl) addTSSPFile(m *MmsTables, isOrder bool, f TSSPFile, nameWithVer string) {
	mmsTbls := m.CSFiles
	v, ok := mmsTbls[nameWithVer]
	if !ok || v == nil {
		v = NewTSSPFiles()
		mmsTbls[nameWithVer] = v
	}

	v.lock.Lock()
	v.files = append(v.files, f)
	v.lock.Unlock()
}

func (c *csImmTableImpl) getFiles(m *MmsTables, isOrder bool) map[string]*TSSPFiles {
	return m.CSFiles
}

func (c *csImmTableImpl) SetMstInfo(name string, mstInfo *MeasurementInfo) {
	c.mstsInfo[name] = mstInfo
}

func (c *csImmTableImpl) getTSSPFiles(m *MmsTables, mstName string, isOrder bool) (*TSSPFiles, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.CSFiles
	files, ok := mmsTbls[mstName]
	return files, ok
}

func (c *csImmTableImpl) refMmsTable(m *MmsTables, name string, refOutOfOrder bool) (*sync.WaitGroup, *sync.WaitGroup) {
	var csWg *sync.WaitGroup
	m.mu.RLock()
	fs, ok := m.CSFiles[name]
	if ok {
		fs.wg.Add(1)
		csWg = &fs.wg
	}
	m.mu.RUnlock()
	return nil, csWg
}

func (c *csImmTableImpl) unrefMmsTable(m *MmsTables, tsWg, csWg *sync.WaitGroup) {
	if csWg != nil {
		csWg.Done()
	}
}

func (c *csImmTableImpl) NewFileIterators(m *MmsTables, group *CompactGroup) (FilesInfo, error) {
	var fi FilesInfo
	fi.compIts = make(FileIterators, 0, len(group.group))
	fi.oldFiles = make([]TSSPFile, 0, len(group.group))
	fi.oldIndexFiles = make([]string, 0, len(group.group))
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
		indexFile := c.getIndexFileByTssp(m, group.name, fn)
		if indexFile == "" {
			fi.compIts.Close()
			return fi, fmt.Errorf("table %v, %v corresponding index file not find", group.name, fn)
		}
		fi.oldIndexFiles = append(fi.oldIndexFiles, indexFile)

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

func (c *csImmTableImpl) getIndexFileByTssp(m *MmsTables, mstName string, tsspFileName string) string {
	pkFileName := tsspFileName[:len(tsspFileName)-len(tsspFileSuffix)] + colstore.IndexFileSuffix
	files, ok := m.getPKFiles(mstName)
	if !ok || files == nil {
		return ""
	}
	f, exist := files.GetPKInfo(pkFileName)
	if !exist || f == nil {
		return ""
	}
	return pkFileName
}

func (c *csImmTableImpl) NewFragmentIterators(m *MmsTables, group FilesInfo, sortKey []string) (*FragmentIterators, error) {
	compItrs := getFragmentIterators()
	compItrs.closed = m.closed
	compItrs.dropping = group.dropping
	compItrs.name = group.name
	compItrs.dir = m.path
	compItrs.lock = m.lock
	compItrs.Conf = m.Conf
	compItrs.itrs = compItrs.itrs[:0]
	compItrs.SortKeyFileds = compItrs.SortKeyFileds[:0]
	compItrs.genFragmentSchema(group, sortKey)

	for _, fi := range group.compIts {
		itr, err := NewSortKeyIterator(fi, compItrs.SortKeyFileds, compItrs.ctx, compItrs.fields)
		if err != nil {
			panicInfo := fmt.Sprintf("[Column store compact Panic:err:%s, name:%s, oldFids:%v,] %s",
				err, group.name, group.oldFids, debug.Stack())
			log.Error(panicInfo)
			return nil, err
		}
		compItrs.itrs = append(compItrs.itrs, itr)
	}
	compItrs.estimateSize = group.estimateSize
	compItrs.breakPoint = &breakPoint{}

	heap.Init(compItrs)

	return compItrs, nil
}

func (c *csImmTableImpl) UpdatePrimaryKey(msName string, mstsInfo map[string]*MeasurementInfo) error {
	c.mu.RLock()
	_, ok := c.primaryKey[msName]
	c.mu.RUnlock()
	if ok {
		return nil
	}

	if mstInfo, ok := mstsInfo[msName]; ok && mstInfo != nil {
		primaryKey := make(record.Schemas, len(mstInfo.PrimaryKey))
		for i, pk := range mstInfo.PrimaryKey {
			if pk == record.TimeField {
				primaryKey[i] = record.Field{Name: pk, Type: influx.Field_Type_Int}
			} else {
				primaryKey[i] = record.Field{Name: pk, Type: record.ToPrimitiveType(mstInfo.Schema[pk])}
			}
			c.updatePrimaryKey(msName, primaryKey)
		}
		return nil
	}
	return errors.New("measurements info not found")
}

func (c *csImmTableImpl) updatePrimaryKey(mst string, pk record.Schemas) {
	c.mu.RLock()
	_, ok := c.primaryKey[mst]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.primaryKey[mst]; !ok {
			c.primaryKey[mst] = pk
		}
	}
}
