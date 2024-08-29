// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import (
	"sort"
	"sync"
)

type ColumnSortHelper struct {
	aux      SortAux
	nilCount NilCount
	times    []int64
}

var columnSortHelperPool sync.Pool

func NewColumnSortHelper() *ColumnSortHelper {
	hlp, ok := columnSortHelperPool.Get().(*ColumnSortHelper)
	if !ok || hlp == nil {
		hlp = &ColumnSortHelper{}
	}
	return hlp
}

func (h *ColumnSortHelper) Release() {
	columnSortHelperPool.Put(h)
}

func (h *ColumnSortHelper) Sort(rec *Record) *Record {
	if rec.RowNums() == 0 {
		return rec
	}

	aux := &h.aux
	aux.InitRecord(rec.Schema)
	aux.Init(rec.Times())
	sort.Stable(aux)
	h.times = h.times[:0]

	return h.sort(rec, aux)
}

func (h *ColumnSortHelper) sort(rec *Record, aux *SortAux) *Record {
	aux.InitSections()
	times := aux.Times

	for i := 0; i < rec.Len()-1; i++ {
		col := rec.Column(i)
		h.initNilCount(col, len(times)+1)
		h.sortColumn(col, aux, i)
	}

	auxRec := aux.SortRec
	auxRec.AppendTime(times[0])
	for i := 1; i < len(times); i++ {
		if times[i] != times[i-1] {
			auxRec.AppendTime(times[i])
		}
	}

	rec, aux.SortRec = aux.SortRec, rec
	return rec
}

func (h *ColumnSortHelper) sortColumn(col *ColVal, aux *SortAux, n int) {
	size := aux.SectionLen()
	times := aux.Times
	dst := aux.SortRec.Column(n)
	typ := aux.SortRec.Schema[n].Type

	for i := 0; i < size; i += 2 {
		idx, rowStat, rowEnd := aux.RowIndex(i)

		if idx > 0 && times[idx] == times[idx-1] {
			h.replace(col, dst, typ, rowStat)
			rowStat++
		}

		if rowStat >= rowEnd {
			continue
		}

		dst.AppendWithNilCount(col, typ, rowStat, rowEnd, &h.nilCount)
	}
}

func (h *ColumnSortHelper) replace(col *ColVal, aux *ColVal, typ, idx int) {
	if col.IsNil(idx) {
		return
	}

	aux.deleteLast(typ)
	aux.AppendWithNilCount(col, typ, idx, idx+1, &h.nilCount)
}

func (h *ColumnSortHelper) initNilCount(col *ColVal, size int) {
	nc := &h.nilCount
	nc.init(col.NilCount, size)
	if col.NilCount == 0 {
		return
	}

	for j := 1; j < size; j++ {
		nc.value[j] = nc.value[j-1]
		if col.IsNil(j - 1) {
			nc.value[j]++
		}
	}
}

type SortAux struct {
	RowIds   []int32
	Times    []int64
	sections []int
	SortRec  *Record
}

func (aux *SortAux) Len() int {
	return len(aux.RowIds)
}

func (aux *SortAux) Less(i, j int) bool {
	return aux.Times[i] < aux.Times[j]
}

func (aux *SortAux) Swap(i, j int) {
	aux.Times[i], aux.Times[j] = aux.Times[j], aux.Times[i]
	aux.RowIds[i], aux.RowIds[j] = aux.RowIds[j], aux.RowIds[i]
}

func (aux *SortAux) Init(times []int64) {
	aux.init(times)
}

func (aux *SortAux) init(times []int64) {
	size := len(times)
	if cap(aux.Times) < size {
		aux.Times = make([]int64, size)
	}
	aux.Times = aux.Times[:size]

	if cap(aux.RowIds) < size {
		aux.RowIds = make([]int32, size)
	}
	aux.RowIds = aux.RowIds[:size]

	for i := 0; i < size; i++ {
		aux.RowIds[i] = int32(i)
		aux.Times[i] = times[i]
	}
}

func (aux *SortAux) InitRecord(schemas Schemas) {
	if aux.SortRec == nil {
		aux.SortRec = NewRecordBuilder(schemas)
	} else {
		aux.SortRec.ResetWithSchema(schemas)
	}
}

func (aux *SortAux) InitSections() {
	times := aux.Times
	rows := aux.RowIds
	sections := aux.sections[:0]
	start := 0

	for i := 0; i < len(times)-1; i++ {
		if (rows[i+1]-rows[i]) != 1 || times[i] == times[i+1] {
			sections = append(sections, start, i)
			start = i + 1
			continue
		}
	}
	sections = append(sections, start, len(rows)-1)
	aux.sections = sections
}

func (aux *SortAux) RowIndex(i int) (int, int, int) {
	start, end := aux.sections[i], aux.sections[i+1]
	rowStat, rowEnd := int(aux.RowIds[start]), int(aux.RowIds[end]+1)
	return start, rowStat, rowEnd
}

func (aux *SortAux) SectionLen() int {
	return len(aux.sections)
}
