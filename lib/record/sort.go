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

package record

import (
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type PrimaryKey struct {
	Key  string
	Type int32
}

type SortData struct {
	idx           int
	Times         []int64
	RowIds        []int32
	DuplicateRows []bool
	Data          []SortItem
	SortRec       *Record
}

func (d *SortData) Less(i, j int) bool {
	for idx, item := range d.Data {
		if item.Len() == 0 {
			continue
		}
		v := item.Compare(i, j)
		if v == 0 {
			if len(d.Data)-1 == idx && len(d.DuplicateRows) > 0 {
				//mark duplicate rows as true
				d.DuplicateRows[d.RowIds[j]] = true
			}
			continue
		}
		d.idx = idx //current swap column idx
		return v > 0
	}
	return false
}

func (d *SortData) Swap(i, j int) {
	for l := 0; l < len(d.Data); l++ {
		if d.Data[l].Len() == 0 {
			continue
		}

		d.Data[l].Swap(i, j)
	}
	d.RowIds[i], d.RowIds[j] = d.RowIds[j], d.RowIds[i]
	d.Times[i], d.Times[j] = d.Times[j], d.Times[i]
}

func (d *SortData) Len() int {
	return len(d.Times)
}

func (d *SortData) InitRecord(schemas Schemas) {
	if d.SortRec == nil {
		d.SortRec = NewRecordBuilder(schemas)
	} else {
		d.SortRec.ResetWithSchema(schemas)
	}
}

func (d *SortData) Init(times []int64, primaryKey, sortKey []PrimaryKey, record *Record) {
	if len(record.ColVals) == 0 {
		return
	}
	d.Data = d.Data[:0]
	size := len(times)
	if cap(d.Times) < size {
		d.Times = make([]int64, size)
	}
	d.Times = d.Times[:size]

	if cap(d.RowIds) < size {
		d.RowIds = make([]int32, size)
	}
	d.RowIds = d.RowIds[:size]

	for i := 0; i < size; i++ {
		d.RowIds[i] = int32(i)
		d.Times[i] = times[i]
	}

	orderByMap := d.setOrderByMap(sortKey, record.Schema)
	d.genSortData(times, orderByMap, sortKey, record, 0)
}

func (d *SortData) InitDuplicateRows(size int, record *Record, deduplicate bool) {
	if !deduplicate {
		d.DuplicateRows = d.DuplicateRows[:0]
	} else {
		if len(record.ColVals) == 0 {
			return
		}
		if cap(d.DuplicateRows) < size {
			d.DuplicateRows = make([]bool, size)
		} else {
			d.DuplicateRows = d.DuplicateRows[:size]
			for i := 0; i < size; i++ {
				d.DuplicateRows[i] = false
			}
		}
	}
}

func (d *SortData) genSortData(times []int64, orderByMap map[int]int, sortFileds []PrimaryKey, record *Record, start int) {
	var cv ColVal
	for i := start; i < len(sortFileds); i++ {
		if sortFileds[i].Key == "time" {
			is := IntegerSlice{}
			is.V = append(is.V, times...)
			is.CV = record.ColVals[len(record.ColVals)-1]
			d.Data = append(d.Data, &is)
			continue
		}

		cv = record.ColVals[orderByMap[i]]
		switch sortFileds[i].Type {
		case influx.Field_Type_Int:
			is := IntegerSlice{}
			is.PadIntSlice(cv)
			is.CV = cv
			d.Data = append(d.Data, &is)
		case influx.Field_Type_Float:
			fs := FloatSlice{}
			fs.PadFloatSlice(cv)
			fs.CV = cv
			d.Data = append(d.Data, &fs)
		case influx.Field_Type_String:
			ss := StringSlice{}
			ss.PadStringSlice(cv)
			ss.CV = cv
			d.Data = append(d.Data, &ss)
		case influx.Field_Type_Boolean:
			bs := BooleanSlice{}
			bs.PadBoolSlice(cv)
			bs.CV = cv
			d.Data = append(d.Data, &bs)
		}
	}
}

func (d *SortData) setOrderByMap(orderBy []PrimaryKey, schemas Schemas) map[int]int {
	orderByMap := make(map[int]int)
	for i, ob := range orderBy {
		for j := range schemas {
			if ob.Key == schemas[j].Name {
				orderByMap[i] = j
			}
		}
	}
	return orderByMap
}
