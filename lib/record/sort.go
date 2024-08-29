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

package record

import (
	"time"

	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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

func (d *SortData) Reset() {
	d.Times = d.Times[:0]
	d.RowIds = d.RowIds[:0]
	d.DuplicateRows = d.DuplicateRows[:0]
	d.Data = d.Data[:0]
}

func (d *SortData) Less(i, j int) bool {
	for idx, item := range d.Data {
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
		d.Data[l].Swap(i, j)
	}
	d.RowIds[i], d.RowIds[j] = d.RowIds[j], d.RowIds[i]
	d.idx = 0
}

func (d *SortData) Len() int {
	return len(d.RowIds)
}

func (d *SortData) InitRecord(recSchemas Schemas) {
	schemas := make(Schemas, len(recSchemas))
	copy(schemas, recSchemas)
	if d.SortRec == nil {
		d.SortRec = NewRecordBuilder(schemas)
	} else {
		d.SortRec.ResetWithSchema(schemas)
	}
}

func (d *SortData) Init(times []int64, sortKey []PrimaryKey, record *Record, tcDuration time.Duration) {
	if len(record.ColVals) == 0 {
		return
	}
	size := len(times)

	if cap(d.RowIds) < size {
		d.RowIds = make([]int32, size)
	}
	d.RowIds = d.RowIds[:size]
	d.Data = d.Data[:0]

	for i := 0; i < size; i++ {
		d.RowIds[i] = int32(i)
	}
	if tcDuration > 0 {
		// if time cluster duration index is SET, need to sort record with "clustered" time co
		if cap(d.Times) < size {
			d.Times = make([]int64, size)
		}
		d.Times = d.Times[:size]
		for i, t := range times {
			// truncate times and get "clustered" timestamp
			d.Times[i] = int64(time.Duration(t).Truncate(tcDuration))
		}
		is := IntegerSlice{}
		is.V = append(is.V, d.Times...)
		// append "clustered" to the first column of d.Data
		d.Data = append(d.Data, &is)
	}
	d.idx = 0
	d.genSortData(times, sortKey, record, d.SortRec, 0)
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

func (d *SortData) genSortData(times []int64, sortFields []PrimaryKey, record, sortRec *Record, start int) {
	for i := start; i < len(sortFields); i++ {
		if sortFields[i].Key == "time" {
			is := IntegerSlice{}
			is.V = append(is.V, times...)
			d.Data = append(d.Data, &is)
			continue
		}

		idx := record.Schema.FieldIndex(sortFields[i].Key)
		if idx < 0 {
			Logger.GetLogger().Error("sortField is not exist")
			return
		}
		switch sortFields[i].Type {
		case influx.Field_Type_Int:
			is := IntegerSlice{}
			is.PadIntSlice(&record.ColVals[idx])
			d.Data = append(d.Data, &is)
		case influx.Field_Type_Float:
			fs := FloatSlice{}
			fs.PadFloatSlice(&record.ColVals[idx])
			d.Data = append(d.Data, &fs)
		case influx.Field_Type_String:
			ss := StringSlice{}
			ss.PadStringSlice(&record.ColVals[idx])
			d.Data = append(d.Data, &ss)
		case influx.Field_Type_Boolean:
			bs := BooleanSlice{}
			bs.PadBoolSlice(&record.ColVals[idx])
			d.Data = append(d.Data, &bs)
		}
	}
}
