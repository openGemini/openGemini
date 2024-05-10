/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type SortLimitRows struct {
	isAscending bool
	sortIndex   []int
	rows        [][]interface{}
	schema      record.Schemas
}

func NewSortLimitRows(sortIndex []int, schema record.Schemas) *SortLimitRows {
	return &SortLimitRows{
		sortIndex: sortIndex,
		schema:    schema,
	}
}

func (rs *SortLimitRows) Len() int {
	return len(rs.rows)
}

func (rs *SortLimitRows) Less(i, j int) bool {
	return compareRows(rs.sortIndex, rs.schema, rs.rows[j], rs.rows[i], rs.isAscending)
}

func (rs *SortLimitRows) Swap(i, j int) {
	rs.rows[i], rs.rows[j] = rs.rows[j], rs.rows[i]
}

func (rs *SortLimitRows) Pop() interface{} {
	old := rs.rows
	n := len(old)
	x := old[n-1]
	rs.rows = old[0 : n-1]
	return x
}

func (rs *SortLimitRows) Push(x interface{}) {
	rs.rows = append(rs.rows, x.([]interface{}))
}

func (rs *SortLimitRows) PopToRec() *record.Record {
	if rs.Len() == 0 {
		return nil
	}
	r := record.NewRecord(rs.schema.Copy(), false)
	r.RecMeta = &record.RecMeta{}
	for rs.Len() > 0 {
		data := heap.Pop(rs).([]interface{})
		for i := 0; i < r.Schema.Len()-1; i += 1 {
			if data[i] == nil {
				switch r.Schemas()[i].Type {
				case influx.Field_Type_Float:
					r.ColVals[i].AppendFloatNull()
				case influx.Field_Type_Int:
					r.ColVals[i].AppendIntegerNull()
				case influx.Field_Type_String:
					r.ColVals[i].AppendStringNull()
				case influx.Field_Type_Boolean:
					r.ColVals[i].AppendBooleanNull()
				default:
					continue
				}
				continue
			}
			switch r.Schemas()[i].Type {
			case influx.Field_Type_Float:
				r.ColVals[i].AppendFloat(data[i].(float64))
			case influx.Field_Type_Int:
				r.ColVals[i].AppendInteger(data[i].(int64))
			case influx.Field_Type_String:
				r.ColVals[i].AppendString(data[i].(string))
			case influx.Field_Type_Boolean:
				r.ColVals[i].AppendBoolean(data[i].(bool))
			default:
				continue
			}
		}
		r.AppendTime(data[r.Schema.Len()-1].(int64))
	}
	return r
}

func compareRows(sortIndex []int, schema record.Schemas, row1, row2 []interface{}, isAscending bool) bool {
	for _, index := range sortIndex {
		if row1[index] == nil {
			if isAscending {
				return false
			}
			continue
		}
		if row2[index] == nil {
			if !isAscending {
				return false
			}
			continue
		}
		switch schema[index].Type {
		case influx.Field_Type_Float:
			if !CompareT(row1[index].(float64), row2[index].(float64), isAscending) {
				return false
			}
		case influx.Field_Type_Int:
			if !CompareT(row1[index].(int64), row2[index].(int64), isAscending) {
				return false
			}
		case influx.Field_Type_String:
			if !CompareT(row1[index].(string), row2[index].(string), isAscending) {
				return false
			}
		default:
			continue
		}
	}
	return true
}

func CompareT[T int | int64 | float64 | string](s1, s2 T, isAscending bool) bool {
	if isAscending {
		return s1 < s2
	}
	return !(s1 < s2)
}
