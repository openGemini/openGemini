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

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type SortLimitCursor struct {
	isRead   bool
	options  hybridqp.Options
	schemas  record.Schemas
	input    comm.TimeCutKeyCursor
	sortHeap *SortLimitRows
}

func NewSortLimitCursor(options hybridqp.Options, schemas record.Schemas, input comm.TimeCutKeyCursor) *SortLimitCursor {
	sortIndex := make([]int, len(options.GetSortFields()))
	for fk, field := range options.GetSortFields() {
		for sk, v := range schemas {
			if field.Name == v.Name {
				sortIndex[fk] = sk
			}
		}
	}
	h := NewSortLimitRows(sortIndex, schemas)
	heap.Init(h)
	return &SortLimitCursor{
		options:  options,
		schemas:  schemas,
		input:    input,
		sortHeap: h,
	}
}

func (t *SortLimitCursor) Name() string {
	return "SortLimitCursor"
}

func (t *SortLimitCursor) StartSpan(span *tracing.Span) {
}

func (t *SortLimitCursor) EndSpan() {
}

func (t *SortLimitCursor) SinkPlan(plan hybridqp.QueryNode) {
}

func (t *SortLimitCursor) GetSchema() record.Schemas {
	return t.schemas
}

func (t *SortLimitCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if t.input == nil {
		return nil, nil, nil
	}
	if t.isRead {
		return nil, nil, nil
	}
	for {
		re, se, err := t.input.Next()
		if err != nil {
			return re, se, err
		}
		if re == nil || re.RowNums() == 0 {
			break
		}
		rowNum := re.RowNums()
		for i := 0; i < rowNum; i++ {
			data := make([]interface{}, 0, len(t.schemas))
			for k, schema := range t.schemas {
				if re.Column(k).IsNil(i) {
					data = append(data, nil)
					continue
				}
				switch schema.Type {
				case influx.Field_Type_Float:
					value, _ := re.Column(k).FloatValue(i)
					data = append(data, value)
				case influx.Field_Type_Int:
					value, _ := re.Column(k).IntegerValue(i)
					data = append(data, value)
				case influx.Field_Type_String:
					value, _ := re.Column(k).StringValueUnsafe(i)
					data = append(data, value)
				case influx.Field_Type_Boolean:
					value, _ := re.Column(k).BooleanValue(i)
					data = append(data, value)
				default:
					data = append(data, nil)
				}
			}
			if t.sortHeap.Len() < t.options.GetLimit() {
				heap.Push(t.sortHeap, data)
			} else if compareRows(t.sortHeap.sortIndex, t.schemas, data, t.sortHeap.rows[0], t.options.IsAscending()) {
				heap.Pop(t.sortHeap)
				heap.Push(t.sortHeap, data)
			}
		}

		if t.options.CanTimeLimitPushDown() && t.sortHeap.Len() >= t.options.GetLimit() {
			t.input.UpdateTime(t.sortHeap.rows[0][len(t.schemas)-1].(int64))
		}
	}
	t.isRead = true
	return t.sortHeap.PopToRec(), nil, nil
}

func (t *SortLimitCursor) Close() error {
	if t.input != nil {
		return t.input.Close()
	}
	return nil
}

func (t *SortLimitCursor) SetOps(ops []*comm.CallOption) {
}

func (t *SortLimitCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}
