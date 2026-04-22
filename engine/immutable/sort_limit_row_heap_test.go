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
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestSortLimitRowsByTime(t *testing.T) {
	s := []record.Field{
		{Name: record.SeqIDField, Type: influx.Field_Type_Int},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_float", Type: influx.Field_Type_Float},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	h := NewSortLimitRows([]int{4, 0}, s, 0)
	heap.Init(h)
	data := make([]interface{}, len(s))
	data[0] = int64(0)
	data[1] = int64(2)
	data[2] = 0.1
	data[3] = "test"
	data[4] = int64(1)
	heap.Push(h, data)
	data1 := make([]interface{}, len(s))
	data1[0] = int64(1)
	data1[1] = int64(2)
	data1[2] = 0.1
	data1[3] = "test"
	data1[4] = int64(1)
	heap.Push(h, data1)

	data2 := make([]interface{}, len(s))
	data2[0] = int64(1)
	data2[1] = int64(2)
	data2[2] = 0.1
	data2[3] = "test"
	data2[4] = int64(3)
	heap.Push(h, data2)

	data3 := make([]interface{}, len(s))
	data3[0] = int64(1)
	data3[1] = int64(2)
	data3[2] = 0.1
	data3[3] = "test"
	data3[4] = int64(2)
	heap.Push(h, data3)

	times := h.PopToRec().Times()
	if times[0] != 1 || times[2] != 2 || times[3] != 3 {
		t.Errorf("get wrong times")
	}
}

func TestSortLimitRowsByString(t *testing.T) {
	s := []record.Field{
		{Name: record.SeqIDField, Type: influx.Field_Type_Int},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_float", Type: influx.Field_Type_Float},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	h := NewSortLimitRows([]int{3, 0}, s, 0)
	heap.Init(h)
	data := make([]interface{}, len(s))
	data[0] = int64(0)
	data[1] = int64(2)
	data[2] = 0.1
	data[3] = "test1"
	data[4] = int64(1)
	heap.Push(h, data)
	data1 := make([]interface{}, len(s))
	data1[0] = int64(1)
	data1[1] = int64(2)
	data1[2] = 0.1
	data1[3] = "test3"
	data1[4] = int64(1)
	heap.Push(h, data1)

	data2 := make([]interface{}, len(s))
	data2[0] = int64(1)
	data2[1] = int64(2)
	data2[2] = 0.1
	data2[3] = "test2"
	data2[4] = int64(3)
	heap.Push(h, data2)

	data3 := make([]interface{}, len(s))
	data3[0] = int64(1)
	data3[1] = int64(2)
	data3[2] = 0.1
	data3[3] = "test4"
	data3[4] = int64(2)
	heap.Push(h, data3)

	times := h.PopToRec().Times()
	if times[0] != 1 || times[2] != 1 || times[3] != 2 {
		t.Errorf("get wrong times")
	}
}

func TestSortLimitRowsByFloat(t *testing.T) {
	s := []record.Field{
		{Name: record.SeqIDField, Type: influx.Field_Type_Int},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_float", Type: influx.Field_Type_Float},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	h := NewSortLimitRows([]int{2, 0}, s, 0)
	heap.Init(h)
	data := make([]interface{}, len(s))
	data[0] = int64(0)
	data[1] = int64(2)
	data[2] = 0.1
	data[3] = "test1"
	data[4] = int64(1)
	heap.Push(h, data)
	data1 := make([]interface{}, len(s))
	data1[0] = int64(1)
	data1[1] = int64(2)
	data1[2] = 0.3
	data1[3] = "test3"
	data1[4] = int64(1)
	heap.Push(h, data1)

	data2 := make([]interface{}, len(s))
	data2[0] = int64(1)
	data2[1] = int64(2)
	data2[2] = 0.2
	data2[3] = "test2"
	data2[4] = int64(3)
	heap.Push(h, data2)

	data3 := make([]interface{}, len(s))
	data3[0] = int64(1)
	data3[1] = int64(2)
	data3[2] = 0.4
	data3[3] = "test4"
	data3[4] = int64(2)
	heap.Push(h, data3)

	times := h.PopToRec().Times()
	if times[0] != 1 || times[2] != 1 || times[3] != 2 {
		t.Errorf("get wrong times")
	}
}
