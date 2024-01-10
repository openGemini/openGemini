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

package record_test

import (
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestColumnSortHelper_Sort(t *testing.T) {
	data := []recordSortData{
		{
			unordered: Values{
				vs:    []string{"a", "b", "c", "d", "e"},
				vi:    []int64{3, 1, 2, 5, 4},
				vb:    []bool{true, true, false, false, false},
				vf:    []float64{.3, .1, .2, .5, .4},
				times: []int64{3, 1, 2, 5, 4},
				nil:   []bool{false, false, false, false, false},
			},
			exp: Values{
				vs:    []string{"b", "c", "a", "e", "d"},
				vi:    []int64{1, 2, 3, 4, 5},
				vb:    []bool{true, false, true, false, false},
				vf:    []float64{.1, .2, .3, .4, .5},
				times: []int64{1, 2, 3, 4, 5},
			},
		}, {
			unordered: Values{
				vs:    []string{"a", "b", "c", "d", "e"},
				vi:    []int64{5, 4, 3, 2, 1},
				vb:    []bool{false, false, false, true, true},
				vf:    []float64{.5, .4, .3, .2, .1},
				times: []int64{5, 4, 3, 2, 1},
				nil:   []bool{false, false, false, false, false},
			},
			exp: Values{
				vs:    []string{"e", "d", "c", "b", "a"},
				vi:    []int64{1, 2, 3, 4, 5},
				vb:    []bool{true, true, false, false, false},
				vf:    []float64{.1, .2, .3, .4, .5},
				times: []int64{1, 2, 3, 4, 5},
			},
		}, {
			unordered: Values{
				vs:    []string{"a", "b", "c", "d", "e"},
				vi:    []int64{1, 2, 3, 4, 5},
				vb:    []bool{true, true, false, false, false},
				vf:    []float64{.1, .2, .3, .4, .5},
				times: []int64{1, 2, 3, 4, 5},
				nil:   []bool{false, false, false, false, false},
			},
			exp: Values{
				vs:    []string{"a", "b", "c", "d", "e"},
				vi:    []int64{1, 2, 3, 4, 5},
				vb:    []bool{true, true, false, false, false},
				vf:    []float64{.1, .2, .3, .4, .5},
				times: []int64{1, 2, 3, 4, 5},
			},
		},
	}

	item := recordSortData{
		unordered: Values{
			vs:        []string{"a", "b", "c", "d", "e"},
			vi:        []int64{1, 2, 5, 3, 4},
			vb:        []bool{true, true, false, false, false},
			vf:        []float64{.1, .2, .5, .3, .4},
			times:     []int64{1, 2, 5, 3, 4},
			nil:       []bool{false, true, false, true, false},
			nilColIdx: 0,
		},
		exp: Values{
			vs:    []string{"a", "b", "d", "e", "c"},
			vi:    []int64{1, 2, 3, 4, 5},
			vb:    []bool{true, true, false, false, false},
			vf:    []float64{.1, .2, .3, .4, .5},
			times: []int64{1, 2, 3, 4, 5},
		},
	}
	for i := 0; i < 4; i++ {
		tmp := item
		tmp.unordered.nilColIdx = i

		switch i {
		case 0:
			tmp.exp.vs = []string{"a", "e", "c"}
		case 1:
			tmp.exp.vi = []int64{1, 4, 5}
		case 2:
			tmp.exp.vb = []bool{true, false, false}
		case 3:
			tmp.exp.vf = []float64{.1, .4, .5}
		}

		data = append(data, tmp)
	}

	doSort(t, data)
}

func TestColumnSortHelper_SortSameTime(t *testing.T) {
	data := []recordSortData{
		{
			unordered: Values{
				vs:        []string{"a", "b", "c", "d", "e", "xx", "oo"},
				vi:        []int64{3, 1, 2, 5, 4, 22, 55},
				vb:        []bool{true, true, false, false, false, true, false},
				vf:        []float64{.3, .1, .2, .5, .4, .22, .55},
				times:     []int64{3, 1, 2, 5, 4, 2, 5},
				nil:       []bool{false, false, false, true, false, true, false},
				nilColIdx: 1,
			},
			exp: Values{
				vs:    []string{"b", "xx", "a", "e", "oo"},
				vi:    []int64{1, 2, 3, 4, 55},
				vb:    []bool{true, true, true, false, false},
				vf:    []float64{.1, .22, .3, .4, .55},
				times: []int64{1, 2, 3, 4, 5},
			},
		},
		{
			unordered: Values{
				vs:        []string{"a", "b", "c", "d", "e", "xx", "oo"},
				vi:        []int64{3, 1, 2, 5, 4, 22, 55},
				vb:        []bool{true, true, false, false, false, true, false},
				vf:        []float64{.3, .1, .2, .5, .4, .22, .55},
				times:     []int64{3, 1, 2, 5, 4, 2, 5},
				nil:       []bool{false, false, false, true, false, true, false},
				nilColIdx: 0,
			},
			exp: Values{
				vs:    []string{"b", "c", "a", "e", "oo"},
				vi:    []int64{1, 22, 3, 4, 55},
				vb:    []bool{true, true, true, false, false},
				vf:    []float64{.1, .22, .3, .4, .55},
				times: []int64{1, 2, 3, 4, 5},
			},
		},
	}

	doColumnSort(t, data)
}

func doColumnSort(t *testing.T, data []recordSortData) {
	sh := record.NewColumnSortHelper()

	for i := 0; i < len(data); i++ {
		rec := buildRecord(data[i].unordered)
		sr := sh.Sort(rec)

		exp := data[i].exp

		if !assert.Equal(t, exp.times, sr.Times()) {
			return
		}
		if !assert.Equal(t, exp.vs, sr.ColVals[0].StringValues(nil)) {
			return
		}
		if !assert.Equal(t, exp.vi, sr.ColVals[1].IntegerValues()) {
			return
		}
		if !assert.Equal(t, exp.vb, sr.ColVals[2].BooleanValues()) {
			return
		}
		if !assert.Equal(t, exp.vf, sr.ColVals[3].FloatValues()) {
			return
		}
	}
}

func BenchmarkColumnSort(b *testing.B) {
	rec := buildRandomTimeRecord()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hlp := record.NewColumnSortHelper()
		hlp.Sort(rec)
	}
}

func BenchmarkRecordSort(b *testing.B) {
	rec := buildRandomTimeRecord()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hlp := record.NewSortHelper()
		hlp.Sort(rec)
	}
}

func buildRandomTimeRecord() *record.Record {
	rec := record.NewRecordBuilder(record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int1"},
		record.Field{Type: influx.Field_Type_Int, Name: "int2"},
		record.Field{Type: influx.Field_Type_Int, Name: "int3"},
		record.Field{Type: influx.Field_Type_Int, Name: "int4"},
		record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
	})

	values := make([]int64, 10000)
	for i := 0; i < len(rec.Schema)-1; i++ {
		rec.ColVals[i].AppendIntegers(values...)
	}
	for i := 0; i < len(values); i++ {
		values[i] = int64(i)
	}
	sort.Slice(values, func(i, j int) bool {
		return i%2 == 0
	})
	rec.AppendTime(values...)
	return rec
}
