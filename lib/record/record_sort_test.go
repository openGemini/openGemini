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

package record_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Values struct {
	vs    []string
	vi    []int64
	vb    []bool
	vf    []float64
	times []int64

	nil       []bool
	nilColIdx int
}

type recordSortData struct {
	unordered Values
	exp       Values
}

var schema = record.Schemas{
	{Name: "vs", Type: influx.Field_Type_String},
	{Name: "vi", Type: influx.Field_Type_Int},
	{Name: "vb", Type: influx.Field_Type_Boolean},
	{Name: "vf", Type: influx.Field_Type_Float},
	{Name: record.TimeField, Type: influx.Field_Type_Int},
}

func TestSortHelper_Sort(t *testing.T) {
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

func TestSortHelper_SortSameTime(t *testing.T) {
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

	doSort(t, data)
}

func doSort(t *testing.T, data []recordSortData) {
	sh := record.NewSortHelper()

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

func appendValue(rec *record.Record, values Values, colIdx, i int) {
	switch colIdx {
	case 0:
		rec.ColVals[0].AppendString(values.vs[i])
	case 1:
		rec.ColVals[1].AppendInteger(values.vi[i])
	case 2:
		rec.ColVals[2].AppendBoolean(values.vb[i])
	case 3:
		rec.ColVals[3].AppendFloat(values.vf[i])
	}
}

func appendNull(rec *record.Record, colIdx int) {
	switch colIdx {
	case 0:
		rec.ColVals[0].AppendStringNull()
	case 1:
		rec.ColVals[1].AppendIntegerNull()
	case 2:
		rec.ColVals[2].AppendBooleanNull()
	case 3:
		rec.ColVals[3].AppendFloatNull()
	}
}

func buildRecord(values Values) *record.Record {
	rec := &record.Record{}
	rec.SetSchema(schema)
	rec.ColVals = make([]record.ColVal, schema.Len())

	for i := 0; i < len(values.times); i++ {
		rec.AppendTime(values.times[i])

		for n := 0; n < 4; n++ {
			if values.nil[i] && n == values.nilColIdx {
				appendNull(rec, n)
			} else {
				appendValue(rec, values, n, i)
			}
		}
	}

	return rec
}

func TestFastSortRecord(t *testing.T) {
	var run = func(n int) {
		rec := buildFastSortRecord(n)
		sortFast := &record.Record{}
		cloneRecord(rec, sortFast)
		record.FastSortRecord(sortFast, n)

		sortNormal := &record.Record{}
		cloneRecord(rec, sortNormal)
		sort.Sort(sortNormal)

		record.CheckRecord(sortFast)
		record.CheckRecord(sortNormal)

		require.Equal(t, sortNormal.String(), sortFast.String())
	}

	for _, i := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 77, 88, 7} {
		run(i)
	}
}

func cloneRecord(src, dst *record.Record) {
	dst.Schema = append(dst.Schema[:0], src.Schema...)
	dst.ColVals = append(dst.ColVals[:0], src.ColVals...)
}

func buildFastSortRecord(n int) *record.Record {
	rec := &record.Record{
		RecMeta: nil,
		ColVals: make([]record.ColVal, n),
		Schema:  make(record.Schemas, n),
	}

	for i := 10; i < n+9; i++ {
		rec.Schema[i-10].Type = influx.Field_Type_Int
		rec.Schema[i-10].Name = fmt.Sprintf("foo_%08d", i*4)
		rec.ColVals[i-10].AppendInteger(int64(i * 4))
	}
	rec.Schema[n-1].Name = record.TimeField
	rec.ColVals[n-1].AppendInteger(1000000)

	names := []int{7, 1, 2, 99, 97, 123, 143, 145, 142, 141, 3, 999999, 999997, 999998}
	sort.Ints(names)
	added := &record.Record{
		RecMeta: nil,
		ColVals: make([]record.ColVal, len(names)),
		Schema:  make(record.Schemas, len(names)),
	}

	for i, name := range names {
		added.Schema[i].Type = influx.Field_Type_Int
		added.Schema[i].Name = fmt.Sprintf("foo_%08d", name)
		added.ColVals[i].AppendInteger(int64(name))
	}

	rec.Schema = append(rec.Schema, added.Schema...)
	rec.ColVals = append(rec.ColVals, added.ColVals...)
	return rec
}

func BenchmarkFastSortRecord(b *testing.B) {
	n := 256
	rec1 := buildFastSortRecord(n)
	rec2 := buildFastSortRecord(n)
	b.ResetTimer()

	b.Run("sort-normal", func(b *testing.B) {
		var tmp = &record.Record{}
		for i := 0; i < b.N; i++ {
			cloneRecord(rec1, tmp)
			sort.Sort(tmp)
		}
	})

	b.Run("sort-fast", func(b *testing.B) {
		var tmp = &record.Record{}
		for i := 0; i < b.N; i++ {
			cloneRecord(rec2, tmp)
			record.FastSortRecord(tmp, n)
		}
	})
}
