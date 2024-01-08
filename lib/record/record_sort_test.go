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

package record_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
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
