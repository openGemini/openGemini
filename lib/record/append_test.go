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
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

var defaultSchemas = record.Schemas{
	record.Field{Type: influx.Field_Type_Int, Name: "int"},
	record.Field{Type: influx.Field_Type_Float, Name: "float"},
	record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
	record.Field{Type: influx.Field_Type_String, Name: "string"},
	record.Field{Type: influx.Field_Type_Int, Name: "time"},
}

func genRecToAppendInit(schema record.Schemas) *record.Record {
	return genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{0})
}

func genRecToAppend(schema record.Schemas, middle bool) *record.Record {
	newRec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{100, 101, 102, 103},
		[]int{1, 1, 1, 1}, []float64{1.0, 1.1, 1.2, 1.3},
		[]int{0, 0, 1, 1}, []string{"", "", "", "world"},
		[]int{0, 0, 0, 0}, []bool{true, true, true, true},
		[]int64{1, 2, 3, 4})

	if middle {
		newRec.Merge(genRowRec(schema,
			[]int{1, 1, 1, 1}, []int64{200, 201, 202, 203},
			[]int{1, 1, 1, 1}, []float64{2.0, 2.1, 2.2, 2.3},
			[]int{0, 0, 1, 1}, []string{"mm", "mm", "mm", "mm"},
			[]int{0, 0, 0, 0}, []bool{true, true, true, true},
			[]int64{5, 6, 7, 8}))
	}

	newRec.Merge(genRowRec(schema,
		[]int{0, 0, 0, 1}, []int64{1107, 0, 0, 0},
		[]int{0, 1, 0, 0}, []float64{11.5, 0, 0, 0},
		[]int{1, 0, 0, 0}, []string{"hello", "", "", ""},
		[]int{0, 0, 0, 0}, []bool{false, false, false, false},
		[]int64{15, 16, 17, 18}))

	return newRec
}

func TestAppendSequence(t *testing.T) {
	oldRec1 := genRecToAppendInit(defaultSchemas)
	oldRec2 := genRecToAppendInit(defaultSchemas)
	newRec := genRecToAppend(defaultSchemas, false)

	sort.Sort(newRec)
	sort.Sort(oldRec1)
	sort.Sort(oldRec2)
	oldRec2.Merge(newRec)

	r1 := record.NewAppendRecord(oldRec1)
	r2 := record.NewAppendRecord(newRec)

	r1.AppendSequence(r2, 1)
	r1.AppendSequence(r2, 3)
	r1.AppendSequence(r2, 2)
	r1.AppendSequence(r2, 2)

	if !testRecsEqual(oldRec1, oldRec2) {
		t.Fatal("error result")
	}
}

func TestAppendSequence_Skip(t *testing.T) {
	oldRec1 := genRecToAppendInit(defaultSchemas)
	oldRec2 := genRecToAppendInit(defaultSchemas)
	newRec1 := genRecToAppend(defaultSchemas, true)
	newRec2 := genRecToAppend(defaultSchemas, false)

	sort.Sort(newRec1)
	sort.Sort(newRec2)
	sort.Sort(oldRec1)
	sort.Sort(oldRec2)
	oldRec2.Merge(newRec2)

	r1 := record.NewAppendRecord(oldRec1)
	r2 := record.NewAppendRecord(newRec1)

	r1.AppendSequence(r2, 1)
	r1.AppendSequence(r2, 3)
	r2.Skip(4)
	r1.AppendSequence(r2, 2)
	r1.AppendSequence(r2, 2)

	if !testRecsEqual(oldRec1, oldRec2) {
		t.Fatal("error result")
	}
}

func TestAppend(t *testing.T) {
	oldRec1 := genRecToAppendInit(defaultSchemas)
	oldRec2 := genRecToAppendInit(defaultSchemas)
	newRec := genRecToAppend(defaultSchemas, false)

	sort.Sort(newRec)
	sort.Sort(oldRec1)
	sort.Sort(oldRec2)
	oldRec2.Merge(newRec)

	r1 := record.NewAppendRecord(oldRec1)
	r2 := record.NewAppendRecord(newRec)

	r1.Append(r2, 0, 1)
	r1.Append(r2, 1, 3)
	r1.Append(r2, 4, 2)
	r1.Append(r2, 6, 2)

	if !testRecsEqual(oldRec1, oldRec2) {
		t.Fatal("error result")
	}
}

func TestAppendAll(t *testing.T) {
	oldRec1 := genRecToAppendInit(defaultSchemas)
	oldRec2 := genRecToAppendInit(defaultSchemas)
	newRec := genRecToAppend(defaultSchemas, false)
	oldRec1.ColVals[0] = record.ColVal{}
	oldRec2.ColVals[0] = record.ColVal{}

	sort.Sort(newRec)
	sort.Sort(oldRec1)
	sort.Sort(oldRec2)
	oldRec2.Merge(newRec)

	r1 := record.NewAppendRecord(oldRec1)
	r2 := record.NewAppendRecord(newRec)
	defer func() {
		record.ReleaseAppendRecord(r2)
	}()

	assert.NoError(t, r1.AppendSequence(r2, 8))

	if !testRecsEqual(oldRec1, oldRec2) {
		t.Fatal("error result")
	}
}

func TestUpdateCols(t *testing.T) {
	rec := genRecToAppendInit(defaultSchemas)
	ar := record.NewAppendRecord(rec)

	rec.ColVals = append(rec.ColVals, rec.ColVals[0])
	ar.UpdateCols()

	assert.Equal(t, len(rec.ColVals), len(ar.GetCols()))
}

func TestAppendNotNil(t *testing.T) {
	rec1 := genRecToAppendInit(defaultSchemas)
	rec2 := genRecToAppendInit(defaultSchemas)

	rec1.ColVals[0] = record.ColVal{}
	rec1.ColVals[0].AppendBooleanNull()
	stringIdx := 0
	for i, s := range rec1.Schema {
		if s.Type == influx.Field_Type_String {
			stringIdx = i
			rec1.ColVals[i] = record.ColVal{}
			rec1.ColVals[i].AppendStringNull()
			break
		}
	}

	r1 := record.NewAppendRecord(rec1)
	r2 := record.NewAppendRecord(rec2)

	r1.AppendNotNil(r2)
	assert.Equal(t, rec1.ColVals[0].Len, 1)
	assert.Equal(t, rec1.ColVals[stringIdx].Len, 1)
}

func TestSplitRecord(t *testing.T) {
	rec := record.NewRecordBuilder(defaultSchemas)
	ints := [20]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	floats := [20]float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	bools := [20]bool{}
	strs := [20]string{"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10",
		"a11", "a12", "a13", "a14", "a15", "a16", "a17", "a18", "a19"}
	for i, ref := range defaultSchemas {
		col := rec.Column(i)
		switch ref.Type {
		case influx.Field_Type_Int:
			col.AppendIntegers(ints[:]...)
		case influx.Field_Type_Float:
			col.AppendFloats(floats[:]...)
		case influx.Field_Type_Boolean:
			col.AppendBooleans(bools[:]...)
		case influx.Field_Type_String:
			col.AppendStrings(strs[:]...)
		}
	}

	recs := rec.Split(nil, 5)
	if len(recs) != 4 {
		t.Fatalf("split record fail, exp:4, get:%v", len(recs))
	}

	for i := range recs {
		rec := &recs[i]
		t.Logf("rec(%v):%v", i, rec.String())
	}
}
