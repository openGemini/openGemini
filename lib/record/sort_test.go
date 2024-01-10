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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func SortAndDedup(table *mutable.MemTable, msName string) {
	msInfo, err := table.GetMsInfo(msName)
	if err != nil {
		return
	}
	mutable.JoinWriteRec(table, msName)
	msInfo.GetWriteChunk().SortRecord(0)
}

func TestSortRecordByOrderTags(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "ttbool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 1}, []string{"ha", "helloNew", "worldNew", "hb"},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 20, 19})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{0, 0, 1000, 1100},
		[]int{1, 1, 1, 1}, []float64{1002.4, 1002.4, 1001.3, 0},
		[]int{1, 1, 1, 1}, []string{"helloNew", "worldNew", "ha", "hb"},
		[]int{1, 1, 1, 1}, []bool{true, false, true, false},
		[]int64{21, 20, 22, 19})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order1_int", "order2_float", "order3_string"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags1(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "ttbool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []int64{700, 600, 500, 400, 300, 200, 100, 30, 20, 10, 9, 8},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3, 0, 1003.5, 0, 1002.4, 1001.3},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []string{"test", "", "world", "", "", "hello", "", "", "testNew1", "worldNew", "helloNew", "a"},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []bool{false, false, true, false, true, false, false, false, true, false, true, true},
		[]int64{1, 2, 3, 4, 5, 6, 7, 18, 19, 20, 21, 22})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []int64{8, 9, 10, 20, 30, 100, 200, 300, 400, 500, 600, 700},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []float64{1001.3, 1002.4, 0, 1003.5, 0, 2.3, 0, 3.3, 0, 4.3, 5.3, 0},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []string{"a", "helloNew", "worldNew", "testNew1", "", "", "hello", "", "", "world", "", "test"},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []bool{true, true, false, true, false, false, false, true, false, true, false, false},
		[]int64{22, 21, 20, 19, 18, 7, 6, 5, 4, 3, 2, 1})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order1_int", "order2_float", "order3_string"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "ttbool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 1}, []string{"ha", "helloNew", "worldNew", "hb"},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 20, 19})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{0, 0, 1000, 1100},
		[]int{1, 1, 1, 1}, []float64{1002.4, 1002.4, 1001.3, 0},
		[]int{1, 1, 1, 1}, []string{"worldNew", "helloNew", "ha", "hb"},
		[]int{1, 1, 1, 1}, []bool{false, true, true, false},
		[]int64{20, 21, 22, 19})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order1_int", "ttbool"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags3(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 0, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 0}, []float64{1001.3, 1002.4, 1003.4, 0},
		[]int{1, 1, 1, 0}, []string{"ha", "helloNew", "helloNew", ""},
		[]int{1, 1, 0, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 20, 19})
	expRec := genRowRec(schema,
		[]int{1, 1, 0, 0}, []int64{1100, 1000, 0, 0},
		[]int{0, 1, 1, 1}, []float64{0, 1001.3, 1002.4, 1003.4},
		[]int{0, 1, 1, 1}, []string{"", "ha", "helloNew", "helloNew"},
		[]int{1, 1, 1, 0}, []bool{false, true, true, false},
		[]int64{19, 22, 21, 20})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order3_string", "order2_float"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags4(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 0, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 0}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 0, 0, 0}, []string{"ha", "", "", ""},
		[]int{1, 1, 0, 1}, []bool{true, false, true, false},
		[]int64{22, 21, 21, 19})
	expRec := genRowRec(schema,
		[]int{0, 0, 1, 1}, []int64{0, 0, 1000, 1100},
		[]int{1, 1, 1, 0}, []float64{1002.4, 1002.4, 1001.3, 0},
		[]int{0, 0, 1, 0}, []string{"", "", "ha", ""},
		[]int{1, 0, 1, 1}, []bool{false, false, true, false},
		[]int64{21, 21, 22, 19})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order1_int", "order2_float", "order3_string", "order4_bool"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags5(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 0, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 0}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 0}, []string{"ha", "helloNew", "worldNew", ""},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 21, 19})
	expRec := genRowRec(schema,
		[]int{0, 0, 1, 1}, []int64{0, 0, 1000, 1100},
		[]int{1, 1, 1, 0}, []float64{1002.4, 1002.4, 1001.3, 0},
		[]int{1, 1, 1, 0}, []string{"worldNew", "helloNew", "ha", ""},
		[]int{1, 1, 1, 1}, []bool{false, true, true, false},
		[]int64{21, 21, 22, 19})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order1_int", "order4_bool"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags6(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 0, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 0}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 0}, []string{"ha", "worldNew", "helloNew", "a"},
		[]int{1, 1, 1, 1}, []bool{true, false, false, false},
		[]int64{22, 21, 21, 19})
	expRec := genRowRec(schema,
		[]int{1, 1, 0, 0}, []int64{1100, 1000, 0, 0},
		[]int{0, 1, 1, 1}, []float64{0, 1001.3, 1002.4, 1002.4},
		[]int{0, 1, 1, 1}, []string{"a", "ha", "helloNew", "worldNew"},
		[]int{1, 1, 1, 1}, []bool{false, true, false, false},
		[]int64{19, 22, 21, 21})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order3_string", "order4_bool"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags7(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 0, 0, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 0}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 0}, []string{"ha", "worldNew", "worldNew", ""},
		[]int{1, 1, 1, 1}, []bool{true, false, false, false},
		[]int64{22, 21, 21, 19})
	expRec := genRowRec(schema,
		[]int{1, 0, 0, 1}, []int64{1100, 0, 0, 1000},
		[]int{0, 1, 1, 1}, []float64{0, 1002.4, 1002.4, 1001.3},
		[]int{0, 1, 1, 1}, []string{"", "worldNew", "worldNew", "ha"},
		[]int{1, 1, 1, 1}, []bool{false, false, false, true},
		[]int64{19, 21, 21, 22})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}

	sk := []string{"time"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)

	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordByOrderTags8(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 300, 0, 1100},
		[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 1}, []string{"d", "c", "b", "a"},
		[]int{1, 1, 1, 1}, []bool{true, false, true, false},
		[]int64{22, 21, 20, 19})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1100, 0, 300, 1000},
		[]int{1, 1, 1, 1}, []float64{0, 1002.4, 1002.4, 1001.3},
		[]int{1, 1, 1, 1}, []string{"a", "b", "c", "d"},
		[]int{1, 1, 1, 1}, []bool{false, true, false, true},
		[]int64{19, 20, 21, 22})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order3_string"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)
	SortAndDedup(tbl, "cpu")
	if !testRecsEqual(msInfo.GetWriteChunk().WriteRec.GetRecord(), expRec) {
		t.Fatal("error result")
	}
}

func TestBoolSliceSingleValueCompare(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1}, []int64{1000},
		[]int{1}, []float64{1001.3},
		[]int{1}, []string{"ha"},
		[]int{1}, []bool{true},
		[]int64{22})
	cmpRec := genRowRec(schema,
		[]int{1}, []int64{1100},
		[]int{0}, []float64{0},
		[]int{0}, []string{"worldNew"},
		[]int{1}, []bool{false},
		[]int64{19})
	expResult := []int{1, -1, -1, -1}
	sort.Sort(rec)
	sort.Sort(cmpRec)
	times := rec.Times()
	pk := []record.PrimaryKey{{"order1_int", influx.Field_Type_Int}, {"order2_float", influx.Field_Type_Float},
		{"order3_string", influx.Field_Type_String}, {"order4_bool", influx.Field_Type_Boolean}}
	sk := pk
	data := record.SortData{}
	dataCmp := record.SortData{}
	data.Init(times, sk, rec, 0)
	dataCmp.Init(cmpRec.Times(), sk, cmpRec, 0)

	im := data.Data
	jm := dataCmp.Data
	res := make([]int, 0, len(pk))
	for idx := 0; idx < len(im); idx++ {
		v, err := im[idx].CompareSingleValue(jm[idx], 0, 0)
		if err != nil {
			t.Fatal()
		}
		res = append(res, v)
	}
	if !compareResult(res, expResult) {
		t.Fatal()
	}
}

func TestBoolSliceSingleValueCompare2(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1}, []int64{1000},
		[]int{1}, []float64{1001.3},
		[]int{1}, []string{"ha"},
		[]int{1}, []bool{true},
		[]int64{22})
	cmpRec := genRowRec(schema,
		[]int{1}, []int64{1100},
		[]int{1}, []float64{200},
		[]int{1}, []string{"world"},
		[]int{1}, []bool{true},
		[]int64{19})
	expResult := []int{1, -1, 1, 0}
	sort.Sort(rec)
	sort.Sort(cmpRec)
	times := rec.Times()
	pk := []record.PrimaryKey{{"order1_int", influx.Field_Type_Int}, {"order2_float", influx.Field_Type_Float},
		{"order3_string", influx.Field_Type_String}, {"order4_bool", influx.Field_Type_Boolean}}
	sk := pk
	data := record.SortData{}
	dataCmp := record.SortData{}
	data.Init(times, sk, rec, 0)
	dataCmp.Init(cmpRec.Times(), sk, cmpRec, 0)

	im := data.Data
	jm := dataCmp.Data
	res := make([]int, 0, len(pk))
	for idx := 0; idx < len(im); idx++ {
		v, err := im[idx].CompareSingleValue(jm[idx], 0, 0)
		if err != nil {
			t.Fatal()
		}
		res = append(res, v)
	}
	if !compareResult(res, expResult) {
		t.Fatal()
	}
}

func TestBoolSliceSingleValueCompare3(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "order4_bool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1}, []int64{1100},
		[]int{1}, []float64{1001.3},
		[]int{1}, []string{"hb"},
		[]int{0}, []bool{false},
		[]int64{22})
	cmpRec := genRowRec(schema,
		[]int{1}, []int64{1100},
		[]int{1}, []float64{1001.3},
		[]int{1}, []string{"ha"},
		[]int{1}, []bool{false},
		[]int64{19})
	expResult := []int{0, 0, -1, 0}
	sort.Sort(rec)
	sort.Sort(cmpRec)
	times := rec.Times()
	pk := []record.PrimaryKey{{"order1_int", influx.Field_Type_Int}, {"order2_float", influx.Field_Type_Float},
		{"order3_string", influx.Field_Type_String}, {"order4_bool", influx.Field_Type_Boolean}}
	sk := pk
	data := record.SortData{}
	dataCmp := record.SortData{}
	data.Init(times, sk, rec, 0)
	dataCmp.Init(cmpRec.Times(), sk, cmpRec, 0)

	im := data.Data
	jm := dataCmp.Data
	res := make([]int, 0, len(pk))
	for idx := 0; idx < len(im); idx++ {
		v, err := im[idx].CompareSingleValue(jm[idx], 0, 0)
		if err != nil {
			t.Fatal()
		}
		res = append(res, v)
	}
	if !compareResult(res, expResult) {
		t.Fatal()
	}
}

func compareResult(res, expRes []int) bool {
	if len(res) != len(expRes) {
		return false
	}
	if len(res) == 0 {
		return false
	}
	for i := 0; i < len(res); i++ {
		if res[i] != expRes[i] {
			return false
		}
	}
	return true
}

func TestSortRecordAndDeduplicate(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "ttbool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 1}, []string{"ha", "helloNew", "helloNew", "hb"},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 20, 19})
	expRec := genRowRec(schema,
		[]int{1, 1, 1}, []int64{0, 1000, 1100},
		[]int{1, 1, 1}, []float64{1002.4, 1001.3, 0},
		[]int{1, 1, 1}, []string{"helloNew", "ha", "hb"},
		[]int{1, 1, 1}, []bool{false, true, false},
		[]int64{20, 22, 19})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order1_int", "order2_float", "order3_string"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)

	hlp := record.NewSortHelper()
	defer hlp.Release()
	rec = hlp.SortForColumnStore(wk.WriteRec.GetRecord(), mutable.GetPrimaryKeys(schema, sk), true, 0)

	if !testRecsEqual(rec, expRec) {
		t.Fatal("error result")
	}
}

func TestSortRecordWithTimeCluster(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "order1_int"},
		record.Field{Type: influx.Field_Type_Float, Name: "order2_float"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "ttbool"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	ts, _ := time.Parse(time.RFC3339, "2023-01-01T01:00:00Z")
	timestamp := int64(ts.UnixNano())
	d, _ := time.ParseDuration("1m")
	duration := int64(d)

	timeClusterDuration, _ := time.ParseDuration("2m")
	times := []int64{
		timestamp,
		timestamp + duration*1,
		timestamp + duration*2,
		timestamp + duration*3,
	}

	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 1}, []string{"ha", "helloNew", "helloNew", "hb"},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{times[3], times[2], times[1], times[0]})
	expRec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{0, 1100, 0, 1000},
		[]int{1, 1, 1, 1}, []float64{1002.4, 0, 1002.4, 1001.3},
		[]int{1, 1, 1, 1}, []string{"helloNew", "hb", "helloNew", "ha"},
		[]int{1, 1, 1, 1}, []bool{false, false, true, true},
		[]int64{times[1], times[0], times[2], times[3]})
	sort.Sort(rec)
	sort.Sort(expRec)

	tbl := mutable.NewMemTable(config.COLUMNSTORE)
	msInfo := &mutable.MsInfo{
		Name:   "cpu",
		Schema: schema,
	}
	sk := []string{"order1_int", "order2_float", "order3_string"}
	msInfo.CreateWriteChunkForColumnStore(sk)
	wk := msInfo.GetWriteChunk()
	wk.WriteRec.SetWriteRec(rec)
	tbl.SetMsInfo("cpu", msInfo)

	hlp := record.NewSortHelper()
	defer hlp.Release()
	rec = hlp.SortForColumnStore(wk.WriteRec.GetRecord(), mutable.GetPrimaryKeys(schema, sk), false, timeClusterDuration)

	// need to remove the first column of rec(clustered time)
	rec.ColVals = rec.ColVals[1:]
	rec.Schema = rec.Schema[1:]

	if !testRecsEqual(rec, expRec) {
		t.Fatal("error result")
	}
}

func BenchmarkSortForColumnStore(b *testing.B) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "order1_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order2_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order3_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order4_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order5_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order6_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order7_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order8_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order9_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order10_string"},
		record.Field{Type: influx.Field_Type_String, Name: "order11_string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	sk := []record.PrimaryKey{{"order1_string", influx.Field_Type_String}, {"order2_string", influx.Field_Type_String},
		{"order3_string", influx.Field_Type_String}, {"order4_string", influx.Field_Type_String}, {"order5_string", influx.Field_Type_String},
		{"order6_string", influx.Field_Type_String}, {"order7_string", influx.Field_Type_String}, {"order8_string", influx.Field_Type_String},
		{"order9_string", influx.Field_Type_String}, {"order10_string", influx.Field_Type_String}, {"order11_string", influx.Field_Type_String}}
	hlp := record.NewSortHelper()
	defer hlp.Release()
	rec := genSortData(5000000, 10, schema)
	hlp.SortForColumnStore(rec, sk, false, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000; j++ {
			hlp.SortForColumnStore(rec, sk, false, 0)
		}
	}
}

func genSortData(rows, targetIdx int, schema record.Schemas) *record.Record {
	tm := time.Now().Truncate(time.Minute).UnixNano()
	genRecFn := func() *record.Record {
		b := record.NewRecordBuilder(schema)
		for idx := 0; idx < len(schema)-1; idx++ {
			builder := b.Column(idx)
			if idx < targetIdx {
				for i := 0; i <= rows; i++ {
					builder.AppendString("test_test_test")
				}
				continue
			}
			if idx == targetIdx {
				tmBuilder := b.Column(len(schema) - 1)
				for i := 0; i < rows; i++ {
					f := fmt.Sprintf("test_test_test_%d", rows-i)
					builder.AppendString(f)
					tmBuilder.AppendInteger(tm)
					tm += time.Millisecond.Milliseconds()
				}
			}
		}

		return b
	}
	data := genRecFn()
	return data
}
