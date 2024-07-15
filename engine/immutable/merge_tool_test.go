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

package immutable_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/readcache"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type generateParams struct {
	sid      uint64
	rowCount int
	incr     int
	order    bool
}

func recoverConfig(segLimit int) func() {
	cacheIns := readcache.GetReadMetaCacheIns()
	cacheIns.Purge()

	origSegLimit := immutable.GetMaxRowsPerSegment4TsStore()
	if segLimit > 0 {
		immutable.SetMaxRowsPerSegment4TsStore(segLimit)
	}

	conf := config.GetStoreConfig()
	conf.Merge.MaxUnorderedFileNumber = 100

	return func() {
		conf.Merge.MergeSelfOnly = false
		conf.UnorderedOnly = false
		conf.Merge.MaxUnorderedFileNumber = 8
		immutable.SetMaxRowsPerSegment4TsStore(origSegLimit)
	}
}

func beforeTest(t *testing.T, segLimit int) func() {
	saveDir = t.TempDir()
	return recoverConfig(segLimit)
}

func beforeBenchmark(b *testing.B, segLimit int) func() {
	saveDir = b.TempDir()
	return recoverConfig(segLimit)
}

// merge three unordered files into one ordered file
// |    -------------order file-------------
// |	    ------unordered-----
// |           ------unordered-------
// |  ------unordered------
func TestMergeTool_Merge_mod1(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	for _, n := range []int{6, 4, 8} {
		rg.setBegin(begin + 1).incrBegin(-n * 10)
		mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
		require.NoError(t, mh.saveToUnordered())
	}

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

// merge remain
// order file 1: seriesID = 100, order file 2: seriesID = 200
// unordered file: seriesID = 100
// | --------order file 1---------     --------order file 2---------
// |	     -----------------unordered---------------------
func TestMergeTool_Merge_mod2(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i, sid := range []uint64{100, 200} {
		rg.incrBegin(15 * i)
		mh.addRecord(sid, rg.generate(getDefaultSchemas(), 10))
		require.NoError(t, mh.saveToOrder())
	}

	rg.setBegin(begin).incrBegin(5)
	mh.addRecord(100, rg.generate(getDefaultSchemas(), 15))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

// merge records in different schemas
// |     --------order---------
// | ------unordered----
func TestMergeTool_Merge_mod3(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	// order
	var schemaOrder = getDefaultSchemas()
	schemaOrder = append(schemaOrder, record.Field{Type: influx.Field_Type_Int, Name: "int_order"})
	schemaOrder = append(schemaOrder, record.Field{Type: influx.Field_Type_String, Name: "string_order"})

	mh.addRecord(100, rg.generate(schemaOrder, 16))
	require.NoError(t, mh.saveToOrder())

	// unordered
	var schemaUnordered = getDefaultSchemas()
	schemaUnordered = append(schemaUnordered, record.Field{Type: influx.Field_Type_Float, Name: "float_unordered"})
	schemaUnordered = append(schemaUnordered, record.Field{Type: influx.Field_Type_String, Name: "string_unordered"})

	rg.setBegin(begin).incrBegin(-5)
	mh.addRecord(100, rg.generate(schemaUnordered, 16))
	require.NoError(t, mh.saveToUnordered())

	var schemaUnordered2 = getDefaultSchemas()
	schemaUnordered2 = append(schemaUnordered2, record.Field{Type: influx.Field_Type_Float, Name: "float2_unordered"})
	schemaUnordered2 = append(schemaUnordered2, record.Field{Type: influx.Field_Type_String, Name: "string2_unordered"})

	rg.setBegin(begin - 1).incrBegin(-5)
	mh.addRecord(100, rg.generate(schemaUnordered, 16))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

// merge multiple segments
// |     --------order---------|----------order---------|---------order----------
// | ------unordered----                                     ----unordered----
func TestMergeTool_Merge_mod4(t *testing.T) {
	var begin int64 = 1e12
	var segLimit = 16

	defer beforeTest(t, segLimit)()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	rg.setBegin(begin)
	for _, item := range []generateParams{
		{100, segLimit * 3, 0, true},
		{100, segLimit * 1, -5, false},
		{100, segLimit - 5, segLimit*2 + 7, false},
	} {
		mh.addRecord(item.sid, rg.incrBegin(item.incr).generate(schemas, item.rowCount))
		require.NoError(t, mh.save(item.order))
	}

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

// One file contains data for multiple time series
func TestMergeTool_Merge_mod5(t *testing.T) {
	var begin int64 = 1e12
	var segLimit = 256

	defer beforeTest(t, segLimit)()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(80, rg.generate(schemas, segLimit*3))
	mh.addRecord(100, rg.generate(schemas, segLimit*3))
	mh.addRecord(200, rg.incrBegin(-5).generate(schemas, segLimit*3))
	mh.addRecord(300, rg.incrBegin(10).generate(schemas, segLimit*3))
	mh.addRecord(400, rg.incrBegin(-5).generate(schemas, segLimit))
	mh.addRecord(500, rg.incrBegin(20).generate(schemas, segLimit/2))
	require.NoError(t, mh.saveToOrder())

	rg.setBegin(begin + 1)
	for _, item := range []generateParams{
		{100, segLimit, -segLimit / 4, false},
		{300, segLimit, -segLimit / 4, false},
		{400, segLimit / 2, segLimit / 2, false},
	} {
		mh.addRecord(item.sid, rg.incrBegin(item.incr).generate(schemas, item.rowCount))
		require.NoError(t, mh.save(item.order))
	}

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

// large number of series and different schemas
func TestMergeTool_Merge_mod6(t *testing.T) {
	var begin int64 = 1e12
	var segLimit = 16
	defer beforeTest(t, segLimit)()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	seriesCount := 1000

	for i := 0; i < seriesCount; i++ {
		mh.addRecord(100+uint64(i), rg.generate(schemas, segLimit))
	}
	require.NoError(t, mh.saveToOrder())

	diffSchemas := []record.Field{
		{Type: influx.Field_Type_Float, Name: "float2_unordered"},
		{Type: influx.Field_Type_Float, Name: "float3_unordered"},
		{Type: influx.Field_Type_Int, Name: "int2_unordered"},
		{Type: influx.Field_Type_Int, Name: "int3_unordered"},
		{Type: influx.Field_Type_Boolean, Name: "bool2_unordered"},
		{Type: influx.Field_Type_Boolean, Name: "bool3_unordered"},
		{Type: influx.Field_Type_String, Name: "string2_unordered"},
		{Type: influx.Field_Type_String, Name: "string3_unordered"},
		{Type: influx.Field_Type_String, Name: "string4_unordered"},
	}

	rg.setBegin(begin + 1).incrBegin(-10)
	for i := 0; i < seriesCount; i++ {
		schemas = getDefaultSchemas()
		schemas = append(schemas, diffSchemas[i%len(diffSchemas)])
		schemas = append(schemas, diffSchemas[(i+3)%len(diffSchemas)])
		mh.addRecord(100+uint64(i), rg.generate(schemas, 10))
	}

	require.NoError(t, mh.saveToUnordered())
	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

// 1. The minimum time of out-of-order data is greater than the maximum time of ordered data
// 2. The minimum seriesID of out-of-order data is greater than the maximum seriesID of ordered data
// order:     | -----seriesID=100-----
// unordered: |                           ----seriesID=100----
// unordered: |                           ----seriesID=101----
func TestMergeTool_Merge_mod7(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)
	schema := getDefaultSchemas()

	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())
	mh.addRecord(110, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	begin++
	mh.addRecord(98, rg.setBegin(begin).incrBegin(15).generate(schema, 10))
	mh.addRecord(99, rg.setBegin(begin).incrBegin(-5).generate(schema, 10))
	mh.addRecord(100, rg.setBegin(begin).incrBegin(15).generate(schema, 10))
	mh.addRecord(101, rg.setBegin(begin).incrBegin(15).generate(schema, 10))
	mh.addRecord(111, rg.setBegin(begin).incrBegin(15).generate(schema, 10))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_Merge_mod8(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int_1"},
	}

	mh.addRecord(100, rg.generate(schema, 10*1000))
	mh.addRecord(101, rg.setBegin(begin+1).generate(schema, 10*1000))
	require.NoError(t, mh.saveToOrder())

	schema = append(schema, record.Field{Type: influx.Field_Type_Int, Name: "int_2"})

	mh.addRecord(100, rg.setBegin(begin).incrBegin(10).generate(schema, 2000))
	mh.addRecord(101, rg.setBegin(begin+1).incrBegin(10).generate(schema, 2000))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_Merge_mod9(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 16)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int_1"},
	}

	mh.addRecord(100, rg.generate(schema, 16))
	mh.addRecord(100, rg.setBegin(begin).incrBegin(20).generate(schema, 32))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.setBegin(begin+1).incrBegin(-5).generate(schema, 16))
	require.NoError(t, mh.saveToUnordered())

	schema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int_2"},
	}
	mh.addRecord(100, rg.setBegin(begin+1).incrBegin(17).generate(schema, 16))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_Merge_mod10(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 1000)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int_1"},
	}
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	schema[0].Name = "int_2"
	mh.addRecord(100, rg.setBegin(begin+1).incrBegin(9).generate(schema, 5))
	require.NoError(t, mh.saveToOrder())

	schema[0].Name = "int_3"
	mh.addRecord(100, rg.setBegin(begin+2).incrBegin(5).generate(schema, 9))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_Merge_mod11(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 1000)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	schema := getDefaultSchemas()
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.setBegin(begin).incrBegin(11).generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.setBegin(begin).incrBegin(9).generate(schema, 5))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_Merge_mod12(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 1000)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	schema := getDefaultSchemas()
	recs := []*record.Record{
		rg.generate(schema, 10),
		rg.setBegin(begin).incrBegin(9).generate(schema, 10),
	}

	var err error
	func() {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Errorf("%v", e)
			}
		}()

		sh := record.NewSortHelper()

		for i := range recs {
			recs[i] = sh.Sort(recs[i])
			record.CheckRecord(recs[i])
		}
	}()
	require.NoError(t, err)
}

func TestMergeTool_Merge_mod13(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 1000)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int_1"},
		record.Field{Type: influx.Field_Type_Int, Name: "int_2"},
		record.Field{Type: influx.Field_Type_Int, Name: "int_3"},
	}
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	schema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int_1"},
		record.Field{Type: influx.Field_Type_Int, Name: "int_2"},
	}
	mh.addRecord(100, rg.setBegin(begin).incrBegin(-10).generate(schema, 5))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_Merge_mod14(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 1000)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	schema := getDefaultSchemas()
	mh.addRecord(100, rg.generate(schema, 10))
	mh.addRecord(101, rg.generate(schema, 10))
	mh.addRecord(102, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(101, rg.setBegin(begin).incrBegin(15).generate(schema, 10))
	mh.addRecord(103, rg.setBegin(begin).incrBegin(15).generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.setBegin(begin-1).incrBegin(-10).generate(schema, 25))
	mh.addRecord(101, rg.setBegin(begin-1).incrBegin(-10).generate(schema, 5))
	mh.addRecord(102, rg.setBegin(begin-1).incrBegin(-10).generate(schema, 5))
	mh.addRecord(103, rg.setBegin(begin-1).incrBegin(-10).generate(schema, 5))
	mh.addRecord(104, rg.setBegin(begin-1).incrBegin(-10).generate(schema, 5))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	contains, _ := mh.store.Order["mst"].Files()[0].Contains(103)
	require.False(t, contains)
	contains, _ = mh.store.Order["mst"].Files()[1].Contains(104)
	require.True(t, contains)

	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
	fmt.Println(mh.store.Order["mst"].Files()[0].Contains(103))
}

func TestMergeTool_Merge_mod15(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	schema := getDefaultSchemas()
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(99, rg.setBegin(begin-1).incrBegin(-10).generate(schema, 100))
	mh.addRecord(100, rg.setBegin(begin-1).incrBegin(-10).generate(schema, 5))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_Merge_mod16(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 8; i++ {
		rg.setBegin(begin).incrBegin(i)
		mh.addRecord(100, rg.generate(schemas, 1))
		mh.addRecord(101, rg.generate(schemas, 1))
		require.NoError(t, mh.saveToOrder())
	}

	rg.setBegin(begin - 1)
	mh.addRecord(100, rg.generate(schemas, 10))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	for _, rec := range mh.readMergedRecord() {
		record.CheckTimes(rec.Times())
	}
}

func TestMergeTool_Merge_mod17(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	immutable.SetMergeFlag4TsStore(1)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	rg.setBegin(begin)
	mh.addRecord(100, rg.generate(schemas, 1))
	mh.addRecord(101, rg.generate(schemas, 1))
	require.NoError(t, mh.saveToOrder())

	rg.setBegin(begin).incrBegin(1)
	mh.addRecord(100, rg.generate(schemas, 1))
	require.NoError(t, mh.saveToOrder())

	rg.setBegin(begin).incrBegin(-5)
	mh.addRecord(100, rg.generate(schemas, 2))
	mh.addRecord(101, rg.generate(schemas, 2))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	for _, rec := range mh.readMergedRecord() {
		record.CheckTimes(rec.Times())
	}
}

func TestMergeTool_Merge_mod18(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	immutable.SetMergeFlag4TsStore(1)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	rg.setBegin(begin)
	mh.addRecord(100, rg.generate(schemas, 1))
	mh.addRecord(101, rg.generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	rg.setBegin(begin).incrBegin(2)
	mh.addRecord(100, rg.generate(schemas, 1))
	require.NoError(t, mh.saveToOrder())

	rg.setBegin(begin).incrBegin(5)
	mh.addRecord(100, rg.generate(schemas, 1))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()
	for _, rec := range mh.readMergedRecord() {
		record.CheckTimes(rec.Times())
	}
}

func TestMergeTool_recentFile(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	schema := getDefaultSchemas()
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(101, rg.setBegin(begin).incrBegin(-10).generate(schema, 100))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.setBegin(begin).incrBegin(-20).generate(schema, 10))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))

	orderFiles, ok := mh.store.GetTSSPFiles("mst", true)
	require.True(t, ok)
	require.Equal(t, 2, orderFiles.Len())
	require.Equal(t, uint16(1), orderFiles.Files()[0].FileNameMerge())
}

func TestMergeTool_SkipMerge(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.generate(schemas, 10))
	require.NoError(t, mh.saveToUnordered())
	require.NoError(t, mh.mergeAndCompact(false))

	mh.addRecord(100, rg.generate(schemas, 10))
	require.NoError(t, mh.saveToUnordered())
	require.NoError(t, mh.mergeAndCompact(false))

	files, ok := mh.store.OutOfOrder["mst"]
	require.True(t, ok)
	require.Equal(t, 1, len(files.Files()))
}

func TestMergeTool_MergeUnorderedSelf(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	conf := config.GetStoreConfig()
	conf.Merge.MergeSelfOnly = true

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(uint64(100), rg.generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	for i := 0; i < 66; i++ {
		schemas = getDefaultSchemas()
		mh.addRecord(uint64(100+i%2), rg.incrBegin(1).generate(schemas, 10))
		require.NoError(t, mh.saveToUnordered())
	}

	var assertFileNumber = func(exp int) {
		files, ok := mh.store.OutOfOrder["mst"]
		require.True(t, ok)
		require.Equal(t, exp, files.Len())
	}

	require.NoError(t, mh.mergeAndCompact(false))
	assertFileNumber(10)

	conf.Merge.MergeSelfOnly = false
	conf.UnorderedOnly = true
	require.NoError(t, mh.mergeAndCompact(false))
	assertFileNumber(3)

	require.NoError(t, mh.mergeAndCompact(false))
	assertFileNumber(3)
}

func TestMergeTool_MergeAndCompact(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 256)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 10; i++ {
		rg.setBegin(begin).incrBegin(rand.Int() % 30)
		mh.addRecord(uint64(100+i), rg.generate(schemas, 1000))
		mh.addRecord(uint64(100+i+10), rg.generate(schemas, 1000))
		require.NoError(t, mh.saveToOrder())
	}

	for i := 0; i < 20; i++ {
		rg.setBegin(begin).incrBegin(-rand.Int() % 10)
		mh.addRecord(uint64(100+i), rg.generate(schemas, 100))
		require.NoError(t, mh.saveToUnordered())
	}

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeTool_PreAgg(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 256)()
	schemas := getDefaultSchemas()
	schemas = append(schemas, record.Field{Type: influx.Field_Type_String, Name: "string2"})
	schemas = append(schemas, record.Field{Type: influx.Field_Type_Boolean, Name: "bool2"})
	schemas = append(schemas, record.Field{Type: influx.Field_Type_Float, Name: "float2"})
	schemas = append(schemas, record.Field{Type: influx.Field_Type_Int, Name: "int2"})

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	for i := 0; i < 10; i++ {
		rg.setBegin(begin).incrBegin(rand.Int() % 30)
		mh.addRecord(uint64(100+i), rg.generate(schemas, 1000))
		mh.addRecord(uint64(100+i+10), rg.generate(schemas, 1000))
		require.NoError(t, mh.saveToOrder())
	}

	for i := 0; i < 20; i++ {
		rg.setBegin(begin + 1).incrBegin(-rand.Int() % 10)
		mh.addRecord(uint64(100+i), rg.generate(schemas, 100))
		require.NoError(t, mh.saveToUnordered())
	}

	assert.NoError(t, mh.mergeAndCompact(true))

	for _, f := range mh.store.Order["mst"].Files() {
		fi := immutable.NewFileIterator(f, immutable.CLog)
		itr := immutable.NewColumnIterator(fi)

		ctx := &immutable.ReadContext{}

		for {
			if !itr.NextChunkMeta() {
				require.NoError(t, itr.Error())
				break
			}

			sort.Sort(schemas)

			for i, cm := range fi.GetCurtChunkMeta().GetColMeta() {
				if i >= len(schemas) {
					break
				}
				s := &schemas[i]

				count, err := cm.RowCount(s, ctx)
				require.NoError(t, err)
				require.Equal(t, int64(1100), count)
			}
		}
	}
}

func TestMergeTool_CleanTmpFiles(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 16)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	rg := newRecordGenerator(begin, defaultInterval, false)

	fp := "github.com/openGemini/openGemini/engine/immutable/column-writer-error"
	require.NoError(t, failpoint.Enable(fp, `return("error")`))
	defer failpoint.Disable(fp)

	rg.setBegin(begin)
	mh.addRecord(100, rg.generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.setBegin(begin-1).generate(schemas, 10))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))

	mh.store.Wait()
	mh.store.Close()

	tmpFile := filepath.Join(saveDir, "mst", "00000001-0000-00010000.tssp.init")
	_, err := os.Stat(tmpFile)
	require.True(t, os.IsNotExist(err))
}

func TestMergeTool_WriteCurrentMetaError(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 16)()
	schemas := getDefaultSchemas()

	fp := "github.com/openGemini/openGemini/engine/immutable/write-current-meta-error"
	require.NoError(t, failpoint.Enable(fp, `return("error")`))
	defer failpoint.Disable(fp)

	func() {
		mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
		defer mh.store.Close()

		rg := newRecordGenerator(begin, defaultInterval, false)
		rg.setBegin(begin)
		mh.addRecord(100, rg.generate(schemas, 100))
		require.NoError(t, mh.saveToOrder())

		mh.addRecord(100, rg.generate(schemas, 100))
		require.NoError(t, mh.saveToUnordered())

		require.NoError(t, mh.mergeAndCompact(true))
	}()

	func() {
		mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
		mh.seq += 10
		defer mh.store.Close()

		rg := newRecordGenerator(begin, defaultInterval, false)
		rg.setBegin(begin)
		mh.addRecord(100, rg.generate(schemas, 100))
		require.NoError(t, mh.saveToOrder())

		mh.addRecord(98, rg.generate(schemas, 100))
		mh.addRecord(99, rg.generate(schemas, 100))
		require.NoError(t, mh.saveToUnordered())

		require.NoError(t, mh.mergeAndCompact(true))
	}()
}

func TestCompactionDiffSchemas(t *testing.T) {
	t.Skip()
	var begin int64 = 1e12
	defer beforeTest(t, 16)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 4; i++ {
		schema := record.Schemas{
			record.Field{Type: influx.Field_Type_Int, Name: fmt.Sprintf("int_%d", i+1)},
		}
		mh.addRecord(100, rg.incrBegin(200).generate(schema, 100+i*10))
		require.NoError(t, mh.saveToOrder())
	}

	immutable.SetMergeFlag4TsStore(util.StreamingCompact)
	require.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()

	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeStopFiles(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 256)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	rg.setBegin(begin)
	mh.addRecord(100, rg.generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.generate(schemas, 100))
	require.NoError(t, mh.saveToUnordered())

	mh.store.DisableCompAndMerge()
	require.NoError(t, mh.store.MergeOutOfOrder(1, false, true))
	mh.store.Wait()
	mh.store.EnableCompAndMerge()

	orderFiles := immutable.NewTSSPFiles()
	orderFiles.Append(mh.store.Order["mst"].Files()[0])

	files := mh.store.OutOfOrder["mst"]
	files.StopFiles()
	require.Empty(t, mh.store.File("mst", files.Files()[0].Path(), false))

	assert.NoError(t, mh.mergeAndCompact(true))
	delete(mh.store.OutOfOrder, "mst")
	require.NoError(t, mh.store.ReplaceFiles("mst", files.Files(), files.Files(), true))
	mh.store.Order["mst"] = orderFiles
}

func TestMergeTool_OneRowMode(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 1000)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	rg.setBegin(begin).incrBegin(0)
	mh.addRecord(100, rg.generate(schemas, 1001))
	rec := rg.generate(schemas, 1)
	col := rec.Column(3)
	col.Init()
	col.AppendStrings("")
	mh.addRecord(101, rec)
	require.NoError(t, mh.saveToOrder())

	rg.setBegin(begin - 1).incrBegin(5)
	mh.addRecord(100, rg.generate(schemas, 1000))
	require.NoError(t, mh.saveToUnordered())

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

// order:
// sid 100:                -----------------
// sid 101:                       --------------
// sid 102:           --------------
// unordered:
// sid 100:  ------------
// sid 101:   -------------
// sid 102:  --------------
func TestMergeTool_writeHistory(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	orderIncr := []int{20, 20, 2, 3, 30}
	unorderIncr := []int{-9, -1, -6, -2, -8}
	for i := 0; i < 5; i++ {
		rg.setBegin(begin).incrBegin(orderIncr[i])
		mh.addRecord(uint64(100+i), rg.generate(schemas, 11))
		require.NoError(t, mh.saveToOrder())
	}

	for i := 0; i < 5; i++ {
		rg.setBegin(begin - 1).incrBegin(unorderIncr[i])
		mh.addRecord(uint64(100+i), rg.generate(schemas, 10))
		require.NoError(t, mh.saveToUnordered())
	}

	assert.NoError(t, mh.mergeAndCompact(true))
	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestCompressChunkMeta(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()
	immutable.InitWriterPool(8)

	defer func() {
		immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressNone)
		immutable.InitWriterPool(8)
	}()

	var run = func(seriesNum int) {
		mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
		defer mh.store.Close()
		rg := newRecordGenerator(begin, defaultInterval, true)

		for i := 0; i < seriesNum; i++ {
			rg.incrBegin(15)
			mh.addRecord(uint64(100+i), rg.generate(getDefaultSchemas(), 10))
		}

		require.NoError(t, mh.saveToOrder())
		total := 0
		itrTSSPFile(mh.store.Order["mst"].Files()[0], func(sid uint64, rec *record.Record) {
			record.CheckRecord(rec)
			total++
		})
		require.Equal(t, seriesNum, total)
	}

	// test case for snappy
	for _, v := range []int{immutable.ChunkMetaCompressSnappy, immutable.ChunkMetaCompressNone, 200} {
		immutable.SetChunkMetaCompressMode(v)
		run(20)
	}

	for _, v := range []int{immutable.ChunkMetaCompressSnappy, immutable.ChunkMetaCompressNone, 200} {
		immutable.SetChunkMetaCompressMode(v)
		run(2000)
	}

	// test case for lz4
	for _, v := range []int{immutable.ChunkMetaCompressLZ4, immutable.ChunkMetaCompressNone, 200} {
		immutable.SetChunkMetaCompressMode(v)
		run(20)
	}

	for _, v := range []int{immutable.ChunkMetaCompressLZ4, immutable.ChunkMetaCompressNone, 200} {
		immutable.SetChunkMetaCompressMode(v)
		run(2000)
	}
}

func TestChunkMeta(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 1000)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	schema := getDefaultSchemas()
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.Order["mst"]
	require.True(t, ok)
	require.True(t, len(files.Files()) == 1)

	file := files.Files()[0]
	tr := file.FileStat()
	require.True(t, tr.DataSize() > 0)
	require.True(t, tr.IndexSize() > 0)
	require.True(t, tr.MetaIndexSize() > 0)

	mi, err := file.MetaIndexAt(0)
	require.NoError(t, err)

	metas, err := file.ReadChunkMetaData(0, mi, nil, fileops.IO_PRIORITY_HIGH)
	require.NoError(t, err)

	chunkMeta := &metas[0]
	require.True(t, len(chunkMeta.GetColMeta()) > 0)
	cm := chunkMeta.GetColMeta()[0]
	require.NotEmpty(t, cm.GetPreAgg())
	ofs, size := cm.GetSegment(0)
	require.True(t, ofs > 0)
	require.True(t, size > 0)
	require.Equal(t, uint8(5), cm.Type())
}

func TestCompactErrorTime(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 8; i++ {
		mh.addRecord(uint64(100+i), rg.generate(schemas, 10))
		require.NoError(t, mh.saveToOrder())
		mh.seq++
	}

	mh.seq = 1
	mh.addRecord(100, rg.incrBegin(-1).generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	immutable.SetMergeFlag4TsStore(util.StreamingCompact)
	defer func() {
		immutable.SetMergeFlag4TsStore(util.AutoCompact)
	}()
	require.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()
	require.Equal(t, 9, mh.store.Order["mst"].Len())
}

func BenchmarkChunkMeta_compress(b *testing.B) {
	var begin int64 = 1e13
	defer beforeBenchmark(b, 0)()
	immutable.InitWriterPool(8)

	defer func() {
		immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressNone)
		immutable.InitWriterPool(8)
	}()

	var run = func(seriesNum int) {
		mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
		defer mh.store.Close()
		rg := newRecordGenerator(begin, defaultInterval, true)

		for i := 0; i < seriesNum; i++ {
			rg.incrBegin(15)
			mh.addRecord(uint64(100+i), rg.generate(getDefaultSchemas(), 10))
		}

		require.NoError(b, mh.saveToOrder())
		total := 0
		itrTSSPFile(mh.store.Order["mst"].Files()[0], func(sid uint64, rec *record.Record) {
			record.CheckRecord(rec)
			total++
		})
		require.Equal(b, seriesNum, total)
	}

	b.ResetTimer()
	b.Run("snappy", func(b *testing.B) {
		immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressSnappy)
		run(20)

		immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressSnappy)
		run(2000)
	})

	b.ResetTimer()
	b.Run("lz4", func(b *testing.B) {
		immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressLZ4)
		run(20)

		immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressLZ4)
		run(2000)
	})
}

func TestMergePerformers_close(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	schemas := getDefaultSchemas()
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 8; i++ {
		mh.addRecord(uint64(100+i), rg.generate(schemas, 10))
		require.NoError(t, mh.saveToOrder())
	}

	performers := immutable.NewMergePerformers(nil)
	var tempFiles []*immutable.StreamWriteFile

	defer func() {
		for _, f := range tempFiles {
			f.Close(true)
		}
	}()
	for _, f := range mh.store.Order["mst"].Files() {
		sw := mh.store.NewStreamWriteFile("mst")
		require.NoError(t, sw.InitMergedFile(f))
		sw.SetValidate(true)
		tempFiles = append(tempFiles, sw)

		p := immutable.NewMergePerformer(nil, nil)

		itr := immutable.NewColumnIterator(immutable.NewFileIterator(f, nil))
		p.Reset(sw, itr)
		performers.Push(p)
	}

	performers.Done()
	performers.Close()
}
