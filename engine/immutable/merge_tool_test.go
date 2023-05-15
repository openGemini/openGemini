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
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/readcache"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
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

func beforeTest(t *testing.T, segLimit int) func() {
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	saveDir = t.TempDir()

	origSegLimit := immutable.MaxRowsPerSegment()
	if segLimit > 0 {
		immutable.SetMaxRowsPerSegment(segLimit)
	}

	return func() {
		immutable.SetMaxRowsPerSegment(origSegLimit)
	}
}

// merge three unordered files into one ordered file
// |    -------------order file-------------
// |	    ------unordered-----
// |           ------unordered-------
// |  ------unordered------
func TestMergeTool_Merge_mod1(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

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

	mh := NewMergeTestHelper(immutable.NewConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	seriesCount := 3000

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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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

		sh := &record.SortHelper{}
		aux := &record.SortAux{}
		for i := range recs {
			aux.InitRecord(recs[i].Schema)
			aux.Init(recs[i].Times())
			sh.Sort(recs[i], aux)
			recs[i], aux.SortRec = aux.SortRec, recs[i]
			record.CheckRecord(recs[i])
		}
	}()
	require.NoError(t, err)
}

func TestMergeTool_SkipMerge(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	schemas = append(schemas, record.Field{Type: influx.Field_Type_String, Name: "string2"})
	schemas = append(schemas, record.Field{Type: influx.Field_Type_String, Name: "string3"})
	schemas = append(schemas, record.Field{Type: influx.Field_Type_Float, Name: "float2"})
	schemas = append(schemas, record.Field{Type: influx.Field_Type_Float, Name: "float3"})
	for i := 0; i < 20; i++ {
		mh.addRecord(uint64(100+i), rg.generate(schemas, 50000))
	}
	require.NoError(t, mh.saveToOrder())

	for i := 0; i < 10; i++ {
		schemas = getDefaultSchemas()
		mh.addRecord(100, rg.incrBegin(1).generate(schemas, 10))
		schemas = append(schemas, record.Field{Type: influx.Field_Type_Float, Name: "float2"})
		schemas = append(schemas, record.Field{Type: influx.Field_Type_String, Name: "string2"})
		mh.addRecord(uint64(100+i), rg.incrBegin(1).generate(schemas, 10))

		mh.addRecord(200, rg.incrBegin(1).generate(getDefaultSchemas(), 10))
		require.NoError(t, mh.saveToUnordered())
	}
	require.NoError(t, mh.mergeAndCompact(false))

	files, ok := mh.store.OutOfOrder["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())

	name := files.Files()[0].FileName()
	require.Equal(t, "00000002-0000-00010000", name.String())
}

func TestMergeTool_MergeAndCompact(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 256)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
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
			itr.IncrChunkUsed()

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

	mh := NewMergeTestHelper(immutable.NewConfig())
	rg := newRecordGenerator(begin, defaultInterval, false)

	fp := "github.com/openGemini/openGemini/engine/immutable/column-writer-error"
	require.NoError(t, failpoint.Enable(fp, `return("error")`))
	defer failpoint.Disable(fp)

	rg.setBegin(begin)
	mh.addRecord(100, rg.generate(schemas, 100))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(100, rg.generate(schemas, 100))
	require.NoError(t, mh.saveToUnordered())

	tmpFile := filepath.Join(saveDir, "mst", "00000001-0000-00010000.tssp.init")
	_, err := os.Stat(tmpFile)
	require.NotEmpty(t, err)

	assert.NoError(t, mh.mergeAndCompact(true))

	mh.store.Close()
	perf := immutable.NewMergePerformer(nil, statistics.NewMergeStatItem("mst", 1))
	perf.Reset(mh.store.NewStreamWriteFile("mst"), true)
	for _, f := range mh.store.Order["mst"].Files() {
		perf.AppendMergedFile(f)
	}
	for _, f := range mh.store.OutOfOrder["mst"].Files() {
		perf.AppendMergedFile(f)
	}
	perf.CleanTmpFiles()
}

func TestMergeTool_WriteCurrentMetaError(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 16)()
	schemas := getDefaultSchemas()

	fp := "github.com/openGemini/openGemini/engine/immutable/write-current-meta-error"
	require.NoError(t, failpoint.Enable(fp, `return("error")`))
	defer failpoint.Disable(fp)

	func() {
		mh := NewMergeTestHelper(immutable.NewConfig())
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
		mh := NewMergeTestHelper(immutable.NewConfig())
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

	mh := NewMergeTestHelper(immutable.NewConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 4; i++ {
		schema := record.Schemas{
			record.Field{Type: influx.Field_Type_Int, Name: fmt.Sprintf("int_%d", i+1)},
		}
		mh.addRecord(100, rg.incrBegin(200).generate(schema, 100+i*10))
		require.NoError(t, mh.saveToOrder())
	}

	immutable.SegMergeFlag(immutable.StreamingCompact)
	require.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()

	assert.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestItrTSSPFile(t *testing.T) {
	pathName := "00000001-0000-00000000.tssp"
	lockPath := ""
	f, err := immutable.OpenTSSPFile(pathName, &lockPath, true, false)
	if err != nil {
		fmt.Println(err)
	}
	itrTSSPFile(f, func(sid uint64, rec *record.Record) {
		assert.Equal(t, strconv.FormatUint(sid, 10), "12096311501173")
		times := []int64([]int64{1683596485519169600})
		assert.Equal(t, rec.Times(), times)
		assert.Equal(t, rec.String(), "field(value):[]float64{75.3}\nfield(time):[]int64{1683596485519169600}\n")

		record.CheckRecord(rec)
	})
}
