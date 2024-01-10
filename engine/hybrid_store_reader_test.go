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

package engine

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	assert2 "github.com/stretchr/testify/assert"
)

var (
	testTimeStart = time.Date(2021, 11, 1, 0, 0, 0, 0, time.UTC)
	timeInterval  = time.Second
)

func NewMockColumnStoreHybridMstInfo() *meta2.MeasurementInfo {
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	schema := make(map[string]int32)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = int32(schemaForColumnStore[j].Type)
			}
		}
	}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstinfo := &meta2.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta2.ColStoreInfo{
			PrimaryKey:     primaryKey,
			SortKey:        sortKey,
			CompactionType: config.BLOCK,
		},
		Schema: schema,
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{4},
			IndexList: list},
	}
	return mstinfo
}

func MustParseSelectStatementByYacc(s string) *influxql.SelectStatement {
	r := strings.NewReader(s)
	p := influxql.NewParser(r)
	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	q, err := YyParser.GetQuery()
	if err != nil {
		panic(err)
	}
	return q.Statements[0].(*influxql.SelectStatement)
}

func genTestDataForColumnStore(rows int, startValue *float64, starTime *time.Time) *record.Record {
	tm := *starTime
	value := *startValue
	genRecFn := func() *record.Record {
		bv := false
		b := record.NewRecordBuilder(schemaForColumnStore)
		f1Builder := b.Column(0) // float
		f2Builder := b.Column(1) // string
		f3Builder := b.Column(2) // float
		f4Builder := b.Column(3) // int
		f5Builder := b.Column(4) // float
		f6Builder := b.Column(5) // int
		f7Builder := b.Column(6) // bool
		f8Builder := b.Column(7) // string
		tmBuilder := b.Column(8) // timestamp
		for i := 1; i <= rows; i++ {
			f1Builder.AppendFloat(value)
			f2Builder.AppendInteger(int64(value))
			f3Builder.AppendBoolean(bv)
			f4Builder.AppendString(fmt.Sprintf("test-%f", value))
			f5Builder.AppendFloat(value)
			f6Builder.AppendString(fmt.Sprintf("test-%f", value+1.0))
			f7Builder.AppendString(fmt.Sprintf("test-%f", value+2.0))
			f8Builder.AppendInteger(int64(value))
			tmBuilder.AppendInteger(tm.UnixNano())

			tm = tm.Add(timeInterval)
			value += 1.0
			bv = !bv
		}

		*starTime = tm
		*startValue = value
		return b
	}
	rec := genRecFn()
	return rec
}

var schemaForColumnStore = []record.Field{
	{Name: "field1_float", Type: influx.Field_Type_Float},
	{Name: "field2_int", Type: influx.Field_Type_Int},
	{Name: "field3_bool", Type: influx.Field_Type_Boolean},
	{Name: "field4_string", Type: influx.Field_Type_String},
	{Name: "primaryKey_float1", Type: influx.Field_Type_Float},
	{Name: "primaryKey_string1", Type: influx.Field_Type_String},
	{Name: "primaryKey_string2", Type: influx.Field_Type_String},
	{Name: "sortKey_int1", Type: influx.Field_Type_Int},
	{Name: "time", Type: influx.Field_Type_Int},
}

func testEqualChunkNum(out, expect int) bool {
	return out == expect
}

func TestHybridStoreReaderFunctions(t *testing.T) {
	schema := createSortQuerySchema()
	readerPlan := executor.NewLogicalColumnStoreReader(nil, schema)
	reader := NewHybridStoreReader(readerPlan, executor.NewCSIndexInfo("", executor.NewAttachedIndexInfo(nil, nil), logstore.CurrentLogTokenizerVersion))
	assert2.Equal(t, reader.Name(), "HybridStoreReader")
	assert2.Equal(t, len(reader.Explain()), 1)
	reader.Abort()
	assert2.Equal(t, reader.IsSink(), true)
	assert2.Equal(t, len(reader.GetOutputs()), 1)
	assert2.Equal(t, len(reader.GetInputs()), 0)
	assert2.Equal(t, reader.GetOutputNumber(nil), 0)
	assert2.Equal(t, reader.GetInputNumber(nil), 0)
	reader.schema = nil
	err := reader.initQueryCtx()
	assert2.Equal(t, errno.Equal(err, errno.InvalidQuerySchema), true)
	err = reader.Work(context.Background())
	assert2.Equal(t, errno.Equal(err, errno.InvalidQuerySchema), true)
	schema.Options().(*query.ProcessorOptions).Sources[0] = &influxql.Measurement{Name: "students", IsTimeSorted: true}
	reader.schema = schema
	err = reader.initQueryCtx()
	assert2.Equal(t, err, nil)
	_, err = reader.initAttachedFileReader(executor.NewDetachedFrags("", 0))
	assert2.Equal(t, strings.Contains(err.Error(), "invalid index info for attached file reader"), true)
	_, err = reader.initDetachedFileReader(executor.NewAttachedFrags("", 0))
	assert2.Equal(t, strings.Contains(err.Error(), "invalid index info for detached file reader"), true)
	_, err = reader.initFileReader(&executor.DetachedFrags{BaseFrags: *executor.NewBaseFrags("", 3)})
	assert2.Equal(t, strings.Contains(err.Error(), "invalid file reader"), true)
	frag := executor.NewBaseFrags("", 3)
	assert2.Equal(t, 72, frag.Size())
	err = reader.initSchema()
	assert2.Equal(t, err, nil)
	schema1 := createSortQuerySchema()
	schema1.Options().(*query.ProcessorOptions).Condition = &influxql.BinaryExpr{Op: influxql.ADD}
	reader.schema = schema1
	err = reader.initSchema()
	assert2.Equal(t, errno.Equal(err, errno.ErrRPNOp), true)
	schema2 := createSortQuerySchema()
	schema2.Options().(*query.ProcessorOptions).Dimensions = []string{"d1"}
	reader.opt = *schema2.Options().(*query.ProcessorOptions)
	err = reader.initSchema()
	assert2.Equal(t, errno.Equal(err, errno.ErrRPNOp), true)
	err = reader.Work(context.Background())
	assert2.Equal(t, errno.Equal(err, errno.ErrRPNOp), true)
	readerPlan = executor.NewLogicalColumnStoreReader(nil, schema)
	reader = NewHybridStoreReader(readerPlan, executor.NewCSIndexInfo("", executor.NewAttachedIndexInfo(nil, nil), logstore.CurrentLogTokenizerVersion))
	err = reader.Work(context.Background())
	assert2.Equal(t, strings.Contains(err.Error(), "students/segment.idx"), true)
	reader.Close()
	reader.sendChunk(nil)
	assert2.Equal(t, reader.closedCount, int64(1))
}

func TestHybridIndexReaderFunctions(t *testing.T) {
	indexReader := NewDetachedIndexReader(NewIndexContext(true, 8, createSortQuerySchema(), ""), &obs.ObsOptions{}, query.ProcessorOptions{})
	err := indexReader.Init()
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	_, err = indexReader.Next()
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	indexReader1 := NewAttachedIndexReader(NewIndexContext(true, 8, createSortQuerySchema(), ""), &executor.AttachedIndexInfo{})
	_, err = indexReader1.Next()
	assert2.Equal(t, err, nil)
	db, rp, mst, ptId := "db0", "rp0", "mst", uint32(0)
	testDir := t.TempDir()
	defer func() {
		_ = fileops.RemoveAll(testDir)
	}()
	sh, err := createShard(db, rp, ptId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = sh.Close(); err != nil {
			t.Fatal(err)
		}
		if err = sh.indexBuilder.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	opt := query.ProcessorOptions{Sources: influxql.Sources{&influxql.Measurement{Name: mst}}}
	querySchema := executor.NewQuerySchema(nil, nil, &opt, nil)
	_, err = sh.GetIndexInfo(querySchema)
	assert2.Equal(t, err, nil)
	immutable.SetDetachedFlushEnabled(true)
	indexReader = NewDetachedIndexReader(NewIndexContext(false, 0, querySchema, ""), &obs.ObsOptions{}, query.ProcessorOptions{})
	err = indexReader.Init()
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	immutable.SetDetachedFlushEnabled(false)
}

func TestHybridStoreReader(t *testing.T) {
	testDir := t.TempDir()
	defer func() {
		_ = fileops.RemoveAll(testDir)
	}()
	db, rp, mst := "db0", "rp0", "mst"
	ptId := uint32(1)
	executor.RegistryTransformCreator(&executor.LogicalColumnStoreReader{}, &ColumnStoreReaderCreator{})
	fields := make(map[string]influxql.DataType)
	for _, field := range schemaForColumnStore {
		fields[field.Name] = record.ToInfluxqlTypes(field.Type)
	}

	// create the shard.
	var err error
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(1024)
	sh, err := createShard(db, rp, ptId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	compWorker.UnregisterShard(sh.GetID())
	immutable.SetMaxCompactor(8)
	defer func() {
		if err = sh.Close(); err != nil {
			t.Fatal(err)
		}
		if err = sh.indexBuilder.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// write data to the shard.
	startValue := 999999.0
	tm := testTimeStart
	recRows := 1000
	filesN := 8
	sh.SetMstInfo(NewMockColumnStoreHybridMstInfo())
	for i := 0; i < filesN; i++ {
		rec := genTestDataForColumnStore(recRows, &startValue, &tm)
		if err = sh.WriteCols(mst, rec, nil); err != nil {
			t.Fatal(err)
		}
		sh.ForceFlush()
		time.Sleep(time.Second * 1)
	}

	// start to compact
	time.Sleep(time.Second * 3)
	err = sh.immTables.LevelCompact(0, sh.GetID())
	if err != nil {
		t.Fatal(err)
	}
	sh.immTables.(*immutable.MmsTables).Wait()

	// set the primary index reader
	sh.pkIndexReader = sparseindex.NewPKIndexReader(colstore.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// set the skip index reader
	sh.skIndexReader = sparseindex.NewSKIndexReader(colstore.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// build the shard group
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	// build the storage engine
	storeEngine := NewMockStoreEngine()
	storeEngine.SetShard(sh)

	for _, tt := range []struct {
		skip      bool
		exec      bool
		scanErr   bool
		name      string
		q         string
		tr        util.TimeRange
		out       hybridqp.RowDataType
		frags     executor.ShardsFragments
		expected  int
		readerOps []hybridqp.ExprOptions
		fields    map[string]influxql.DataType
		expect    func(out, expect int) bool
	}{
		{
			name:      "select * from db0.rp0.mst limit 1",
			q:         `select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test') limit 1`,
			tr:        util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  1,
			expect:    testEqualChunkNum,
		},
		{
			name:      "select * from db0.rp0.mst",
			q:         `select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test')`,
			tr:        util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  8000,
			expect:    testEqualChunkNum,
		},
		{
			name:      "select * from db0.rp0.mst group by primaryKey_string1",
			q:         `select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test') group by primaryKey_string1`,
			tr:        util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  8000,
			expect:    testEqualChunkNum,
		},
		{
			name:      "select * from db0.rp0.mst group by primaryKey_string1",
			q:         `select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test') group by primaryKey_string2`,
			tr:        util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  8000,
			expect:    testEqualChunkNum,
		},
		{
			name:      "select * from db0.rp0.mst group by primaryKey_string1",
			q:         `select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test111') group by primaryKey_string2`,
			tr:        util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  0,
			expect:    testEqualChunkNum,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// step1: parse stmt and opt
			ctx := context.Background()

			stmt := MustParseSelectStatementByYacc(tt.q)
			stmt, err = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024, IncQuery: false}
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			list := make([]*influxql.IndexList, 1)
			bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
			iList := influxql.IndexList{IList: bfColumn}
			list[0] = &iList
			source := influxql.Sources{&influxql.Measurement{
				Database:        db,
				RetentionPolicy: rp,
				Name:            mst,
				IndexRelation: &influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
					Oids:      []uint32{4},
					IndexList: list},
				EngineType: config.COLUMNSTORE},
			}
			opt.Name = mst
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			opt.Condition = stmt.Condition
			opt.MaxParallel = 1
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

			// step2: build the store executor
			info := buildIndexScanExtraInfo(storeEngine, db, ptId, []uint64{sh.GetID()}, []uint32{logstore.CurrentLogTokenizerVersion}, filepath.Join(sh.dataPath, "columnstore"))
			reader := executor.NewLogicalColumnStoreReader(nil, querySchema)
			var input hybridqp.QueryNode
			if querySchema.HasCall() && querySchema.CanCallsPushdown() {
				input = executor.NewLogicalHashAgg(reader, querySchema, executor.UNKNOWN_EXCHANGE, nil)
			} else {
				input = executor.NewLogicalHashMerge(reader, querySchema, executor.READER_EXCHANGE, nil)
			}
			indexScan := executor.NewLogicalIndexScan(input, querySchema)
			executor.ReWriteArgs(indexScan, false)
			scan := executor.NewIndexScanTransform(tt.out, indexScan.RowExprOptions(), indexScan.Schema(), indexScan.Children()[0], info, make(chan struct{}, 1), 0)
			sink := NewNilSink(tt.out)
			err = executor.Connect(scan.GetOutputs()[0], sink.Input)
			if err != nil {
				t.Fatal(err)
			}

			var processors executor.Processors
			processors = append(processors, scan)
			processors = append(processors, sink)

			// step3: build the pipeline executor from the dag
			executors := executor.NewPipelineExecutor(processors)
			err = executors.Execute(ctx)
			if err != nil && !tt.scanErr {
				t.Fatalf("connect error")
			}
			executors.Release()
			executors.Crash()
			rowCount := 0
			for _, c := range sink.Chunks {
				rowCount += c.Len()
			}
			if !tt.expect(rowCount, tt.expected) {
				t.Errorf("`%s` failed", tt.name)
			}
		})
	}
}

func TestHybridStoreReaderByLogStore(t *testing.T) {
	schema := createSortQuerySchema()
	readerPlan := executor.NewLogicalColumnStoreReader(nil, schema)
	reader := NewHybridStoreReader(readerPlan, executor.NewCSIndexInfo("", executor.NewAttachedIndexInfo(nil, nil), 4))
	reader.initQueryCtx()
	_, err := reader.CreateLogStoreCursor()
	if err == nil {
		t.Errorf("get wrong cursor")
	}

	var fields influxql.Fields
	m := createMeasurement()
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}, IncQuery: true}
	fields = append(fields, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:   "f1",
			Type:  influxql.Integer,
			Alias: "",
		},
	})
	var names []string
	names = append(names, "f1")
	schema = executor.NewQuerySchema(fields, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())
	readerPlan = executor.NewLogicalColumnStoreReader(nil, schema)
	reader = NewHybridStoreReader(readerPlan, executor.NewCSIndexInfo("", executor.NewAttachedIndexInfo(nil, nil), 4))
	reader.initQueryCtx()
	cursor, _ := reader.CreateLogStoreCursor()
	if _, ok := cursor.(*AggTagSetCursor); !ok {
		t.Errorf("get wrong cursor")
	}
}

func TestHybridStoreReaderForInc(t *testing.T) {
	testDir := t.TempDir()
	defer func() {
		_ = fileops.RemoveAll(testDir)
	}()
	db, rp, mst := "db0", "rp0", "mst"
	ptId := uint32(1)
	executor.RegistryTransformCreator(&executor.LogicalColumnStoreReader{}, &ColumnStoreReaderCreator{})
	fields := make(map[string]influxql.DataType)
	for _, field := range schemaForColumnStore {
		fields[field.Name] = record.ToInfluxqlTypes(field.Type)
	}

	// create the shard.
	var err error
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(1024)
	sh, err := createShard(db, rp, ptId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	compWorker.UnregisterShard(sh.GetID())
	immutable.SetMaxCompactor(8)
	defer func() {
		if err = sh.Close(); err != nil {
			t.Fatal(err)
		}
		if err = sh.indexBuilder.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// write data to the shard.
	startValue := 999999.0
	tm := testTimeStart
	recRows := 1000
	filesN := 8
	sh.SetMstInfo(NewMockColumnStoreHybridMstInfo())
	for i := 0; i < filesN; i++ {
		rec := genTestDataForColumnStore(recRows, &startValue, &tm)
		if err = sh.WriteCols(mst, rec, nil); err != nil {
			t.Fatal(err)
		}
		sh.ForceFlush()
		time.Sleep(time.Second * 1)
	}

	// start to compact
	time.Sleep(time.Second * 3)
	err = sh.immTables.LevelCompact(0, sh.GetID())
	if err != nil {
		t.Fatal(err)
	}
	sh.immTables.(*immutable.MmsTables).Wait()

	// set the primary index reader
	sh.pkIndexReader = sparseindex.NewPKIndexReader(colstore.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// set the skip index reader
	sh.skIndexReader = sparseindex.NewSKIndexReader(colstore.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)
	var fields2 influxql.Fields
	m := &influxql.Measurement{Name: mst}
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}, EndTime: math.MaxInt64}
	fields2 = append(fields2, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:   "f1",
			Type:  influxql.Integer,
			Alias: "",
		},
	})
	var names []string
	names = append(names, "f1")
	schema := executor.NewQuerySchema(fields2, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())
	iterId := int32(0)
	queryID := t.Name()
	for {
		indexReader := NewDetachedIndexReader(NewIndexContext(true, 8, schema, sh.filesPath), nil, query.ProcessorOptions{IncQuery: true, IterID: iterId, LogQueryCurrId: queryID})
		iterId += 1
		indexReader.Init()
		frag, err := indexReader.Next()
		if err != nil {
			t.Errorf("index reader fail")
		}
		if frag == nil || frag.FragCount() == 0 {
			break
		}
		frag, _ = indexReader.Next()
	}
	opt.EndTime = 0
	schema = executor.NewQuerySchema(fields2, names, &opt, nil)
	indexReader := NewDetachedIndexReader(NewIndexContext(true, 8, schema, sh.filesPath), nil, query.ProcessorOptions{IncQuery: true, IterID: iterId})
	iterId += 1
	indexReader.Init()
	_, ok := immutable.GetDetachedSegmentTask(queryID)
	if !ok {
		t.Error("get wrong cache")
	}
	_, ok = immutable.GetDetachedSegmentTask(queryID + "wrong")
	if ok {
		t.Error("get cache")
	}
}
