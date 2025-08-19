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

package engine

import (
	"context"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"reflect"
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
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testTimeStart = time.Date(2021, 11, 1, 0, 0, 0, 0, time.UTC)
	timeInterval  = time.Second
)

func NewMockColumnStoreHybridMstInfo() *meta2.MeasurementInfo {
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta2.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	mstinfo := &meta2.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta2.ColStoreInfo{
			PrimaryKey:     primaryKey,
			SortKey:        sortKey,
			CompactionType: config.BLOCK,
		},
		Schema: &schema,
		IndexRelation: influxql.IndexRelation{
			IndexNames: []string{"bloomfilter", "bloomfilter"},
			Oids:       []uint32{uint32(index.BloomFilter), uint32(index.BloomFilter)},
			IndexList: []*influxql.IndexList{
				{IList: []string{"primaryKey_string1"}},
				{IList: []string{"primaryKey_string2"}}},
		},
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
	reader.Close()
	reader.sendChunk(nil)
	assert2.Equal(t, reader.closedCount, int64(1))
}

func TestHybridStoreReaderFunctionsForFullText(t *testing.T) {
	var fields influxql.Fields
	m := createMeasurement()
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}}
	fields = append(fields, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:   logparser.DefaultFieldForFullText,
			Type:  influxql.String,
			Alias: "",
		},
	})
	var names []string
	names = append(names, "f1")
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())

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
	schema.Options().(*query.ProcessorOptions).Sources[0] = &influxql.Measurement{
		Name:         "students",
		IsTimeSorted: true,
		IndexRelation: &influxql.IndexRelation{IndexNames: []string{index.BloomFilterFullTextIndex},
			Oids:      []uint32{uint32(index.BloomFilterFullText)},
			IndexList: []*influxql.IndexList{&influxql.IndexList{IList: []string{"field1_string"}}},
		},
	}
	reader.schema = schema
	err = reader.initQueryCtx()
	assert2.Equal(t, err, nil)
	frag := executor.NewBaseFrags("", 3)
	assert2.Equal(t, 72, frag.Size())
	err = reader.initSchema()
	assert2.Equal(t, err, nil)
}

func TestHybridIndexReaderFunctions(t *testing.T) {
	unnest := &influxql.Unnest{
		Expr: &influxql.Call{
			Name: "match_all",
			Args: []influxql.Expr{
				&influxql.VarRef{Val: "([a-z]+),([0-9]+)", Type: influxql.String},
				&influxql.VarRef{Val: "field1", Type: influxql.String},
			},
		},
		Aliases: []string{"key1", "value1"},
		DstType: []influxql.DataType{influxql.String, influxql.String},
	}
	schema := createSortQuerySchema()
	schema.SetUnnests([]*influxql.Unnest{unnest})
	readCtx := immutable.NewFileReaderContext(util.TimeRange{}, nil, immutable.NewReadContext(true), nil, nil, true)
	indexReader := NewDetachedIndexReader(NewIndexContext(true, 8, schema, ""), &obs.ObsOptions{}, readCtx)
	_, _, err := indexReader.CreateCursors()
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	_, err = indexReader.initFileReader(executor.NewDetachedFrags("", 0))
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)

	indexReader = NewDetachedIndexReader(NewIndexContext(true, 8, createSortQuerySchema(), ""), &obs.ObsOptions{}, nil)
	err = indexReader.Init()
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	_, err = indexReader.Next()
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	indexReader1 := NewAttachedIndexReader(NewIndexContext(true, 8, createSortQuerySchema(), ""), &executor.AttachedIndexInfo{}, nil)
	_, err = indexReader1.Next()
	assert2.Equal(t, err, nil)

	indexReader1 = NewAttachedIndexReader(NewIndexContext(true, 8, createSortQuerySchema(), ""), &executor.AttachedIndexInfo{}, nil)
	_, err = indexReader1.initFileReader(executor.NewDetachedFrags("", 0))
	assert2.Equal(t, strings.Contains(err.Error(), "invalid index info for attached file reader"), true)
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
	indexReader = NewDetachedIndexReader(NewIndexContext(false, 0, querySchema, ""), &obs.ObsOptions{}, nil)
	err = indexReader.Init()
	assert2.Equal(t, strings.Contains(err.Error(), "endpoint is not set"), true)
	immutable.SetDetachedFlushEnabled(false)
}

func TestHybridStoreReader(t *testing.T) {
	t.Skipf("TODO: column store engine is being improved and will be adapted to this use case in a later MR")
	testDir := t.TempDir()
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
	require.NoError(t, err)

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

	defer clearMstInfo()
	ident := defaultIdent()
	ident.SetName("mst")
	colstore.MstManagerIns().Add(ident, NewMockColumnStoreHybridMstInfo())

	for i := 0; i < filesN; i++ {
		rec := genTestDataForColumnStore(recRows, &startValue, &tm)
		if err = sh.WriteCols(mst, rec, nil); err != nil {
			t.Fatal(err)
		}
		sh.ForceFlush()
		sh.waitSnapshot()
	}

	// start to compact
	err = sh.immTables.LevelCompact(0, sh.GetID())
	if err != nil {
		t.Fatal(err)
	}
	sh.immTables.(*immutable.MmsTables).Wait()

	// set the primary index reader
	sh.pkIndexReader = sparseindex.NewPKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// set the skip index reader
	sh.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// build the shard group
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	// build the storage engine
	storeEngine := NewMockStoreEngine()
	storeEngine.SetShard(sh)
	config.SetProductType(config.LogKeeperService)
	defer config.SetProductType("")

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
			name:      "select * from db0.rp0.mst order by time limit 1",
			q:         `select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test') order by time limit 1`,
			tr:        util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  1,
			expect:    testEqualChunkNum,
		},
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
		{
			name:      "select * from db0.rp0.mst where or",
			q:         `select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test111') or MATCHPHRASE(primaryKey_string2, 'test')`,
			tr:        util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  1245,
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
			source := influxql.Sources{&influxql.Measurement{
				Database:        db,
				RetentionPolicy: rp,
				Name:            mst,
				IndexRelation: &influxql.IndexRelation{
					IndexNames: []string{"bloomfilter", "bloomfilter"},
					Oids:       []uint32{uint32(index.BloomFilter), uint32(index.BloomFilter)},
					IndexList: []*influxql.IndexList{
						{IList: []string{"primaryKey_string1"}},
						{IList: []string{"primaryKey_string2"}}},
				},
				IsTimeSorted: true,
				EngineType:   config.COLUMNSTORE},
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
			scan := executor.NewIndexScanTransform(tt.out, indexScan.RowExprOptions(), indexScan.Schema(), indexScan.Children()[0], info, make(chan struct{}, 1), 0, false)
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
}

func TestHybridStoreReaderForInc(t *testing.T) {
	t.Skipf("TODO: column store engine is being improved and will be adapted to this use case in a later MR")
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

	defer clearMstInfo()
	ident := defaultIdent()
	ident.SetName("mst")
	colstore.MstManagerIns().Add(ident, NewMockColumnStoreHybridMstInfo())

	for i := 0; i < filesN; i++ {
		rec := genTestDataForColumnStore(recRows, &startValue, &tm)
		if err = sh.WriteCols(mst, rec, nil); err != nil {
			t.Fatal(err)
		}
		sh.ForceFlush()
		sh.waitSnapshot()
	}

	// start to compact
	time.Sleep(time.Second)
	err = sh.immTables.LevelCompact(0, sh.GetID())
	if err != nil {
		t.Fatal(err)
	}
	sh.immTables.(*immutable.MmsTables).Wait()

	// set the primary index reader
	sh.pkIndexReader = sparseindex.NewPKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// set the skip index reader
	sh.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)
	var fields2 influxql.Fields
	m := &influxql.Measurement{Name: mst}
	fields2 = append(fields2, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:   "f1",
			Type:  influxql.Integer,
			Alias: "",
		},
	})
	var names []string
	names = append(names, "f1")
	iterId := int32(0)
	queryID := t.Name()
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}, EndTime: math.MaxInt64, IncQuery: true, IterID: iterId, LogQueryCurrId: queryID}
	schema := executor.NewQuerySchema(fields2, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())
	for {
		indexReader := NewDetachedIndexReader(NewIndexContext(true, 8, schema, sh.filesPath), nil, nil)
		schema.Options().(*query.ProcessorOptions).IterID += 1
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
	indexReader := NewDetachedIndexReader(NewIndexContext(true, 8, schema, sh.filesPath), nil, nil)
	schema.Options().(*query.ProcessorOptions).IterID += 1
	indexReader.Init()
	_, ok := immutable.GetDetachedSegmentTask(sh.filesPath + queryID)
	if !ok {
		t.Error("get wrong cache")
	}
	_, ok = immutable.GetDetachedSegmentTask(queryID + "wrong")
	if ok {
		t.Error("get cache")
	}
	indexReader.ctx.schema.Options().(*query.ProcessorOptions).IterID = 0
	frag, _ := indexReader.Next()
	assert2.Equal(t, frag, nil)
	indexReader.ctx.schema.Options().(*query.ProcessorOptions).IterID = 1
	indexReader.ctx.schema.Options().(*query.ProcessorOptions).LogQueryCurrId = "query_id"
	indexReader.init = false
	frag, _ = indexReader.Next()
	assert2.Equal(t, frag, nil)
}

func TestInitSchemaByUnnest(t *testing.T) {
	field := "content"
	alias := "key1"
	unnest := &influxql.Unnest{
		Expr: &influxql.Call{
			Name: "match_all",
			Args: []influxql.Expr{
				&influxql.VarRef{Val: "([a-z]+),([0-9]+)", Type: influxql.String},
				&influxql.VarRef{Val: field, Type: influxql.String},
			},
		},
		Aliases: []string{alias},
		DstType: []influxql.DataType{influxql.String, influxql.String},
	}
	schema := createSortQuerySchema()
	schema.SetUnnests([]*influxql.Unnest{unnest})
	readerPlan := executor.NewLogicalColumnStoreReader(nil, schema)
	reader := NewHybridStoreReader(readerPlan, executor.NewCSIndexInfo("", executor.NewAttachedIndexInfo(nil, nil), logstore.CurrentLogTokenizerVersion))
	err := reader.initSchemaByUnnest()
	if err != nil {
		t.Fatal("initSchemaByUnnest failed")
	}
	if reader.inSchema.FieldIndex(field) == -1 {
		t.Fatalf("initSchemaByUnnest failed, %s is not existed in schema", field)
	}
	if reader.inSchema.FieldIndex(alias) == -1 {
		t.Fatalf("initSchemaByUnnest failed, %s is not existed in schema", alias)
	}

	reader.inSchema = make(record.Schemas, 0)
	reader.inSchema = append(reader.inSchema, record.Field{Name: field, Type: influx.Field_Type_String})
	err = reader.initSchemaByUnnest()
	if err != nil {
		t.Fatal("initSchemaByUnnest failed")
	}
	if reader.inSchema.FieldIndex(field) == -1 {
		t.Fatalf("initSchemaByUnnest failed, %s is not existed in schema", field)
	}
	if reader.inSchema.FieldIndex(alias) == -1 {
		t.Fatalf("initSchemaByUnnest failed, %s is not existed in schema", alias)
	}
}

func genRecordFortRranRec() *record.Record {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{1, 2, 3, 4, 5, 6, 7})
	return rec
}

func createQuerySchemaForTranRec() *executor.QuerySchema {
	var fields influxql.Fields
	m := &influxql.Measurement{Name: "tranRec"}
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}, Dimensions: []string{"string", "float"}}
	fields = append(fields, influxql.Fields{
		{Expr: &influxql.VarRef{Val: "int", Type: influxql.Integer, Alias: ""}},
		{Expr: &influxql.VarRef{Val: "float", Type: influxql.Float, Alias: ""}},
		{Expr: &influxql.VarRef{Val: "boolean", Type: influxql.Boolean, Alias: ""}},
		{Expr: &influxql.VarRef{Val: "string", Type: influxql.String, Alias: ""}},
		{Expr: &influxql.VarRef{Val: "time", Type: influxql.Integer, Alias: ""}},
	}...)
	names := []string{"int", "float", "boolean", "string", "time"}
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())
	return schema
}

func TestHybridStoreReaderTranRecToChunk(t *testing.T) {
	schema := createQuerySchemaForTranRec()
	readerPlan := executor.NewLogicalColumnStoreReader(nil, schema)
	reader := NewHybridStoreReader(readerPlan, executor.NewCSIndexInfo("", executor.NewAttachedIndexInfo(nil, nil), logstore.CurrentLogTokenizerVersion))
	if err := reader.initQueryCtx(); err != nil {
		t.Fatalf("HybridStoreReader initQueryCtx , err: %+v", err)
	}
	if err := reader.initSchema(); err != nil {
		t.Fatalf("initSchema initQueryCtx , err: %+v", err)
	}
	rec := genRecordFortRranRec()
	chk, err := reader.tranRecToChunk(rec)
	if err != nil {
		t.Fatalf("trans rec to chunk failed, err: %+v", err)
	}
	// compare column and dim for the filed "string"
	if !reflect.DeepEqual(chk.Column(3), chk.Dim(0)) {
		t.Fatal("trans rec to dim failed. The column[3] not equal to dim[0]")
	}
	fmt.Println(chk.Column(3).BitMap())
}

func TestNewDetachedLazyLoadIndexReader(t *testing.T) {
	config.SetProductType(config.LogKeeperService)
	defer config.SetProductType("")
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

	defer clearMstInfo()
	ident := defaultIdent()
	ident.SetName("mst")
	colstore.MstManagerIns().Add(ident, NewMockColumnStoreHybridMstInfo())

	for i := 0; i < filesN; i++ {
		rec := genTestDataForColumnStore(recRows, &startValue, &tm)
		if err = sh.WriteCols(mst, rec, nil); err != nil {
			t.Fatal(err)
		}
		sh.ForceFlush()
		sh.waitSnapshot()
	}

	// start to compact
	time.Sleep(time.Second * 3)
	err = sh.immTables.LevelCompact(0, sh.GetID())
	if err != nil {
		t.Fatal(err)
	}
	sh.immTables.(*immutable.MmsTables).Wait()

	// set the primary index reader
	sh.pkIndexReader = sparseindex.NewPKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// set the skip index reader
	sh.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// build the shard group
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	// build the storage engine
	storeEngine := NewMockStoreEngine()
	storeEngine.SetShard(sh)
	tr := util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000}
	q := "select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst order by time limit 1"
	stmt := MustParseSelectStatementByYacc(q)
	stmt, err = stmt.RewriteFields(shardGroup, true, false)
	stmt.OmitTime = true
	sopt := query.SelectOptions{ChunkSize: 1024, IncQuery: false}
	opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
	source := influxql.Sources{&influxql.Measurement{
		Database:        db,
		RetentionPolicy: rp,
		Name:            mst,
		IndexRelation: &influxql.IndexRelation{
			IndexNames: []string{"bloomfilter", "bloomfilter"},
			Oids:       []uint32{uint32(index.BloomFilter), uint32(index.BloomFilter)},
			IndexList: []*influxql.IndexList{
				{IList: []string{"primaryKey_string1"}},
				{IList: []string{"primaryKey_string2"}}},
		},
		IsTimeSorted: true,
		EngineType:   config.COLUMNSTORE},
	}
	opt.Name = mst
	opt.Sources = source
	opt.StartTime = tr.Min
	opt.EndTime = tr.Max
	opt.Condition = stmt.Condition
	opt.MaxParallel = 1
	querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
	readCtx := immutable.NewFileReaderContext(tr, schemaForColumnStore, immutable.NewReadContext(true), immutable.NewFilterOpts(nil, nil, nil, nil), nil, true)
	indexReader := NewDetachedLazyLoadIndexReader(NewIndexContext(true, 8, querySchema, sh.filesPath), nil, readCtx)
	indexReader.StartSpan(nil)
	_, span := tracing.NewTrace("root")
	indexReader.StartSpan(span)
	c, _, _ := indexReader.CreateCursors()
	re, _, _ := c[0].Next()
	if re.RowNums() != 1 {
		t.Errorf("readdata failed")
	}
	if re.Times()[0] != 1635731554000000000 {
		t.Errorf("get wrong data")
	}
	testCur := c[0].(*immutable.SortLimitCursor).GetInput()
	if testCur.Name() != "StreamDetachedReader" {
		t.Errorf("get wrong name")
	}
	if testCur.GetSchema() == nil {
		t.Errorf("get wrong schema")
	}
	testCur.UpdateTime(0)
	if testCur.Close() != nil {
		t.Errorf("close err")
	}
	reAgg, _, _ := testCur.NextAggData()
	if reAgg != nil {
		t.Errorf("get wrong data")
	}

	immutable.SetDetachedFlushEnabled(true)
	defer immutable.SetDetachedFlushEnabled(false)

	q = "select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst where MATCHPHRASE(primaryKey_string1, 'test') order by time limit 1"
	stmt = MustParseSelectStatementByYacc(q)
	stmt, err = stmt.RewriteFields(shardGroup, true, false)
	stmt.OmitTime = true
	opt.Condition = stmt.Condition
	unnest := &influxql.Unnest{
		Expr: &influxql.Call{
			Name: "match_all",
			Args: []influxql.Expr{
				&influxql.VarRef{Val: "([a-z]+),([0-9]+)", Type: influxql.String},
				&influxql.VarRef{Val: "field4_string", Type: influxql.String},
			},
		},
		Aliases: []string{"key1", "value1"},
		DstType: []influxql.DataType{influxql.String, influxql.String},
	}
	querySchema2 := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
	querySchema2.SetUnnests([]*influxql.Unnest{unnest})
	currSchemaForColumnStore := []record.Field{
		{Name: "field1_float", Type: influx.Field_Type_Float},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_bool", Type: influx.Field_Type_Boolean},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "key1", Type: influx.Field_Type_String},
		{Name: "primaryKey_float1", Type: influx.Field_Type_Float},
		{Name: "primaryKey_string1", Type: influx.Field_Type_String},
		{Name: "primaryKey_string2", Type: influx.Field_Type_String},
		{Name: "sortKey_int1", Type: influx.Field_Type_Int},
		{Name: "value1", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	readCtx = immutable.NewFileReaderContext(tr, currSchemaForColumnStore, immutable.NewReadContext(true), immutable.NewFilterOpts(nil, nil, nil, nil), nil, true)
	indexReader = NewDetachedLazyLoadIndexReader(NewIndexContext(true, 8, querySchema2, sh.filesPath), nil, readCtx)
	c, _, _ = indexReader.CreateCursors()
	_, _, err = c[0].Next()
	if err != nil {
		t.Errorf("readdata failed, %s", err.Error())
	}

	opt.Ascending = false
	tr.Min = 0
	tr.Max = 1635724700000000000
	readCtx = immutable.NewFileReaderContext(tr, schemaForColumnStore, immutable.NewReadContext(true), immutable.NewFilterOpts(nil, nil, nil, nil), nil, true)
	indexReader = NewDetachedLazyLoadIndexReader(NewIndexContext(true, 8, querySchema, sh.filesPath), nil, readCtx)
	c, _, _ = indexReader.CreateCursors()
	re, _, _ = c[0].Next()
	if re != nil {
		t.Errorf("readdata failed")
	}
	c[0].(*immutable.SortLimitCursor).GetInput().SetOps(nil)
	c[0].(*immutable.SortLimitCursor).GetInput().UpdateTime(0)

	c[0].(*immutable.SortLimitCursor).GetInput().StartSpan(nil)
	c[0].(*immutable.SortLimitCursor).GetInput().EndSpan()
	c[0].(*immutable.SortLimitCursor).GetInput().SinkPlan(nil)

	readCtx = immutable.NewFileReaderContext(util.TimeRange{}, schemaForColumnStore, immutable.NewReadContext(true), immutable.NewFilterOpts(nil, nil, nil, nil), nil, true)
	indexReader = NewDetachedLazyLoadIndexReader(NewIndexContext(true, 8, querySchema, ""), nil, readCtx)
	c, _, err = indexReader.CreateCursors()
	if err != nil {
		t.Errorf("create cursor failed")
	}

	_, _, err = c[0].Next()
	if err != nil {
		t.Errorf("readdata failed")
	}
}

func TestNewDetachedLazyLoadIndexReaderWhenErr(t *testing.T) {
	config.SetProductType(config.LogKeeperService)
	defer config.SetProductType("")
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

	defer clearMstInfo()
	ident := defaultIdent()
	ident.SetName("mst")
	colstore.MstManagerIns().Add(ident, NewMockColumnStoreHybridMstInfo())

	for i := 0; i < filesN; i++ {
		rec := genTestDataForColumnStore(recRows, &startValue, &tm)
		if err = sh.WriteCols(mst, rec, nil); err != nil {
			t.Fatal(err)
		}
		sh.ForceFlush()
		sh.waitSnapshot()
	}

	// start to compact
	time.Sleep(time.Second * 3)
	err = sh.immTables.LevelCompact(0, sh.GetID())
	if err != nil {
		t.Fatal(err)
	}
	sh.immTables.(*immutable.MmsTables).Wait()

	// set the primary index reader
	sh.pkIndexReader = sparseindex.NewPKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// set the skip index reader
	sh.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	// build the shard group
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	// build the storage engine
	storeEngine := NewMockStoreEngine()
	storeEngine.SetShard(sh)
	tr := util.TimeRange{Min: 1635724800000000000, Max: 1635732800000000000}
	q := "select field1_float,field2_int,field3_bool,field4_string from db0.rp0.mst order by time limit 1"
	stmt := MustParseSelectStatementByYacc(q)
	stmt, err = stmt.RewriteFields(shardGroup, true, false)
	stmt.OmitTime = true
	sopt := query.SelectOptions{ChunkSize: 1024, IncQuery: false}
	opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
	source := influxql.Sources{&influxql.Measurement{
		Database:        db,
		RetentionPolicy: rp,
		Name:            mst,
		IndexRelation: &influxql.IndexRelation{
			IndexNames: []string{"bloomfilter", "bloomfilter"},
			Oids:       []uint32{uint32(index.BloomFilter), uint32(index.BloomFilter)},
			IndexList: []*influxql.IndexList{
				{IList: []string{"primaryKey_string1"}},
				{IList: []string{"primaryKey_string2"}}},
		},
		IsTimeSorted: true,
		EngineType:   config.COLUMNSTORE},
	}
	opt.Name = mst
	opt.Sources = source
	opt.StartTime = tr.Min
	opt.EndTime = tr.Max
	opt.Condition = stmt.Condition
	opt.MaxParallel = 1
	querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
	readCtx := immutable.NewFileReaderContext(tr, schemaForColumnStore, immutable.NewReadContext(true), immutable.NewFilterOpts(nil, nil, nil, nil), nil, true)
	indexReader := NewDetachedLazyLoadIndexReader(NewIndexContext(true, 8, querySchema, sh.filesPath), nil, readCtx)
	err = fileops.Remove(path.Join(sh.filesPath, opt.GetMeasurements()[0].Name, immutable.DataFile))
	if err != nil {
		t.Fatal("remove file failed")
	}
	c, _, err := indexReader.CreateCursors()
	_, _, err = c[0].Next()
	if err == nil {
		t.Errorf("readdata failed")
	}

	err = fileops.Remove(path.Join(sh.filesPath, opt.GetMeasurements()[0].Name, immutable.PrimaryMetaFile))
	if err != nil {
		t.Fatal("remove file failed")
	}
	c, _, _ = indexReader.CreateCursors()
	_, _, err = c[0].Next()
	if err == nil {
		t.Errorf("readdata failed")
	}
}

func TestHybridStoreReaderWhileAbort(t *testing.T) {
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
	frag := executor.NewBaseFrags("", 3)
	assert2.Equal(t, 72, frag.Size())
	err = reader.initSchema()
	assert2.Equal(t, err, nil)
	reader.cursors = append(reader.cursors, immutable.NewSortLimitCursor(schema.Options(), reader.inSchema, nil, 0))
	err = reader.Work(context.Background())
	assert2.Equal(t, nil, err)
}
