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
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	assert2 "github.com/stretchr/testify/assert"
)

type NilSink struct {
	executor.BaseProcessor

	Input  *executor.ChunkPort
	Chunks []executor.Chunk
}

func NewNilSink(rowDataType hybridqp.RowDataType) *NilSink {
	return &NilSink{
		Input:  executor.NewChunkPort(rowDataType),
		Chunks: make([]executor.Chunk, 0),
	}
}

func (sink *NilSink) Name() string {
	return "NilSink"
}

func (sink *NilSink) Explain() []executor.ValuePair {
	return nil
}

func (sink *NilSink) Close() {}

func (sink *NilSink) Work(ctx context.Context) error {
	for {
		select {
		case c, ok := <-sink.Input.State:
			if !ok {
				return nil
			}
			sink.Chunks = append(sink.Chunks, c.Clone())
		case <-ctx.Done():
			return nil
		}
	}
}

func (sink *NilSink) GetOutputs() executor.Ports {
	return executor.Ports{}
}

func (sink *NilSink) GetInputs() executor.Ports {
	if sink.Input == nil {
		return executor.Ports{}
	}

	return executor.Ports{sink.Input}
}

func (sink *NilSink) GetOutputNumber(_ executor.Port) int {
	return executor.INVALID_NUMBER
}

func (sink *NilSink) GetInputNumber(_ executor.Port) int {
	return 0
}

func testEqualChunks(t *testing.T, outChunks, dstChunks []executor.Chunk) bool {
	// check the result
	if len(dstChunks) != len(outChunks) {
		t.Fatalf("the chunk number is not the same as the target: %d != %d\n", len(dstChunks), len(outChunks))
	}
	for i := range outChunks {
		assert2.Equal(t, outChunks[i].Name(), dstChunks[i].Name())
		assert2.Equal(t, outChunks[i].Tags(), dstChunks[i].Tags())
		assert2.Equal(t, outChunks[i].Time(), dstChunks[i].Time())
		assert2.Equal(t, outChunks[i].TagIndex(), dstChunks[i].TagIndex())
		assert2.Equal(t, outChunks[i].IntervalIndex(), dstChunks[i].IntervalIndex())
		for j := range outChunks[i].Columns() {
			assert2.Equal(t, outChunks[i].Column(j), dstChunks[i].Column(j))
		}
	}
	return true
}

func testEqualChunk1(t *testing.T, outChunks, dstChunks []executor.Chunk) bool {
	return len(outChunks) == len(dstChunks)
}

func buildComReaderOps() []hybridqp.ExprOptions {
	return []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: `field1_string`, Type: influxql.String},
			Ref:  influxql.VarRef{Val: `field1_string`, Type: influxql.String},
		},
		{
			Expr: &influxql.VarRef{Val: `field2_int`, Type: influxql.Integer},
			Ref:  influxql.VarRef{Val: `field2_int`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: `field3_bool`, Type: influxql.Boolean},
			Ref:  influxql.VarRef{Val: `field3_bool`, Type: influxql.Boolean},
		},
		{
			Expr: &influxql.VarRef{Val: `field4_float`, Type: influxql.Float},
			Ref:  influxql.VarRef{Val: `field4_float`, Type: influxql.Float},
		},
	}
}

func buildComRowDataType() hybridqp.RowDataType {
	return hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "field1_string", Type: influxql.String},
		influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
		influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean},
		influxql.VarRef{Val: "field4_float", Type: influxql.Float})
}

func buildDstChunk() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("cpu")
	inCk1.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	inCk1.AppendTimes([]int64{1609459200000000000})

	inCk1.Column(0).AppendStringValues([]string{"test-test-test-test-0"})
	inCk1.Column(0).AppendNotNil()

	inCk1.Column(1).AppendIntegerValues([]int64{1})
	inCk1.Column(1).AppendNotNil()

	inCk1.Column(2).AppendBooleanValues([]bool{true})
	inCk1.Column(2).AppendNotNil()

	inCk1.Column(3).AppendFloatValues([]float64{1.1})
	inCk1.Column(3).AppendNotNil()

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstChunkDesc() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("cpu")
	inCk1.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	inCk1.AppendTimes([]int64{1609460838300000000})

	inCk1.Column(0).AppendStringValues([]string{"test-test-test-test-0"})
	inCk1.Column(0).AppendNotNil()

	inCk1.Column(1).AppendIntegerValues([]int64{1})
	inCk1.Column(1).AppendNotNil()

	inCk1.Column(2).AppendBooleanValues([]bool{true})
	inCk1.Column(2).AppendNotNil()

	inCk1.Column(3).AppendFloatValues([]float64{1.1})
	inCk1.Column(3).AppendNotNil()

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildAggReaderOps() []hybridqp.ExprOptions {
	return []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("field2_int")}},
			Ref:  influxql.VarRef{Val: `field2_int`, Type: influxql.Integer},
		},
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("field4_float")}},
			Ref:  influxql.VarRef{Val: `field4_float`, Type: influxql.Float},
		},
	}
}

func buildAggRowDataType() hybridqp.RowDataType {
	return hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
		influxql.VarRef{Val: "field4_float", Type: influxql.Float})
}

func buildAggChunk() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildAggRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("cpu")
	inCk1.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	inCk1.AppendTimes([]int64{1609459200000000000})

	inCk1.Column(0).AppendIntegerValues([]int64{1})
	inCk1.Column(0).AppendNotNil()

	inCk1.Column(1).AppendFloatValues([]float64{1.1})
	inCk1.Column(1).AppendNotNil()

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildAggGroupByChunk() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildAggRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("cpu")
	inCk1.AppendTagsAndIndex(*executor.NewChunkTagsByTagKVs([]string{"field1_string"}, []string{"test-test-test-test-0"}), 0)
	inCk1.AppendTimes([]int64{1609459200000000000})

	inCk1.Column(0).AppendIntegerValues([]int64{1})
	inCk1.Column(0).AppendNotNil()

	inCk1.Column(1).AppendFloatValues([]float64{1.1})
	inCk1.Column(1).AppendNotNil()

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildCountChunk() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "count_time", Type: influxql.Integer})
	b := executor.NewChunkBuilder(rowDataType)
	inCk1 := b.NewChunk("cpu")
	inCk1.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	inCk1.AppendTimes([]int64{0})
	inCk1.Column(0).AppendIntegerValues([]int64{16384})
	inCk1.Column(0).AppendNotNil()
	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func buildDstChunkTimeFilter() []executor.Chunk {
	dstChunks := make([]executor.Chunk, 0, 1)
	rowDataType := buildComRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	inCk1 := b.NewChunk("cpu")
	inCk1.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	inCk1.AppendTimes([]int64{1609459200000000000, 1609459200100000000})

	inCk1.Column(0).AppendStringValues([]string{"test-test-test-test-0", "test-test-test-test-0"})
	inCk1.Column(0).AppendNilsV2(true, true)

	inCk1.Column(1).AppendIntegerValues([]int64{1, 1})
	inCk1.Column(1).AppendNilsV2(true, true)

	inCk1.Column(2).AppendBooleanValues([]bool{true, true})
	inCk1.Column(2).AppendNilsV2(true, true)

	inCk1.Column(3).AppendFloatValues([]float64{1.1, 1.1})
	inCk1.Column(3).AppendNilsV2(true, true)

	dstChunks = append(dstChunks, inCk1)
	return dstChunks
}

func createMeasurement() *influxql.Measurement {
	return &influxql.Measurement{Name: "students"}
}

func createSortQuerySchema() *executor.QuerySchema {
	var fields influxql.Fields
	m := createMeasurement()
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}}
	fields = append(fields, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:   "f1",
			Type:  influxql.Integer,
			Alias: "",
		},
	})
	var names []string
	names = append(names, "f1")
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())
	return schema
}

func TestColumnStoreReaderFunctions(t *testing.T) {
	schema := createSortQuerySchema()
	node := executor.NewLogicalSeries(schema)
	readerPlan := executor.NewLogicalColumnStoreReader(node, schema)

	creator := &ColumnStoreReaderCreator{}
	if _, err := creator.Create(readerPlan, &query.ProcessorOptions{}); err != nil {
		assert2.Equal(t, err.Error(), "")
	}
	executor.RegistryReaderCreator(&executor.LogicalColumnStoreReader{}, &ColumnStoreReaderCreator{})
	if _, err := creator.CreateReader(readerPlan, executor.NewShardsFragments()); err != nil {
		assert2.Equal(t, err.Error(), "")
	}
	if _, err := creator.CreateReader(readerPlan, nil); err != nil {
		assert2.Equal(t, strings.Contains(err.Error(), "unsupported index info type for the colstore"), true)
	}

	reader := NewColumnStoreReader(readerPlan, nil)
	assert2.Equal(t, reader.Name(), "ColumnStoreReader")
	assert2.Equal(t, len(reader.Explain()), 1)
	reader.Abort()
	assert2.Equal(t, reader.IsSink(), true)
	assert2.Equal(t, len(reader.GetOutputs()), 1)
	assert2.Equal(t, len(reader.GetInputs()), 0)
	assert2.Equal(t, reader.GetOutputNumber(nil), 0)
	assert2.Equal(t, reader.GetInputNumber(nil), 0)
	reader.sendChunk(nil)
	assert2.Equal(t, *(reader.closedSignal), true)
}

type MockStoreEngine struct {
	shard Shard
}

func NewMockStoreEngine() *MockStoreEngine {
	return &MockStoreEngine{}
}

func (s *MockStoreEngine) ReportLoad() {
}

func (s *MockStoreEngine) CreateLogicPlan(ctx context.Context, db string, ptId uint32, shardID uint64, sources influxql.Sources, schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	return s.shard.CreateLogicalPlan(ctx, sources, schema.(*executor.QuerySchema))
}

func (s *MockStoreEngine) GetIndexInfo(db string, ptId uint32, shardID uint64, schema hybridqp.Catalog) (interface{}, error) {
	return s.shard.GetIndexInfo(schema.(*executor.QuerySchema))
}

func (s *MockStoreEngine) ScanWithSparseIndex(ctx context.Context, _ string, _ uint32, _ []uint64, schema hybridqp.Catalog) (hybridqp.IShardsFragments, error) {
	if s.shard == nil {
		return nil, nil
	}
	shardFrags := executor.NewShardsFragments()
	filesFrags, err := s.shard.ScanWithSparseIndex(ctx, schema.(*executor.QuerySchema), func(int64) error { return nil })
	if err != nil {
		return nil, err
	}
	if filesFrags == nil {
		return shardFrags, nil
	}
	shardFrags[s.shard.GetID()] = filesFrags
	return shardFrags, nil
}

func (s *MockStoreEngine) RowCount(_ string, _ uint32, _ []uint64, schema hybridqp.Catalog) (int64, error) {
	rowCount, err := s.shard.RowCount(schema.(*executor.QuerySchema))
	if err != nil {
		return rowCount, err
	}
	return rowCount, nil
}

func (s *MockStoreEngine) UnrefEngineDbPt(_ string, _ uint32) {

}

func (s *MockStoreEngine) GetShardDownSampleLevel(_ string, _ uint32, _ uint64) int {
	return 0
}

func (s *MockStoreEngine) SetShard(shard Shard) {
	s.shard = shard
}

func buildIndexScanExtraInfo(engine hybridqp.StoreEngine, db string, ptId uint32, shardIDs []uint64, shardVersions []uint32, shardPath string) *executor.IndexScanExtraInfo {
	info := &executor.IndexScanExtraInfo{
		Store: engine,
		Req: &executor.RemoteQuery{
			Database: db,
			PtID:     ptId,
			ShardIDs: shardIDs,
		},
		PtQuery: &executor.PtQuery{PtID: ptId, ShardInfos: []executor.ShardInfo{{ID: shardIDs[0], Path: shardPath, Version: shardVersions[0]}}},
	}
	return info
}

func TestColumnStoreReader(t *testing.T) {
	testDir := t.TempDir()
	db := "db0"
	rp := "rp0"
	ptId := uint32(1)
	executor.RegistryTransformCreator(&executor.LogicalColumnStoreReader{}, &ColumnStoreReaderCreator{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 1, 16384, time.Millisecond*100, startTime, true, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	var err error
	// start write data to the shard.
	sh, err := createShard(db, rp, ptId, testDir, config.COLUMNSTORE)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = sh.Close(); err != nil {
			t.Fatal(err)
		}
		if err = sh.indexBuilder.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	sh.SetMstInfo(NewMockColumnStoreMstInfo())
	sh.SetClient(&MockMetaClient{
		mstInfo: []*meta2.MeasurementInfo{NewMockColumnStoreMstInfo()},
	})
	if err = sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	sh.waitSnapshot()

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

	for _, tt := range []struct {
		skip      bool
		exec      bool
		scanErr   bool
		name      string
		q         string
		tr        util.TimeRange
		out       hybridqp.RowDataType
		frags     executor.ShardsFragments
		expected  []executor.Chunk
		readerOps []hybridqp.ExprOptions
		fields    map[string]influxql.DataType
		expect    func(t *testing.T, outChunks, dstChunks []executor.Chunk) bool
	}{
		{
			name:      "select * from cpu limit 1",
			q:         `select field1_string,field2_int,field3_bool,field4_float from cpu limit 1`,
			tr:        util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  buildDstChunk(),
			expect:    testEqualChunks,
		},
		{
			name:      "select * from cpu orderBy desc limit 1",
			q:         `select field1_string,field2_int,field3_bool,field4_float from cpu order by time desc limit 1`,
			tr:        util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  buildDstChunkDesc(),
			expect:    testEqualChunks,
			exec:      false,
		},
		{
			name: "select * from cpu where timeFilter groupBy",
			q: `select field1_string,field2_int,field3_bool,field4_float from cpu where
				time >= 1609459200000000000 and time <= 1609459201000000000`,
			tr:        util.TimeRange{Min: 1609459200000000000, Max: 1609459200100000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  buildDstChunkTimeFilter(),
			expect:    testEqualChunks,
		},
		{
			name: "select * from cpu where timeFilter and fieldFilter",
			q: `select field1_string,field2_int,field3_bool,field4_float from cpu where
		time = 1609459200000000000 and field2_int = 1`,
			tr:        util.TimeRange{Min: 1609459200000000000, Max: 1609459200000000000},
			out:       buildComRowDataType(),
			readerOps: buildComReaderOps(),
			fields:    fields,
			expected:  buildDstChunk(),
			expect:    testEqualChunks,
		},
		{
			name: "select sum(filed) from cpu where timeFilter and fieldFilter",
			q: `select sum(field2_int),sum(field4_float) from cpu where
		time >= 1609459200000000000`,
			tr:        util.TimeRange{Min: 1609459200000000000, Max: 1609459200000000000},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  buildAggChunk(),
			expect:    testEqualChunks,
		},
		{
			name: "select sum(filed) from cpu where timeFilter and fieldFilter limit 1",
			q: `select sum(field2_int),sum(field4_float) from cpu where
		time >= 1609459200000000000 limit 1`,
			tr:        util.TimeRange{Min: 1609459200000000000, Max: 1609459200000000000},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  buildAggChunk(),
			expect:    testEqualChunks,
		},
		{
			name: "select sum(field) from cpu where timeFilter and fieldFilter groupBy",
			q: `select sum(field2_int),sum(field4_float) from cpu where
		time >= 1609459200000000000 group by field1_string`,
			tr:        util.TimeRange{Min: 1609459200000000000, Max: 1609459200000000000},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  buildAggGroupByChunk(),
			expect:    testEqualChunks,
		},
		{
			name:      "select count(time) from cpu",
			q:         `select count(time) from cpu`,
			tr:        util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  buildCountChunk(),
			expect:    testEqualChunk1,
		},
		{
			name:      "select sum(field) from cpu where timeFilter and fieldFilter",
			q:         `select sum(field2_int) from cpu where time >= 1609460838300000000 and field1_string = 'test-test-test-test-1'`,
			tr:        util.TimeRange{Min: 1609460838300000000, Max: influxql.MaxTime},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  nil,
			expect:    testEqualChunk1,
		},
		{
			name:      "select sum(field) from cpu where timeFilter and fieldFilter group by",
			q:         `select sum(field2_int) from cpu where time >= 1609460838300000000 and field1_string = 'test-test-test-test-1' group by field1_string, tagKey1`,
			tr:        util.TimeRange{Min: 1609459200000000000, Max: influxql.MaxTime},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  nil,
			expect:    testEqualChunk1,
		},
		{
			name:      "select sum(field) from cpu where timeFilter and fieldFilter",
			q:         `select sum(field2_int) from cpu where time >= 1609460838300000000 and field1_string = 'test-test-test-test-1'`,
			tr:        util.TimeRange{Min: 1609460838300000000, Max: influxql.MaxTime},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  nil,
			expect:    testEqualChunk1,
			scanErr:   true, // Initialize an index scan err
		},
		{
			name:      "select sum(field2_int) from cpu",
			q:         `select sum(field2_int) from cpu`,
			tr:        util.TimeRange{Min: 1609459200000000000, Max: 1609459200000000000},
			out:       buildAggRowDataType(),
			readerOps: buildAggReaderOps(),
			fields:    fields,
			expected:  nil,
			expect:    testEqualChunk1,
			exec:      true, // Initialize an empty table store.
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			if tt.exec {
				sh.immTables = immutable.NewTableStore(immutable.GetDir(config.COLUMNSTORE, testDir), sh.lock, &sh.tier, false, immutable.GetColStoreConfig())
			}
			if tt.scanErr {
				sh.pkIndexReader = sparseindex.NewPKIndexReader(0, 0, 0)
			}
			// step1: parse stmt and opt
			ctx := context.Background()

			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			opt.Condition = stmt.Condition
			opt.MaxParallel = 1
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

			// step2: build the store executor
			info := buildIndexScanExtraInfo(storeEngine, db, ptId, []uint64{sh.GetID()}, []uint32{logstore.CurrentLogTokenizerVersion}, "")
			reader := executor.NewLogicalColumnStoreReader(nil, querySchema)
			var input hybridqp.QueryNode
			if querySchema.HasCall() {
				agg := executor.NewLogicalHashAgg(reader, querySchema, executor.READER_EXCHANGE, nil)
				input = executor.NewLogicalHashAgg(agg, querySchema, executor.SHARD_EXCHANGE, nil)
			} else {
				input = executor.NewLogicalHashMerge(reader, querySchema, executor.READER_EXCHANGE, nil)
			}
			indexScan := executor.NewLogicalSparseIndexScan(input, querySchema)
			executor.ReWriteArgs(indexScan, false)
			scan := executor.NewSparseIndexScanTransform(tt.out, indexScan.Children()[0], indexScan.RowExprOptions(), info, querySchema)
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

			if !tt.expect(t, sink.Chunks, tt.expected) {
				t.Errorf("`%s` failed", tt.name)
			}
		})
	}
}

func TestColumnStoreReaderTranRecToChunk(t *testing.T) {
	schema := createQuerySchemaForTranRec()
	readerPlan := executor.NewLogicalColumnStoreReader(nil, schema)
	reader := NewColumnStoreReader(readerPlan, nil)
	if err := reader.initReadCursor(); err != nil {
		t.Fatalf("ColumnStoreReader initReadCursor, err: %+v", err)
	}
	if err := reader.initSchemaAndPool(); err != nil {
		t.Fatalf("ColumnStoreReader initSchemaAndPool, err: %+v", err)
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
}
