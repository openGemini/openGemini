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
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func Test_fileLoopCursor_SinkPlan(t *testing.T) {
	ctx := &idKeyCursorContext{
		schema: record.Schemas{{Name: "value", Type: influx.Field_Type_Int}, {Name: "time", Type: influx.Field_Type_Int}},
	}
	tagSet := tsi.NewTagSetInfo()
	cursor := NewFileLoopCursor(ctx, nil, nil, tagSet, 0, 1, nil)

	opt := query.ProcessorOptions{}
	fields := []*influxql.Field{
		{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	}
	schema := executor.NewQuerySchema(fields, []string{"id", "value", "alive", "name"}, &opt, nil)
	series := executor.NewLogicalSeries(schema)
	plan := executor.NewLogicalReader(series, schema)
	cursor.SinkPlan(plan)
	_, _, _ = cursor.Next()
	require.Equal(t, 1, len(cursor.ridIdx))
	if _, ok := cursor.ridIdx[0]; !ok {
		t.Fatal("column 0 should rid out")
	}
	require.Equal(t, true, cursor.isCutSchema)
}

func genRowRec(schema []record.Field, intValBitmap []int, intVal []int64, floatValBitmap []int, floatVal []float64,
	stringValBitmap []int, stringVal []string, booleanValBitmap []int, boolVal []bool, time []int64) *record.Record {
	var rec record.Record

	rec.Schema = append(rec.Schema, schema...)
	for i, v := range rec.Schema {
		var colVal record.ColVal
		if v.Type == influx.Field_Type_Int {
			if i == len(rec.Schema)-1 {
				// time col
				for index := range time {
					colVal.AppendInteger(time[index])
				}
			} else {
				for index := range time {
					if intValBitmap[index] == 1 {
						colVal.AppendInteger(intVal[index])
					} else {
						colVal.AppendIntegerNull()
					}
				}
			}
		} else if v.Type == influx.Field_Type_Boolean {
			for index := range time {
				if booleanValBitmap[index] == 1 {
					colVal.AppendBoolean(boolVal[index])
				} else {
					colVal.AppendBooleanNull()
				}
			}
		} else if v.Type == influx.Field_Type_Float {
			for index := range time {
				if floatValBitmap[index] == 1 {
					colVal.AppendFloat(floatVal[index])
				} else {
					colVal.AppendFloatNull()
				}
			}
		} else if v.Type == influx.Field_Type_String {
			for index := range time {
				if stringValBitmap[index] == 1 {
					colVal.AppendString(stringVal[index])
				} else {
					colVal.AppendStringNull()
				}
			}
		} else {
			panic("error type")
		}
		rec.ColVals = append(rec.ColVals, colVal)
	}
	rec.RecMeta = &record.RecMeta{}
	return &rec
}

func TestFilterRecInMemTable(t *testing.T) {
	sInfo := &seriesInfo{
		sid:  1,
		key:  []byte{'a', 'b'},
		tags: influx.PointTags{{Key: "a", Value: "b", IsArray: false}},
	}
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
		[]int64{57, 56, 55, 37, 3, 2, 1})
	s := &fileLoopCursor{}
	s.ctx = &idKeyCursorContext{
		tr: util.TimeRange{Max: 100},
	}
	s.ctx.filterOption.FiltersMap = map[string]*influxql.FilterMapValue{}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: true}, nil)
	s.mergeRecIters = make(map[uint64][]*SeriesIter, 1)
	s.mergeRecIters[sInfo.sid] = nil
	s.tagSetInfo = &tsi.TagSetInfo{Filters: []influxql.Expr{&influxql.VarRef{Val: "a"}}}
	s.ridIdx = map[int]struct{}{}
	s.recPool = record.NewCircularRecordPool(record.NewRecordPool(record.UnknownPool), 4, schema, false)
	s.FilesInfoPool = NewSeriesInfoPool(fileInfoNum)
	re, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range re.Times() {
		if re.Times()[i] != rec.Times()[i] {
			t.Fatal()
		}
	}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: false}, nil)
	r, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range rec.Times() {
		if rec.Times()[i] != r.Times()[i] {
			t.Fatal()
		}
	}
	s.isCutSchema = true
	r2, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range rec.Times() {
		if rec.Times()[i] != r2.Times()[i] {
			t.Fatal()
		}
	}
	rec2 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 1}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 1}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{157, 156, 155, 137, 113, 112, 111})
	s.FilterRecInMemTable(rec2, s.tagSetInfo.Filters[0], sInfo, nil)
	rec3 := genRowRec(schema,
		[]int{1, 1, 1, 1, 0, 1, 0}, []int64{17, 16, 15, 14, 0, 13, 12},
		[]int{0, 1, 1, 0, 1, 0, 0}, []float64{0, 5.3, 4.3, 0, 3.3, 0, 2.3},
		[]int{1, 0, 1, 0, 0, 1, 0}, []string{"test1", "", "world1", "", "", "hello1", ""},
		[]int{0, 1, 1, 0, 1, 0, 0}, []bool{false, false, true, false, true, false, false},
		[]int64{157, 156, 155, 137, 113, 112, 99})
	s.FilterRecInMemTable(rec3, s.tagSetInfo.Filters[0], sInfo, nil)
}

func TestInitOutOfOrderItersByRecordASC(t *testing.T) {
	sInfo := &seriesInfo{
		sid:  1,
		key:  []byte{'a', 'b'},
		tags: influx.PointTags{{Key: "a", Value: "b", IsArray: false}},
	}
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
		[]int64{57, 56, 55, 37, 3, 2, 1})
	s := &fileLoopCursor{}
	s.ctx = &idKeyCursorContext{
		tr:   util.TimeRange{Max: 100},
		decs: &immutable.ReadContext{},
	}
	s.ctx.filterOption.FiltersMap = map[string]*influxql.FilterMapValue{}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: true}, nil)
	s.mergeRecIters = make(map[uint64][]*SeriesIter, 1)
	s.mergeRecIters[sInfo.sid] = make([]*SeriesIter, 0)
	s.mergeRecIters[sInfo.sid] = append(s.mergeRecIters[sInfo.sid], &SeriesIter{iter: &recordIter{record: rec, rowCnt: 1}})
	s.tagSetInfo = &tsi.TagSetInfo{Filters: []influxql.Expr{&influxql.VarRef{Val: "a"}}}
	s.ridIdx = map[int]struct{}{}
	s.recPool = record.NewCircularRecordPool(record.NewRecordPool(record.UnknownPool), 4, schema, false)
	s.FilesInfoPool = NewSeriesInfoPool(fileInfoNum)
	re, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range re.Times() {
		if re.Times()[i] != rec.Times()[i] {
			t.Fatal()
		}
	}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: false}, nil)
	data := &DataBlockInfo{sInfo: sInfo, record: rec, sid: 1, index: 1, tagSetIndex: 1}
	s.initOutOfOrderItersByRecord(data, 10, 1, 0)
	for i := range rec.Times() {
		if rec.Times()[i] != s.mergeRecIters[sInfo.sid][0].iter.record.Time(i) {
			t.Fatal()
		}
	}
}

func TestInitOutOfOrderItersByRecordDSC(t *testing.T) {
	sInfo := &seriesInfo{
		sid:  1,
		key:  []byte{'a', 'b'},
		tags: influx.PointTags{{Key: "a", Value: "b", IsArray: false}},
	}
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
		[]int64{57, 56, 55, 37, 3, 2, 1})
	s := &fileLoopCursor{}
	s.ctx = &idKeyCursorContext{
		tr:   util.TimeRange{Max: 100},
		decs: &immutable.ReadContext{Ascending: true},
	}
	s.ctx.filterOption.FiltersMap = map[string]*influxql.FilterMapValue{}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: true}, nil)
	s.mergeRecIters = make(map[uint64][]*SeriesIter, 1)
	s.mergeRecIters[sInfo.sid] = make([]*SeriesIter, 0)
	s.mergeRecIters[sInfo.sid] = append(s.mergeRecIters[sInfo.sid], &SeriesIter{iter: &recordIter{record: rec, rowCnt: 1}})
	s.tagSetInfo = &tsi.TagSetInfo{Filters: []influxql.Expr{&influxql.VarRef{Val: "a"}}}
	s.ridIdx = map[int]struct{}{}
	s.recPool = record.NewCircularRecordPool(record.NewRecordPool(record.UnknownPool), 4, schema, false)
	s.FilesInfoPool = NewSeriesInfoPool(fileInfoNum)
	re, _ := s.FilterRecInMemTable(rec, s.tagSetInfo.Filters[0], sInfo, nil)
	for i := range re.Times() {
		if re.Times()[i] != rec.Times()[i] {
			t.Fatal()
		}
	}
	s.schema = executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Ascending: false}, nil)
	data := &DataBlockInfo{sInfo: sInfo, record: rec, sid: 1, index: 1, tagSetIndex: 1}
	s.initOutOfOrderItersByRecord(data, 10, 1, 0)
	for i := range rec.Times() {
		if rec.Times()[i] != s.mergeRecIters[sInfo.sid][0].iter.record.Time(i) {
			t.Fatal()
		}
	}
}

func testEqualChunkNumV1(out, expect int) bool {
	return true
}

func Test_Query_Abort(t *testing.T) {
	testDir := t.TempDir()
	db, rp, mst, ptId := "db0", "rp0", "mst", uint32(1)
	msNames := []string{mst}
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 1, 16384, time.Millisecond*100, startTime, true, false, true)

	fields := map[string]influxql.DataType{
		"field1_string": influxql.String,
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
	}

	var err error
	// start write data to the shard.
	sh, err := createShard(db, rp, ptId, testDir, config.TSSTORE)
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

	if err = sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	sh.waitSnapshot()

	// build the shard group
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	// build the storage engine
	storeEngine := NewMockStoreEngine()
	storeEngine.SetShard(sh)
	enableStates := []bool{true, false}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			skip      bool
			exec      bool
			scanErr   bool
			aggQuery  bool
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
				name:      "select * from db0.rp0.mst",
				q:         `select field4_float,field2_int,field3_bool,field1_string from db0.rp0.mst`,
				tr:        util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				out:       buildComRowDataType(),
				readerOps: buildComReaderOps(),
				fields:    fields,
				expected:  0,
			},
			{
				name:      "select sum(field4_float) from db0.rp0.mst",
				q:         `select sum(field4_float) from db0.rp0.mst`,
				tr:        util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				out:       buildComRowDataType(),
				readerOps: buildComReaderOps(),
				fields:    fields,
				expected:  0,
				aggQuery:  true,
			},
			{
				name:      "select sum(field4_float) from db0.rp0.mst group by time(1d)",
				q:         `select sum(field4_float) from db0.rp0.mst group by time(1d)`,
				tr:        util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				out:       buildComRowDataType(),
				readerOps: buildComReaderOps(),
				fields:    fields,
				aggQuery:  true,
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
					EngineType:      config.TSSTORE},
				}
				opt.Name = mst
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				opt.Condition = stmt.Condition
				opt.MaxParallel = 1
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				// step2: build the store executor
				info := buildIndexScanExtraInfo(storeEngine, db, ptId, []uint64{sh.GetID()}, []uint32{logstore.CurrentLogTokenizerVersion}, filepath.Join(sh.dataPath, "tssp"))
				info.Req.Opt.Sources = source
				var indexScan *executor.LogicalIndexScan
				if tt.aggQuery {
					series := executor.NewLogicalSeries(querySchema)
					aggr := executor.NewLogicalAggregate(series, querySchema)
					aggTagSet := executor.NewLogicalTagSetAggregate(aggr, querySchema)
					reader := executor.NewLogicalReader(aggTagSet, querySchema)
					indexScan = executor.NewLogicalIndexScan(reader, querySchema)
				} else {
					series := executor.NewLogicalSeries(querySchema)
					exchange := executor.NewLogicalExchange(series, executor.SERIES_EXCHANGE, nil, querySchema)
					reader := executor.NewLogicalReader(exchange, querySchema)
					indexScan = executor.NewLogicalIndexScan(reader, querySchema)
				}
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
				if v {
					scan.Abort()
					tt.expect = testEqualChunkNum
				} else {
					tt.expect = testEqualChunkNumV1
					go func() {
						time.Sleep(1 * time.Millisecond)
						scan.Abort()
					}()
				}
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
}
