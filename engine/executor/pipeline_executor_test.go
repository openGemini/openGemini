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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	qry "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

// Second represents a helper for type converting durations.
const Second = int64(time.Second)

func MockNewExecutorBuilder() (hybridqp.PipelineExecutorBuilder, *executor.QuerySchema, *executor.StoreExchangeTraits) {
	sql := "SELECT v1,u1 FROM mst1 limit 10"
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	query, err := yaccParser.GetQuery()
	if err != nil {
		return nil, nil, nil
	}
	stmt := query.Statements[0]

	stmt, err = qry.RewriteStatement(stmt)
	if err != nil {
		return nil, nil, nil
	}
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		return nil, nil, nil
	}

	selectStmt.OmitTime = true
	msts := MeasurementsFromSelectStmt(selectStmt)
	mapShard2Reader := make(map[uint64][][]interface{})
	for i := range msts {
		mapShard2Reader[uint64(i)] = [][]interface{}{nil}
	}

	traits := executor.NewStoreExchangeTraits(nil, mapShard2Reader)
	schema := executor.NewQuerySchemaWithJoinCase(selectStmt.Fields, []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}}, selectStmt.ColumnNames(), createQuerySchema().Options(),
		selectStmt.JoinSource, nil, selectStmt.SortFields)
	schema.SetFill(influxql.NoFill)
	var executorBuilder *executor.ExecutorBuilder = executor.NewMocStoreExecutorBuilder(traits, nil, nil, 0)
	return executorBuilder, schema, traits
}
func buildRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "id", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)

	return rowDataType
}
func buildSubQueryRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "id", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
		influxql.VarRef{Val: "value", Type: influxql.Float},
		influxql.VarRef{Val: "host", Type: influxql.Tag},
	)

	return rowDataType
}
func buildRowDataType1() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Float},
	)

	return rowDataType
}

func buildAnotherRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "id1", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)

	return rowDataType
}

func buildChunk() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("schema_chunk")

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "vergilA", "danteA", "martino"})
	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4, 1.5})

	return chunk
}

func TestConnectPort(t *testing.T) {
	chunk := buildChunk()

	source := executor.NewSourceFromSingleChunk(buildRowDataType(), chunk)
	sink := executor.NewNilSink(buildAnotherRowDataType())

	if err := executor.Connect(source.Output, sink.Input); err == nil {
		t.Error("a error must occur when connect ports with different schema")
	}
}

func TestConnectPortWithDiffSchema(t *testing.T) {
	chunk := buildChunk()

	source := executor.NewSourceFromSingleChunk(buildRowDataType(), chunk)
	sink := executor.NewNilSink(buildRowDataType())

	executor.Connect(source.Output, sink.Input)

	if source.Output.State == nil {
		t.Error("output of source can't be nil after connect")
	}

	if sink.Input.State == nil {
		t.Error("input of sink can't be nil after connect")
	}

	if source.Output.State != sink.Input.State {
		t.Error("output of source must be equal to input of sink")
	}

	if source.Output.ConnectionId() != sink.Input.ConnectionId() {
		t.Error("id of output of source must be equal to id of input of sink")
	}
}

func TestDAG(t *testing.T) {
	chunk := buildChunk()

	source := executor.NewSourceFromSingleChunk(buildRowDataType(), chunk)
	sink := executor.NewNilSink(buildRowDataType())

	executor.Connect(source.Output, sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.Size() != 2 {
		t.Errorf("there are must be %d vertex in dag, but %d", 2, dag.Size())
	}

	if dag.Path() != 1 {
		t.Errorf("there are must be %d vertex in dag, but %d", 1, dag.Path())
	}

	if len(dag.SinkVertexs()) != 1 {
		t.Errorf("len of sink vertex must be %d, but %d", 1, dag.SinkVertexs())
	}

	if len(dag.SourceVertexs()) != 1 {
		t.Errorf("len of source vertex must be %d, but %d", 1, dag.SourceVertexs())
	}

	if len(dag.OrphanVertexs()) != 0 {
		t.Errorf("len of orphan vertex must be %d, but %d", 0, dag.OrphanVertexs())
	}

	if dag.CyclicGraph() {
		t.Error("this must be a acyclic graph")
	}
}

func TestCyclicDAG(t *testing.T) {
	chunk := buildChunk()

	source := executor.NewSourceFromSingleChunk(buildRowDataType(), chunk)
	trans1 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType(), buildRowDataType(), buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType(), buildRowDataType()})
	trans2 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType(), buildRowDataType(), buildRowDataType(), buildRowDataType()})
	trans3 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{})
	trans4 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{})
	trans5 := executor.NewNilTransform([]hybridqp.RowDataType{}, []hybridqp.RowDataType{})
	sink := executor.NewNilSink(buildRowDataType())

	executor.Connect(source.Output, trans1.Inputs[0])
	executor.Connect(trans1.Outputs[0], trans2.Inputs[0])
	executor.Connect(trans2.Outputs[0], trans1.Inputs[1])
	executor.Connect(trans2.Outputs[1], trans1.Inputs[2])
	executor.Connect(trans1.Outputs[1], trans3.Inputs[0])
	executor.Connect(trans2.Outputs[2], trans4.Inputs[0])
	executor.Connect(trans2.Outputs[3], sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, trans2)
	processors = append(processors, trans3)
	processors = append(processors, trans4)
	processors = append(processors, trans5)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.Size() != 7 {
		t.Errorf("there are must be %d vertex in dag, but %d", 7, dag.Size())
	}

	if dag.Path() != 7 {
		t.Errorf("there are must be %d edge in dag, but %d", 7, dag.Path())
	}

	if len(dag.SinkVertexs()) != 3 {
		t.Errorf("len of sink vertex must be %d, but %d", 3, len(dag.SinkVertexs()))
	}

	if len(dag.SourceVertexs()) != 1 {
		t.Errorf("len of source vertex must be %d, but %d", 1, len(dag.SourceVertexs()))
	}

	if len(dag.OrphanVertexs()) != 1 {
		t.Errorf("len of orphan vertex must be %d, but %d", 1, dag.OrphanVertexs())
	}

	if dag.CyclicGraph() == false {
		t.Error("this must be a cyclic graph")
	}
}

func TestExecutePipeline(t *testing.T) {
	chunk := buildChunk()

	source := executor.NewSourceFromSingleChunk(buildRowDataType(), chunk)
	sink := executor.NewNilSink(buildRowDataType())

	executor.Connect(source.Output, sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, sink)

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestAbortPipeline(t *testing.T) {
	source := executor.NewCancelOnlySource(buildRowDataType())
	sink := executor.NewNilSink(buildRowDataType())

	executor.Connect(source.Output, sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, sink)

	executor := executor.NewPipelineExecutor(processors)

	go func() {
		<-time.After(1 * time.Second)
		executor.Crash()
	}()

	executor.Execute(context.Background())
	executor.Release()
}

func TestPipelineByFunction(t *testing.T) {
	chunk := buildChunk()

	expectNames := []string{"id", "name", "value"}

	source := executor.NewSourceFromSingleChunk(buildRowDataType(), chunk)
	sink := executor.NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			if chunk.RowDataType().Field(i).Name() != expectNames[i] {
				t.Errorf("column at %d is %s, but expect %s", i, chunk.RowDataType().Field(i).Name(), expectNames[i])
			}
		}
		return nil
	})

	executor.Connect(source.Output, sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, sink)

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestPipelineWithTransformByFunction(t *testing.T) {
	chunk := buildChunk()

	expectNames := []string{"id", "name", "value"}

	source := executor.NewSourceFromSingleChunk(buildRowDataType(), chunk)
	trans1 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType(), buildRowDataType()})
	trans2 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()})
	trans3 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()})
	trans4 := executor.NewNilTransform([]hybridqp.RowDataType{buildRowDataType(), buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()})
	sink := executor.NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			if chunk.RowDataType().Field(i).Name() != expectNames[i] {
				t.Errorf("column at %d is %s, but expect %s", i, chunk.RowDataType().Field(i).Name(), expectNames[i])
			}
		}
		return nil
	})

	executor.Connect(source.Output, trans1.Inputs[0])
	executor.Connect(trans1.Outputs[0], trans2.Inputs[0])
	executor.Connect(trans1.Outputs[1], trans3.Inputs[0])
	executor.Connect(trans2.Outputs[0], trans4.Inputs[0])
	executor.Connect(trans3.Outputs[0], trans4.Inputs[1])
	executor.Connect(trans4.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, trans2)
	processors = append(processors, trans3)
	processors = append(processors, trans4)
	processors = append(processors, sink)

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestMergeHelper_Ascending(t *testing.T) {
	chunk1 := BuildChunk1()
	chunk2 := BuildChunk2()
	chunk3 := BuildChunk3()
	chunk4 := BuildChunk4()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 41, 42, 24, 25}
	expectTagIndex := []int{0, 10, 15}
	expectIntervalIndex := []int{0, 5, 8, 10, 13, 15}
	expectColumnValues1 := []int64{1, 2, 3, 4, 5, 2, 5, 8, 19, 21, 31, 33, 7, 8, 12}
	expectColumnTimes1 := []int64{1, 2, 3, 4, 5, 1, 2, 3, 2, 3, 4, 5, 1, 2, 5}
	expectColumnValues2 := []string{"tomA", "jerryA", "danteA", "martino", "tomB", "vergilB",
		"tomC", "jerryC", "vergilC", "danteC", "martino", "tomD", "jerryD", "danteB", "martino"}
	expectColumnTimes2 := []int64{1, 2, 4, 5, 1, 3, 1, 2, 3, 4, 5, 4, 5}
	expectColumnValues3 := []float64{1.1, 1.2, 1.3, 1.4, 2.2, 2.3, 2.4, 3.2, 3.3, 3.4, 3.5, 3.6, 4.2, 4.3, 2.5, 2.6}
	expectColumnTimes3 := []int64{1, 2, 3, 4, 1, 2, 3, 1, 2, 4, 5}

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1, chunk4})
	source2 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk3})
	trans := executor.NewMergeTransform([]hybridqp.RowDataType{buildRowDataType(), buildRowDataType(), buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, nil, schema)

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 16})
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 1, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		assert.Equal(t, chunk.Column(2).NilsV2().GetArray(), []uint16{0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(source3.Output, trans.Inputs[2])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestMergeHelper_Descending(t *testing.T) {
	chunk1 := ReverseChunk(BuildChunk1())
	chunk2 := ReverseChunk(BuildChunk2())
	chunk3 := ReverseChunk(BuildChunk3())
	chunk4 := ReverseChunk(BuildChunk4())

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{25, 24, 42, 41, 35, 34, 33, 32, 31, 23, 22, 21, 15, 14, 13, 12, 11}
	expectTagIndex := ReverseIndex([]int{0, 10, 15}, len(expectTime))
	expectIntervalIndex := ReverseIndex([]int{0, 5, 8, 10, 13, 15}, len(expectTime))
	expectColumnValues1 := []int64{12, 8, 7, 33, 31, 21, 19, 8, 5, 2, 5, 4, 3, 2, 1}
	expectColumnTimes1 := []int64{5, 2, 1, 5, 4, 3, 2, 3, 2, 1, 5, 4, 3, 2, 1}
	expectColumnValues2 := []string{"martino", "danteB", "jerryD", "tomD", "martino", "danteC", "vergilC", "jerryC", "tomC", "vergilB", "tomB", "martino", "danteA", "jerryA", "tomA"}
	expectColumnTimes2 := []int64{5, 4, 5, 4, 3, 2, 1, 3, 1, 5, 4, 2, 1}
	expectColumnValues3 := []float64{2.6, 2.5, 4.3, 4.2, 3.6, 3.5, 3.4, 3.3, 3.2, 2.4, 2.3, 2.2, 1.4, 1.3, 1.2, 1.1}
	expectColumnTimes3 := []int64{5, 4, 2, 1, 3, 2, 1, 4, 3, 2, 1}

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  false,
		ChunkSize:  100,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk4, chunk1})
	source2 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk3})
	trans := executor.NewMergeTransform([]hybridqp.RowDataType{buildRowDataType(), buildRowDataType(), buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, nil, schema)

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {

			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), ReverseNilsV2([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 16}, len(expectTime)))
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), ReverseNilsV2([]uint16{0, 1, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, len(expectTime)))
		// TODO: fix me
		//assert.Equal(t, chunk.Column(2).NilsV2().ToArray(), ReverseNilsV2([]uint16{0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, len(expectTime)))
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(source3.Output, trans.Inputs[2])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSortedMergeHelper_TimeCompare_Ascending(t *testing.T) {
	chunk1 := BuildSortedChunk1()
	chunk2 := BuildSortedChunk2()
	chunk3 := BuildSortedChunk3()
	chunk4 := BuildSortedChunk4()

	expectSchemaNames := []string{"val0", "val1", "val2"}
	expectName := "mst"
	expectTime := []int64{5, 6, 11, 12, 21, 22, 23, 23, 24, 35, 33, 34, 35, 41, 42, 1, 2}
	expectTagIndex := []int{0, 10, 15}
	expectIntervalIndex := []int{0, 2, 4, 9, 10, 13, 15}
	expectColumnValues1 := []int64{1, 19, 2, 2, 5, 3, 8, 4, 5, 21, 31, 33, 7, 8, 12}
	expectColumnTimes1 := []int64{1, 2, 2, 3, 4, 5, 3, 4, 5}
	expectColumnValues2 := []string{"tomC", "tomA", "jerryC", "jerryA", "tomB", "vergilB", "danteA", "martino",
		"vergilC", "danteC", "martino", "tomD", "jerryD", "danteB", "martino"}
	expectColumnTimes2 := []int64{1, 1, 2, 2, 1, 3, 4, 5, 3, 4, 5, 4, 5}
	expectColumnValues3 := []float64{3.2, 1.1, 3.3, 1.2, 2.2, 2.3, 1.3, 2.4, 1.4, 3.4, 3.5, 3.6, 4.2, 4.3, 2.5, 2.6}
	expectColumnTimes3 := []int64{1, 1, 2, 2, 1, 2, 3, 3, 4, 3, 4, 5, 4, 5}

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	schema := executor.NewQuerySchema(createSortedMergeFields(), []string{"id", "name", "value"}, &opt, nil)

	source1 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk1, chunk4})
	source2 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk3})
	trans := executor.NewSortedMergeTransform([]hybridqp.RowDataType{buildRowDataType1(), buildRowDataType1(), buildRowDataType1()}, []hybridqp.RowDataType{buildRowDataType1()}, nil, schema)

	sink := NewSinkFromFunction(buildRowDataType1(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16})
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		assert.Equal(t, chunk.Column(2).NilsV2().GetArray(), []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16})
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(source3.Output, trans.Inputs[2])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSortedMergeHelper_TimeCompare_Descending(t *testing.T) {
	chunk1 := ReverseChunk(BuildSortedChunk1())
	chunk2 := ReverseChunk(BuildSortedChunk2())
	chunk3 := ReverseChunk(BuildSortedChunk3())
	chunk4 := ReverseChunk(BuildSortedChunk4())

	expectSchemaNames := []string{"val0", "val1", "val2"}
	expectName := "mst"
	expectTime := ReverseInt64Slice([]int64{5, 6, 11, 12, 21, 22, 23, 23, 24, 35, 33, 34, 35, 41, 42, 1, 2})
	expectTagIndex := ReverseIndex([]int{0, 10, 15}, len(expectTime))
	expectIntervalIndex := ReverseIndex([]int{0, 2, 4, 9, 10, 13, 15}, len(expectTime))
	expectColumnValues1 := ReverseInt64Slice([]int64{1, 19, 2, 2, 5, 3, 8, 4, 5, 21, 31, 33, 7, 8, 12})
	expectColumnTimes1 := ReverseInt64Slice([]int64{1, 2, 2, 3, 4, 5, 3, 4, 5})
	expectColumnValues2 := ReverseStringSlice([]string{"tomC", "tomA", "jerryC", "jerryA", "tomB", "vergilB", "danteA", "martino",
		"vergilC", "danteC", "martino", "tomD", "jerryD", "danteB", "martino"})
	expectColumnTimes2 := ReverseInt64Slice([]int64{1, 1, 2, 2, 1, 3, 4, 5, 3, 4, 5, 4, 5})
	expectColumnValues3 := ReverseFloat64Slice([]float64{3.2, 1.1, 3.3, 1.2, 2.2, 2.3, 1.3, 2.4, 1.4, 3.4, 3.5, 3.6, 4.2, 4.3, 2.5, 2.6})
	expectColumnTimes3 := ReverseInt64Slice([]int64{1, 1, 2, 2, 1, 2, 3, 3, 4, 3, 4, 5, 4, 5})

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  false,
		ChunkSize:  100,
	}
	schema := executor.NewQuerySchema(createSortedMergeFields(), []string{"id", "name", "value"}, &opt, nil)

	source1 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk4, chunk1})
	source2 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk3})
	trans := executor.NewSortedMergeTransform([]hybridqp.RowDataType{buildRowDataType1(), buildRowDataType1(), buildRowDataType1()}, []hybridqp.RowDataType{buildRowDataType1()}, nil, schema)

	sink := NewSinkFromFunction(buildRowDataType1(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), ReverseNilsV2([]uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}, len(expectTime)))
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), ReverseNilsV2([]uint16{0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, len(expectTime)))
		assert.Equal(t, chunk.Column(2).NilsV2().GetArray(), ReverseNilsV2([]uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16}, len(expectTime)))
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(source3.Output, trans.Inputs[2])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSortedMergeHelper_AuxCompare_Ascending(t *testing.T) {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Float},
		influxql.VarRef{Val: "val3", Type: influxql.Boolean},
	)

	chunk1 := BuildSortedChunk5()
	chunk2 := BuildSortedChunk6()

	expectSchemaNames := []string{"val0", "val1", "val2", "val3"}
	expectName := "mst"
	expectTime := []int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12}
	expectTagIndex := []int{0}
	expectIntervalIndex := []int{0}
	expectColumnValues1 := []int64{1, 2, 2, 2, 2, 2, 2, 2, 2}
	expectColumnTimes1 := []int64{1, 2, 2, 3, 3, 4, 5, 4, 5}
	expectColumnValues2 := []string{"tomB", "tomA", "jerryA", "jerryB", "vergil", "vergil", "zara", "zara", "zara", "zara"}
	expectColumnTimes2 := []int64{1, 2, 3, 4, 5}
	expectColumnValues3 := []float64{2.2, 1.1, 1.2, 2.3, 1.3, 2.4, 1.3, 1.3, 1.3, 1.3}
	expectColumnTimes3 := []int64{1, 2, 3, 4, 5}
	expectColumnValues4 := []bool{true, true, false, false, false, false, false, false, true, true}
	expectColumnTimes4 := []int64{1, 1, 2, 2, 3, 3, 4, 5, 4, 5}

	opt := query.ProcessorOptions{
		Ascending: true,
		ChunkSize: 100,
	}
	schema := executor.NewQuerySchema(createSortedMergeFields1(), []string{"id", "names", "value", "yield"}, &opt, nil)

	source1 := NewSourceFromSingleChunk(rowDataType, []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(rowDataType, []executor.Chunk{chunk2})
	trans := executor.NewSortedMergeTransform([]hybridqp.RowDataType{rowDataType, rowDataType}, []hybridqp.RowDataType{rowDataType}, nil, schema)

	sink := NewSinkFromFunction(rowDataType, func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)
		assert.Equal(t, chunk.Column(3).BooleanValues(), expectColumnValues4)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)
		assert.Equal(t, chunk.Column(3).ColumnTimes(), expectColumnTimes4)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9})
		assert.Equal(t, chunk.Column(1).NilCount(), 0)
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		assert.Equal(t, chunk.Column(3).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSortedMergeHelper_AuxCompare_Descending(t *testing.T) {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Float},
		influxql.VarRef{Val: "val3", Type: influxql.Boolean},
	)

	chunk1 := ReverseChunk(BuildSortedChunk5())
	chunk2 := ReverseChunk(BuildSortedChunk6())

	expectSchemaNames := []string{"val0", "val1", "val2", "val3"}
	expectName := "mst"
	expectTime := ReverseInt64Slice([]int64{12, 12, 12, 12, 12, 12, 12, 12, 12, 12})
	expectTagIndex := []int{0}
	expectIntervalIndex := []int{0}
	expectColumnValues1 := ReverseInt64Slice([]int64{1, 2, 2, 2, 2, 2, 2, 2, 2})
	expectColumnTimes1 := ReverseInt64Slice([]int64{1, 2, 2, 3, 3, 4, 5, 4, 5})
	expectColumnValues2 := ReverseStringSlice([]string{"tomB", "tomA", "jerryA", "jerryB", "vergil", "vergil", "zara", "zara", "zara", "zara"})
	expectColumnTimes2 := ReverseInt64Slice([]int64{1, 2, 3, 4, 5})
	expectColumnValues3 := ReverseFloat64Slice([]float64{2.2, 1.1, 1.2, 2.3, 1.3, 2.4, 1.3, 1.3, 1.3, 1.3})
	expectColumnTimes3 := ReverseInt64Slice([]int64{1, 2, 3, 4, 5})
	expectColumnValues4 := ReverseBooleanSlice([]bool{true, true, false, false, false, false, false, false, true, true})
	expectColumnTimes4 := ReverseInt64Slice([]int64{1, 1, 2, 2, 3, 3, 4, 5, 4, 5})

	opt := query.ProcessorOptions{
		Ascending: false,
		ChunkSize: 100,
	}
	schema := executor.NewQuerySchema(createSortedMergeFields1(), []string{"id", "name", "value", "yield"}, &opt, nil)

	source1 := NewSourceFromSingleChunk(rowDataType, []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(rowDataType, []executor.Chunk{chunk2})
	trans := executor.NewSortedMergeTransform([]hybridqp.RowDataType{rowDataType, rowDataType}, []hybridqp.RowDataType{rowDataType}, nil, schema)

	sink := NewSinkFromFunction(rowDataType, func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)
		assert.Equal(t, chunk.Column(3).BooleanValues(), expectColumnValues4)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)
		assert.Equal(t, chunk.Column(3).ColumnTimes(), expectColumnTimes4)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8})
		assert.Equal(t, chunk.Column(1).NilCount(), 0)
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		assert.Equal(t, chunk.Column(3).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSortedAppendHelper(t *testing.T) {
	chunk1 := BuildSortedChunk1()
	chunk2 := BuildSortedChunk2()
	chunk2.SetName("mst1")
	chunk3 := BuildSortedChunk3()
	chunk3.SetName("mst2")
	chunk4 := BuildSortedChunk4()

	expectSchemaNames := []string{"val0", "val1", "val2"}
	expectName := "mst,mst1,mst2"
	expectTime := []int64{5, 6, 11, 12, 21, 22, 23, 23, 24, 35, 33, 34, 35, 41, 42, 1, 2}
	expectTagIndex := []int{0, 10, 15}
	expectIntervalIndex := []int{0, 2, 4, 9, 10, 13, 15}
	expectColumnValues1 := []int64{1, 19, 2, 2, 5, 3, 8, 4, 5, 21, 31, 33, 7, 8, 12}
	expectColumnTimes1 := []int64{1, 2, 2, 3, 4, 5, 3, 4, 5}
	expectColumnValues2 := []string{"tomC", "tomA", "jerryC", "jerryA", "tomB", "vergilB", "danteA", "martino",
		"vergilC", "danteC", "martino", "tomD", "jerryD", "danteB", "martino"}
	expectColumnTimes2 := []int64{1, 1, 2, 2, 1, 3, 4, 5, 3, 4, 5, 4, 5}
	expectColumnValues3 := []float64{3.2, 1.1, 3.3, 1.2, 2.2, 2.3, 1.3, 2.4, 1.4, 3.4, 3.5, 3.6, 4.2, 4.3, 2.5, 2.6}
	expectColumnTimes3 := []int64{1, 1, 2, 2, 1, 2, 3, 3, 4, 3, 4, 5, 4, 5}

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	schema := executor.NewQuerySchema(createSortedMergeFields(), []string{"id", "name", "value"}, &opt, nil)

	source1 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk1, chunk4})
	source2 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk2})
	source3 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk3})
	trans := executor.NewSortAppendTransform([]hybridqp.RowDataType{buildRowDataType1(), buildRowDataType1(), buildRowDataType1()}, []hybridqp.RowDataType{buildRowDataType1()}, schema, []hybridqp.QueryNode{})
	trans.ReflectionTables = []executor.ReflectionTable{[]int{0, 1, 2}, []int{0, 1, 2}, []int{0, 1, 2}}
	sink := NewSinkFromFunction(buildRowDataType1(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16})
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		assert.Equal(t, chunk.Column(2).NilsV2().GetArray(), []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16})
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(source3.Output, trans.Inputs[2])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, source3)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSortedAppendHelper_Empty(t *testing.T) {
	chunk1 := BuildNilChunk()
	chunk2 := BuildNilChunk()

	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	schema := executor.NewQuerySchema(createSortedMergeFields(), []string{"id", "name", "value"}, &opt, nil)

	source1 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk1})
	source2 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk2})
	trans := executor.NewSortAppendTransform([]hybridqp.RowDataType{buildRowDataType1(), buildRowDataType1()}, []hybridqp.RowDataType{buildRowDataType1()}, schema, []hybridqp.QueryNode{})
	trans.ReflectionTables = []executor.ReflectionTable{[]int{0, 1, 2}, []int{0, 1, 2}, []int{0, 1, 2}}
	sink := NewSinkFromFunction(buildRowDataType1(), func(chunk executor.Chunk) error {
		if chunk != nil {
			t.Error("expect nil chunk")
		}
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(source2.Output, trans.Inputs[1])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestFilterHelper(t *testing.T) {
	chunk1 := BuildChunk1()
	chunk3 := BuildChunk3()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{12}
	expectTagIndex := []int{0}
	expectIntervalIndex := []int{0}
	expectColumnValues1 := []int64{2}
	expectColumnTimes1 := []int64{2}
	expectColumnValues2 := []string{"jerryA"}
	expectColumnTimes2 := []int64{2}
	expectColumnValues3 := []float64{1.2}
	expectColumnTimes3 := []int64{2}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1, chunk3})
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
		ChunkSize: 100,
		Condition: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "name"},
			Op:  influxql.EQ,
			RHS: &influxql.StringLiteral{Val: "jerryA"},
		},
	}
	schema := executor.NewQuerySchema(createSortedMergeFields(), []string{"id", "name", "value"}, &opt, nil)
	trans := executor.NewFilterTransform(buildRowDataType(), buildRowDataType(), schema, &opt)

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)

		assert.Equal(t, chunk.Column(0).NilCount(), 0)
		assert.Equal(t, chunk.Column(1).NilCount(), 0)
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.Input)
	executor.Connect(trans.Output, sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	if err := executor.Execute(context.Background()); err != nil {
		fmt.Println(err)
	}
	executor.Release()
}

func TestLimitHelper(t *testing.T) {
	chunk1 := BuildChunk1()
	chunk3 := BuildChunk3()
	chunk4 := BuildChunk4()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{12, 13, 34, 35}
	expectTagIndex := []int{0, 2}
	expectIntervalIndex := []int{0, 2}
	expectColumnValues1 := []int64{2, 3, 31, 33}
	expectColumnTimes1 := []int64{2, 3, 4, 5}
	expectColumnValues2 := []string{"jerryA", "danteC", "martino"}
	expectColumnTimes2 := []int64{2, 4, 5}
	expectColumnValues3 := []float64{1.2, 1.3, 3.5, 3.6}
	expectColumnTimes3 := []int64{2, 3}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1, chunk3, chunk4})
	trans := executor.NewLimitTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
		Limit:      2,
		Offset:     1,
	}, executor.LimitTransformParameters{
		Limit:     2,
		Offset:    1,
		LimitType: hybridqp.SingleRowLimit,
	})

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)
		assert.Equal(t, chunk.Column(2).ColumnTimes(), expectColumnTimes3)

		assert.Equal(t, chunk.Column(0).NilCount(), 0)
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 2, 3})
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestMultiRowsLimitHelper(t *testing.T) {
	chunk1 := BuildChunk1()
	chunk3 := BuildChunk3()
	chunk4 := BuildChunk5()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{31, 32, 41, 42, 55, 56}
	expectTagIndex := []int{0, 2}
	expectIntervalIndex := []int{0, 2, 4}
	expectColumnValues1 := []int64{19, 19, 21, 31}
	expectColumnTimes1 := []int64{2, 2, 3, 4}
	expectColumnValues2 := []string{"tomC", "jerryC", "tomD", "jerryD", "vergilD", "danteD"}
	expectColumnTimes2 := []int64{1, 2, 1, 2, 3, 4}
	expectColumnValues3 := []float64{3.2, 3.3, 3.2, 3.3, 3.4, 3.5}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1, chunk3, chunk4})
	trans := executor.NewLimitTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
		Limit:      2,
		Offset:     1,
	}, executor.LimitTransformParameters{
		Limit:     2,
		Offset:    1,
		LimitType: hybridqp.MultipleRowsLimit,
	})

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 3, 4, 5})
		assert.Equal(t, chunk.Column(1).NilCount(), 0)
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestIgnoreTagLimitHelper(t *testing.T) {
	chunk1 := BuildChunk1()
	chunk3 := BuildChunk3()
	chunk4 := BuildChunk4()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{31, 32, 33}
	expectTagIndex := []int{0, 2}
	expectIntervalIndex := []int{0, 2}
	expectColumnValues1 := []int64{19, 21}
	expectColumnTimes1 := []int64{2, 3}
	expectColumnValues2 := []string{"tomC", "jerryC", "vergilC"}
	expectColumnTimes2 := []int64{1, 2, 3}
	expectColumnValues3 := []float64{3.2, 3.3, 3.4}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1, chunk3, chunk4})
	trans := executor.NewLimitTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}, executor.LimitTransformParameters{
		Limit:     3,
		Offset:    5,
		LimitType: hybridqp.SingleRowIgnoreTagLimit,
	})

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 2})
		assert.Equal(t, chunk.Column(1).NilCount(), 0)
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestMultiRowsIgnoreTagLimitHelper(t *testing.T) {
	chunk1 := BuildChunk1()
	chunk3 := BuildChunk3()
	chunk4 := BuildChunk5()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{31, 32, 33, 34, 35}
	expectTagIndex := []int{0, 2}
	expectIntervalIndex := []int{0, 2}
	expectColumnValues1 := []int64{19, 21, 31, 33}
	expectColumnTimes1 := []int64{2, 3, 4, 5}
	expectColumnValues2 := []string{"tomC", "jerryC", "vergilC", "danteC", "martino"}
	expectColumnTimes2 := []int64{1, 2, 3, 4, 5}
	expectColumnValues3 := []float64{3.2, 3.3, 3.4, 3.5, 3.6}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1, chunk3, chunk4})
	trans := executor.NewLimitTransform([]hybridqp.RowDataType{buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}, executor.LimitTransformParameters{
		Limit:     2,
		Offset:    1,
		LimitType: hybridqp.MultipleRowsIgnoreTagLimit,
	})

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).ColumnTimes(), expectColumnTimes1)
		assert.Equal(t, chunk.Column(1).ColumnTimes(), expectColumnTimes2)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 2, 3, 4})
		assert.Equal(t, chunk.Column(1).NilCount(), 0)
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.Inputs[0])
	executor.Connect(trans.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestOrderByHelperWithoutTimeWindow(t *testing.T) {
	chunk1 := BuildTimeOrderChunk()

	expectSchemaNames := []string{"val0", "val1", "val2"}
	expectName := "mst"
	expectTime := []int64{21, 22, 23, 1, 2}
	expectTagIndex := []int{0}
	expectIntervalIndex := []int{0}
	expectColumnValues1 := []int64{2, 5, 8, 12}
	expectColumnValues2 := []string{"tomB", "vergilB", "danteB", "martino"}
	expectColumnValues3 := []float64{2.2, 2.3, 2.4, 2.5, 2.6}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1})
	trans := executor.NewOrderByTransform(buildRowDataType(), buildRowDataType(), []hybridqp.ExprOptions{}, &query.ProcessorOptions{
		Ascending: true,
		ChunkSize: 100,
		Limit:     2,
		Offset:    1,
	}, []string{""})

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{0, 1, 2, 4})
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 2, 3, 4})
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestOrderByHelperWithTimeWindow(t *testing.T) {
	chunk1 := BuildTimeOrderChunk()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{1, 2, 21, 22, 23}
	expectTagIndex := []int{0}
	expectIntervalIndex := []int{0, 2}
	expectColumnValues1 := []int64{12, 2, 5, 8}
	expectColumnValues2 := []string{"danteB", "martino", "tomB", "vergilB"}
	expectColumnValues3 := []float64{2.5, 2.6, 2.2, 2.3, 2.4}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1})
	trans := executor.NewOrderByTransform(buildRowDataType(), buildRowDataType(), []hybridqp.ExprOptions{}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
		ChunkSize: 100,
		Limit:     2,
		Offset:    1,
	}, []string{""})

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 2, 3, 4})
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 1, 2, 4})
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestOrderByHelperWithChunksAndTimeWindow(t *testing.T) {
	chunk1 := BuildTimeOrderChunk1()
	chunk2 := BuildTimeOrderChunk2()

	expectSchemaNames := []string{"id", "name", "value"}
	expectName := "mst"
	expectTime := []int64{1, 2, 11, 12, 13, 21, 22, 23, 41, 42}
	expectTagIndex := []int{0}
	expectIntervalIndex := []int{0, 2, 5, 8}
	expectColumnValues1 := []int64{12, 2, 5, 8, 2, 5, 8, 12}
	expectColumnValues2 := []string{"danteB", "martino", "tomB", "vergilB", "tomB", "vergilB", "danteB", "martino"}
	expectColumnValues3 := []float64{2.5, 2.6, 2.2, 2.3, 2.4, 2.2, 2.3, 2.4, 2.5, 2.6}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1, chunk2})
	trans := executor.NewOrderByTransform(buildRowDataType(), buildRowDataType(), []hybridqp.ExprOptions{}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
		ChunkSize: 100,
		Limit:     2,
		Offset:    1,
	}, []string{""})

	sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{1, 2, 3, 4, 5, 6, 7, 9})
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 1, 2, 4, 5, 7, 8, 9})
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSubQueryHelper(t *testing.T) {
	chunk1 := BuildTimeOrderChunk1()

	expectSchemaNames := []string{"id", "name", "value", "host"}
	expectName := "mst"
	expectTime := []int64{21, 22, 23, 1, 2}
	expectTagIndex := []int{0, 3}
	expectIntervalIndex := []int{0, 3}
	expectColumnValues1 := []int64{2, 5, 8, 12}
	expectColumnValues2 := []string{"tomB", "vergilB", "danteB", "martino"}
	expectColumnValues3 := []float64{2.2, 2.3, 2.4, 2.5, 2.6}
	expectColumnValues4 := []string{"A", "A", "A", "B", "B"}

	source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1})
	trans := executor.NewSubQueryTransform(buildRowDataType(), buildSubQueryRowDataType(), []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "id", Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "name", Type: influxql.String},
		},
		{
			Expr: &influxql.VarRef{Val: "value", Type: influxql.Float},
		},
		{
			Expr: &influxql.VarRef{Val: "host", Type: influxql.Tag},
		},
	}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
		ChunkSize: 100,
		Limit:     2,
		Offset:    1,
	})

	sink := NewSinkFromFunction(buildSubQueryRowDataType(), func(chunk executor.Chunk) error {
		for i := range chunk.Columns() {
			assert.Equal(t, chunk.RowDataType().Field(i).Name(), expectSchemaNames[i])
		}
		assert.Equal(t, chunk.Name(), expectName)
		assert.Equal(t, chunk.TagIndex(), expectTagIndex)
		assert.Equal(t, chunk.IntervalIndex(), expectIntervalIndex)
		assert.Equal(t, chunk.Time(), expectTime)

		assert.Equal(t, chunk.Column(0).IntegerValues(), expectColumnValues1)
		assert.Equal(t, chunk.Column(1).StringValuesV2(nil), expectColumnValues2)
		assert.Equal(t, chunk.Column(2).FloatValues(), expectColumnValues3)
		assert.Equal(t, chunk.Column(3).StringValuesV2(nil), expectColumnValues4)

		assert.Equal(t, chunk.Column(0).NilsV2().GetArray(), []uint16{0, 1, 2, 4})
		assert.Equal(t, chunk.Column(1).NilsV2().GetArray(), []uint16{0, 2, 3, 4})
		assert.Equal(t, chunk.Column(2).NilCount(), 0)
		assert.Equal(t, chunk.Column(3).NilCount(), 0)
		return nil
	})

	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

func TestSplitGroupHelper(t *testing.T) {
	chunk1 := BuildTimeOrderChunk1()

	source1 := NewSourceFromSingleChunk(buildRowDataType1(), []executor.Chunk{chunk1})
	trans := executor.NewSplitTransformTransform(buildRowDataType1(), buildRowDataType1(), []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "val0", Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
		},
		{
			Expr: &influxql.VarRef{Val: "val2", Type: influxql.Float},
		},
	}, &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Ascending: true,
		ChunkSize: 100,
		Limit:     2,
		Offset:    1,
	})

	sink := NewSinkFromFunction(buildRowDataType1(), func(chunk executor.Chunk) error {
		return nil
	})

	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)

	dag := executor.NewDAG(processors)

	if dag.CyclicGraph() == true {
		t.Error("dag has circle")
	}

	trans.Explain()
	trans.Release()
	if len(trans.GetOutputs()) != len(trans.GetInputs()) || trans.GetInputNumber(trans.GetInputs()[0]) != trans.GetInputNumber(trans.GetInputs()[0]) {
		t.Error("unexpected ports len")
	}
	executor := executor.NewPipelineExecutor(processors)
	executor.Execute(context.Background())
	executor.Release()
}

// SourceFromSingleRecord used to generate records we need
type SourceFromSingleChunk struct {
	executor.BaseProcessor

	Output *executor.ChunkPort
	Record []executor.Chunk
}

func NewSourceFromSingleChunk(rowDataType hybridqp.RowDataType, records []executor.Chunk) *SourceFromSingleChunk {
	return &SourceFromSingleChunk{
		Output: executor.NewChunkPort(rowDataType),
		Record: records,
	}
}

func (source *SourceFromSingleChunk) Name() string {
	return "SourceFromSingleRecord"
}

func (source *SourceFromSingleChunk) Explain() []executor.ValuePair {
	return nil
}

func (source *SourceFromSingleChunk) Close() {
	source.Output.Close()
}

func (source *SourceFromSingleChunk) Work(ctx context.Context) error {
	i := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if source.Record == nil {
				source.Output.Close()
				return nil
			}
			source.Output.State <- source.Record[i]
			i += 1
			if i == len(source.Record) {
				source.Record = nil
			}
		}
	}
}

func (source *SourceFromSingleChunk) GetOutputs() executor.Ports {
	return executor.Ports{source.Output}
}

func (source *SourceFromSingleChunk) GetInputs() executor.Ports {
	return executor.Ports{}
}

func (source *SourceFromSingleChunk) GetOutputNumber(port executor.Port) int {
	return 0
}

func (source *SourceFromSingleChunk) GetInputNumber(port executor.Port) int {
	return 0
}

type SinkFromFunction struct {
	executor.BaseProcessor

	Input    *executor.ChunkPort
	Function func(chunk executor.Chunk) error
}

func NewSinkFromFunction(schema hybridqp.RowDataType, function func(chunk executor.Chunk) error) *SinkFromFunction {
	return &SinkFromFunction{
		Input:    executor.NewChunkPort(schema),
		Function: function,
	}
}

func (sink *SinkFromFunction) Name() string {
	return "SinkFromFunction"
}

func (sink *SinkFromFunction) Explain() []executor.ValuePair {
	return nil
}

func (sink *SinkFromFunction) Close() {
	return
}

func (sink *SinkFromFunction) Work(ctx context.Context) error {
	for {
		select {
		case r, ok := <-sink.Input.State:
			if !ok {
				return nil
			}
			sink.Function(r)
		case <-ctx.Done():
			return nil
		}
	}
}

func (sink *SinkFromFunction) GetOutputs() executor.Ports {
	return executor.Ports{}
}

func (sink *SinkFromFunction) GetInputs() executor.Ports {
	if sink.Input == nil {
		return executor.Ports{}
	}

	return executor.Ports{sink.Input}
}

func (sink *SinkFromFunction) GetOutputNumber(port executor.Port) int {
	return 0
}

func (sink *SinkFromFunction) GetInputNumber(port executor.Port) int {
	return 0
}

func ParseChunkTags(s string) *executor.ChunkTags {
	var m influx.PointTags
	var ss []string
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m = append(m, influx.Tag{Key: a[0], Value: a[1], IsArray: false})
		ss = append(ss, a[0])
	}
	return executor.NewChunkTags(m, ss)
}

func createSortedMergeFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "id",
				Type: influxql.Integer,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "name",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			Alias: "",
		},
	)

	return fields
}

func createSortedMergeFields1() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "id",
				Type: influxql.Integer,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "names",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "yield",
				Type: influxql.Boolean,
			},
			Alias: "",
		},
	)

	return fields
}

func TestMaxFunction(t *testing.T) {
	if hybridqp.MaxInt(1, 2) != 2 {
		t.Fatal()
	}
	if hybridqp.MaxInt(2, 1) != 2 {
		t.Fatal()
	}
}

func createQuerySchemaWithSum() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	fields := make(influxql.Fields, 0, 1)
	fields = append(fields, &influxql.Field{
		Expr: &influxql.Call{
			Name: "sum",
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  "id",
					Type: influxql.Unknown,
				},
			},
		},
	})
	schema := executor.NewQuerySchema(fields, []string{"id"}, &opt, nil)
	return schema
}

func createQuerySchemaWithID() *executor.QuerySchema {
	opt := query.ProcessorOptions{Dimensions: []string{"name"}}
	fields := make(influxql.Fields, 0, 1)
	fields = append(fields, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:  "id",
			Type: influxql.Unknown,
		},
	})
	schema := executor.NewQuerySchema(fields, []string{"id"}, &opt, nil)
	return schema
}

func createQuerySchemaWithAge() *executor.QuerySchema {
	opt := query.ProcessorOptions{Dimensions: []string{"name"}}
	fields := make(influxql.Fields, 0, 1)
	fields = append(fields, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:  "age",
			Type: influxql.Integer,
		},
	})
	schema := executor.NewQuerySchema(fields, []string{"age"}, &opt, nil)
	return schema
}

func testBuildPipelineExecutor(t *testing.T, querySchema *executor.QuerySchema, rowDataType hybridqp.RowDataType) error {
	info := buildIndexScanExtraInfo()
	reader := executor.NewLogicalColumnStoreReader(nil, querySchema)
	var input hybridqp.QueryNode
	if querySchema.HasCall() {
		agg := executor.NewLogicalHashAgg(reader, querySchema, executor.READER_EXCHANGE, nil)
		input = executor.NewLogicalHashAgg(agg, querySchema, executor.SHARD_EXCHANGE, nil)
	} else {
		if len(querySchema.Options().GetDimensions()) == 0 {
			input = executor.NewLogicalHashMerge(reader, querySchema, executor.SHARD_EXCHANGE, nil)
		} else {
			merge := executor.NewLogicalHashMerge(reader, querySchema, executor.READER_EXCHANGE, nil)
			input = executor.NewLogicalHashMerge(merge, querySchema, executor.SHARD_EXCHANGE, nil)
		}
	}
	indexScan := executor.NewLogicalSparseIndexScan(input, querySchema)
	executor.ReWriteArgs(indexScan, false)
	scan := executor.NewSparseIndexScanTransform(rowDataType, indexScan.Children()[0], indexScan.RowExprOptions(), info, querySchema)
	sink := NewNilSink(rowDataType)
	if err := executor.Connect(scan.GetOutputs()[0], sink.Input); err != nil {
		t.Fatal(err)
	}
	var processors executor.Processors
	processors = append(processors, scan)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	return executors.Execute(context.Background())
}

func TestBuildPipelineExecutorWithHashAggAndHashMerge(t *testing.T) {
	// for hash agg
	rowDataType := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "id", Type: influxql.Unknown})
	querySchema := createQuerySchemaWithSum()
	assert.Equal(t, strings.Contains(testBuildPipelineExecutor(t, querySchema, rowDataType).Error(), "unsupported (sum/mean) iterator type: (unknown)"), true)

	// for hash merge
	rowDataType = hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "id", Type: influxql.Unknown})
	querySchema = createQuerySchemaWithID()
	assert.Equal(t, strings.Contains(testBuildPipelineExecutor(t, querySchema, rowDataType).Error(), "HashMergeTransform run error"), true)

	// for hash merge group by
	rowDataType = hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "age", Type: influxql.Integer})
	querySchema = createQuerySchemaWithAge()
	assert.Equal(t, testBuildPipelineExecutor(t, querySchema, rowDataType), nil)
}

func TestExecutorBuilder_addNodeExchange1(t *testing.T) {
	var schema *executor.QuerySchema
	var builder hybridqp.PipelineExecutorBuilder
	var traits *executor.StoreExchangeTraits
	builder, schema, traits = MockNewExecutorBuilder()
	logicSeries1 := executor.NewLogicalSeries(schema)
	inputLogicSeries1 := []hybridqp.QueryNode{logicSeries1}
	Exchange := executor.NewLogicalExchange(inputLogicSeries1[0], executor.SERIES_EXCHANGE, []hybridqp.Trait{traits}, schema)
	_, err := builder.Build(Exchange)
	require.Contains(t, err.Error(), "unsupport logical plan, can't build processor from itr")
	input := executor.NewLogicalReader(nil, schema)
	Exchange = executor.NewLogicalExchange(input, executor.SERIES_EXCHANGE, []hybridqp.Trait{traits}, schema)
	p, _ := builder.Build(Exchange)
	pipelineExecutor := p.(*executor.PipelineExecutor)
	require.Equal(t, pipelineExecutor.GetProcessors().Empty(), false)
}

func TestExecutorBuilder_addNodeExchange2(t *testing.T) {
	var schema *executor.QuerySchema
	var builder hybridqp.PipelineExecutorBuilder
	builder, schema, _ = MockNewExecutorBuilder()
	input := executor.NewLogicalReader(nil, schema)
	rq := executor.RemoteQuery{
		Database: "db0",
		PtID:     1,
		NodeID:   0,
		ShardIDs: []uint64{1, 2, 3, 4},
		Opt: query.ProcessorOptions{
			Name:                  "test",
			Expr:                  nil,
			Exprs:                 nil,
			Aux:                   []influxql.VarRef{{Val: "key", Type: influxql.String}},
			Sources:               []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}},
			Interval:              hybridqp.Interval{Duration: time.Second, Offset: 101},
			Dimensions:            []string{"a", "b", "c"},
			GroupBy:               map[string]struct{}{"a": {}, "b": {}},
			Location:              nil,
			Fill:                  1,
			FillValue:             1.11,
			Condition:             nil,
			StartTime:             1,
			EndTime:               2,
			Limit:                 3,
			Offset:                4,
			SLimit:                5,
			SOffset:               6,
			Ascending:             false,
			StripName:             false,
			Dedupe:                false,
			Ordered:               false,
			Parallel:              false,
			MaxSeriesN:            0,
			InterruptCh:           nil,
			Authorizer:            nil,
			ChunkedSize:           0,
			Chunked:               false,
			ChunkSize:             0,
			MaxParallel:           0,
			RowsChan:              nil,
			QueryId:               100001,
			Query:                 "SELECT * FROM mst1 limit 10",
			EnableBinaryTreeMerge: 0,
			HintType:              0,
		},
		Analyze: false,
		Node:    []byte{1, 2, 3, 4, 5, 6, 7},
	}
	Exchange := executor.NewLogicalExchange(input, executor.NODE_EXCHANGE, []hybridqp.Trait{&rq}, schema)
	builder.Build(Exchange)
	p, _ := builder.Build(Exchange)
	pipelineExecutor := p.(*executor.PipelineExecutor)
	require.Equal(t, pipelineExecutor.GetProcessors().Empty(), false)
}

func TestExecutorBuilder_addPartitionExchange1(t *testing.T) {
	var schema *executor.QuerySchema
	var builder hybridqp.PipelineExecutorBuilder
	_, schema, _ = MockNewExecutorBuilder()
	ptQuerys := make([]executor.PtQuery, 0)
	ptQuerys = append(ptQuerys, executor.PtQuery{PtID: 1, ShardInfos: []executor.ShardInfo{{ID: 1, Path: "/obs/db0/log1/seg0", Version: 4}}})
	ptQuerys = append(ptQuerys, executor.PtQuery{PtID: 2, ShardInfos: []executor.ShardInfo{{ID: 2, Path: "/obs/db0/log1/seg1", Version: 4}}})
	rq := executor.RemoteQuery{
		Database: "db0",
		PtID:     1,
		NodeID:   0,
		PtQuerys: ptQuerys,
		Opt: query.ProcessorOptions{
			Name:                  "test",
			Expr:                  nil,
			Exprs:                 nil,
			Aux:                   []influxql.VarRef{{Val: "key", Type: influxql.String}},
			Sources:               []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}},
			Interval:              hybridqp.Interval{Duration: time.Second, Offset: 101},
			Dimensions:            []string{"a", "b", "c"},
			GroupBy:               map[string]struct{}{"a": {}, "b": {}},
			Location:              nil,
			Fill:                  1,
			FillValue:             1.11,
			Condition:             nil,
			StartTime:             1,
			EndTime:               2,
			Limit:                 3,
			Offset:                4,
			SLimit:                5,
			SOffset:               6,
			Ascending:             false,
			StripName:             false,
			Dedupe:                false,
			Ordered:               false,
			Parallel:              false,
			MaxSeriesN:            0,
			InterruptCh:           nil,
			Authorizer:            nil,
			ChunkedSize:           0,
			Chunked:               false,
			ChunkSize:             0,
			MaxParallel:           0,
			RowsChan:              nil,
			QueryId:               100001,
			Query:                 "SELECT * FROM mst1 limit 10",
			EnableBinaryTreeMerge: 0,
			HintType:              0,
		},
		Analyze: false,
		Node:    []byte{1, 2, 3, 4, 5, 6, 7},
	}
	csTraits := executor.NewCsStoreExchangeTraits(nil, ptQuerys)
	info := executor.IndexScanExtraInfo{
		Req:     &rq,
		PtQuery: &ptQuerys[0],
	}
	builder = executor.NewMocStoreExecutorBuilder(nil, csTraits, &info, 0)
	input := executor.NewLogicalColumnStoreReader(nil, schema)
	Exchange := executor.NewLogicalExchange(input, executor.PARTITION_EXCHANGE, []hybridqp.Trait{&rq}, schema)
	builder.Build(Exchange)
	p, _ := builder.Build(Exchange)
	pipelineExecutor := p.(*executor.PipelineExecutor)
	require.Equal(t, pipelineExecutor.GetProcessors().Empty(), true)
}

func TestIsMultiMstPlanNode(t *testing.T) {
	node := &executor.LogicalBinOp{}
	builder := &executor.ExecutorBuilder{}
	assert.Equal(t, builder.IsMultiMstPlanNode(node), true)
}

func TestExecutorInitCtxErr(t *testing.T) {
	var processors executor.Processors
	executors := executor.NewPipelineExecutor(processors)
	ctx := context.Background()
	executors.InitContext(ctx)
	err := executors.Execute(ctx)
	assert.NotEqual(t, err, nil)
}

func TestPipelineExecutorInterrupt(t *testing.T) {
	outputRowDataType := buildIRowDataType()
	schema := buildISchema()
	trans := executor.NewIndexScanTransform(outputRowDataType, nil, schema, nil, nil, make(chan struct{}, 1), 0, false)
	pe := executor.NewPipelineExecutor(executor.Processors{trans})
	pe.Crash()
	assert.Equal(t, pe.Crashed(), true)
}

func TestExecutorBuilder_Except_Limit(t *testing.T) {
	var schema *executor.QuerySchema
	var builder hybridqp.PipelineExecutorBuilder
	var traits *executor.StoreExchangeTraits
	builder, schema, traits = MockNewExecutorBuilder()
	schema.Options().(*query.ProcessorOptions).Without = true
	schema.Options().(*query.ProcessorOptions).Limit = 1
	input := executor.NewLogicalReader(nil, schema)
	exchange := executor.NewLogicalExchange(input, executor.SERIES_EXCHANGE, []hybridqp.Trait{traits}, schema)
	sender := executor.NewLogicalHttpSender(exchange, schema)
	p, _ := builder.Build(sender)
	pipelineExecutor := p.(*executor.PipelineExecutor)
	require.Equal(t, pipelineExecutor.GetProcessors().Empty(), false)

	builder, schema, traits = MockNewExecutorBuilder()
	schema.Options().(*query.ProcessorOptions).Without = true
	schema.Options().(*query.ProcessorOptions).Limit = 1
	input = executor.NewLogicalReader(nil, schema)
	exchange = executor.NewLogicalExchange(input, executor.SERIES_EXCHANGE, []hybridqp.Trait{traits}, schema)
	sender1 := executor.NewLogicalHttpSenderHint(exchange, schema)
	p, _ = builder.Build(sender1)
	pipelineExecutor = p.(*executor.PipelineExecutor)
	require.Equal(t, pipelineExecutor.GetProcessors().Empty(), false)
}
