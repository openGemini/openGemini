// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

var reqId = "1122222333333333"

func buildCTEStmt(t *testing.T, sql string) *influxql.WithSelectStatement {
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	q, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal(err)
	}

	stmt := q.Statements[0]
	selectStmt, ok := stmt.(*influxql.WithSelectStatement)
	ctes := selectStmt.CTEs
	for _, cte := range ctes {
		cte.ReqId = reqId
	}
	if !ok {
		t.Fatal(fmt.Errorf("invalid WithSelectStatement"))
	}
	return selectStmt
}

func buildCTEOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
		influxql.VarRef{Val: "f2", Type: influxql.String},
		influxql.VarRef{Val: "f3", Type: influxql.Float},
		influxql.VarRef{Val: "f4", Type: influxql.Boolean},
		influxql.VarRef{Val: "f5", Type: influxql.Graph},
	)
	return rowDataType
}

func BuildWithChunk1() executor.Chunk {
	rowDataType := buildCTEOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=1"), 0)
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=2"), 1)
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=3"), 2)
	chunk.AppendIntervalIndex(0)
	chunk.AppendIntervalIndex(1)
	chunk.AppendIntervalIndex(2)
	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(0).AppendManyNotNil(3)
	chunk.Column(1).AppendStringValues([]string{"11", "12", "13"})
	chunk.Column(1).AppendManyNotNil(3)
	chunk.Column(2).AppendFloatValues([]float64{1.1, 2.1, 3.1})
	chunk.Column(2).AppendManyNotNil(3)
	chunk.Column(3).AppendBooleanValues([]bool{true, false, true})
	chunk.Column(3).AppendManyNotNil(3)
	return chunk
}

func buildCTEOuterSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NullFill,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildOps() []hybridqp.ExprOptions {
	return []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "f1", Type: influxql.Integer},
		},
		{
			Expr: &influxql.VarRef{Val: "f2", Type: influxql.String},
		},
		{
			Expr: &influxql.VarRef{Val: "f3", Type: influxql.Float},
		},
		{
			Expr: &influxql.VarRef{Val: "f4", Type: influxql.Boolean},
		},
		{
			Expr: &influxql.VarRef{Val: "f5", Type: influxql.Graph},
		},
	}
}

func TestCTETransform1(t *testing.T) {
	sql := "with t1 as (select * from mst) select f1 from t1"
	stmt := buildCTEStmt(t, sql)
	chunk1 := BuildWithChunk1()
	chunks1 := []executor.Chunk{chunk1}
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	var inrts []hybridqp.RowDataType
	inrts = append(inrts, source1.Output.RowDataType)
	ort := buildCTEOutputRowDataType()
	outerSchema := buildCTEOuterSchema()

	ops := buildOps()
	ctePlan := buildPlan(t, outerSchema)
	trans, err := executor.NewCTETransform(inrts[0], ort, outerSchema, ctePlan, stmt.CTEs[0], ops)
	assert.True(t, err == nil)

	trans.GetOutputNumber(nil)
	trans.GetInputNumber(nil)
	trans.Name()
	trans.Explain()
	trans.TagValueFromChunk(chunk1, "f5")
	trans.ExecutorBuilder = &mockExecutorBuilder{
		build: func() (hybridqp.Executor, error) {
			source := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
			dag := &executor.TransformDag{}
			root := executor.NewTransformVertex(nil, source)
			pipeline := executor.NewPipelineExecutorFromDag(dag, root)
			pipeline.SetProcessors([]executor.Processor{source})
			return pipeline, nil
		},
	}

	checkResult := func(chunk executor.Chunk) error {
		err := PromResultCompareMultiCol(chunk, BuildWithChunk1())
		assert.Equal(t, err, nil)
		return nil
	}
	sink := NewSinkFromFunction(ort, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Execute(context.Background()) // using cache
	executors.Release()
	executor.CTEBuf.ClearDataStoreByKey(reqId, stmt.CTEs)
}
