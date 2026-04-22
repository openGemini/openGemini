// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestSchemaOverLimit(t *testing.T) {
	statement := &preparedStatement{}
	statement.stmt = &influxql.SelectStatement{}
	ok, err := statement.isSchemaOverLimit(nil)
	if err != nil {
		t.Error("isSchemaOverLimit error")
	}
	if ok {
		t.Error("expect ok")
	}
}

func Test_defaultQueryExecutorBuilderCreator(t *testing.T) {
	tests := []struct {
		name string
		want hybridqp.PipelineExecutorBuilder
	}{
		{
			name: "test",
			want: &ExecutorBuilder{
				dag:                   NewTransformDag(),
				root:                  nil,
				traits:                nil,
				currConsumer:          0,
				enableBinaryTreeMerge: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultQueryExecutorBuilderCreator(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("defaultQueryExecutorBuilderCreator() = %v, want %v", got, tt.want)
			}
		})
	}
}

func retNodeHashAggCheck(t *testing.T, node hybridqp.QueryNode) {
	if _, ok := node.(*LogicalHashAgg); !ok {
		t.Fatal("retNodeHashAggCheck err")
	}
}

func retNodeStreamAggCheck(t *testing.T, node hybridqp.QueryNode) {
	if _, ok := node.(*LogicalAggregate); !ok {
		t.Fatal("retNodeStreamAggCheck err")
	}
}

func retNodePromSortCheck(t *testing.T, node hybridqp.QueryNode) {
	if _, ok := node.(*LogicalPromSort); !ok {
		t.Fatal("LogicalPromSort err")
	}
}

func Test_BuildAggNode(t *testing.T) {
	lowerOpt := &query.ProcessorOptions{
		PromQuery: true,
	}
	upperOpt := &query.ProcessorOptions{
		LowerOpt:       lowerOpt,
		PromQuery:      true,
		GroupByAllDims: true,
	}
	schema := &QuerySchema{
		opt: upperOpt,
	}
	builder := NewLogicalPlanBuilderImpl(schema)
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.GroupByAllDims = false
	upperOpt.Without = true
	lowerOpt.GroupByAllDims = true
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	lowerOpt.GroupByAllDims = false
	lowerOpt.Without = true
	lowerOpt.Dimensions = []string{"a", "b"}
	upperOpt.Dimensions = []string{"a"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.Dimensions = []string{"c"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	lowerOpt.Without = false
	upperOpt.Dimensions = []string{"b"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.Dimensions = []string{"a"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	upperOpt.Without = false
	lowerOpt.GroupByAllDims = true
	lowerOpt.Dimensions = nil
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	lowerOpt.GroupByAllDims = false
	lowerOpt.Without = true
	lowerOpt.Dimensions = []string{"a"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	lowerOpt.Dimensions = []string{"b"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	lowerOpt.Without = false
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	upperOpt.Dimensions = []string{"b"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.BinOp = true
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())
}

func Test_BuildSortNode(t *testing.T) {
	opt := &query.ProcessorOptions{
		PromQuery: true,
	}
	schema := &QuerySchema{
		opt: opt,
	}
	builder := NewLogicalPlanBuilderImpl(schema)
	builder.Series()
	buildSortNode(builder, schema, schema)
	retNodePromSortCheck(t, builder.stack.Pop())
}

func TestAggTransformCancel(t *testing.T) {
	inRowDataType := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "age", Type: influxql.Float})
	outRowDataType := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "max_prom(\"age\")", Type: influxql.Float})
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "max_prom", Args: []influxql.Expr{hybridqp.MustParseExpr("age")}},
			Ref:  influxql.VarRef{Val: `max_prom("age")`, Type: influxql.Float},
		},
	}
	opt := &query.ProcessorOptions{}
	// case1: cancel the ctx
	trans, _ := NewStreamAggregateTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		exprOpt,
		opt, &QuerySchema{}, false)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	trans.errs.Init(1, trans.Close)
	go trans.run(ctx, &trans.errs)
	cancel()
	assert.NoError(t, trans.errs.Err())
	// case2: close the next signal chan
	trans, _ = NewStreamAggregateTransform(
		[]hybridqp.RowDataType{inRowDataType},
		[]hybridqp.RowDataType{outRowDataType},
		exprOpt,
		opt, &QuerySchema{}, false)
	trans.Inputs[0].State = make(chan Chunk, 1)
	trans.Inputs[0].State <- nil
	trans.errs.Init(1, trans.Close)
	go trans.run(context.Background(), &trans.errs)
	<-trans.inputChunk
	close(trans.nextSignal)
	assert.NoError(t, trans.errs.Err())
}

func TestFillTransformCancel(t *testing.T) {
	opt := query.ProcessorOptions{
		Interval:  hybridqp.Interval{Duration: 10 * time.Nanosecond},
		StartTime: 0,
		EndTime:   40,
		Ordered:   true,
		Fill:      influxql.PreviousFill,
	}
	rowDataType := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "max(\"age\")", Type: influxql.Float})
	schema := &QuerySchema{}
	schema.SetOpt(&opt)
	// case1: cancel the ctx
	trans, _ := NewFillTransform(
		[]hybridqp.RowDataType{rowDataType},
		[]hybridqp.RowDataType{rowDataType},
		nil, schema)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	trans.errs.Init(1, trans.Close)
	go trans.run(ctx, &trans.errs)
	cancel()
	assert.NoError(t, trans.errs.Err())
	// case2: close the next signal chan
	trans, _ = NewFillTransform(
		[]hybridqp.RowDataType{rowDataType},
		[]hybridqp.RowDataType{rowDataType},
		nil, schema)
	trans.Inputs[0].State = make(chan Chunk, 1)
	trans.Inputs[0].State <- nil
	trans.errs.Init(1, trans.Close)
	go trans.run(context.Background(), &trans.errs)
	<-trans.inputChunk
	close(trans.nextSignal)
	assert.NoError(t, trans.errs.Err())
}

func TestBuildLogicalPlanWithCompare(t *testing.T) {
	sqlReader := strings.NewReader(TemplateSql[NO_AGG_NO_GROUP])
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	yaccQuery, _ := yaccParser.GetQuery()
	stmt := yaccQuery.Statements[0]

	stmt, _ = query.RewriteStatement(stmt)
	selectStmt := stmt.(*influxql.SelectStatement)
	selectStmt.IsCompareCall = true

	sOpts := query.SelectOptions{}
	shardMapper := NewPlanTypeInitShardMapper()

	p, _ := query.Prepare(selectStmt, shardMapper, sOpts)
	node, _, _ := p.BuildLogicalPlan(context.Background())
	assert.Equal(t, true, node.Schema().IsCompareCall())
}

func TestBuildNodeExchange(t *testing.T) {
	nodeTraits := []hybridqp.Trait{}
	rq1 := &RemoteQuery{Database: "db0", PtID: uint32(1), NodeID: uint64(1), ShardIDs: []uint64{1}}
	nodeTraits = append(nodeTraits, rq1, rq1)
	ctx := context.Background()
	ctx = context.WithValue(ctx, hybridqp.NodeTrait, &nodeTraits)
	fields := make(influxql.Fields, 0, 1)
	fields = append(fields, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:  "id",
			Type: influxql.Integer,
		},
		Alias: "",
	})
	schema := NewQuerySchema(fields, []string{"id"}, &query.ProcessorOptions{}, nil)
	reader := NewLogicalReader(nil, schema)
	builder := NewLogicalPlanBuilderImpl(schema)
	project := NewLogicalProject(reader, schema)
	assert.NoError(t, BuildNodeExchange(ctx, builder, project))
}
