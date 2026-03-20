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

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func Test_Select_Illegal_Context(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, query.QueryDurationKey, 1)
	stmt := &influxql.SelectStatement{}
	// Mock query.Prepare since it is hard to construct it directly
	patch := gomonkey.NewPatches()
	defer patch.Reset()
	patch.ApplyFunc(query.Prepare, func(stmt *influxql.SelectStatement, shardMapper query.ShardMapper, opt query.SelectOptions) (query.PreparedStatement, error) {
		return nil, nil
	})
	_, err := Select(ctx, stmt, nil, query.SelectOptions{})
	assert.Error(t, err, "Select context value type illegal")
}

type MockLogicalPlanCreator struct {
	query.LogicalPlanCreator
}

func TestSetResource(t *testing.T) {
	convey.Convey("SetResource_Without_LLMFunc", t, func() {
		schema := &QuerySchema{}
		patch := gomonkey.ApplyMethod(schema, "HasLLMFunc", func(schema *QuerySchema) bool {
			return false
		})
		defer patch.Reset()
		convey.So(SetResource(nil, schema), convey.ShouldEqual, nil)
	})

	convey.Convey("SetResource_With_Invalid_LLMFunc", t, func() {
		schema := &QuerySchema{}
		patch1 := gomonkey.ApplyMethod(schema, "HasLLMFunc", func(schema *QuerySchema) bool {
			return true
		})
		defer patch1.Reset()
		patch2 := gomonkey.ApplyMethod(schema, "LLMFunc", func(schema *QuerySchema) []*influxql.Field {
			return []*influxql.Field{
				{Expr: &influxql.VarRef{}},
			}
		})
		defer patch2.Reset()
		convey.So(SetResource(nil, schema), convey.ShouldEqual, nil)
	})

	convey.Convey("SetResource_With_Invalid_LLMFunc", t, func() {
		schema := &QuerySchema{}
		patch1 := gomonkey.ApplyMethod(schema, "HasLLMFunc", func(schema *QuerySchema) bool {
			return true
		})
		defer patch1.Reset()
		patch2 := gomonkey.ApplyMethod(schema, "LLMFunc", func(schema *QuerySchema) []*influxql.Field {
			return []*influxql.Field{
				{Expr: &influxql.Call{Name: "llm_generate", Args: []influxql.Expr{
					&influxql.VarRef{}, &influxql.StringLiteral{Val: ""},
				}}},
			}
		})
		defer patch2.Reset()
		convey.So(SetResource(nil, schema), convey.ShouldBeError, "invalid resource(func=llm_generate)")
	})

	convey.Convey("SetResource_Without_Resource", t, func() {
		schema := &QuerySchema{}
		patch1 := gomonkey.ApplyMethod(schema, "HasLLMFunc", func(schema *QuerySchema) bool {
			return true
		})
		defer patch1.Reset()
		patch2 := gomonkey.ApplyMethod(schema, "LLMFunc", func(schema *QuerySchema) []*influxql.Field {
			return []*influxql.Field{
				{Expr: &influxql.Call{Name: "llm_generate", Args: []influxql.Expr{
					&influxql.VarRef{}, &influxql.StringLiteral{Val: "gemini"},
				}}},
			}
		})
		defer patch2.Reset()
		var qc = &MockLogicalPlanCreator{}
		patch3 := gomonkey.ApplyMethod(qc, "GetResource", func(qc *MockLogicalPlanCreator, resource string) map[string]string {
			return nil
		})
		defer patch3.Reset()
		convey.So(SetResource(qc, schema), convey.ShouldBeError, "resource(name=gemini) not found")
	})

	convey.Convey("SetResource_With_Valid_Resource", t, func() {
		schema := &QuerySchema{}
		patch1 := gomonkey.ApplyMethod(schema, "HasLLMFunc", func(schema *QuerySchema) bool {
			return true
		})
		defer patch1.Reset()
		patch2 := gomonkey.ApplyMethod(schema, "LLMFunc", func(schema *QuerySchema) []*influxql.Field {
			return []*influxql.Field{
				{Expr: &influxql.Call{
					Name: "llm_generate",
					Args: []influxql.Expr{
						&influxql.VarRef{}, &influxql.StringLiteral{Val: "gemini"},
					}}},
			}
		})
		defer patch2.Reset()
		var qc = &MockLogicalPlanCreator{}
		patch3 := gomonkey.ApplyMethod(qc, "GetResource", func(qc *MockLogicalPlanCreator, resource string) map[string]string {
			return map[string]string{"type": "llm"}
		})
		defer patch3.Reset()
		assert.NoError(t, SetResource(qc, schema))
	})
}

func TestBuildQueryPlan(t *testing.T) {
	convey.Convey("SetResource_With_Valid_Resource", t, func() {
		ctx := context.Background()
		stmt := &influxql.SelectStatement{}
		qc := &MockLogicalPlanCreator{}
		patch1 := gomonkey.ApplyMethod(qc, "GetSources", func(_ *MockLogicalPlanCreator, _ influxql.Sources) influxql.Sources {
			return make([]influxql.Source, 2)
		})
		defer patch1.Reset()
		schema := NewQuerySchema(nil, []string{"id"}, &query.ProcessorOptions{}, nil)
		csMst := &influxql.Measurement{EngineType: config.COLUMNSTORE}
		schema.SetSources(influxql.Sources{csMst, csMst})
		_, err := buildQueryPlan(ctx, stmt, qc, schema)
		require.Error(t, err, "column store does not currently support the multi-measurement query")
	})
}
