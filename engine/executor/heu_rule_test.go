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

package executor_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

type AggPushDownVerifier struct {
	aggCount int
}

func NewAggPushDownVerifier() *AggPushDownVerifier {
	return &AggPushDownVerifier{
		aggCount: 0,
	}
}

type SlideWindowPushDownVerifier struct {
	slideWindowCount int
}

func NewSlideWindowPushDownVerifier() *SlideWindowPushDownVerifier {
	return &SlideWindowPushDownVerifier{
		slideWindowCount: 0,
	}
}

func (visitor *SlideWindowPushDownVerifier) Visit(node hybridqp.QueryNode) hybridqp.QueryNodeVisitor {
	if _, ok := node.(*executor.LogicalSlidingWindow); ok {
		visitor.slideWindowCount++
	}
	return visitor
}

func (visitor *SlideWindowPushDownVerifier) SlideWindowCount() int {
	return visitor.slideWindowCount
}

func (visitor *AggPushDownVerifier) Visit(node hybridqp.QueryNode) hybridqp.QueryNodeVisitor {
	if _, ok := node.(*executor.LogicalAggregate); ok {
		visitor.aggCount++
	}
	return visitor
}

func (visitor *AggPushDownVerifier) AggCount() int {
	return visitor.aggCount
}

type LimitPushDownVerifier struct {
	limitCount int
}

func NewLimitPushDownVerifier() *LimitPushDownVerifier {
	return &LimitPushDownVerifier{
		limitCount: 0,
	}
}

func (visitor *LimitPushDownVerifier) Visit(node hybridqp.QueryNode) hybridqp.QueryNodeVisitor {
	if _, ok := node.(*executor.LogicalLimit); ok {
		visitor.limitCount++
	}
	return visitor
}

func (visitor *LimitPushDownVerifier) LimitCount() int {
	return visitor.limitCount
}

func TestLimitPushdownRule(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
		},
	}
	columnsName := []string{"value"}
	opt := query.ProcessorOptions{}
	opt.Limit = 10
	opt.Offset = 10

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	limitType := schema.LimitType()
	limit, offset := schema.LimitAndOffset()
	planBuilder.Limit(executor.LimitTransformParameters{
		Limit:     limit,
		Offset:    offset,
		LimitType: limitType,
	})
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewLimitPushdownToExchangeRule("")
	toMeasurement := executor.NewLimitPushdownToReaderRule("")
	toSeries := executor.NewLimitPushdownToSeriesRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_LIMIT)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewLimitPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if verifier.LimitCount() != 5 {
		t.Errorf("5 limit in plan tree, but %d", verifier.LimitCount())
	}
}

func TestAggPushdownToExchangeRuleWithPercentile(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "percentile",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"percentile"}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewAggPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if verifier.AggCount() != 1 {
		t.Errorf("only one agg in plan tree, but %d", verifier.AggCount())
	}
}

func TestAggPushdownToExchangeRuleWithCount(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"count"}
	opt := query.ProcessorOptions{}
	opt.Interval.Duration = 1000

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewAggPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if verifier.AggCount() != 4 && !executor.GetEnableFileCursor() {
		t.Errorf("5 agg in plan tree, but %d", verifier.AggCount())
	}
	if verifier.AggCount() != 5 && executor.GetEnableFileCursor() {
		t.Errorf("6 agg in plan tree, but %d", verifier.AggCount())
	}
}

func TestAggPushdownToExchangeRuleWithPreCount(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"count"}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewAggPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if verifier.AggCount() != 3 {
		t.Errorf("3 agg in plan tree, but %d", verifier.AggCount())
	}
}

func testAggPushDownToSubQueryRule(t *testing.T, schemaIn, schemaOut *executor.QuerySchema) hybridqp.QueryNode {
	planBuilder := executor.NewLogicalPlanBuilderImpl(schemaIn)
	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Project()
	planBuilder.SubQuery()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	planBuilderOut := executor.NewLogicalPlanBuilderImpl(schemaOut)
	planBuilderOut.Push(plan)
	planBuilderOut.GroupBy()
	planBuilderOut.OrderBy()
	planBuilderOut.Aggregate()
	planBuilderOut.Project()

	if plan, err = planBuilderOut.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	aggToSubquery := executor.NewAggPushDownToSubQueryRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(aggToSubquery)
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	return planner.FindBestExp()
}

func TestAggPushDownToSubQueryRuleWithCountDistinct(t *testing.T) {
	fieldsSub := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			Alias: "a",
		},
	}
	columnsName := []string{"a"}
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt, nil)
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.Call{
						Name: "distinct",
						Args: []influxql.Expr{
							&influxql.VarRef{Val: "a", Type: influxql.Float},
						},
					},
				},
			},
		},
	}
	columnsName = []string{"count"}
	schemaOut := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	best := testAggPushDownToSubQueryRule(t, schema, schemaOut)
	if best == nil {
		t.Error("no best plan found")
	}
	goal := best.Schema().GetColumnNames()[0]
	if goal != "count" {
		t.Errorf("subquery count(disintct) push down failed")
	}
	if !best.Schema().HasCall() {
		t.Errorf("subquery count(disintct) push down failed")
	}
}

func TestAggPushDownToSubQueryRuleWithStr(t *testing.T) {
	fieldsSub := influxql.Fields{}
	columnsName := []string{"str"}
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt, nil)
	stringCall := influxql.Call{
		Name: "str",
		Args: []influxql.Expr{
			&influxql.VarRef{
				Val:  "address",
				Type: influxql.String,
			},
			&influxql.StringLiteral{
				Val: "sh",
			},
		},
	}
	schema.AddString("str(address::string, 'sh')", &stringCall)
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "str",
						Type: influxql.String,
					},
				},
			},
		},
	}
	schemaOut := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	best := testAggPushDownToSubQueryRule(t, schema, schemaOut)
	if best == nil {
		t.Error("no best plan found")
	}
	if !best.Schema().HasCall() {
		t.Errorf("agg can't push down while has string function ")
	}
}

func TestAggPushDownToSubQueryRuleWithAbs(t *testing.T) {
	fieldsSub := influxql.Fields{}
	columnsName := []string{"abs"}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt, nil)
	mathCall := influxql.Call{
		Name: "abs",
		Args: []influxql.Expr{
			&influxql.VarRef{
				Val:  "height",
				Type: influxql.Integer,
			},
		},
	}
	schema.AddMath("abs(height::integer)", &mathCall)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Project()
	planBuilder.SubQuery()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "abs",
						Type: influxql.Integer,
					},
				},
			},
		},
	}

	schemaOut := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilderOut := executor.NewLogicalPlanBuilderImpl(schemaOut)
	planBuilderOut.Push(plan)
	planBuilderOut.GroupBy()
	planBuilderOut.OrderBy()
	planBuilderOut.Aggregate()
	planBuilderOut.Project()

	if plan, err = planBuilderOut.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	aggToSubquery := executor.NewAggPushDownToSubQueryRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(aggToSubquery)
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	if !best.Schema().HasCall() {
		t.Errorf("agg can't push down while has string function ")
	}
}

func TestAggPushDownToSubQueryRuleWithAlias(t *testing.T) {
	fieldsSub := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			Alias: "a",
		},
	}
	columnsName := []string{"a"}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Project()
	planBuilder.SubQuery()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "a",
						Type: influxql.Integer,
					},
				},
			},
		},
	}
	sources := make(influxql.Sources, 0)
	sources = append(sources, &influxql.SubQuery{})
	schemaOut := executor.NewQuerySchemaWithSources(fields, sources, columnsName, &opt, nil)
	planBuilderOut := executor.NewLogicalPlanBuilderImpl(schemaOut)
	planBuilderOut.Push(plan)
	planBuilderOut.GroupBy()
	planBuilderOut.OrderBy()
	planBuilderOut.Aggregate()
	planBuilderOut.Project()

	if plan, err = planBuilderOut.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	aggToSubquery := executor.NewAggPushDownToSubQueryRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(aggToSubquery)
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	goal := best.Schema().GetColumnNames()[0]

	if goal != "a" {
		t.Errorf("subquery has alias push down failed")
	}

	if best.Schema().HasCall() {
		t.Errorf("subquery has alias push down failed")
	}
}

func TestAggPushDownToSubQueryRuleWithAliasWithNoPreAgg1(t *testing.T) {
	config.GetCommon().PreAggEnabled = false
	defer func() {
		config.GetCommon().PreAggEnabled = true
	}()
	fieldsSub := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			Alias: "a",
		},
	}
	columnsName := []string{"a"}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Project()
	planBuilder.SubQuery()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "a",
						Type: influxql.Integer,
					},
				},
			},
		},
	}
	sources := make(influxql.Sources, 0)
	sources = append(sources, &influxql.SubQuery{})
	schemaOut := executor.NewQuerySchemaWithSources(fields, sources, columnsName, &opt, nil)
	planBuilderOut := executor.NewLogicalPlanBuilderImpl(schemaOut)
	planBuilderOut.Push(plan)
	planBuilderOut.GroupBy()
	planBuilderOut.OrderBy()
	planBuilderOut.Aggregate()
	planBuilderOut.Project()

	if plan, err = planBuilderOut.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	aggToSubquery := executor.NewAggPushDownToSubQueryRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(aggToSubquery)
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	goal := best.Schema().GetColumnNames()[0]

	if goal != "a" {
		t.Errorf("subquery has alias push down failed")
	}

	if !best.Schema().HasCall() {
		t.Errorf("subquery has alias WithNoPreAgg1 push down failed")
	}
}

func TestAggPushDownToSubQueryRuleWithAliasWithNoPreAgg2(t *testing.T) {
	fieldsSub := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			Alias: "a",
		},
	}
	columnsName := []string{"a"}
	opt := query.ProcessorOptions{}
	opt.HintType = hybridqp.ExactStatisticQuery
	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Project()
	planBuilder.SubQuery()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "a",
						Type: influxql.Integer,
					},
				},
			},
		},
	}
	sources := make(influxql.Sources, 0)
	sources = append(sources, &influxql.SubQuery{})
	schemaOut := executor.NewQuerySchemaWithSources(fields, sources, columnsName, &opt, nil)
	planBuilderOut := executor.NewLogicalPlanBuilderImpl(schemaOut)
	planBuilderOut.Push(plan)
	planBuilderOut.GroupBy()
	planBuilderOut.OrderBy()
	planBuilderOut.Aggregate()
	planBuilderOut.Project()

	if plan, err = planBuilderOut.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	aggToSubquery := executor.NewAggPushDownToSubQueryRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(aggToSubquery)
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	goal := best.Schema().GetColumnNames()[0]

	if goal != "a" {
		t.Errorf("subquery has alias push down failed")
	}

	if !best.Schema().HasCall() {
		t.Errorf("subquery has alias WithNoPreAgg2 push down failed")
	}
}

func TestAggPushDownToSubQueryRuleWithAliasAndBinary(t *testing.T) {
	fieldsSub := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			Alias: "a",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value1",
				Type: influxql.Float,
			},
			Alias: "b",
		},
	}
	columnsName := []string{"a", "b"}
	opt := query.ProcessorOptions{}

	sources := make(influxql.Sources, 0)
	sources = append(sources, &influxql.SubQuery{
		Statement: &influxql.SelectStatement{},
	})
	schema := executor.NewQuerySchemaWithSources(fieldsSub, sources, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Project()
	planBuilder.SubQuery()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "a",
						Type: influxql.Integer,
					},
				},
			},
		},
		&influxql.Field{
			Expr: &influxql.BinaryExpr{
				Op: influxql.ADD,
				LHS: &influxql.Call{
					Name: "count",
					Args: []influxql.Expr{
						&influxql.VarRef{
							Val:  "a",
							Type: influxql.Integer,
						},
					},
				},
				RHS: &influxql.Call{
					Name: "count",
					Args: []influxql.Expr{
						&influxql.VarRef{
							Val:  "b",
							Type: influxql.Integer,
						},
					},
				},
			},
		},
	}

	columnsNameOut := []string{"count", "count_count"}
	schemaOut := executor.NewQuerySchemaWithSources(fields, sources, columnsNameOut, &opt, nil)
	planBuilderOut := executor.NewLogicalPlanBuilderImpl(schemaOut)
	planBuilderOut.Push(plan)
	planBuilderOut.GroupBy()
	planBuilderOut.OrderBy()
	planBuilderOut.Aggregate()
	planBuilderOut.Project()

	if plan, err = planBuilderOut.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	aggToSubquery := executor.NewAggPushDownToSubQueryRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(aggToSubquery)
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	if best.Schema().HasCall() {
		t.Errorf("subquery has alias push down failed")
	}

	if best.Schema().HasCall() {
		t.Errorf("subquery has alias push down failed")
	}
}

func TestSlideWindowPushDownToExchange(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sliding_window",
				Args: []influxql.Expr{
					&influxql.Call{
						Name: "sum",
						Args: []influxql.Expr{
							&influxql.VarRef{
								Val:  "age",
								Type: influxql.Integer,
							},
						},
					},
					&influxql.IntegerLiteral{
						Val: 3,
					},
				},
			},
		},
	}
	columnsName := []string{"value"}
	opt := query.ProcessorOptions{}
	opt.Interval.Duration = 1000
	sysconfig.OnSlidingWindowPushUp = 1
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	planBuilder.Project()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(executor.NewAggPushDownToSubQueryRule(""))
	planner.AddRule(executor.NewAggToProjectInSubQueryRule(""))
	planner.AddRule(executor.NewReaderUpdateInSubQueryRule(""))

	planner.AddRule(executor.NewLimitPushdownToExchangeRule(""))
	planner.AddRule(executor.NewLimitPushdownToReaderRule(""))
	planner.AddRule(executor.NewLimitPushdownToSeriesRule(""))
	planner.AddRule(executor.NewAggPushdownToExchangeRule(""))
	planner.AddRule(executor.NewAggPushdownToReaderRule(""))
	planner.AddRule(executor.NewAggPushdownToSeriesRule(""))
	planner.AddRule(executor.NewAggSpreadToSortAppendRule(""))
	planner.AddRule(executor.NewAggSpreadToExchangeRule(""))
	planner.AddRule(executor.NewAggSpreadToReaderRule(""))
	planner.AddRule(executor.NewSlideWindowSpreadRule(""))
	planner.SetRoot(plan)
	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewSlideWindowPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if verifier.SlideWindowCount() != 1 {
		t.Errorf("slide window push down failed")
	}
}

func getPlanner() *executor.HeuPlannerImpl {
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	pb.AddRuleCatagory(executor.RULE_HEIMADLL_PUSHDOWN)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(executor.NewAggPushDownToSubQueryRule(""))
	planner.AddRule(executor.NewAggToProjectInSubQueryRule(""))
	planner.AddRule(executor.NewReaderUpdateInSubQueryRule(""))

	planner.AddRule(executor.NewLimitPushdownToExchangeRule(""))
	planner.AddRule(executor.NewLimitPushdownToReaderRule(""))
	planner.AddRule(executor.NewLimitPushdownToSeriesRule(""))
	planner.AddRule(executor.NewAggPushdownToExchangeRule(""))
	planner.AddRule(executor.NewAggPushdownToReaderRule(""))
	planner.AddRule(executor.NewAggPushdownToSeriesRule(""))

	planner.AddRule(executor.NewCastorAggCutRule(""))

	planner.AddRule(executor.NewAggSpreadToSortAppendRule(""))
	planner.AddRule(executor.NewAggSpreadToExchangeRule(""))
	planner.AddRule(executor.NewAggSpreadToReaderRule(""))
	planner.AddRule(executor.NewSlideWindowSpreadRule(""))
	return planner
}

func TestCastorDetectPushDownGroupByAllSeries(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "castor",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"castor"}
	opt := query.ProcessorOptions{}
	opt.GroupByAllDims = true

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)

	var plan hybridqp.QueryNode
	plan = buildPlan(t, schema)

	planner := getPlanner()
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewAggPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if executor.GetEnableFileCursor() && verifier.AggCount() != 1 {
		t.Errorf("only 1 agg in plan tree, but %d", verifier.AggCount())
	}
}

func TestCastorPushDownGroupByNotAllSeries(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "castor",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"castor"}
	opt := query.ProcessorOptions{}
	opt.GroupByAllDims = false

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)

	var plan hybridqp.QueryNode
	plan = buildPlan(t, schema)

	planner := getPlanner()
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewAggPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if executor.GetEnableFileCursor() && verifier.AggCount() != 1 || (!executor.GetEnableFileCursor() && verifier.AggCount() != 1) {
		t.Errorf("only four agg in plan tree, but %d", verifier.AggCount())
	}
}

func TestCastorRuleEquale(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "castor",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"castor"}
	opt := query.ProcessorOptions{}
	opt.GroupByAllDims = false

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)

	var plan hybridqp.QueryNode
	plan = buildPlan(t, schema)

	planner := getPlanner()
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}
	hRule := executor.NewCastorAggCutRule("")
	hRul2 := executor.NewCastorAggCutRule("")
	if !hRule.Equals(hRul2) {
		t.Error("hemidall rule not equal")
	}
	limitRule := executor.NewLimitPushdownToSeriesRule("")
	if hRule.Equals(limitRule) {
		t.Error("hemidall rule not equal")
	}
}

func buildPlan(t *testing.T, schema *executor.QuerySchema) hybridqp.QueryNode {
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}
	return plan
}

type HeuProgramBuilder struct {
	instructions []executor.HeuInstruction
}

// NewHeuProgramBuilder deprecation, only for mock or test
func NewHeuProgramBuilder() *HeuProgramBuilder {
	return &HeuProgramBuilder{
		instructions: nil,
	}
}

func (b *HeuProgramBuilder) InitDefaultRules() {
	rules := []executor.OptRuleCatagory{
		executor.RULE_SUBQUERY,
		executor.RULE_PUSHDOWN_LIMIT,
		executor.RULE_PUSHDOWN_AGG,
		executor.RULE_SPREAD_AGG,
		executor.RULE_HEIMADLL_PUSHDOWN,
	}
	for _, r := range rules {
		b.AddRuleCatagory(r)
	}
}

func (b *HeuProgramBuilder) AddRuleCatagory(ruleCatagory executor.OptRuleCatagory) *HeuProgramBuilder {
	ri := executor.NewRuleInstruction(ruleCatagory)
	b.instructions = append(b.instructions, ri)
	return b
}

func (b *HeuProgramBuilder) Clear() {
	b.instructions = nil
}

func (b *HeuProgramBuilder) Build() *executor.HeuProgram {
	program := executor.NewHeuProgram(b.instructions)
	b.Clear()

	return program
}

func TestAggPushDownToColumnStoreReaderRule(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"sum"}
	opt := query.ProcessorOptions{}
	opt.GroupByAllDims = false
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)
	plan, err := coordinator.CreateColumnStorePlan(schema, nil, planBuilder)
	if err != nil {
		t.Fatal(err)
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}
	planner := getPlanner()
	aggPushDown := executor.NewAggPushDownToColumnStoreReaderRule("")
	planner.AddRule(aggPushDown)
	planner.SetRoot(plan)
	best := planner.FindBestExp()
	if best == nil {
		t.Error("no best plan found")
	}
	verifier := NewAggPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	if verifier.AggCount() != 4 {
		t.Fatalf("4 agg in plan tree, but %d", verifier.AggCount())
	}
	aggPushDown1 := executor.NewAggPushDownToColumnStoreReaderRule("")
	aggPushDown.ToString()
	if !aggPushDown.Equals(aggPushDown1) {
		t.Error("agg push down rule not equal")
	}

	sources := []influxql.Source{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: "mst0", EngineType: config.COLUMNSTORE}}
	sortFields := influxql.SortFields{{Name: "value"}}
	s := executor.NewQuerySchemaWithSources(fields, sources, columnsName, &opt, sortFields)
	assert.Equal(t, s.HasSort(), true)
}

func TestIncAggRule(t *testing.T) {
	rule1 := executor.NewIncAggRule("IncAggRule")
	schema := createQuerySchemaWithCountCalls()
	node := executor.NewLogicalSeries(schema)
	agg := executor.NewLogicalIncAgg(node, schema)
	ruleCall := executor.NewOptRuleCall(nil, rule1.GetOperand(), []hybridqp.QueryNode{agg})
	rule1.OnMatch(ruleCall)
	if rule1.ToString() != "IncAggRule" {
		t.Errorf("IncAggRule has wrong name")
	}
	rule2 := executor.NewIncAggRule("IncAggRule")
	if rule1.Equals(rule2) != true {
		t.Errorf("IncAggRule match failed")
	}
	if rule1.Equals(rule1) != true {
		t.Errorf("IncAggRule match failed")
	}
	rule3 := executor.NewIncHashAggRule("IncAggRule")
	if rule1.Equals(rule3) == true {
		t.Errorf("IncAggRule match failed")
	}
}

func TestIncHashAggRule(t *testing.T) {
	rule1 := executor.NewIncHashAggRule("IncHashAggRule")
	schema := createQuerySchemaWithCountCalls()
	node := executor.NewLogicalSeries(schema)
	agg := executor.NewLogicalIncHashAgg(node, schema)
	ruleCall := executor.NewOptRuleCall(nil, rule1.GetOperand(), []hybridqp.QueryNode{agg})
	rule1.OnMatch(ruleCall)
	if rule1.ToString() != "IncHashAggRule" {
		t.Errorf("IncHashAggRule has wrong name")
	}
	rule2 := executor.NewIncHashAggRule("IncHashAggRule")
	if rule1.Equals(rule2) != true {
		t.Errorf("IncHashAggRule match failed")
	}
	if rule1.Equals(rule1) != true {
		t.Errorf("IncHashAggRule match failed")
	}
	rule3 := executor.NewIncAggRule("IncAggRule")
	if rule1.Equals(rule3) == true {
		t.Errorf("IncHashAggRule match failed")
	}
}

func testAggPushDownBase(t *testing.T, schema *executor.QuerySchema) *AggPushDownVerifier {
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	toExchange := executor.NewAggPushdownToExchangeRule("")
	toMeasurement := executor.NewAggPushdownToReaderRule("")
	toSeries := executor.NewAggPushdownToSeriesRule("")
	spreadToExchange := executor.NewAggSpreadToExchangeRule("")
	spreadToReader := executor.NewAggSpreadToReaderRule("")
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(executor.RULE_SPREAD_AGG)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(toExchange)
	planner.AddRule(toMeasurement)
	planner.AddRule(toSeries)
	planner.AddRule(spreadToExchange)
	planner.AddRule(spreadToReader)
	planner.SetRoot(plan)

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best plan found")
	}

	verifier := NewAggPushDownVerifier()
	hybridqp.WalkQueryNodeInPreOrder(verifier, best)
	return verifier
}

func TestAggPushDownWithRangeVector(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	columnsName := []string{"count"}
	opt := query.ProcessorOptions{PromQuery: true, Range: time.Minute, Step: time.Second}
	opt.Interval.Duration = 1000

	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	verifier := testAggPushDownBase(t, schema)
	if verifier.AggCount() != 4 && !executor.GetEnableFileCursor() {
		t.Errorf("4 agg in plan tree, but %d", verifier.AggCount())
	}
	if verifier.AggCount() != 3 && executor.GetEnableFileCursor() {
		t.Errorf("3 agg in plan tree, but %d", verifier.AggCount())
	}
}

func TestAggPushDownWithPromNestedCall(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.Call{
						Name: "rate_prom",
						Args: []influxql.Expr{
							&influxql.VarRef{Val: "value", Type: influxql.Float},
						},
					},
				},
			},
		},
	}
	columnsName := []string{"count"}
	opt := query.ProcessorOptions{PromQuery: true, Range: time.Minute, Step: time.Second}
	opt.Interval.Duration = 1000
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	verifier := testAggPushDownBase(t, schema)
	if verifier.AggCount() != 4 && !executor.GetEnableFileCursor() {
		t.Errorf("4 agg in plan tree, but %d", verifier.AggCount())
	}
	if verifier.AggCount() != 5 && executor.GetEnableFileCursor() {
		t.Errorf("5 agg in plan tree, but %d", verifier.AggCount())
	}
}

func TestAgg2SubQueryQueryCountDistinct(t *testing.T) {
	fields := influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: "count", Type: influxql.Integer}}}
	outSchema := executor.NewQuerySchema(fields, []string{"count"}, &query.ProcessorOptions{}, nil)
	fields = influxql.Fields{&influxql.Field{Expr: &influxql.Call{Name: "count",
		Args: []influxql.Expr{&influxql.Call{Name: "distinct",
			Args: []influxql.Expr{&influxql.VarRef{Val: "value", Type: influxql.Float}}}}}}}
	schema := executor.NewQuerySchema(fields, []string{"count"}, &query.ProcessorOptions{}, nil)
	rule := executor.NewAggPushDownToSubQueryRule("")
	reader := executor.NewLogicalReader(nil, schema)
	agg := executor.NewLogicalAggregate(reader, schema)
	merge := executor.NewLogicalExchange(agg, executor.READER_EXCHANGE, nil, agg.Schema())
	project := executor.NewLogicalProject(merge, outSchema)
	ruleCall := executor.NewOptRuleCall(nil, rule.GetOperand(), []hybridqp.QueryNode{agg, merge, project})
	rule.OnMatch(ruleCall)
	assert.False(t, project.Schema().HasCall())
}

func TestAggPushdownRuleWithIncondition(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sliding_window",
				Args: []influxql.Expr{
					&influxql.Call{
						Name: "sum",
						Args: []influxql.Expr{
							&influxql.VarRef{
								Val:  "age",
								Type: influxql.Integer,
							},
						},
					},
					&influxql.IntegerLiteral{
						Val: 3,
					},
				},
			},
		},
	}
	columnsName := []string{"value"}
	opt := query.ProcessorOptions{}
	opt.Interval.Duration = 1000
	sysconfig.OnSlidingWindowPushUp = 1
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	schema.SetIsInSubquerySchema(true)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Aggregate()
	planBuilder.Project()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_SUBQUERY)
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_AGG)

	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(executor.NewAggPushDownToSubQueryRule(""))
	planner.AddRule(executor.NewAggPushdownToExchangeRule(""))
	planner.AddRule(executor.NewAggPushdownToReaderRule(""))
	planner.AddRule(executor.NewAggPushdownToSeriesRule(""))
	planner.SetRoot(plan)
	best := planner.FindBestExp()
	if best == nil {
		t.Error("no best plan found")
	}

	var countNodes func(hybridqp.QueryNode) int
	countNodes = func(node hybridqp.QueryNode) int {
		if node == nil {
			return 0
		}
		nodeNum := 1
		for _, child := range node.Children() {
			nodeNum += countNodes(child)
		}
		return nodeNum
	}
	nodeNums := countNodes(best)
	assert.Equal(t, 10, nodeNums)
}

type CountDistinctVisitor struct {
	distinctNum int
}

func (v *CountDistinctVisitor) Visit(node hybridqp.QueryNode) hybridqp.QueryNodeVisitor {
	if _, ok := node.(*executor.LogicalDistinct); ok {
		v.distinctNum++
	}
	return v
}

func testDistinctPushDownToExchangeRuleBase(t *testing.T, schema hybridqp.Catalog) *CountDistinctVisitor {
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateShardPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.Project()
	planBuilder.SubQuery()
	planBuilder.Distinct()

	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}

	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_PUSHDOWN_DISTINCT)
	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(executor.NewDistinctPushDownToExchangeRule(""))
	planner.SetRoot(plan)
	best := planner.FindBestExp()
	if best == nil {
		t.Error("no best plan found")
	}
	visitor := &CountDistinctVisitor{}
	hybridqp.WalkQueryNodeInPreOrder(visitor, best)
	return visitor
}

func TestDistinctPushDownToExchangeRule(t *testing.T) {
	rule := executor.NewDistinctPushDownToExchangeRule("DistinctPushDownToExchangeRule")

	// Test Catagory method
	if rule.Catagory() != executor.RULE_PUSHDOWN_DISTINCT {
		t.Errorf("Expected Catagory to return %v, but got %v", executor.RULE_PUSHDOWN_DISTINCT, rule.Catagory())
	}

	// Test ToString method
	if rule.ToString() != "DistinctPushDownToExchangeRule" {
		t.Errorf("Expected ToString to return %v, but got %v", "DistinctPushDownToExchangeRule", rule.ToString())
	}

	// Test Equals method
	otherRule := executor.NewDistinctPushDownToExchangeRule("DistinctPushDownToExchangeRule")
	if !rule.Equals(otherRule) {
		t.Errorf("Expected Equals to return true, but got false")
	}

	// case1: Push distinct down to shardExchange
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	columnsName := []string{"value"}
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	visitor := testDistinctPushDownToExchangeRuleBase(t, schema)
	assert.Equal(t, 2, visitor.distinctNum)

	// case2: Distinct cannot be pushed down to shardExchange because of aggregation.
	fields = influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "age",
						Type: influxql.Integer,
					},
				},
			},
		},
	}
	schema = executor.NewQuerySchema(fields, columnsName, &opt, nil)
	visitor = testDistinctPushDownToExchangeRuleBase(t, schema)
	assert.Equal(t, 1, visitor.distinctNum)

	// case3: Distinct cannot be pushed down to shardExchange because of in subquery.
	fields = influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	schema = executor.NewQuerySchema(fields, columnsName, &opt, nil)
	schema.SetIsInSubquerySchema(true)
	visitor = testDistinctPushDownToExchangeRuleBase(t, schema)
	assert.Equal(t, 1, visitor.distinctNum)

	// case4: Distinct cannot be pushed down to shardExchange because of dimension.
	fields = influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	opt.Dimensions = []string{"region"}
	schema = executor.NewQuerySchema(fields, columnsName, &opt, nil)
	visitor = testDistinctPushDownToExchangeRuleBase(t, schema)
	assert.Equal(t, 1, visitor.distinctNum)

	// case5: Distinct cannot be pushed down to shardExchange because of limit.
	fields = influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	opt.Limit = 1
	schema = executor.NewQuerySchema(fields, columnsName, &opt, nil)
	visitor = testDistinctPushDownToExchangeRuleBase(t, schema)
	assert.Equal(t, 1, visitor.distinctNum)
}
