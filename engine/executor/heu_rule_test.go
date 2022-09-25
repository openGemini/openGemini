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
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
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

	schema := executor.NewQuerySchema(fields, columnsName, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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

	schema := executor.NewQuerySchema(fields, columnsName, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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

	schema := executor.NewQuerySchema(fields, columnsName, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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
	if verifier.AggCount() != 5 {
		t.Errorf("5 agg in plan tree, but %d", verifier.AggCount())
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

	schema := executor.NewQuerySchema(fields, columnsName, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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

func TestAggPushDownToSubQueryRuleWithStr(t *testing.T) {
	fieldsSub := influxql.Fields{}
	columnsName := []string{"str"}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt)
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
						Val:  "str",
						Type: influxql.String,
					},
				},
			},
		},
	}

	schemaOut := executor.NewQuerySchema(fields, columnsName, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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

func TestAggPushDownToSubQueryRuleWithAbs(t *testing.T) {
	fieldsSub := influxql.Fields{}
	columnsName := []string{"abs"}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt)
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

	schemaOut := executor.NewQuerySchema(fields, columnsName, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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

	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt)
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

	schemaOut := executor.NewQuerySchema(fields, columnsName, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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

	schema := executor.NewQuerySchema(fieldsSub, columnsName, &opt)
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
	schemaOut := executor.NewQuerySchema(fields, columnsNameOut, &opt)
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
	pb := executor.NewHeuProgramBuilder()
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
	executor.OnSlidingWindowPushUp = 1
	schema := executor.NewQuerySchema(fields, columnsName, &opt)
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

	pb := executor.NewHeuProgramBuilder()
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
