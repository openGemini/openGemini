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

package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mitchellh/copystructure"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

type ContextKey string

const (
	NowKey ContextKey = "now"
)

// Select compiles, prepares, and then initiates execution of the query using the
// default compile options.
func Select(ctx context.Context, stmt *influxql.SelectStatement, shardMapper query.ShardMapper, opt query.SelectOptions) (hybridqp.Executor, error) {
	start := time.Now()
	s, err := query.Prepare(stmt, shardMapper, opt)
	if err != nil {
		return nil, err
	}
	qStat, ok := ctx.Value(query.QueryDurationKey).(*statistics.SQLSlowQueryStatistics)
	if !ok {
		return nil, errors.New("Select context value type illegal ")
	}
	if qStat != nil {
		qStat.AddDuration("PrepareDuration", time.Since(start).Nanoseconds())
	}
	// Must be deferred so it runs after Select.
	defer util.MustClose(s)

	return s.Select(ctx)
}

func defaultQueryExecutorBuilderCreator() hybridqp.PipelineExecutorBuilder {
	return NewQueryExecutorBuilder(GetEnableBinaryTreeMerge())
}

type preparedStatement struct {
	stmt *influxql.SelectStatement
	opt  hybridqp.Options
	qc   interface {
		query.LogicalPlanCreator
		io.Closer
	}
	creator   hybridqp.ExecutorBuilderCreator
	optimizer hybridqp.ExecutorBuilderOptimizer
	columns   []string
	maxPointN int
	now       time.Time
}

func NewPreparedStatement(stmt *influxql.SelectStatement, opt hybridqp.Options,
	shards interface {
		query.LogicalPlanCreator
		io.Closer
	}, columns []string, MaxPointN int, now time.Time) *preparedStatement {
	return &preparedStatement{
		stmt:      stmt,
		opt:       opt,
		qc:        shards,
		creator:   defaultQueryExecutorBuilderCreator,
		optimizer: buildHeuristicPlanner,
		columns:   columns,
		maxPointN: MaxPointN,
		now:       now,
	}
}

func (p *preparedStatement) Select(ctx context.Context) (hybridqp.Executor, error) {
	if len(p.stmt.Fields) == 0 {
		return nil, nil
	}

	ctx = context.WithValue(ctx, NowKey, p.now)

	opt, ok := p.opt.(*query.ProcessorOptions)
	if !ok {
		return nil, errors.New("preparedStatement Select p.opt isn't *query.ProcessorOptions type")
	}
	opt.EnableBinaryTreeMerge = GetEnableBinaryTreeMerge()

	schema := NewQuerySchemaWithSources(p.stmt.Fields, p.stmt.Sources, p.stmt.ColumnNames(), opt)

	plan, err := buildSender(ctx, p.stmt, p.qc, schema)

	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}

	if GetEnablePrintLogicalPlan() == OnPrintLogicalPlan {
		planWriter := NewLogicalPlanWriterImpl(&strings.Builder{})
		plan.(LogicalPlan).Explain(planWriter)
		fmt.Println("origin plan\n", planWriter.String())
	}

	executorBuilder := p.creator()

	planner := p.optimizer()

	planner.SetRoot(plan)
	best := planner.FindBestExp()

	if GetEnablePrintLogicalPlan() == OnPrintLogicalPlan {
		planWriter := NewLogicalPlanWriterImpl(&strings.Builder{})
		best.(LogicalPlan).Explain(planWriter)
		fmt.Println("optimized plan\n", planWriter.String())
	}

	span := tracing.SpanFromContext(ctx)
	if span != nil {
		executorBuilder.Analyze(span)
	}

	return executorBuilder.Build(best)
}

func (p *preparedStatement) ChangeCreator(creator hybridqp.ExecutorBuilderCreator) {
	p.creator = creator
}

func (p *preparedStatement) ChangeOptimizer(optimizer hybridqp.ExecutorBuilderOptimizer) {
	p.optimizer = optimizer
}

func (p *preparedStatement) Explain() (string, error) {
	return "", nil
}

func (p *preparedStatement) Close() error {
	return p.qc.Close()
}

func buildHeuristicPlanner() hybridqp.Planner {
	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(RULE_SUBQUERY)
	pb.AddRuleCatagory(RULE_PUSHDOWN_LIMIT)
	pb.AddRuleCatagory(RULE_PUSHDOWN_AGG)
	pb.AddRuleCatagory(RULE_SPREAD_AGG)
	pb.AddRuleCatagory(RULE_HEIMADLL_PUSHDOWN)
	planner := NewHeuPlannerImpl(pb.Build())

	// subquery
	planner.AddRule(NewAggPushDownToSubQueryRule(""))
	planner.AddRule(NewAggToProjectInSubQueryRule(""))
	planner.AddRule(NewReaderUpdateInSubQueryRule(""))

	planner.AddRule(NewLimitPushdownToExchangeRule(""))
	planner.AddRule(NewLimitPushdownToReaderRule(""))
	planner.AddRule(NewLimitPushdownToSeriesRule(""))
	planner.AddRule(NewAggPushdownToExchangeRule(""))
	planner.AddRule(NewAggPushdownToReaderRule(""))
	planner.AddRule(NewAggPushdownToSeriesRule(""))

	planner.AddRule(NewCastorAggCutRule(""))

	planner.AddRule(NewAggSpreadToSortAppendRule(""))
	planner.AddRule(NewAggSpreadToExchangeRule(""))
	planner.AddRule(NewAggSpreadToReaderRule(""))
	planner.AddRule(NewSlideWindowSpreadRule(""))
	return planner
}

func buildSortAppendQueryPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.SelectStatement, schema *QuerySchema) (hybridqp.QueryNode, error) {
	joinNodes := make([]hybridqp.QueryNode, 0, len(stmt.Sources))
	for i := range stmt.Sources {
		source, e := copystructure.Copy(stmt.Sources[i])
		if e != nil {
			return nil, e
		}
		optSource := influxql.Sources{source.(influxql.Source)}
		childOpt := schema.opt.(*query.ProcessorOptions).Clone()
		childOpt.UpdateSources(optSource)
		s := NewQuerySchemaWithSources(stmt.Fields, influxql.Sources{stmt.Sources[i]}, stmt.ColumnNames(), childOpt)
		child, err := buildSources(ctx, qc, influxql.Sources{stmt.Sources[i]}, s)
		if err != nil {
			return nil, err
		}
		if child != nil {
			joinNodes = append(joinNodes, child)
		}
		schema.sources = append(schema.sources, source.(influxql.Source))
	}

	if len(joinNodes) == 0 {
		return nil, nil
	}
	return NewLogicalSortAppend(joinNodes, schema), nil
}

func hasDistinctSelectorCall(s *QuerySchema) (bool, bool) {
	var hasDistinct, hasSelector bool
	for _, c := range s.calls {
		if c.Name == "distinct" {
			hasDistinct = true
			hasSelector = true
		}
		if c.Name == "top" || c.Name == "bottom" {
			hasSelector = true
		}
	}
	return hasDistinct, hasSelector
}

func buildAggNode(builder *LogicalPlanBuilderImpl, schema hybridqp.Catalog, hasSlidingWindow bool) {
	if hasSlidingWindow && (!schema.CanAggPushDown() ||
		(schema.CanAggPushDown() && GetEnableSlidingWindowPushUp() == OnSlidingWindowPushUp) || schema.HasSubQuery()) {
		builder.SlidingWindow()
	} else {
		builder.Aggregate()
	}
}

func buildNodes(builder *LogicalPlanBuilderImpl, schema hybridqp.Catalog, s *QuerySchema) {
	hasDistinct, hasSelector := hasDistinctSelectorCall(s)
	hasSlidingWindow := schema.HasSlidingWindowCall()

	if len(schema.Calls()) > 0 {
		buildAggNode(builder, schema, hasSlidingWindow)
	}

	if schema.CountDistinct() != nil {
		builder.CountDistinct()
	}

	if !hasSelector && !hasSlidingWindow && s.opt.HasInterval() || hasDistinct {
		builder.Interval()
	}

	builder.Project()

	if schema.HasBlankRowCall() {
		builder.FilterBlank()
	}

	if !hasSelector && !hasSlidingWindow && s.opt.HasInterval() && s.opt.(*query.ProcessorOptions).Fill != influxql.NoFill {
		builder.Fill()
	}

	// Apply limit & offset.
	if schema.HasLimit() {
		limitType := schema.LimitType()
		limit, offset := schema.LimitAndOffset()
		builder.Limit(LimitTransformParameters{
			Limit:     limit,
			Offset:    offset,
			LimitType: limitType,
		})
	}
}

func buildQueryPlan(ctx context.Context, stmt *influxql.SelectStatement, qc query.LogicalPlanCreator, schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	var sp hybridqp.QueryNode
	var err error
	builder := NewLogicalPlanBuilderImpl(schema)

	s, ok := schema.(*QuerySchema)
	if !ok {
		return nil, errors.New("buildQueryPlan schema type isn't *QuerySchema")
	}
	if stmt.Sources = qc.GetSources(stmt.Sources); len(stmt.Sources) > 1 {
		sp, err = buildSortAppendQueryPlan(ctx, qc, stmt, s)
	} else {
		sp, err = buildSources(ctx, qc, stmt.Sources, s)
	}
	if err != nil {
		return nil, err
	}
	if sp == nil {
		return nil, nil
	}
	builder.Push(sp)

	buildNodes(builder, schema, s)

	return builder.Build()
}

func buildSender(ctx context.Context, stmt *influxql.SelectStatement, qc query.LogicalPlanCreator, schema *QuerySchema) (hybridqp.QueryNode, error) {
	builder := NewLogicalPlanBuilderImpl(schema)
	queryPlan, err := buildQueryPlan(ctx, stmt, qc, schema)
	if err != nil {
		return nil, err
	}
	if queryPlan == nil {
		return nil, nil
	}
	builder.Push(queryPlan)

	if schema.Options().(*query.ProcessorOptions).HintType == hybridqp.FilterNullColumn {
		builder.HttpSenderHint()
	} else {
		builder.HttpSender()
	}

	return builder.Build()
}

func buildSources(ctx context.Context, qc query.LogicalPlanCreator, sources influxql.Sources, schema *QuerySchema) (hybridqp.QueryNode, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	switch source := sources[0].(type) {
	case *influxql.Measurement:
		plan, err := qc.CreateLogicalPlan(ctx, sources, schema)
		if err != nil {
			return nil, err
		}
		if plan == nil {
			return nil, nil
		}
		return plan, nil
	case *influxql.SubQuery:
		builder := NewLogicalPlanBuilderImpl(schema)
		subQueryBuilder := SubQueryBuilder{
			qc:   qc,
			stmt: source.Statement,
		}
		subQueryPlan, err := subQueryBuilder.Build(ctx, *schema.Options().(*query.ProcessorOptions))
		if subQueryPlan == nil {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		builder.Push(subQueryPlan)
		if schema.Options().GetCondition() != nil {
			builder.Filter()
		}
		builder.SubQuery()
		builder.GroupBy()
		builder.OrderBy()
		return builder.Build()

	default:
		return nil, nil
	}
}

var _ = query.RegistryStmtBuilderCreator(&PrepareStmtBuilderCreator{})

type PrepareStmtBuilderCreator struct {
}

func (p *PrepareStmtBuilderCreator) Create(stmt *influxql.SelectStatement, opt hybridqp.Options,
	shards interface {
		query.LogicalPlanCreator
		io.Closer
	}, columns []string, MaxPointN int, now time.Time) query.PreparedStatement {
	return NewPreparedStatement(stmt, opt, shards, columns, MaxPointN, now)

}
