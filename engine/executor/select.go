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
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
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
		optimizer: BuildHeuristicPlanner,
		columns:   columns,
		maxPointN: MaxPointN,
		now:       now,
	}
}

func (p *preparedStatement) Statement() *influxql.SelectStatement {
	return p.stmt
}

func (p *preparedStatement) buildPlanByCache(ctx context.Context, schema *QuerySchema, plan []hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	p.stmt.Sources = p.qc.GetSources(p.stmt.Sources)
	eTraits, err := p.qc.GetETraits(ctx, p.stmt.Sources, schema)
	if eTraits == nil || err != nil {
		return nil, err
	}
	return NewPlanBySchemaAndSrcPlan(schema, plan, eTraits)
}

func (p *preparedStatement) BuildLogicalPlan(ctx context.Context) (hybridqp.QueryNode, error) {
	if len(p.stmt.Fields) == 0 {
		return nil, nil
	}

	ctx = context.WithValue(ctx, NowKey, p.now)

	opt, ok := p.opt.(*query.ProcessorOptions)
	if !ok {
		return nil, errors.New("preparedStatement Select p.opt isn't *query.ProcessorOptions type")
	}
	opt.EnableBinaryTreeMerge = GetEnableBinaryTreeMerge()

	rewriteVarfName(p.stmt.Fields)

	schema := NewQuerySchemaWithJoinCase(p.stmt.Fields, p.stmt.Sources, p.stmt.ColumnNames(), opt, p.stmt.JoinSource, p.stmt.SortFields)

	HaveOnlyCSStore := schema.Sources().HaveOnlyCSStore()
	planType := GetPlanType(schema, p.stmt)
	if planType != UNKNOWN {
		templatePlan := SqlPlanTemplate[planType].GetPlan()
		if p != nil && !HaveOnlyCSStore {
			schema.SetPlanType(planType)
			return p.buildPlanByCache(ctx, schema, templatePlan)
		}
	}
	plan, err := buildExtendedPlan(ctx, p.stmt, p.qc, schema)

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
	planner := p.optimizer()

	planner.SetRoot(plan)
	best := planner.FindBestExp()

	if HaveOnlyCSStore {
		best = RebuildColumnStorePlan(best)[0]
		RebuildAggNodes(best)
	}

	if GetEnablePrintLogicalPlan() == OnPrintLogicalPlan {
		planWriter := NewLogicalPlanWriterImpl(&strings.Builder{})
		best.(LogicalPlan).Explain(planWriter)
		fmt.Println("optimized plan\n", planWriter.String())
	}
	return best, nil
}

func (p *preparedStatement) Select(ctx context.Context) (hybridqp.Executor, error) {
	best, err := p.BuildLogicalPlan(ctx)
	if err != nil {
		return nil, err
	}
	if ok, err := p.isSchemaOverLimit(best); ok {
		return nil, err
	}
	executorBuilder := p.creator()
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		executorBuilder.Analyze(span)
	}
	return executorBuilder.Build(best)
}

func rewriteVarfName(fields influxql.Fields) {
	for i := range fields {
		if f, ok := fields[i].Expr.(*influxql.VarRef); ok && len(fields[i].Alias) == 0 {
			fields[i].Alias = f.Alias
		}
	}
}

func (p *preparedStatement) isSchemaOverLimit(best hybridqp.QueryNode) (bool, error) {
	if best == nil {
		logger.GetLogger().Error("nil plan", zap.Any("stmt", p.stmt))
		return false, nil
	}
	querySchemaLimit := GetQuerySchemaLimit()
	schemaNum := len(best.Schema().Options().GetDimensions()) + best.Schema().Fields().Len()
	if querySchemaLimit > 0 && schemaNum > querySchemaLimit {
		return true, errno.NewError(errno.ErrQuerySchemaUpperBound, schemaNum, querySchemaLimit)

	}
	return false, nil
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
		s := NewQuerySchemaWithJoinCase(stmt.Fields, influxql.Sources{stmt.Sources[i]}, stmt.ColumnNames(), childOpt, stmt.JoinSource, stmt.SortFields)
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

func BuildInConditionPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.SelectStatement, schema *QuerySchema) (hybridqp.QueryNode, hybridqp.Catalog, error) {
	c, _ := schema.Options().GetCondition().(*influxql.InCondition)
	joinNodes := make([]hybridqp.QueryNode, 0, len(stmt.Sources))
	opt, _ := schema.Options().(*query.ProcessorOptions)
	sopt := query.SelectOptions{
		MaxSeriesN:       opt.MaxSeriesN,
		Authorizer:       opt.Authorizer,
		ChunkedSize:      opt.ChunkedSize,
		Chunked:          opt.Chunked,
		ChunkSize:        opt.ChunkSize,
		MaxQueryParallel: opt.MaxParallel,
		AbortChan:        opt.AbortChan,
		RowsChan:         opt.RowsChan,
	}
	c.Stmt.Sources = qc.GetSources(c.Stmt.Sources)
	stmt.Sources = qc.GetSources(stmt.Sources)
	rightOpt, _ := query.NewProcessorOptionsStmt(c.Stmt, sopt)
	sRight := NewQuerySchemaWithJoinCase(c.Stmt.Fields, c.Stmt.Sources, c.Stmt.ColumnNames(), &rightOpt, c.Stmt.JoinSource, c.Stmt.SortFields)
	right, err := buildSources(ctx, qc, c.Stmt.Sources, sRight)
	if err != nil {
		return nil, nil, err
	}
	if right != nil {
		joinNodes = append(joinNodes, right)
	}
	leftOpt := opt.Clone()
	sLeft := NewQuerySchemaWithJoinCase(stmt.Fields, stmt.Sources, stmt.ColumnNames(), leftOpt, stmt.JoinSource, stmt.SortFields)
	left, err := buildSources(ctx, qc, stmt.Sources, sLeft)
	if err != nil {
		return nil, nil, err
	}
	if left != nil {
		joinNodes = append(joinNodes, left)
	}
	joinSchema := NewQuerySchemaWithJoinCase(append(c.Stmt.Fields, stmt.Fields...), c.Stmt.Sources, append(c.Stmt.ColumnNames(), stmt.ColumnNames()...), opt, c.Stmt.JoinSource, c.Stmt.SortFields)
	return NewLogicalJoin(joinNodes, joinSchema), joinSchema, nil
}

func buildFullJoinQueryPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.SelectStatement, schema *QuerySchema) (hybridqp.QueryNode, error) {
	joinCases := schema.GetJoinCases()
	if len(joinCases) != 1 {
		return nil, fmt.Errorf("only surrport two subquery join")
	}
	var joinConditon influxql.Expr
	joinNodes := make([]hybridqp.QueryNode, 0, len(stmt.Sources))

	for i := range stmt.Sources {
		source, e := copystructure.Copy(stmt.Sources[i])
		if e != nil {
			return nil, e
		}
		optSource := influxql.Sources{source.(influxql.Source)}
		childOpt := schema.opt.(*query.ProcessorOptions).Clone()
		childOpt.UpdateSources(optSource)
		s := NewQuerySchemaWithSources(stmt.Fields, influxql.Sources{stmt.Sources[i]}, stmt.ColumnNames(), childOpt, nil)
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
	return NewLogicalFullJoin(joinNodes[0], joinNodes[1], joinConditon, schema), nil
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
	hasHoltWinters := schema.HasHoltWintersCall()
	hasSort := s.HasSort()
	HaveOnlyCSStore := schema.Options().HaveOnlyCSStore()

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

	if hasHoltWinters {
		builder.HoltWinters()
	}

	// not support fill: selector, slidingWindow, holtWinters
	if !hasSelector && !hasSlidingWindow && !hasHoltWinters && s.opt.HasInterval() &&
		s.opt.(*query.ProcessorOptions).Fill != influxql.NoFill && !HaveOnlyCSStore {
		builder.Fill()
	}

	if hasSort && HaveOnlyCSStore {
		builder.Sort()
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
	if _, ok = schema.Options().GetCondition().(*influxql.InCondition); ok {
		sp, schema, err = BuildInConditionPlan(ctx, qc, stmt, s)
	} else if schema.GetJoinCaseCount() > 0 && len(stmt.Sources) == 2 {
		sp, err = buildFullJoinQueryPlan(ctx, qc, stmt, s)
	} else if stmt.Sources = qc.GetSources(stmt.Sources); len(stmt.Sources) > 1 {
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

func buildExtendedPlan(ctx context.Context, stmt *influxql.SelectStatement, qc query.LogicalPlanCreator, schema *QuerySchema) (hybridqp.QueryNode, error) {
	builder := NewLogicalPlanBuilderImpl(schema)
	queryPlan, err := buildQueryPlan(ctx, stmt, qc, schema)
	if err != nil {
		return nil, err
	}
	if queryPlan == nil {
		return nil, nil
	}
	builder.Push(queryPlan)

	if stmt.Target != nil {
		builder.Target(stmt.Target.Measurement)
	}

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

func RebuildColumnStorePlan(plan hybridqp.QueryNode) []hybridqp.QueryNode {
	if plan.Children() == nil {
		return []hybridqp.QueryNode{plan}
	}
	var replace bool
	var nodes []hybridqp.QueryNode
	for _, child := range plan.Children() {
		if child == nil {
			continue
		}
		p := RebuildColumnStorePlan(child)
		switch n := plan.(type) {
		case *LogicalExchange:
			var eType ExchangeType
			var eTraits []hybridqp.Trait
			if n.eType == NODE_EXCHANGE {
				eType = NODE_EXCHANGE
				eTraits = n.eTraits
			} else if n.eType == READER_EXCHANGE {
				eType = READER_EXCHANGE
			}
			if plan.Schema().HasCall() && (plan.Schema().CanAggPushDown() || eType == NODE_EXCHANGE) {
				node := NewLogicalHashAgg(p[0], plan.Schema(), eType, eTraits)
				if node.schema.HasCall() {
					n := findChildAggNode(plan)
					if n != nil {
						node.LogicalPlanBase = n.(*LogicalAggregate).LogicalPlanBase
						node.inputs = p
					}
				}
				nodes = append(nodes, node)
			} else {
				node := NewLogicalHashMerge(p[0], plan.Schema(), eType, eTraits)
				nodes = append(nodes, node)
			}
		default:
			nodes = append(nodes, p...)
			replace = true
		}
	}
	if replace {
		plan.ReplaceChildren(nodes)
		return []hybridqp.QueryNode{plan}
	}
	return nodes
}

func findChildAggNode(plan hybridqp.QueryNode) hybridqp.QueryNode {
	for plan != nil {
		switch plan.(type) {
		case *LogicalAggregate:
			return plan
		default:
			if len(plan.Children()) != 0 {
				plan = plan.Children()[0]
			} else {
				return nil
			}
		}
	}
	return nil
}

func RebuildAggNodes(plan hybridqp.QueryNode) {
	if len(plan.Children()) == 0 {
		return
	}
	if len(plan.Children()) > 1 {
		for i := range plan.Children() {
			RebuildAggNodes(plan.Children()[i])
		}
	}
	if childPlan, ok := plan.Children()[0].(*LogicalAggregate); ok {
		switch childPlan.Children()[0].(type) {
		case *LogicalOrderBy, *LogicalSortAppend, *LogicalFullJoin:
		default:
			plan.SetInputs(plan.Children()[0].Children())
		}
	}
	RebuildAggNodes(plan.Children()[0])
}
