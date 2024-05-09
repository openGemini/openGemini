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
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

type ContextKey string

const (
	NowKey ContextKey = "now"
)

var localStorageForQuery hybridqp.StoreEngine

func SetLocalStorageForQuery(storage hybridqp.StoreEngine) {
	localStorageForQuery = storage
}

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
	if s.Statement().SubQueryHasDifferentAscending && s.Statement().Sources.HaveOnlyTSStore() {
		return nil, errors.New("subqueries must be ordered in the same direction as the query itself")
	}
	return s.Select(ctx)
}

func defaultQueryExecutorBuilderCreator() hybridqp.PipelineExecutorBuilder {
	return NewQueryExecutorBuilder(sysconfig.GetEnableBinaryTreeMerge())
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

func (p *preparedStatement) buildPlanByCache(ctx context.Context, schema *QuerySchema, plan []hybridqp.QueryNode, mstsReqs *[]*MultiMstReqs) (hybridqp.QueryNode, error) {
	p.stmt.Sources = p.qc.GetSources(p.stmt.Sources)
	eTraits, err := p.qc.GetETraits(ctx, p.stmt.Sources, schema)
	if eTraits == nil || err != nil {
		return nil, err
	}
	if localStorageForQuery != nil {
		if len(eTraits) > 0 {
			*mstsReqs = append(*mstsReqs, NewMultiMstReqs())
			for _, e := range eTraits {
				(*mstsReqs)[len(*mstsReqs)-1].reqs = append((*mstsReqs)[len(*mstsReqs)-1].reqs, e.(*RemoteQuery))
			}
		}
		return NewPlanBySchemaAndSrcPlan(schema, plan, eTraits, true)
	}
	return NewPlanBySchemaAndSrcPlan(schema, plan, eTraits, false)
}

func (p *preparedStatement) removeNodeLogicalExchange(plan hybridqp.QueryNode, mstsReqs *[]*MultiMstReqs) hybridqp.QueryNode {
	if len(plan.Children()) == 0 {
		return plan
	}
	for i := range plan.Children() {
		if nodeExchange, ok := plan.Children()[i].(*LogicalExchange); ok {
			if nodeExchange.eType == NODE_EXCHANGE {
				eTraits := nodeExchange.eTraits
				if len(nodeExchange.Children()) == 1 {
					plan.Children()[i] = nodeExchange.Children()[0]
					if len(eTraits) > 0 {
						*mstsReqs = append(*mstsReqs, NewMultiMstReqs())
						for _, e := range eTraits {
							(*mstsReqs)[len(*mstsReqs)-1].reqs = append((*mstsReqs)[len(*mstsReqs)-1].reqs, e.(*RemoteQuery))
						}
					}
					continue
				}
			}
		}
		p.removeNodeLogicalExchange(plan.Children()[i], mstsReqs)
	}

	return plan
}

type MultiMstReqs struct {
	reqs []*RemoteQuery
}

func NewMultiMstReqs() *MultiMstReqs {
	return &MultiMstReqs{
		reqs: make([]*RemoteQuery, 0),
	}
}

func (p *preparedStatement) BuildLogicalPlan(ctx context.Context) (hybridqp.QueryNode, hybridqp.Trait, error) {
	if len(p.stmt.Fields) == 0 {
		return nil, nil, nil
	}
	var mstsReqs []*MultiMstReqs = make([]*MultiMstReqs, 0)
	ctx = context.WithValue(ctx, NowKey, p.now)

	opt, ok := p.opt.(*query.ProcessorOptions)
	if !ok {
		return nil, mstsReqs, errors.New("preparedStatement Select p.opt isn't *query.ProcessorOptions type")
	}
	opt.EnableBinaryTreeMerge = sysconfig.GetEnableBinaryTreeMerge()

	rewriteVarfName(p.stmt.Fields)

	schema := NewQuerySchemaWithJoinCase(p.stmt.Fields, p.stmt.Sources, p.stmt.ColumnNames(), opt, p.stmt.JoinSource,
		p.stmt.UnnestSource, p.stmt.SortFields)

	HaveOnlyCSStore := schema.Sources().HaveOnlyCSStore()
	planType := GetPlanType(schema, p.stmt)
	if planType != UNKNOWN {
		if p != nil && !HaveOnlyCSStore {
			var templatePlan []hybridqp.QueryNode
			if localStorageForQuery != nil {
				templatePlan = SqlPlanTemplate[planType].GetLocalStorePlan()
			} else {
				templatePlan = SqlPlanTemplate[planType].GetPlan()
			}
			schema.SetPlanType(planType)
			plan, err := p.buildPlanByCache(ctx, schema, templatePlan, &mstsReqs)
			PrintPlan("template plan", plan)
			return plan, mstsReqs, err
		}
	}
	plan, err := buildExtendedPlan(ctx, p.stmt, p.qc, schema)

	if err != nil {
		return nil, mstsReqs, err
	}
	if plan == nil {
		return nil, mstsReqs, nil
	}

	PrintPlan("origin plan", plan)
	if localStorageForQuery != nil && !HaveOnlyCSStore {
		p.removeNodeLogicalExchange(plan, &mstsReqs)
		PrintPlan("ts-server plan", plan)
	}
	planner := p.optimizer()

	planner.SetRoot(plan)
	best := planner.FindBestExp()

	if HaveOnlyCSStore {
		if schema.Options().IsUnifyPlan() {
			if best.Schema().HasCall() {
				if !best.Schema().CanSeqAggPushDown() {
					best = ReplaceSortAggMergeWithHashAgg(best)[0]
					best = ReplaceSortMergeWithHashMerge(best)[0]
				}
			} else {
				best = ReplaceSortMergeWithHashMerge(best)[0]
			}
		} else {
			best = RebuildColumnStorePlan(best)[0]
			RebuildAggNodes(best)
			best = ReplaceSortAggWithHashAgg(best)[0]
		}
	}

	PrintPlan("optimized plan", best)
	return best, mstsReqs, nil
}

func (p *preparedStatement) Select(ctx context.Context) (hybridqp.Executor, error) {
	best, req, err := p.BuildLogicalPlan(ctx)
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
	// ts-server + tsEngine remove node_exchange
	if localStorageForQuery != nil {
		executorBuilder.(*ExecutorBuilder).SetInfosAndTraits(req.([]*MultiMstReqs), ctx)
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
	querySchemaLimit := sysconfig.GetQuerySchemaLimit()
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
		source := influxql.CloneSource(stmt.Sources[i])
		optSource := influxql.Sources{source}
		childOpt := schema.opt.(*query.ProcessorOptions).Clone()
		childOpt.UpdateSources(optSource)
		s := NewQuerySchemaWithJoinCase(stmt.Fields, influxql.Sources{stmt.Sources[i]}, stmt.ColumnNames(), childOpt, stmt.JoinSource,
			stmt.UnnestSource, stmt.SortFields)
		child, err := BuildSources(ctx, qc, influxql.Sources{stmt.Sources[i]}, s, false)
		if err != nil {
			return nil, err
		}
		if child != nil {
			joinNodes = append(joinNodes, child)
		}
		schema.sources = append(schema.sources, source)
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
	sRight := NewQuerySchemaWithJoinCase(c.Stmt.Fields, c.Stmt.Sources, c.Stmt.ColumnNames(), &rightOpt, c.Stmt.JoinSource,
		c.Stmt.UnnestSource, c.Stmt.SortFields)
	right, err := BuildSources(ctx, qc, c.Stmt.Sources, sRight, false)
	if err != nil {
		return nil, nil, err
	}
	if right != nil {
		joinNodes = append(joinNodes, right)
	}
	leftOpt := opt.Clone()
	sLeft := NewQuerySchemaWithJoinCase(stmt.Fields, stmt.Sources, stmt.ColumnNames(), leftOpt, stmt.JoinSource,
		stmt.UnnestSource, stmt.SortFields)
	left, err := BuildSources(ctx, qc, stmt.Sources, sLeft, false)
	if err != nil {
		return nil, nil, err
	}
	if left != nil {
		joinNodes = append(joinNodes, left)
	}
	joinSchema := NewQuerySchemaWithJoinCase(append(c.Stmt.Fields, stmt.Fields...), c.Stmt.Sources, append(c.Stmt.ColumnNames(), stmt.ColumnNames()...), opt,
		c.Stmt.JoinSource, c.Stmt.UnnestSource, c.Stmt.SortFields)
	return NewLogicalJoin(joinNodes, joinSchema), joinSchema, nil
}

func BuildFullJoinQueryPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.SelectStatement, schema *QuerySchema) (hybridqp.QueryNode, error) {
	joinCases := schema.GetJoinCases()
	if len(joinCases) != 1 {
		return nil, fmt.Errorf("only surrport two subquery join")
	}
	var joinConditon influxql.Expr
	joinNodes := make([]hybridqp.QueryNode, 0, len(stmt.Sources))

	for i := range stmt.Sources {
		source := influxql.CloneSource(stmt.Sources[i])
		optSource := influxql.Sources{source}
		childOpt := schema.opt.(*query.ProcessorOptions).Clone()
		childOpt.UpdateSources(optSource)
		s := NewQuerySchemaWithSources(stmt.Fields, influxql.Sources{stmt.Sources[i]}, stmt.ColumnNames(), childOpt, nil)
		child, err := BuildSources(ctx, qc, influxql.Sources{stmt.Sources[i]}, s, false)
		if err != nil {
			return nil, err
		}
		if child != nil {
			joinNodes = append(joinNodes, child)
		}
		schema.sources = append(schema.sources, source)
	}

	if len(joinNodes) == 0 {
		return nil, nil
	}
	return NewLogicalFullJoin(joinNodes[0], joinNodes[1], joinConditon, schema), nil
}

func BuildBinOpQueryPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.SelectStatement, schema *QuerySchema) (hybridqp.QueryNode, error) {
	binOps := stmt.BinOpSource
	if len(binOps) != 1 {
		return nil, fmt.Errorf("only surrport two subquery binOp")
	}
	binOpNodes := make([]hybridqp.QueryNode, 0, len(stmt.Sources))

	for i := range stmt.Sources {
		source := influxql.CloneSource(stmt.Sources[i])
		optSource := influxql.Sources{source}
		childOpt := schema.opt.(*query.ProcessorOptions).Clone()
		childOpt.UpdateSources(optSource)
		s := NewQuerySchemaWithSources(stmt.Fields, influxql.Sources{stmt.Sources[i]}, stmt.ColumnNames(), childOpt, nil)
		child, err := BuildSources(ctx, qc, influxql.Sources{stmt.Sources[i]}, s, true)
		if err != nil {
			return nil, err
		}
		if child != nil {
			binOpNodes = append(binOpNodes, child)
		}
		schema.sources = append(schema.sources, source)
	}

	if len(binOpNodes) == 0 {
		return nil, nil
	}
	binop := NewLogicalBinOp(binOpNodes[0], binOpNodes[1], binOps[0], schema)
	if schema.HasCall() {
		builder := NewLogicalPlanBuilderImpl(schema)
		builder.Push(binop)
		builder.GroupBy()
		builder.OrderBy()
		return builder.Build()
	}
	return binop, nil
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
		(schema.CanAggPushDown() && sysconfig.GetEnableSlidingWindowPushUp() == sysconfig.OnSlidingWindowPushUp) || schema.HasSubQuery()) {
		builder.SlidingWindow()
	} else {
		if !schema.Options().IsRangeVectorSelector() {
			builder.Aggregate()
		}
	}
}

func buildIncAggNode(builder *LogicalPlanBuilderImpl) {
	builder.IncHashAgg()
}

func buildNodes(builder *LogicalPlanBuilderImpl, schema hybridqp.Catalog, s *QuerySchema) {
	hasDistinct, hasSelector := hasDistinctSelectorCall(s)
	hasSlidingWindow := schema.HasSlidingWindowCall()
	hasHoltWinters := schema.HasHoltWintersCall()
	hasSort := s.HasSort()
	HaveOnlyCSStore := schema.Options().HaveOnlyCSStore()
	isSubQuery := schema.Sources().IsSubQuery()

	if len(schema.Calls()) > 0 {
		buildAggNode(builder, schema, hasSlidingWindow)
	}

	if len(schema.Calls()) > 0 && schema.Options().IsIncQuery() {
		buildIncAggNode(builder)
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

	if hasSort && (HaveOnlyCSStore || isSubQuery) {
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
		sp, err = BuildFullJoinQueryPlan(ctx, qc, stmt, s)
	} else if len(stmt.BinOpSource) > 0 && len(stmt.Sources) == 2 {
		sp, err = BuildBinOpQueryPlan(ctx, qc, stmt, s)
	} else if stmt.Sources = qc.GetSources(stmt.Sources); len(stmt.Sources) > 1 {
		sp, err = buildSortAppendQueryPlan(ctx, qc, stmt, s)
	} else {
		sp, err = BuildSources(ctx, qc, stmt.Sources, s, false)
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

func BuildSources(ctx context.Context, qc query.LogicalPlanCreator, sources influxql.Sources, schema *QuerySchema, outerBinOp bool) (hybridqp.QueryNode, error) {
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
		if err != nil {
			return nil, err
		}
		if subQueryPlan == nil {
			return nil, nil
		}
		builder.Push(subQueryPlan)
		if schema.Options().GetCondition() != nil {
			builder.Filter()
		}
		builder.SubQuery()
		if !schema.opt.IsPromQuery() || (!outerBinOp && schema.HasCall()) {
			builder.GroupBy()
			builder.OrderBy()
		}
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
			} else if n.eType == PARTITION_EXCHANGE {
				eType = PARTITION_EXCHANGE
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

func replaceChildAggNode(plan hybridqp.QueryNode, node *LogicalHashAgg, p []hybridqp.QueryNode) {
	aggNode := findChildAggNode(plan)
	if aggNode != nil {
		node.LogicalPlanBase = aggNode.(*LogicalAggregate).LogicalPlanBase
		node.inputs = p
	}
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
		case *LogicalOrderBy, *LogicalSortAppend, *LogicalFullJoin, *LogicalGroupBy, *LogicalSubQuery, *LogicalSort:
		default:
			plan.SetInputs(plan.Children()[0].Children())
		}
	}
	RebuildAggNodes(plan.Children()[0])
}

func ReplaceSortAggWithHashAgg(plan hybridqp.QueryNode) []hybridqp.QueryNode {
	if plan.Children() == nil || len(plan.Schema().Sources()) > 1 {
		return []hybridqp.QueryNode{plan}
	}
	var replace bool
	var nodes []hybridqp.QueryNode
	for _, child := range plan.Children() {
		if child == nil {
			continue
		}
		p := ReplaceSortAggWithHashAgg(child)
		switch plan.(type) {
		case *LogicalAggregate:
			node := NewLogicalHashAgg(p[0], plan.Schema(), NODE_EXCHANGE, nil)
			if node.schema.HasCall() {
				n1 := findChildAggNode(plan)
				if n1 != nil {
					node.LogicalPlanBase = n1.(*LogicalAggregate).LogicalPlanBase
					node.inputs = p
				}
			}
			nodes = append(nodes, node)
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

func ReplaceSortMergeWithHashMerge(plan hybridqp.QueryNode) []hybridqp.QueryNode {
	if plan.Children() == nil || len(plan.Schema().Sources()) > 1 {
		return []hybridqp.QueryNode{plan}
	}
	var replace bool
	var nodes []hybridqp.QueryNode
	for _, child := range plan.Children() {
		if child == nil {
			continue
		}
		p := ReplaceSortMergeWithHashMerge(child)
		switch n := plan.(type) {
		case *LogicalExchange:
			node := NewLogicalHashMerge(p[0], plan.Schema(), n.eType, n.eTraits)
			nodes = append(nodes, node)
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

func ReplaceSortAggMergeWithHashAgg(plan hybridqp.QueryNode) []hybridqp.QueryNode {
	if plan.Children() == nil || len(plan.Schema().Sources()) > 1 {
		return []hybridqp.QueryNode{plan}
	}
	var replace bool
	var nodes []hybridqp.QueryNode
	for _, child := range plan.Children() {
		if child == nil {
			continue
		}
		p := ReplaceSortAggMergeWithHashAgg(child)
		if len(p) == 0 {
			continue
		}
		switch n := plan.(type) {
		case *LogicalAggregate:
			if len(n.Children()) != 1 {
				panic("agg node should have a child node")
			}
			var node *LogicalHashAgg
			if exchange, ok := n.Children()[0].(*LogicalExchange); ok {
				node = NewLogicalHashAgg(p[0], plan.Schema(), exchange.eType, exchange.eTraits)
				replaceChildAggNode(plan, node, p)
				node.SetInputs(node.Children()[0].Children())
			} else {
				node = NewLogicalHashAgg(p[0], plan.Schema(), UNKNOWN_EXCHANGE, nil)
				replaceChildAggNode(plan, node, p)
			}
			nodes = append(nodes, node)
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
