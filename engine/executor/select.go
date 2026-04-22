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

package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
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

func (p *preparedStatement) ProcessorOptions() *query.ProcessorOptions {
	opt, ok := p.opt.(*query.ProcessorOptions)
	if !ok {
		return nil
	}

	return opt
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

func (m *MultiMstReqs) SetReqs(reqs []*RemoteQuery) {
	m.reqs = reqs
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

	schema := NewQuerySchemaWithJoinCase(p.stmt.Fields, p.stmt.Sources, p.stmt.ColumnNames(), opt, p.stmt.JoinSource, p.stmt.UnionSource,
		p.stmt.UnnestSource, p.stmt.SortFields)
	schema.SetPromCalls(p.stmt.PromSubCalls)
	if p.stmt.IsCompareCall {
		schema.SetCompareCall()
	}

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
	if localStorageForQuery != nil && req != nil {
		executorBuilder.(*ExecutorBuilder).SetInfosAndTraits(ctx, req.([]*MultiMstReqs), nil, nil)
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
		s := NewQuerySchemaWithJoinCase(stmt.Fields, influxql.Sources{stmt.Sources[i]}, stmt.ColumnNames(), childOpt, stmt.JoinSource, nil,
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

func BuildInConditionPlan(ctx context.Context, outerQc query.LogicalPlanCreator, outerStmt *influxql.SelectStatement, outerSchema *QuerySchema,
	builder *LogicalPlanBuilderImpl) (hybridqp.QueryNode, error) {
	in := outerSchema.InConditons[0]
	inQc, ok := in.Csming.(query.LogicalPlanCreator)
	if !ok {
		return nil, fmt.Errorf("inConditon csming err")
	}
	outerOpt, ok := outerSchema.Options().(*query.ProcessorOptions)
	if !ok {
		return nil, fmt.Errorf("inConditon outerOpt err")
	}

	// 1. build inOpt
	inOpt, err := query.NewProcessorOptionsWithopt(in.Stmt, outerOpt)
	if err != nil {
		return nil, fmt.Errorf("NewProcessorOptions of inContition.stmt err")
	}
	if inOpt.StartTime < in.TimeRange.MinTimeNano() {
		inOpt.StartTime = in.TimeRange.MinTimeNano()
	}
	if inOpt.EndTime > in.TimeRange.MaxTimeNano() {
		inOpt.EndTime = in.TimeRange.MaxTimeNano()
	}
	if !inOpt.Interval.IsZero() && inOpt.EndTime == influxql.MaxTime {
		if now := ctx.Value(NowKey); now != nil {
			inOpt.EndTime = now.(time.Time).UnixNano()
		}
	}
	// notUse dimPushDown of inCondition clause
	inOpt.StmtId = outerOpt.StmtId

	// 2. build inSchema
	inSchema := NewQuerySchemaWithJoinCase(in.Stmt.Fields, in.Stmt.Sources, in.Stmt.ColumnNames(), &inOpt, in.Stmt.JoinSource, nil,
		in.Stmt.UnnestSource, in.Stmt.SortFields)

	// isInSubquerySchema used to judge if agg transform should be pushed down
	if inSchema != nil {
		inSchema.SetIsInSubquerySchema(true)
	}

	// 3.build innerPlan
	var valType influxql.DataType
	if len(in.Stmt.Fields) == 0 {
		return nil, errors.New("fields is empty")
	}
	field := in.Stmt.Fields[0]
	if ref, ok := field.Expr.(*influxql.VarRef); !ok {
		return nil, errors.New("type assertion failed")
	} else {
		valType = ref.Type
	}

	var innerPlan hybridqp.QueryNode
	if valType != influxql.Tag {
		innerPlan, err = buildQueryPlan(ctx, in.Stmt, inQc, inSchema)
		if err != nil {
			return nil, fmt.Errorf("buildQueryPlan of inContition.stmt err")
		}
	}

	var newStmt *influxql.ShowTagValuesStatement
	if valType == influxql.Tag {
		newShowTagVal := &influxql.ShowTagValuesStatement{}
		stmt, err := newShowTagVal.RewriteShowTagValStmt(in)
		if err != nil {
			return nil, err
		}
		condState, err := query.RewriteStatement(stmt)
		if err != nil {
			return nil, err
		}
		newStmt, ok = condState.(*influxql.ShowTagValuesStatement)
		if !ok {
			return nil, errors.New("type assertion failed")
		}
	}

	// 4. build logicalInOp
	inOp := NewLogicalIn(innerPlan, outerStmt, outerSchema, in.Column, outerQc, newStmt)
	builder.Push(inOp)
	return builder.Build()
}

func BuildJoinQueryPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.SelectStatement, schema *QuerySchema) (hybridqp.QueryNode, error) {
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
		if joinCases[0].JoinType != influxql.FullJoin {
			childOpt.NoPushDownDim = true
		}
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
	if joinCases[0].JoinType == influxql.FullJoin {
		return NewLogicalFullJoin(joinNodes[0], joinNodes[1], joinConditon, schema), nil
	}
	return NewLogicalJoin(joinNodes[0], joinNodes[1], joinConditon, joinCases[0].JoinType, schema), nil
}

func BuildBinOpQueryPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.SelectStatement, schema *QuerySchema) (hybridqp.QueryNode, error) {
	binOps := stmt.BinOpSource
	if len(binOps) != 1 {
		return nil, fmt.Errorf("only surrport two subquery binOp")
	}
	binOpNodes := make([]hybridqp.QueryNode, 0, len(stmt.Sources))

	for i := range stmt.Sources {
		source := influxql.CloneSource(stmt.Sources[i])
		child, err := BuildSources(ctx, qc, influxql.Sources{stmt.Sources[i]}, schema, true)
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

	var binop hybridqp.QueryNode
	if len(binOpNodes) == 1 && binOps[0].NilMst == influxql.NoNilMst {
		if binOps[0].LExpr != nil {
			binop = NewLogicalBinOp(nil, binOpNodes[0], binOps[0].LExpr, nil, binOps[0], schema)
		} else if binOps[0].RExpr != nil {
			binop = NewLogicalBinOp(binOpNodes[0], nil, nil, binOps[0].RExpr, binOps[0], schema)
		}
	} else {
		if binOps[0].NilMst == influxql.NoNilMst {
			binop = NewLogicalBinOp(binOpNodes[0], binOpNodes[1], nil, nil, binOps[0], schema)
		} else if binOps[0].NilMst == influxql.LNilMst {
			binop = NewLogicalBinOp(nil, binOpNodes[0], nil, nil, binOps[0], schema)
		} else {
			binop = NewLogicalBinOp(binOpNodes[0], nil, nil, nil, binOps[0], schema)
		}
	}

	schema.Options().SetBinOp(true)
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

// whether subDims is subset of wholeDims
func subDims(subDims, wholeDims []string) bool {
	if len(subDims) > len(wholeDims) {
		return false
	}
	find := false
	for _, subD := range subDims {
		find = false
		for _, wholeD := range wholeDims {
			if subD == wholeD {
				find = true
				break
			}
		}
		if !find {
			return false
		}
	}
	return true
}

// whether suffixDims if suffix of wholeDims
func suffixDims(suffixDims, wholeDims []string) bool {
	i, j := len(suffixDims)-1, len(wholeDims)-1
	for i >= 0 && j >= 0 {
		if suffixDims[i] == wholeDims[j] {
			i--
			j--
		} else {
			return false
		}
	}
	return i < 0
}

func prefixDims(prefixDims, wholeDims []string) bool {
	i, j := 0, 0
	for i < len(prefixDims) && j < len(wholeDims) {
		if prefixDims[i] == wholeDims[j] {
			i++
			j++
		} else {
			return false
		}
	}
	return i >= len(prefixDims)
}

func sameDims(dims1, dims2 []string) bool {
	if len(dims1) != len(dims2) {
		return false
	}
	for i := 0; i < len(dims1); i++ {
		if dims1[i] != dims2[i] {
			return false
		}
	}
	return true
}

// whether  upperOptDims is prefix of lowerOptDims: streamAgg: return true; hashAgg: return false
func prefixDimsOfOpts(upperOpt, lowerOpt hybridqp.Options) bool {
	upperDims, lowerDims := upperOpt.GetOptDimension(), lowerOpt.GetOptDimension()
	if upperOpt.IsGroupByAllDims() {
		return true
	}
	if upperOpt.IsWithout() {
		if lowerOpt.IsGroupByAllDims() { // lower is call(xx)
			return len(upperDims) == 0 // without() == groupByAll -> both is groupByAll return true
		}
		if lowerOpt.IsWithout() {
			return subDims(upperDims, lowerDims)
		}
		return suffixDims(upperDims, lowerDims)
	}
	if lowerOpt.IsGroupByAllDims() && len(upperOpt.GetDimensions()) > 0 {
		return false
	}
	if lowerOpt.IsWithout() {
		return sameDims(upperDims, lowerDims)
	}
	return prefixDims(upperDims, lowerDims)
}

func isBuildHashAgg(options hybridqp.Options) bool {
	lowerOpt := options.GetLowerOpt()
	if !options.IsPromQuery() || lowerOpt == nil {
		return false
	}
	processorOpt, ok := lowerOpt.(*query.ProcessorOptions)
	if !ok || processorOpt == nil {
		return false
	}
	if options.GetBinop() || !prefixDimsOfOpts(options, processorOpt) || processorOpt.IsCountValues {
		return true
	}
	return false
}

func buildAggNode(builder *LogicalPlanBuilderImpl, schema hybridqp.Catalog, hasSlidingWindow bool) {
	if hasSlidingWindow && (!schema.CanAggPushDown() ||
		(schema.CanAggPushDown() && sysconfig.GetEnableSlidingWindowPushUp() == sysconfig.OnSlidingWindowPushUp) || schema.HasSubQuery()) {
		builder.SlidingWindow()
	} else {
		if !schema.Options().IsRangeVectorSelector() || schema.HasPromNestedCall() || schema.IsPromAbsentCall() {
			if isBuildHashAgg(schema.Options()) {
				builder.HashAgg()
				return
			}
			builder.Aggregate()
		}
	}
}

func buildSortNode(builder *LogicalPlanBuilderImpl, schema hybridqp.Catalog, s *QuerySchema) {
	isSubQuery := schema.Sources().IsSubQuery()
	isPromQuery := schema.Options().IsPromQuery()
	HaveOnlyCSStore := schema.Options().HaveOnlyCSStore()
	if HaveOnlyCSStore || isSubQuery {
		builder.Sort()
	} else if isPromQuery {
		builder.PromSort()
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
	isCTE := schema.Sources().IsCTE()

	if len(schema.Calls()) > 0 {
		buildAggNode(builder, schema, hasSlidingWindow)
	}

	if len(schema.Calls()) > 0 && schema.Options().IsIncQuery() {
		buildIncAggNode(builder)
	}

	if schema.CountDistinct() != nil {
		builder.CountDistinct()
	}

	if !hasSelector && !hasSlidingWindow && s.opt.HasInterval() || (hasDistinct && !isSubQuery) {
		builder.Interval()
	}

	if schema.IsCompareCall() {
		builder.Align()
	}

	_, ok := builder.stack.Peek().(*LogicalFilter)
	if !ok || !isCTE {
		builder.Project()
	}

	if len(s.PromSubCalls) > 0 {
		for _, call := range s.PromSubCalls {
			builder.PromSubquery(call)
		}
	}

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

	if hasSort {
		buildSortNode(builder, schema, s)
	}

	// Apply limit & offset.
	if schema.HasLimit() {
		if schema.Options().IsExcept() {
			return
		}
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
	if schema.HasInCondition() {
		return BuildInConditionPlan(ctx, qc, stmt, s, builder)
	} else if schema.GetJoinCaseCount() > 0 && len(stmt.Sources) == 2 {
		sp, err = BuildJoinQueryPlan(ctx, qc, stmt, s)
	} else if len(stmt.BinOpSource) > 0 {
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

	unionCases := s.GetUnionCases()
	if len(unionCases) > 0 {
		unionType := unionCases[0].UnionType
		if unionType == influxql.UnionDistinct || unionType == influxql.UnionDistinctByName {
			builder.Distinct()
		}
	}

	buildNodes(builder, schema, s)

	return builder.Build()
}

func BuildNodeExchange(ctx context.Context, builder LogicalPlanBuilder, queryPlan hybridqp.QueryNode) error {
	nodeTraits, ok := ctx.Value(hybridqp.NodeTrait).(*[]hybridqp.Trait)
	if !ok {
		return errno.NewError(errno.NoNodeTraits)
	}
	project, ok := queryPlan.(*LogicalProject)
	if ok {
		queryPlan = project.Children()[0]
	}
	traits, err := TransNodeTraits(*nodeTraits)
	if err != nil {
		return err
	}
	queryPlan, err = builder.CreateNodePlan(queryPlan, traits)
	if err != nil {
		return err
	}
	builder.Push(queryPlan)
	if ok {
		builder.Project()
	}
	return nil
}

type NodeTraitKey struct {
	Database string
	PtID     uint32 // for tsstore
	NodeID   uint64
}

func TransNodeTraits(nodeTraits []hybridqp.Trait) ([]hybridqp.Trait, error) {
	res := make([]hybridqp.Trait, 0, len(nodeTraits))
	nodeTraitsMap := make(map[NodeTraitKey]*RemoteQuery)
	for _, trait := range nodeTraits {
		rq, ok := trait.(*RemoteQuery)
		if !ok {
			return nil, fmt.Errorf("invalid node remote query")
		}
		key := NodeTraitKey{Database: rq.Database, PtID: rq.PtID, NodeID: rq.NodeID}
		value, ok := nodeTraitsMap[key]
		if !ok {
			value = rq
			value.MstInfos = []*MultiMstInfo{{ShardIds: rq.ShardIDs, Opt: rq.Opt}}
		} else {
			value.MstInfos = append(value.MstInfos, &MultiMstInfo{ShardIds: rq.ShardIDs, Opt: rq.Opt})
		}
		nodeTraitsMap[key] = value
	}
	for _, rqs := range nodeTraitsMap {
		res = append(res, rqs)
	}
	return res, nil
}

func buildExtendedPlan(ctx context.Context, stmt *influxql.SelectStatement, qc query.LogicalPlanCreator, schema *QuerySchema) (hybridqp.QueryNode, error) {
	builder := NewLogicalPlanBuilderImpl(schema)
	nodeTraits := []hybridqp.Trait{}
	ctx = context.WithValue(ctx, hybridqp.NodeTrait, &nodeTraits)
	queryPlan, err := buildQueryPlan(ctx, stmt, qc, schema)
	if err != nil {
		return nil, err
	}
	if queryPlan == nil {
		return nil, nil
	}

	if queryPlan.Schema().Options().CanQueryPushDown() {
		err = BuildNodeExchange(ctx, builder, queryPlan)
		if err != nil {
			return nil, err
		}
	} else {
		builder.Push(queryPlan)
	}

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
		subQueryPlan, err := subQueryBuilder.Build(ctx, schema.Options().(*query.ProcessorOptions))
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
		// check whether the inner and outer dims of a subquery are the same.
		isSameDims := slices.Equal(subQueryPlan.Schema().Options().GetOptDimension(), schema.Options().GetOptDimension())
		isSameDims = isSameDims && !subQueryPlan.Schema().Options().IsPromGroupAllOrWithout() && !schema.Options().IsPromGroupAllOrWithout()
		schema.Options().SetSameDims(isSameDims)
		builder.SubQuery()
		if !schema.opt.IsPromQuery() || (!outerBinOp && schema.HasCall() && !schema.HasPromAbsentCall()) {
			builder.GroupBy()
			builder.OrderBy()
		}
		return builder.Build()
	case *influxql.CTE:
		return BuildCTELogicalPlan(ctx, schema, source)
	case *influxql.TableFunction:
		return buildTableFunctionQueryPlan(ctx, qc, source, schema)
	default:
		return nil, nil
	}
}

func BuildCTELogicalPlan(ctx context.Context, outerSchema *QuerySchema, cte *influxql.CTE) (hybridqp.QueryNode, error) {
	builder := NewLogicalPlanBuilderImpl(outerSchema)

	// todo: more abundant qc of graph query
	cteQc, ok := cte.Csming.(query.LogicalPlanCreator)
	if cte.Csming != nil && !ok {
		return nil, fmt.Errorf("cte csming err")
	}
	outerOpt, ok := outerSchema.Options().(*query.ProcessorOptions)
	if !ok || outerOpt == nil {
		return nil, fmt.Errorf("cte outerOpt err")
	}

	// 1. build cteOpt
	var cteOpt query.ProcessorOptions
	var err error
	if cte.GraphQuery != nil {
		// todo: more abundant opt of graph query, like parse graph query time_args...
		cteOpt = query.NewGraphProcessorOptions(outerOpt)
	} else {
		cteOpt, err = query.NewProcessorOptionsWithopt(cte.Query, outerOpt)
		if err != nil {
			return nil, fmt.Errorf("new processor options of cte.stmt err")
		}
		if cteOpt.StartTime <= cte.TimeRange.MinTimeNano() {
			cteOpt.StartTime = cte.TimeRange.MinTimeNano()
		}
		if cteOpt.EndTime >= cte.TimeRange.MaxTimeNano() {
			cteOpt.EndTime = cte.TimeRange.MaxTimeNano()
		}
		if !cteOpt.Interval.IsZero() && cteOpt.EndTime == influxql.MaxTime {
			if now := ctx.Value(NowKey); now != nil {
				cteOpt.EndTime = now.(time.Time).UnixNano()
			}
		}
	}

	// notUse dimPushDown of cteCondition clause
	cteOpt.StmtId = outerOpt.StmtId

	// 2. build cteSchema and ctePlan
	var cteSchema *QuerySchema
	var ctePlan hybridqp.QueryNode
	if cte.GraphQuery == nil {
		cteSchema = NewQuerySchemaWithJoinCase(cte.Query.Fields, cte.Query.Sources, cte.Query.ColumnNames(), &cteOpt, cte.Query.JoinSource, nil,
			cte.Query.UnnestSource, cte.Query.SortFields)
		ctePlan, err = buildQueryPlan(ctx, cte.Query, cteQc, cteSchema)
		if err != nil {
			return nil, fmt.Errorf("buildQueryPlan of cteContition.stmt err")
		}
	} else {
		// todo: more abundant schema of graph query
		cteSchema = &QuerySchema{opt: &cteOpt}
		ctePlan = NewLogicalGraph(nil, cte.GraphQuery, cteSchema, cteQc)
	}

	// 4. optimize ctePlan
	optimizer := BuildHeuristicPlanner()
	optimizer.SetRoot(ctePlan)
	bestCTEPlan := optimizer.FindBestExp()
	builder.Push(NewLogicalCTE(cte, outerSchema, bestCTEPlan))

	if outerSchema.Options().GetCondition() != nil {
		builder.Project()
		builder.Filter()
	}
	return builder.Build()
}

func buildTableFunctionQueryPlan(ctx context.Context, qc query.LogicalPlanCreator, stmt *influxql.TableFunction, schema *QuerySchema) (hybridqp.QueryNode, error) {
	tableFunctionSource := stmt.GetTableFunctionSource()
	nodes := make([]hybridqp.QueryNode, 0, len(tableFunctionSource))
	params := stmt.GetParams()

	for i := range tableFunctionSource {
		source := influxql.CloneSource(tableFunctionSource[i])
		optSource := influxql.Sources{source}
		childOpt := schema.opt.(*query.ProcessorOptions).Clone()
		childOpt.UpdateSources(optSource)

		switch source := source.(type) {
		case *influxql.Measurement:
			optSource = qc.GetSources(optSource)
			childSource, err := BuildSources(ctx, qc, optSource, schema, false)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, childSource)
		case *influxql.CTE:
			cteStmt := source.Query
			if cteStmt != nil {
				schema := NewQuerySchemaWithJoinCase(cteStmt.Fields, optSource, cteStmt.ColumnNames(), childOpt, cteStmt.JoinSource, nil,
					cteStmt.UnnestSource, cteStmt.SortFields)
				childSource, err := BuildSources(ctx, qc, optSource, schema, false)
				if err != nil {
					return nil, err
				}
				if childSource != nil {
					nodes = append(nodes, childSource)
				}
			} else {
				schema := NewQuerySchema(nil, nil, childOpt, nil)
				schema.SetSources(optSource)
				childSource, err := BuildSources(ctx, qc, optSource, schema, false)
				if err != nil {
					return nil, err
				}
				if childSource != nil {
					nodes = append(nodes, childSource)
				}
			}
		default:
			return nil, errno.NewError(errno.InternalError, "tablefunction param error")
		}

	}

	if len(nodes) == 0 {
		return nil, nil
	}
	return NewLogicalTableFunction(nodes, schema, stmt.GetName(), params), nil
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
			_, ok := plan.Schema().HasTopN()
			if plan.Schema().HasCall() && (plan.Schema().CanAggPushDown() || eType == NODE_EXCHANGE || ok) {
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
		case *LogicalOrderBy, *LogicalSortAppend, *LogicalFullJoin, *LogicalGroupBy, *LogicalSubQuery, *LogicalSort, *LogicalPromSort:
		default:
			plan.SetInputs(plan.Children()[0].Children())
		}
	}
	RebuildAggNodes(plan.Children()[0])
}

func hasTopn(plan hybridqp.QueryNode) bool {
	return strings.HasPrefix(plan.(*LogicalAggregate).callsOrder[0], "topn_ddcm") || strings.HasPrefix(plan.(*LogicalAggregate).callsOrder[0], nagtName)
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
			eType := NODE_EXCHANGE
			if len(plan.(*LogicalAggregate).callsOrder) == 1 && hasTopn(plan) {
				eType = SUBQUERY_EXCHANGE
			}
			node := NewLogicalHashAgg(p[0], plan.Schema(), eType, nil)
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
