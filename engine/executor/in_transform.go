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

package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

type InTransformRunningState int

const (
	innerStmt InTransformRunningState = iota
	outerStmt
)

type InTransform struct {
	BaseProcessor
	input       *ChunkPort
	output      *ChunkPort
	outputChunk Chunk
	chunkPool   *CircularChunkPool
	workTracing *tracing.Span
	inOpLogger  *logger.Logger

	innerSchema hybridqp.Catalog
	innerOpt    hybridqp.Options

	outerStmt   *influxql.SelectStatement
	outerSchema hybridqp.Catalog
	outerQc     query.LogicalPlanCreator
	outerField  influxql.Expr
	OuterVarRef *influxql.VarRef

	outerPlan       hybridqp.QueryNode
	outerExecutor   *PipelineExecutor
	ExecutorBuilder OuterExecutorBuilder
	runingState     InTransformRunningState
	workHelper      func(c Chunk)
	tagValuesAst    *influxql.ShowTagValuesStatement

	BufColumnMap map[interface{}]struct{}
}

type OuterExecutorBuilder interface {
	Analyze(span *tracing.Span)
	Build(node hybridqp.QueryNode) (hybridqp.Executor, error)
}

const (
	InTransformName = "InTransform"
)

type InTransformCreator struct {
}

func (c *InTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))
	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}
	inOp, ok := plan.(*LogicalIn)
	if !ok {
		return nil, fmt.Errorf("logicalplan isnot inOp")
	}
	p, err := NewInTransform(inRowDataTypes, plan.RowDataType(), inOp.Schema(), inOp.outerStmt,
		inOp.outerSchema, inOp.outerQc, inOp.outerField, inOp.showTagVal)
	return p, err
}

var _ = RegistryTransformCreator(&LogicalIn{}, &InTransformCreator{})

func NewInTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, schema hybridqp.Catalog,
	outerStmt *influxql.SelectStatement, outerSchema hybridqp.Catalog, outerQc query.LogicalPlanCreator, outerField influxql.Expr, tagstmt *influxql.ShowTagValuesStatement) (*InTransform, error) {
	if len(inRowDataTypes) > 1 {
		return nil, fmt.Errorf("InTransform input rt err")
	}
	trans := &InTransform{
		output:      NewChunkPort(outRowDataType),
		chunkPool:   NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		innerSchema: schema,
		innerOpt:    schema.Options(),
		inOpLogger:  logger.NewLogger(errno.ModuleQueryEngine),

		outerStmt:   outerStmt,
		outerSchema: outerSchema,
		outerQc:     outerQc,
		outerField:  outerField,

		runingState: innerStmt,
	}
	trans.workHelper = trans.AddChunkToBufColumn
	trans.ExecutorBuilder = NewQueryExecutorBuilder(sysconfig.GetEnableBinaryTreeMerge())
	for i := range inRowDataTypes {
		trans.input = NewChunkPort(inRowDataTypes[i])
	}
	trans.outputChunk = trans.chunkPool.GetChunk()
	if tagstmt == nil {
		err := trans.checkOuterFieldType()
		if err != nil {
			return nil, err
		}
	} else {
		outVarRef, ok := trans.outerField.(*influxql.VarRef)
		if !ok {
			return nil, errors.New("InTranform outerColumn type err")
		}
		trans.OuterVarRef = outVarRef
	}
	trans.tagValuesAst = tagstmt
	trans.BufColumnMap = make(map[interface{}]struct{})
	return trans, nil
}

func (trans *InTransform) checkOuterFieldType() error {
	if trans.input.RowDataType.NumColumn() != 1 {
		return fmt.Errorf("InTransform input.rt size err")
	}
	field := trans.input.RowDataType.Fields()[0].Expr
	innerVarRef, ok := field.(*influxql.VarRef)
	if !ok {
		return fmt.Errorf("InTranform input.rt type err")
	}
	outVarRef, ok := trans.outerField.(*influxql.VarRef)
	if !ok {
		return fmt.Errorf("InTranform outerColumn type err")
	}
	trans.OuterVarRef = outVarRef
	if innerVarRef.Type == influxql.Graph {
		trans.workHelper = trans.AddGraphChunkToBufColumn
		return trans.checkGraphFieldType(innerVarRef)
	}
	if innerVarRef.Type != outVarRef.Type {
		return fmt.Errorf("InTransform inner.type != outer.type err")
	}
	trans.OuterVarRef = outVarRef
	return nil
}

// todo: use static graphschema
func (trans *InTransform) checkGraphFieldType(innerVarRef *influxql.VarRef) error {
	if trans.OuterVarRef.Type != influxql.String && trans.OuterVarRef.Type != influxql.Tag {
		return fmt.Errorf("InTransform outerVarRef.typ err when innerVarRef.typ is graph")
	}
	// todo: more abundant graph.col of where in
	if innerVarRef.Val != "uid" {
		return fmt.Errorf("wrong innerVarRef.val of InTransform for graph")
	}
	return nil
}

func (trans *InTransform) Name() string {
	return InTransformName
}

func (trans *InTransform) Explain() []ValuePair {
	return nil
}

func (trans *InTransform) Close() {
	trans.output.Close()
}

func (trans *InTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[InTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_inOp", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()
	if trans.tagValuesAst != nil {
		err := trans.ShowTagValRun(trans.tagValuesAst)
		if err != nil {
			return err
		}
		if err := trans.RunOuterPlan(ctx); err != nil {
			if errno.Equal(err, errno.FilterAllPoints) {
				return nil
			}
			return err
		}
	}
	for {
		select {
		case c, ok := <-trans.input.State:
			if !ok {
				if trans.runingState == outerStmt {
					return nil
				}
				if err := trans.RunOuterPlan(ctx); err != nil {
					if errno.Equal(err, errno.FilterAllPoints) {
						return nil
					}
					return err
				}
			}
			trans.workHelper(c)
		case <-ctx.Done():
			return nil
		}
	}
}

func (trans *InTransform) ShowTagValRun(stmt *influxql.ShowTagValuesStatement) error {
	tagKeys, err := trans.outerQc.GetTagKeys(stmt)
	if err != nil {
		return err
	}

	lock := new(sync.Mutex)
	exact := influxql.IsExactStatisticQueryForDDL(stmt)

	err = trans.WalkDBNodes(stmt.Database, func(nodeID uint64, pts []uint32) error {
		s, err := trans.outerQc.GetTagVals(nodeID, stmt, pts, tagKeys, exact)
		lock.Lock()
		defer lock.Unlock()
		if err != nil {
			return err
		}
		trans.AddTagValsToBufColumn(s)
		return nil
	})
	return err
}

func (trans *InTransform) WalkDBNodes(database string, fn func(nodeID uint64, pts []uint32) error) error {
	if err := trans.outerQc.CheckDatabaseExists(database); err != nil {
		return err
	}
	start := time.Now()

	for {
		nodePtMap, err := trans.outerQc.QueryNodePtsMap(database)
		if err != nil || len(nodePtMap) == 0 {
			return err
		}

		chErr := make(chan error, len(nodePtMap))

		for id, pts := range nodePtMap {
			go func(nodeID uint64, pts []uint32) {
				chErr <- fn(nodeID, pts)
			}(id, pts)
		}

		retryable := true
		for i := 0; i < len(nodePtMap); i++ {
			if e := <-chErr; e != nil && retryable {
				retryable = errno.IsRetryErrorForPtView(e)
				err = e
			}
		}
		if err == nil || !retryable {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		if time.Since(start) >= 30*time.Second {
			return err
		}
	}
}

// don't add nilMap to buf, skip same val to add
func (trans *InTransform) AddChunkToBufColumn(c Chunk) {
	switch trans.OuterVarRef.Type {
	case influxql.Integer:
		for _, val := range c.Column(0).IntegerValues() {
			_, ok := trans.BufColumnMap[val]
			if !ok {
				trans.BufColumnMap[val] = struct{}{}
			}
		}
	case influxql.Float:
		for _, val := range c.Column(0).FloatValues() {
			_, ok := trans.BufColumnMap[val]
			if !ok {
				trans.BufColumnMap[val] = struct{}{}
			}
		}
	case influxql.Boolean:
		for _, val := range c.Column(0).BooleanValues() {
			_, ok := trans.BufColumnMap[val]
			if !ok {
				trans.BufColumnMap[val] = struct{}{}
			}
		}
	case influxql.String, influxql.Tag:
		_, offsets := c.Column(0).GetStringBytes()
		for i := range offsets {
			val := c.Column(0).StringValue(i)
			_, ok := trans.BufColumnMap[val]
			if !ok {
				trans.BufColumnMap[val] = struct{}{}
			}
		}
	default:
		panic("wrong type in AddChunkToBufColumn")
	}
}

// todo: support graph.properties
func (trans *InTransform) AddGraphChunkToBufColumn(c Chunk) {
	iGraph := c.GetGraph()
	graph, ok := iGraph.(*Graph)
	if !ok || graph == nil {
		return
	}
	graph.addToBufMap(trans.BufColumnMap)
}

func (trans *InTransform) AddTagValsToBufColumn(s influxql.TablesTagSets) {
	for _, tableTagSet := range s {
		for _, tagSet := range tableTagSet.Values {
			if _, exists := trans.BufColumnMap[tagSet.Value]; !exists {
				trans.BufColumnMap[tagSet.Value] = struct{}{}
			}
		}
	}
}

func (trans *InTransform) RunOuterPlan(ctx context.Context) error {
	// 1. rewrite outerStmt.condition
	err := trans.RewriteOuterStmtCondition()
	if err != nil {
		return err
	}
	// 2. build plan
	err = trans.buildPlan(ctx)
	if err != nil {
		return err
	}

	// 3. build dag
	err = trans.buildDag(ctx)
	if err != nil {
		return err
	}

	// 4. rewrite channel
	err = trans.connectOuterDag()
	if err != nil {
		return err
	}

	// 5. exec dag
	trans.runDag(ctx)

	// 6. change runing state
	trans.runingState = outerStmt
	trans.workHelper = trans.SendChunk
	return nil
}

func (trans *InTransform) runDag(ctx context.Context) {
	go func() {
		err := trans.outerExecutor.ExecuteExecutor(ctx)
		if err != nil {
			trans.inOpLogger.Error("InTransform innerPlan runDag err", zap.Error(err))
		}
	}()
}

func (trans *InTransform) connectOuterDag() error {
	outerDagOutput := trans.outerExecutor.root.transform.GetOutputs()
	if len(outerDagOutput) != 1 {
		return fmt.Errorf("the outerDagOutput should be 1")
	}
	if trans.input == nil {
		port := outerDagOutput[0]
		chunkPort, ok := port.(*ChunkPort)
		if !ok {
			return fmt.Errorf("failed to assert port as *ChunkPort")
		}
		trans.input = NewChunkPort(chunkPort.RowDataType)
	}
	trans.input.ConnectNoneCache(outerDagOutput[0])
	return nil
}

func (trans *InTransform) buildDag(ctx context.Context) error {
	// skip schemaOverLimit check
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		trans.ExecutorBuilder.Analyze(span)
	}
	// skip localstorage perf
	dag, err := trans.ExecutorBuilder.Build(trans.outerPlan)
	if err != nil {
		return err
	}
	var ok bool
	if trans.outerExecutor, ok = dag.(*PipelineExecutor); !ok {
		return fmt.Errorf("buildDag err")
	}
	return nil
}

func (trans *InTransform) buildPlan(ctx context.Context) error {
	trans.outerSchema.ClearInConditions()
	outerPlan, err := buildQueryPlan(ctx, trans.outerStmt, trans.outerQc, trans.outerSchema)
	if err != nil {
		return err
	}
	if outerPlan == nil {
		return fmt.Errorf("InTransform outerPlan is nil")
	}

	// skip localstorage perf
	PrintPlan("InTransform origin outerPlan", outerPlan)
	optimizer := BuildHeuristicPlanner()

	optimizer.SetRoot(outerPlan)
	bestOuterPlan := optimizer.FindBestExp()
	// skip CSStore
	PrintPlan("InTransform optimized outerPlan", bestOuterPlan)
	trans.outerPlan = bestOuterPlan
	return nil
}

func (trans *InTransform) RewriteOuterStmtCondition() error {
	newCondition, err := trans.rewriteOuterStmtConditionDFS(trans.outerSchema.Options().GetCondition())
	if err != nil {
		return err
	}
	// filter all points
	if newCondition == nil {
		return errno.NewError(errno.FilterAllPoints)
	}
	trans.outerSchema.Options().SetCondition(newCondition)
	return nil
}

func (trans *InTransform) rewriteOuterStmtConditionDFS(conditon influxql.Expr) (influxql.Expr, error) {
	switch t := conditon.(type) {
	case *influxql.BinaryExpr:
		lhs, err1 := trans.rewriteOuterStmtConditionDFS(t.LHS)
		if err1 != nil {
			return nil, err1
		}
		rhs, err2 := trans.rewriteOuterStmtConditionDFS(t.RHS)
		if err2 != nil {
			return nil, err2
		}
		if lhs == nil && rhs == nil {
			return nil, fmt.Errorf("lhs and rhs is all nil in binaryExpr")
		} else if lhs == nil {
			if t.Op == influxql.OR {
				return rhs, nil
			} else if t.Op == influxql.AND {
				return nil, nil
			} else {
				return nil, fmt.Errorf("wrong binaryOp type upper lhs inConditon")
			}
		} else if rhs == nil {
			if t.Op == influxql.OR {
				return lhs, nil
			} else if t.Op == influxql.AND {
				return nil, nil
			} else {
				return nil, fmt.Errorf("wrong binaryOp type upper rhs inConditon")
			}
		} else {
			t.LHS = lhs
			t.RHS = rhs
		}
	case *influxql.ParenExpr:
		expr, err := trans.rewriteOuterStmtConditionDFS(t.Expr)
		if err != nil {
			return nil, err
		}
		if expr == nil {
			return nil, nil
		}
		t.Expr = expr
	case *influxql.InCondition:
		return trans.rewriteInCondition(t)
	default:
		return conditon, nil
	}
	return conditon, nil
}

// if len(bufColumn) == 0, return nil, todo: in[a, xx] optimization
func (trans *InTransform) rewriteInCondition(inCondition *influxql.InCondition) (influxql.Expr, error) {
	if len(trans.BufColumnMap) == 0 {
		return nil, nil
	}
	ident := influxql.CloneExpr(trans.OuterVarRef)
	vals := make(map[interface{}]bool)
	for bufVal := range trans.BufColumnMap {
		switch trans.OuterVarRef.Type {
		case influxql.Integer:
			vals[float64(bufVal.(int64))] = true
		default:
			vals[bufVal] = true
		}
	}
	var op influxql.Token = influxql.IN
	if inCondition.NotEqual {
		op = influxql.NOTIN
	}
	node := &influxql.BinaryExpr{LHS: ident, Op: op, RHS: &influxql.SetLiteral{Vals: vals}}
	return node, nil
}

func (trans *InTransform) SendChunk(c Chunk) {
	if c != nil {
		trans.output.State <- c
	}
}

func (trans *InTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *InTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *InTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *InTransform) GetInputNumber(_ Port) int {
	return 0
}
