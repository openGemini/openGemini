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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type GraphTransform struct {
	BaseProcessor
	output      *ChunkPort
	workTracing *tracing.Span
	graphLogger *logger.Logger

	stmt         *influxql.GraphStatement
	chunkBuilder ChunkBuilder
	outputChunk  Chunk
}

type GraphTransformCreator struct {
}

func (c *GraphTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	// todo: more abundant args of graphTransform, like schema...
	graphOp, ok := plan.(*LogicalGraph)
	if !ok {
		return nil, fmt.Errorf("logicalplan is not LogicalGraph")
	}
	p, err := NewGraphTransform(graphOp.stmt)
	return p, err
}

var _ = RegistryTransformCreator(&LogicalGraph{}, &GraphTransformCreator{})

func NewGraphTransform(stmt *influxql.GraphStatement) (*GraphTransform, error) {
	rt := hybridqp.NewRowDataTypeImpl(*influxql.DefaultGraphVarRef())
	trans := &GraphTransform{
		stmt:         stmt,
		graphLogger:  logger.NewLogger(errno.ModuleQueryEngine),
		chunkBuilder: *NewChunkBuilder(rt),
		output:       NewChunkPort(rt),
	}
	return trans, nil
}

func (trans *GraphTransform) Close() {
	trans.output.Close()
}

func (trans *GraphTransform) ParseGraphOtherCondition(cond string) (influxql.Expr, string, string, error) {
	var expr influxql.Expr
	if cond == "" {
		return expr, "", "", nil
	}
	p := influxql.NewParser(strings.NewReader(cond))
	defer p.Release()
	expr, err := p.ParseExpr()
	if err != nil {
		p.Release()
		return expr, "", "", err
	}

	valuer := influxql.NowValuer{Now: time.Now()}
	e, tr, err := influxql.ConditionExpr(expr, &valuer)
	if err != nil {
		return e, "", "", err
	}
	if tr.Min == (time.Time{}) && tr.Max == (time.Time{}) {
		return e, "", "", nil
	}
	msMax := tr.Max.UnixNano() / int64(time.Millisecond)
	msMin := tr.Min.UnixNano() / int64(time.Millisecond)
	if msMax < 0 || msMin < 0 {
		return e, "", "", errors.New("time-range query: start and end time must be provided")
	}
	return e, strconv.FormatInt(msMax, 10), strconv.FormatInt(msMin, 10), nil
}

func OperateParam(e influxql.Expr, param *util.Param) error {
	if param == nil {
		return errors.New("nil param")
	}

	if e != nil {
		paramExpr, ok := e.(*influxql.BinaryExpr)
		if !ok {
			return errno.NewError(errno.ConvertToBinaryExprFailed, e)
		}
		var varRef *influxql.VarRef
		var literal *influxql.StringLiteral
		switch paramExpr.Op {
		case influxql.EQ:
			if varRef, ok = paramExpr.LHS.(*influxql.VarRef); !ok {
				if varRef, ok = paramExpr.RHS.(*influxql.VarRef); !ok {
					return errors.New("unsupported topo filter param condition syntax")
				}
			}
			if literal, ok = paramExpr.RHS.(*influxql.StringLiteral); !ok {
				if literal, ok = paramExpr.LHS.(*influxql.StringLiteral); !ok {
					return errors.New("unsupported topo filter param condition syntax")
				}
			}
			param.ParamName = varRef.Val
			param.ParamValue = literal.Val
		default:
			return fmt.Errorf("unsupported operator: %s", paramExpr.Op)
		}
	}
	return nil
}

func (trans *GraphTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[GraphTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_GraphTransform", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	var startTime string
	var endTime string
	var param util.Param
	if trans.stmt.AdditionalCondition != nil {
		e, msMax, msMin, err := trans.ParseGraphOtherCondition(trans.stmt.AdditionalCondition.String())
		if err != nil {
			return err
		}
		startTime = msMin
		endTime = msMax
		err = OperateParam(e, &param)
		if err != nil {
			return err
		}
	}

	var topoData string
	if util.GetClientConf().Conf.Path != "" {
		response, err := util.GetClientConf().SendGetRequest(util.HttpsClient, param, startTime, endTime)
		if err == nil && response != "" {
			topoData = response
		} else {
			return err
		}
	} else {
		topoData = mockGetTimeGraph()
	}

	graph := NewGraph()
	success, err := graph.CreateGraph(topoData)
	if !success {
		return err
	}
	subGraph, err := graph.MultiHopFilter(trans.stmt.StartNodeId, trans.stmt.HopNum, trans.stmt.NodeCondition, trans.stmt.EdgeCondition)
	if err != nil {
		return err
	}
	trans.outputChunk = trans.chunkBuilder.NewChunk("")
	trans.outputChunk.SetName("graph")
	trans.outputChunk.AppendTagsAndIndex(*NewChunkTagsV2(nil), 0)
	trans.outputChunk.SetGraph(subGraph)
	trans.output.State <- trans.outputChunk
	return nil
}

func (trans *GraphTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *GraphTransform) GetInputs() Ports {
	return Ports{}
}

func (trans *GraphTransform) Explain() []ValuePair {
	return nil
}

func (trans *GraphTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *GraphTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *GraphTransform) Name() string {
	return "GraphTransform"
}
