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
	"fmt"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type SubQueryBuilder struct {
	qc   query.LogicalPlanCreator
	stmt *influxql.SelectStatement
}

func (b *SubQueryBuilder) newSubOptions(ctx context.Context, opt query.ProcessorOptions) (query.ProcessorOptions, error) {
	if len(b.stmt.ExceptDimensions) > 0 {
		return query.ProcessorOptions{}, fmt.Errorf("except: sub-query or join-query is unsupported")
	}
	subOpt, err := query.NewProcessorOptionsStmt(b.stmt, query.SelectOptions{
		Authorizer:  opt.Authorizer,
		MaxSeriesN:  opt.MaxSeriesN,
		ChunkedSize: opt.ChunkedSize,
		Chunked:     opt.Chunked,
		ChunkSize:   opt.ChunkSize,
		RowsChan:    opt.RowsChan,
	})

	if err != nil {
		return query.ProcessorOptions{}, err
	}

	if subOpt.StartTime < opt.StartTime {
		subOpt.StartTime = opt.StartTime
	}
	if subOpt.EndTime > opt.EndTime {
		subOpt.EndTime = opt.EndTime
	}
	if !subOpt.Interval.IsZero() && subOpt.EndTime == influxql.MaxTime {
		if now := ctx.Value(NowKey); now != nil {
			subOpt.EndTime = now.(time.Time).UnixNano()
		}
	}
	// use dimPushDown: 1.noPromQuery 2.PromQuery agg by(xx) call(mst[range])
	if !opt.PromQuery || subOpt.GroupByAllDims && subOpt.Range > 0 {
		pushDownDimension := GetInnerDimensions(opt.Dimensions, subOpt.Dimensions)
		subOpt.Dimensions = pushDownDimension
		for d := range opt.GroupBy {
			subOpt.GroupBy[d] = struct{}{}
		}
		if opt.PromQuery && len(pushDownDimension) > 0 {
			subOpt.GroupByAllDims = opt.GroupByAllDims
		}
		if opt.Without {
			subOpt.Without = opt.Without
		}
	}

	valuer := &influxql.NowValuer{Location: b.stmt.Location}
	cond, t, err := influxql.ConditionExpr(b.stmt.Condition, valuer)
	if err != nil {
		return query.ProcessorOptions{}, err
	}
	subOpt.Condition = cond
	if !t.Min.IsZero() && t.MinTimeNano() > opt.StartTime {
		subOpt.StartTime = t.MinTimeNano()
	}
	if !t.Max.IsZero() && t.MaxTimeNano() < opt.EndTime {
		subOpt.EndTime = t.MaxTimeNano()
	}

	subOpt.SLimit += opt.SLimit
	subOpt.SOffset += opt.SOffset

	subOpt.Ascending = opt.Ascending

	if !b.stmt.IsRawQuery && subOpt.Fill == influxql.NullFill {
		subOpt.Fill = influxql.NoFill
	}
	subOpt.PromQuery = opt.PromQuery
	subOpt.Ordered = opt.Ordered
	subOpt.HintType = opt.HintType
	subOpt.StmtId = opt.StmtId
	subOpt.MaxParallel = opt.MaxParallel
	opt.Step = b.stmt.Step
	opt.Range = b.stmt.Range
	opt.LookBackDelta = b.stmt.LookBackDelta
	opt.QueryOffset = b.stmt.QueryOffset
	return subOpt, nil
}

func (b *SubQueryBuilder) Build(ctx context.Context, opt *query.ProcessorOptions) (hybridqp.QueryNode, error) {
	subOpt, err := b.newSubOptions(ctx, *opt)
	if err != nil {
		return nil, err
	}
	schema := NewQuerySchemaWithJoinCase(b.stmt.Fields, b.stmt.Sources, b.stmt.ColumnNames(), &subOpt, b.stmt.JoinSource,
		b.stmt.UnnestSource, b.stmt.SortFields)
	schema.SetPromCalls(b.stmt.PromSubCalls)
	opt.LowerOpt = &subOpt
	return buildQueryPlan(ctx, b.stmt, b.qc, schema)
}

func GetInnerDimensions(outer, inner []string) []string {
	dimensionPushDownMap := make(map[string]struct{})
	pushDownDimension := make([]string, 0, len(outer)+len(inner))
	pushDownDimension = append(pushDownDimension, outer...)
	for _, d := range outer {
		dimensionPushDownMap[d] = struct{}{}
	}
	for i, d := range inner {
		if _, ok := dimensionPushDownMap[inner[i]]; !ok {
			pushDownDimension = append(pushDownDimension, d)
		}
	}
	return pushDownDimension
}
