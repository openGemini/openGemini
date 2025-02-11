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
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

var SqlPlanTemplate []*PlanTemplate
var StorePlanTemplate []*PlanTemplate
var OneShardStorePlanTemplate []*PlanTemplate
var PlanTypes []PlanType
var MatchPlanFunc []func(hybridqp.Catalog) PlanType
var GetPlanType func(hybridqp.Catalog, *influxql.SelectStatement) PlanType

func init() {
	GetPlanType = NilGetPlanType
	PlanTypes = []PlanType{AGG_INTERVAL, AGG_INTERVAL_LIMIT, NO_AGG_NO_GROUP, AGG_GROUP, NO_AGG_NO_GROUP_LIMIT, AGG_INTERVAL_FILLNONE}
	for _, t := range PlanTypes {
		SqlPlanTemplate = append(SqlPlanTemplate, NewSqlPlanTemplate(t)) // 0-5
		StorePlanTemplate = append(StorePlanTemplate, NewStorePlanTemplate(t))
		OneShardStorePlanTemplate = append(OneShardStorePlanTemplate, NewOneShardStorePlanTemplate(t))
	}
	MatchPlanFunc = append(MatchPlanFunc, MatchAggInterval)
	MatchPlanFunc = append(MatchPlanFunc, MatchAggIntervalLimit)
	MatchPlanFunc = append(MatchPlanFunc, MatchNoAggNoGroup)
	MatchPlanFunc = append(MatchPlanFunc, MatchAggGroup)
	MatchPlanFunc = append(MatchPlanFunc, MatchNoAggNoGroupLimit)
	GetPlanType = NormalGetPlanType
}

func InitLocalStoreTemplatePlan() {
	GetPlanType = NilGetPlanType
	for _, sqlPlan := range SqlPlanTemplate {
		sqlPlan.NewLocalStoreSqlPlanTemplate()
	}
	GetPlanType = NormalGetPlanType
}

func GetStorePlanTemplate(shardNum int, planType PlanType) []hybridqp.QueryNode {
	if shardNum == 1 {
		return OneShardStorePlanTemplate[planType].GetPlan()
	}
	return StorePlanTemplate[planType].GetPlan()
}

func MatchAggInterval(schema hybridqp.Catalog) PlanType {
	if GetEnableFileCursor() && schema.HasCall() && schema.HasInterval() && !schema.HasLimit() &&
		schema.HasOptimizeCall() && schema.CanCallsPushdown() && !schema.ContainSeriesIgnoreCall() {
		if schema.Options().(*query.ProcessorOptions).Fill == influxql.NoFill {
			return AGG_INTERVAL_FILLNONE
		}
		return AGG_INTERVAL
	}
	return UNKNOWN
}

func MatchAggIntervalLimit(schema hybridqp.Catalog) PlanType {
	if GetEnableFileCursor() && schema.HasLimit() && schema.HasCall() && schema.HasOptimizeCall() &&
		schema.CanCallsPushdown() && schema.HasInterval() && !schema.ContainSeriesIgnoreCall() &&
		len(schema.Options().GetDimensions()) == 0 {
		return AGG_INTERVAL_LIMIT
	}
	return UNKNOWN
}

func MatchNoAggNoGroup(schema hybridqp.Catalog) PlanType {
	if !schema.HasCall() && !schema.HasLimit() && !schema.HasInterval() && len(schema.Options().GetDimensions()) == 0 {
		return NO_AGG_NO_GROUP
	}
	return UNKNOWN
}

func MatchAggGroup(schema hybridqp.Catalog) PlanType {
	if schema.MatchPreAgg() && len(schema.Options().GetDimensions()) > 0 && !schema.HasLimit() {
		return AGG_GROUP
	}
	return UNKNOWN
}

func MatchNoAggNoGroupLimit(schema hybridqp.Catalog) PlanType {
	if !schema.HasCall() && schema.HasLimit() && !schema.HasInterval() && len(schema.Options().GetDimensions()) == 0 && !schema.Options().IsExcept() {
		return NO_AGG_NO_GROUP_LIMIT
	}
	return UNKNOWN
}

type PlanType uint32

const (
	AGG_INTERVAL PlanType = iota
	AGG_INTERVAL_LIMIT
	NO_AGG_NO_GROUP
	AGG_GROUP
	NO_AGG_NO_GROUP_LIMIT
	AGG_INTERVAL_FILLNONE
	UNKNOWN
)

type PlanTemplate struct {
	planType       PlanType
	plan           []hybridqp.QueryNode
	localStorePlan []hybridqp.QueryNode
}

func NewSqlPlanTemplate(t PlanType) *PlanTemplate {
	pp := &PlanTemplate{
		planType:       t,
		plan:           nil,
		localStorePlan: nil,
	}
	var err error
	pp.plan, err = NewSqlPlanTypePool(t)
	if err != nil {
		panic(fmt.Errorf("sql plan type init error: %v", err))
	}
	return pp
}

func (pp *PlanTemplate) NewLocalStoreSqlPlanTemplate() {
	var err error
	pp.localStorePlan, err = NewLocalStoreSqlPlanTypePool(pp.planType)
	if err != nil {
		panic(fmt.Errorf("local store sql plan type init error: %v", err))
	}
}

func NewStorePlanTemplate(t PlanType) *PlanTemplate {
	pp := &PlanTemplate{
		planType: t,
		plan:     nil,
	}
	pp.plan = NewStorePlanTypePool(t)
	return pp
}

func NewOneShardStorePlanTemplate(t PlanType) *PlanTemplate {
	pp := &PlanTemplate{
		planType: t,
		plan:     nil,
	}
	pp.plan = NewOneShardStorePlanTypePool(t)
	return pp
}

func (pp *PlanTemplate) GetPlan() []hybridqp.QueryNode {
	return pp.plan
}

func (pp *PlanTemplate) GetLocalStorePlan() []hybridqp.QueryNode {
	return pp.localStorePlan
}

func NilGetPlanType(schema hybridqp.Catalog, stmt *influxql.SelectStatement) PlanType {
	return UNKNOWN
}

func NormalGetPlanType(schema hybridqp.Catalog, stmt *influxql.SelectStatement) PlanType {
	// Avoid promql function call
	hasPromCall := len(schema.GetPromCalls()) > 0
	isPromQuery := schema.Options().IsPromQuery()
	hasPromSortField := (len(schema.Options().GetSortFields()) > 0)
	if hasPromCall || (isPromQuery && hasPromSortField) {
		return UNKNOWN
	}

	// Avoid subquery hint
	hintType := schema.Options().GetHintType()
	if schema.HasSubQuery() || (hintType != hybridqp.DefaultNoHint && hintType != hybridqp.FullSeriesQuery) {
		return UNKNOWN
	}

	// Avoid all special operators
	if schema.HasSlidingWindowCall() || schema.HasHoltWintersCall() || schema.HasBlankRowCall() {
		return UNKNOWN
	}

	// Avoid target regex source
	if stmt != nil {
		if stmt.Target != nil {
			return UNKNOWN
		}
		if m, rex := stmt.Sources[0].(*influxql.Measurement); rex {
			if m.Regex != nil {
				return UNKNOWN
			}
		}
	}

	// Avoid multiSource
	if len(schema.Sources()) > 1 {
		return UNKNOWN
	}

	// Avoid promql range vector selector
	if schema.Options().IsRangeVectorSelector() {
		return UNKNOWN
	}

	if typ := MatchPlanFunc[0](schema); typ != UNKNOWN {
		return typ
	}

	// Avoid the lack of the Fill operator.
	if schema.Options().(*query.ProcessorOptions).Fill == influxql.NoFill {
		return UNKNOWN
	}

	for i := 1; i < len(MatchPlanFunc); i++ {
		if typ := MatchPlanFunc[i](schema); typ != UNKNOWN {
			return typ
		}
	}
	return UNKNOWN
}
