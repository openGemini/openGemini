/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

var SqlPlanTemplate []*PlanTemplate
var StorePlanTemplate []*PlanTemplate
var OneShardStorePlanTemplate []*PlanTemplate
var PlanTypes []PlanType
var MatchPlanFunc []func(hybridqp.Catalog) bool
var GetPlanType func(hybridqp.Catalog, *influxql.SelectStatement) PlanType

func init() {
	GetPlanType = NilGetPlanType
	PlanTypes = []PlanType{AGG_INTERVAL, AGG_INTERVAL_LIMIT, NO_AGG_NO_GROUP, AGG_GROUP}
	for _, t := range PlanTypes {
		SqlPlanTemplate = append(SqlPlanTemplate, NewSqlPlanTemplate(t)) // 0 1 2 3
		StorePlanTemplate = append(StorePlanTemplate, NewStorePlanTemplate(t))
		OneShardStorePlanTemplate = append(OneShardStorePlanTemplate, NewOneShardStorePlanTemplate(t))
	}
	MatchPlanFunc = append(MatchPlanFunc, MatchAggInterval)
	MatchPlanFunc = append(MatchPlanFunc, MatchAggIntervalLimit)
	MatchPlanFunc = append(MatchPlanFunc, MatchNoAggNoGroup)
	MatchPlanFunc = append(MatchPlanFunc, MatchAggGroup)
	GetPlanType = NormalGetPlanType
}

func GetStorePlanTemplate(shardNum int, planType PlanType) []hybridqp.QueryNode {
	if shardNum == 1 {
		return OneShardStorePlanTemplate[planType].GetPlan()
	}
	return StorePlanTemplate[planType].GetPlan()
}

func MatchAggInterval(schema hybridqp.Catalog) bool {
	if GetEnableFileCursor() && schema.HasCall() && schema.HasInterval() && !schema.HasLimit() &&
		schema.HasOptimizeCall() && schema.CanCallsPushdown() && !schema.ContainSeriesIgnoreCall() {
		return true
	}
	return false
}

func MatchAggIntervalLimit(schema hybridqp.Catalog) bool {
	if GetEnableFileCursor() && schema.HasLimit() && schema.HasCall() && schema.HasOptimizeCall() &&
		schema.CanCallsPushdown() && schema.HasInterval() && !schema.ContainSeriesIgnoreCall() &&
		(schema.Options().GetDimensions() == nil || len(schema.Options().GetDimensions()) == 0) {
		return true
	}
	return false
}

func MatchNoAggNoGroup(schema hybridqp.Catalog) bool {
	if !schema.HasCall() && !schema.HasLimit() && !schema.HasInterval() &&
		(schema.Options().GetDimensions() == nil || len(schema.Options().GetDimensions()) == 0) {
		return true
	}
	return false
}

func MatchAggGroup(schema hybridqp.Catalog) bool {
	if schema.MatchPreAgg() && schema.Options().GetDimensions() != nil && len(schema.Options().GetDimensions()) > 0 &&
		!schema.HasLimit() {
		return true
	}
	return false
}

type PlanType uint32

const (
	AGG_INTERVAL PlanType = iota
	AGG_INTERVAL_LIMIT
	NO_AGG_NO_GROUP
	AGG_GROUP
	UNKNOWN
)

type PlanTemplate struct {
	planType PlanType
	plan     []hybridqp.QueryNode
}

func NewSqlPlanTemplate(t PlanType) *PlanTemplate {
	pp := &PlanTemplate{
		planType: t,
		plan:     nil,
	}
	var err error
	pp.plan, err = NewSqlPlanTypePool(t)
	if err != nil {
		panic("sql plan type init error")
	}
	return pp
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

func NilGetPlanType(schema hybridqp.Catalog, stmt *influxql.SelectStatement) PlanType {
	return UNKNOWN
}

func NormalGetPlanType(schema hybridqp.Catalog, stmt *influxql.SelectStatement) PlanType {
	// Avoid subquery hint
	if schema.HasSubQuery() || (schema.Options().GetHintType() != hybridqp.DefaultNoHint && schema.Options().GetHintType() != hybridqp.FullSeriesQuery) {
		return UNKNOWN
	}

	// Avoid all special operators
	if schema.HasSlidingWindowCall() || schema.HasHoltWintersCall() || schema.HasBlankRowCall() {
		return UNKNOWN
	}
	// Avoid the lack of the Fill operator.
	if schema.Options().(*query.ProcessorOptions).Fill == influxql.NoFill {
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

	for i := range PlanTypes {
		if MatchPlanFunc[i](schema) {
			return PlanTypes[i]
		}
	}
	return UNKNOWN
}
