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

package executor_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

func NewAggIntervalSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	}
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{Duration: 1},
	}
	schema := executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	return schema
}

func TestAggInterval(t *testing.T) {
	executor.GetPlanType = executor.NormalGetPlanType
	schema := NewAggIntervalSchema()
	planType := executor.GetPlanType(schema, nil)
	if planType != executor.AGG_INTERVAL {
		t.Errorf("error plan type")
	} else {
		newPlan, err := executor.NewPlanBySchemaAndSrcPlan(schema, executor.SqlPlanTemplate[planType].GetPlan(), nil)
		if newPlan == nil || err != nil {
			t.Error("nil result")
		}
	}
}

func NewAggIntervalLimitSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	}
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{Duration: 1},
		Limit:    1,
	}
	schema := executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	return schema
}

func TestAggIntervalLimit(t *testing.T) {
	executor.GetPlanType = executor.NormalGetPlanType
	schema := NewAggIntervalLimitSchema()
	planType := executor.GetPlanType(schema, nil)
	if planType != executor.AGG_INTERVAL_LIMIT {
		t.Errorf("error plan type")
	} else {
		newPlan, err := executor.NewPlanBySchemaAndSrcPlan(schema, executor.SqlPlanTemplate[planType].GetPlan(), nil)
		if newPlan == nil || err != nil {
			t.Error("nil result")
		}
	}
}

func NewNoAggNoGroupSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	return schema
}

func TestNoAggNoGroup(t *testing.T) {
	executor.GetPlanType = executor.NormalGetPlanType
	schema := NewNoAggNoGroupSchema()
	planType := executor.GetPlanType(schema, nil)
	if planType != executor.NO_AGG_NO_GROUP {
		t.Errorf("error plan type")
	} else {
		newPlan, err := executor.NewPlanBySchemaAndSrcPlan(schema, executor.SqlPlanTemplate[planType].GetPlan(), nil)
		if newPlan == nil || err != nil {
			t.Error("nil result")
		}
	}
}

func NewAggGroupSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	}
	opt := query.ProcessorOptions{
		Dimensions: []string{"tag1"},
	}
	schema := executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	return schema
}

func TestAggGroup(t *testing.T) {
	executor.GetPlanType = executor.NormalGetPlanType
	schema := NewAggGroupSchema()
	planType := executor.GetPlanType(schema, nil)
	if planType != executor.AGG_GROUP {
		t.Errorf("error plan type")
	} else {
		newPlan, err := executor.NewPlanBySchemaAndSrcPlan(schema, executor.SqlPlanTemplate[planType].GetPlan(), nil)
		if newPlan == nil || err != nil {
			t.Error("nil result")
		}
	}
}

func NewHintSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	opt := query.ProcessorOptions{
		HintType: hybridqp.ExactStatisticQuery,
	}
	schema := executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	return schema
}

func NewHoltWinterSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	schema.SetHoltWinters([]*influxql.Call{&influxql.Call{}})
	return schema
}

func NewAggGroupLimitSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	}
	opt := query.ProcessorOptions{
		Dimensions: []string{"tag1"},
		Limit:      1,
	}
	schema := executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	return schema
}

func NewMultiSourceSchema() hybridqp.Catalog {
	fields := []*influxql.Field{
		{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Integer,
			},
		},
	}
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchemaWithSources(fields, []influxql.Source{&influxql.Measurement{}, &influxql.Measurement{}}, []string{"value"}, &opt, nil)
	return schema
}

func TestUnCachedPlanType(t *testing.T) {
	schema := []hybridqp.Catalog{NewHintSchema(), NewHoltWinterSchema(), NewAggGroupSchema(), NewAggGroupLimitSchema(),
		NewAggGroupSchema(), NewMultiSourceSchema()}
	stmt := []*influxql.SelectStatement{nil, nil, &influxql.SelectStatement{Target: &influxql.Target{}}, nil,
		&influxql.SelectStatement{Sources: []influxql.Source{&influxql.Measurement{Regex: &influxql.RegexLiteral{}}}}, nil}
	for i, s := range schema {
		planType := executor.NormalGetPlanType(s, stmt[i])
		if planType != executor.UNKNOWN {
			t.Errorf("error plan type")
		}
	}
}

func TestUnCachedPlan(t *testing.T) {
	schema := NewAggGroupSchema()
	queryNode := []hybridqp.QueryNode{&executor.LogicalHttpSenderHint{}}
	_, err := executor.NewPlanBySchemaAndSrcPlan(schema, queryNode, nil)
	if err == nil {
		t.Error("error plan result")
	}
}

func TestCachePlan(t *testing.T) {
	for _, planType := range executor.PlanTypes {
		executor.GetPlanType = executor.NilGetPlanType
		unCacheSqlPlan, _ := executor.NewSqlPlanTypePool(planType)
		executor.GetPlanType = executor.NormalGetPlanType
		CachedSqlPlan, _ := executor.NewSqlPlanTypePool(planType)
		if len(unCacheSqlPlan) != len(CachedSqlPlan) {
			t.Error("cache plan error")
		} else {
			for i, unCacheNode := range unCacheSqlPlan {
				if unCacheNode.Type() != CachedSqlPlan[i].Type() {
					t.Error("cache plan error")
					return
				}
			}
		}
	}
}

func TestPlanTypeShard(t *testing.T) {
	init := executor.NewPlanTypeInitShardGroup()
	if init.Close() != nil {
		t.Error("planTypeShard close error")
	}
	if init.MapType(nil, "tag1") != influxql.Tag {
		t.Error("planTypeShard mapType error")
	}
	if _, err := init.LogicalPlanCost(nil, query.ProcessorOptions{}); err != nil {
		t.Error("planTypeShard cost error")
	}
	initMapper := executor.NewPlanTypeInitShardMapper()
	if initMapper.Close() != nil {
		t.Error("planTypeShardMapper close error")
	}
}

func TestPlanTypeSliceByTree(t *testing.T) {
	children := []hybridqp.QueryNode{&executor.LogicalSeries{}, &executor.LogicalSeries{}}
	var root hybridqp.QueryNode = &executor.LogicalMerge{}
	root.SetInputs(children)
	if executor.GetPlanSliceByTree(root) != nil {
		t.Error("slice plantree error")
	}
}

func TestErrorTemplatePlan(t *testing.T) {
	executor.TemplateSql = append(executor.TemplateSql, "error query")
	_, err := executor.NewSqlPlanTypePool(executor.PlanType(len(executor.TemplateSql) - 1))
	assert.ErrorContains(t, err, "syntax error")
	executor.TemplateSql = executor.TemplateSql[:len(executor.TemplateSql)-1]
}
