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
	"context"
	"fmt"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

// only for OptimizeAgg or call PreAggregateCallMapping, some calls like top/bottom/percentile_ogsketch which cancallspushdown but not use template plan
var TemplateSql []string = []string{
	"SELECT max(f1) from mst WHERE (tag1 = 'tag1val') and time > 1 and time < 2 group by time(1ns)",
	"SELECT max(f1) from mst WHERE time < 2 group by time(1ns) limit 1",
	"SELECT * from mst where f1 > 1 and (tag1 = 'tag1val') and time > 1 and time < 2",
	"SELECT last(*) from mst group by tag1",
}

type PlanTypeInitShardMapper struct {
}

func NewPlanTypeInitShardMapper() *PlanTypeInitShardMapper {
	return &PlanTypeInitShardMapper{}
}

func (pts *PlanTypeInitShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions, condition influxql.Expr) (query.ShardGroup, error) {
	shardGroup := NewPlanTypeInitShardGroup()
	return shardGroup, nil
}

func (pts *PlanTypeInitShardMapper) Close() error {
	return nil
}

type PlanTypeInitShard struct {
	fieldMap    map[string]influxql.DataType
	fields      map[string]influxql.DataType
	tags        map[string]struct{}
	tagsSlice   []string
	fieldsSlice []string
}

func NewPlanTypeInitShardGroup() *PlanTypeInitShard {
	tagsSlice := []string{"tag1"}
	fieldsSlice := []string{"f1"}
	fieldMap := make(map[string]influxql.DataType)
	tags := make(map[string]struct{})
	fields := make(map[string]influxql.DataType)
	for _, tag := range tagsSlice {
		fieldMap[tag] = influxql.Tag
		tags[tag] = struct{}{}
	}
	for _, f := range fieldsSlice {
		fieldMap[f] = influxql.Integer
		fields[f] = influxql.Integer
	}
	return &PlanTypeInitShard{
		fieldMap:    fieldMap,
		tagsSlice:   tagsSlice,
		fieldsSlice: fieldsSlice,
		tags:        tags,
		fields:      fields,
	}
}

func (pts *PlanTypeInitShard) GetSeriesKey() []byte {
	return nil
}

func (pts *PlanTypeInitShard) FieldDimensions(
	m *influxql.Measurement) (map[string]influxql.DataType, map[string]struct{}, *influxql.Schema, error) {
	s := &influxql.Schema{
		TagKeys:  pts.tagsSlice,
		Files:    1,
		FileSize: 1,
		MinTime:  influxql.MinTime,
		MaxTime:  influxql.MaxTime,
	}
	return pts.fields, pts.tags, s, nil
}

func (pts *PlanTypeInitShard) MapType(m *influxql.Measurement, field string) influxql.DataType {
	return pts.fieldMap[field]
}

func (pts *PlanTypeInitShard) MapTypeBatch(m *influxql.Measurement, fields map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	for k := range fields {
		fields[k].DataType = pts.fieldMap[k]
	}
	return nil
}

func (pts *PlanTypeInitShard) CreateLogicalPlan(ctx context.Context, sources influxql.Sources, schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	eTraits, err := pts.GetETraits(ctx, sources, schema)
	if err != nil {
		return nil, err
	}
	var plan hybridqp.QueryNode
	var pErr error

	builder := NewLogicalPlanBuilderImpl(schema)

	// push down to chunk reader.
	plan, pErr = builder.CreateSeriesPlan()
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateMeasurementPlan(plan)
	if pErr != nil {
		return nil, pErr
	}

	//todo:create scanner plan
	plan, pErr = builder.CreateScanPlan(plan)
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateShardPlan(plan)
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateNodePlan(plan, eTraits)
	if pErr != nil {
		return nil, pErr
	}

	return plan.(LogicalPlan), pErr
}
func (pts *PlanTypeInitShard) GetETraits(ctx context.Context, sources influxql.Sources, schema hybridqp.Catalog) ([]hybridqp.Trait, error) {
	return []hybridqp.Trait{}, nil
}

func (pts *PlanTypeInitShard) Close() error {
	return nil
}

func (pts *PlanTypeInitShard) GetSources(sources influxql.Sources) influxql.Sources {
	return sources
}

func (pts *PlanTypeInitShard) LogicalPlanCost(source *influxql.Measurement, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	return hybridqp.LogicalPlanCost{}, nil
}

func NewSqlPlanTypePool(planType PlanType) ([]hybridqp.QueryNode, error) {
	sqlReader := strings.NewReader(TemplateSql[planType])
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	var err error
	q, err := yaccParser.GetQuery()
	if err != nil {
		return nil, err
	}
	stmt := q.Statements[0]
	stmt, err = query.RewriteStatement(stmt)
	if err != nil {
		return nil, err
	}
	sopts := query.SelectOptions{}
	shardMapper := NewPlanTypeInitShardMapper()
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("error template query")
	}
	selectStmt.OmitTime = true
	preparedStmt, err := query.Prepare(selectStmt, shardMapper, sopts)
	if err != nil {
		return nil, err
	}
	plan, err := preparedStmt.BuildLogicalPlan(context.Background())
	if err != nil {
		return nil, err
	}
	return GetPlanSliceByTree(plan), nil
}

func GetPlanSliceByTreeDFS(plan hybridqp.QueryNode, dstPlan *[]hybridqp.QueryNode) error {
	if plan.Children() == nil || len(plan.Children()) == 0 {
		*dstPlan = append(*dstPlan, plan)
		return nil
	} else if len(plan.Children()) > 1 {
		return fmt.Errorf("error input number")
	}
	if err := GetPlanSliceByTreeDFS(plan.Children()[0], dstPlan); err != nil {
		return err
	}
	*dstPlan = append(*dstPlan, plan)
	return nil
}

func GetPlanSliceByTree(plan hybridqp.QueryNode) []hybridqp.QueryNode {
	dstPlan := make([]hybridqp.QueryNode, 0)
	if GetPlanSliceByTreeDFS(plan, &dstPlan) != nil {
		return nil
	}
	return dstPlan
}

func NewStorePlanTypePool(planType PlanType) []hybridqp.QueryNode {
	sqlPlan := SqlPlanTemplate[planType].plan
	storePlan := make([]hybridqp.QueryNode, 0)
	for _, node := range sqlPlan {
		if exchange, exOk := node.(*LogicalExchange); exOk {
			if exchange.EType() == NODE_EXCHANGE {
				storeExchange := &LogicalExchange{
					LogicalExchangeBase: LogicalExchangeBase{eType: NODE_EXCHANGE, eRole: PRODUCER_ROLE},
				}
				storePlan = append(storePlan, storeExchange)
				break
			}
		}
		storePlan = append(storePlan, node)
	}
	return storePlan
}

func NewOneShardStorePlanTypePool(planType PlanType) []hybridqp.QueryNode {
	sqlPlan := SqlPlanTemplate[planType].plan
	oneShardStorePlan := make([]hybridqp.QueryNode, 0)
	for i := 0; i < len(sqlPlan); i++ {
		if exchange, exOk := sqlPlan[i].(*LogicalExchange); exOk {
			if exchange.EType() == NODE_EXCHANGE {
				storeExchange := &LogicalExchange{
					LogicalExchangeBase: LogicalExchangeBase{eType: NODE_EXCHANGE, eRole: PRODUCER_ROLE},
				}
				oneShardStorePlan = append(oneShardStorePlan, storeExchange)
				break
			} else if exchange.EType() == SHARD_EXCHANGE {
				if i+1 < len(sqlPlan) {
					_, ok := sqlPlan[i+1].(*LogicalAggregate)
					if ok {
						i++
					}
				}
				continue
			}
		}
		oneShardStorePlan = append(oneShardStorePlan, sqlPlan[i])
	}
	return oneShardStorePlan
}
