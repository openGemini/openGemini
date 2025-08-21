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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func ComparePlanNode(a, b hybridqp.QueryNode) bool {
	if a.String() != b.String() {
		return false
	}

	ac := a.Children()
	bc := b.Children()

	if len(ac) != len(bc) {
		return false
	}

	for i, p := range ac {
		same := ComparePlanNode(p, bc[i])
		if !same {
			return false
		}
	}

	return true
}

func createQuerySchema() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createFields(), createColumnNames(), &opt, nil)
	m := createMeasurement()
	schema.AddTable(m, schema.MakeRefs())

	return schema
}

func createQuerySchemaWithCalls() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	fields := createFields()
	fields = append(fields, &influxql.Field{
		Expr: &influxql.Call{
			Name: "mean",
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  "id",
					Type: influxql.Integer,
				},
			},
		},
	})
	names := createColumnNames()
	names = append(names, "mean")
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	m := createMeasurement()
	schema.AddTable(m, schema.MakeRefs())

	return schema
}

func createQuerySchemaWithCountCalls() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	fields := createFields()
	fields = append(fields, &influxql.Field{
		Expr: &influxql.Call{
			Name: "count",
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  "id",
					Type: influxql.Integer,
				},
			},
		},
	})
	names := createColumnNames()
	names = append(names, "count")
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	m := createMeasurement()
	schema.AddTable(m, schema.MakeRefs())

	return schema
}

func IsAuxExprOptions(ExprOptions []hybridqp.ExprOptions) bool {
	for _, option := range ExprOptions {
		switch option.Expr.(type) {
		case *influxql.VarRef:
			continue
		default:
			return false
		}
	}

	return true
}

func TestLogicalPlan(t *testing.T) {
	schema := createQuerySchema()

	series := executor.NewLogicalSeries(schema)
	reader := executor.NewLogicalReader(series, schema)

	if len(reader.RowExprOptions()) != 4 {
		t.Errorf("call options of reader must be 4, but %v", len(reader.RowExprOptions()))
	}

	if !IsAuxExprOptions(reader.RowExprOptions()) {
		t.Error("call options of reader must be aux call")
	}

	agg := executor.NewLogicalAggregate(reader, schema)

	if len(agg.RowExprOptions()) != 4 {
		t.Errorf("call options of agg must be 4, but %v", len(agg.RowExprOptions()))
	}

	if !IsAuxExprOptions(agg.RowExprOptions()) {
		t.Error("call options of agg must be aux call")
	}
}

func createFieldsFilterBlank() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "non_negative_difference",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "id",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "non_negative_difference",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "score",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
	)

	return fields
}

func createColumnNamesFilterBlank() []string {
	return []string{"id", "score"}
}

func createQuerySchemaFilterBlank() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createFieldsFilterBlank(), createColumnNamesFilterBlank(), &opt, nil)
	m := createMeasurement()
	schema.AddTable(m, schema.MakeRefs())

	return schema
}

func TestLogicalPlanFilterBlank(t *testing.T) {
	schema := createQuerySchemaFilterBlank()

	series := executor.NewLogicalSeries(schema)
	reader := executor.NewLogicalReader(series, schema)

	if len(reader.RowExprOptions()) != 2 {
		t.Errorf("call options of reader must be 2, but %v", len(reader.RowExprOptions()))
	}

	agg := executor.NewLogicalAggregate(reader, schema)

	if len(agg.RowExprOptions()) != 2 {
		t.Errorf("call options of agg must be 2, but %v", len(agg.RowExprOptions()))
	}
	if !schema.HasBlankRowCall() {
		t.Errorf("Options have calls that maybe generate blank row")
	}

	filterBlank := executor.NewLogicalFilterBlank(agg, schema)

	if !IsAuxExprOptions(filterBlank.RowExprOptions()) {
		t.Error("call options of agg must be aux call")
	}
}

func TestNewLogicalSTSSPScan(t *testing.T) {
	schema := createQuerySchema()

	reader := executor.NewLogicalTSSPScan(schema)
	_ = reader.Digest()
	_ = reader.Schema()
	_ = reader.RowDataType()
	_ = reader.Dummy()
	reader.DeriveOperations()
	if len(reader.RowExprOptions()) != len(schema.GetColumnNames()) {
		t.Errorf("call options of reader must be 0, but %v", len(reader.RowExprOptions()))
	}

	if !IsAuxExprOptions(reader.RowExprOptions()) {
		t.Error("call options of reader must be aux call")
	}
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	reader.Explain(planWriter)
	child := reader.Children()
	if child != nil {
		t.Error("children of reader must be nil")
	}
}

func TestNewLogicalSequenceAggregate(t *testing.T) {
	schema := createQuerySchemaWithCalls()
	node := executor.NewLogicalSeries(schema)
	agg := executor.NewLogicalSequenceAggregate(node, schema)
	_ = agg.Digest()
	_ = agg.Schema()
	_ = agg.RowDataType()
	_ = agg.Dummy()
	agg.DeriveOperations()
	aggClone := agg.Clone()
	if aggClone.Type() != agg.Type() {
		t.Error("wrong result")
	}
}

func TestNewLogicalSplitGroup(t *testing.T) {
	schema := createQuerySchemaWithCalls()
	node := executor.NewLogicalSeries(schema)
	sg := executor.NewLogicalSplitGroup(node, schema)
	sg2 := executor.NewLogicalSplitGroup(node, schema)
	_ = sg.Digest()
	_ = sg.Schema()
	_ = sg.RowDataType()
	_ = sg.Dummy()
	_ = sg.RowExprOptions()
	sg.ReplaceChild(0, sg2)
	sg.DeriveOperations()
	aggClone := sg.Clone()
	if aggClone.Type() != sg.Type() {
		t.Error("wrong result")
	}
}

func TestNewLogicalFullJoin(t *testing.T) {
	schema := createQuerySchema()
	node := executor.NewLogicalSeries(schema)
	leftSubquery := executor.NewLogicalSubQuery(node, schema)
	rightSubquery := executor.NewLogicalSubQuery(node, schema)
	fullJoin := executor.NewLogicalFullJoin(leftSubquery, rightSubquery, nil, schema)
	fullJoinClone := fullJoin.Clone()
	if fullJoinClone.Type() != fullJoin.Type() {
		t.Error("wrong result")
	}
}

func createHoltWintersQuerySchema() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	var fields influxql.Fields
	var callArgs []influxql.Expr
	callArgs = append(callArgs, &influxql.VarRef{
		Val:  "value",
		Type: influxql.Float,
	})
	var args []influxql.Expr
	args = append(args, &influxql.Call{
		Name: "count",
		Args: callArgs,
	})
	args = append(args, &influxql.IntegerLiteral{
		Val: 30,
	})
	args = append(args, &influxql.IntegerLiteral{
		Val: 4,
	})
	fields = append(fields, &influxql.Field{
		Expr: &influxql.Call{
			Name: "holt_winters_with_fit",
			Args: args,
		},
	})
	var names []string
	names = append(names, "holt_winters_with_fit")
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	m := createMeasurement()
	schema.AddTable(m, schema.MakeRefs())

	return schema
}

func TestNewHoltWintersQueryTransforms(t *testing.T) {
	schema := createHoltWintersQuerySchema()
	node := executor.NewLogicalSeries(schema)
	project := executor.NewLogicalProject(node, schema)
	holtWinters := executor.NewLogicalHoltWinters(project, schema)
	limitParas := executor.LimitTransformParameters{}
	limit := executor.NewLogicalLimit(holtWinters, schema, limitParas)
	httpsender := executor.NewLogicalHttpSender(limit, schema)
	httpsenderClone := httpsender.Clone()
	if httpsender.Type() != httpsenderClone.Type() {
		t.Error("wrong result")
	}
}

func TestSetHoltWintersType(t *testing.T) {
	var fields influxql.Fields
	f := &influxql.IntegerLiteral{Val: 1}
	fields = append(fields, &influxql.Field{Expr: f})
	p := executor.NewLogicalPlanSingle(nil, nil)
	p.SetHoltWintersType(true, fields)
}

func createSortQuerySchema() *executor.QuerySchema {
	m := createMeasurement()
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}}
	var fields influxql.Fields
	fields = append(fields, &influxql.Field{
		Expr: &influxql.VarRef{
			Val:   "f1",
			Type:  influxql.Integer,
			Alias: "",
		},
	})
	var names []string
	names = append(names, "f1")
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())
	return schema
}

func TestNewLogicalSort(t *testing.T) {
	schema := createSortQuerySchema()
	node := executor.NewLogicalSeries(schema)
	sort := executor.NewLogicalSort(node, schema)
	newSort := sort.Clone().(*executor.LogicalSort)
	str := newSort.Digest()
	tStr := newSort.Type()
	if newSort == nil {
		t.Error("wrong result")
	} else {
		fmt.Println(tStr, str)
	}
}

func createPromSortQuerySchema() *executor.QuerySchema {
	m := createMeasurement()
	opt := query.ProcessorOptions{Sources: []influxql.Source{m}}
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
		},
	}
	names := []string{"value"}
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	schema.AddTable(m, schema.MakeRefs())
	return schema
}

func TestNewLogicalPromSort(t *testing.T) {
	schema := createPromSortQuerySchema()
	node := executor.NewLogicalSeries(schema)
	sort := executor.NewLogicalPromSort(node, schema)
	newSort := sort.Clone().(*executor.LogicalPromSort)
	str := newSort.Digest()
	tStr := newSort.Type()
	if newSort == nil {
		t.Error("wrong result")
	} else {
		fmt.Println(tStr, str)
	}
}

func TestNewLogicalHashMerge(t *testing.T) {
	schema := createSortQuerySchema()
	node := executor.NewLogicalSeries(schema)
	merge := executor.NewLogicalHashMerge(node, schema, executor.NODE_EXCHANGE, nil)
	merge.DeriveOperations()
	newMerge := merge.Clone().(*executor.LogicalHashMerge)
	str := newMerge.Digest()
	tStr := newMerge.String()
	tType := newMerge.Type()
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	newMerge.Explain(planWriter)
	if newMerge == nil {
		t.Error("wrong result")
	} else {
		fmt.Println(tStr, str, tType)
	}
	marStr, err := executor.MarshalBinary(newMerge)
	if err != nil {
		t.Error("wrong result")
	} else {
		fmt.Println(marStr)
	}
	newMerge1 := merge.New([]hybridqp.QueryNode{node}, schema, nil).(*executor.LogicalHashMerge)
	assert.Equal(t, newMerge1.EType(), executor.NODE_EXCHANGE)
	assert.Equal(t, newMerge1.ERole(), executor.CONSUMER_ROLE)
	assert.Equal(t, newMerge1.ETraits(), []hybridqp.Trait(nil))
	assert.Equal(t, newMerge1.Schema().GetColumnNames(), schema.GetColumnNames())

	newMerge1.ToProducer()
	assert.Equal(t, newMerge1.ERole(), executor.PRODUCER_ROLE)

	merge1 := executor.NewLogicalHashMerge(node, schema, executor.SHARD_EXCHANGE, nil)
	newMerge1 = merge1.New([]hybridqp.QueryNode{node}, schema, nil).(*executor.LogicalHashMerge)
	assert.Equal(t, newMerge1.EType(), executor.SHARD_EXCHANGE)
}

func TestNewLogicalHashAgg(t *testing.T) {
	schema := createSortQuerySchema()
	node := executor.NewLogicalSeries(schema)
	agg := executor.NewLogicalHashAgg(node, schema, executor.NODE_EXCHANGE, nil)
	agg.DeriveOperations()
	newAgg := agg.Clone().(*executor.LogicalHashAgg)
	str := newAgg.Digest()
	tStr := newAgg.String()
	tType := newAgg.Type()
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	newAgg.Explain(planWriter)
	if newAgg == nil {
		t.Error("wrong result")
	} else {
		fmt.Println(tStr, str, tType)
	}
	marStr, err := executor.MarshalBinary(newAgg)
	if err != nil {
		t.Error("wrong result")
	} else {
		fmt.Println(marStr)
	}
	newAgg1 := agg.New([]hybridqp.QueryNode{node}, schema, nil).(*executor.LogicalHashAgg)
	assert.Equal(t, newAgg1.EType(), executor.NODE_EXCHANGE)
	assert.Equal(t, newAgg1.ERole(), executor.CONSUMER_ROLE)
	assert.Equal(t, newAgg1.ETraits(), []hybridqp.Trait(nil))
	assert.Equal(t, newAgg1.Schema().GetColumnNames(), schema.GetColumnNames())
	newAgg1.ToProducer()
	assert.Equal(t, newAgg1.ERole(), executor.PRODUCER_ROLE)

	agg1 := executor.NewLogicalHashAgg(node, schema, executor.SHARD_EXCHANGE, nil)
	newAgg1 = agg1.New([]hybridqp.QueryNode{node}, schema, nil).(*executor.LogicalHashAgg)
	assert.Equal(t, newAgg1.EType(), executor.SHARD_EXCHANGE)
}

func TestLogicalPlanNilNew(t *testing.T) {
	logicalNode := []hybridqp.QueryNode{&executor.LogicalSlidingWindow{}, &executor.LogicalFilter{}, &executor.LogicalSortAppend{},
		&executor.LogicalDistinct{}, &executor.LogicalFilterBlank{}, &executor.LogicalAlign{}, &executor.LogicalMst{}, &executor.LogicalSubQuery{},
		&executor.LogicalTagSubset{}, &executor.LogicalGroupBy{}, &executor.LogicalOrderBy{}, &executor.LogicalHttpSenderHint{},
		&executor.LogicalTarget{}, &executor.LogicalDummyShard{}, &executor.LogicalTSSPScan{}, &executor.LogicalWriteIntoStorage{},
		&executor.LogicalSequenceAggregate{}, &executor.LogicalSplitGroup{}, &executor.LogicalFullJoin{}, &executor.LogicalHoltWinters{},
		&executor.LogicalSort{}, &executor.LogicalMerge{}, &executor.LogicalSortMerge{}}
	newResult := make([]hybridqp.QueryNode, len(logicalNode))
	for i, node := range logicalNode {
		if newResult[i] != node.New(nil, nil, nil) {
			t.Error("nil new fail in logical plan")
		}
	}
}

func TestNewLogicalSparseIndexScan(t *testing.T) {
	schema := createSortQuerySchema()
	node := executor.NewLogicalSeries(schema)
	indexScan := executor.NewLogicalSparseIndexScan(node, schema)
	indexScan.DeriveOperations()
	newIndexScan := indexScan.Clone().(*executor.LogicalSparseIndexScan)
	str := newIndexScan.Digest()
	tStr := newIndexScan.String()
	tType := newIndexScan.Type()
	newIndexScan.New(nil, nil, nil)
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	newIndexScan.Explain(planWriter)
	if newIndexScan == nil {
		t.Error("wrong result")
	} else {
		fmt.Println(tStr, str, tType)
	}
	marStr, err := executor.MarshalBinary(newIndexScan)
	if err != nil {
		t.Error("wrong result")
	} else {
		fmt.Println(marStr)
	}
}

func TestNewLogicalColumnStoreReader(t *testing.T) {
	schema := createSortQuerySchema()
	node := executor.NewLogicalSeries(schema)
	reader := executor.NewLogicalColumnStoreReader(node, schema)
	reader.DeriveOperations()
	newReader := reader.Clone().(*executor.LogicalColumnStoreReader)
	str := newReader.Digest()
	tStr := newReader.String()
	tType := newReader.Type()
	reader.MstName()
	newReader.New(nil, nil, nil)
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	newReader.Explain(planWriter)
	if newReader == nil {
		t.Error("wrong result")
	} else {
		fmt.Println(tStr, str, tType)
	}
	marStr, err := executor.MarshalBinary(newReader)
	if err != nil {
		t.Error("wrong result")
	} else {
		fmt.Println(marStr)
	}
}

func TestRebuildColumnStorePlan(t *testing.T) {
	schema := createSortQuerySchema()
	reader := executor.NewLogicalColumnStoreReader(nil, schema)
	readerExchange := executor.NewLogicalExchange(reader, executor.READER_EXCHANGE, nil, reader.Schema())
	readerMerge := executor.NewLogicalHashMerge(readerExchange, schema, executor.READER_EXCHANGE, nil)
	nodeMerge := executor.NewLogicalHashMerge(readerMerge, schema, executor.NODE_EXCHANGE, nil)
	best := executor.RebuildColumnStorePlan(nodeMerge)[0]
	executor.RebuildAggNodes(best)
	if best == nil {
		t.Fatalf(" wrong result")
	}

	agg := executor.NewLogicalAggregate(readerMerge, schema)
	nodeExchange := executor.NewLogicalExchange(agg, executor.NODE_EXCHANGE, nil, agg.Schema())
	merge := executor.NewLogicalHashMerge(nodeExchange, schema, executor.NODE_EXCHANGE, nil)
	best = executor.RebuildColumnStorePlan(merge)[0]
	executor.RebuildAggNodes(best)
	if best == nil {
		t.Fatalf(" wrong result")
	}

	agg = executor.NewLogicalAggregate(readerMerge, schema)
	orderBy := executor.NewLogicalOrderBy(agg, agg.Schema())
	groupBy := executor.NewLogicalGroupBy(orderBy, orderBy.Schema())
	subQuery := executor.NewLogicalSubQuery(groupBy, groupBy.Schema())
	merge = executor.NewLogicalHashMerge(subQuery, schema, executor.NODE_EXCHANGE, nil)
	best = executor.RebuildColumnStorePlan(merge)[0]
	executor.RebuildAggNodes(best)
	if best == nil {
		t.Fatalf(" wrong result")
	}

	merge = executor.NewLogicalHashMerge(readerMerge, schema, executor.NODE_EXCHANGE, nil)
	subQuery = executor.NewLogicalSubQuery(merge, groupBy.Schema())
	groupBy = executor.NewLogicalGroupBy(subQuery, orderBy.Schema())
	orderBy = executor.NewLogicalOrderBy(groupBy, agg.Schema())
	agg = executor.NewLogicalAggregate(orderBy, schema)
	best = executor.RebuildColumnStorePlan(agg)[0]
	executor.RebuildAggNodes(best)
	best = executor.ReplaceSortAggWithHashAgg(best)[0]
	if best == nil {
		t.Fatalf(" wrong result")
	}
	assert.Equal(t, executor.ReplaceSortAggWithHashAgg(reader)[0], reader)
}

func TestRebuildColumnStorePlan_UnifyPlan(t *testing.T) {
	// query without agg
	schema := createSortQuerySchema()
	reader := executor.NewLogicalColumnStoreReader(nil, schema)
	readerExchange := executor.NewLogicalExchange(reader, executor.READER_EXCHANGE, nil, reader.Schema())
	shardExchange := executor.NewLogicalExchange(readerExchange, executor.SHARD_EXCHANGE, nil, readerExchange.Schema())
	best := executor.ReplaceSortMergeWithHashMerge(shardExchange)[0]
	if best == nil {
		t.Fatalf(" wrong result")
	}

	// query with agg
	readAgg := executor.NewLogicalAggregate(reader, reader.Schema())
	readerExchange1 := executor.NewLogicalExchange(readAgg, executor.READER_EXCHANGE, nil, readAgg.Schema())
	shardAgg := executor.NewLogicalAggregate(readerExchange1, readerExchange1.Schema())
	shardExchange1 := executor.NewLogicalExchange(shardAgg, executor.SHARD_EXCHANGE, nil, shardAgg.Schema())
	nodeAgg := executor.NewLogicalAggregate(shardExchange1, shardExchange1.Schema())
	best = executor.ReplaceSortAggMergeWithHashAgg(nodeAgg)[0]
	best = executor.ReplaceSortMergeWithHashMerge(best)[0]
	if best == nil {
		t.Fatalf(" wrong result")
	}
}

func buildColumnStorePlan(t *testing.T, schema *executor.QuerySchema) hybridqp.QueryNode {
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	var err error
	if plan, err = planBuilder.CreateSegmentPlan(schema); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateMeasurementPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateSparseIndexScanPlan(plan); err != nil {
		t.Error(err.Error())
	}
	if plan, err = planBuilder.CreateNodePlan(plan, nil); err != nil {
		t.Error(err.Error())
	}
	planBuilder.Push(plan)
	planBuilder.HashMerge(executor.NODE_EXCHANGE, nil)
	planBuilder.Sort()
	if plan, err = planBuilder.Build(); err != nil {
		t.Error(err.Error())
	}
	return plan
}

func buildColumnStorePlanForSql(schema *executor.QuerySchema) hybridqp.QueryNode {
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)
	plan, _ := coordinator.CreateColumnStorePlan(schema, nil, planBuilder)
	return plan
}

func TestHashMergeAndSort(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{Expr: &influxql.VarRef{
			Val:  "value",
			Type: influxql.Float},
		},
	}
	columnsName := []string{"columnstore"}
	opt := query.ProcessorOptions{}
	opt.GroupByAllDims = true
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)

	plan := buildColumnStorePlan(t, schema)
	planner := getPlanner()
	planner.SetRoot(plan)
	best := planner.FindBestExp()
	if best == nil {
		t.Error("no best plan found")
	}
}

func TestColumnStoreForSql(t *testing.T) {
	fields := influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float}}}
	columnsName := []string{"columnstore"}
	opt := query.ProcessorOptions{}
	opt.GroupByAllDims = true
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)

	plan := buildColumnStorePlanForSql(schema)
	planner := getPlanner()
	planner.SetRoot(plan)
	best := planner.FindBestExp()
	if best == nil {
		t.Error("no best plan found")
	}

	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)
	res, _ := planBuilder.CreateSparseIndexScanPlan(nil)
	assert.Equal(t, res, nil)

	fields = influxql.Fields{
		&influxql.Field{Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "value", Type: influxql.Float}}}},
	}
	columnsName = []string{"value"}
	opt = query.ProcessorOptions{}
	opt.GroupByAllDims = true
	schema = executor.NewQuerySchema(fields, columnsName, &opt, nil)
	_, err := planBuilder.CreateSegmentPlan(schema)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := recover(); err != nil {
			err, _ := err.(string)
			assert.Equal(t, strings.Contains(err, "unsupported engine type"), true)
		}
	}()
	planBuilder.Reader(config.ENGINETYPEEND)
}

func TestBuildInConditionPlan(t *testing.T) {
	sql := "select id from students where \"name\" in (select \"name\" from students where score > 90)"
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	q, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal(err)
	}

	stmt := q.Statements[0]
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal(fmt.Errorf("invalid SelectStatement"))
	}

	schema := createQuerySchema()
	inCondition, ok := selectStmt.Condition.(*influxql.InCondition)
	if !ok {
		t.Fatal(fmt.Errorf("invalid inCondition"))
	}
	schema.InConditons = append(schema.InConditons, inCondition)

	creator := NewMockShardGroup()
	table := NewTable("students")
	table.AddDataTypes(map[string]influxql.DataType{"id": influxql.Integer, "name": influxql.String, "score": influxql.Float, "good": influxql.Boolean})
	creator.AddShard(table)
	builder := executor.NewLogicalPlanBuilderImpl(schema)
	inCondition.Csming = creator
	schema.Options().(*query.ProcessorOptions).Condition = selectStmt.Condition
	_, err = executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema, builder)
	if err != nil {
		t.Fatal(err)
	}

	inCondition.Csming = nil
	_, err = executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema, builder)
	assert.NotEqual(t, err, nil)

	inCondition.Csming = creator
	opt, ok := schema.Options().(*query.ProcessorOptions)
	if !ok {
		t.Fatal("opt err")
	}
	schema.SetOpt(nil)
	_, err = executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema, builder)
	assert.NotEqual(t, err, nil)

	inCondition.TimeRange = influxql.TimeRange{Min: time.Unix(0, 1000).UTC(), Max: time.Unix(0, 2000).UTC()}
	opt.StartTime = 999
	opt.EndTime = influxql.MaxTime
	opt.Interval.Duration = 1
	inCondition.Stmt.SetTimeInterval(1)
	schema.SetOpt(opt)
	_, err = executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema, builder)
	assert.Equal(t, err, nil)

	inCondition.TimeRange.Max = time.Unix(0, influxql.MaxTime).UTC()
	ctx := context.Background()
	ctx = context.WithValue(ctx, executor.NowKey, time.Now())
	createPlanErr = true
	inSubQuery = true
	defer func() {
		createPlanErr = false
		inSubQuery = false
	}()
	_, err = executor.BuildInConditionPlan(ctx, creator, selectStmt, schema, builder)
	assert.NotEqual(t, err, nil)
}

func TestLogicalIncAgg(t *testing.T) {
	schema := createQuerySchemaWithCalls()
	node := executor.NewLogicalSeries(schema)
	agg := executor.NewLogicalIncAgg(node, schema)
	_ = agg.Digest()
	_ = agg.Schema()
	_ = agg.RowDataType()
	_ = agg.Dummy()
	agg.DeriveOperations()
	agg.ForwardCallArgs()
	agg.CountToSum()
	aggClone := agg.Clone()
	if aggClone.Type() != agg.Type() {
		t.Error("wrong result")
	}
}

func TestLogicalIncHashAgg(t *testing.T) {
	schema := createQuerySchemaWithCountCalls()
	node := executor.NewLogicalSeries(schema)
	agg := executor.NewLogicalIncHashAgg(node, schema)
	_ = agg.Digest()
	_ = agg.Digest()
	_ = agg.Schema()
	_ = agg.RowDataType()
	_ = agg.Dummy()
	agg.DeriveOperations()
	agg.ForwardCallArgs()
	agg.CountToSum()
	aggClone := agg.Clone()
	if aggClone.Type() != agg.Type() {
		t.Error("wrong result")
	}
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	agg.Explain(planWriter)
	newAgg := agg.New([]hybridqp.QueryNode{node}, schema, nil)
	if newAgg.Type() != agg.Type() {
		t.Error("wrong result")
	}
}

func TestNewLogicalBinOp(t *testing.T) {
	schema := createQuerySchema()
	node := executor.NewLogicalSeries(schema)
	leftSubquery := executor.NewLogicalSubQuery(node, schema)
	rightSubquery := executor.NewLogicalSubQuery(node, schema)
	binOp := executor.NewLogicalBinOp(leftSubquery, rightSubquery, nil, nil, &influxql.BinOp{}, schema)
	binOpClone := binOp.Clone()
	if binOpClone.Type() != binOp.Type() {
		t.Fatal("wrong type result")
	}
	if len(binOp.Children()) != 2 {
		t.Fatal("wrong children len result")
	}
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	binOp.Explain(planWriter)
	if binOp.Digest() == "" {
		t.Fatal("wrong Digest result")
	}
	binOp.ReplaceChildren([]hybridqp.QueryNode{nil, nil})
	binOp.ReplaceChild(0, nil)
	binOp.ReplaceChild(0, nil)
	binOp.ReplaceChild(1, nil)
	if binOp.Children()[0] != nil {
		t.Fatal("wrong replace child result")
	}
	binOpClone.DeriveOperations()
	if binOp.New(nil, nil, nil) != nil {
		t.Fatal("wrong new result")
	}
}

func TestNewLogicalBinOpOfNilMst(t *testing.T) {
	para := &influxql.BinOp{
		NilMst: influxql.LNilMst,
	}
	schema := createQuerySchema()
	node := executor.NewLogicalSeries(schema)
	rightSubquery := executor.NewLogicalSubQuery(node, schema)
	binOp := executor.NewLogicalBinOp(nil, rightSubquery, nil, nil, para, schema)
	if len(binOp.Children()) != 1 {
		t.Fatal("wrong children len result")
	}

	binOp.ReplaceChild(0, binOp.Children()[0])
	binOp.ReplaceChildren([]hybridqp.QueryNode{nil})
	if binOp.Children()[0] != nil {
		t.Fatal("wrong replace child result")
	}
	para.NilMst = influxql.RNilMst
	binOp = executor.NewLogicalBinOp(rightSubquery, nil, nil, nil, para, schema)
	if len(binOp.Children()) != 1 {
		t.Fatal("wrong children len result")
	}

	binOp.ReplaceChild(0, binOp.Children()[0])
	binOp.ReplaceChildren([]hybridqp.QueryNode{nil})
	if binOp.Children()[0] != nil {
		t.Fatal("wrong replace child result")
	}
}

func TestBuildBinOpPlan(t *testing.T) {
	fields := []*influxql.Field{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	stmt := &influxql.SelectStatement{
		Fields: fields,
		Sources: []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}},
			&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}},
		BinOpSource: []*influxql.BinOp{&influxql.BinOp{}},
	}
	schema := createQuerySchema()

	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	_, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	stmt.Sources[0].(*influxql.SubQuery).Statement.Dimensions = append(stmt.Sources[0].(*influxql.SubQuery).Statement.Dimensions, &influxql.Dimension{Expr: &influxql.Call{Name: "time"}})
	if _, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema); err == nil {
		t.Fatal("TestBuildBinOpPlan error1")
	}
	stmt.Sources = nil
	if _, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema); err != nil {
		t.Fatal("TestBuildBinOpPlan error3")
	}
	stmt.BinOpSource = nil
	if _, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema); err == nil {
		t.Fatal("TestBuildBinOpPlan error2")
	}
}

func TestBuildBinOpPlanOfNilMst(t *testing.T) {
	fields := []*influxql.Field{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	stmt := &influxql.SelectStatement{
		Fields:      fields,
		Sources:     []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}},
		BinOpSource: []*influxql.BinOp{&influxql.BinOp{NilMst: influxql.LNilMst}},
	}
	schema := createQuerySchema()

	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	_, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	stmt.BinOpSource[0].NilMst = influxql.RNilMst
	if _, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema); err != nil {
		t.Fatal("TestBuildBinOpPlanOfNilMst error1")
	}
}

// binop(subquery1, subquery2) -> without groupby+orderby
func TestBuildPromPlanNoGroupByOfBinOp(t *testing.T) {
	fields := []*influxql.Field{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	stmt := &influxql.SelectStatement{
		Fields: fields,
		Sources: []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}},
			&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}},
		BinOpSource: []*influxql.BinOp{&influxql.BinOp{}},
	}
	schema := createQuerySchema()
	schema.Options().SetPromQuery(true)
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	plan, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema)
	if err != nil {
		t.Fatal(err)
	}

	logicSeries1 := executor.NewLogicalSeries(schema)
	logicSeries2 := executor.NewLogicalSeries(schema)

	ex1 := executor.NewLogicalExchange(logicSeries1, executor.SERIES_EXCHANGE, nil, schema)
	ex2 := executor.NewLogicalExchange(logicSeries2, executor.SERIES_EXCHANGE, nil, schema)

	reader1 := executor.NewLogicalReader(ex1, schema)
	reader2 := executor.NewLogicalReader(ex2, schema)

	ex3 := executor.NewLogicalExchange(reader1, executor.READER_EXCHANGE, nil, schema)
	ex4 := executor.NewLogicalExchange(reader2, executor.READER_EXCHANGE, nil, schema)
	ex5 := executor.NewLogicalExchange(ex3, executor.SINGLE_SHARD_EXCHANGE, nil, schema)
	ex6 := executor.NewLogicalExchange(ex4, executor.SINGLE_SHARD_EXCHANGE, nil, schema)

	project1 := executor.NewLogicalProject(ex5, schema)
	project2 := executor.NewLogicalProject(ex6, schema)

	subquery1 := executor.NewLogicalSubQuery(project1, schema)
	subquery2 := executor.NewLogicalSubQuery(project2, schema)

	binop := executor.NewLogicalBinOp(subquery1, subquery2, nil, nil, nil, schema)
	if !ComparePlanNode(plan, binop) {
		t.Fatal("TestBuildPromPlanNoGroupByOfBinOp err")
	}
}

// agg(subquery) -> with groupby+orderby
func TestBuildPromPlanGroupByOfAgg(t *testing.T) {
	fields := []*influxql.Field{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	Sources := []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}}

	schema := createQuerySchema()
	schema.Options().SetPromQuery(true)
	schema.Calls()["sum"] = nil
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	plan, err := executor.BuildSources(context.Background(), creator, Sources, schema, false)
	if err != nil {
		t.Fatal(err)
	}

	logicSeries := executor.NewLogicalSeries(schema)
	ex := executor.NewLogicalExchange(logicSeries, executor.SERIES_EXCHANGE, nil, schema)
	reader := executor.NewLogicalReader(ex, schema)
	ex1 := executor.NewLogicalExchange(reader, executor.READER_EXCHANGE, nil, schema)
	ex2 := executor.NewLogicalExchange(ex1, executor.SINGLE_SHARD_EXCHANGE, nil, schema)
	project := executor.NewLogicalProject(ex2, schema)
	subquery := executor.NewLogicalSubQuery(project, schema)
	groupby := executor.NewLogicalGroupBy(subquery, schema)
	orderby := executor.NewLogicalOrderBy(groupby, schema)

	if !ComparePlanNode(plan, orderby) {
		t.Fatal("TestBuildPromPlanGroupByOfAgg err")
	}
}

// noagg(subquery) -> without groupby+orderby
func TestBuildPromPlanGroupByOfNoAgg(t *testing.T) {
	fields := []*influxql.Field{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	Sources := []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}}

	schema := createQuerySchema()
	schema.Options().SetPromQuery(true)
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	plan, err := executor.BuildSources(context.Background(), creator, Sources, schema, false)
	if err != nil {
		t.Fatal(err)
	}

	logicSeries := executor.NewLogicalSeries(schema)
	ex := executor.NewLogicalExchange(logicSeries, executor.SERIES_EXCHANGE, nil, schema)
	reader := executor.NewLogicalReader(ex, schema)
	ex1 := executor.NewLogicalExchange(reader, executor.READER_EXCHANGE, nil, schema)
	ex2 := executor.NewLogicalExchange(ex1, executor.SINGLE_SHARD_EXCHANGE, nil, schema)
	project := executor.NewLogicalProject(ex2, schema)
	subquery := executor.NewLogicalSubQuery(project, schema)

	if !ComparePlanNode(plan, subquery) {
		t.Fatal("TestBuildPromPlanGroupByOfNoAgg err")
	}
}

func Test_BuildFullJoinQueryPlant(t *testing.T) {
	fields := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "m1.f1", Type: influxql.Float, Alias: ""}, Alias: ""},
		{Expr: &influxql.VarRef{Val: "m2.f1", Type: influxql.Float, Alias: ""}, Alias: ""},
	}
	sub_query_fields_1 := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "f1", Type: influxql.Float, Alias: ""}, Alias: "m1.f1"},
	}

	sub_query_fields_2 := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "f1", Type: influxql.Float, Alias: ""}, Alias: "m2.f1"},
	}

	sub_query_fields_3 := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "f1", Type: influxql.Unknown, Alias: ""}, Alias: ""},
	}

	sources := []influxql.Source{
		&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: sub_query_fields_1, Sources: []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}}}, Alias: "m1"},
		&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: sub_query_fields_2, Sources: []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}}}, Alias: "m2"},
	}
	joincases := []*influxql.Join{
		{
			LSrc:      &influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: sub_query_fields_3, Sources: []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}}}, Alias: "m1"},
			RSrc:      &influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: sub_query_fields_3, Sources: []influxql.Source{&influxql.Measurement{Database: "db0", Name: "mst"}}}, Alias: "m2"},
			Condition: &influxql.BinaryExpr{},
		},
	}
	unsets := []*influxql.Unnest{}
	clonames := []string{"tk1", "f1"}

	stmt := &influxql.SelectStatement{
		Fields:     fields,
		Sources:    sources,
		JoinSource: joincases,
	}
	opt := query.ProcessorOptions{}

	schema := executor.NewQuerySchemaWithJoinCase(fields, sources, clonames, &opt, joincases, nil, unsets, nil)
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"f1": influxql.Float})
	creator.AddShard(table)
	_, err := executor.BuildJoinQueryPlan(context.Background(), creator, stmt, schema)
	if err != nil {
		t.Fatal("TestBuildFullJoinQueryPlan error")
	}
}

func Test_ExplainNode(t *testing.T) {
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	opt := query.ProcessorOptions{}
	fields := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "m1.f1", Type: influxql.Float, Alias: ""}, Alias: ""},
	}
	clonames := []string{"f1"}
	schema := executor.NewQuerySchema(fields, clonames, &opt, nil)
	logicSeries := executor.NewLogicalSeries(schema)
	groupBy := executor.NewLogicalGroupBy(logicSeries, schema)
	orderBy := executor.NewLogicalOrderBy(groupBy, schema)
	orderBy.Explain(planWriter)
}

func TestNewLogicalPromSubquery(t *testing.T) {
	schema := createQuerySchema()
	call := &influxql.PromSubCall{
		Name: "rate_prom",
	}
	node := executor.NewLogicalSeries(schema)
	op := executor.NewLogicalPromSubquery(node, schema, call)
	opClone := op.Clone()
	if opClone.Type() != op.Type() {
		t.Fatal("wrong type result")
	}
	if len(op.Children()) != 1 {
		t.Fatal("wrong children len result")
	}
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	op.Explain(planWriter)
	if op.Digest() == "" {
		t.Fatal("wrong Digest result")
	}
	op.ReplaceChild(0, nil)
	op.ReplaceChildren([]hybridqp.QueryNode{nil})
	if op.Children()[0] != nil {
		t.Fatal("wrong replace child result")
	}
	opClone.DeriveOperations()
	if op.New(nil, nil, nil) != nil {
		t.Fatal("wrong new result")
	}
}

// except return err if it's a sub-query
func TestBuildSubQuery_Except(t *testing.T) {
	fields := []*influxql.Field{{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	Sources := []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
		ExceptDimensions: make(influxql.Dimensions, 1),
		Fields:           fields,
		Sources:          []influxql.Source{&influxql.Measurement{Name: "mst"}}},
	}}
	schema := createQuerySchema()
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	_, err := executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.True(t, strings.Contains(err.Error(), "except: sub-query or join-query is unsupported"))
}

func TestBuildSubQuery_TimeRange(t *testing.T) {
	condition := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: &influxql.BinaryExpr{Op: influxql.GTE, LHS: &influxql.VarRef{Val: "time"}, RHS: &influxql.IntegerLiteral{Val: 1}},
		RHS: &influxql.BinaryExpr{Op: influxql.LTE, LHS: &influxql.VarRef{Val: "time"}, RHS: &influxql.IntegerLiteral{Val: 2}},
	}
	fields := []*influxql.Field{{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	Sources := []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
		Fields:    fields,
		Sources:   []influxql.Source{&influxql.Measurement{Name: "mst"}},
		Condition: condition,
	}}}
	schema := createQuerySchema()
	schema.Options().(*query.ProcessorOptions).StartTime = 0
	schema.Options().(*query.ProcessorOptions).EndTime = 3
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	queryNode, err := executor.BuildSources(context.Background(), creator, Sources, schema, false)
	opt := queryNode.Children()[0].Schema().Options()
	assert.NoError(t, err)
	assert.Equal(t, opt.GetStartTime(), int64(0))
	assert.Equal(t, opt.GetEndTime(), int64(3))
	schema.Options().(*query.ProcessorOptions).PromQuery = true
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	opt = queryNode.Children()[0].Schema().Options()
	assert.NoError(t, err)
	assert.Equal(t, opt.GetStartTime(), int64(1))
	assert.Equal(t, opt.GetEndTime(), int64(2))
}

func TestBuildSubQuery_FilterPushDown(t *testing.T) {
	condition := &influxql.BinaryExpr{
		Op: influxql.LTE,
		LHS: &influxql.VarRef{
			Val:  "value",
			Type: influxql.Float,
		},
		RHS: &influxql.NumberLiteral{
			Val: 1,
		},
	}
	conditionWithCall := &influxql.BinaryExpr{
		Op: influxql.LTE,
		LHS: &influxql.VarRef{
			Val:  "value",
			Type: influxql.Float,
		},
		RHS: &influxql.Call{},
	}
	fields := []*influxql.Field{{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	Sources := []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
		Fields:    fields,
		Sources:   []influxql.Source{&influxql.Measurement{Name: "mst"}},
		Condition: condition,
	}}}
	schema := createQuerySchema()
	schema.Options().SetCondition(condition)
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)

	//filter push down
	schema.Options().(*query.ProcessorOptions).PromQuery = true
	queryNode, err := executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon := queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.NotNil(t, valueCon)
	assert.Nil(t, schema.Options().GetCondition())

	//not PromQuery
	schema.Options().(*query.ProcessorOptions).PromQuery = false
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon = queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.Nil(t, valueCon)

	//Condition has call
	schema.Options().(*query.ProcessorOptions).PromQuery = true
	schema.Options().SetCondition(conditionWithCall)
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon = queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.Nil(t, valueCon)

	//Condition is nil
	schema.Options().SetCondition(nil)
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon = queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.Nil(t, valueCon)

	//field type is not VarRef
	fields = []*influxql.Field{{Expr: condition}}
	Sources = []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
		Fields:    fields,
		Sources:   []influxql.Source{&influxql.Measurement{Name: "mst"}},
		Condition: condition,
	}}}
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon = queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.Nil(t, valueCon)

	//fields len is not one
	fields = []*influxql.Field{{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"},
		{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	Sources = []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
		Fields:    fields,
		Sources:   []influxql.Source{&influxql.Measurement{Name: "mst"}},
		Condition: condition,
	}}}
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon = queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.Nil(t, valueCon)

	//source is not Measurement
	fields = []*influxql.Field{{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	Sources = []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
		Fields: fields,
		Sources: []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
			Fields:    fields,
			Sources:   []influxql.Source{&influxql.Measurement{Name: "mst"}},
			Condition: condition,
		}}},
		Condition: condition,
	}}}
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon = queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.Nil(t, valueCon)

	//source measurement len is not one
	Sources = []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{
		Fields:    fields,
		Sources:   []influxql.Source{&influxql.Measurement{Name: "mst"}, &influxql.Measurement{Name: "mst"}},
		Condition: condition,
	}}}
	queryNode, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.NotNil(t, queryNode)
	assert.Nil(t, err)
	valueCon = queryNode.Children()[0].Schema().Options().GetValueCondition()
	assert.Nil(t, valueCon)
}

func TestBuildBinOpPlanOfOneSideExpr(t *testing.T) {
	fields := []*influxql.Field{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}
	stmt := &influxql.SelectStatement{
		Fields:      fields,
		Sources:     []influxql.Source{&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}},
		BinOpSource: []*influxql.BinOp{&influxql.BinOp{RExpr: &influxql.Call{Name: "vector_prom", Args: []influxql.Expr{&influxql.NumberLiteral{Val: 1}}}}},
	}
	schema := createQuerySchema()

	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	_, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	stmt.BinOpSource[0].LExpr = stmt.BinOpSource[0].RExpr
	stmt.BinOpSource[0].RExpr = nil
	if _, err := executor.BuildBinOpQueryPlan(context.Background(), creator, stmt, schema); err != nil {
		t.Fatal("TestBuildBinOpPlanOfNilMst error1")
	}
}

func TestNewLogicalAggForPromNestOp(t *testing.T) {
	opt := &query.ProcessorOptions{PromQuery: true, Range: time.Second}
	fields := influxql.Fields{
		&influxql.Field{Expr: &influxql.Call{
			Name: "count_prom",
			Args: []influxql.Expr{
				&influxql.Call{
					Name: "rate",
					Args: []influxql.Expr{
						&influxql.VarRef{
							Val:  "value",
							Type: influxql.Float,
						},
					},
				},
			},
		},
			Alias: "value",
		},
	}
	schema := executor.NewQuerySchema(fields, []string{"value"}, opt, nil)
	series := executor.NewLogicalSeries(schema)
	seriesAgg := executor.NewLogicalAggregate(series, schema)
	tagSetAgg := executor.NewLogicalAggregate(seriesAgg, schema)
	merge := executor.NewLogicalExchange(tagSetAgg, executor.SHARD_EXCHANGE, nil, schema)
	shardAgg := executor.NewLogicalAggregate(merge, schema)
	assert.Equal(t, len(shardAgg.Schema().Calls()), 1)
}

func TestAlign(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{Expr: &influxql.VarRef{
			Val:  "value",
			Type: influxql.Float},
		},
	}
	columnsName := []string{"columnstore"}
	opt := query.ProcessorOptions{}
	opt.GroupByAllDims = true
	schema := executor.NewQuerySchema(fields, columnsName, &opt, nil)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)
	planBuilder.Series()
	planBuilder.Align()
	node, err := planBuilder.Build()
	if err != nil {
		t.Fatal("wrong Align()")
	}
	_, ok := node.(*executor.LogicalAlign)
	assert.Equal(t, true, ok)
}

func buildJoinPlan() (hybridqp.QueryNode, error) {
	fields := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "m1.f1", Type: influxql.Float, Alias: ""}, Alias: ""},
		{Expr: &influxql.VarRef{Val: "m2.f1", Type: influxql.Float, Alias: ""}, Alias: ""},
	}
	field11 := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "f1", Type: influxql.Float, Alias: ""}, Alias: "m1.f1"},
	}

	field21 := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "f1", Type: influxql.Float, Alias: ""}, Alias: "m2.f1"},
	}

	field1 := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "f1", Type: influxql.Unknown, Alias: ""}, Alias: ""},
	}

	mst1 := &influxql.Measurement{Database: "db0", Name: "mst1"}
	mst2 := &influxql.Measurement{Database: "db0", Name: "mst2"}

	sources := []influxql.Source{
		&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: field11, Sources: []influxql.Source{mst1}}, Alias: "m1"},
		&influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: field21, Sources: []influxql.Source{mst2}}, Alias: "m2"},
	}
	joinCase := []*influxql.Join{
		{
			LSrc:      &influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: field1, Sources: []influxql.Source{mst1}}, Alias: "m1"},
			RSrc:      &influxql.SubQuery{Statement: &influxql.SelectStatement{Fields: field1, Sources: []influxql.Source{mst2}}, Alias: "m2"},
			Condition: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "m1.tk", Type: influxql.Tag}, RHS: &influxql.VarRef{Val: "m2.tk", Type: influxql.Tag}},
			JoinType:  influxql.InnerJoin,
		},
	}
	unsets := []*influxql.Unnest{}
	opt := query.ProcessorOptions{HintType: hybridqp.QueryPushDown}
	colNames := []string{"tk1", "f1"}

	stmt := &influxql.SelectStatement{
		Fields:     fields,
		Sources:    sources,
		JoinSource: joinCase,
	}

	schema := executor.NewQuerySchemaWithJoinCase(fields, sources, colNames, &opt, joinCase, nil, unsets, nil)
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"f1": influxql.Float})
	creator.AddShard(table)
	joinNode, err := executor.BuildJoinQueryPlan(context.Background(), creator, stmt, schema)
	return joinNode, err
}

func TestBuildJoinQueryPlan(t *testing.T) {
	joinNode, err := buildJoinPlan()
	if err != nil {
		t.Fatalf("TestBuildJoinQueryPlan error: %v", err)
	}
	joinNode.DeriveOperations()
	joinNode.ReplaceChildren(joinNode.Children())
	joinNode.ReplaceChild(0, joinNode.Children()[0])
	joinNode.ReplaceChild(1, joinNode.Children()[1])
	joinNode.Clone()
	joinNode.Digest()
	joinNode.Digest()
	assert.Equal(t, joinNode.New(nil, nil, nil), nil)
	assert.Equal(t, len(joinNode.Children()), 2)
	assert.Equal(t, joinNode.Type(), "*executor.LogicalJoin")
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	joinNode.(executor.LogicalPlan).Explain(planWriter)
	assert.Contains(t, planWriter.String(), "LogicalJoin")
	func() {
		defer func() {
			if err := recover(); err != nil {
				if !strings.Contains(fmt.Sprintf("%s", err), "children count in LogicalJoin is not 2") {
					t.Fatal("unexpect panic", "ReplaceChildren")
				}
			}
		}()
		joinNode.ReplaceChildren(nil)
	}()
	func() {
		defer func() {
			if err := recover(); err != nil {
				if !strings.Contains(fmt.Sprintf("%s", err), "index 2 out of range 1") {
					t.Fatal("unexpect panic", "ReplaceChild")
				}
			}
		}()
		joinNode.ReplaceChild(2, nil)
	}()
}

func TestNewLogicalIn(t *testing.T) {
	schema := createQuerySchema()
	node := executor.NewLogicalSeries(schema)
	op := executor.NewLogicalIn(node, nil, schema, nil, nil, nil)
	assert.Equal(t, op.New(nil, nil, nil), nil)
	op.DeriveOperations()
	opClone := op.Clone()
	if opClone.Type() != op.Type() {
		t.Fatal("wrong type result")
	}
	if len(op.Children()) != 1 {
		t.Fatal("wrong children len result")
	}
	op.ReplaceChildren([]hybridqp.QueryNode{node})
	op.ReplaceChild(0, node)

	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	op.Explain(planWriter)
	if op.Digest() == "" {
		t.Fatal("wrong Digest result")
	}
	op.Digest()
}

func TestBuildCTESource(t *testing.T) {
	schema := createQuerySchema()
	schema.Calls()["sum"] = nil
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	expr := &influxql.StringLiteral{Val: "http"}
	schema.Options().SetCondition(expr)
	fields := []*influxql.Field{&influxql.Field{Expr: &influxql.VarRef{Val: "value", Type: influxql.Float, Alias: "value"}, Alias: "value"}}

	cte := &influxql.CTE{Query: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}
	cte.Csming = creator
	Sources := []influxql.Source{cte}

	plan, err := executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.Equal(t, err, nil)
	ctePlan := plan.Children()[0].Children()[0].(*executor.LogicalCTE)
	ctePlan.New(nil, schema, nil)
	ctePlan.GetCTEPlan()
	ctePlan.DeriveOperations()
	clone := ctePlan.Clone()
	clone.ReplaceChildren([]hybridqp.QueryNode{ctePlan.GetCTEPlan()})
	clone.ReplaceChild(0, ctePlan.GetCTEPlan())

	assertPanic(t, clone)
	clone.Type()
	assert.True(t, clone.Digest() == clone.Digest())

	cte = &influxql.CTE{GraphQuery: &influxql.GraphStatement{}}
	_, e := executor.BuildCTELogicalPlan(context.Background(), schema, cte)
	assert.Equal(t, e, nil)

	schema.SetOpt(nil)
	plan, err = executor.BuildSources(context.Background(), creator, Sources, schema, false)

	cte = &influxql.CTE{Query: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}}}
	cte.Csming = "abc"
	Sources = []influxql.Source{cte}
	_, e = executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.Equal(t, e.Error(), "cte csming err")

}

func TestBuildTableFunctionSource(t *testing.T) {
	schema := createQuerySchema()
	schema.Calls()["sum"] = nil
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"value": influxql.Float})
	creator.AddShard(table)
	expr := &influxql.StringLiteral{Val: "http"}
	schema.Options().SetCondition(expr)

	fields := []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "m1.f1", Type: influxql.Float, Alias: ""}, Alias: ""},
		{Expr: &influxql.VarRef{Val: "m2.f1", Type: influxql.Float, Alias: ""}, Alias: ""},
	}

	mst1 := &influxql.Measurement{Database: "db0", Name: "mst1"}
	mst2 := &influxql.Measurement{Database: "db0", Name: "mst2"}
	mst3 := &influxql.CTE{Query: &influxql.SelectStatement{Fields: fields, Sources: []influxql.Source{&influxql.Measurement{Name: "cte1"}}}}
	mst3.Csming = creator
	mst4 := &influxql.CTE{GraphQuery: &influxql.GraphStatement{}}

	sources := []influxql.Source{
		mst1, mst2, mst3, mst4,
	}
	tableFunction := &influxql.TableFunction{
		FunctionName:        "test",
		TableFunctionSource: sources,
		Params:              "{}",
	}
	Sources := []influxql.Source{tableFunction}

	_, err := executor.BuildSources(context.Background(), creator, Sources, schema, false)
	assert.Equal(t, err, nil)

}

func assertPanic(t *testing.T, clone hybridqp.QueryNode) {
	defer func() {
		if r := recover(); r != nil {
			assert.Equal(t, "index 1 out of range 1", r, "Panic message should match")
		}
	}()
	clone.ReplaceChild(1, nil)
}

func TestBuildInTagConditionPlan(t *testing.T) {
	convey.Convey("normal condition", t, func() {
		sql := "select * from mst where \"host\" in (select \"host02\" from mst1 where time > now() - 5m)"
		creator, selectStmt, schema, builder := PrepareForInnerPlan(sql, t)
		_, err := executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema, builder)
		if err != nil {
			t.Fatal(err)
		}
	})

	convey.Convey("without time filter", t, func() {
		sql := "select * from mst where \"host\" in (select \"host02\" from mst1)"
		creator, selectStmt, schema, builder := PrepareForInnerPlan(sql, t)
		_, err := executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema, builder)
		if err != nil {
			t.Fatal(err)
		}
	})

	convey.Convey("err condition", t, func() {
		sql := "select * from mst where \"host\" in (select \"host02\" from mst1)"
		creator, selectStmt, schema, builder := PrepareForInnerPlan(sql, t)
		selectStmt.Condition.(*influxql.InCondition).Stmt.Fields[0].Expr = nil
		_, err := executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema, builder)
		assert.Equal(t, err.Error(), "type assertion failed")
	})
}

func PrepareForInnerPlan(sql string, t *testing.T) (*MockShardGroup, *influxql.SelectStatement, *executor.QuerySchema, *executor.LogicalPlanBuilderImpl) {
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	q, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal(err)
	}

	stmt := q.Statements[0]
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal(fmt.Errorf("invalid SelectStatement"))
	}

	opt := query.ProcessorOptions{}
	fields := influxql.Fields{
		&influxql.Field{Expr: &influxql.VarRef{
			Val:  "",
			Type: influxql.Float},
		},
	}
	condFields := &influxql.VarRef{
		Val:  "host",
		Type: influxql.Tag,
	}
	inCondFields := &influxql.VarRef{
		Val:  "host02",
		Type: influxql.Tag,
	}
	selectStmt.Condition.(*influxql.InCondition).Column = condFields
	selectStmt.Condition.(*influxql.InCondition).Stmt.Fields[0].Expr = inCondFields
	colNames := []string{"host", "region", "value", "f2"}
	schema := executor.NewQuerySchema(fields, colNames, &opt, nil)
	m := &influxql.Measurement{Name: "mst"}
	schema.AddTable(m, schema.MakeRefs())

	inCondition, ok := selectStmt.Condition.(*influxql.InCondition)
	if !ok {
		t.Fatal(fmt.Errorf("invalid inCondition"))
	}
	schema.InConditons = append(schema.InConditons, inCondition)

	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"host": influxql.Tag, "region": influxql.Tag, "value": influxql.Float, "f2": influxql.Float})
	creator.AddShard(table)
	builder := executor.NewLogicalPlanBuilderImpl(schema)
	inCondition.Csming = creator
	if inCondition.Stmt.Condition != nil {
		inCondition.TimeCond = inCondition.Stmt.Condition.(*influxql.BinaryExpr)
	}
	schema.Options().(*query.ProcessorOptions).Condition = selectStmt.Condition
	return creator, selectStmt, schema, builder
}

func TestNewLogicalGraph(t *testing.T) {
	schema := createQuerySchema()
	graphStmt := &influxql.GraphStatement{}
	op := executor.NewLogicalGraph(nil, graphStmt, schema, nil)
	assert.Equal(t, op.New(nil, nil, nil), nil)
	op.DeriveOperations()
	opClone := op.Clone()
	if opClone.Type() != op.Type() {
		t.Fatal("wrong type result")
	}
	if len(op.Children()) != 0 {
		t.Fatal("wrong children len result")
	}
	op.ReplaceChildren(nil)
	op.ReplaceChild(0, nil)

	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	op.Explain(planWriter)
	if op.Digest() == "" {
		t.Fatal("wrong Digest result")
	}
	op.Digest()
	node := executor.NewLogicalSeries(schema)
	executor.NewLogicalGraph(node, graphStmt, schema, nil)
}

func TestQueryPushDown(t *testing.T) {
	joinNode, err := buildJoinPlan()
	if err != nil {
		t.Fatal(err)
	}
	buf, err := executor.MarshalBinary(joinNode)
	if err != nil {
		t.Error("wrong result")
	}
	_, err = executor.UnmarshalBinary(buf, joinNode.Schema())
	if err != nil {
		t.Fatal(err)
	}
}

func TestInferAggAndPromAggLevel(t *testing.T) {
	opt := &query.ProcessorOptions{}
	fields := influxql.Fields{
		&influxql.Field{Expr: &influxql.Call{
			Name: "percentile_ogsketch",
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  "value",
					Type: influxql.Float,
				},
				&influxql.NumberLiteral{
					Val: 90,
				},
			},
		},
		},
	}
	// source level
	schema := executor.NewQuerySchema(fields, []string{"value"}, opt, nil)
	series := executor.NewLogicalSeries(schema)
	agg := executor.NewLogicalAggregate(series, schema)
	assert.Equal(t, agg.InferAggLevel(), executor.SourceLevel)
	assert.Equal(t, agg.InferPromAggLevel(), executor.SourceLevel)

	// middle level
	shardExchange := executor.NewLogicalExchange(agg, executor.SHARD_EXCHANGE, nil, schema)
	shardAgg := executor.NewLogicalAggregate(shardExchange, schema)
	assert.Equal(t, shardAgg.InferAggLevel(), executor.MiddleLevel)
	assert.Equal(t, shardAgg.InferPromAggLevel(), executor.SinkLevel)

	// sink level
	nodeExchange := executor.NewLogicalExchange(agg, executor.NODE_EXCHANGE, nil, schema)
	nodeAgg := executor.NewLogicalAggregate(nodeExchange, schema)
	assert.Equal(t, nodeAgg.InferAggLevel(), executor.SinkLevel)
	assert.Equal(t, nodeAgg.InferPromAggLevel(), executor.SinkLevel)

	// sink level
	unionNode := executor.NewLogicalSortAppend([]hybridqp.QueryNode{nodeAgg}, schema)
	unionAgg := executor.NewLogicalAggregate(unionNode, schema)
	assert.Equal(t, unionAgg.InferAggLevel(), executor.SinkLevel)
	assert.Equal(t, unionAgg.InferPromAggLevel(), executor.SinkLevel)
}

func TestLogicalDistinct(t *testing.T) {
	opt := &query.ProcessorOptions{}
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
		},
	}
	schema := executor.NewQuerySchema(fields, []string{"value"}, opt, nil)
	series := executor.NewLogicalSeries(schema)
	reader := executor.NewLogicalReader(series, schema)
	distinct := executor.NewLogicalDistinct(reader, schema)
	assert.Equal(t, nil, distinct.New(nil, nil, nil))
	distinct.DeriveOperations()
	distinct.Clone()
	distinct.Digest()
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	distinct.Explain(planWriter)
	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)
	node, _ := planBuilder.CreateDistinct(nil)
	assert.Equal(t, node, nil)
	assert.Equal(t, len(distinct.Children()), 1)
	assert.Equal(t, distinct.Type(), "*executor.LogicalDistinct")
}
