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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

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
}

func TestLogicalPlanNilNew(t *testing.T) {
	logicalNode := []hybridqp.QueryNode{&executor.LogicalSlidingWindow{}, &executor.LogicalFilter{}, &executor.LogicalSortAppend{},
		&executor.LogicalDedupe{}, &executor.LogicalFilterBlank{}, &executor.LogicalAlign{}, &executor.LogicalMst{}, &executor.LogicalSubQuery{},
		&executor.LogicalTagSubset{}, &executor.LogicalGroupBy{}, &executor.LogicalOrderBy{}, &executor.LogicalHttpSenderHint{},
		&executor.LogicalTarget{}, &executor.LogicalDummyShard{}, &executor.LogicalTSSPScan{}, &executor.LogicalWriteIntoStorage{},
		&executor.LogicalSequenceAggregate{}, &executor.LogicalSplitGroup{}, &executor.LogicalFullJoin{}, &executor.LogicalHoltWinters{},
		&executor.LogicalSort{}, &executor.LogicalHashMerge{}, &executor.LogicalMerge{}, &executor.LogicalSortMerge{}}
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

func TestNewLogicalJoin(t *testing.T) {
	schema := createQuerySchema()
	node := executor.NewLogicalSeries(schema)
	leftSubQuery := executor.NewLogicalSubQuery(node, schema)
	rightSubQuery := executor.NewLogicalSubQuery(node, schema)
	join := executor.NewLogicalJoin([]hybridqp.QueryNode{leftSubQuery, rightSubQuery}, schema)
	newJoin := join.Clone()
	if newJoin.Type() != join.Type() {
		t.Error("wrong result")
	}
	str := join.Digest()
	assert.Equal(t, join.Digest(), str)
	tStr := join.String()
	tType := join.Type()
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	join.Explain(planWriter)
	if join == nil {
		t.Error("wrong result")
	} else {
		fmt.Println(tStr, str, tType)
	}
	join.DeriveOperations()
	assert.Equal(t, join.New(nil, nil, nil), nil)
	defer func() {
		if err := recover(); err != nil {
			assert.Equal(t, err.(string), "validate all input of join failed")
		}
	}()
	_ = executor.NewLogicalJoin(nil, schema)
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
	selectStmt1, selectStmt2 := selectStmt.Clone(), selectStmt.Clone()
	schema := createQuerySchema()

	creator := NewMockShardGroup()
	table := NewTable("students")
	table.AddDataTypes(map[string]influxql.DataType{"id": influxql.Integer, "name": influxql.String, "score": influxql.Float, "good": influxql.Boolean})
	creator.AddShard(table)

	schema.Options().(*query.ProcessorOptions).Condition = selectStmt.Condition
	_, _, err = executor.BuildInConditionPlan(context.Background(), creator, selectStmt, schema)
	if err != nil {
		t.Fatal(err)
	}

	createPlanErr = true
	schema.Options().(*query.ProcessorOptions).Condition = selectStmt1.Condition
	_, _, err = executor.BuildInConditionPlan(context.Background(), creator, selectStmt1, schema)
	assert.Equal(t, strings.Contains(err.Error(), "CreateLogicalPlan failed"), true)

	inSubQuery = true
	schema.Options().(*query.ProcessorOptions).Condition = selectStmt2.Condition
	_, _, err = executor.BuildInConditionPlan(context.Background(), creator, selectStmt2, schema)
	assert.Equal(t, strings.Contains(err.Error(), "CreateLogicalPlan failed"), true)
}
