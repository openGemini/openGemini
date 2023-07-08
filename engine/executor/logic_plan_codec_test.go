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
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestExprOptionsCodec(t *testing.T) {
	ops := hybridqp.ExprOptions{Expr: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "id", Type: influxql.Tag},
		RHS: &influxql.StringLiteral{Val: "P001"},
	}}

	pb := ops.Marshal()

	other := hybridqp.ExprOptions{}
	err := other.Unmarshal(pb)
	if err != nil {
		t.Fatalf("failed to unmarshal %v", err)
	}

	if other.Expr.String() != ops.Expr.String() {
		t.Fatalf("failed to codec ExprOptions.Expr, exp: %s, got: %s", ops.Expr, other.Expr)
	}

	if other.Ref.String() != ops.Ref.String() {
		t.Fatalf("failed to codec ExprOptions.Ref, exp: %s, got: %s", ops.Ref, other.Ref)
	}
}

type LogicPlanVisit struct {
	space string
}

func (v *LogicPlanVisit) Visit(plan hybridqp.QueryNode, span *tracing.Span) executor.LogicalPlanVisitor {
	if plan.Children() == nil {
		return nil
	}

	fmt.Println(v.space, plan.String())

	for _, child := range plan.Children() {
		v.space += " "
		v.Visit(child, span)
		v.space = v.space[:len(v.space)-1]
	}

	return nil
}

func BenchmarkMarshalBinary(t *testing.B) {
	for i := 0; i < t.N; i++ {
		dag := buildDag()
		node, err := executor.MarshalQueryNode(dag)
		if !assert.NoError(t, err) {
			return
		}

		other, err := executor.UnmarshalQueryNode(node)
		if !assert.NoError(t, err) {
			return
		}

		assert.NoError(t, ComparePlan(dag, other))
	}
}

func buildDag() hybridqp.QueryNode {
	schema := createQuerySchema()

	logicSeries1 := executor.NewLogicalSeries(schema)
	logicSeries2 := executor.NewLogicalSeries(schema)

	indexScan1 := executor.NewLogicalIndexScan(logicSeries1, schema)
	indexScan2 := executor.NewLogicalIndexScan(logicSeries2, schema)

	reader1 := executor.NewLogicalReader(indexScan1, schema)
	reader2 := executor.NewLogicalReader(indexScan2, schema)

	tagSubset1 := executor.NewLogicalTagSubset(reader1, schema)
	tagSubset2 := executor.NewLogicalTagSubset(reader2, schema)

	agg1 := executor.NewLogicalAggregate(tagSubset1, schema)
	agg2 := executor.NewLogicalAggregate(tagSubset2, schema)

	merge := executor.NewLogicalMerge([]hybridqp.QueryNode{agg1, agg2}, schema)

	sortMerge := executor.NewLogicalSortMerge([]hybridqp.QueryNode{merge}, schema)

	limit := executor.NewLogicalLimit(sortMerge, schema, executor.LimitTransformParameters{})

	dedupe := executor.NewLogicalDedupe(limit, schema)

	interval := executor.NewLogicalInterval(dedupe, schema)

	fill := executor.NewLogicalFill(interval, schema)

	align := executor.NewLogicalAlign(fill, schema)

	project := executor.NewLogicalProject(align, schema)

	filter := executor.NewLogicalFilter(project, schema)

	exg := executor.NewLogicalExchange(filter, executor.NODE_EXCHANGE, []hybridqp.Trait{1, 2, 3}, schema)

	return exg
}

func TestLogicalPlanCodec(t *testing.T) {
	schema := createQuerySchema()
	schema.SetFill(influxql.NoFill)

	logicSeries1 := executor.NewLogicalSeries(schema)
	logicSeries2 := executor.NewLogicalSeries(schema)

	indexScan1 := executor.NewLogicalIndexScan(logicSeries1, schema)
	indexScan2 := executor.NewLogicalIndexScan(logicSeries2, schema)

	reader1 := executor.NewLogicalReader(indexScan1, schema)
	reader2 := executor.NewLogicalReader(indexScan2, schema)

	tagSubset1 := executor.NewLogicalTagSubset(reader1, schema)
	tagSubset2 := executor.NewLogicalTagSubset(reader2, schema)

	agg1 := executor.NewLogicalAggregate(tagSubset1, schema)
	agg2 := executor.NewLogicalAggregate(tagSubset2, schema)

	merge := executor.NewLogicalMerge([]hybridqp.QueryNode{agg1, agg2}, schema)

	sortMerge := executor.NewLogicalSortMerge([]hybridqp.QueryNode{merge}, schema)

	limit := executor.NewLogicalLimit(sortMerge, schema, executor.LimitTransformParameters{})

	dedupe := executor.NewLogicalDedupe(limit, schema)

	interval := executor.NewLogicalInterval(dedupe, schema)

	fill := executor.NewLogicalFill(interval, schema)

	align := executor.NewLogicalAlign(fill, schema)

	project := executor.NewLogicalProject(align, schema)

	filter := executor.NewLogicalFilter(project, schema)

	exg := executor.NewLogicalExchange(filter, executor.NODE_EXCHANGE, []hybridqp.Trait{1, 2, 3}, schema)

	//fmt.Println("----------Visit Begin------------")
	//(&LogicPlanVisit{}).Visit(exg, nil)

	buf, err := executor.MarshalQueryNode(exg)
	if err != nil {
		t.Fatalf("failed to marshal logical plan: %v", err)
	}

	newPlan, err := executor.UnmarshalQueryNode(buf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	//fmt.Println("----------Visit Begin------------")
	//(&LogicPlanVisit{}).Visit(newPlan, nil)

	if err := ComparePlan(exg, newPlan); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestQueryNodeCodec(t *testing.T) {
	schema := createQuerySchema()
	schema.SetFill(influxql.NoFill)

	logicSeries1 := executor.NewLogicalSeries(schema)
	logicSeries2 := executor.NewLogicalSeries(schema)

	indexScan1 := executor.NewLogicalIndexScan(logicSeries1, schema)
	indexScan2 := executor.NewLogicalIndexScan(logicSeries2, schema)

	reader1 := executor.NewLogicalReader(indexScan1, schema)
	reader2 := executor.NewLogicalReader(indexScan2, schema)

	merge := executor.NewLogicalMerge([]hybridqp.QueryNode{reader1, reader2}, schema)

	project := executor.NewLogicalProject(merge, schema)

	filter := executor.NewLogicalFilter(project, schema)

	exg := executor.NewLogicalExchange(filter, executor.NODE_EXCHANGE, []hybridqp.Trait{1, 2, 3}, schema)
	node, err := executor.MarshalQueryNode(exg)
	if !assert.NoError(t, err) {
		return
	}

	other, err := executor.UnmarshalQueryNode(node)
	if !assert.NoError(t, err) {
		return
	}

	assert.NoError(t, ComparePlan(exg, other))

	inputs := []hybridqp.QueryNode{nil, logicSeries1}
	for _, input := range inputs {
		colStoreReader := executor.NewLogicalColumnStoreReader(input, schema)
		exg = executor.NewLogicalExchange(colStoreReader, executor.NODE_EXCHANGE, []hybridqp.Trait{1, 2, 3}, schema)
		node, err = executor.MarshalQueryNode(exg)
		if !assert.NoError(t, err) {
			return
		}
		other, err = executor.UnmarshalQueryNode(node)
		if !assert.NoError(t, err) {
			return
		}
		assert.NoError(t, ComparePlan(exg, other))
	}
}

func TestUnmarshalQueryNode(t *testing.T) {
	buf := []byte{0}
	_, err := executor.UnmarshalQueryNode(buf)
	assert.EqualError(t, err, errno.NewError(errno.ShortBufferSize, util.Uint64SizeBytes, len(buf)).Error())

	buf = make([]byte, 10)
	size := uint64(100)
	binary.BigEndian.PutUint64(buf[:util.Uint64SizeBytes], size)
	_, err = executor.UnmarshalQueryNode(buf)
	assert.EqualError(t, err, errno.NewError(errno.ShortBufferSize, size, len(buf)-util.Uint64SizeBytes).Error())
}

func CompareRowDataType(a, b hybridqp.RowDataType) error {
	var fn = func() error {
		af := a.Fields()
		sort.Sort(af)
		bf := b.Fields()
		sort.Sort(bf)
		if err := CompareFields(af, bf, "RowDataType"); err != nil {
			return err
		}

		if !reflect.DeepEqual(a.Aux(), b.Aux()) {
			return fmt.Errorf("failed to marshal RowDataType.aux: exp: %+v, got: %+v", a.Aux(), b.Aux())
		}

		return nil
	}

	return CompareNil(a, b, "RowDataType", fn)
}

func ComparePlan(a, b hybridqp.QueryNode) error {
	if a.String() != b.String() {
		return fmt.Errorf("failed to marshal LogicalPlan. exp: %s , got: %s", a.String(), b.String())
	}

	/*
		err := compareSchema(a.Schema(), b.Schema())
		if err != nil {
			return err
		}
	*/

	if err := CompareRowDataType(a.RowDataType(), b.RowDataType()); err != nil {
		return err
	}

	if exa, ok := a.(*executor.LogicalExchange); ok {
		exb, _ := b.(*executor.LogicalExchange)

		if exa.ExchangeType() != exb.ExchangeType() {
			return fmt.Errorf("failed to marshal LogicalPlan.eType. exp: %d, got: %d", exa.ExchangeType(), exb.ExchangeType())
		}

		if exa.ExchangeRole() != exb.ExchangeRole() {
			return fmt.Errorf("failed to marshal LogicalPlan.eRole. exp: %d, got: %d", exa.ExchangeRole(), exb.ExchangeRole())
		}
	}

	ac := a.Children()
	bc := b.Children()

	if len(ac) != len(bc) {
		return fmt.Errorf("failed to marshal LogicalPlan.children: exp len: %d , got len: %d", len(ac), len(bc))
	}

	for i, p := range ac {
		err := ComparePlan(p, bc[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func CompareNil(a, b interface{}, object string, fn func() error) error {
	if a == nil || b == nil {
		return nil
	}
	aIsNil := reflect.ValueOf(a).IsNil()
	bIsNil := reflect.ValueOf(b).IsNil()

	if aIsNil && !bIsNil {
		return fmt.Errorf("failed to marshal %s. exp: nil, got: %+v ", object, b)
	}

	if !aIsNil && bIsNil {
		return fmt.Errorf("failed to marshal %s. exp: %+v, got: nil ", a, object)
	}

	if !aIsNil && !bIsNil {
		return fn()
	}

	return nil
}

func CompareFields(fa, fb influxql.Fields, object string) error {
	fn := func() error {
		if fa.String() != fb.String() {
			return fmt.Errorf("failed to marshal %s.fields. exp: %s, got: %s ", object, fa, fb)
		}
		return nil
	}

	return CompareNil(fa, fb, fmt.Sprintf("%s.fields", object), fn)
}
