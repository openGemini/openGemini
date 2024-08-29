// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
)

type MockAToBRule struct {
	executor.OptRuleBase
}

func NewMockAToBRule(operand executor.OptRuleOperand, description string) *MockAToBRule {
	mr := &MockAToBRule{}
	if description == "" {
		description = executor.GetTypeName(mr)
	}
	mr.Initialize(mr, operand, description)
	return mr
}

func (r *MockAToBRule) Catagory() executor.OptRuleCatagory {
	return executor.RULE_TEST
}

func (r *MockAToBRule) ToString() string {
	return executor.GetTypeName(r)
}

func (r *MockAToBRule) Equals(rhs executor.OptRule) bool {
	rr, ok := rhs.(*MockAToBRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *MockAToBRule) OnMatch(call *executor.OptRuleCall) {
	a := call.Node(0).(*MockLogicalPlanA)
	b := NewMockLogicalPlanB(a.Children(), a.Schema().(*executor.QuerySchema))
	call.TransformTo(b)
}

type MockMergeBRule struct {
	executor.OptRuleBase
}

func NewMockMergeBRule(operand executor.OptRuleOperand, description string) *MockMergeBRule {
	mr := &MockMergeBRule{}
	if description == "" {
		description = executor.GetTypeName(mr)
	}
	mr.Initialize(mr, operand, description)
	return mr
}

func (r *MockMergeBRule) Catagory() executor.OptRuleCatagory {
	return executor.RULE_TEST
}

func (r *MockMergeBRule) ToString() string {
	return executor.GetTypeName(r)
}

func (r *MockMergeBRule) Equals(rhs executor.OptRule) bool {
	rr, ok := rhs.(*MockMergeBRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *MockMergeBRule) OnMatch(call *executor.OptRuleCall) {
	b1 := call.Node(0).(*MockLogicalPlanB)
	b2 := call.Node(1).(*MockLogicalPlanB)
	b := NewMockLogicalPlanB(b2.Children(), b1.Schema().(*executor.QuerySchema))
	call.TransformTo(b)
}

type MockLogicalPlanA struct {
	inputs []hybridqp.QueryNode
	executor.LogicalPlanBase
}

func NewMockLogicalPlanA(inputs []hybridqp.QueryNode, schema *executor.QuerySchema) *MockLogicalPlanA {
	plan := &MockLogicalPlanA{
		inputs:          inputs,
		LogicalPlanBase: *executor.NewLogicalPlanBase(schema, nil, nil),
	}

	return plan
}

func (p *MockLogicalPlanA) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *MockLogicalPlanA) DeriveOperations() {
}

func (p *MockLogicalPlanA) Clone() hybridqp.QueryNode {
	clone := NewMockLogicalPlanA(p.inputs, p.Schema().(*executor.QuerySchema))
	*clone = *p
	return clone
}

func (p *MockLogicalPlanA) Children() []hybridqp.QueryNode {
	nodes := make([]hybridqp.QueryNode, 0, len(p.inputs))

	for _, input := range p.inputs {
		nodes = append(nodes, input)
	}
	return nodes
}

func (p *MockLogicalPlanA) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(p.inputs) != len(children) {
		panic(fmt.Sprintf("%d children in logical merge, but replace with %d children", len(p.inputs), len(children)))
	}

	for i := range p.inputs {
		p.inputs[i] = children[i]
	}
}

func (p *MockLogicalPlanA) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	p.inputs[ordinal] = child
}

func (p *MockLogicalPlanA) Explain(writer executor.LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *MockLogicalPlanA) String() string {
	return executor.GetTypeName(p)
}

func (p *MockLogicalPlanA) Type() string {
	return executor.GetType(p)
}

func (p *MockLogicalPlanA) Digest() string {
	childrenDigest := fmt.Sprintf("(%d)", len(p.inputs))
	for _, input := range p.inputs {
		childrenDigest = fmt.Sprintf("%s[%d]", childrenDigest, input.ID())
	}
	return fmt.Sprintf("%s%s", executor.GetTypeName(p), childrenDigest)
}

func (p *MockLogicalPlanA) RowDataType() hybridqp.RowDataType {
	return p.LogicalPlanBase.RowDataType()
}

func (p *MockLogicalPlanA) RowExprOptions() []hybridqp.ExprOptions {
	return p.LogicalPlanBase.RowExprOptions()
}

func (p *MockLogicalPlanA) Schema() hybridqp.Catalog {
	return p.LogicalPlanBase.Schema()
}

func (p *MockLogicalPlanA) Dummy() bool {
	return false
}

type MockLogicalPlanAs []*MockLogicalPlanA

type MockLogicalPlanB struct {
	inputs []hybridqp.QueryNode
	executor.LogicalPlanBase
}

func NewMockLogicalPlanB(inputs []hybridqp.QueryNode, schema *executor.QuerySchema) *MockLogicalPlanB {
	plan := &MockLogicalPlanB{
		inputs:          inputs,
		LogicalPlanBase: *executor.NewLogicalPlanBase(schema, nil, nil),
	}

	return plan
}

func (p *MockLogicalPlanB) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *MockLogicalPlanB) DeriveOperations() {
}

func (p *MockLogicalPlanB) Clone() hybridqp.QueryNode {
	clone := NewMockLogicalPlanB(p.inputs, p.Schema().(*executor.QuerySchema))
	*clone = *p
	return clone
}

func (p *MockLogicalPlanB) Children() []hybridqp.QueryNode {
	nodes := make([]hybridqp.QueryNode, 0, len(p.inputs))

	for _, input := range p.inputs {
		nodes = append(nodes, input)
	}
	return nodes
}

func (p *MockLogicalPlanB) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(p.inputs) != len(children) {
		panic(fmt.Sprintf("%d children in logical merge, but replace with %d children", len(p.inputs), len(children)))
	}

	for i := range p.inputs {
		p.inputs[i] = children[i]
	}
}

func (p *MockLogicalPlanB) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	p.inputs[ordinal] = child
}

func (p *MockLogicalPlanB) Explain(writer executor.LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *MockLogicalPlanB) String() string {
	return executor.GetTypeName(p)
}

func (p *MockLogicalPlanB) Type() string {
	return executor.GetType(p)
}

func (p *MockLogicalPlanB) Digest() string {
	childrenDigest := fmt.Sprintf("(%d)", len(p.inputs))
	for _, input := range p.inputs {
		childrenDigest = fmt.Sprintf("%s[%d]", childrenDigest, input.ID())
	}
	return fmt.Sprintf("%s%s", executor.GetTypeName(p), childrenDigest)
}

func (p *MockLogicalPlanB) RowDataType() hybridqp.RowDataType {
	return p.LogicalPlanBase.RowDataType()
}

func (p *MockLogicalPlanB) RowExprOptions() []hybridqp.ExprOptions {
	return p.LogicalPlanBase.RowExprOptions()
}

func (p *MockLogicalPlanB) Schema() hybridqp.Catalog {
	return p.LogicalPlanBase.Schema()
}

func (p *MockLogicalPlanB) Dummy() bool {
	return false
}

type MockLogicalPlanBs []*MockLogicalPlanB

func (mocks MockLogicalPlanAs) LogicalPlans() []executor.LogicalPlan {
	plans := make([]executor.LogicalPlan, 0, len(mocks))
	for _, mock := range mocks {
		plans = append(plans, mock)
	}
	return plans
}

func (mocks MockLogicalPlanBs) LogicalPlans() []executor.LogicalPlan {
	plans := make([]executor.LogicalPlan, 0, len(mocks))
	for _, mock := range mocks {
		plans = append(plans, mock)
	}
	return plans
}

func TestCreateHeuDag(t *testing.T) {
	maxnum := 3
	mocks := make([]*MockLogicalPlanA, 0, maxnum)

	for i := 0; i < maxnum; i++ {
		mocks = append(mocks, NewMockLogicalPlanA(nil, nil))
	}

	mocks[0].inputs = append(mocks[0].inputs, mocks[1])
	mocks[0].inputs = append(mocks[0].inputs, mocks[2])

	dag := executor.NewHeuDag()
	nTransformation := 0

	cloneNodeWithChildren := func(node hybridqp.QueryNode, children []hybridqp.QueryNode) hybridqp.QueryNode {
		clone := node.Clone()
		clone.ReplaceChildren(children)
		return clone
	}

	equalNodes := func(lhs []hybridqp.QueryNode, rhs []hybridqp.QueryNode) bool {
		if len(lhs) != len(rhs) {
			return false
		}

		for i, l := range lhs {
			if l != rhs[i] {
				return false
			}
		}

		return true
	}

	var addNodeToDag func(node hybridqp.QueryNode) *executor.HeuVertex
	addNodeToDag = func(node hybridqp.QueryNode) *executor.HeuVertex {
		if vertex, ok := node.(*executor.HeuVertex); ok && dag.Contains(vertex) {
			return vertex
		}

		children := node.Children()
		vertexs := make([]hybridqp.QueryNode, 0, len(children))

		for _, child := range children {
			vertexs = append(vertexs, addNodeToDag(child))
		}

		if equalNodes(children, vertexs) {
			node = cloneNodeWithChildren(node, vertexs)
		}

		if equivVertex, ok := dag.GetVertexByDigest(node.Digest()); ok {
			return equivVertex
		}

		vertex := executor.NewHeuVertex(node)
		dag.AddVerteix(vertex)
		dag.UpdateVerteix(vertex, node)

		for _, from := range vertexs {
			dag.AddEdge(from.(*executor.HeuVertex), vertex)
		}

		nTransformation++
		return vertex
	}

	root := addNodeToDag(mocks[0])

	if dag.EdgeSize() != 2 {
		t.Errorf("expect 2 edges in dag, but %d", dag.EdgeSize())
	}

	if dag.VertexSize() != 2 {
		t.Errorf("expect 2 edges in dag, but %d", dag.VertexSize())
	}

	if dag.DigestSize() != 2 {
		t.Errorf("expect 2 digest in dag, but %d", dag.DigestSize())
	}

	iter := dag.GetGraphIterator(root, executor.DEPTH_FIRST)

	if iter.Size() != 3 {
		t.Errorf("expect 3 vertexs in dag, but %d", iter.Size())
	}
}

func TestFindBestExpWithMockAToBRule(t *testing.T) {
	maxnum := 3
	mocks := make([]*MockLogicalPlanA, 0, maxnum)

	for i := 0; i < maxnum; i++ {
		mocks = append(mocks, NewMockLogicalPlanA(nil, nil))
	}

	mocks[0].inputs = append(mocks[0].inputs, mocks[1])
	mocks[0].inputs = append(mocks[0].inputs, mocks[2])

	builder := executor.NewOptRuleOperandBuilderBase()
	builder.AnyInput((&MockLogicalPlanA{}).Type())
	atoBRule := NewMockAToBRule(builder.Operand(), "")

	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_TEST)

	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(atoBRule)
	planner.SetRoot(mocks[0])

	if planner.Transformations() != 2 {
		t.Errorf("only two transformations after set root, but %d", planner.Transformations())
	}

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best node found by planner, expect one")
	}

	if planner.Transformations() != 7 {
		t.Errorf("7 transformations after find best logical expression, but %d", planner.Transformations())
	}

	expectNode := &MockLogicalPlanB{}
	visitor := &hybridqp.FlattenQueryNodeVisitor{}
	hybridqp.WalkQueryNodeInPreOrder(visitor, best)
	for _, node := range visitor.Nodes() {
		if node.Type() != expectNode.Type() {
			t.Errorf("expect all node are %v, but one is %v", expectNode.Type(), node.Type())
		}
	}
}

func TestFindBestExpWithMockMergeBRule(t *testing.T) {
	maxnum := 3
	mocks := make([]*MockLogicalPlanB, 0, maxnum)

	for i := 0; i < maxnum; i++ {
		mocks = append(mocks, NewMockLogicalPlanB(nil, nil))
	}

	mocks[0].inputs = append(mocks[0].inputs, mocks[1])
	mocks[0].inputs = append(mocks[0].inputs, mocks[2])

	builder := executor.NewOptRuleOperandBuilderBase()
	builder.AnyInput((&MockLogicalPlanB{}).Type())
	input := builder.Operand()
	builder.OneInput((&MockLogicalPlanB{}).Type(), input)
	mergeBRule := NewMockMergeBRule(builder.Operand(), "")

	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_TEST)

	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(mergeBRule)
	planner.SetRoot(mocks[0])

	if planner.Transformations() != 2 {
		t.Errorf("only two transformations after set root, but %d", planner.Transformations())
	}

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best node found by planner, expect one")
	}

	if planner.Transformations() != 3 {
		t.Errorf("3 transformations after find best logical expression, but %d", planner.Transformations())
	}

	expectNode := &MockLogicalPlanB{}
	visitor := &hybridqp.FlattenQueryNodeVisitor{}
	hybridqp.WalkQueryNodeInPreOrder(visitor, best)

	if len(visitor.Nodes()) != 1 {
		t.Errorf("only one query node in best node, but %v", visitor.Nodes())
	}

	if visitor.Nodes()[0].Type() != expectNode.Type() {
		t.Errorf("expect node is %v, but is %v", expectNode.Type(), visitor.Nodes()[0].Type())
	}
}

func TestFindBestExpWithManyRules(t *testing.T) {
	maxnum := 3
	mocks := make([]*MockLogicalPlanA, 0, maxnum)

	for i := 0; i < maxnum; i++ {
		mocks = append(mocks, NewMockLogicalPlanA(nil, nil))
	}

	mocks[0].inputs = append(mocks[0].inputs, mocks[1])
	mocks[0].inputs = append(mocks[0].inputs, mocks[2])

	builder := executor.NewOptRuleOperandBuilderBase()
	builder.AnyInput((&MockLogicalPlanA{}).Type())
	atoBRule := NewMockAToBRule(builder.Operand(), "")

	builder.AnyInput((&MockLogicalPlanB{}).Type())
	input := builder.Operand()
	builder.OneInput((&MockLogicalPlanB{}).Type(), input)
	mergeBRule := NewMockMergeBRule(builder.Operand(), "")

	pb := NewHeuProgramBuilder()
	pb.AddRuleCatagory(executor.RULE_TEST)

	planner := executor.NewHeuPlannerImpl(pb.Build())
	planner.AddRule(mergeBRule)
	planner.AddRule(atoBRule)
	planner.SetRoot(mocks[0])

	if planner.Transformations() != 2 {
		t.Errorf("only two transformations after set root, but %d", planner.Transformations())
	}

	best := planner.FindBestExp()

	if best == nil {
		t.Error("no best node found by planner, expect one")
	}

	if planner.Transformations() != 8 {
		t.Errorf("3 transformations after find best logical expression, but %d", planner.Transformations())
	}

	expectNode := &MockLogicalPlanB{}
	visitor := &hybridqp.FlattenQueryNodeVisitor{}
	hybridqp.WalkQueryNodeInPreOrder(visitor, best)

	if len(visitor.Nodes()) != 1 {
		t.Errorf("only one query node in best node, but %v", visitor.Nodes())
	}

	if visitor.Nodes()[0].Type() != expectNode.Type() {
		t.Errorf("expect node is %v, but is %v", expectNode.Type(), visitor.Nodes()[0].Type())
	}
}

func TestBuildHeuristicPlanner(t *testing.T) {
	plan := executor.BuildHeuristicPlanner()
	if plan == nil {
		t.Fatalf("BuildHeuristicPlanner fail")
	}
}
