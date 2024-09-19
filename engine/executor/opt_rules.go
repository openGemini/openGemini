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

package executor

import (
	"container/list"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/hybridqp"
)

type OptRuleOperandChildPolicy int

const (
	ANY OptRuleOperandChildPolicy = iota
	LEAF
	SOME
	UNORDERED
	WILDCARD
	AFTER
)

type OptRuleOperand interface {
	Equals(rhs OptRuleOperand) bool
	SetParent(parent OptRuleOperand)
	Parent() OptRuleOperand
	Matches(node hybridqp.QueryNode) bool
	Policy() OptRuleOperandChildPolicy
	SetRule(rule OptRule)
	Rule() OptRule
	SetOrdinalInParent(int)
	SetOrdinalInRule(int)
	Children() []OptRuleOperand
}

type OptRuleOperands []OptRuleOperand

func (operands OptRuleOperands) Equals(rhs OptRuleOperands) bool {
	if len(operands) != len(rhs) {
		return false
	}

	for i, operand := range operands {
		if !operand.Equals(rhs[i]) {
			return false
		}
	}

	return true
}

type OptRuleOperandBase struct {
	policy          OptRuleOperandChildPolicy
	planType        string
	children        []OptRuleOperand
	parent          OptRuleOperand
	rule            OptRule
	ordinalInParent int
	ordinalInRule   int
}

func NewOptRuleOperandBase(planType string, policy OptRuleOperandChildPolicy, children []OptRuleOperand) *OptRuleOperandBase {
	ob := &OptRuleOperandBase{
		planType:        planType,
		policy:          policy,
		children:        children,
		parent:          nil,
		rule:            nil,
		ordinalInParent: 0,
		ordinalInRule:   0,
	}

	return ob
}

func (ob *OptRuleOperandBase) Equals(rhs OptRuleOperand) bool {
	if ob == rhs {
		return true
	}

	if rob, ok := rhs.(*OptRuleOperandBase); !ok {
		return false
	} else {
		return ob.planType == rob.planType &&
			OptRuleOperands(ob.children).Equals(OptRuleOperands(rob.children))
	}
}

func (ob *OptRuleOperandBase) SetParent(parent OptRuleOperand) {
	ob.parent = parent
}

func (ob *OptRuleOperandBase) Parent() OptRuleOperand {
	return ob.parent
}

func (ob *OptRuleOperandBase) Matches(node hybridqp.QueryNode) bool {
	return ob.planType == node.Type()
}

func (ob *OptRuleOperandBase) Policy() OptRuleOperandChildPolicy {
	return ob.policy
}

func (ob *OptRuleOperandBase) SetRule(rule OptRule) {
	ob.rule = rule
}

func (ob *OptRuleOperandBase) Rule() OptRule {
	return ob.rule
}

func (ob *OptRuleOperandBase) SetOrdinalInParent(ordinal int) {
	ob.ordinalInParent = ordinal
}

func (ob *OptRuleOperandBase) SetOrdinalInRule(ordinal int) {
	ob.ordinalInRule = ordinal
}

func (ob *OptRuleOperandBase) Children() []OptRuleOperand {
	return ob.children
}

type OptRuleOperandBuilder interface {
	NoInput(planType string)
	AnyInput(planType string)
	OneInput(planType string, input OptRuleOperand)
	WildCardInput(planType string)
	Inputs(planType string, inputs ...OptRuleOperand)
	UnorderedInputs(planType string, inputs ...OptRuleOperand)
	Operands() []OptRuleOperand
	Operand() OptRuleOperand
}

type OptRuleOperandBuilderBase struct {
	operands []OptRuleOperand
}

func NewOptRuleOperandBuilderBase() *OptRuleOperandBuilderBase {
	builder := &OptRuleOperandBuilderBase{
		operands: nil,
	}

	return builder
}

func (builder *OptRuleOperandBuilderBase) build(planType string, policy OptRuleOperandChildPolicy, inputs []OptRuleOperand) {
	operand := NewOptRuleOperandBase(planType, policy, inputs)
	builder.operands = append(builder.operands, operand)
}

func (builder *OptRuleOperandBuilderBase) NoInput(planType string) {
	builder.build(planType, LEAF, nil)
}

func (builder *OptRuleOperandBuilderBase) AnyInput(planType string) {
	builder.build(planType, ANY, nil)
}

func (builder *OptRuleOperandBuilderBase) AfterInput(planType string) {
	builder.build(planType, AFTER, nil)
}

func (builder *OptRuleOperandBuilderBase) WildCardInput(planType string, input OptRuleOperand) {
	builder.build(planType, WILDCARD, []OptRuleOperand{input})
}

func (builder *OptRuleOperandBuilderBase) OneInput(planType string, input OptRuleOperand) {
	builder.build(planType, SOME, []OptRuleOperand{input})
}

func (builder *OptRuleOperandBuilderBase) Inputs(planType string, inputs ...OptRuleOperand) {
	builder.build(planType, SOME, inputs)
}

func (builder *OptRuleOperandBuilderBase) UnorderedInputs(planType string, inputs ...OptRuleOperand) {
	builder.build(planType, UNORDERED, inputs)
}

func (builder *OptRuleOperandBuilderBase) Operands() []OptRuleOperand {
	operands := builder.operands
	builder.operands = nil
	return operands
}

func (builder *OptRuleOperandBuilderBase) Operand() OptRuleOperand {
	if len(builder.operands) == 0 {
		builder.operands = nil
		return nil
	}
	operands := builder.Operands()
	return operands[0]
}

var (
	nextOptRuleCallId uint64 = 0
)

type OptRuleCall struct {
	id      uint64
	results []hybridqp.QueryNode
	planner HeuPlanner
	rule    OptRule
	nodes   []hybridqp.QueryNode
}

func NewOptRuleCall(planner HeuPlanner,
	operand OptRuleOperand,
	nodes []hybridqp.QueryNode,
) *OptRuleCall {
	return &OptRuleCall{
		id:      atomic.AddUint64(&nextOptRuleCallId, 1),
		results: nil,
		planner: planner,
		rule:    operand.Rule(),
		nodes:   nodes,
	}
}

func (c *OptRuleCall) Rule() OptRule {
	return c.rule
}

func (c *OptRuleCall) Node(ordinal int) hybridqp.QueryNode {
	return c.nodes[ordinal]
}

func (c *OptRuleCall) GetResult() []hybridqp.QueryNode {
	return c.results
}

func (c *OptRuleCall) TransformTo(to hybridqp.QueryNode) []hybridqp.QueryNode {
	c.results = append(c.results, to)
	return c.results
}

type OptRuleCatagory int

const (
	RULE_TEST OptRuleCatagory = iota
	RULE_PUSHDOWN_LIMIT
	RULE_PUSHDOWN_AGG
	RULE_SPREAD_AGG
	RULE_SUBQUERY
	RULE_HEIMADLL_PUSHDOWN
)

type OptRule interface {
	Initialize(rule OptRule, operand OptRuleOperand, description string)
	ToString() string
	Description() string
	Catagory() OptRuleCatagory
	Equals(OptRule) bool
	GetOperand() OptRuleOperand
	Matches(call *OptRuleCall) bool
	OnMatch(call *OptRuleCall)
}

type OptRuleBase struct {
	derive      OptRule
	description string
	operand     OptRuleOperand
	operands    []OptRuleOperand
}

func (r *OptRuleBase) flattenOptRuleOperand(operand OptRuleOperand) []OptRuleOperand {
	operandList := list.New()

	operand.SetRule(r.derive)
	operand.SetParent(nil)
	operand.SetOrdinalInParent(0)
	operand.SetOrdinalInRule(operandList.Len())
	operandList.PushBack(operand)

	r.flatten(operandList, operand)

	operands := make([]OptRuleOperand, 0, operandList.Len())
	for e := operandList.Front(); e != nil; e = e.Next() {
		operands = append(operands, e.Value.(OptRuleOperand))
	}
	return operands
}

func (r *OptRuleBase) flatten(operandList *list.List, parentOperand OptRuleOperand) {
	for i, operand := range parentOperand.Children() {
		operand.SetRule(r.derive)
		operand.SetParent(parentOperand)
		operand.SetOrdinalInParent(i)
		operand.SetOrdinalInRule(operandList.Len())
		operandList.PushBack(operand)
		r.flatten(operandList, operand)
	}
}

func (r *OptRuleBase) Initialize(rule OptRule, operand OptRuleOperand, description string) {
	r.derive = rule
	r.operand = operand
	r.description = description
	r.operands = r.flattenOptRuleOperand(operand)
}

func (r *OptRuleBase) Description() string {
	return r.description
}

func (r *OptRuleBase) GetOperand() OptRuleOperand {
	return r.operand
}

func (r *OptRuleBase) Equals(rhs *OptRuleBase) bool {
	if r == rhs {
		return true
	}

	if r.operand.Equals(rhs.operand) && r.description == rhs.description {
		return true
	}

	return false
}

func (r *OptRuleBase) Matches(_ *OptRuleCall) bool {
	return true
}
