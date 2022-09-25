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
	"container/list"
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type HeuInstruction interface {
	Initialize(bool)
	Execute(HeuPlanner)
}

type HeuPlanner interface {
	hybridqp.Planner
	ExecuteInstruction(HeuInstruction)
	Vertex(node hybridqp.QueryNode) (*HeuVertex, bool)
}

type RuleSet map[OptRule]struct{}

func (rs RuleSet) Add(rule OptRule) {
	rs[rule] = struct{}{}
}

func (rs RuleSet) AddAll(ruleSet RuleSet) {
	for rule := range ruleSet {
		rs[rule] = struct{}{}
	}
}

type BeginGroup struct {
}

func NewBeginGroup() *BeginGroup {
	return &BeginGroup{}
}

func (g *BeginGroup) Initialize(clearCache bool) {

}

func (g *BeginGroup) Execute(planner HeuPlanner) {
	planner.ExecuteInstruction(g)
}

type EndGroup struct {
	ruleSet    RuleSet
	collecting bool
}

func NewEndGroup() *EndGroup {
	return &EndGroup{}
}

func (g *EndGroup) Initialize(clearCache bool) {
	if !clearCache {
		return
	}

	g.ruleSet = make(RuleSet)
	g.collecting = true
}

func (g *EndGroup) Execute(planner HeuPlanner) {
	planner.ExecuteInstruction(g)
}

type RuleInstruction struct {
	ruleCatatory OptRuleCatagory
	ruleSet      RuleSet
}

func NewRuleInstruction(ruleCatatory OptRuleCatagory) *RuleInstruction {
	return &RuleInstruction{
		ruleCatatory: ruleCatatory,
		ruleSet:      nil,
	}
}

func (g *RuleInstruction) Initialize(clearCache bool) {
	if !clearCache {
		return
	}

	g.ruleSet = nil
}

func (g *RuleInstruction) Execute(planner HeuPlanner) {
	planner.ExecuteInstruction(g)
}

func (g *RuleInstruction) RuleCatagory() OptRuleCatagory {
	return g.ruleCatatory
}

type HeuMatchOrder uint8

const (
	ARBITRARY HeuMatchOrder = iota
	BOTTOM_UP
	TOP_DOWN
	DEPTH_FIRST
)

type HeuProgram struct {
	instructions []HeuInstruction
	matchLimit   int
	matchOrder   HeuMatchOrder
	group        *EndGroup
}

func NewHeuProgram(instructions []HeuInstruction) *HeuProgram {
	program := &HeuProgram{
		instructions: instructions,
	}

	return program
}

func (p *HeuProgram) Initialize(clearCache bool) {
	p.matchLimit = int(^uint(0) >> 1)
	p.matchOrder = DEPTH_FIRST
	p.group = nil

	for _, instruction := range p.instructions {
		instruction.Initialize(clearCache)
	}
}

type HeuProgramBuilder struct {
	instructions []HeuInstruction
	group        *BeginGroup
}

func NewHeuProgramBuilder() *HeuProgramBuilder {
	return &HeuProgramBuilder{
		instructions: nil,
		group:        nil,
	}
}

func (b *HeuProgramBuilder) AddRuleCatagory(ruleCatagory OptRuleCatagory) *HeuProgramBuilder {
	ri := NewRuleInstruction(ruleCatagory)
	b.instructions = append(b.instructions, ri)
	return b
}

func (b *HeuProgramBuilder) Clear() {
	b.instructions = nil
	b.group = nil
}

func (b *HeuProgramBuilder) Build() *HeuProgram {
	program := NewHeuProgram(b.instructions)
	b.Clear()

	return program
}

type HeuEdge struct {
	from *HeuVertex
	to   *HeuVertex
}

func NewHeuEdge(from *HeuVertex, to *HeuVertex) *HeuEdge {
	return &HeuEdge{
		from: from,
		to:   to,
	}
}

type HeuVertexInfo struct {
	directEdges   map[*HeuEdge]struct{}
	backwardEdges map[*HeuEdge]struct{}
}

func NewHeuVertexInfo() *HeuVertexInfo {
	return &HeuVertexInfo{
		directEdges:   make(map[*HeuEdge]struct{}),
		backwardEdges: make(map[*HeuEdge]struct{}),
	}
}

func (info *HeuVertexInfo) AddDirectEdge(edge *HeuEdge) {
	info.directEdges[edge] = struct{}{}
}

func (info *HeuVertexInfo) AddBackwardEdge(edge *HeuEdge) {
	info.backwardEdges[edge] = struct{}{}
}

type HeuVertex struct {
	id   uint64
	node hybridqp.QueryNode
	info *HeuVertexInfo
}

type HeuVertexWriter interface {
	Explain(*HeuVertex)
	Item(string, interface{})
	String() string
}

func NewHeuVertex(node hybridqp.QueryNode) *HeuVertex {
	return &HeuVertex{
		id:   hybridqp.GenerateNodeId(),
		node: node,
		info: NewHeuVertexInfo(),
	}
}

func (v *HeuVertex) Trait() hybridqp.Trait {
	return nil
}

func (v *HeuVertex) ApplyTrait(trait hybridqp.Trait) {
}

func (v *HeuVertex) Node() hybridqp.QueryNode {
	return v.node
}

func (v *HeuVertex) ReplaceNode(node hybridqp.QueryNode) {
	v.node = node
}

func (v *HeuVertex) Children() []hybridqp.QueryNode {
	return v.node.Children()
}

func (v *HeuVertex) String() string {
	return GetTypeName(v)
}

func (v *HeuVertex) Type() string {
	return GetType(v)
}

func (v *HeuVertex) Digest() string {
	return v.node.Digest()
}

func (v *HeuVertex) Clone() hybridqp.QueryNode {
	clone := &HeuVertex{}
	*clone = *v
	return clone
}

func (v *HeuVertex) ReplaceChildren(children []hybridqp.QueryNode) {
	v.node.ReplaceChildren(children)
}

func (v *HeuVertex) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	v.node.ReplaceChild(ordinal, child)
}

func (v *HeuVertex) RowDataType() hybridqp.RowDataType {
	return v.node.RowDataType()
}

func (v *HeuVertex) RowExprOptions() []hybridqp.ExprOptions {
	return nil
}

func (v *HeuVertex) Dummy() bool {
	return v.node.Dummy()
}

func (v *HeuVertex) Equals(rhs *HeuVertex) bool {
	return v.Digest() == rhs.Digest()
}

func (v *HeuVertex) ID() uint64 {
	return v.id
}

func (v *HeuVertex) DeriveOperations() {
	v.node.DeriveOperations()
}

func (v *HeuVertex) Schema() hybridqp.Catalog {
	return v.node.Schema()
}

func (v *HeuVertex) SetSchema(schema hybridqp.Catalog) {
	v.node.SetSchema(schema)
}

func (v *HeuVertex) GetParentVertex(vertex *HeuVertex) *HeuVertex {
	for element := range v.info.directEdges {
		if element.from.Digest() == vertex.Digest() {
			return element.to
		}
	}
	return nil
}

func (v *HeuVertex) SubTreeEqual(vertex *HeuVertex) bool {
	if v.Digest() != vertex.Digest() {
		return false
	} else {
		if v.Children() != nil && vertex.Children() != nil {
			return v.Children()[0].(*HeuVertex).SubTreeEqual(vertex.Children()[0].(*HeuVertex))
		} else if v.Children() == nil && vertex.Children() == nil {
			return true
		} else {
			return false
		}
	}
}

type HeuVertexs []*HeuVertex

func (hvs HeuVertexs) IndexOf(vertex *HeuVertex) int {
	for i, v := range hvs {
		if v.Digest() == vertex.Digest() {
			return i
		}
	}

	return -1
}

type HeuDag struct {
	vertexSet         map[*HeuVertex]struct{}
	edgeSet           map[*HeuEdge]struct{}
	mapDigestToVertex map[string]*HeuVertex
}

func NewHeuDag() *HeuDag {
	return &HeuDag{
		vertexSet:         make(map[*HeuVertex]struct{}),
		edgeSet:           make(map[*HeuEdge]struct{}),
		mapDigestToVertex: make(map[string]*HeuVertex),
	}
}

func (dag *HeuDag) VertexSize() int {
	return len(dag.vertexSet)
}

func (dag *HeuDag) EdgeSize() int {
	return len(dag.edgeSet)
}

func (dag *HeuDag) DigestSize() int {
	return len(dag.mapDigestToVertex)
}

func (dag *HeuDag) Contains(vertex *HeuVertex) bool {
	_, ok := dag.vertexSet[vertex]
	return ok
}

func (dag *HeuDag) GetVertexByDigest(digest string) (*HeuVertex, bool) {
	vertex, ok := dag.mapDigestToVertex[digest]
	return vertex, ok
}

func (dag *HeuDag) AddVerteix(vertex *HeuVertex) bool {
	if _, ok := dag.vertexSet[vertex]; ok {
		return false
	} else {
		dag.vertexSet[vertex] = struct{}{}
		return true
	}
}

func (dag *HeuDag) RemoveVertex(vertex *HeuVertex) {
	delete(dag.vertexSet, vertex)

	for edge := range dag.edgeSet {
		if edge.from.Digest() == vertex.Digest() || edge.to.Digest() == vertex.Digest() {
			dag.RemoveEdge(edge.from, edge.to)
		}
	}
}

func (dag *HeuDag) RemoveEdge(from *HeuVertex, to *HeuVertex) {
	for edge := range from.info.directEdges {
		if edge.to == to {
			delete(from.info.directEdges, edge)
			break
		}
	}

	for edge := range to.info.backwardEdges {
		if edge.from == from {
			delete(to.info.backwardEdges, edge)
		}
	}

	for edge := range dag.edgeSet {
		if edge.from == from && edge.to == to {
			delete(dag.edgeSet, edge)
		}
	}
}

func (dag *HeuDag) AddEdge(from *HeuVertex, to *HeuVertex) bool {
	edge := NewHeuEdge(from, to)

	if _, ok := dag.edgeSet[edge]; ok {
		return !ok
	}
	dag.edgeSet[edge] = struct{}{}

	from.info.AddDirectEdge(edge)
	to.info.AddBackwardEdge(edge)
	return true
}

func (dag *HeuDag) UpdateVerteix(vertex *HeuVertex, node hybridqp.QueryNode) {
	if dag.mapDigestToVertex[vertex.Digest()] == vertex {
		delete(dag.mapDigestToVertex, vertex.Digest())
	}

	dag.mapDigestToVertex[node.Digest()] = vertex

	if vertex.Node() != node {
		vertex.ReplaceNode(node)
	}
}

func (dag *HeuDag) GetGraphIterator(vertex *HeuVertex, matchOrder HeuMatchOrder) *GraphIterator {
	switch matchOrder {
	case DEPTH_FIRST:
		return dag.depthFirstIterator(vertex)
	default:
		panic("unknown heuristic match orfer")
	}
}

func (dag *HeuDag) depthFirstIterator(vertex *HeuVertex) *GraphIterator {
	iter := NewGraphIterator(len(dag.vertexSet))
	dag.WalkHeuDag(iter, vertex)
	return iter
}

func (dag *HeuDag) WalkHeuDag(visitor HeuDagVisitor, vertex *HeuVertex) {
	if !dag.Contains(vertex) {
		return
	}

	if visitor := visitor.Visit(vertex); visitor == nil {
		return
	}

	for edge := range vertex.info.backwardEdges {
		dag.WalkHeuDag(visitor, edge.from)
	}
}

func (dag *HeuDag) AllParents(vertex *HeuVertex) []*HeuVertex {
	if !dag.Contains(vertex) {
		return nil
	}
	vertexs := make([]*HeuVertex, 0, len(vertex.info.directEdges))
	for edge := range vertex.info.directEdges {
		vertexs = append(vertexs, edge.to)
	}

	return vertexs
}

type HeuDagVisitor interface {
	Visit(vertex *HeuVertex) HeuDagVisitor
}
type GraphIterator struct {
	vertexs []*HeuVertex
	index   int
}

func NewGraphIterator(capacity int) *GraphIterator {
	return &GraphIterator{
		vertexs: make([]*HeuVertex, 0, capacity),
		index:   0,
	}
}

func (iter *GraphIterator) Visit(vertex *HeuVertex) HeuDagVisitor {
	iter.vertexs = append(iter.vertexs, vertex)
	return iter
}

func (iter *GraphIterator) HasNext() bool {
	return iter.index < len(iter.vertexs)
}

func (iter *GraphIterator) Next() *HeuVertex {
	if iter.HasNext() {
		node := iter.vertexs[iter.index]
		iter.index++
		return node
	}
	return nil
}

func (iter *GraphIterator) Reset() {
	iter.index = 0
}

func (iter *GraphIterator) Size() int {
	return len(iter.vertexs)
}

type HeuPlannerImpl struct {
	mainProgram     *HeuProgram
	currentProgram  *HeuProgram
	mapDescToRule   map[string]OptRule
	dag             *HeuDag
	root            *HeuVertex
	nTransformation int
	heuLogger       *logger.Logger
}

func NewHeuPlannerImpl(program *HeuProgram) *HeuPlannerImpl {
	planner := &HeuPlannerImpl{
		mainProgram:     program,
		currentProgram:  nil,
		mapDescToRule:   make(map[string]OptRule),
		dag:             NewHeuDag(),
		root:            nil,
		nTransformation: 0,
		heuLogger:       logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "HeuPlanner")),
	}

	return planner
}

func (p *HeuPlannerImpl) Vertex(node hybridqp.QueryNode) (*HeuVertex, bool) {
	return p.dag.GetVertexByDigest(node.Digest())
}

func (p *HeuPlannerImpl) AddRule(rule OptRule) bool {
	description := rule.Description()
	existingRule, ok := p.mapDescToRule[description]

	if ok {
		if existingRule.Equals(rule) {
			return false
		} else {
			panic(fmt.Sprintf("description of rules must be unique, existing rule(%v), new rule(%v)", existingRule, rule))
		}
	}

	p.mapDescToRule[description] = rule

	return true
}

func (p *HeuPlannerImpl) equalNodes(lhs []hybridqp.QueryNode, rhs []hybridqp.QueryNode) bool {
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

func (p *HeuPlannerImpl) cloneNodeWithChildren(node hybridqp.QueryNode, children []hybridqp.QueryNode) hybridqp.QueryNode {
	clone := node.Clone()
	clone.ReplaceChildren(children)
	return clone
}

func (p *HeuPlannerImpl) addNodeToDag(node hybridqp.QueryNode) *HeuVertex {
	if vertex, ok := node.(*HeuVertex); ok && p.dag.Contains(vertex) {
		return vertex
	}

	children := node.Children()
	vertexs := make([]hybridqp.QueryNode, 0, len(children))

	for _, child := range children {
		vertexs = append(vertexs, p.addNodeToDag(child))
	}

	if !p.equalNodes(children, vertexs) {
		node = p.cloneNodeWithChildren(node, vertexs)
	}

	if equivVertex, ok := p.dag.GetVertexByDigest(node.Digest()); ok {
		return equivVertex
	}

	vertex := NewHeuVertex(node)
	p.dag.AddVerteix(vertex)
	p.dag.UpdateVerteix(vertex, node)

	for _, from := range vertexs {
		p.dag.AddEdge(from.(*HeuVertex), vertex)
	}

	p.nTransformation++
	return vertex
}

func (p *HeuPlannerImpl) SetRoot(root hybridqp.QueryNode) {
	p.root = p.addNodeToDag(root)
}

func (p *HeuPlannerImpl) Transformations() int {
	return p.nTransformation
}

func (p *HeuPlannerImpl) FindBestExp() hybridqp.QueryNode {
	p.executeProgram(p.mainProgram)
	final := p.buildFinalPlan(p.root)
	hybridqp.WalkQueryNodeInPostOrder(p, final)
	return final
}

func (p *HeuPlannerImpl) buildFinalPlan(vertex *HeuVertex) hybridqp.QueryNode {
	node := vertex.Node()

	for i, input := range node.Children() {
		v, ok := input.(*HeuVertex)
		if !ok {
			continue
		}
		child := p.buildFinalPlan(v)
		node.ReplaceChild(i, child)
	}

	return node
}

func (p *HeuPlannerImpl) ExecuteInstruction(instruction HeuInstruction) {
	switch t := instruction.(type) {
	case *BeginGroup:
		p.executeBeginGroup(t)
	case *EndGroup:
		p.executeEndGroup(t)
	case *RuleInstruction:
		p.executeRuleInstruction(t)
	default:
		panic(fmt.Sprintf("unsupport instruction %v", t))
	}
}

func (p *HeuPlannerImpl) skippingGroup() bool {
	if p.currentProgram != nil && p.currentProgram.group != nil {
		return !p.currentProgram.group.collecting
	} else {
		return false
	}
}

func (p *HeuPlannerImpl) executeRuleInstruction(instruction *RuleInstruction) {
	if p.skippingGroup() {
		return
	}

	p.heuLogger.Debug(fmt.Sprintf("Apply rule type %v", instruction))

	if instruction.ruleSet == nil {
		instruction.ruleSet = make(RuleSet)
		for _, rule := range p.mapDescToRule {
			// all rule
			if rule.Catagory() == instruction.RuleCatagory() {
				instruction.ruleSet.Add(rule)
			}
		}
	}

	p.applyRules(instruction.ruleSet, true)
}

func (p *HeuPlannerImpl) applyRules(ruleSet RuleSet, forceConversions bool) {
	if p.currentProgram.group != nil {
		ruleSet.AddAll(p.currentProgram.group.ruleSet)
		return
	}
	p.heuLogger.Debug(fmt.Sprintf("Apply rule set %v", ruleSet))

	forceRestartAfterTransformation := p.currentProgram.matchOrder != ARBITRARY &&
		p.currentProgram.matchOrder != DEPTH_FIRST
	nMatches := 0
	var fixedPoint bool
	for {
		iter := p.dag.GetGraphIterator(p.root, p.currentProgram.matchOrder)
		fixedPoint = true
		for {
			if !iter.HasNext() {
				break
			}
			vertex := iter.Next()
			for rule := range ruleSet {
				newVertex := p.applyRule(rule, vertex, forceConversions)
				if newVertex == nil || newVertex.Equals(vertex) {
					continue
				}
				nMatches++
				if nMatches >= p.currentProgram.matchLimit {
					return
				}
				if forceRestartAfterTransformation {
					iter = p.dag.GetGraphIterator(p.root, p.currentProgram.matchOrder)
				} else {
					iter = p.dag.GetGraphIterator(newVertex, p.currentProgram.matchOrder)
					if p.currentProgram.matchOrder == DEPTH_FIRST {
						nMatches = p.depthFirstApply(iter, ruleSet, forceConversions, nMatches)
					}
					if nMatches >= p.currentProgram.matchLimit {
						return
					}
					fixedPoint = false
				}
				break
			}
		}
		if fixedPoint {
			break
		}
	}
}

func (p *HeuPlannerImpl) nodeListToSlice(nodes *list.List) []hybridqp.QueryNode {
	s := make([]hybridqp.QueryNode, 0, nodes.Len())
	for entry := nodes.Front(); entry != nil; entry = entry.Next() {
		s = append(s, entry.Value.(hybridqp.QueryNode))
	}
	return s
}

func (p *HeuPlannerImpl) applyRule(rule OptRule, vertex *HeuVertex, forceConversions bool) *HeuVertex {
	if !p.dag.Contains(vertex) {
		return nil
	}

	parent := list.New()
	nodes := list.New()
	nodeChildren := make(map[hybridqp.QueryNode][]hybridqp.QueryNode)

	match := p.matchOperands(rule.GetOperand(), vertex.node, nodes, nodeChildren)

	if !match {
		return nil
	}

	call := NewOptRuleCall(p, rule.GetOperand(), p.nodeListToSlice(nodes), nodeChildren, p.nodeListToSlice(parent))

	if !rule.Matches(call) {
		return nil
	}

	p.fireRule(call)

	if len(call.GetResult()) > 0 {
		return p.applyTransformationResults(vertex, call)
	}

	return nil
}

func (p *HeuPlannerImpl) splicingVertexs(newVertex *HeuVertex, vertex *HeuVertex, parents []*HeuVertex) {
	p.dag.UpdateVerteix(newVertex, newVertex.Node())

	for _, parent := range parents {
		node := parent.Node()
		for i, child := range node.Children() {
			if child != vertex {
				continue
			}
			node.ReplaceChild(i, newVertex)
		}
		p.dag.RemoveEdge(vertex, parent)
		p.dag.AddEdge(newVertex, parent)
		p.dag.UpdateVerteix(parent, parent.node)
	}

	if p.root == vertex {
		p.root = newVertex
	}
}

func (p *HeuPlannerImpl) applyTransformationResults(vertex *HeuVertex, call *OptRuleCall) *HeuVertex {
	var bestNode hybridqp.QueryNode

	if len(call.GetResult()) == 1 {
		bestNode = call.GetResult()[0]
	} else {
		panic("need cost model to choose best node")
	}

	p.nTransformation++

	parents := p.dag.AllParents(vertex)
	newVertex := p.addNodeToDag(bestNode)
	equivParentIndex := HeuVertexs(parents).IndexOf(newVertex)
	if equivParentIndex != -1 {
		newVertex = parents[equivParentIndex]
	} else {
		p.splicingVertexs(newVertex, vertex, parents)
	}

	return newVertex
}

func (p *HeuPlannerImpl) fireRule(call *OptRuleCall) {
	call.Rule().OnMatch(call)
}

func (p *HeuPlannerImpl) matchOperands(operand OptRuleOperand,
	node hybridqp.QueryNode,
	nodes *list.List,
	nodeChildren map[hybridqp.QueryNode][]hybridqp.QueryNode) bool {

	if operand.Policy() == AFTER {
		for _, node := range node.Children() {
			_, ok := node.(*HeuVertex)
			if !ok {
				return false
			}
		}

		nodes.PushBack(node)

		vertex, ok := p.Vertex(node)
		if !ok {
			return false
		}
		parents := p.dag.AllParents(vertex)
		if len(parents) == 0 {
			return false
		}
		if operand.Matches(parents[0].Node()) {
			return true
		}
		return false
	}

	if !operand.Matches(node) {
		return false
	}

	for _, node := range node.Children() {
		_, ok := node.(*HeuVertex)
		if !ok {
			return false
		}
	}

	nodes.PushBack(node)
	children := node.Children()

	switch operand.Policy() {
	case LEAF:
		if len(children) > 0 {
			return false
		}
		return true
	case ANY:
		return true
	case WILDCARD:
		operandChildren := operand.Children()
		for _, operandChild := range operandChildren {
			match := false
			for _, child := range children {
				midChild := child.Clone()
				for {
					matchChild := p.matchOperands(operandChild, midChild.(*HeuVertex).Node(), nodes, nodeChildren)
					if matchChild {
						match = true
						break
					}
					if midChild.Children() == nil {
						break
					}
					midChild = midChild.Children()[0]
				}
			}
			if !match {
				return false
			}
		}
		return true
	case UNORDERED:
		operandChildren := operand.Children()
		for _, operandChild := range operandChildren {
			match := false
			for _, child := range children {
				match = p.matchOperands(operandChild, child.(*HeuVertex).Node(), nodes, nodeChildren)
				if match {
					break
				}
			}

			if !match {
				return false
			}
		}
		nodes := make([]hybridqp.QueryNode, 0, len(children))
		for _, child := range children {
			nodes = append(nodes, child.(*HeuVertex).Node())
		}
		nodeChildren[node] = nodes
		return true
	default:
		operandChildren := operand.Children()
		if len(children) < len(operandChildren) {
			return false
		}

		for i, operandChild := range operandChildren {
			match := p.matchOperands(operandChild, children[i].(*HeuVertex).Node(), nodes, nodeChildren)
			if !match {
				return false
			}
		}

		return true
	}
}

func (p *HeuPlannerImpl) depthFirstApply(iter *GraphIterator, ruleSet RuleSet, forceConversions bool, nMatches int) int {
	for {
		if !iter.HasNext() {
			break
		}
		vertex := iter.Next()
		for rule := range ruleSet {
			newVertex := p.applyRule(rule, vertex, forceConversions)
			if newVertex == nil || newVertex.Equals(vertex) {
				continue
			}
			nMatches++
			if nMatches >= p.currentProgram.matchLimit {
				return nMatches
			}
			depthIter := p.dag.GetGraphIterator(newVertex, p.currentProgram.matchOrder)
			nMatches = p.depthFirstApply(depthIter, ruleSet, forceConversions, nMatches)
			break
		}
	}
	return nMatches
}

func (p *HeuPlannerImpl) executeBeginGroup(group *BeginGroup) {
	panic("impl me")
}

func (p *HeuPlannerImpl) executeEndGroup(group *EndGroup) {
	panic("impl me")
}

func (p *HeuPlannerImpl) executeProgram(program *HeuProgram) {
	savedProgram := p.currentProgram
	p.currentProgram = program
	p.currentProgram.Initialize(program == p.mainProgram)

	for _, instruction := range p.currentProgram.instructions {
		instruction.Execute(p)
	}

	p.currentProgram = savedProgram
}

func (p *HeuPlannerImpl) Visit(node hybridqp.QueryNode) hybridqp.QueryNodeVisitor {
	node.DeriveOperations()
	return p
}

func IsSubTreeEqual(node hybridqp.QueryNode, comparedNode hybridqp.QueryNode) bool {
	if node.Digest() != comparedNode.Digest() {
		return false
	} else {
		if node.Children() != nil && comparedNode.Children() != nil {
			return node.Children()[0].(*HeuVertex).SubTreeEqual(comparedNode.Children()[0].(*HeuVertex))
		} else if node.Children() == nil && comparedNode.Children() == nil {
			return true
		} else {
			return false
		}
	}
}
