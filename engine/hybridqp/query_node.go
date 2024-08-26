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

package hybridqp

import (
	"sync/atomic"
)

var (
	gNodeId uint64 = 0
)

func GenerateNodeId() uint64 {
	id := atomic.AddUint64(&gNodeId, 1)
	return id
}

type Trait interface{}

type QueryNode interface {
	ID() uint64
	Digest() string
	Children() []QueryNode
	String() string
	Type() string
	ReplaceChild(int, QueryNode)
	ReplaceChildren([]QueryNode)
	Clone() QueryNode
	RowDataType() RowDataType
	RowExprOptions() []ExprOptions
	Dummy() bool
	DeriveOperations()
	Schema() Catalog
	Trait() Trait
	ApplyTrait(Trait)

	SetInputs([]QueryNode)
	SetSchema(Catalog)
	New(inputs []QueryNode, schema Catalog, eTrait []Trait) QueryNode
}

type QueryNodeVisitor interface {
	Visit(QueryNode) QueryNodeVisitor
}

func WalkQueryNodeInPreOrder(v QueryNodeVisitor, node QueryNode) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	for _, child := range node.Children() {
		WalkQueryNodeInPreOrder(v, child)
	}
}

func WalkQueryNodeInPostOrder(v QueryNodeVisitor, node QueryNode) {
	if node == nil {
		return
	}

	for _, child := range node.Children() {
		WalkQueryNodeInPostOrder(v, child)
	}

	if v = v.Visit(node); v == nil {
		return
	}
}

type FlattenQueryNodeVisitor struct {
	nodes []QueryNode
}

func (visitor *FlattenQueryNodeVisitor) Visit(node QueryNode) QueryNodeVisitor {
	visitor.nodes = append(visitor.nodes, node)
	return visitor
}

func (visitor *FlattenQueryNodeVisitor) Nodes() []QueryNode {
	return visitor.nodes
}
