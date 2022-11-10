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
package tokenClvc

import "github.com/openGemini/openGemini/lib/utils"

type TrieTree struct {
	qmin int
	qmax int
	root *TrieTreeNode
}

func (t *TrieTree) Qmin() int {
	return t.qmin
}

func (t *TrieTree) SetQmin(qmin int) {
	t.qmin = qmin
}

func (t *TrieTree) Qmax() int {
	return t.qmax
}

func (t *TrieTree) SetQmax(qmax int) {
	t.qmax = qmax
}

func (t *TrieTree) Root() *TrieTreeNode {
	return t.root
}

func (t *TrieTree) SetRoot(root *TrieTreeNode) {
	t.root = root
}

func NewTrieTree(qmin int, qmax int) *TrieTree {
	return &TrieTree{
		qmin: qmin,
		qmax: qmax,
		root: NewTrieTreeNode(""),
	}
}

func (tree *TrieTree) InsertIntoTrieTree(substring *[]string) {
	node := tree.root
	qmin := tree.qmin
	var childindex = -1
	for i, str := range *substring {
		childindex = getNode(node.children, (*substring)[i])
		if childindex == -1 {
			currentnode := NewTrieTreeNode(str)
			node.children[utils.StringToHashCode(str)] = currentnode
			node = currentnode
		} else {
			node = node.children[childindex]
			node.frequency++
		}
		if i >= qmin-1 {
			node.isleaf = true
		}
	}
}

func (tree *TrieTree) PruneTree(T int) {
	tree.root.PruneNode(T)
}

func (tree *TrieTree) PrintTree() {
	tree.root.PrintTreeNode(0)
}

func (tree *TrieTree) UpdateRootFrequency() {
	for _, child := range tree.root.children {
		tree.root.frequency += child.frequency
	}
	tree.root.frequency--
}
