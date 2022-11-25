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

package gramIndex

type LogTree struct {
	qmax int
	root *LogTreeNode
}

func (tree *LogTree) Qmax() int {
	return tree.qmax
}

func (tree *LogTree) SetQmax(qmax int) {
	tree.qmax = qmax
}

func (tree *LogTree) Root() *LogTreeNode {
	return tree.root
}

func (tree *LogTree) SetRoot(root *LogTreeNode) {
	tree.root = root
}

func NewLogTree(qmax int) *LogTree {
	return &LogTree{
		qmax: qmax,
		root: NewLogTreeNode(""),
	}
}

func (tree *LogTree) InsertIntoTrieTreeLogTree(gram string) {
	node := tree.root
	var childIndex int8 = -1
	for i := 0; i < len(gram); i++ {
		childIndex = GetNode(node.children, gram[i])
		if childIndex == -1 {
			currentNode := NewLogTreeNode(string(gram[i]))
			node.children[gram[i]] = currentNode
			node = currentNode
		} else {
			node = node.children[uint8(childIndex)]
		}
	}
}

func (tree *LogTree) PrintTree() {
	tree.root.PrintTreeNode(0)
}
