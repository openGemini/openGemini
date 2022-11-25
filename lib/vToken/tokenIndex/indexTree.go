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

package tokenIndex

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type IndexTree struct {
	qmin int
	qmax int
	cout int
	root *IndexTreeNode
}

func (i *IndexTree) Qmin() int {
	return i.qmin
}

func (i *IndexTree) SetQmin(qmin int) {
	i.qmin = qmin
}

func (i *IndexTree) Qmax() int {
	return i.qmax
}

func (i *IndexTree) SetQmax(qmax int) {
	i.qmax = qmax
}

func (i *IndexTree) Cout() int {
	return i.cout
}

func (i *IndexTree) SetCout(cout int) {
	i.cout = cout
}

func (i *IndexTree) Root() *IndexTreeNode {
	return i.root
}

func (i *IndexTree) SetRoot(root *IndexTreeNode) {
	i.root = root
}

// 初始化trieTree
func NewIndexTree(qmin int, qmax int) *IndexTree {
	return &IndexTree{
		qmin: qmin,
		qmax: qmax,
		cout: 0,
		root: NewIndexTreeNode(""),
	}
}

// 08年
func NewIndexTrie08(qmin int) *IndexTree {
	return &IndexTree{
		qmin: qmin,
		cout: 0,
		root: NewIndexTreeNode(""),
	}
}

func (tree *IndexTree) InsertIntoIndexTree(token []string, inverted_index Inverted_index) *IndexTreeNode {
	node := tree.root
	var childIndex = -1
	var addr *IndexTreeNode
	for i, str := range token {
		childIndex = GetIndexNode(node.children, token[i])
		if childIndex == -1 {
			currentNode := NewIndexTreeNode(str)
			node.children[utils.StringToHashCode(str)] = currentNode
			node = currentNode
		} else {
			node = node.children[childIndex]
		}
		if i == len(token)-1 {
			node.isleaf = true
			node.invertedIndex = inverted_index
			addr = node
		}
	}
	return addr
}

func (tree *IndexTree) InsertOnlyTokenIntoIndexTree(tokenSubs []SubTokenOffset, addr *IndexTreeNode) {
	var childIndex = -1
	for k := 0; k < len(tokenSubs); k++ {
		token := tokenSubs[k].subToken
		offset := tokenSubs[k].offset
		node := tree.root
		for i, str := range token {
			childIndex = GetIndexNode(node.children, token[i])
			if childIndex == -1 {
				currentNode := NewIndexTreeNode(str)
				node.children[utils.StringToHashCode(str)] = currentNode
				node = currentNode
			} else {
				node = node.children[childIndex]
			}
			if i == len(token)-1 {
				node.isleaf = true
				if _, ok := node.addrOffset[addr]; !ok {
					node.addrOffset[addr] = offset
				}
			}
		}
	}
}

func (tree *IndexTree) InsertTokensIntoIndexTree08(token *[]string, sid utils.SeriesId, position uint16) {
	node := tree.root
	var childindex = 0
	for i, str := range *token {
		childindex = GetIndexNode(node.children, (*token)[i])
		if childindex == -1 {
			currentnode := NewIndexTreeNode(str)
			node.children[utils.StringToHashCode(str)] = currentnode
			node = currentnode
		} else {
			node = node.children[childindex]
		}
		if i == len(*token)-1 {
			node.isleaf = true
			if _, ok := node.invertedIndex[sid]; !ok {
				node.InsertSidAndPosArrToInvertedIndexMap(sid, position)
			} else {
				node.InsertPosArrToInvertedIndexMap(sid, position)
			}
		}
	}
}

func (tree *IndexTree) PrintIndexTree() {
	tree.root.PrintIndexTreeNode(0)
}

var Res []int
var Rea []int

func (root *IndexTreeNode) FixInvertedIndexSize() {
	for _, child := range root.children {
		if child.isleaf == true && len(child.invertedIndex) > 0 {
			Res = append(Res, len(child.invertedIndex))
		}
		child.FixInvertedIndexSize()
	}
}

func (root *IndexTreeNode) FixInvertedAddrSize() {
	for _, child := range root.children {
		if child.isleaf == true && len(child.addrOffset) > 0 {
			Rea = append(Rea, len(child.addrOffset)) //The append function must be used, and i cannot be used for variable addition, because there is no make initialization
		}
		child.FixInvertedAddrSize()
	}
}

var Grams [][]string
var temp []string
var SumInvertLen = 0

func (root *IndexTreeNode) SearchGramsFromIndexTree() {
	if len(root.children) == 0 {
		return
	}
	for _, child := range root.children {
		temp = append(temp, child.data)
		if child.isleaf == true {
			for j := 0; j < len(temp); j++ {
				val := temp[j]
				SumInvertLen += len(val)
			}
			Grams = append(Grams, temp)
		}
		child.SearchGramsFromIndexTree()
		if len(temp) > 0 {
			temp = temp[:len(temp)-1]
		}
	}
}
