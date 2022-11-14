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
	"fmt"

	"github.com/openGemini/openGemini/lib/utils"
)

type IndexTreeNode struct {
	data          string
	children      map[int]*IndexTreeNode
	isleaf        bool
	invertedIndex Inverted_index
	addrOffset    map[*IndexTreeNode]uint16
}

func (node *IndexTreeNode) Children() map[int]*IndexTreeNode {
	return node.children
}

func (node *IndexTreeNode) SetChildren(children map[int]*IndexTreeNode) {
	node.children = children
}

func (node *IndexTreeNode) Data() string {
	return node.data
}

func (node *IndexTreeNode) SetData(data string) {
	node.data = data
}

func (node *IndexTreeNode) Isleaf() bool {
	return node.isleaf
}

func (node *IndexTreeNode) SetIsleaf(isleaf bool) {
	node.isleaf = isleaf
}

func (node *IndexTreeNode) InvertedIndex() Inverted_index {
	return node.invertedIndex
}

func (node *IndexTreeNode) SetInvertedIndex(invertedIndex Inverted_index) {
	node.invertedIndex = invertedIndex
}

func (node *IndexTreeNode) AddrOffset() map[*IndexTreeNode]uint16 {
	return node.addrOffset
}

func (node *IndexTreeNode) SetAddrOffset(addrOffset map[*IndexTreeNode]uint16) {
	node.addrOffset = addrOffset
}

func NewIndexTreeNode(data string) *IndexTreeNode {
	return &IndexTreeNode{
		data:          data,
		isleaf:        false,
		children:      make(map[int]*IndexTreeNode),
		invertedIndex: make(map[utils.SeriesId][]uint16),
		addrOffset:    make(map[*IndexTreeNode]uint16),
	}
}

func GetIndexNode(children map[int]*IndexTreeNode, str string) int {
	if children[utils.StringToHashCode(str)] != nil {
		return utils.StringToHashCode(str)
	}
	return -1
}

func (node *IndexTreeNode) InsertPosArrToInvertedIndexMap(sid utils.SeriesId, position uint16) {
	//倒排列表数组中找到sid的invertedIndex，把position加入到invertedIndex中的posArray中去
	node.invertedIndex[sid] = append(node.invertedIndex[sid], position)
}

func (node *IndexTreeNode) InsertSidAndPosArrToInvertedIndexMap(sid utils.SeriesId, position uint16) {
	posArray := []uint16{}
	posArray = append(posArray, position)
	node.invertedIndex[sid] = posArray
}

func (node *IndexTreeNode) PrintIndexTreeNode(level int) {
	fmt.Println()
	for i := 0; i < level; i++ {
		fmt.Print("      ")
	}
	fmt.Print(node.data, " - ", " - ", node.isleaf, " - ", node.invertedIndex, " - ", node.addrOffset)
	for _, child := range node.children {
		child.PrintIndexTreeNode(level + 1)
	}
}

func InsertInvertedIndexPos(invertedIndex Inverted_index, sid utils.SeriesId, position uint16) {
	//倒排列表数组中找到sid的invertedIndex，把position加入到invertedIndex中的posArray中去
	invertedIndex[sid] = append(invertedIndex[sid], position)
}

func InsertInvertedIndexList(node *IndexTreeNode, sid utils.SeriesId, position uint16) {
	posArray := []uint16{}
	posArray = append(posArray, position)
	node.invertedIndex[sid] = posArray
}
