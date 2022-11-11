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
package tokenClvl

import (
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
)

func CalculateListLengh(treeW *tokenIndex.IndexTree) int {
	posListLength := make([]int, 0)
	CalculateListLenghNode(treeW.Root(), 0, &posListLength)
	sum := 0
	for i := 0; i < len(posListLength); i++ {
		sum = sum + posListLength[i]
	}
	return sum
}

func CalculateListLenghNode(node *tokenIndex.IndexTreeNode, level int, posList *[]int) {
	for _, posArray := range node.InvertedIndex() {
		*posList = append(*posList, len(posArray))
	}
	for _, child := range node.Children() {
		CalculateListLenghNode(child, level+1, posList)
	}
}

func CalculateNodeListLength(node *tokenIndex.IndexTreeNode) int {
	if node == nil {
		return 0
	}
	list := node.InvertedIndex()
	length := 0
	if list == nil {
		return 0
	} else {
		for _, posArray := range list {
			length += len(posArray)
		}

	}
	return length
}

func FindGInTreeW(treeW *tokenIndex.IndexTree, gram *[]string) *tokenIndex.IndexTreeNode {
	node := treeW.Root()
	var childindex = 0
	for i, _ := range *gram {
		childindex = tokenIndex.GetIndexNode(node.Children(), (*gram)[i])
		if childindex == -1 {
			return nil
		} else {
			node = node.Children()[childindex]
		}
	}
	if node.Isleaf() == false {
		return nil
	}
	return node
}
