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
package gramClvl

import (
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
)

func GeneratorTw(sampleStringOfW map[utils.SeriesId]string, qmin int, trie *gramIndex.IndexTree) *gramIndex.IndexTree {
	treeW := gramIndex.NewIndexTrie(trie.Qmin())
	var vgMaps = make(map[string]gramIndex.Inverted_index)
	for key, value := range sampleStringOfW {
		var vgMap map[uint16]string
		vgMap = make(map[uint16]string)
		log := value
		tsid := key.Id
		timeStamp := key.Time
		sid := utils.NewSeriesId(tsid, timeStamp)
		gramIndex.VGConsBasicIndex08(trie.Root(), trie.Qmin(), log, vgMap)
		vgMaps = gramIndex.WriteToVgMaps(vgMap, sid, vgMaps)
	}
	var addr *gramIndex.IndexTreeNode
	for gram := range vgMaps {
		invert_index := vgMaps[gram]
		addr = treeW.InsertIntoIndexTree(gram, invert_index)
		if len(gram) > qmin { //Generate all gramIndex entries between qmin+1 - len(gram)
			gramIndex.GramSubs = make([]gramIndex.SubGramOffset, 0)
			gramIndex.GenerateQmin2QmaxGrams(gram, qmin)
			treeW.InsertOnlyGramIntoIndexTree(gramIndex.GramSubs, addr)
		}
	}
	treeW.SetCout(len(sampleStringOfW))
	return treeW
}

func CalculateListLengh(treeW *gramIndex.IndexTree) int {
	posListLength := make([]int, 0)
	CalculateListLenghNode(treeW.Root(), 0, &posListLength)
	sum := 0
	for i := 0; i < len(posListLength); i++ {
		sum = sum + posListLength[i]
	}
	return sum
}

func CalculateListLenghNode(node *gramIndex.IndexTreeNode, level int, posList *[]int) {
	for _, posArray := range node.InvertedIndex() {
		*posList = append(*posList, len(posArray))
	}
	for _, child := range node.Children() {
		CalculateListLenghNode(child, level+1, posList)
	}
}

func CalculateNodeListLength(node *gramIndex.IndexTreeNode) int {
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

func FindGInTreeW(treeW *gramIndex.IndexTree, gram *[]string) *gramIndex.IndexTreeNode {
	node := treeW.Root()
	var childindex int8 = 0

	for i, _ := range *gram {
		childindex = HasIndexNode(node.Children(), (*gram)[i])
		if childindex == -1 {
			// childrenlist里没有该节点,找不到
			return nil
		} else {
			node = node.Children()[uint8(childindex)]
		}
	}
	if node.Isleaf() == false {
		return nil
	}
	return node
}

func StrToStrArray(s string) *[]string {
	gramArray := make([]string, len(s))
	for j := 0; j < len(s); j++ {
		gramArray[j] = s[j : j+1]
	}
	return &gramArray
}
