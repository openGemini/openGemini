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

import (
	"fmt"
	"sort"

	"github.com/openGemini/openGemini/lib/utils"
)

type TrieTreeNode struct {
	data      string
	frequency int
	children  map[int]*TrieTreeNode
	isleaf    bool
}

func (t *TrieTreeNode) Children() map[int]*TrieTreeNode {
	return t.children
}

func (t *TrieTreeNode) SetChildren(children map[int]*TrieTreeNode) {
	t.children = children
}

func (t *TrieTreeNode) Data() string {
	return t.data
}

func (t *TrieTreeNode) SetData(data string) {
	t.data = data
}

func (t *TrieTreeNode) Frequency() int {
	return t.frequency
}

func (t *TrieTreeNode) SetFrequency(frequency int) {
	t.frequency = frequency
}

func (t *TrieTreeNode) Isleaf() bool {
	return t.isleaf
}

func (t *TrieTreeNode) SetIsleaf(isleaf bool) {
	t.isleaf = isleaf
}

func NewTrieTreeNode(data string) *TrieTreeNode {
	return &TrieTreeNode{
		data:      data,
		frequency: 1,
		isleaf:    false,
		children:  make(map[int]*TrieTreeNode),
	}
}

func (node *TrieTreeNode) PruneNode(T int) {
	if !node.isleaf {
		for _, child := range node.children {
			child.PruneNode(T)
		}
	} else {
		if node.frequency <= T {
			node.PruneStrategyLessT()
		} else {
			node.PruneStrategyMoreT(T)
		}
	}
}

func (node *TrieTreeNode) PruneStrategyLessT() {
	node.children = make(map[int]*TrieTreeNode)
}

func (node *TrieTreeNode) PruneStrategyMoreT(T int) {
	var freqList = make([]FreqList, len(node.children))
	k := 0
	for _, child := range node.children {
		freqList[k].token = child.data
		freqList[k].freq = child.frequency
		k++
	}
	sort.SliceStable(freqList, func(i, j int) bool {
		if freqList[i].freq < freqList[j].freq {
			return true
		}
		return false
	})
	totoalSum := 0
	for i := k - 1; i >= 0; i-- {
		//从大到小遍历数组
		if totoalSum+freqList[i].freq <= T {
			totoalSum = totoalSum + freqList[i].freq
			var index int
			if freqList[i].token != "" {
				index = utils.StringToHashCode(freqList[i].token)
			}
			if node.children[index] != nil {
				delete(node.children, index)
			}
		}
	}
	for _, child := range node.children {
		child.PruneNode(T)
	}
}

func getNode(children map[int]*TrieTreeNode, str string) int {
	if children[utils.StringToHashCode(str)] != nil {
		return utils.StringToHashCode(str)
	}
	return -1
}

// 输出以node为根的子树
func (node *TrieTreeNode) PrintTreeNode(level int) {
	fmt.Println()
	for i := 0; i < level; i++ {
		fmt.Print("      ")
	}
	fmt.Print(node.data, " - ", node.frequency, " - ", node.isleaf)
	for _, child := range node.children {
		child.PrintTreeNode(level + 1)
	}
}
