package gramClvc

import (
	"fmt"
	"sort"
)

//Dictionary node information
type TrieTreeNode struct {
	data      string
	frequency int
	children  map[uint8]*TrieTreeNode
	isleaf    bool
}

func (node *TrieTreeNode) Data() string {
	return node.data
}

func (node *TrieTreeNode) SetData(data string) {
	node.data = data
}

func (node *TrieTreeNode) Frequency() int {
	return node.frequency
}

func (node *TrieTreeNode) SetFrequency(frequency int) {
	node.frequency = frequency
}

func (node *TrieTreeNode) Children() map[uint8]*TrieTreeNode {
	return node.children
}

func (node *TrieTreeNode) SetChildren(children map[uint8]*TrieTreeNode) {
	node.children = children
}

func (node *TrieTreeNode) Isleaf() bool {
	return node.isleaf
}

func (node *TrieTreeNode) SetIsleaf(isleaf bool) {
	node.isleaf = isleaf
}

func NewTrieTreeNode(data string) *TrieTreeNode {
	return &TrieTreeNode{
		data:      data,
		frequency: 1,
		isleaf:    false,
		children:  make(map[uint8]*TrieTreeNode),
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

//Pruning strategy1: freq <= T
func (node *TrieTreeNode) PruneStrategyLessT() {
	node.children = make(map[uint8]*TrieTreeNode)
}

//Pruning strategy2: freq > T
//Prune the largest subset, then recursively prune the tree
func (node *TrieTreeNode) PruneStrategyMoreT(T int) {
	var freqList = make([]FreqList, 128)
	k := 0
	for _, child := range node.children {
		freqList[k].char = child.data
		freqList[k].freq = child.frequency
		k++
	}
	sort.SliceStable(freqList, func(i, j int) bool {
		if freqList[i].freq < freqList[j].freq {
			return true
		}
		return false
	})
	var totoalSum = 0
	for i := k - 1; i >= 0; i-- {
		//Traverse freqList from largest to smallest
		if totoalSum+freqList[i].freq <= T {
			totoalSum = totoalSum + freqList[i].freq
			var index uint8
			if freqList[i].char != "" {
				index = freqList[i].char[0]
			}
			if node.children[index] != nil {
				delete(node.children, index)
			}
		}
	}
	// recursively prune the tree
	for _, child := range node.children {
		child.PruneNode(T)
	}
}

// Determine whether children have this node, key is the character ASCII code value
func GetNode(children map[uint8]*TrieTreeNode, char uint8) int8 {
	if children[char] != nil {
		return int8(char)
	}
	return -1
}

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
