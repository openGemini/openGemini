package gramIndex

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
)

type IndexTreeNode struct {
	data          string
	children      map[uint8]*IndexTreeNode
	isleaf        bool
	invertedIndex Inverted_index
	addrOffset    map[*IndexTreeNode]uint16
}

func (root *IndexTreeNode) AddrOffset() map[*IndexTreeNode]uint16 {
	return root.addrOffset
}

func (root *IndexTreeNode) SetAddrOffset(addrOffset map[*IndexTreeNode]uint16) {
	root.addrOffset = addrOffset
}

func (root *IndexTreeNode) Data() string {
	return root.data
}

func (root *IndexTreeNode) SetData(data string) {
	root.data = data
}

func (root *IndexTreeNode) Children() map[uint8]*IndexTreeNode {
	return root.children
}

func (root *IndexTreeNode) SetChildren(children map[uint8]*IndexTreeNode) {
	root.children = children
}

func (root *IndexTreeNode) Isleaf() bool {
	return root.isleaf
}

func (root *IndexTreeNode) SetIsleaf(isleaf bool) {
	root.isleaf = isleaf
}

func (root *IndexTreeNode) InvertedIndex() Inverted_index {
	return root.invertedIndex
}

func (root *IndexTreeNode) SetInvertedIndex(invertedIndex Inverted_index) {
	root.invertedIndex = invertedIndex
}

func NewIndexTreeNode(data string) *IndexTreeNode {
	return &IndexTreeNode{
		data:          data,
		isleaf:        false,
		children:      make(map[uint8]*IndexTreeNode),
		invertedIndex: make(map[utils.SeriesId][]uint16),
		addrOffset:    make(map[*IndexTreeNode]uint16),
	}
}

//Determine whether children have this node
func GetIndexNode(children map[uint8]*IndexTreeNode, char uint8) int8 {
	if children[char] != nil {
		return int8(char)
	}
	return -1
}

func (node *IndexTreeNode) InsertPosArrToInvertedIndexMap(sid utils.SeriesId, position uint16) {
	//Find the invertedIndex of sid in the inverted listArray, and add position to the posArray in the invertedIndex
	node.invertedIndex[sid] = append(node.invertedIndex[sid], position)
}

//Insert a new inverted structure
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
	fmt.Print(node.data, " - ", " - ", node.isleaf, " - ", node.invertedIndex, " - ", node.addrOffset) //, node.frequency
	for _, child := range node.children {
		child.PrintIndexTreeNode(level + 1)
	}
}
