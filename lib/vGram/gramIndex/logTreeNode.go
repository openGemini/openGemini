package gramIndex

import "fmt"

type LogTreeNode struct {
	data     string
	children map[uint8]*LogTreeNode
}

func (node *LogTreeNode) Data() string {
	return node.data
}

func (node *LogTreeNode) SetData(data string) {
	node.data = data
}

func (node *LogTreeNode) Children() map[uint8]*LogTreeNode {
	return node.children
}

func (node *LogTreeNode) SetChildren(children map[uint8]*LogTreeNode) {
	node.children = children
}

func NewLogTreeNode(data string) *LogTreeNode {
	return &LogTreeNode{
		data:     data,
		children: make(map[uint8]*LogTreeNode),
	}
}

func GetNode(children map[uint8]*LogTreeNode, char uint8) int8 {
	if children[char] != nil {
		return int8(char)
	}
	return -1
}

func (node *LogTreeNode) PrintTreeNode(level int) {
	fmt.Println()
	for i := 0; i < level; i++ {
		fmt.Print("        ")
	}
	fmt.Print(node.data, " - ")
	for _, child := range node.children {
		child.PrintTreeNode(level + 1)
	}
}
