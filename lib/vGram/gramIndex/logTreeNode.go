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
