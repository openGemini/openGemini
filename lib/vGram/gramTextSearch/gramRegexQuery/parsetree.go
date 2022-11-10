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
package gramRegexQuery

import (
	"fmt"
	"os"
	"strconv"
)

const op = "|+*()?{}.[]"
const op1 = "|*&()#?+."

type ParseTreeNode struct {
	isoperator bool
	value      string
	lchild     *ParseTreeNode
	rchild     *ParseTreeNode
}

func NewParseTreeNode(value string, isoperator bool, lchild *ParseTreeNode, rchild *ParseTreeNode) *ParseTreeNode {
	return &ParseTreeNode{
		isoperator: isoperator,
		value:      value,
		lchild:     lchild,
		rchild:     rchild,
	}
}

func GenerateParseTree(regex string) *ParseTreeNode {
	return Compile(Split(regex))
}

func Split(regex string) *[]*ParseTreeNode {
	length := len(regex)
	part := ""
	// result set
	expression := make([]*ParseTreeNode, 0)
	for i := 0; i < length; i++ {
		if !CheckOP(regex[i]) {
			if part == "" && i >= 1 && (regex[i-1] != '(' && regex[i-1] != '|' && regex[i-1] != '{') {
				expression = append(expression, NewParseTreeNode("&", true, nil, nil))
			}
			// deal with '\' such as '\d' '\w' ...
			if regex[i] == '\\' && i < len(regex)-1 {
				if CheckOP(regex[i+1]) || CheckESC(regex[i+1]) {
					part = part + string(regex[i+1])
					i++
				} else if regex[i+1] == '\\' {
					part = part + string('\\')
					i++
					break
				} else if regex[i+1] == 's' {
					part = part + string('~')
					i++
					break
				} else if regex[i+1] == 'd' {
					expression = append(expression, NewParseTreeNode("(", true, nil, nil))
					for i := 0; i < 9; i++ {
						digit := strconv.Itoa(i)
						expression = append(expression, NewParseTreeNode(digit, false, nil, nil))
						expression = append(expression, NewParseTreeNode("|", true, nil, nil))
					}
					nine := strconv.Itoa(9)
					expression = append(expression, NewParseTreeNode(nine, false, nil, nil))
					regex = Replacei(regex, i+1, ')')
					break
				} else if regex[i+1] == 'w' {
					expression = append(expression, NewParseTreeNode("(", true, nil, nil))
					for i := 0; i < 10; i++ {
						digit := strconv.Itoa(i)
						expression = append(expression, NewParseTreeNode(digit, false, nil, nil))
						expression = append(expression, NewParseTreeNode("|", true, nil, nil))
					}
					for i := 97; i < 123; i++ {
						char := strconv.Itoa(i)
						expression = append(expression, NewParseTreeNode(char, false, nil, nil))
						expression = append(expression, NewParseTreeNode("|", true, nil, nil))
					}
					for i := 65; i < 91; i++ {
						char := strconv.Itoa(i)
						expression = append(expression, NewParseTreeNode(char, false, nil, nil))
						expression = append(expression, NewParseTreeNode("|", true, nil, nil))
					}
					expression = append(expression, NewParseTreeNode("_", false, nil, nil))
					regex = Replacei(regex, i+1, ')')
				}
			} else {
				part = part + string(regex[i])
			}
			// rewrite or not
			if Rewrite() {
				for i := 0; i < len(part); i++ {
					ascii := int(part[i])
					if ascii < 33 {
						Replacei(part, i, '~')
					}
					if part[i] != '#' && ((ascii > 32 && ascii < 44) || (ascii > 57 && ascii < 64)) {
						Replacei(part, i, '*')
					}
				}
			}
		} else {
			if len(part) != 0 {
				expression = append(expression, NewParseTreeNode(part, false, nil, nil))
				part = ""
			}
			if (regex[i] == '(' || regex[i] == '.' || regex[i] == '[') && i >= 1 && (regex[i-1] != '|' && regex[i-1] != '(') {
				expression = append(expression, NewParseTreeNode("&", true, nil, nil))
			}
			if regex[i] == '[' {
				expression = append(expression, NewParseTreeNode("(", true, nil, nil))
				i++
				tt := 0
				prevs := 0
				flag := false
				// operate "[]"
				for regex[i] != ']' {
					if regex[i] == '-' {
						flag = true
					} else {
						if flag == true {
							// check regex
							up := int(regex[i])
							if up < prevs {
								fmt.Println("Incorrect syntax")
								os.Exit(1)
							} else {
								low := prevs + 1
								for low < up {
									expression = append(expression, NewParseTreeNode(strconv.Itoa(low), false, nil, nil))
									expression = append(expression, NewParseTreeNode("|", true, nil, nil))
									low++
								}
							}
						}
						expression = append(expression, NewParseTreeNode(string(regex[i]), false, nil, nil))
						prevs = int(regex[i])
						flag = false
						expression = append(expression, NewParseTreeNode("|", true, nil, nil))
						tt++
					}
					i++
				}
				if tt > 0 {
					// delete the last element : "|"
					expression = expression[:len(expression)-1]
				}
				if regex[i] == ']' {
					Replacei(regex, i, ')')
				}
			}
			expression = append(expression, NewParseTreeNode(string(regex[i]), true, nil, nil))
		}

	}
	if len(part) != 0 {
		expression = append(expression, NewParseTreeNode(part, false, nil, nil))
	}
	return &expression

}

func Compile(expression *[]*ParseTreeNode) *ParseTreeNode {

	OPTR := InitializeParseTreeStack()
	OPND := InitializeParseTreeStack()
	i := 0
	*expression = append(*expression, NewParseTreeNode("#", true, nil, nil))
	OPTR.Push(NewParseTreeNode("#", true, nil, nil))
	example := (*expression)[i]
	i++
	for example.value != "#" || OPTR.Top().value != "#" {
		if example.isoperator == false {
			OPND.Push(example)
			example = (*expression)[i]
			i++
		} else {
			temp := OPTR.Top()
			result := Compare(temp.value, example.value)
			if result == '<' {
				if example.value[0] == '*' {
					temp = OPND.Pop()
					//Parameters::isSimple=false
					if temp.value == "#" {
						OPND.Push(NewParseTreeNode("#", false, nil, nil))
					} else {
						//Parameters::avoidVerification=false
						OPND.Push(NewParseTreeNode("*", true, temp, nil))
					}
				} else if example.value[0] == '+' {
					temp = OPND.Pop()
					OPND.Push(NewParseTreeNode("+", true, temp, nil))
					//Parameters::isSimple=false
				} else if example.value[0] == '?' {
					temp = OPND.Pop()
					OPND.Push(NewParseTreeNode("?", true, temp, nil))
				} else if example.value[0] == '.' {
					OPND.Push(example)
				} else {
					OPTR.Push(example)
				}
				example = (*expression)[i]
				i++
			} else if result == '=' {
				OPTR.Pop()
				example = (*expression)[i]
				i++
			} else if result == '>' {
				temp = OPTR.Pop()
				if temp.value[0] == '|' {
					b := OPND.Pop()
					a := OPND.Pop()
					OPND.Push(NewParseTreeNode("|", true, a, b))
				} else if temp.value[0] == '&' {
					b := OPND.Pop()
					a := OPND.Pop()
					OPND.Push(NewParseTreeNode("&", true, a, b))
				}
			} else {
				fmt.Println("Incorrect syntax")
				os.Exit(1)
			}

		}
	}
	root := OPND.Pop()
	return root
}

func CheckOP(c uint8) bool {
	size := len(op)
	for i := 0; i < size; i++ {
		if op[i] == c {
			return true
		}
	}
	return false
}

func CheckESC(c uint8) bool {
	if c == '\\' {
		return true
	} else {
		return false
	}
}

func Rewrite() bool {
	return true
}

func Compare(a string, b string) uint8 {
	opblock := [10][10]uint8{
		{'>', '<', '<', '<', '>', '>', '<', '<', '<'},
		{'>', '>', '>', '>', '>', '>', '>', '>', '<'},
		{'>', '<', '>', '<', '>', '>', '<', '<', '<'},
		{'<', '<', '<', '<', '=', ' ', '<', '<', '<'},
		{'>', '>', '>', ' ', '>', '>', '>', '>', '<'},
		{'<', '<', '<', '<', ' ', '=', '<', '<', '<'},
		{'>', '<', '>', '>', '>', '>', '>', '<', '<'},
		{'>', '>', '>', '>', '>', '>', '>', '>', '<'},
		{'>', '>', '>', '>', '>', '>', '>', '>', '<'},
	}
	var row int
	var column int
	for i := 0; i < len(op1); i++ {
		if string(op1[i]) == a {
			row = i
		}
		if string(op1[i]) == b {
			column = i
		}
	}
	return opblock[row][column]
}

func (node *ParseTreeNode) PrintTree(level int) {
	if node.lchild != nil {
		node.lchild.PrintTree(level + 1)
	}
	for i := 0; i < level; i++ {
		fmt.Print(" ")
	}
	fmt.Println(node.value)
	if node.rchild != nil {
		node.rchild.PrintTree(level + 1)
	}
}
