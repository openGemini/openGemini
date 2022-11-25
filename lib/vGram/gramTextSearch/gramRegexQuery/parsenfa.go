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
	"strconv"

	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
)

type Transition struct {
	node *ParseNfaNode
	edge *ParseNfaEdge
}

type ParseNfaNode struct {
	nexts []*Transition
}

type ParseNfaEdge struct {
	label    string
	epsilon  bool
	wildcard bool
	checked  bool
	start    *ParseNfaNode
	end      *ParseNfaNode
}

type Nfa struct {
	nodes []*ParseNfaNode
	edges []*ParseNfaEdge
	inode *ParseNfaNode
	fnode *ParseNfaNode
}

type SubNfa struct {
	start *ParseNfaNode
	end   *ParseNfaNode
}

type SubPath struct {
	nodes []*ParseNfaNode
	str   string
}

func NewTransition(node *ParseNfaNode, edge *ParseNfaEdge) *Transition {
	return &Transition{
		node: node,
		edge: edge,
	}
}

func NewSubNfa(start *ParseNfaNode, end *ParseNfaNode) *SubNfa {
	return &SubNfa{
		start: start,
		end:   end,
	}
}

func NewParseNfaNode() *ParseNfaNode {
	return &ParseNfaNode{
		nexts: make([]*Transition, 0),
	}
}

func NewParseNfaEdge(label string, start *ParseNfaNode, end *ParseNfaNode) *ParseNfaEdge {
	// the checked should be set to false in order to check wildcard
	edge := ParseNfaEdge{label, false, false, false, start, end}
	if label == "" {
		edge.epsilon = true
	} else if label == "." {
		edge.wildcard = true
	}
	return &edge
}

func NewNfa() *Nfa {
	return &Nfa{
		nodes: make([]*ParseNfaNode, 0),
		edges: make([]*ParseNfaEdge, 0),
		inode: nil,
		fnode: nil,
	}
}

func NewSubPath(nodes []*ParseNfaNode, str string) *SubPath {
	return &SubPath{
		nodes: nodes,
		str:   str,
	}
}

func GenerateNfa(root *ParseTreeNode) *Nfa {
	nfa := NewNfa()
	subnfa := nfa.BuildNFA(root)
	nfa.inode = subnfa.start
	nfa.fnode = subnfa.end
	//nfa.CheckWildcard(nfa.inode)
	return nfa
}

func (n *Nfa) BuildNFA(node *ParseTreeNode) *SubNfa {
	if node.isoperator {
		if node.value == "*" {
			sub := n.BuildNFA(node.lchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", sub.end, sub.start)
			n.AddEdge("", start, sub.start)
			n.AddEdge("", start, end)
			n.AddEdge("", sub.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "+" {
			sub := n.BuildNFA(node.lchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", sub.end, sub.start)
			n.AddEdge("", start, sub.start)
			n.AddEdge("", sub.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "&" {
			subl := n.BuildNFA(node.lchild)
			subr := n.BuildNFA(node.rchild)
			n.AddEdge("", subl.end, subr.start)
			return NewSubNfa(subl.start, subr.end)
		} else if node.value == "|" {
			subl := n.BuildNFA(node.lchild)
			subr := n.BuildNFA(node.rchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", start, subl.start)
			n.AddEdge("", start, subr.start)
			n.AddEdge("", subl.end, end)
			n.AddEdge("", subr.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "?" {
			sub := n.BuildNFA(node.lchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", start, sub.start)
			n.AddEdge("", start, end)
			n.AddEdge("", sub.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "." {
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge(".", start, end)
			return NewSubNfa(start, end)
		} else {
			return nil
			// other operator
		}
	} else {
		return n.BuildNfaWithString(node.value)
	}
}

func (n *Nfa) CheckWildcard(node *ParseNfaNode) {
	for i := 0; i < len(node.nexts); i++ {
		edge := node.nexts[i].edge
		end := node.nexts[i].node
		if edge.wildcard && !edge.checked {
			// remove wildcard
			node.nexts = append(node.nexts[:i], node.nexts[i+1:]...)
			// expand
			n.ExpandWildcard(node, end)
		}
		edge.checked = true
		if !IsChecked(end) {
			n.CheckWildcard(end)
		}
	}

}

func IsChecked(node *ParseNfaNode) bool {
	for i := 0; i < len(node.nexts); i++ {
		if node.nexts[i].edge.checked == false {
			return false
		}
	}
	return true
}

func (n *Nfa) ExpandWildcard(start *ParseNfaNode, end *ParseNfaNode) {
	// the alphabet represent all allowed characters
	alphabet := NewParameters().alphabet
	for i := 0; i < 127; i++ {
		if alphabet[i] == 1 {
			//c := string(i)
			c := strconv.Itoa(i)
			sub := n.BuildNfaWithString(c)
			n.AddEdge("", start, sub.start)
			sub.start.nexts[0].edge.checked = true
			sub.start.nexts[0].edge.epsilon = false
			sub.start.nexts[0].edge.wildcard = false
			n.AddEdge("", sub.end, end)
		}
	}
}

func (n *Nfa) BuildNfaWithString(str string) *SubNfa {
	start := NewParseNfaNode()
	start_ := start
	n.nodes = append(n.nodes, start)
	for i := 0; i < len(str); i++ {
		end := NewParseNfaNode()
		n.AddNode(end)
		n.AddEdge(str[i:i+1], start, end)
		start = end
	}
	return NewSubNfa(start_, start)

}

func (n *Nfa) AddEdge(label string, start *ParseNfaNode, end *ParseNfaNode) {
	edge := NewParseNfaEdge(label, start, end)
	start.nexts = append(start.nexts, NewTransition(end, edge))
	n.edges = append(n.edges, edge)
}

func (n *Nfa) AddNode(node *ParseNfaNode) {
	n.nodes = append(n.nodes, node)
}

func (n *Nfa) FindSubPath(startpath []*ParseNfaNode, trietree *gramClvc.TrieTree, label string) (bool, []*SubPath) {
	findtree := trietree.SearchGramFromDicTree(label)
	tolast := false
	qmin := trietree.Qmin()
	results := make([]*SubPath, 0)
	if !findtree && label != "" {
		// not in trietree
		firstnode := startpath[0:1]
		return n.FindSubPath(firstnode, trietree, "")

	}
	S := InitializeSubPathStack()
	S.Push(NewSubPath(startpath, label))
	for len(S.list) != 0 {
		path := S.Pop()
		// Avoid duplicate additions
		isadd := false
		lastnode := path.nodes[len(path.nodes)-1]
		subpaths := n.GetPathToNextNode(lastnode)
		for i := 0; i < len(subpaths); i++ {
			newpath := NewSubPath(make([]*ParseNfaNode, len(path.nodes)), path.str)
			copy(newpath.nodes, path.nodes)
			pathtonext := subpaths[i]
			if pathtonext.str == "" {
				// find the last node
				if len(newpath.str) >= qmin && !isadd {
					isadd = true
					results = append(results, newpath)
				} else {
					tolast = true
				}

			} else {
				findintree := trietree.SearchGramFromDicTree(newpath.str + pathtonext.str)
				if findintree || len(newpath.str) < qmin {
					newpath.nodes = append(newpath.nodes, pathtonext.nodes...)
					newpath.str = newpath.str + pathtonext.str
					S.Push(newpath)
				} else if !isadd {
					isadd = true
					results = append(results, newpath)
				}
			}
		}

	}
	return tolast, results
}

func (n *Nfa) GetPathToNextNode(node *ParseNfaNode) []*SubPath {
	results := make([]*SubPath, 0)
	p := make([]*ParseNfaNode, 0)
	p = append(p, node)
	if node == n.fnode {
		results = append(results, NewSubPath(p, ""))
		return results
	}
	S := InitializeSubPathStack()
	S.Push(NewSubPath(p, ""))
	for len(S.list) != 0 {
		path := S.Pop()
		lastnode := path.nodes[len(path.nodes)-1]
		if lastnode == n.fnode {
			results = append(results, NewSubPath(make([]*ParseNfaNode, 0), ""))
		}
		for i := 0; i < len(lastnode.nexts); i++ {
			newpath := NewSubPath(make([]*ParseNfaNode, len(path.nodes)), path.str)
			copy(newpath.nodes, path.nodes)
			edge := lastnode.nexts[i].edge
			newpath.nodes = append(newpath.nodes, lastnode.nexts[i].node)
			if edge.epsilon {
				S.Push(newpath)
			} else {
				newpath.str = edge.label
				newpath.nodes = newpath.nodes[1:]
				results = append(results, newpath)
			}
		}
	}
	return results
}

func GetSuffixPath(path []*ParseNfaNode) []*ParseNfaNode {
	n := 0
	start := 0
	for start == 0 && (n+1) <= len(path)-1 {
		n1 := path[n]
		n2 := path[n+1]
		for i := 0; i < len(n1.nexts); i++ {
			if n1.nexts[i].node == n2 {
				if !n1.nexts[i].edge.epsilon {
					start = n + 1
					break
				}
			}
		}
		n++
	}
	cont := true
	// maybe duplicate ?
	for cont && start <= len(path)-2 {
		n1 := path[start]
		n2 := path[start+1]
		i := 0
		for ; i < len(n1.nexts); i++ {
			if n1.nexts[i].node == n2 {
				if n1.nexts[i].edge.epsilon {
					start++
					break
				} else {
					cont = false
					break
				}
			}

		}
		if i == len(n1.nexts) {
			break
		}
	}
	suffpath := make([]*ParseNfaNode, 0)
	for i := start; i < len(path); i++ {
		suffpath = append(suffpath, path[i])
	}
	return suffpath
}
