/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package clv

import (
	"fmt"
	"sort"
	"strings"
)

const (
	DefaultCap = 16
)

type Collector interface {
	Collect(tokens []string)
}

type VToken struct {
	tokens []string
	id     uint32
	pos    uint16
}

type dicNode struct {
	child []*dicNode
	token string
	id    uint32
}

func newDicNode() *dicNode {
	return &dicNode{}
}

func (node *dicNode) sort() {
	if len(node.child) == 0 {
		return
	}

	child := node.child
	sort.SliceStable(child, func(i, j int) bool {
		return child[i].token <= child[j].token
	})
}

// find a node with an token equal to the input token
func (node *dicNode) findNodeByToken(token string) *dicNode {
	if len(node.child) == 0 {
		return nil
	}

	n := sort.Search(len(node.child), func(i int) bool {
		return token <= node.child[i].token
	})

	if n < len(node.child) && node.child[n].token == token {
		return node.child[n]
	}

	return nil
}

// find a node with an ID greater than or equal to the input ID
func (node *dicNode) findNodeById(id uint32) *dicNode {
	if len(node.child) == 0 {
		return nil
	}

	n := sort.Search(len(node.child), func(i int) bool {
		return id <= node.child[i].id
	})

	if n < len(node.child) {
		return node.child[n]
	}

	return nil
}

type dicMapNode struct {
	child map[string]*dicMapNode
	id    uint32
}

func newDicMapNode() *dicMapNode {
	return &dicMapNode{
		child: make(map[string]*dicMapNode, 0),
	}
}

type Analyzer struct {
	dictionary  *dicNode
	dicMap      *dicMapNode
	collector   Collector
	path        string
	measurement string
	field       string
	version     uint32
}

func newAnalyzer(path, measurement, field string, version uint32) *Analyzer {
	return &Analyzer{
		path:        path,
		measurement: measurement,
		field:       field,
		version:     version,
	}
}

func Tokenizer(log []byte) []string {
	tokens := make([]string, 0, DefaultCap)

	tokenizer := NewDefaultSimpleTokenzier()
	tokenizer.SetData(log)

	for tokenizer.Next() {
		tokens = append(tokens, string(tokenizer.Token()))
	}

	return tokens
}

func (a *Analyzer) insertToDic(tokens []string) {
	if a.dictionary == nil {
		a.dictionary = newDicNode()
	}
	node := a.dictionary

	for i := 0; i < len(tokens); i++ {
		child := node.findNodeByToken(tokens[i])
		if child == nil {
			child = newDicNode()
			child.token = tokens[i]
			node.child = append(node.child, child)
		}
		node = child
	}
}

func (a *Analyzer) findLongestTokens(tokens []string) VToken {
	var vtoken VToken
	// for default analyzer.
	if a.dicMap == nil {
		vtoken.tokens = append(vtoken.tokens, tokens[0])
		return vtoken
	}

	// for learning-type analyzer
	var i int
	node := a.dicMap
	vtoken.tokens = make([]string, 0, len(tokens))
	for ; i < len(tokens); i++ {
		child, ok := node.child[tokens[i]]
		if !ok {
			break
		}
		node = child
		vtoken.id = node.id
		vtoken.tokens = append(vtoken.tokens, tokens[i])
	}

	if i == 0 {
		vtoken.tokens = append(vtoken.tokens, tokens[0])
	}

	return vtoken
}

func (a *Analyzer) InsertToDictionary(tokens string) {
	a.insertToDic(strings.Split(tokens, " "))
}

func (a *Analyzer) sort(node *dicNode) {
	if node == nil {
		return
	}

	for _, child := range node.child {
		a.sort(child)
	}
	node.sort()
}

func (a *Analyzer) assignId(id uint32, node *dicNode, dicMapNode *dicMapNode) uint32 {
	if node == nil {
		return id
	}

	for _, child := range node.child {
		dicMapChild := newDicMapNode()
		id = a.assignId(id, child, dicMapChild)
		child.id = id

		dicMapChild.id = child.id
		dicMapNode.child[child.token] = dicMapChild
		id++
	}
	return id
}

func (a *Analyzer) AssignId() error {
	if a.dictionary == nil {
		return fmt.Errorf("can not assign id for dictionary{%s-%s-%d}, because it is blank", a.measurement, a.field, a.version)
	}

	// sort the dictionary.
	a.sort(a.dictionary)

	a.dicMap = newDicMapNode()
	a.assignId(1, a.dictionary, a.dicMap)

	return nil
}

func (a *Analyzer) Version() uint32 {
	return a.version
}

func (a *Analyzer) RegisterCollector(collector Collector) {
	a.collector = collector
}

func (a *Analyzer) FindVtokenByID(id uint32) string {
	tokens := ""
	node := a.dictionary

	for {
		node = node.findNodeById(id)
		if node == nil {
			break
		}
		tokens += node.token + " "
		if node.id == id {
			return tokens
		}
	}
	return ""
}

func (a *Analyzer) Analyze(log []byte) ([]VToken, error) {
	tokens := Tokenizer(log)
	if a.collector != nil {
		a.collector.Collect(tokens)
	}

	var last int = 0
	vtokens := make([]VToken, 0, len(tokens))
	for i := 0; i < len(tokens); i++ {
		vtoken := a.findLongestTokens(tokens[i:])
		if i+len(vtoken.tokens) <= last {
			continue
		}
		vtoken.pos = uint16(i)
		vtokens = append(vtokens, vtoken)
		last = i + len(vtoken.tokens)
	}

	return vtokens, nil
}
