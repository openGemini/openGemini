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

package clv

import (
	"fmt"
	"sort"
	"strings"

	"github.com/clipperhouse/uax29/iterators/filter"
	"github.com/clipperhouse/uax29/words"
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
		return string(child[i].token) <= string(child[j].token)
	})
}

func (node *dicNode) findNodeByToken(token string) *dicNode {
	if len(node.child) == 0 {
		return nil
	}

	n := sort.Search(len(node.child), func(i int) bool {
		return token <= node.child[i].token
	})

	if n == len(node.child) || node.child[n].token != token {
		return nil
	}

	return node.child[n]
}

func (node *dicNode) findNodeById(id uint32) *dicNode {
	if len(node.child) == 0 {
		return nil
	}

	n := sort.Search(len(node.child), func(i int) bool {
		return id <= node.child[i].id
	})

	if n == len(node.child) {
		return nil
	}

	return node.child[n]
}

type Analyzer struct {
	dictionary  *dicNode
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

func Tokenizer(log string) []string {
	tokens := make([]string, 0, DefaultCap)
	tmpLog := strings.ToLower(log)

	seg := words.NewSegmenter([]byte(tmpLog))
	seg.Filter(filter.Wordlike)

	for seg.Next() {
		tokens = append(tokens, seg.Text())
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

func (a *Analyzer) findLongestTokens(tokens []string) (VToken, int) {
	var vtoken VToken
	node := a.dictionary
	// for default analyzer.
	if node == nil {
		vtoken.tokens = append(vtoken.tokens, tokens[0])
		return vtoken, 1
	}

	// for learning-type analyzer
	var i int
	for ; i < len(tokens); i++ {
		child := node.findNodeByToken(tokens[i])
		if child == nil {
			break
		}
		node = child
		vtoken.id = node.id
		vtoken.tokens = append(vtoken.tokens, tokens[i])
	}

	if i == 0 {
		vtoken.tokens = append(vtoken.tokens, tokens[0])
		i++
	}

	return vtoken, i
}

func (a *Analyzer) InsertToDictionary(tokens string) {
	fmt.Printf("load dictionary:%s\n", tokens)
	ts := strings.Split(tokens, " ")
	a.insertToDic(ts)
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

func (a *Analyzer) assignId(id uint32, node *dicNode) uint32 {
	if node == nil {
		return id
	}

	for _, child := range node.child {
		id = a.assignId(id, child)
		child.id = id
		id++
	}
	return id
}

func (a *Analyzer) AssignId() error {
	if a.dictionary == nil {
		return fmt.Errorf("Can not assign id for dictionary{%s-%s-%d}, because it is blank.", a.measurement, a.field, a.version)
	}

	// sort the dictionary.
	a.sort(a.dictionary)
	a.assignId(1, a.dictionary)

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

func (a *Analyzer) Analyze(log string) ([]VToken, error) {
	tokens := Tokenizer(log)
	if a.collector != nil {
		a.collector.Collect(tokens)
	}

	var i int
	vtokens := make([]VToken, 0)
	for i < len(tokens) {
		vtoken, j := a.findLongestTokens(tokens[i:])
		vtoken.pos = uint16(i)
		vtokens = append(vtokens, vtoken)
		i = i + j
	}

	return vtokens, nil
}
