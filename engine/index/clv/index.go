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
	"path"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
)

const (
	qmin   = 1
	maxBuf = 2000 // can not to large, otherwise AddItems may fail because the item size is greater than 64k.
)

type InvertState struct {
	timestamp int64
	position  uint16
}

type InvertStates struct {
	invertState []InvertState
	tsid        uint64
}

func NewInvertStates() *InvertStates {
	return &InvertStates{
		invertState: make([]InvertState, 0),
	}
}

type InvertIndex struct {
	invertStates map[uint64]*InvertStates // tsid is the key.
	ids          map[uint32]struct{}
}

func NewInvertIndex() InvertIndex {
	return InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
	}
}

type TrieNode struct {
	children    map[string]*TrieNode
	invertIndex InvertIndex
}

func NewTrieNode() *TrieNode {
	return &TrieNode{
		children:    make(map[string]*TrieNode),
		invertIndex: NewInvertIndex(),
	}
}

type Options struct {
	Path        string
	Measurement string
	Field       string
}

type TokenIndex struct {
	tb          *mergeset.Table
	root        *TrieNode
	analyzer    *Analyzer
	trieLock    sync.RWMutex
	closing     chan struct{}
	path        string
	measurement string
	field       string
	docNum      uint32
}

func NewTokenIndex(opts *Options) (*TokenIndex, error) {
	idx := &TokenIndex{
		root:        NewTrieNode(),
		closing:     make(chan struct{}),
		path:        opts.Path,
		measurement: opts.Measurement,
		field:       opts.Field,
	}

	// open token index
	err := idx.Open()
	if err != nil {
		return nil, err
	}

	// get dictionary version
	var version uint32
	version, err = idx.searchDicVersion()
	if err != nil {
		return nil, err
	}

	// get a analyzer
	dirs := strings.Split(opts.Path, "/")
	analyzerPath := ""
	for i := 0; i < len(dirs)-2; i++ {
		analyzerPath = analyzerPath + dirs[i] + "/"
	}
	analyzerPath = analyzerPath + "/directory"
	idx.analyzer, err = GetAnalyzer(analyzerPath, opts.Measurement, opts.Field, version)
	if err != nil {
		return nil, err
	}

	// write version to mergeset table
	if version == Blank {
		idx.writeDicVersion(idx.analyzer.Version())
	}

	return idx, nil
}

func (idx *TokenIndex) Open() error {
	tbPath := path.Join(idx.path, idx.measurement, idx.field)
	tb, err := mergeset.OpenTable(tbPath, nil, mergeDocIdxItems)
	if err != nil {
		return fmt.Errorf("cannot open text index:%s, err: %+v", tbPath, err)
	}
	idx.tb = tb

	// start a process routine
	go idx.process()

	return nil
}

func (idx *TokenIndex) Close() error {
	if idx.closing != nil {
		idx.closing <- struct{}{}
		close(idx.closing)
	}
	idx.tb.MustClose()
	return nil
}

func (idx *TokenIndex) insertInvertedIndex(node *TrieNode, tsid uint64, timestamp int64, position uint16) {
	invertedIndex, ok := node.invertIndex.invertStates[tsid]
	if !ok {
		invertedIndex = NewInvertStates()
		invertedIndex.tsid = tsid
		node.invertIndex.invertStates[tsid] = invertedIndex
	}
	invertedIndex.invertState = append(invertedIndex.invertState, InvertState{timestamp, position})
}

func (idx *TokenIndex) insertTrieNode(vtoken []string, tsid uint64, timestamp int64, position uint16) {
	idx.trieLock.Lock()
	idx.docNum++
	node := idx.root
	for _, token := range vtoken {
		child, ok := node.children[token]
		if !ok {
			child = NewTrieNode()
			node.children[token] = child
		}
		node = child
	}
	idx.insertInvertedIndex(node, tsid, timestamp, position)
	idx.trieLock.Unlock()
}

func (idx *TokenIndex) insertSuffixToTrie(vtoken []string, id uint32) {
	idx.trieLock.Lock()
	node := idx.root
	for _, token := range vtoken {
		child, ok := node.children[token]
		if !ok {
			child = NewTrieNode()
			node.children[token] = child
		}
		node = child
	}

	if _, ok := node.invertIndex.ids[id]; !ok {
		node.invertIndex.ids[id] = struct{}{}
	}
	idx.trieLock.Unlock()

	return
}

var idxItemsPool mergeindex.IndexItemsPool

func (idx *TokenIndex) writeDicVersion(version uint32) {
	return
}

func (idx *TokenIndex) createIndex(vtoken string, node *TrieNode) error {
	if len(node.invertIndex.invertStates) == 0 && len(node.invertIndex.ids) == 0 {
		return nil
	}

	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	ii.B = marshal(ii.B, vtoken, &node.invertIndex)
	ii.Next()
	// write to mergeset
	return idx.tb.AddItems(ii.Items)
}

func (idx *TokenIndex) writeDocumentIndex(vtoken string, node *TrieNode) {
	idx.createIndex(vtoken, node)
	// traversal of trees.
	for token, child := range node.children {
		vtokens := vtoken + token + " "
		idx.writeDocumentIndex(vtokens, child)
	}
}

func (idx *TokenIndex) processDocument() {
	// replace the root node
	idx.trieLock.Lock()
	if len(idx.root.children) == 0 {
		idx.trieLock.Unlock()
		return
	}
	node := idx.root
	idx.docNum = 0
	idx.root = NewTrieNode()
	idx.trieLock.Unlock()

	// Deal the first level node of the tree.
	for token, child := range node.children {
		vtokens := token + " "
		idx.writeDocumentIndex(vtokens, child)
	}
}

func (idx *TokenIndex) process() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-idx.closing:
			return
		case <-ticker.C:
			idx.processDocument()
		}
	}
}

func (idx *TokenIndex) AddDocument(log string, tsid uint64, timestamp int64) error {
	// tokenizer analyze
	tokens, err := idx.analyzer.Analyze(log)
	if err != nil {
		return err
	}
	for position, vtoken := range tokens {
		idx.insertTrieNode(vtoken.tokens, tsid, timestamp, position)
		if len(vtoken.tokens) <= qmin {
			continue
		}
		// v-token suffix insert
		oriVtoken := ""
		for i := 0; i < len(vtoken.tokens); i++ {
			oriVtoken += vtoken.tokens[i] + " "
		}

		for i := 1; i < len(vtoken.tokens); i++ {
			idx.insertSuffixToTrie(vtoken.tokens[i:], vtoken.id)
		}
	}

	if idx.docNum >= maxBuf {
		idx.processDocument()
	}

	return nil
}

var clvSearchPool sync.Pool

func (idx *TokenIndex) getClvSearch() *tokenSearch {
	v := clvSearchPool.Get()
	if v == nil {
		v = &tokenSearch{
			idx: idx,
		}
	}

	is := v.(*tokenSearch)
	is.ts.Init(idx.tb)
	is.idx = idx

	return is
}

func (idx *TokenIndex) putClvSearch(is *tokenSearch) {
	is.kb.Reset()
	is.ts.MustClose()
	is.idx = nil
	clvSearchPool.Put(is)
}

func (idx *TokenIndex) searchDicVersion() (uint32, error) {
	cs := idx.getClvSearch()
	dicVersion := cs.searchDicVersion()

	idx.putClvSearch(cs)
	return dicVersion, nil
}
