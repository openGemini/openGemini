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
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
)

const (
	qmin   = 1
	maxBuf = 2000 // can not too large, otherwise AddItems may fail because the item size is greater than 64k.
)

type QueryType int

const (
	Match        QueryType = 1
	Match_Phrase QueryType = 2
	Fuzzy        QueryType = 3
)

type InvertState struct {
	timestamp int64
	position  uint16
}

type InvertStates struct {
	invertState []InvertState
	sid         uint64
}

func NewInvertStates() *InvertStates {
	return &InvertStates{
		invertState: make([]InvertState, 0),
	}
}

// Len is the number of elements in the InvertStates.
func (iss *InvertStates) Len() int {
	return len(iss.invertState)
}

// Swap swaps the elements with indexes i and j.
func (iss *InvertStates) Swap(i, j int) {
	ivss := iss.invertState
	ivss[i].timestamp, ivss[j].timestamp = ivss[j].timestamp, ivss[i].timestamp
	ivss[i].position, ivss[j].position = ivss[j].position, ivss[i].position
}

// i < j. if Less() return false, should call Swap()
func (iss *InvertStates) Less(i, j int) bool {
	ivss := iss.invertState
	if ivss[i].timestamp < ivss[j].timestamp {
		return true
	}
	if ivss[i].timestamp == ivss[j].timestamp {
		return ivss[i].position <= ivss[j].position
	}
	return false
}

func (iss *InvertStates) InvertIsExisted(is InvertState) bool {
	ivss := iss.invertState

	n := sort.Search(len(ivss), func(i int) bool {
		if is.timestamp < ivss[i].timestamp {
			return true
		}
		if is.timestamp == ivss[i].timestamp {
			return is.position <= ivss[i].position
		}
		return false
	})

	if n == len(ivss) ||
		ivss[n].timestamp != is.timestamp ||
		ivss[n].position != is.position {
		return false
	}

	return true
}

func (iss *InvertStates) InvertTimestampIsExisted(timestamp int64) bool {
	ivss := iss.invertState

	n := sort.Search(len(ivss), func(i int) bool {
		return timestamp <= ivss[i].timestamp
	})

	if n == len(ivss) || ivss[n].timestamp != timestamp {
		return false
	}

	return true
}

type InvertIndex struct {
	invertStates map[uint64]*InvertStates // sid is the key.
	ids          map[uint32]struct{}
}

func NewInvertIndex() InvertIndex {
	return InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
	}
}

func (ii *InvertIndex) Sort() {
	for _, iss := range ii.invertStates {
		sort.Sort(iss)
	}
}

func (ii *InvertIndex) AddInvertState(sid uint64, is InvertState) {
	iss, ok := ii.invertStates[sid]
	if !ok {
		iss = NewInvertStates()
		iss.sid = sid
		ii.invertStates[sid] = iss
	}

	iss.invertState = append(iss.invertState, is)
}

func (ii *InvertIndex) AddId(id uint32) {
	if _, ok := ii.ids[id]; !ok {
		ii.ids[id] = struct{}{}
	}
}

func (ii *InvertIndex) Append(sid uint64, is []InvertState) {
	iss, ok := ii.invertStates[sid]
	if !ok {
		iss = NewInvertStates()
		iss.sid = sid
		ii.invertStates[sid] = iss
	}

	iss.invertState = append(iss.invertState, is...)
}

func (ii *InvertIndex) GetFilterTimeBySid(sid uint64) []int64 {
	iss, ok := ii.invertStates[sid]
	if !ok {
		return nil
	}
	var preTime int64
	timeStamps := make([]int64, 0, len(iss.invertState))
	for _, is := range iss.invertState {
		if preTime == is.timestamp {
			continue
		}
		timeStamps = append(timeStamps, is.timestamp)
		preTime = is.timestamp
	}
	return timeStamps
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

	termSet     map[string]struct{}
	termSetLock sync.RWMutex
}

func NewTokenIndex(opts *Options) (*TokenIndex, error) {
	idx := &TokenIndex{
		root:        NewTrieNode(),
		closing:     make(chan struct{}),
		path:        opts.Path,
		measurement: opts.Measurement,
		field:       opts.Field,
		termSet:     make(map[string]struct{}),
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
	analyzerPath = analyzerPath + "directory"
	idx.analyzer, err = GetAnalyzer(analyzerPath, opts.Measurement, opts.Field, version)
	if err != nil {
		return nil, err
	}

	// write version to mergeset table
	if version == Unknown {
		err = idx.writeDicVersion(idx.analyzer.Version())
		if err != nil {
			return nil, err
		}
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

func (idx *TokenIndex) insertTrieNode(vtoken []string, sid uint64, timestamp int64, position uint16) {
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
	node.invertIndex.AddInvertState(sid, InvertState{timestamp, position})
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
	node.invertIndex.AddId(id)
	idx.trieLock.Unlock()

	return
}

var idxItemsPool mergeindex.IndexItemsPool

func (idx *TokenIndex) writeDicVersion(version uint32) error {
	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	ii.B = marshalDicVersion(ii.B, version)
	ii.Next()

	return idx.tb.AddItems(ii.Items)
}

func (idx *TokenIndex) createDocumentIndex(vtoken string, node *TrieNode) error {
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

func (idx *TokenIndex) writeDocumentIndex(vtoken string, node *TrieNode) error {
	err := idx.createDocumentIndex(vtoken, node)
	if err != nil {
		return err
	}
	// traversal of trees.
	for token, child := range node.children {
		vtokens := vtoken + token + " "
		err = idx.writeDocumentIndex(vtokens, child)
		if err != nil {
			return err
		}
	}
	return nil
}

// Need to make the best effort to ensure that the written token is not duplicated
func (idx *TokenIndex) createTermIndex(terms []string) error {
	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	for i := 0; i < len(terms); i++ {
		ii.B = marshalTerm(ii.B, terms[i])
		ii.Next()
	}

	return idx.tb.AddItems(ii.Items)
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

	terms := make([]string, 0, len(node.children))
	// Deal the first level node of the tree.
	for token, child := range node.children {
		vtokens := token + " "
		err := idx.writeDocumentIndex(vtokens, child)
		if err != nil {
			panic(fmt.Errorf("Write document index failed, err: %+v", err))
		}
		// update the token set
		idx.termSetLock.Lock()
		if _, ok := idx.termSet[token]; !ok {
			terms = append(terms, token)
			idx.termSet[token] = struct{}{}
		}
		idx.termSetLock.Unlock()
	}

	err := idx.createTermIndex(terms)
	if err != nil {
		panic(fmt.Errorf("Write term index failed, err: %+v", err))
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

func (idx *TokenIndex) AddDocument(log string, sid uint64, timestamp int64) error {
	// tokenizer analyze
	tokens, err := idx.analyzer.Analyze(log)
	if err != nil {
		return err
	}
	for _, vtoken := range tokens {
		idx.insertTrieNode(vtoken.tokens, sid, timestamp, vtoken.pos)
		if len(vtoken.tokens) <= qmin {
			continue
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

var tokenSearchPool sync.Pool

func (idx *TokenIndex) getTokenSearch() *tokenSearch {
	v := tokenSearchPool.Get()
	if v == nil {
		v = &tokenSearch{}
	}

	ts := v.(*tokenSearch)
	ts.tbs.Init(idx.tb)

	return ts
}

func (idx *TokenIndex) putTokenSearch(ts *tokenSearch) {
	ts.kb.Reset()
	ts.tbs.MustClose()
	tokenSearchPool.Put(ts)
}

func (idx *TokenIndex) searchDicVersion() (uint32, error) {
	ts := idx.getTokenSearch()
	dicVersion := ts.searchDicVersion()
	idx.putTokenSearch(ts)
	return dicVersion, nil
}

func (idx *TokenIndex) searchInvertByVtoken(tokens []string, ts *tokenSearch) *InvertIndex {
	vtoken := ""
	for i := 0; i < len(tokens); i++ {
		vtoken += tokens[i] + " "
	}

	invert := NewInvertIndex()
	ts.searchInvertIndexByVtoken(vtoken, &invert)

	return &invert
}

func (idx *TokenIndex) searchInvertByVtokenAndId(tokens []string, ts *tokenSearch) *InvertIndex {
	invert := idx.searchInvertByVtoken(tokens, ts)
	if invert == nil {
		return nil
	}

	// Obtain the inverted list corresponding to the ID
	vtokens := make([]string, 0, len(invert.ids))
	for id := range invert.ids {
		vtoken := idx.analyzer.FindVtokenByID(id)
		if len(vtoken) == 0 {
			panic(fmt.Errorf("cannot find the vtoken by id: %d", id))
		}
		vtokens = append(vtokens, vtoken)
	}

	// Merge inverted lit
	for i := 0; i < len(vtokens); i++ {
		ts.searchInvertIndexByVtoken(vtokens[i], invert)
	}
	invert.Sort()

	return invert
}

func (idx *TokenIndex) searchInvertByPrefixVtoken(tokens []string, ts *tokenSearch) *InvertIndex {
	vtoken := ""
	for i := 0; i < len(tokens); i++ {
		vtoken += tokens[i] + " "
	}
	invert := NewInvertIndex()
	ts.searchInvertIndexByPrefixVtoken(vtoken, &invert)

	return &invert
}

func (idx *TokenIndex) searchInvertByPrefixVtokenAndId(tokens []string, ts *tokenSearch) *InvertIndex {
	invert := idx.searchInvertByPrefixVtoken(tokens, ts)
	if invert == nil {
		return nil
	}

	// Obtain the inverted list corresponding to the ID
	vtokens := make([]string, 0, len(invert.ids))
	for id := range invert.ids {
		vtoken := idx.analyzer.FindVtokenByID(id)
		if len(vtoken) == 0 {
			panic(fmt.Errorf("cannot find the vtoken by id: %d", id))
		}
		vtokens = append(vtokens, vtoken)
	}

	// Merge inverted lit
	for i := 0; i < len(vtokens); i++ {
		ts.searchInvertIndexByVtoken(vtokens[i], invert)
	}
	invert.Sort()

	return invert
}

func (idx *TokenIndex) Match(queryStr string) (*InvertIndex, error) {
	ts := idx.getTokenSearch()
	defer idx.putTokenSearch(ts)

	tokens := Tokenizer(queryStr)
	var pre *InvertIndex
	for i := 0; i < len(tokens); i++ {
		cur := idx.searchInvertByPrefixVtokenAndId([]string{tokens[i]}, ts)
		pre = IntersectInvertIndex(pre, cur)
	}
	return pre, nil
}

func (idx *TokenIndex) Match_Phrase(queryStr string) (*InvertIndex, error) {
	ts := idx.getTokenSearch()
	defer idx.putTokenSearch(ts)

	vtokens, err := idx.analyzer.Analyze(queryStr)
	if err != nil {
		return nil, err
	}

	var pre *InvertIndex
	if len(vtokens) == 1 {
		pre = idx.searchInvertByPrefixVtokenAndId(vtokens[0].tokens, ts)
		return pre, nil
	}

	var cur *InvertIndex
	for i := 0; i < len(vtokens); i++ {
		// get the inverted poslist
		if i == 0 {
			cur = idx.searchInvertByVtokenAndId(vtokens[i].tokens, ts)
		} else if i == len(vtokens)-1 {
			cur = idx.searchInvertByPrefixVtoken(vtokens[i].tokens, ts)
		} else {
			cur = idx.searchInvertByVtoken(vtokens[i].tokens, ts)
		}
		cur.Sort()

		if i == 0 {
			pre = cur
			continue
		}
		pre = IntersectInvertByDistance(pre, cur, vtokens[i].pos)
	}

	return pre, nil
}

// if not found any matched text, need return a empty InvertIndex, not nil InvertIndex.
func (idx *TokenIndex) Search(t QueryType, queryStr string) (*InvertIndex, error) {
	if t == Match {
		return idx.Match(queryStr)
	} else if t == Match_Phrase {
		return idx.Match_Phrase(queryStr)
	}
	return nil, fmt.Errorf("cannot find the query type:%d", t)
}

func removeDuplicateTimestamp(iss []InvertState) []InvertState {
	res := iss[:0]
	var preTime int64
	for i := 0; i < len(iss); i++ {
		if preTime == iss[i].timestamp {
			continue
		}
		res = append(res, iss[i])
		preTime = iss[i].timestamp
	}

	return res
}

func IntersectInvertByDistance(pre *InvertIndex, cur *InvertIndex, dis uint16) *InvertIndex {
	res := NewInvertIndex()
	for sid, preIss := range pre.invertStates {
		curIss, ok := cur.invertStates[sid]
		if !ok {
			continue
		}

		swap := false
		if len(preIss.invertState) > len(curIss.invertState) {
			preIss, curIss = curIss, preIss
			swap = true
		}

		for i := 0; i < len(preIss.invertState); i++ {
			if swap == true {
				if preIss.invertState[i].position < dis {
					continue
				}
				position := preIss.invertState[i].position - dis
				if curIss.InvertIsExisted(InvertState{preIss.invertState[i].timestamp, position}) {
					res.AddInvertState(sid, InvertState{preIss.invertState[i].timestamp, position})
				}
			} else {
				position := preIss.invertState[i].position + dis
				if curIss.InvertIsExisted(InvertState{preIss.invertState[i].timestamp, position}) {
					// add the poslist
					res.AddInvertState(sid, preIss.invertState[i])
				}
			}
		}
	}

	return &res
}

func IntersectInvertIndex(lii *InvertIndex, rii *InvertIndex) *InvertIndex {
	if lii == nil {
		return rii
	}
	if rii == nil {
		return lii
	}

	res := NewInvertIndex()
	for sid, lIss := range lii.invertStates {
		rIss, ok := rii.invertStates[sid]
		if !ok {
			continue
		}
		if len(lIss.invertState) > len(rIss.invertState) {
			lIss.invertState, rIss.invertState = rIss.invertState, lIss.invertState
		}

		var preTimestamp int64
		for i := 0; i < len(lIss.invertState); i++ {
			if !rIss.InvertTimestampIsExisted(lIss.invertState[i].timestamp) ||
				preTimestamp == lIss.invertState[i].timestamp {
				continue
			}
			res.AddInvertState(sid, lIss.invertState[i])
			preTimestamp = lIss.invertState[i].timestamp
		}
	}

	return &res
}

func UnionInvertIndex(lii *InvertIndex, rii *InvertIndex) *InvertIndex {
	if lii == nil {
		return rii
	}
	if rii == nil {
		return lii
	}

	// append
	for sid, rIss := range rii.invertStates {
		lii.Append(sid, rIss.invertState)
	}
	lii.Sort()

	// duplicate removal
	for _, lIss := range lii.invertStates {
		lIss.invertState = removeDuplicateTimestamp(lIss.invertState)
	}

	return lii
}
