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
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
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

type RowFilter struct {
	RowId  int64 // RowId is the timestamp, primary key for timeseries databases.
	Filter influxql.Expr
}

type InvertState struct {
	timestamp int64
	position  uint16
	filter    influxql.Expr
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
	ivss[i].filter, ivss[j].filter = ivss[j].filter, ivss[i].filter
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

func (iss *InvertStates) GetInvertStateByTimestamp(timestamp int64) *InvertState {
	ivss := iss.invertState

	n := sort.Search(len(ivss), func(i int) bool {
		return timestamp <= ivss[i].timestamp
	})

	if n == len(ivss) || ivss[n].timestamp != timestamp {
		return nil
	}

	return &ivss[n]
}

type InvertIndex struct {
	invertStates map[uint64]*InvertStates // sid is the key.
	ids          map[uint32]struct{}
	filter       influxql.Expr // filter only applies to data outside of the dataset(.invertStates).
}

func NewInvertIndex() InvertIndex {
	return InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       nil,
	}
}

func NewInvertIndexPointer() *InvertIndex {
	return &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       nil,
	}
}

func isInDataSet(dataSet []uint64, data uint64) bool {
	if len(dataSet) == 0 {
		return true
	}

	n := sort.Search(len(dataSet), func(i int) bool {
		return data <= dataSet[i]
	})

	if n == len(dataSet) || dataSet[n] != data {
		return false
	}
	return true
}

func (ii *InvertIndex) Sort(sids []uint64) {
	for sid, iss := range ii.invertStates {
		if !isInDataSet(sids, sid) {
			delete(ii.invertStates, sid)
		}
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

func (ii *InvertIndex) GetFilter() influxql.Expr {
	return ii.filter
}

func (ii *InvertIndex) SetFilter(filter influxql.Expr) {
	ii.filter = filter
}

func (ii *InvertIndex) GetRowFilterBySid(sid uint64) []RowFilter {
	iss, ok := ii.invertStates[sid]
	if !ok {
		return nil
	}
	var preTime int64
	rowFilter := make([]RowFilter, 0, len(iss.invertState))
	for _, is := range iss.invertState {
		if preTime == is.timestamp {
			continue
		}
		rowFilter = append(rowFilter, RowFilter{is.timestamp, is.filter})
		preTime = is.timestamp
	}
	return rowFilter
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
	Lock        *string
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
	lock        *string
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
		lock:        opts.Lock,
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
		idx.Close()
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
		idx.Close()
		return nil, err
	}

	// write version to mergeset table
	if version == Unknown {
		err = idx.writeDicVersion(idx.analyzer.Version())
		if err != nil {
			idx.Close()
			return nil, err
		}
	}

	return idx, nil
}

func (idx *TokenIndex) Open() error {
	tbPath := path.Join(idx.path, idx.measurement, idx.field)
	tb, err := mergeset.OpenTable(tbPath, nil, mergeDocIdxItems, idx.lock)
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
	node.invertIndex.AddInvertState(sid, InvertState{timestamp, position, nil})
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
			panic(fmt.Errorf("write document index failed, err: %+v", err))
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
		panic(fmt.Errorf("write term index failed, err: %+v", err))
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

	return invert
}

func (idx *TokenIndex) Match(queryStr string, sids []uint64) (*InvertIndex, error) {
	ts := idx.getTokenSearch()
	defer idx.putTokenSearch(ts)

	tokens := Tokenizer(queryStr)
	var pre *InvertIndex
	for i := 0; i < len(tokens); i++ {
		cur := idx.searchInvertByPrefixVtokenAndId([]string{tokens[i]}, ts)
		cur.Sort(sids)
		pre = IntersectInvertIndex(pre, cur, sids)
	}
	return pre, nil
}

func (idx *TokenIndex) MatchPhrase(queryStr string, sids []uint64) (*InvertIndex, error) {
	ts := idx.getTokenSearch()
	defer idx.putTokenSearch(ts)

	vtokens, err := idx.analyzer.Analyze(queryStr)
	if err != nil {
		return nil, err
	}

	var pre *InvertIndex
	if len(vtokens) == 1 {
		pre = idx.searchInvertByPrefixVtokenAndId(vtokens[0].tokens, ts)
		pre.Sort(sids)
		return pre, nil
	}

	var cur *InvertIndex
	for i := 0; i < len(vtokens); i++ {
		// get the inverted poslist
		if i == 0 {
			pre = idx.searchInvertByVtokenAndId(vtokens[i].tokens, ts)
			pre.Sort(sids)
			continue
		} else if i == len(vtokens)-1 {
			cur = idx.searchInvertByPrefixVtoken(vtokens[i].tokens, ts)
		} else {
			cur = idx.searchInvertByVtoken(vtokens[i].tokens, ts)
		}
		cur.Sort(sids)

		pre = IntersectInvertByDistance(pre, cur, vtokens[i].pos, sids)
	}

	return pre, nil
}

func queryStrToPattern(queryStr string) string {
	sb := strings.Builder{}
	for i := 0; i < len(queryStr); i++ {
		if queryStr[i] == '_' {
			sb.WriteString(".{1}")
		} else if queryStr[i] == '%' {
			sb.WriteString(".*")
		} else {
			sb.WriteByte(queryStr[i])
		}

		if i+1 == len(queryStr) {
			sb.WriteString("$")
		}
	}

	return sb.String()
}

func (idx *TokenIndex) Fuzzy(queryStr string, sids []uint64) (*InvertIndex, error) {
	ts := idx.getTokenSearch()
	defer idx.putTokenSearch(ts)

	queryStr = strings.ToLower(queryStr)
	regex, err := regexp.Compile(queryStrToPattern(queryStr))
	if err != nil {
		return nil, err
	}

	index := strings.IndexFunc(queryStr, func(r rune) bool {
		return r == '_' || r == '%'
	})
	if index > 0 {
		queryStr = queryStr[0:index]
	} else {
		return idx.Match(queryStr, sids)
	}

	terms := ts.searchTermsIndex(queryStr, regex.Match)
	c := make([]*InvertIndex, len(terms))
	var wg sync.WaitGroup
	wg.Add(len(terms))
	for i, term := range terms {
		go func(t string, i int) {
			invertIndex, err := idx.Match(t, sids)
			if err == nil {
				c[i] = invertIndex
			}

			wg.Done()
		}(term, i)
	}

	wg.Wait()
	var invert *InvertIndex
	for _, invertIndex := range c {
		invert = UnionInvertIndex(invertIndex, invert, sids)
	}

	if invert != nil {
		invert.Sort(sids)
	}

	return invert, nil
}

// if not found any matched text, need return a empty InvertIndex, not nil InvertIndex.
func (idx *TokenIndex) Search(t QueryType, queryStr string, sids []uint64) (*InvertIndex, error) {
	var invert *InvertIndex
	var err error
	switch t {
	case Match:
		invert, err = idx.Match(queryStr, sids)
	case Match_Phrase:
		invert, err = idx.MatchPhrase(queryStr, sids)
	case Fuzzy:
		invert, err = idx.Fuzzy(queryStr, sids)
	default:
		return nil, fmt.Errorf("cannot find the query type:%d", t)
	}

	if invert == nil {
		ii := NewInvertIndex()
		invert = &ii
	}

	return invert, err
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

func IntersectInvertByDistance(pre *InvertIndex, cur *InvertIndex, dis uint16, sids []uint64) *InvertIndex {
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
			if swap {
				if preIss.invertState[i].position < dis {
					continue
				}
				position := preIss.invertState[i].position - dis
				if curIss.InvertIsExisted(InvertState{preIss.invertState[i].timestamp, position, nil}) {
					res.AddInvertState(sid, InvertState{preIss.invertState[i].timestamp, position, nil})
				}
			} else {
				position := preIss.invertState[i].position + dis
				if curIss.InvertIsExisted(InvertState{preIss.invertState[i].timestamp, position, nil}) {
					// add the poslist
					res.AddInvertState(sid, preIss.invertState[i])
				}
			}
		}
	}

	return &res
}

func IntersectInvertIndex(lii *InvertIndex, rii *InvertIndex, sids []uint64) *InvertIndex {
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

func UnionInvertIndex(lii *InvertIndex, rii *InvertIndex, sids []uint64) *InvertIndex {
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
	lii.Sort(sids)

	// duplicate removal
	for _, lIss := range lii.invertStates {
		lIss.invertState = removeDuplicateTimestamp(lIss.invertState)
	}

	return lii
}

func newIntersectExpr(lexpr, rexpr influxql.Expr) influxql.Expr {
	if lexpr == nil {
		return rexpr
	} else if rexpr == nil {
		return lexpr
	}
	return influxql.Reduce(
		&influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: lexpr,
			RHS: rexpr,
		}, nil)
}

func NewInvertStatesAndIntersect(sid uint64, invertState []InvertState, expr influxql.Expr) *InvertStates {
	invertStates := NewInvertStates()
	invertStates.sid = sid
	invertStates.invertState = append(invertStates.invertState, invertState...)
	for i := 0; i < len(invertStates.invertState); i++ {
		invertStates.invertState[i].filter = newIntersectExpr(invertStates.invertState[i].filter, expr)
	}
	return invertStates
}

func IntersectExprToInvertIndex(ii *InvertIndex, expr influxql.Expr) *InvertIndex {
	for _, iss := range ii.invertStates {
		for i := 0; i < len(iss.invertState); i++ {
			iss.invertState[i].filter = newIntersectExpr(iss.invertState[i].filter, expr)
		}
	}
	if ii.filter != nil {
		ii.filter = newIntersectExpr(ii.filter, expr)
	}
	return ii
}

func IntersectInvertIndexBySlip(li, ri *InvertIndex) *InvertIndex {
	res := NewInvertIndex()
	for sid, lIss := range li.invertStates {
		rIss, ok := ri.invertStates[sid]
		if !ok {
			if ri.filter != nil {
				// ri.expr is not nil, indicating that this part of right invertIndex also has data.
				res.invertStates[sid] = NewInvertStatesAndIntersect(sid, lIss.invertState, ri.filter)
			}
			continue
		}

		var l, r int
		for l < len(lIss.invertState) && r < len(rIss.invertState) {
			if lIss.invertState[l].timestamp < rIss.invertState[r].timestamp {
				if ri.filter != nil {
					res.AddInvertState(sid, InvertState{
						timestamp: lIss.invertState[l].timestamp,
						filter:    newIntersectExpr(lIss.invertState[l].filter, ri.filter)})
				}
				l++
				continue
			}
			if lIss.invertState[l].timestamp > rIss.invertState[r].timestamp {
				if li.filter != nil {
					res.AddInvertState(sid, InvertState{
						timestamp: rIss.invertState[r].timestamp,
						filter:    newIntersectExpr(rIss.invertState[r].filter, li.filter)})
				}
				r++
				continue
			}
			res.AddInvertState(sid, InvertState{
				timestamp: lIss.invertState[l].timestamp,
				filter:    newIntersectExpr(rIss.invertState[r].filter, lIss.invertState[l].filter)})
			l++
			r++
		}

		for (ri.filter != nil) && (l < len(lIss.invertState)) {
			res.AddInvertState(sid, InvertState{
				timestamp: lIss.invertState[l].timestamp,
				filter:    newIntersectExpr(lIss.invertState[l].filter, ri.filter)})
			l++
		}

		for (li.filter != nil) && (r < len(rIss.invertState)) {
			res.AddInvertState(sid, InvertState{
				timestamp: rIss.invertState[r].timestamp,
				filter:    newIntersectExpr(rIss.invertState[r].filter, li.filter)})
			r++
		}

		// last, delete the sid.
		if li.filter != nil {
			delete(ri.invertStates, sid)
		}
	}

	if li.filter != nil {
		for sid, rIss := range ri.invertStates {
			res.invertStates[sid] = NewInvertStatesAndIntersect(sid, rIss.invertState, li.filter)
		}
		if ri.filter != nil {
			res.filter = newIntersectExpr(li.filter, ri.filter)
		}
	}

	return &res
}

func IntersectInvertIndexAndExpr(li, ri *InvertIndex) *InvertIndex {
	if li.filter != nil && len(li.invertStates) == 0 &&
		ri.filter != nil && len(ri.invertStates) == 0 {
		li.filter = newIntersectExpr(li.filter, ri.filter)
		return li
	}

	if li.filter != nil && len(li.invertStates) == 0 {
		return IntersectExprToInvertIndex(ri, li.filter)
	}

	if ri.filter != nil && len(ri.invertStates) == 0 {
		return IntersectExprToInvertIndex(li, ri.filter)
	}

	return IntersectInvertIndexBySlip(li, ri)
}

func newUnionExpr(lexpr, rexpr influxql.Expr) influxql.Expr {
	if lexpr != nil && rexpr != nil {
		return influxql.Reduce(
			&influxql.BinaryExpr{
				Op:  influxql.OR,
				LHS: lexpr,
				RHS: rexpr,
			}, nil)
	}
	return nil
}

func NewInvertStatesAndUion(sid uint64, invertState []InvertState, filter influxql.Expr) *InvertStates {
	invertStates := NewInvertStates()
	invertStates.sid = sid
	invertStates.invertState = append(invertStates.invertState, invertState...)
	if filter != nil {
		for i := 0; i < len(invertStates.invertState); i++ {
			invertStates.invertState[i].filter = newUnionExpr(invertStates.invertState[i].filter, filter)
		}
	}
	return invertStates
}

func UnionExprToInvertIndex(ii *InvertIndex, filter influxql.Expr) *InvertIndex {
	for _, iss := range ii.invertStates {
		for i := 0; i < len(iss.invertState); i++ {
			iss.invertState[i].filter = newUnionExpr(iss.invertState[i].filter, filter)
		}
	}
	if ii.filter != nil {
		ii.filter = newUnionExpr(ii.filter, filter)
	} else {
		ii.filter = filter
	}
	return ii
}

func UnionInvertIndexBySlip(li, ri *InvertIndex) *InvertIndex {
	res := NewInvertIndex()
	for sid, lIss := range li.invertStates {
		rIss, ok := ri.invertStates[sid]
		if !ok {
			res.invertStates[sid] = NewInvertStatesAndUion(sid, lIss.invertState, ri.filter)
			continue
		}
		var l, r int
		for l < len(lIss.invertState) && r < len(rIss.invertState) {
			if lIss.invertState[l].timestamp < rIss.invertState[r].timestamp {
				if ri.filter != nil {
					lIss.invertState[l].filter = newUnionExpr(lIss.invertState[l].filter, ri.filter)
				}
				res.AddInvertState(sid, lIss.invertState[l])
				l++
				continue
			}
			if lIss.invertState[l].timestamp > rIss.invertState[r].timestamp {
				if li.filter != nil {
					rIss.invertState[r].filter = newUnionExpr(rIss.invertState[r].filter, li.filter)
				}
				res.AddInvertState(sid, rIss.invertState[r])
				r++
				continue
			}
			res.AddInvertState(sid, InvertState{
				timestamp: lIss.invertState[l].timestamp,
				filter:    newUnionExpr(lIss.invertState[l].filter, rIss.invertState[r].filter)})

			l++
			r++
		}
		delete(ri.invertStates, sid)
	}
	for sid, rIss := range ri.invertStates {
		res.invertStates[sid] = NewInvertStatesAndUion(sid, rIss.invertState, li.filter)
	}

	if li.filter != nil && ri.filter != nil {
		res.filter = newUnionExpr(li.filter, ri.filter)
	}
	if li.filter != nil {
		res.filter = li.filter
	} else {
		res.filter = ri.filter
	}

	return &res
}

// Union's principle:
// (1) the part of same data, use the union of lexpr and rexpr;
// (2) only the part of LHS, use the lexpr;
// (3) only the part of RHS, use the rexpr;
func UnionInvertIndexAndExpr(li *InvertIndex, ri *InvertIndex) *InvertIndex {
	if li.filter != nil && len(li.invertStates) == 0 &&
		ri.filter != nil && len(ri.invertStates) == 0 {
		li.filter = newUnionExpr(li.filter, ri.filter)
		return li
	}

	if li.filter != nil && len(li.invertStates) == 0 {
		return UnionExprToInvertIndex(ri, li.filter)
	}

	if ri.filter != nil && len(ri.invertStates) == 0 {
		return UnionExprToInvertIndex(li, ri.filter)
	}

	return UnionInvertIndexBySlip(li, ri)
}
