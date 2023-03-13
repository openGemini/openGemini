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
	"bytes"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/clipperhouse/uax29/iterators/filter"
	"github.com/clipperhouse/uax29/words"
	"github.com/pkg/errors"
)

const (
	Blank   = 0
	Default = 1
	Qmax    = 7
	T       = 100
	N       = 500000
)

const (
	txPrefixDic = iota
	txPrefixDicVersion
	txSuffix = 9
)

const (
	DefaultCap = 16
)

type VToken struct {
	tokens []string
	id     uint32
}

// for read
type TrieDicNode struct {
	child []*TrieDicNode
	token string
	id    uint32
}

func NewTrieDicNode() *TrieDicNode {
	return &TrieDicNode{}
}

func (tree *TrieDicNode) FindNodeByToken(token string) *TrieDicNode {
	if len(tree.child) == 0 {
		return nil
	}

	n := sort.Search(len(tree.child), func(i int) bool {
		return token <= tree.child[i].token
	})

	if n == len(tree.child) || tree.child[n].token != token {
		return nil
	}

	return tree.child[n]
}

func (tree *TrieDicNode) FindNodeById(id uint32) *TrieDicNode {
	if len(tree.child) == 0 {
		return nil
	}

	n := sort.Search(len(tree.child), func(i int) bool {
		return id <= tree.child[i].id
	})

	if n == len(tree.child) {
		return nil
	}

	return tree.child[n]
}

// for write
type TrieWriteNode struct {
	children  map[string]*TrieWriteNode
	frequency int
}

type TrieWriteTree struct {
	root      TrieWriteNode
	lock      sync.RWMutex
	sampleNum uint32
}

type TokenInfo struct {
	token     string
	frequency int
}

func NewTrieWriteNode() *TrieWriteNode {
	return &TrieWriteNode{
		children: make(map[string]*TrieWriteNode),
	}
}

func NewTrieWriteTree() *TrieWriteTree {
	writeTree := &TrieWriteTree{}
	writeTree.root.children = make(map[string]*TrieWriteNode)
	return writeTree
}

func (tree *TrieWriteTree) insertNode(tokens []string) {
	node := &tree.root
	for _, token := range tokens {
		child, ok := node.children[token]
		if !ok {
			child = NewTrieWriteNode()
			node.children[token] = child
		}
		node = child
		node.frequency++
	}
}

func (tree *TrieWriteTree) Insert(tokens []string) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	tree.sampleNum++
	for start := 0; start < len(tokens); start++ {
		end := start + Qmax
		if end > len(tokens) {
			end = len(tokens)
		}
		tree.insertNode(tokens[start:end])
	}
}

// Prunce by the frequency threshold
func (tree *TrieWriteTree) pruneNode(node *TrieWriteNode, th int) {
	tokens := make([]TokenInfo, len(node.children))
	i := 0
	for token, child := range node.children {
		tokens[i].token = token
		tokens[i].frequency = child.frequency
		i++
	}

	for i = 0; i < len(tokens); i++ {
		if tokens[i].frequency < th {
			delete(node.children, tokens[i].token)
		}
	}

	for _, child := range node.children {
		tree.pruneNode(child, th)
	}
}

func (tree *TrieWriteTree) Prune(th int) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	node := &tree.root
	tree.pruneNode(node, th)
}

// Transfer Node
func (tree *TrieWriteTree) transferNode(token string, node *TrieWriteNode) *TrieDicNode {
	dicNode := NewTrieDicNode()
	dicNode.token = token
	dicNode.child = make([]*TrieDicNode, len(node.children))

	i := 0
	for token, child := range node.children {
		tmpDicNode := tree.transferNode(token, child)
		dicNode.child[i] = tmpDicNode
		i++
	}

	child := dicNode.child
	sort.SliceStable(child, func(i, j int) bool {
		return string(child[i].token) > string(child[j].token)
	})

	return dicNode
}

func (tree *TrieWriteTree) Transfer() *TrieDicNode {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	node := &tree.root
	newRoot := tree.transferNode("", node)

	return newRoot
}

type Analyzer struct {
	readTree    *TrieDicNode
	writeTree   *TrieWriteTree
	path        string
	measurement string
	field       string
	version     uint32

	needSample bool
}

func newAnalyzer(path, measurement, field string) *Analyzer {
	return &Analyzer{
		path:        path,
		measurement: measurement,
		field:       field,
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

func (a *Analyzer) InsertToWriteTree(tokens []string) error {
	if a.writeTree == nil {
		return nil
	}

	a.writeTree.Insert(tokens)
	if a.writeTree.sampleNum >= N {
		a.needSample = false
		a.writeTree.Prune(T)
		dicTree := a.writeTree.Transfer()
		newVersion, err := a.saveToMergeSet(dicTree)
		if err != nil {
			return err
		}

		a.writeTree = nil
		analyzer := newAnalyzer(a.path, a.measurement, a.field)
		analyzer.readTree = dicTree
		analyzer.version = newVersion
		analyzer.saveToCache()
		err = a.saveNewVersion(newVersion)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Analyzer) saveToCache() {
	if _, ok := clvAnalyzerCache[a.measurement]; !ok {
		clvAnalyzerCache[a.measurement] = make(map[string][]*Analyzer)
	}
	if _, ok := clvAnalyzerCache[a.measurement][a.field]; !ok {
		clvAnalyzerCache[a.measurement][a.field] = make([]*Analyzer, 0)
	}
	clvAnalyzerCache[a.measurement][a.field] = append(clvAnalyzerCache[a.measurement][a.field], a)
}

func (a *Analyzer) saveNewVersion(version uint32) error {
	if _, ok := clvAnalyzerVersion[a.measurement]; !ok {
		clvAnalyzerVersion[a.measurement] = make(map[string]uint32)
	}

	clvAnalyzerVersion[a.measurement][a.field] = version

	var b []byte
	b = append(b, txPrefixDicVersion)
	b = encoding.MarshalUint32(b, version)
	tb, err := mergeset.OpenTable(path.Join(a.path, a.measurement, a.field), nil, nil)
	if err != nil {
		return err
	}
	defer tb.MustClose()

	return tb.AddItems([][]byte{b})
}

func (a *Analyzer) insertToReadDic(tokens []string) {
	if a.readTree == nil {
		a.readTree = NewTrieDicNode()
	}
	node := a.readTree

	for i := 0; i < len(tokens); i++ {
		child := node.FindNodeByToken(tokens[i])
		if child == nil {
			child = NewTrieDicNode()
			child.token = tokens[i]
			node.child = append(node.child, child)
		}
		node = child
	}
}

func (a *Analyzer) insertToReadTree(tokens string) {
	ts := strings.Split(tokens, " ")
	a.insertToReadDic(ts)
}

func (a *Analyzer) saveToMergeSet(root *TrieDicNode) (uint32, error) {
	items := make([][]byte, 0)
	_, nextVersion, _ := getAnalyzerVersion(a.path, a.measurement, a.field)

	b := dictPrefix(nextVersion + 1)

	readFromDicTree(root, b, &items)
	tb, err := mergeset.OpenTable(path.Join(a.path, a.measurement, a.field), nil, nil)
	if err != nil {
		return 0, err
	}
	defer tb.MustClose()
	err = tb.AddItems(items)
	if err != nil {
		return 0, err
	}

	return nextVersion, nil
}

func readFromDicTree(node *TrieDicNode, token []byte, items *[][]byte) {
	var b []byte
	b = append(b, token...)
	b = append(b, []byte(node.token)...)

	if node.child == nil || len(node.child) == 0 {
		*items = append(*items, b)
	} else {
		b = append(b, ' ')
	}

	for _, n := range node.child {
		readFromDicTree(n, b, items)
	}
}

func (a *Analyzer) AssignId(id uint32, node *TrieDicNode) uint32 {
	for _, child := range node.child {
		child.id = id
		id++
		id = a.AssignId(id, child)
	}
	return id
}

func (a *Analyzer) findLongestTokens(tokens []string) (VToken, int) {
	var i int
	var vtoken VToken
	node := a.readTree
	// for default analyzer.
	if node == nil {
		vtoken.tokens = append(vtoken.tokens, tokens[0])
		return vtoken, 1
	}

	// for learning-type analyzer
	for ; i < len(tokens); i++ {
		child := node.FindNodeByToken(tokens[i])
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

func (a *Analyzer) Analyze(log string) (map[uint16]VToken, error) {
	tokens := Tokenizer(log)
	if a.needSample {
		err := a.InsertToWriteTree(tokens)
		if err != nil {
			return nil, err
		}
	}

	var i int
	vtokens := make(map[uint16]VToken)
	for i < len(tokens) {
		vtoken, j := a.findLongestTokens(tokens[i:])
		i = i + j
		vtokens[uint16(i)] = vtoken
	}

	return vtokens, nil
}

func (a *Analyzer) Version() uint32 {
	return a.version
}

// (measuremnt, field, version) -> Analyzer
var clvAnalyzerCache = make(map[string]map[string][]*Analyzer)
var clvAnalyzerVersion = make(map[string]map[string]uint32)

// return last version , next version
func getAnalyzerVersion(dicPath, name, field string) (uint32, uint32, error) {
	tb, err := mergeset.OpenTable(path.Join(dicPath, name, field), nil, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tb.MustClose()

	ts := &mergeset.TableSearch{}
	ts.Init(tb)
	defer ts.MustClose()

	var latestVersion uint32
	b := make([]byte, 0)
	b = append(b, txPrefixDicVersion)
	ts.Seek(b)
	if ts.Error() != nil {
		return 0, 0, ts.Error()
	}
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, b) {
			break
		}
		if latestVersion < encoding.UnmarshalUint32(item[len(b):]) {
			latestVersion = encoding.UnmarshalUint32(item[len(b):])
		}
	}

	nextVersion := latestVersion + 1
	for {
		b = dictPrefix(nextVersion)
		ts.Seek(b)
		if ts.NextItem() {
			nextVersion++
		} else {
			break
		}
	}

	if _, ok := clvAnalyzerVersion[name]; !ok {
		clvAnalyzerVersion[name] = make(map[string]uint32)
	}
	clvAnalyzerVersion[name][field] = latestVersion

	return latestVersion, nextVersion, nil
}

func dictPrefix(version uint32) []byte {
	var prefix []byte
	prefix = append(prefix, txPrefixDic)
	prefix = encoding.MarshalUint32(prefix, version)
	prefix = append(prefix, txSuffix)
	return prefix
}

func loadAnalyzer(dicPath, name, field string, version uint32) (*Analyzer, error) {
	tb, err := mergeset.OpenTable(path.Join(dicPath, name, field), nil, nil)
	if err != nil {
		return nil, err
	}
	defer tb.MustClose()

	ts := &mergeset.TableSearch{}
	ts.Init(tb)
	defer ts.MustClose()

	a := newAnalyzer(dicPath, name, field)
	a.needSample = false
	a.version = version

	prefix := dictPrefix(version)
	ts.Seek(prefix)
	if ts.Error() != nil {
		return nil, errors.Wrap(ts.Error(), "error version or measurement")
	}
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		// insert tree
		a.insertToReadTree(string(item[len(prefix):]))
	}

	a.saveToCache()
	return a, nil
}

// find from the cache, if not existed, load from mergeset table.
func getAnalyzer(dicPath, name, field string, version uint32) (*Analyzer, error) {
	if analyzers, ok := clvAnalyzerCache[name][field]; ok {
		for _, a := range analyzers {
			if a.version == version {
				return a, nil
			}
		}
	}

	a, err := loadAnalyzer(dicPath, name, field, version)
	if err != nil {
		return nil, err
	}

	return a, nil
}

func GetAnalyzer(dicPath, name, field string, version uint32) (*Analyzer, error) {
	var err error
	latestVersion, ok := clvAnalyzerVersion[name][field]
	if !ok {
		latestVersion, _, err = getAnalyzerVersion(dicPath, name, field)
		if err != nil {
			return nil, err
		}
	}

	if version == Blank {
		version = latestVersion
	}

	var a *Analyzer
	if version != Blank {
		a, err = getAnalyzer(dicPath, name, field, version)
		if err != nil {
			return nil, err
		}
	} else {
		a = newAnalyzer(dicPath, name, field)
		a.needSample = true
		a.writeTree = NewTrieWriteTree()
		a.version = version
		a.saveToCache()
		_ = a.saveNewVersion(version)
	}

	return a, nil
}
