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
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/lib/config"
)

var Qmax = 7
var T = 100
var N uint32 = 500000

func Init(cfg *config.ClvConfig) {
	if cfg != nil {
		Qmax = cfg.QMax
		T = cfg.T
		N = cfg.N
	}
}

// the status of collector
const (
	Stopped    int32 = 0
	Collecting int32 = 1
	Flushing   int32 = 2
)

type tokenInfo struct {
	token     string
	frequency int
}

type dicItems struct {
	items [][]byte
}

func newDicItems() *dicItems {
	return &dicItems{
		items: make([][]byte, 0),
	}
}

type collectNode struct {
	children  map[string]*collectNode
	frequency int
}

func newCollectNode() *collectNode {
	return &collectNode{
		children: make(map[string]*collectNode),
	}
}

type collector struct {
	root       *collectNode
	lock       sync.RWMutex
	collectNum uint32
	status     int32

	path        string
	measurement string
	field       string
}

func newCollector(path, measurement, field string) *collector {
	return &collector{
		path:        path,
		measurement: measurement,
		field:       field,
	}
}

func (d *collector) setStatus(status int32) {
	atomic.StoreInt32(&d.status, status)
}

func (d *collector) getStatus() int32 {
	return atomic.LoadInt32(&d.status)
}

func (d *collector) insertNode(tokens []string) {
	node := d.root
	for _, token := range tokens {
		child, ok := node.children[token]
		if !ok {
			child = newCollectNode()
			node.children[token] = child
		}
		node = child
		node.frequency++
	}
}

func (d *collector) insert(tokens []string) {
	if d.root == nil {
		d.root = newCollectNode()
	}

	for start := 0; start < len(tokens); start++ {
		end := start + Qmax
		if end > len(tokens) {
			end = len(tokens)
		}
		d.insertNode(tokens[start:end])
	}
}

// Prunce by the frequency threshold
func (d *collector) pruneNode(node *collectNode, th int) {
	tokens := make([]tokenInfo, len(node.children))
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
		d.pruneNode(child, th)
	}
}

func (d *collector) prune(th int) {
	if d.root == nil {
		return
	}
	d.pruneNode(d.root, th)
}

func (d *collector) genItemsFromCollectTree(node *collectNode, tokensCount int, vtoken []byte, dicItems *dicItems) {
	if len(node.children) == 0 {
		if tokensCount <= 1 { // only one token, no need to save.
			return
		}
		dicItems.items = append(dicItems.items, vtoken)
		// fot test
		return
	}

	tokensCount++
	for token, child := range node.children {
		var tokens []byte
		tokens = append(tokens, vtoken...)
		tokens = append(tokens, []byte(token)...)
		tokens = append(tokens, ' ')
		d.genItemsFromCollectTree(child, tokensCount, tokens, dicItems)
	}
}
func (d *collector) genItemsByVersion(version uint32, dicItems *dicItems) {
	var b []byte
	b = marshalDicVersion(b, version)
	dicItems.items = append(dicItems.items, b)
}

func (d *collector) saveDictionaryToMergeset() error {
	nextVersion, _ := getNextValidVersion(d.path, d.measurement, d.field)
	dicItems := newDicItems()
	b := genPrefixForDic(nextVersion)

	d.genItemsFromCollectTree(d.root, 0, b, dicItems)
	d.genItemsByVersion(nextVersion, dicItems)

	tb, err := mergeset.OpenTable(path.Join(d.path, d.measurement, d.field), nil, nil)
	if err != nil {
		return err
	}
	defer tb.MustClose()
	return tb.AddItems(dicItems.items)
}

func (d *collector) saveDictionary() {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.prune(T)
	err := d.saveDictionaryToMergeset()
	if err != nil {
		panic(fmt.Errorf("save the dictionary failed, err: %+v", err))
	}
	d.root = nil
	d.StopCollect()
}

func (d *collector) IsStopped() bool {
	return atomic.LoadInt32(&d.status) == Stopped
}

func (d *collector) StartCollect() {
	d.setStatus(Collecting)
}

func (d *collector) StopCollect() {
	d.setStatus(Stopped)
}

func (d *collector) Collect(tokens []string) {
	if d.getStatus() != Collecting {
		return
	}

	d.lock.Lock()

	d.insert(tokens)
	d.collectNum++
	if d.collectNum >= N {
		d.setStatus(Flushing)
		d.collectNum = 0
		go d.saveDictionary()
	}

	d.lock.Unlock()
}
