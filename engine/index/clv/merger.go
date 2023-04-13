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
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

const (
	fixedLen    = 7 // len(txSuffix) + len(prefixMeta) + len(flag) + len(metaOffset) + [len(SIDs)|len(IDs)] : 1+1+1+2+2
	maxItemSize = 64 * 1024
)

type merger struct {
	items     [][]byte
	prefixKey []byte
	len       int
}

func (m *merger) addItem(item []byte) {
	if len(item) == 0 {
		return
	}

	m.items = append(m.items, item)
	if len(m.items) == 1 {
		prefix := bytes.IndexByte(item, txSuffix)
		m.prefixKey = m.items[0][:prefix]
		m.len = len(m.prefixKey) + fixedLen
	}
	m.len += len(item) - len(m.prefixKey) - fixedLen
}

func (m *merger) reset() {
	m.items = m.items[:0]
	m.len = 0
}

func (m *merger) keysEqual(item []byte) bool {
	if len(m.items) == 0 {
		return true
	}
	// The length of a single item cannot be greater than 64KB
	if m.len+len(item) > maxItemSize {
		return false
	}

	prefix := bytes.IndexByte(item, txSuffix)
	return string(m.prefixKey) == string(item[:prefix])
}

func (m *merger) flushPendingItem(dst []byte, dstItems []mergeset.Item) ([]byte, []mergeset.Item) {
	if len(m.items) == 0 {
		return dst, dstItems
	}

	dataLen := len(dst)
	if len(m.items) == 1 {
		dst = append(dst, m.items[0]...)
		dstItems = append(dstItems, mergeset.Item{
			Start: uint32(dataLen),
			End:   uint32(len(dst)),
		})
		m.reset()
		return dst, dstItems
	}

	// umasharl
	invertIndex := NewInvertIndex()
	for i := 0; i < len(m.items); i++ {
		unmarshal(m.items[i], &invertIndex)
	}
	// masharl
	dst = marshal(dst, string(m.prefixKey[1:]), &invertIndex)
	dstItems = append(dstItems, mergeset.Item{
		Start: uint32(dataLen),
		End:   uint32(len(dst)),
	})

	m.reset()
	return dst, dstItems
}

type MergerPool struct {
	p sync.Pool
}

func (mp *MergerPool) Get() *merger {
	m := mp.p.Get()
	if m == nil {
		return &merger{
			items: make([][]byte, 0, DefaultCap),
		}
	}
	return m.(*merger)
}

func (mp *MergerPool) Put(m *merger) {
	m.reset()
	mp.p.Put(m)
}

func keysEqual(item0 []byte, item1 []byte) bool {
	prefix := bytes.IndexByte(item0, txSuffix)
	if len(item1) < prefix {
		return false
	}

	return string(item0[:prefix]) == string(item1[:prefix])
}

var mergerPool MergerPool

// Perform quick checks whether items contain rows starting from NsPrefix based on the fact that items are sorted.
func mergeDocIdxItems(data []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
	if len(items) <= 2 {
		return data, items
	}
	firstItem := items[0].Bytes(data)
	if len(firstItem) > 0 && firstItem[0] > txPrefixPos {
		return data, items
	}
	lastItem := items[len(items)-1].Bytes(data)
	if len(lastItem) > 0 && lastItem[0] < txPrefixPos {
		return data, items
	}

	dstData := data[:0]
	dstItems := items[:0]

	// The merged item of multiple items may be smaller than the old items. Special
	// processing is required for the first item: Items with the same key as the first item will not be merged.
	var noMerge bool
	var item0 []byte
	m := mergerPool.Get()
	for i, it := range items {
		item := it.Bytes(data)

		if i == 0 {
			item0 = item
			noMerge = true
		}

		if noMerge {
			noMerge = keysEqual(item0, item)
		}

		if len(item) == 0 || item[0] != txPrefixPos || i == 0 || i == len(items)-1 || noMerge {
			dstData, dstItems = m.flushPendingItem(dstData, dstItems)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, mergeset.Item{
				Start: uint32(len(dstData) - len(item)),
				End:   uint32(len(dstData)),
			})
			continue
		}

		if !m.keysEqual(item) {
			dstData, dstItems = m.flushPendingItem(dstData, dstItems)
		}

		m.addItem(item)
	}
	mergerPool.Put(m)

	return dstData, dstItems
}
