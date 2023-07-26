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

package mergeindex

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

type ItemParserForLabelStore interface {
	Init(b []byte, nsPrefixExpected byte) error
	Reset()
	ResetValueSize()
	EqualPrefix(x ItemParserForLabelStore) bool
	MarshalPrefix(dst []byte) []byte
	MarshalValues(values [][]byte, dst []byte) []byte
	GetValuesSize() int
	GetValues() [][]byte
	GetPrifixSize() int
}

type BasicRowValuesParser struct {
	NsPrefix byte
}

func (brvp *BasicRowValuesParser) ResetCommon() {
	brvp.NsPrefix = 0
}

func (brvp *BasicRowValuesParser) InitCommon(b []byte, nsPrefixExpected byte) ([]byte, error) {
	tail, nsPrefix, err := unmarshalCommonPrefix(b)
	if err != nil {
		return tail, fmt.Errorf("invalid row %q: %w", b, err)
	}
	if nsPrefix != nsPrefixExpected {
		return tail, fmt.Errorf("invalid prefix for row %q; got %d; want %d", b, nsPrefix, nsPrefixExpected)
	}

	brvp.NsPrefix = nsPrefix
	return tail, nil
}

func (brvp *BasicRowValuesParser) MarshalPrefixCommon(dst []byte) []byte {
	dst = MarshalCommonPrefix(dst, brvp.NsPrefix)
	return dst
}

type byteSliceSorter [][]byte

func (b byteSliceSorter) Len() int { return len(b) }

func (b byteSliceSorter) Less(i, j int) bool {
	return bytes.Compare(b[i], b[j]) < 0
}

func (b byteSliceSorter) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

type BasicRowValuesMerger struct {
	pendingValues byteSliceSorter
	ItemSize      int
	prefixSize    int
}

func (brvm *BasicRowValuesMerger) ResetCommon() {
	for i := range brvm.pendingValues {
		brvm.pendingValues[i] = brvm.pendingValues[i][:0]
	}
}

func (brvm *BasicRowValuesMerger) FlushPendingValues(dstData []byte, dstItems []mergeset.Item, mp ItemParserForLabelStore) ([]byte, []mergeset.Item) {
	if len(brvm.pendingValues) == 0 {
		// Nothing to flush
		return dstData, dstItems
	}
	sort.Sort(brvm.pendingValues)

	dstDataLen := len(dstData)
	dstData = mp.MarshalPrefix(dstData)
	dstData = mp.MarshalValues(brvm.pendingValues, dstData)
	dstItems = append(dstItems, mergeset.Item{
		Start: uint32(dstDataLen),
		End:   uint32(len(dstData)),
	})
	brvm.pendingValues = brvm.pendingValues[:0]
	brvm.ItemSize = 0
	brvm.prefixSize = 0
	return dstData, dstItems
}

func (brvm *BasicRowValuesMerger) AppendValues(values [][]byte) {
	brvm.pendingValues = append(brvm.pendingValues, values...)
	for i := range values {
		brvm.ItemSize += len(values[i])
	}
}

func (brvm *BasicRowValuesMerger) SetPrifixSize(size int) {
	if brvm.prefixSize == 0 {
		brvm.prefixSize = size
		brvm.ItemSize += size
	}
}

func (brvm *BasicRowValuesMerger) GetPendingValuesSize(values [][]byte) int {
	for i := range values {
		brvm.ItemSize += len(values[i])
	}
	return brvm.ItemSize
}

func (brvm *BasicRowValuesMerger) GetPendingValues() int {
	return len(brvm.pendingValues)
}
