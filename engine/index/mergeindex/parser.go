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

package mergeindex

import (
	"fmt"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/openGemini/openGemini/open_src/vm/uint64set"
)

type ItemParser interface {
	Init(b []byte, nsPrefixExpected byte) error
	Reset()
	MarshalPrefix(dst []byte) []byte
	FirstAndLastTSIDs() (uint64, uint64)
	TSIDsLen() int
	ParseTSIDs()
	HasCommonTSIDs(filter *uint64set.Set) bool
	IsExpectedTag(deletedTSIDs *uint64set.Set, eligibleTSIDs *uint64set.Set) bool
	EqualPrefix(x ItemParser) bool
	GetTSIDs() []uint64
}

type BasicRowParser struct {
	NsPrefix byte

	TSIDs []uint64

	tail []byte
}

func (brp *BasicRowParser) ResetCommon() {
	brp.NsPrefix = 0
	brp.TSIDs = brp.TSIDs[:0]
	brp.tail = nil
}

func (brp *BasicRowParser) InitCommon(b []byte, nsPrefixExpected byte) ([]byte, error) {
	tail, nsPrefix, err := unmarshalCommonPrefix(b)
	if err != nil {
		return tail, fmt.Errorf("invalid row %q: %w", b, err)
	}
	if nsPrefix != nsPrefixExpected {
		return tail, fmt.Errorf("invalid prefix for row %q; got %d; want %d", b, nsPrefix, nsPrefixExpected)
	}

	brp.NsPrefix = nsPrefix
	return tail, nil
}

func (brp *BasicRowParser) InitOnlyTail(b, tail []byte) error {
	if len(tail) == 0 {
		return fmt.Errorf("missing tsid in row %q", b)
	}
	if len(tail)%8 != 0 {
		return fmt.Errorf("invalid tail length in row; got %d bytes; must be multiple of 8 bytes", len(tail))
	}
	brp.tail = tail
	return nil
}

func (brp *BasicRowParser) MarshalPrefixCommon(dst []byte) []byte {
	dst = MarshalCommonPrefix(dst, brp.NsPrefix)
	return dst
}

func (brp *BasicRowParser) HasCommonTSIDs(filter *uint64set.Set) bool {
	for _, sid := range brp.TSIDs {
		if filter.Has(sid) {
			return true
		}
	}
	return false
}

func (brp *BasicRowParser) IsExpectedTag(deletedTSIDs *uint64set.Set, eligibleTSIDs *uint64set.Set) bool {
	if eligibleTSIDs != nil && eligibleTSIDs.Len() == 0 {
		return false
	}

	brp.ParseTSIDs()
	for _, tsid := range brp.TSIDs {
		if !deletedTSIDs.Has(tsid) && (eligibleTSIDs == nil || eligibleTSIDs.Has(tsid)) {
			return true
		}
	}
	return false
}

func (brp *BasicRowParser) ParseTSIDs() {
	tail := brp.tail
	brp.TSIDs = brp.TSIDs[:0]
	n := len(tail) / 8
	if n <= cap(brp.TSIDs) {
		brp.TSIDs = brp.TSIDs[:n]
	} else {
		brp.TSIDs = append(brp.TSIDs[:cap(brp.TSIDs)], make([]uint64, n-cap(brp.TSIDs))...)
	}
	tsids := brp.TSIDs
	_ = tsids[n-1]
	for i := 0; i < n; i++ {
		if len(tail) < 8 {
			logger.Panicf("BUG: tail cannot be smaller than 8 bytes; got %d bytes; tail=%X", len(tail), tail)
			return
		}
		tsid := encoding.UnmarshalUint64(tail)
		tsids[i] = tsid
		tail = tail[8:]
	}
}

func (brp *BasicRowParser) TSIDsLen() int {
	return len(brp.tail) / 8
}

func (brp *BasicRowParser) FirstAndLastTSIDs() (uint64, uint64) {
	tail := brp.tail
	if len(tail) < 8 {
		logger.Panicf("BUG: cannot unmarshal tsid from %d bytes; need 8 bytes", len(tail))
		return 0, 0
	}
	firstTSIDs := encoding.UnmarshalUint64(tail)
	lastTSIDs := firstTSIDs
	if len(tail) > 8 {
		lastTSIDs = encoding.UnmarshalUint64(tail[len(tail)-8:])
	}
	return firstTSIDs, lastTSIDs
}

type uint64Sorter []uint64

func (s uint64Sorter) Len() int { return len(s) }
func (s uint64Sorter) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s uint64Sorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type BasicRowMerger struct {
	pendingTSIDs uint64Sorter
	itemsCopy    []mergeset.Item
	dataCopy     []byte
}

func (brm *BasicRowMerger) ResetCommon() {
	brm.pendingTSIDs = brm.pendingTSIDs[:0]
	brm.dataCopy = brm.dataCopy[:0]
	brm.itemsCopy = brm.itemsCopy[:0]
}

func (brm *BasicRowMerger) FlushPendingTSIDs(dstData []byte, dstItems []mergeset.Item, mp ItemParser) ([]byte, []mergeset.Item) {
	if len(brm.pendingTSIDs) == 0 {
		// Nothing to flush
		return dstData, dstItems
	}
	// Use sort.Sort instead of sort.Slice in order to reduce memory allocations.
	sort.Sort(&brm.pendingTSIDs)

	// marshal pendingTSIDs
	dstDataLen := len(dstData)
	dstData = mp.MarshalPrefix(dstData)
	for _, tsid := range brm.pendingTSIDs {
		dstData = encoding.MarshalUint64(dstData, tsid)
	}
	dstItems = append(dstItems, mergeset.Item{
		Start: uint32(dstDataLen),
		End:   uint32(len(dstData)),
	})
	brm.pendingTSIDs = brm.pendingTSIDs[:0]
	return dstData, dstItems
}

func (brm *BasicRowMerger) AppendTSIDs(tsids []uint64) {
	brm.pendingTSIDs = append(brm.pendingTSIDs, tsids...)
}

func (brm *BasicRowMerger) AppendData(data []byte) {
	brm.dataCopy = append(brm.dataCopy[:0], data...)
}

func (brm *BasicRowMerger) AppendItems(items []mergeset.Item) {
	brm.itemsCopy = append(brm.itemsCopy[:0], items...)
}

func (brm *BasicRowMerger) GetPendingTSIDs() []uint64 {
	return brm.pendingTSIDs
}
