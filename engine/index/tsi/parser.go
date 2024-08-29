// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsi

import (
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/index/mergeindex"
)

type tagToTSIDsRowParser struct {
	// Date contains parsed date for nsPrefixDateTagToTSID rows after Init call
	Date uint64

	// Measurement name
	Name []byte

	// Tag contains parsed tag after Init call
	Tag Tag

	mergeindex.BasicRowParser
}

func (mp *tagToTSIDsRowParser) Reset() {
	mp.Date = 0
	mp.Tag.Reset()
	mp.Name = nil
	mp.ResetCommon()
}

// Init initializes mp from b, which should contain encoded tag->tsids row.
//
// b cannot be re-used until Reset call.
func (mp *tagToTSIDsRowParser) Init(b []byte, nsPrefixExpected byte) error {
	tail, err := mp.InitCommon(b, nsPrefixExpected)
	if err != nil {
		return err
	}
	tail, err = mp.Tag.Unmarshal(tail)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tag from tag->tsids row %q: %w", b, err)
	}

	mp.Tag.Key, mp.Name, err = unmarshalCompositeTagKey(mp.Tag.Key)
	if err != nil {
		return fmt.Errorf("cannot unmarshal composite tag key from tag->tsids row %q: %w", b, err)
	}

	return mp.InitOnlyTail(b, tail)
}

// MarshalPrefix marshals row prefix without tail to dst.
func (mp *tagToTSIDsRowParser) MarshalPrefix(dst []byte) []byte {
	dst = mp.MarshalPrefixCommon(dst)
	compositeKey := kbPool.Get()
	compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], mp.Name, mp.Tag.Key)
	dst = marshalTagValue(dst, compositeKey.B)
	dst = marshalTagValue(dst, mp.Tag.Value)
	kbPool.Put(compositeKey)
	return dst
}

// EqualPrefix returns true if prefixes for mp and x are equal.
// Prefix contains tag
func (mp *tagToTSIDsRowParser) EqualPrefix(x mergeindex.ItemParser) bool {
	switch x := x.(type) {
	case *tagToTSIDsRowParser:
		if !mp.Tag.Equal(&x.Tag) {
			return false
		}
		return mp.NsPrefix == x.NsPrefix && mp.Date == x.Date && string(mp.Name) == string(x.Name)
	default:
		return false
	}
}

func (mp *tagToTSIDsRowParser) GetTSIDs() []uint64 {
	return mp.TSIDs
}

type tagToTSIDsRowsMerger struct {
	mp     tagToTSIDsRowParser
	mpPrev tagToTSIDsRowParser
	mergeindex.BasicRowMerger
}

func (tmm *tagToTSIDsRowsMerger) Reset() {
	tmm.mp.Reset()
	tmm.mpPrev.Reset()
	tmm.ResetCommon()
}

func (tmm *tagToTSIDsRowsMerger) GetMergeParser() mergeindex.ItemParser {
	return &tmm.mp
}

func (tmm *tagToTSIDsRowsMerger) GetPreMergeParser() mergeindex.ItemParser {
	return &tmm.mpPrev
}

func getTagToTSIDsRowsMerger() *tagToTSIDsRowsMerger {
	v := tmmPool.Get()
	if v == nil {
		return &tagToTSIDsRowsMerger{}
	}
	return v.(*tagToTSIDsRowsMerger)
}

func putTagToTSIDsRowsMerger(tmm *tagToTSIDsRowsMerger) {
	tmm.Reset()
	tmmPool.Put(tmm)
}

var tmmPool sync.Pool
