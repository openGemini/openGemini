// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"bytes"
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/index/mergeindex"
)

//lint:ignore U1000 keep this
type tagToValuesRowParser struct {
	// Measurement name
	Name []byte

	// Tag Key  Tag Values
	Key        []byte
	Values     [][]byte // v1 /char/ v2 /char/
	Size       int
	prefixSize int

	mergeindex.BasicRowValuesParser
}

//lint:ignore U1000 keep this
func (mp *tagToValuesRowParser) Reset() {
	mp.Key = mp.Key[:0]
	for i := range mp.Values {
		mp.Values[i] = mp.Values[i][:0]
	}
	mp.Name = nil
	mp.Size = 0
	mp.prefixSize = 0
	mp.ResetCommon()
}

//lint:ignore U1000 keep this
func (mp *tagToValuesRowParser) Init(b []byte, nsPrefixExpected byte) error {
	tail, err := mp.InitCommon(b, nsPrefixExpected)
	if err != nil {
		return err
	}
	err = mp.Unmarshal(tail)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tag from tag->values row %q: %w", b, err)
	}

	mp.Key, mp.Name, err = unmarshalCompositeTagKey(mp.Key)
	if err != nil {
		return fmt.Errorf("cannot unmarshal composite tag key from tag->values row %q: %w", b, err)
	}

	return nil
}

func (mp *tagToValuesRowParser) GetValuesSize() int {
	return mp.Size
}

func (mp *tagToValuesRowParser) ResetValueSize() {
	mp.prefixSize = 0
	mp.Size = 0
}

//lint:ignore U1000 keep this
func (mp *tagToValuesRowParser) MarshalPrefix(dst []byte) []byte {
	dst = mp.MarshalPrefixCommon(dst)
	compositeKey := kbPool.Get()
	compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], mp.Name, mp.Key)
	dst = marshalTagValue(dst, compositeKey.B)
	kbPool.Put(compositeKey)
	return dst
}

//lint:ignore U1000 keep this
func (mp *tagToValuesRowParser) MarshalValues(values [][]byte, dst []byte) []byte {
	for i := range values {
		dst = marshalTagValue(dst, values[i])
	}
	return dst
}

//lint:ignore U1000 keep this
func (mp *tagToValuesRowParser) EqualPrefix(x mergeindex.ItemParserForLabelStore) bool {
	switch x := x.(type) {
	case *tagToValuesRowParser:
		if !bytes.Equal(mp.Key, x.Key) {
			return false
		}
		return mp.NsPrefix == x.NsPrefix && bytes.Equal(mp.Name, x.Name)
	default:
		return false
	}
}

//lint:ignore U1000 keep this
func (mp *tagToValuesRowParser) GetValues() [][]byte {
	return mp.Values
}

func (mp *tagToValuesRowParser) GetPrifixSize() int {
	return mp.prefixSize
}

//lint:ignore U1000 keep this
func (mp *tagToValuesRowParser) Unmarshal(src []byte) error {
	var err error
	src, mp.Key, err = unmarshalTagValue(mp.Key[:0], src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal key: %w", err)
	}

	if mp.prefixSize == 0 {
		mp.Size += len(mp.Key)
		mp.prefixSize = len(mp.Key)
	}

	var value []byte
	mp.Values = mp.Values[:0]
	for {
		if len(src) == 0 {
			break
		}
		src, value, err = unmarshalTagValue(nil, src)
		if err != nil {
			return fmt.Errorf("cannot unmarshal value: %w", err)
		}
		mp.Size += len(value)
		mp.Values = append(mp.Values, value)
	}
	return nil
}

//lint:ignore U1000 keep this
type tagToValuesRowsMerger struct {
	mp     tagToValuesRowParser
	mpPrev tagToValuesRowParser
	mergeindex.BasicRowValuesMerger
}

//lint:ignore U1000 keep this
func (tmm *tagToValuesRowsMerger) Reset() {
	tmm.mp.Reset()
	tmm.mpPrev.Reset()
	tmm.ResetCommon()
}

//lint:ignore U1000 keep this
func (tmm *tagToValuesRowsMerger) GetMergeParser() mergeindex.ItemParserForLabelStore {
	return &tmm.mp
}

//lint:ignore U1000 keep this
func (tmm *tagToValuesRowsMerger) GetPreMergeParser() mergeindex.ItemParserForLabelStore {
	return &tmm.mpPrev
}

//lint:ignore U1000 keep this
func getTagToValuesRowsMerger() *tagToValuesRowsMerger {
	v := tvPool.Get()
	if v == nil {
		return &tagToValuesRowsMerger{}
	}
	return v.(*tagToValuesRowsMerger)
}

//lint:ignore U1000 keep this
func putTagToValuesRowsMerger(tmm *tagToValuesRowsMerger) {
	tmm.Reset()
	tvPool.Put(tmm)
}

var tvPool sync.Pool
