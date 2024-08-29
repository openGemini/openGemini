//nolint
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

package ski

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
)

type shardKeyToSeriesIdParser struct {
	Name     []byte
	shardKey []byte
	mergeindex.BasicRowParser
}

// [shardKeyLen] [shardKey] [mstLen][sids]
func marshalShardKey(dst, name, shardKey []byte) []byte {
	dst = encoding.MarshalVarUint64(dst, uint64(len(shardKey)))
	dst = append(dst, shardKey...)
	dst = encoding.MarshalVarUint64(dst, uint64(len(name)))
	return dst
}

func unmarshalShardKey(dst, src []byte) ([]byte, []byte, error) {
	src, l, err := encoding.UnmarshalVarUint64(src)
	if err != nil {
		return nil, nil, err
	}
	dst = append(dst, src[:l]...)
	return dst, src[l:], nil
}

func unmarshalMeasurement(dst, src []byte) ([]byte, []byte, error) {
	src, l, err := encoding.UnmarshalVarUint64(src)
	if err != nil {
		return nil, nil, err
	}
	return dst[:l], src, nil
}

func (mp *shardKeyToSeriesIdParser) Init(b []byte, nsPrefixExpected byte) error {
	tail, err := mp.InitCommon(b, nsPrefixExpected)
	if err != nil {
		return err
	}
	mp.shardKey, tail, err = unmarshalShardKey(mp.shardKey[:0], tail)
	if err != nil {
		return err
	}

	mp.Name, tail, err = unmarshalMeasurement(mp.shardKey, tail)
	if err != nil {
		return err
	}
	return mp.InitOnlyTail(b, tail)
}

func (mp *shardKeyToSeriesIdParser) Reset() {
	mp.shardKey = nil
	mp.Name = nil
	mp.ResetCommon()
}

func (mp *shardKeyToSeriesIdParser) MarshalPrefix(dst []byte) []byte {
	dst = mp.MarshalPrefixCommon(dst)
	dst = encoding.MarshalVarUint64(dst, uint64(len(mp.shardKey)))
	dst = append(dst, mp.shardKey...)
	dst = encoding.MarshalVarUint64(dst, uint64(len(mp.Name)))
	return dst
}

func (mp *shardKeyToSeriesIdParser) EqualPrefix(x mergeindex.ItemParser) bool {
	switch x := x.(type) {
	case *shardKeyToSeriesIdParser:
		return mp.NsPrefix == x.NsPrefix && string(mp.shardKey) == string(x.shardKey)
	default:
		return false
	}
}

func (mp *shardKeyToSeriesIdParser) GetTSIDs() []uint64 {
	return mp.TSIDs
}

type shardKeyToTSIDsRowsMerger struct {
	mp     shardKeyToSeriesIdParser
	mpPrev shardKeyToSeriesIdParser
	mergeindex.BasicRowMerger
}

func (srm *shardKeyToTSIDsRowsMerger) Reset() {
	srm.mp.Reset()
	srm.mpPrev.Reset()
	srm.ResetCommon()
}

func (srm *shardKeyToTSIDsRowsMerger) GetMergeParser() mergeindex.ItemParser {
	return &srm.mp
}

func (srm *shardKeyToTSIDsRowsMerger) GetPreMergeParser() mergeindex.ItemParser {
	return &srm.mpPrev
}

func getShardKeyToTSIDsRowsMerger() *shardKeyToTSIDsRowsMerger {
	v := srmPool.Get()
	if v == nil {
		return &shardKeyToTSIDsRowsMerger{}
	}
	return v.(*shardKeyToTSIDsRowsMerger)
}

func putShardKeyToTSIDsRowsMerger(srm *shardKeyToTSIDsRowsMerger) {
	srm.Reset()
	srmPool.Put(srm)
}

var srmPool sync.Pool
