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

package clv

import (
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

func TestMarshalAndUnmarshalDicVersion(t *testing.T) {
	dst := make([]byte, 0)
	dst = marshalDicVersion(dst, 12)
	version := unmarshaDiclVersion(dst)
	if version != 12 {
		t.Fatalf("umarshal dic version failed, version:%d", version)
	}
}

func TestMarshalAndUnmarshalInvert(t *testing.T) {
	// marshal
	vtokens := "get image a.png"
	oriInvert := InvertIndex{
		invertStates: map[uint64]*InvertStates{
			0: {
				invertState: []InvertState{{1, 0, nil}, {2, 1, nil}, {3, 4, nil}, {4, 2, nil}, {5, 7, nil}},
				sid:         0,
			},
			2: {
				invertState: []InvertState{{6, 7, nil}, {7, 3, nil}, {8, 1, nil}, {9, 4, nil}, {10, 2, nil}},
				sid:         2,
			},
		},
		ids: map[uint32]struct{}{
			1: struct{}{},
			2: struct{}{},
		},
		filter: nil,
	}
	dst := make([]byte, 0)
	dst = marshal(dst, vtokens, &oriInvert)

	// unmarshl
	newInvert := NewInvertIndex()
	unmarshal(dst, &newInvert, 0)

	// compare
	if !reflect.DeepEqual(oriInvert, newInvert) {
		t.Fatal()
	}
}

func genDocIdxItemsForTest() (*InvertIndex, *InvertIndex, *InvertIndex, *InvertIndex) {
	invert0 := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			0: {
				invertState: []InvertState{{1, 0, nil}, {2, 1, nil}, {3, 4, nil}, {4, 2, nil}, {5, 7, nil}},
				sid:         0,
			},
		},
		filter: nil,
	}
	invert1 := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}, {7, 3, nil}},
				sid:         2,
			},
		},
		ids: map[uint32]struct{}{
			1: struct{}{},
		},
		filter: nil,
	}
	invert2 := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{8, 1, nil}, {9, 4, nil}, {10, 2, nil}},
				sid:         2,
			},
		},
		ids: map[uint32]struct{}{
			2: struct{}{},
		},
		filter: nil,
	}
	mergeInvert := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			0: {
				invertState: []InvertState{{1, 0, nil}, {2, 1, nil}, {3, 4, nil}, {4, 2, nil}, {5, 7, nil}},
				sid:         0,
			},
			2: {
				invertState: []InvertState{{6, 7, nil}, {7, 3, nil}, {8, 1, nil}, {9, 4, nil}, {10, 2, nil}},
				sid:         2,
			},
		},
		ids: map[uint32]struct{}{
			1: struct{}{},
			2: struct{}{},
		},
		filter: nil,
	}

	return invert0, invert1, invert2, mergeInvert
}

func TestMergeDocIdxItems(t *testing.T) {
	vtokens := "get image a.png"
	invert0, invert1, invert2, mergeInvert := genDocIdxItemsForTest()

	dst := make([]byte, 0)
	items := make([]mergeset.Item, 0, 2)

	// marshal invert0
	dst = marshal(dst, vtokens, invert0)
	items = append(items, mergeset.Item{
		Start: 0,
		End:   uint32(len(dst)),
	})

	// marshal invert1
	start := uint32(len(dst))
	dst = marshal(dst, vtokens, invert1)
	items = append(items, mergeset.Item{
		Start: start,
		End:   uint32(len(dst)),
	})

	// marshal invert2
	start = uint32(len(dst))
	dst = marshal(dst, vtokens, invert2)
	items = append(items, mergeset.Item{
		Start: start,
		End:   uint32(len(dst)),
	})

	// merge the items
	data, dstItems := mergeDocIdxItems(dst, items)

	// unmarshal to invertIndex
	invertIndex := NewInvertIndex()
	for _, it := range dstItems {
		item := it.Bytes(data)
		unmarshal(item, &invertIndex, 0)
	}

	// compare
	if !reflect.DeepEqual(mergeInvert, &invertIndex) {
		t.Fatalf("merge wrong, exp:%+v, get:%+v", mergeInvert, invertIndex)
	}
}
