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
//nolint
package executor

import (
	"sort"
	"sync"
	"unsafe"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

type ChunkTags struct {
	subset  []byte
	offsets []uint16
}

func NewChunkTags(pts influx.PointTags, dimensions []string) *ChunkTags {
	c := &ChunkTags{}
	c.encodeTags(pts, dimensions)
	return c
}

func NewChunkTagsV2(subset []byte) *ChunkTags {
	c := &ChunkTags{
		subset: subset,
	}
	return c
}

var chunkTagsPool sync.Pool

func GetChunkTags() *ChunkTags {
	v := chunkTagsPool.Get()
	if v == nil {
		return &ChunkTags{}
	}
	return v.(*ChunkTags)
}

func PutChunkTags(ct *ChunkTags) {
	ct.Reset()
	chunkTagsPool.Put(ct)
}

func (ct *ChunkTags) funkSubset(pts influx.PointTags, keys []string) {
	if len(keys) == 0 {
		return
	}

	for i, k := range keys {
		t1 := pts.FindPointTag(k)
		if t1 == nil {
			ct.subset = append(ct.subset, Str2bytes(k)...)
			ct.subset = append(ct.subset, Str2bytes("=")...)
			if i < len(keys)-1 {
				ct.subset = append(ct.subset, Str2bytes(",")...)
			}
			continue
		}
		ct.subset = append(ct.subset, Str2bytes(t1.Key)...)
		ct.subset = append(ct.subset, Str2bytes("=")...)
		ct.subset = append(ct.subset, Str2bytes(t1.Value)...)
		if i < len(keys)-1 {
			ct.subset = append(ct.subset, Str2bytes(",")...)
		}
	}
}

func (ct *ChunkTags) GetChunkTagValue(name string) (string, bool) {
	for _, kv := range ct.decodeTags() {
		if name == kv[0] {
			return kv[1], true
		}
	}
	return "", false
}

func (ct *ChunkTags) GetChunkTagAndValues() ([]string, []string) {
	if len(ct.subset) == 0 {
		return nil, nil
	}
	tagValue := ct.decodeTags()
	k := make([]string, 0, len(tagValue))
	v := make([]string, 0, len(tagValue))
	for _, kv := range tagValue {
		k = append(k, kv[0])
		v = append(v, kv[1])
	}
	return k, v
}

func DecodeBytes(bytes []byte) []byte {
	offsetLen := record.Bytes2Uint16Slice(bytes[:2])[0]
	return bytes[2+int(offsetLen)*2:]
}

func (ct *ChunkTags) Subset(keys []string) []byte {
	if len(ct.subset) == 0 {
		return nil
	}
	return DecodeBytes(ct.subset)
}

func (ct *ChunkTags) GetTag() []byte {
	return ct.subset
}

func (ct *ChunkTags) Reset() {
	ct.subset = ct.subset[:0]
}

func (ct *ChunkTags) KeepKeys(keys []string) *ChunkTags {
	var m influx.PointTags
	var ss []string
	for _, kv := range ct.decodeTags() {
		if !ContainDim(keys, kv[0]) {
			ss = append(ss, kv[0])
			m = append(m, influx.Tag{kv[0], kv[1]})
		}
	}
	sort.Sort(&m)
	return NewChunkTags(m, ss)
}

func (ct *ChunkTags) KeyValues() map[string]string {
	m := make(map[string]string)
	if len(ct.subset) == 0 {
		return m
	}
	tagValue := ct.decodeTags()
	for _, kv := range tagValue {
		m[kv[0]] = kv[1]
	}
	return m
}

func ContainDim(des []string, src string) bool {
	for i := range des {
		if src == des[i] {
			return false
		}
	}
	return true
}

func (ct *ChunkTags) encodeTags(pts influx.PointTags, keys []string) {
	ct.offsets = make([]uint16, 0, len(keys)*2)
	if len(keys) == 0 {
		return
	}

	for _, k := range keys {
		t1 := pts.FindPointTag(k)
		if t1 == nil {
			ct.subset = append(ct.subset, Str2bytes(k+" ")...)
			ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
			ct.subset = append(ct.subset, Str2bytes(" ")...)
			ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
			continue
		}
		ct.subset = append(ct.subset, Str2bytes(t1.Key+" ")...)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
		ct.subset = append(ct.subset, Str2bytes(t1.Value+" ")...)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
	}
	ct.offsets = append([]uint16{uint16(len(ct.offsets))}, ct.offsets...)
	head := record.Uint16Slice2byte(ct.offsets)
	ct.subset = append(head, ct.subset...)
}

func (ct *ChunkTags) decodeTags() [][]string {
	if len(ct.subset) == 0 {
		return nil
	}
	offsetLen := record.Bytes2Uint16Slice(ct.subset[:2])[0]
	ct.offsets = record.Bytes2Uint16Slice(ct.subset[2 : 2+int(offsetLen)*2])
	tags := ct.subset[2+int(offsetLen)*2:]
	TagValues := make([][]string, 0, offsetLen/2)
	var index uint16
	for i := uint16(0); i < offsetLen; i += 2 {
		k := record.Bytes2str(tags[index:ct.offsets[i]])
		v := record.Bytes2str(tags[ct.offsets[i]:ct.offsets[i+1]])
		TagValues = append(TagValues, []string{k[:len(k)-1], v[:len(v)-1]})
		index = ct.offsets[i+1]
	}
	return TagValues
}
