// Copyright Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// nolint
package executor

import (
	"sort"
	"unsafe"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var IgnoreEmptyTag = false

func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

type TagValues [][]string

func (t TagValues) Less(i, j int) bool {
	return t[i][0] < t[j][0]
}

func (t TagValues) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TagValues) Len() int {
	return len(t)
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

func NewChunkTagsWithoutDims(pts influx.PointTags, withoutDims []string) *ChunkTags {
	c := &ChunkTags{}
	c.encodeTagsWithoutDims(pts, withoutDims)
	return c
}

func NewChunkTagsByTagKVs(k []string, v []string) *ChunkTags {
	c := &ChunkTags{}
	c.encodeTagsByTagKVs(k, v)
	return c
}

func NewChunkTagsByBytes(bytes []byte) *ChunkTags {
	c := &ChunkTags{}
	c.encodeTagsByBytes(bytes)
	return c
}

func NewChunkTagsV2(subset []byte) *ChunkTags {
	c := &ChunkTags{
		subset: subset,
	}
	return c
}

func NewChunkTagsDeepCopy(subset []byte, offsets []uint16) *ChunkTags {
	c := &ChunkTags{}
	c.subset = append(c.subset, subset...)
	c.offsets = append(c.offsets, offsets...)
	return c
}

func (c *ChunkTags) GetOffsets() []uint16 {
	return c.offsets
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
	offsetLen := util.Bytes2Uint16Slice(bytes[:2])[0]
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
		if ContainDim(keys, kv[0]) {
			ss = append(ss, kv[0])
			m = append(m, influx.Tag{Key: kv[0], Value: kv[1], IsArray: false})
		}
	}
	sort.Sort(&m)
	return NewChunkTags(m, ss)
}

func (ct *ChunkTags) RemoveKeys(keys []string) *ChunkTags {
	var m influx.PointTags
	var ss []string
	for _, kv := range ct.decodeTags() {
		if !ContainDim(keys, kv[0]) {
			ss = append(ss, kv[0])
			m = append(m, influx.Tag{Key: kv[0], Value: kv[1], IsArray: false})
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

func (ct *ChunkTags) PointTags() influx.PointTags {
	if len(ct.subset) == 0 {
		return nil
	}
	tags := make(influx.PointTags, 0, len(ct.offsets))

	tagValues := ct.decodeTags()
	sort.Sort(TagValues(tagValues))
	for _, kv := range tagValues {
		tags = append(tags, influx.Tag{Key: kv[0], Value: kv[1]})
	}
	return tags
}

func ContainDim(des []string, src string) bool {
	for i := range des {
		if src == des[i] {
			return true
		}
	}
	return false
}

func (ct *ChunkTags) encodeTagsByTagKVs(keys []string, vals []string) {
	ct.offsets = make([]uint16, 0, len(keys)*2)
	if len(keys) == 0 || len(keys) != len(vals) {
		return
	}

	for i, k := range keys {
		ct.subset = append(ct.subset, k...)
		ct.subset = append(ct.subset, influx.ByteSplit)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
		ct.subset = append(ct.subset, vals[i]...)
		ct.subset = append(ct.subset, influx.ByteSplit)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
	}
	ct.encodeHead()
}

func (ct *ChunkTags) encodeTagsByBytes(bytes []byte) {
	ct.subset = bytes
	for i, b := range bytes {
		if b == influx.ByteSplit {
			ct.offsets = append(ct.offsets, uint16(i+1))
		}
	}
	ct.encodeHead()
}

func (ct *ChunkTags) encodeHead() {
	ct.offsets = append([]uint16{uint16(len(ct.offsets))}, ct.offsets...)
	head := util.Uint16Slice2byte(ct.offsets)
	ct.subset = append(head, ct.subset...)
}

func (ct *ChunkTags) encodeTags(pts influx.PointTags, keys []string) {
	ct.offsets = make([]uint16, 0, len(keys)*2)
	if len(keys) == 0 {
		return
	}

	for _, k := range keys {
		t1 := pts.FindPointTag(k)
		if t1 == nil {
			if IgnoreEmptyTag {
				k = ""
			}
			ct.subset = append(ct.subset, Str2bytes(k+influx.StringSplit)...)
			ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
			ct.subset = append(ct.subset, influx.ByteSplit)
			ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
			continue
		}
		ct.subset = append(ct.subset, Str2bytes(t1.Key+influx.StringSplit)...)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
		ct.subset = append(ct.subset, Str2bytes(t1.Value+influx.StringSplit)...)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
	}
	ct.offsets = append([]uint16{uint16(len(ct.offsets))}, ct.offsets...)
	head := util.Uint16Slice2byte(ct.offsets)
	ct.subset = append(head, ct.subset...)
}

func (ct *ChunkTags) encodeTagsWithoutDims(pts influx.PointTags, withoutKeys []string) {
	ct.offsets = make([]uint16, 0, (len(pts)-len(withoutKeys))*2)
	if len(pts) == 0 {
		return
	}
	i, j := 0, 0
	for i < len(withoutKeys) && j < len(pts) {
		if withoutKeys[i] < pts[j].Key {
			i++
		} else if withoutKeys[i] > pts[j].Key {
			ct.subset = append(ct.subset, Str2bytes(pts[j].Key+influx.StringSplit)...)
			ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
			ct.subset = append(ct.subset, Str2bytes(pts[j].Value+influx.StringSplit)...)
			ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
			j++
		} else {
			i++
			j++
		}
	}
	for ; j < len(pts); j++ {
		ct.subset = append(ct.subset, Str2bytes(pts[j].Key+influx.StringSplit)...)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
		ct.subset = append(ct.subset, Str2bytes(pts[j].Value+influx.StringSplit)...)
		ct.offsets = append(ct.offsets, uint16(len(ct.subset)))
	}

	ct.offsets = append([]uint16{uint16(len(ct.offsets))}, ct.offsets...)
	head := util.Uint16Slice2byte(ct.offsets)
	ct.subset = append(head, ct.subset...)
}

func (ct *ChunkTags) decodeTags() [][]string {
	if len(ct.subset) == 0 {
		return nil
	}
	offsetLen := util.Bytes2Uint16Slice(ct.subset[:2])[0]
	ct.offsets = util.Bytes2Uint16Slice(ct.subset[2 : 2+int(offsetLen)*2])
	tags := ct.subset[2+int(offsetLen)*2:]
	TagValues := make([][]string, 0, offsetLen/2)
	var index uint16
	for i := uint16(0); i < offsetLen; i += 2 {
		k := util.Bytes2str(tags[index:ct.offsets[i]])
		if len(k) == 1 {
			index = ct.offsets[i+1]
			continue
		}
		v := util.Bytes2str(tags[ct.offsets[i]:ct.offsets[i+1]])
		TagValues = append(TagValues, []string{k[:len(k)-1], v[:len(v)-1]})
		index = ct.offsets[i+1]
	}
	return TagValues
}

func (ct *ChunkTags) DecodeTagsWithoutTag(tagName string) ([]byte, string) {
	if len(ct.subset) == 0 {
		return nil, ""
	}
	offsetLen := util.Bytes2Uint16Slice(ct.subset[:2])[0]
	ct.offsets = util.Bytes2Uint16Slice(ct.subset[2 : 2+int(offsetLen)*2])
	tags := ct.subset[2+int(offsetLen)*2:]
	newTags := make([]byte, 0, len(tags))
	var targetTagValue string
	var index uint16
	var flag bool
	for i := uint16(0); i < offsetLen; i += 2 {
		if !flag {
			k := util.Bytes2str(tags[index : ct.offsets[i]-1])
			if k == tagName {
				targetTagValue = string(tags[ct.offsets[i] : ct.offsets[i+1]-1])
				flag = true
				index = ct.offsets[i+1]
				continue
			}
		}
		newTags = append(newTags, tags[index:ct.offsets[i]]...)
		newTags = append(newTags, tags[ct.offsets[i]:ct.offsets[i+1]]...)
		index = ct.offsets[i+1]
	}
	if targetTagValue == "" {
		return nil, targetTagValue
	}
	return newTags, targetTagValue
}
