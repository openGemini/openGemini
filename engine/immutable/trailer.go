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

package immutable

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
)

type Trailer struct {
	dataOffset    int64
	dataSize      int64
	indexSize     int64
	metaIndexSize int64
	bloomSize     int64
	idTimeSize    int64
	TableStat
}

func (t *Trailer) reset() {
	t.dataOffset = 0
	t.dataSize = 0
	t.indexSize = 0
	t.metaIndexSize = 0
	t.bloomSize = 0
	t.idTimeSize = 0
	t.idCount = 0
	t.minId = 0
	t.maxId = 0
	t.minTime = 0
	t.maxTime = 0
	t.metaIndexItemNum = 0
	t.bloomM = 0
	t.bloomK = 0

	t.data = t.data[:0]
	t.name = t.name[:0]
}

func (t *Trailer) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, t.dataOffset)
	dst = numberenc.MarshalInt64Append(dst, t.dataSize)
	dst = numberenc.MarshalInt64Append(dst, t.indexSize)
	dst = numberenc.MarshalInt64Append(dst, t.metaIndexSize)
	dst = numberenc.MarshalInt64Append(dst, t.bloomSize)
	dst = numberenc.MarshalInt64Append(dst, t.idTimeSize)

	return t.marshalStat(dst)
}

func (t *Trailer) unmarshal(src []byte) ([]byte, error) {
	if len(src) < trailerSize {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer", len(src))
	}

	t.dataOffset, src = numberenc.UnmarshalInt64(src), src[8:]
	t.dataSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.indexSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.metaIndexSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.bloomSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.idTimeSize, src = numberenc.UnmarshalInt64(src), src[8:]
	return t.unmarshalStat(src)
}

func (t *Trailer) ContainsId(id uint64) bool {
	if id >= t.minId && id <= t.maxId {
		return true
	}
	return false
}

func (t *Trailer) ContainsTime(tm record.TimeRange) bool {
	return tm.Overlaps(t.minTime, t.maxTime)
}

func (t *Trailer) MetaIndexItemNum() int64 {
	return t.metaIndexItemNum
}

func (t *Trailer) copyTo(tr *Trailer) {
	tr.dataOffset = t.dataOffset
	tr.dataSize = t.dataSize
	tr.indexSize = t.indexSize
	tr.metaIndexSize = t.metaIndexSize
	tr.bloomSize = t.bloomSize
	tr.idCount = t.idCount
	tr.minId, tr.maxId = t.minId, t.maxId
	tr.minTime, tr.maxTime = t.minTime, t.maxTime
	tr.metaIndexItemNum = t.metaIndexItemNum
	tr.bloomM, tr.bloomK = t.bloomM, t.bloomK
	tr.idTimeSize = t.idTimeSize
	if cap(tr.data) < len(t.data) {
		tr.data = make([]byte, len(t.data))
	} else {
		tr.data = tr.data[:len(t.data)]
	}
	copy(tr.data, t.data)

	if cap(tr.name) < len(t.name) {
		tr.name = make([]byte, len(t.name))
	} else {
		tr.name = tr.name[:len(t.name)]
	}
	copy(tr.name, t.name)
}

func (t *Trailer) metaOffsetSize() (int64, int64) {
	return t.dataOffset + t.dataSize, t.indexSize
}

func (t *Trailer) metaIndexOffsetSize() (int64, int64) {
	return t.dataOffset + t.dataSize + t.indexSize, t.metaIndexSize
}

func (t *Trailer) idTimeOffsetSize() (int64, int64) {
	idTimeOff := t.dataOffset + t.dataSize + t.indexSize + t.metaIndexSize + t.bloomSize
	return idTimeOff, t.idTimeSize
}
