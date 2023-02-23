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
)

type TableStat struct {
	idCount          int64
	minId, maxId     uint64
	minTime, maxTime int64
	metaIndexItemNum int64
	bloomM, bloomK   uint64
	data             []byte // reserved for future use
	name             []byte // measurement name
}

func (stat *TableStat) marshalStat(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, stat.idCount)
	dst = numberenc.MarshalUint64Append(dst, stat.minId)
	dst = numberenc.MarshalUint64Append(dst, stat.maxId)
	dst = numberenc.MarshalInt64Append(dst, stat.minTime)
	dst = numberenc.MarshalInt64Append(dst, stat.maxTime)
	dst = numberenc.MarshalInt64Append(dst, stat.metaIndexItemNum)
	dst = numberenc.MarshalUint64Append(dst, stat.bloomM)
	dst = numberenc.MarshalUint64Append(dst, stat.bloomK)

	dst = numberenc.MarshalUint16Append(dst, uint16(len(stat.data)))
	dst = append(dst, stat.data...)
	dst = numberenc.MarshalUint16Append(dst, uint16(len(stat.name)))
	dst = append(dst, stat.name...)

	return dst
}

func (stat *TableStat) unmarshalStat(src []byte) ([]byte, error) {
	stat.idCount, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.minId, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.maxId, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.metaIndexItemNum, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.bloomM, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.bloomK, src = numberenc.UnmarshalUint64(src), src[8:]

	if len(src) < 2 {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer data, expect(2)", len(src))
	}
	dLen := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < dLen+2 {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer data, expect(%v)", len(src), dLen)
	}

	if len(stat.data) < dLen {
		stat.data = make([]byte, dLen)
	}
	stat.data = stat.data[:dLen]
	copy(stat.data, src[:dLen])
	src = src[dLen:]

	nameLen := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < nameLen {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer name, expect(%v)", len(src), nameLen)
	}

	if len(stat.name) < nameLen {
		stat.name = make([]byte, nameLen)
	}
	stat.name = stat.name[:nameLen]
	copy(stat.name, src[:nameLen])
	src = src[nameLen:]

	return src, nil
}
