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
	"bytes"
	"fmt"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

const (
	posFlag = 1
	idFlag  = 2
)

// len(sids) + [prifixSid + sid + len(posList) + {time0 + pos0} + {time1 + pos1} + ...] + [prifixSid...]...
func marshalPosList(dst []byte, invert *InvertIndex) []byte {
	// len(sid)
	dst = encoding.MarshalUint32(dst, uint32(len(invert.invertStates)))

	for sid, state := range invert.invertStates {
		// prefixSid + sid + len(pos) + timelist + positionlist
		istate := state.invertState
		sort.Slice(istate, func(i, j int) bool {
			return istate[i].timestamp < istate[j].timestamp
		})

		dst = append(dst, txPrefixSid)
		dst = encoding.MarshalUint64(dst, sid)
		dst = encoding.MarshalUint32(dst, uint32(len(istate)))

		for i := 0; i < len(istate); i++ {
			dst = encoding.MarshalInt64(dst, istate[i].timestamp)
			dst = encoding.MarshalUint16(dst, istate[i].position)
		}
	}
	return dst
}

func unmarshalPosList(tail []byte, invert *InvertIndex) []byte {
	lenSid := encoding.UnmarshalUint32(tail)
	tail = tail[4:]
	var i, j uint32
	for i = 0; i < lenSid; i++ {
		// unmashral prifixSid
		if tail[0] != txPrefixSid {
			panic(fmt.Errorf("cannot unmarshal poslist: %s", string(tail)))
		}
		tail = tail[1:]

		// unmashral sid
		sid := encoding.UnmarshalUint64(tail)
		tail = tail[8:]

		invertState, ok := invert.invertStates[sid]
		if !ok {
			invertState = NewInvertStates()
			invert.invertStates[sid] = invertState
		}

		// unmashral len(poslist)
		lenPos := encoding.UnmarshalUint32(tail)
		tail = tail[4:]
		for j = 0; j < lenPos; j++ {
			timestamp := encoding.UnmarshalInt64(tail)
			tail = tail[8:]
			position := encoding.UnmarshalUint16(tail)
			tail = tail[2:]
			invertState.invertState = append(invertState.invertState, InvertState{timestamp, position})
		}
	}

	return tail
}

// prefixIds + len(IDs) + ID0 + ID1 + ID2...
func marshalIdList(dst []byte, invert *InvertIndex) []byte {
	if len(invert.ids) == 0 {
		return dst
	}

	idss := make([]uint32, len(invert.ids))
	for id := range invert.ids {
		idss = append(idss, id)
	}
	sort.Slice(idss, func(i, j int) bool {
		return idss[i] < idss[j]
	})

	dst = append(dst, txPrefixId)
	dst = encoding.MarshalUint32(dst, uint32(len(idss)))

	for i := 0; i < len(idss); i++ {
		dst = encoding.MarshalUint32(dst, idss[i])
	}

	return dst
}

func unmarshalIdList(tail []byte, invert *InvertIndex) []byte {
	prefixIds := tail[0]
	if prefixIds != txPrefixId {
		panic(fmt.Errorf("cannot unmarshal idlist: %s", string(tail)))
	}
	tail = tail[1:]

	lenId := encoding.UnmarshalUint32(tail)
	tail = tail[4:]

	var i uint32
	for i = 0; i < lenId; i++ {
		id := encoding.UnmarshalUint32(tail)
		tail = tail[4:]

		if _, ok := invert.ids[id]; !ok {
			invert.ids[id] = struct{}{}
		}
	}

	return tail
}

func marshal(dst []byte, vtoken string, invert *InvertIndex) []byte {
	var flag uint8
	if len(invert.invertStates) != 0 {
		flag = flag | posFlag
	}
	if len(invert.ids) != 0 {
		flag = flag | idFlag
	}

	dst = append(dst, txPrefixPos)
	dst = append(dst, []byte(vtoken)...)
	dst = append(dst, txSuffix)
	dst = append(dst, flag)

	if flag&posFlag != 0 {
		dst = marshalPosList(dst, invert)
	}

	if flag&idFlag != 0 {
		dst = marshalIdList(dst, invert)
	}

	return dst
}

func unmarshal(item []byte, invert *InvertIndex) {
	prefix := bytes.IndexByte(item, txSuffix)
	tail := item[prefix+1:]

	flag := tail[0]
	tail = tail[1:]

	if flag&posFlag != 0 {
		tail = unmarshalPosList(tail, invert)
	}

	if flag&idFlag != 0 {
		tail = unmarshalIdList(tail, invert)
	}
}
