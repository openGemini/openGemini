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
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

const (
	txPrefixPos = iota
	txPrefixTerm
	txPrefixDic
	txPrefixDicVersion
	txPrefixSid
	txPrefixId
	txPrefixMeta
	txSuffix = 9
)

const (
	posFlag = 1
	idFlag  = 2
)

func marshalPosList(dst []byte, invert *InvertIndex) ([]byte, []uint16) {
	invertStates := make([]*InvertStates, 0, len(invert.invertStates))
	for _, state := range invert.invertStates {
		invertStates = append(invertStates, state)
	}
	sort.Slice(invertStates, func(i, j int) bool {
		return invertStates[i].sid < invertStates[j].sid
	})

	sidLens := make([]uint16, 0, len(invertStates))
	for i := 0; i < len(invertStates); i++ {
		// prefixSid + sid + rowIdlist
		istate := invertStates[i].invertState
		dst = append(dst, txPrefixSid)
		dst = encoding.MarshalUint64(dst, invertStates[i].sid)
		for i := 0; i < len(istate); i++ {
			dst = encoding.MarshalInt64(dst, istate[i].rowId)
			dst = encoding.MarshalUint16(dst, istate[i].position)
		}

		sidLens = append(sidLens, uint16(len(istate)))
	}

	return dst, sidLens
}

func unmarshalPosList(tail []byte, sidLens []uint16, invert *InvertIndex, offset uint16) []byte {
	for i := 0; i < len(sidLens); i++ {
		sidLen := int(sidLens[i])
		// unmashral prifixSid
		if tail[0] != txPrefixSid {
			logger.Errorf("cannot unmarshal poslist: %s", string(tail))
			return tail
		}
		tail = tail[1:]
		// unmashral sid
		sid := encoding.UnmarshalUint64(tail)
		tail = tail[8:]
		invertState, ok := invert.invertStates[sid]
		if !ok {
			invertState = NewInvertStates()
			invertState.sid = sid
			invert.invertStates[sid] = invertState
		}

		for j := 0; j < sidLen; j++ {
			rowId := encoding.UnmarshalInt64(tail)
			tail = tail[8:]
			position := encoding.UnmarshalUint16(tail) + offset
			tail = tail[2:]
			invertState.invertState = append(invertState.invertState, InvertState{rowId, position, nil})
		}
	}

	return tail
}

// prefixIds + len(IDs) + ID0 + ID1 + ID2...
func marshalIdList(dst []byte, invert *InvertIndex) ([]byte, uint16) {
	idss := make([]uint32, 0, len(invert.ids))
	for id := range invert.ids {
		idss = append(idss, id)
	}
	sort.Slice(idss, func(i, j int) bool {
		return idss[i] < idss[j]
	})

	dst = append(dst, txPrefixId)
	for i := 0; i < len(idss); i++ {
		dst = encoding.MarshalUint32(dst, idss[i])
	}

	return dst, uint16(len(idss))
}

func unmarshalIdList(tail []byte, idsLen uint16, invert *InvertIndex) []byte {
	prefixIds := tail[0]
	if prefixIds != txPrefixId {
		logger.Errorf("cannot unmarshal idlist: %s", string(tail))
		return nil
	}
	tail = tail[1:]

	var i uint16
	for i = 0; i < idsLen; i++ {
		id := encoding.UnmarshalUint32(tail)
		tail = tail[4:]

		if _, ok := invert.ids[id]; !ok {
			invert.ids[id] = struct{}{}
		}
	}

	return tail
}

// prefixMeta + len(SIDs) + [Len(SID0) + Len(SID1) + Len(SID2) ...] + len(IDS) + flag + metaOffset
func marshalMeta(dst []byte, start int, sidLens []uint16, idsLen uint16) []byte {
	var flag uint8
	metaOffset := len(dst) - start

	dst = append(dst, txPrefixMeta)

	if len(sidLens) != 0 {
		dst = encoding.MarshalUint16(dst, uint16(len(sidLens)))
		for i := 0; i < len(sidLens); i++ {
			dst = encoding.MarshalUint16(dst, sidLens[i])
		}
		flag = flag | posFlag
	}

	if idsLen != 0 {
		dst = encoding.MarshalUint16(dst, idsLen)
		flag = flag | idFlag
	}

	dst = append(dst, flag)
	dst = encoding.MarshalUint16(dst, uint16(metaOffset))

	return dst
}

func unmarshalMeta(item []byte) ([]uint16, uint16) {
	itemLen := len(item)
	metaOffset := encoding.UnmarshalUint16(item[itemLen-2:])
	flag := item[itemLen-3]

	// unmashral prifixSid
	tail := item[metaOffset:]
	if tail[0] != txPrefixMeta {
		logger.Errorf("cannot unmarshal meta: %s", string(tail))
		return nil, 0
	}
	tail = tail[1:]

	// unmashral sidlens
	sidLens := make([]uint16, 0)
	if flag&posFlag != 0 {
		sidLen := int(encoding.UnmarshalUint16(tail))
		tail = tail[2:]

		for i := 0; i < sidLen; i++ {
			sidLens = append(sidLens, encoding.UnmarshalUint16(tail))
			tail = tail[2:]
		}
	}

	var idsLen uint16
	if flag&idFlag != 0 {
		idsLen = encoding.UnmarshalUint16(tail)
	}

	return sidLens, idsLen
}

func marshal(dst []byte, vtoken string, invert *InvertIndex) []byte {
	// prefixPos + vtokens + suffix
	start := len(dst)
	dst = append(dst, txPrefixPos)
	dst = append(dst, vtoken...)
	dst = append(dst, txSuffix)

	invert.Sort(nil)
	// sids
	var sidLens []uint16
	if len(invert.invertStates) != 0 {
		dst, sidLens = marshalPosList(dst, invert)
	}

	// ids
	var idsLen uint16
	if len(invert.ids) != 0 {
		dst, idsLen = marshalIdList(dst, invert)
	}

	// prefixMeta + meta
	dst = marshalMeta(dst, start, sidLens, idsLen)

	return dst
}

func unmarshal(item []byte, invert *InvertIndex, offset uint16) {
	sidLens, idsLen := unmarshalMeta(item)
	prefix := bytes.IndexByte(item, txSuffix)
	tail := item[prefix+1:]

	if len(sidLens) != 0 {
		tail = unmarshalPosList(tail, sidLens, invert, offset)
	}

	if idsLen != 0 {
		unmarshalIdList(tail, idsLen, invert)
	}
}

func marshalDicVersion(dst []byte, version uint32) []byte {
	dst = append(dst, txPrefixDicVersion)
	dst = append(dst, txSuffix)
	dst = encoding.MarshalUint32(dst, version)
	return dst
}

func unmarshaDiclVersion(item []byte) uint32 {
	prefix := bytes.IndexByte(item, txSuffix)
	tail := item[prefix+1:]
	return encoding.UnmarshalUint32(tail)
}

func marshalTerm(dst []byte, term string) []byte {
	dst = append(dst, txPrefixTerm)
	dst = append(dst, term...)
	return dst
}
