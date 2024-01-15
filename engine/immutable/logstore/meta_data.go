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
package logstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	META_DATA_N_BYTES  int32 = 8 + 8 + 8 + 8 + 8 + 4 + 4
	META_STORE_N_BYTES int32 = META_DATA_N_BYTES + 4
	META_DATA_SIZE     int32 = META_STORE_N_BYTES + 4
)

var crc32CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type MetaData struct {
	contentBlockLength int32
	blockRecordsCount  int32
	blockIndex         int64
	minTimestamp       int64
	maxTimestamp       int64
	minSeq             int64
	maxSeq             int64
	contentBlockOffset int64
}

func NewMetaData(blockIndex int64, data []byte, offset int32) (*MetaData, error) {
	checksum := binary.LittleEndian.Uint32(data[offset+META_DATA_N_BYTES : offset+META_STORE_N_BYTES])
	if crc32.Checksum(data[offset:offset+META_DATA_N_BYTES], crc32CastagnoliTable) != checksum {

		return nil, fmt.Errorf("blockindex: %d, meta block checksum invalid", blockIndex)
	}
	m := &MetaData{
		blockIndex:         blockIndex,
		maxTimestamp:       int64(binary.LittleEndian.Uint64(data[offset : offset+8])),
		minTimestamp:       int64(binary.LittleEndian.Uint64(data[offset+8 : offset+16])),
		maxSeq:             int64(binary.LittleEndian.Uint64(data[offset+16 : offset+24])),
		minSeq:             int64(binary.LittleEndian.Uint64(data[offset+24 : offset+32])),
		contentBlockOffset: int64(binary.LittleEndian.Uint64(data[offset+32 : offset+40])),
		contentBlockLength: int32(binary.LittleEndian.Uint32(data[offset+40 : offset+44])),
		blockRecordsCount:  int32(binary.LittleEndian.Uint32(data[offset+44 : offset+48])),
	}
	return m, nil
}

func (m *MetaData) GetBlockIndex() int64 {
	return m.blockIndex
}

func (m *MetaData) GetContentBlockOffset() int64 {
	return m.contentBlockOffset
}

func (m *MetaData) GetContentBlockLength() int32 {
	return m.contentBlockLength
}

func (m *MetaData) GetMinTime() int64 {
	return m.minTimestamp
}

func (m *MetaData) GetMaxTime() int64 {
	return m.maxTimestamp
}

type MetaDataInfo interface {
	GetMinTime() int64
	GetMaxTime() int64
}

type MetaControl interface {
	Push(MetaDataInfo)
	Pop() (MetaDataInfo, bool)
	IsEmpty() bool
}

func NewMetaControl(isQueue bool, count int) MetaControl {
	if isQueue {
		meta := make(MetaQueue, 0, count)
		return &meta
	} else {
		meta := make(MetaStack, 0, count)
		return &meta
	}
}

type MetaQueue []MetaDataInfo

func (q *MetaQueue) Push(v MetaDataInfo) {
	*q = append(*q, v)
}

func (q *MetaQueue) Pop() (MetaDataInfo, bool) {
	if q.IsEmpty() {
		return nil, false
	}
	head := (*q)[0]
	*q = (*q)[1:]
	return head, true
}
func (q *MetaQueue) IsEmpty() bool {
	return len((*q)) == 0
}

type MetaStack []MetaDataInfo

func (s *MetaStack) IsEmpty() bool {
	return len((*s)) == 0
}

func (s *MetaStack) Push(value MetaDataInfo) {
	*s = append(*s, value)
}

func (s *MetaStack) Pop() (MetaDataInfo, bool) {
	if s.IsEmpty() {
		return nil, false
	}
	index := len(*s) - 1
	value := (*s)[index]
	*s = (*s)[:index]
	return value, true
}

type MetaDatas []*MetaData

func (a MetaDatas) Len() int           { return len(a) }
func (a MetaDatas) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MetaDatas) Less(i, j int) bool { return a[i].blockIndex < a[j].blockIndex }
