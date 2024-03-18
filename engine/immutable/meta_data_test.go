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

package immutable

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestMetaDataControl(t *testing.T) {
	bytes := make([]byte, META_STORE_N_BYTES)
	binary.LittleEndian.PutUint64(bytes[0:8], 1)
	binary.LittleEndian.PutUint64(bytes[8:16], 0)
	binary.LittleEndian.PutUint64(bytes[16:24], 1)
	binary.LittleEndian.PutUint64(bytes[24:32], 0)
	binary.LittleEndian.PutUint64(bytes[32:40], 0)
	binary.LittleEndian.PutUint32(bytes[40:44], 4)
	binary.LittleEndian.PutUint32(bytes[44:48], 1)
	crc := crc32.Checksum(bytes[0:48], crc32CastagnoliTable)
	binary.LittleEndian.PutUint32(bytes[48:52], crc)
	m1, _ := NewMetaData(1, bytes, 0)
	m2, _ := NewMetaData(0, bytes, 0)
	m3, _ := NewMetaData(2, bytes, 0)
	metaDataSlice := []*MetaData{m1, m2, m3}
	metaControl := NewMetaControl(true, 3)
	for _, v := range metaDataSlice {
		metaControl.Push(v)
	}
	result := []int64{1, 0, 2}
	index := 0
	for !metaControl.IsEmpty() {
		m, _ := metaControl.Pop()
		meta := m.(*MetaData)
		if meta.blockIndex != result[index] {
			t.Errorf("get wrong meta")
		}
		if meta.GetContentBlockLength() != 4 {
			t.Errorf("get wrong meta")
		}
		if meta.GetContentBlockOffset() != 0 {
			t.Errorf("get wrong meta")
		}
		if meta.GetMinTime() != 0 || meta.GetMaxTime() != 1 {
			t.Errorf("get wrong meta")
		}

		index++
	}
	_, exist := metaControl.Pop()
	if exist {
		t.Errorf("get wrong meta")
	}

	metaControl = NewMetaControl(false, 3)
	for _, v := range metaDataSlice {
		metaControl.Push(v)
	}
	result = []int64{2, 0, 1}
	index = 0
	for !metaControl.IsEmpty() {
		m, _ := metaControl.Pop()
		meta := m.(*MetaData)
		if meta.GetBlockIndex() != result[index] {
			t.Errorf("get wrong meta")
		}
		index++
	}
	_, exist = metaControl.Pop()
	if exist {
		t.Errorf("get wrong meta")
	}
}
