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
	"hash/crc32"
	"math"
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/assert"
)

func TestReadMeta(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	fn := tmpDir + "/" + META_NAME

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}
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

	for i := 0; i < 1000; i++ {
		fd.Write(bytes)
	}
	fd.Sync()

	tr := util.TimeRange{Min: 0, Max: math.MaxInt32}
	metaReader, _ := NewMetaReader(nil, tmpDir, 0, int64(1000*META_STORE_N_BYTES), tr, false, true)

	for i := 0; i < 1000; i++ {
		block, _ := metaReader.next()
		assert.EqualValues(t, 1, block.maxTimestamp)
		assert.EqualValues(t, 0, block.minTimestamp)
		assert.EqualValues(t, 1, block.maxSeq)
		assert.EqualValues(t, 0, block.minSeq)
		assert.EqualValues(t, 0, block.contentBlockOffset)
		assert.EqualValues(t, 4, block.contentBlockLength)
		assert.EqualValues(t, 1, block.blockRecordsCount)
	}
	nilBlock, err := metaReader.next()
	assert.Equal(t, nil, err)
	assert.Nilf(t, nilBlock, "new read block should return nil")
	metaReader.Close()

	_, err = NewMetaReader(nil, tmpDir+"err", 0, int64(1000*META_STORE_N_BYTES), tr, false, true)
	if err == nil {
		t.Error("get wrong reader")
	}
}
