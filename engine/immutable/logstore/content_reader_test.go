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
	"os"
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func TestReadContent(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	fn := tmpDir + "/" + CONTENT_NAME

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	recordsBytes := make([]byte, RECORD_DATA_MAX_N_BYTES)
	binary.LittleEndian.PutUint64(recordsBytes, 0)
	checkSum := crc32.Checksum(recordsBytes[:8], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(recordsBytes[8:12], checkSum)

	tagStr := "test_tag"
	length := TAGS_RECORD_PREFIX_N_BYTES + len(tagStr)
	flag := (FLAG_TAGS << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(recordsBytes[12:16], uint32(flag))
	copy(recordsBytes[16:24], tagStr)

	offset := 24
	for i := 0; i < 1024; i++ {
		content := strconv.Itoa(i)
		length := CONTENT_RECORD_PREFIX_N_BYTES + len(content)
		flag := (FLAG_CONTENT << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
		binary.LittleEndian.PutUint32(recordsBytes[offset:offset+4], uint32(flag))
		binary.LittleEndian.PutUint32(recordsBytes[offset+4:offset+8], uint32(12))
		binary.LittleEndian.PutUint64(recordsBytes[offset+8:offset+16], uint64(i))
		binary.LittleEndian.PutUint64(recordsBytes[offset+16:offset+24], uint64(i))
		copy(recordsBytes[offset+24:offset+24+len(content)], content)
		offset += length
	}
	compressed := make([]byte, MAX_BLOCK_LOG_STORE_N_BYTES)
	binary.LittleEndian.PutUint32(compressed[:4], uint32(offset))
	compressSize, err := lz4.CompressBlock(recordsBytes[0:offset], compressed[4:], make([]int, 64<<10))
	if err != nil {
		t.Errorf("lz4 compress error")
		return
	}
	checkSum = crc32.Checksum(compressed[:4+compressSize], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(compressed[4+compressSize:4+compressSize+4], checkSum)

	for i := 0; i < 10; i++ {
		fd.Write(compressed[:4+compressSize+4])
	}
	fd.Sync()

	offsets := make([]int64, 10)
	lengths := make([]int64, 10)
	for i := 0; i < 10; i++ {
		offsets[i] = int64(i * (4 + compressSize + 4))
		lengths[i] = int64(4 + compressSize + 4)
	}
	_, span := tracing.NewTrace("test")
	contentReader, err := NewContentReader(nil, tmpDir, offsets, lengths, nil, false, -1, true)
	contentReader.StartSpan(span)
	for i := 0; i < 10; i++ {
		records, _, err := contentReader.Next()
		if err != nil {
			t.Errorf("get next content failed")
			return
		}
		assert.Equal(t, 1024, len(records))
		for j := 0; j < 1024; j++ {
			assert.Equal(t, 3, len(records[j]))
			tag := records[j][0]
			content := records[j][1]
			timestamp := records[j][2]
			assert.Equal(t, tagStr, string(tag))
			assert.EqualValues(t, strconv.Itoa(j), string(content))
			assert.Equal(t, j, int(binary.LittleEndian.Uint64(timestamp)))
		}
	}
	nilBlock, _, err := contentReader.Next()
	assert.Equal(t, nil, err)
	assert.Nilf(t, nilBlock, "new read block should return nil")
	contentReader.Close()
}

func TestReadContentFail(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	fn := tmpDir + "/" + CONTENT_NAME

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	recordsBytes := make([]byte, RECORD_DATA_MAX_N_BYTES)
	binary.LittleEndian.PutUint64(recordsBytes, 0)
	checkSum := crc32.Checksum(recordsBytes[:8], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(recordsBytes[8:12], checkSum)

	tagStr := "test_tag"
	length := TAGS_RECORD_PREFIX_N_BYTES + len(tagStr)
	flag := (FLAG_TAGS << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(recordsBytes[12:16], uint32(flag))
	copy(recordsBytes[16:24], tagStr)

	offset := 24
	for i := 0; i < 1024; i++ {
		content := strconv.Itoa(i)
		length := CONTENT_RECORD_PREFIX_N_BYTES + len(content)
		flag := (FLAG_CONTENT << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
		binary.LittleEndian.PutUint32(recordsBytes[offset:offset+4], uint32(flag))
		binary.LittleEndian.PutUint32(recordsBytes[offset+4:offset+8], uint32(12))
		binary.LittleEndian.PutUint64(recordsBytes[offset+8:offset+16], uint64(i))
		binary.LittleEndian.PutUint64(recordsBytes[offset+16:offset+24], uint64(i))
		copy(recordsBytes[offset+24:offset+24+len(content)], content)
		offset += length
	}
	compressed := make([]byte, MAX_BLOCK_LOG_STORE_N_BYTES)
	binary.LittleEndian.PutUint32(compressed[:4], uint32(offset))
	compressSize, err := lz4.CompressBlock(recordsBytes[0:offset], compressed[4:], make([]int, 64<<10))
	if err != nil {
		t.Errorf("lz4 compress error")
		return
	}
	checkSum = crc32.Checksum(compressed[:4+compressSize], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(compressed[4+compressSize:4+compressSize+4], checkSum)

	for i := 0; i < 10; i++ {
		fd.Write(compressed[:4+compressSize+4])
	}
	fd.Sync()

	offsets := make([]int64, 10)
	lengths := make([]int64, 10)
	for i := 0; i < 10; i++ {
		offsets[i] = int64(i * (4 + compressSize + 4))
		lengths[i] = int64(0)
	}
	contentReader, err := NewContentReader(nil, tmpDir, offsets, lengths, nil, true, -1, true)
	contentReader.StartSpan(nil)
	for i := 0; i < 10; i++ {
		_, _, err := contentReader.Next()
		if err != nil {
			t.Errorf("get next content failed")
			return
		}
	}
	contentReader, _ = NewContentReader(nil, tmpDir+"not", offsets, lengths, nil, true, -1, true)
	if contentReader != nil {
		t.Errorf("get wrong contentreader")
	}
}

func TestReadBlg(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	fn := tmpDir + "/" + BLOCK_LOG_NAME

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	bytes := make([]byte, RECORD_DATA_MAX_N_BYTES)
	binary.LittleEndian.PutUint64(bytes, 0)
	checkSum := crc32.Checksum(bytes[:8], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[8:12], checkSum)

	tagStr := "test_tag"
	length := TAGS_RECORD_PREFIX_N_BYTES + len(tagStr)
	flag := (FLAG_TAGS << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(bytes[12:16], uint32(flag))
	copy(bytes[16:24], tagStr)

	offset := 24
	for i := 0; i < 1024; i++ {
		content := strconv.Itoa(i)
		length := CONTENT_RECORD_PREFIX_N_BYTES + len(content)
		flag := (FLAG_CONTENT << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
		binary.LittleEndian.PutUint32(bytes[offset:offset+4], uint32(flag))
		binary.LittleEndian.PutUint32(bytes[offset+4:offset+8], uint32(12))
		binary.LittleEndian.PutUint64(bytes[offset+8:offset+16], uint64(i))
		binary.LittleEndian.PutUint64(bytes[offset+16:offset+24], uint64(i))
		copy(bytes[offset+24:offset+24+len(content)], content)
		offset += length
	}

	blockLogCheckFlag := (FLAG_BLOCK_CHECK << FLAG_HEAD_SHIFT) | (BLOCK_LOG_CHECK_N_BYTES & FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(bytes[offset:offset+4], uint32(blockLogCheckFlag))
	blockLogCheckValue := crc32.Checksum(bytes[0:offset], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[offset+4:offset+8], blockLogCheckValue)

	fd.Write(bytes[:offset+BLOCK_LOG_CHECK_N_BYTES])
	fd.Sync()

	offsets := make([]int64, 0)
	lengths := make([]int64, 0)

	contentReader, err := NewContentReader(nil, tmpDir, offsets, lengths, nil, true, -1, true)
	records, _, err := contentReader.Next()
	if err != nil || len(records) != 1024 {
		t.Errorf("get next content failed")
		return
	}
}

func TestReadWrongBlg(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	fn := tmpDir + "/" + BLOCK_LOG_NAME

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	bytes := make([]byte, RECORD_DATA_MAX_N_BYTES)
	binary.LittleEndian.PutUint64(bytes, 0)
	checkSum := crc32.Checksum(bytes[:8], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[8:12], checkSum)

	tagStr := "test_tag"
	length := TAGS_RECORD_PREFIX_N_BYTES + len(tagStr)
	flag := (FLAG_TAGS << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(bytes[12:16], uint32(flag))
	copy(bytes[16:24], tagStr)

	offset := 24
	for i := 0; i < 1024; i++ {
		content := strconv.Itoa(i)
		length := CONTENT_RECORD_PREFIX_N_BYTES + len(content)
		flag := (FLAG_CONTENT << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
		binary.LittleEndian.PutUint32(bytes[offset:offset+4], uint32(flag))
		binary.LittleEndian.PutUint32(bytes[offset+4:offset+8], uint32(12))
		binary.LittleEndian.PutUint64(bytes[offset+8:offset+16], uint64(i))
		binary.LittleEndian.PutUint64(bytes[offset+16:offset+24], uint64(i))
		copy(bytes[offset+24:offset+24+len(content)], content)
		offset += length
	}

	blockLogCheckFlag := (FLAG_BLOCK_CHECK << FLAG_HEAD_SHIFT) | (BLOCK_LOG_CHECK_N_BYTES & FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(bytes[offset:offset+4], uint32(blockLogCheckFlag))
	blockLogCheckValue := crc32.Checksum(bytes[0:offset], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[offset+4:offset+8], blockLogCheckValue)

	fd.Write(bytes[:offset+BLOCK_LOG_CHECK_N_BYTES])
	fd.Sync()

	offsets := make([]int64, 0)
	lengths := make([]int64, 0)

	contentReader, err := NewContentReader(nil, tmpDir, offsets, lengths, nil, true, 2, true)
	records, _, err := contentReader.Next()
	if err != nil || len(records) != 0 {
		t.Errorf("get next content failed")
		return
	}
}
