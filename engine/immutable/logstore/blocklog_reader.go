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

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
)

const (
	RecordPoolNum                 = 3
	BLOCK_LOG_NAME                = "segment.blg"
	BLOCK_LOG_CHECK_N_BYTES       = 4 + 4
	TAGS_RECORD_PREFIX_N_BYTES    = 4
	FLAG_TAGS                     = 0
	FLAG_CONTENT                  = 1
	FLAG_LENGTH_MASK              = 0xffffff
	FLAG_HEAD_SHIFT               = 30
	RECORD_DATA_MAX_N_BYTES       = 1024 * 1024
	CONTENT_RECORD_PREFIX_N_BYTES = 4 + 4 + 8 + 8
	CONTENT_RECORD_MAX_N_BYTES    = CONTENT_RECORD_PREFIX_N_BYTES + RECORD_DATA_MAX_N_BYTES
	RECORD_MAX_N_BYTES            = CONTENT_RECORD_MAX_N_BYTES
	BLOCK_FLUSH_TRIGGER_N_BYTES   = 1024 * 1024
	FLAG_BLOCK_CHECK              = 2
	BLOCK_PREFIX_N_BYTES          = 8 + 4
)

type BlockLogReader struct {
	r        fileops.BasicFileReader
	path     string
	readChan chan *request.StreamReader
	object   *obs.ObsOptions
}

func NewBlockLogReader(object *obs.ObsOptions, isStat bool, path string) (*BlockLogReader, error) {
	blockLogReader := &BlockLogReader{object: object, path: path}
	fd, err := obs.OpenObsFile(path, BLOCK_LOG_NAME, object)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	blockLogReader.r = dr
	blockLogLen, err := dr.Size()
	if blockLogLen == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	readChan := make(chan *request.StreamReader, RecordPoolNum)
	blockLogReader.readChan = readChan
	blockLogReader.r.StreamReadBatch([]int64{0}, []int64{blockLogLen}, readChan, -1)
	return blockLogReader, nil
}

func (s *BlockLogReader) Next() ([][][]byte, int64, int, int64, error) {
	for r := range s.readChan {
		if r.Err != nil {
			return nil, -1, 0, 0, r.Err
		}
		blockID, records, err := s.readBlockLog(r.Content)
		if err != nil {
			// ignore readContent error
			logger.GetLogger().Error("read block log [" + s.path + "]failed: " + err.Error())
			continue
		}
		return records, -1, len(r.Content), blockID, nil
	}
	return nil, -1, 0, 0, nil
}

func (c *BlockLogReader) readBlockLog(bytes []byte) (int64, [][][]byte, error) {
	contentLength := len(bytes)
	contentBytes := bytes
	indexLoadCheckValue := binary.LittleEndian.Uint32(contentBytes[8:12])
	indexCheckValue := crc32.Checksum(contentBytes[:8], crc32.MakeTable(crc32.Castagnoli))
	if indexCheckValue != indexLoadCheckValue {
		return 0, nil, fmt.Errorf("load block index checksum(%d) mismatch computed valued(%d)", indexLoadCheckValue, indexCheckValue)
	}
	loadedBlockLogCheckedValue := binary.LittleEndian.Uint32(contentBytes[contentLength-4:])
	blockLogCheckedValue := crc32.Checksum(contentBytes[:contentLength-BLOCK_LOG_CHECK_N_BYTES], crc32.MakeTable(crc32.Castagnoli))
	if loadedBlockLogCheckedValue != blockLogCheckedValue {
		return 0, nil, fmt.Errorf("load block log checksum(%d) mismatch computed valued(%d)", loadedBlockLogCheckedValue, blockLogCheckedValue)
	}
	blockID := int64(binary.LittleEndian.Uint64(contentBytes[:8]))
	data, err := parseBytes(contentLength, contentBytes)
	return blockID, data, err
}

func (c *BlockLogReader) Close() {
	if c.r != nil {
		c.r.Close()
	}
}

func parseBytes(contentLength int, contentBytes []byte) ([][][]byte, error) {
	flag := 0
	logRecordLength := 0
	cursor := BLOCK_PREFIX_N_BYTES
	contentOffsetLen := make([][]int, 0)
	tagsOffsetMap := make(map[int][]byte)
	recordCount := 0
	tagCount := 0
	for {
		if cursor+4 >= contentLength {
			break
		}
		flag = int(binary.LittleEndian.Uint32(contentBytes[cursor : cursor+4]))
		logRecordLength = flag & FLAG_LENGTH_MASK
		if cursor+logRecordLength > contentLength {
			break
		}
		switch flag >> FLAG_HEAD_SHIFT {
		case FLAG_CONTENT:
			contentOffsetLen = append(contentOffsetLen, []int{cursor, logRecordLength})
			recordCount++
		case FLAG_TAGS:
			tagsOffsetMap[cursor] = contentBytes[cursor+TAGS_RECORD_PREFIX_N_BYTES : cursor+logRecordLength]
			tagCount++
		}
		cursor += logRecordLength
	}
	records := make([][][]byte, recordCount)
	for i := 0; i < recordCount; i++ {
		records[i] = make([][]byte, 3)
		offsetLen := contentOffsetLen[i]
		offset := offsetLen[0]
		length := offsetLen[1]
		tagOffset := int(binary.LittleEndian.Uint32(contentBytes[offset+4 : offset+8]))
		tags := tagsOffsetMap[tagOffset]
		timestamp := contentBytes[offset+8 : offset+16]
		content := contentBytes[offset+CONTENT_RECORD_PREFIX_N_BYTES : offset+length]
		records[i][0] = tags
		records[i][1] = content
		records[i][2] = timestamp
	}
	return records, nil
}
