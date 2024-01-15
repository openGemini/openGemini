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
	"time"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/pierrec/lz4/v4"
)

const (
	CONTENT_NAME                = "segment.cnt"
	MAX_BLOCK_LOG_STORE_N_BYTES = BLOCK_FLUSH_TRIGGER_N_BYTES + (RECORD_MAX_N_BYTES * 2)
)

type ContentReader struct {
	readBlockLog       bool
	contentStoreReader fileops.BasicFileReader
	maxBlockID         int64
	schema             record.Schemas
	readChan           chan *request.StreamReader
	option             *obs.ObsOptions
	offset             []int64
	length             []int64
	path               string
	blockLogReader     *BlockLogReader
	span               *tracing.Span
}

func NewContentReader(option *obs.ObsOptions, path string, offset []int64, length []int64, schema record.Schemas, readBlockLog bool, maxBlockID int64, isStat bool) (*ContentReader, error) {
	contentReader := &ContentReader{
		option:     option,
		schema:     schema,
		offset:     offset,
		length:     length,
		maxBlockID: maxBlockID,
		path:       path,
	}
	fd, err := obs.OpenObsFile(path, CONTENT_NAME, option)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	contentReader.contentStoreReader = dr
	readChan := make(chan *request.StreamReader, RecordPoolNum)
	contentReader.readChan = readChan
	contentReader.contentStoreReader.StreamReadBatch(offset, length, readChan, -1)

	if readBlockLog {
		contentReader.blockLogReader, err = NewBlockLogReader(option, isStat, path)
		if err != nil || contentReader.blockLogReader == nil {
			readBlockLog = false
		}
	}
	contentReader.readBlockLog = readBlockLog
	return contentReader, nil
}

func (s *ContentReader) Next() ([][][]byte, int64, error) {
	var tm time.Time
	if s.span != nil {
		tm = time.Now()
	}
	if s.readBlockLog {
		s.readBlockLog = false
		bytes, off, _, blockId, err := s.blockLogReader.Next()
		// when empty blg block is read, try to read from cnt
		if err == nil && len(bytes) == 0 {
			return s.Next()
		} else if s.maxBlockID != -1 && s.maxBlockID != blockId {
			// sfs flush not right
			logger.GetLogger().Info(fmt.Sprintf("sfs maxBlockID:%d, meta maxBlockID: %d", blockId, s.maxBlockID))
			return s.Next()
		} else {
			if s.span != nil {
				s.span.Count(ReaderReadContentDuration, int64(time.Since(tm)))
			}
			return bytes, off, err
		}
	}
	for r := range s.readChan {
		if r.Err != nil {
			return nil, 0, r.Err
		}
		records, err := s.readContent(r.Content)
		if err != nil {
			// ignore readContent error
			logger.GetLogger().Error("read content [" + s.path + "] failed: " + err.Error())
			continue
		}
		if s.span != nil {
			s.span.Count(ReaderReadContentDuration, int64(time.Since(tm)))
		}
		return records, r.Offset, nil
	}
	return nil, 0, nil
}

func (s *ContentReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	s.span.Count(ReaderContentSpan, int64(len(s.offset)))
	totalLength := int64(0)
	for _, v := range s.length {
		totalLength += v
	}
	s.span.Count(ReaderReadContentSizeSpan, totalLength)
}

func (c *ContentReader) readContent(bytes []byte) (data [][][]byte, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("read content err")
		}
	}()
	originalLength := len(bytes) - 4
	contentLength := int(binary.LittleEndian.Uint32(bytes[:4]))
	contentBytes := make([]byte, contentLength)
	_, err = lz4.UncompressBlock(bytes[4:originalLength], contentBytes)
	if err != nil {
		return nil, err
	}
	indexLoadCheckValue := binary.LittleEndian.Uint32(contentBytes[8:12])
	indexCheckValue := crc32.Checksum(contentBytes[:8], crc32.MakeTable(crc32.Castagnoli))
	if indexCheckValue != indexLoadCheckValue {
		return nil, fmt.Errorf("load content index checksum(%d) mismatch computed valued(%d)", indexLoadCheckValue, indexCheckValue)
	}
	return parseBytes(contentLength, contentBytes)
}

func (s *ContentReader) Close() {
	if s.blockLogReader != nil {
		s.blockLogReader.Close()
	}
	if s.contentStoreReader != nil {
		s.contentStoreReader.Close()
	}
}
