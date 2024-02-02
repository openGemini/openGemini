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
	"sort"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/bloomfilter"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	ReaderContentSpan         = "reader_content_num_span"
	ReaderReadContentSizeSpan = "reader_content_read_size_span"
	ReaderReadContentDuration = "reader_content_read_duration"
	MetaReadNumSpan           = "meta_read_num_span"
	MetaReadDurationSpan      = "meta_read_duration"
)

var (
	FileCursorPool = record.NewRecordPool(record.LogStoreReaderPool)
)

type SegmentReader struct {
	isAscending          bool
	isFirstQuery         bool
	isReadCache          bool
	version              uint32
	rowCount             int64
	currStartBlockOffset int64
	blockCount           int64
	curOffsets           []int64
	curLength            []int64
	curBlockIndex        []int64
	schema               record.Schemas
	opt                  hybridqp.Options
	metaReader           MetaReader
	tm                   time.Time
	filterReader         *bloomfilter.FilterReader
	contentReader        *ContentReader
	tr                   util.TimeRange
	metaDataQueue        MetaControl
	option               *obs.ObsOptions
	span                 *tracing.Span
	path                 string
}

func NewSegmentReader(option *obs.ObsOptions, path string, version uint32, tr util.TimeRange, opt hybridqp.Options, schema record.Schemas, isAscending bool) (*SegmentReader, error) {
	s := &SegmentReader{
		isFirstQuery:  true,
		tr:            tr,
		opt:           opt,
		path:          path,
		isReadCache:   false,
		option:        option,
		version:       version,
		schema:        schema,
		isAscending:   isAscending,
		metaDataQueue: NewMetaControl(isAscending, MAX_SCAN_META_COUNT),
	}
	metaLen, err := GetMetaLen(option, path)
	if err != nil {
		return nil, err
	}
	if s.isAscending {
		s.currStartBlockOffset = 0
	} else {
		s.currStartBlockOffset = metaLen
	}
	s.blockCount = metaLen
	splitMap := make(map[string][]byte)
	splitMap[CONTENT_FIELD] = tokenizer.CONTENT_SPLIT_TABLE
	splitMap[TAG_FIELD] = tokenizer.TAGS_SPLIT_TABLE
	s.filterReader, err = bloomfilter.NewFilterReader(option, s.opt.GetCondition(), splitMap, true, true, version, path, path, logstore.FilterLogName, logstore.OBSVLMFileName)
	return s, err
}

func (s *SegmentReader) getBlockIndex(target int64) int64 {
	left, right := 0, len(s.curOffsets)-1
	if s.isAscending {
		for left <= right {
			mid := (left + right) / 2
			if s.curOffsets[mid] == target {
				return s.curBlockIndex[mid]
			} else if s.curOffsets[mid] < target {
				left = mid + 1
			} else {
				right = mid - 1
			}
		}
		return -1
	} else {
		for left <= right {
			mid := (left + right) / 2
			if s.curOffsets[mid] == target {
				return s.curBlockIndex[mid]
			} else if s.curOffsets[mid] > target {
				left = mid + 1
			} else {
				right = mid - 1
			}
		}
		return -1
	}
}

func (s *SegmentReader) Next() ([][][]byte, int64, error) {
	if s.contentReader != nil {
		d, blockOffset, err := s.contentReader.Next()
		if err != nil {
			return nil, 0, err
		}
		if d != nil {
			if blockOffset == -1 {
				return d, s.blockCount / int64(META_STORE_N_BYTES), nil
			}
			return d, s.getBlockIndex(blockOffset), nil
		}
	}
	if !s.isFirstQuery {
		if s.isAscending && s.currStartBlockOffset >= s.blockCount || (!s.isAscending && s.currStartBlockOffset <= 0) && s.metaDataQueue.IsEmpty() {
			return nil, 0, nil
		}
	}
	if success, err := s.initContentReader(); err != nil {
		return nil, 0, err
	} else if !success {
		return nil, 0, nil
	}
	d, blockOffset, err := s.contentReader.Next()
	if err != nil {
		return nil, 0, err
	}
	if d == nil {
		return nil, 0, nil
	}
	if blockOffset == -1 {
		return d, s.blockCount / int64(META_STORE_N_BYTES), nil
	}
	return d, s.getBlockIndex(blockOffset), nil
}

func (s *SegmentReader) initMeta() (bool, error) {
	var tm time.Time
	if s.span != nil {
		tm = time.Now()
	}
	var err error
	if s.isAscending {
		if s.currStartBlockOffset >= s.blockCount {
			return false, nil
		}
		if s.blockCount-s.currStartBlockOffset > int64(MAX_SCAN_META_Length) {
			s.metaReader, err = NewMetaReader(s.option, s.path, s.currStartBlockOffset, int64(MAX_SCAN_META_Length), s.tr, true, true)
			if err != nil {
				return false, err
			}
			s.currStartBlockOffset += int64(MAX_SCAN_META_Length)
		} else {
			s.metaReader, err = NewMetaReader(s.option, s.path, s.currStartBlockOffset, s.blockCount-s.currStartBlockOffset, s.tr, true, true)
			if err != nil {
				return false, err
			}
			s.currStartBlockOffset = s.blockCount
		}
	} else {
		if s.currStartBlockOffset <= 0 {
			return false, nil
		}
		if s.currStartBlockOffset > int64(MAX_SCAN_META_Length) {
			s.currStartBlockOffset -= int64(MAX_SCAN_META_Length)
			s.metaReader, err = NewMetaReader(s.option, s.path, s.currStartBlockOffset, int64(MAX_SCAN_META_Length), s.tr, true, true)
			if err != nil {
				return false, err
			}
		} else {
			s.metaReader, err = NewMetaReader(s.option, s.path, 0, s.currStartBlockOffset, s.tr, true, true)
			if err != nil {
				return false, err
			}
			s.currStartBlockOffset = 0
		}
	}

	readerMetaDatas := make(MetaDatas, 0, MAX_SCAN_META_COUNT)
	for {
		currMetaData, _ := s.metaReader.next()
		if currMetaData == nil {
			s.metaReader.Close()
			break
		}
		readerMetaDatas = append(readerMetaDatas, currMetaData)
	}
	sort.Sort(readerMetaDatas)
	if s.span != nil {
		s.span.Count(MetaReadDurationSpan, int64(time.Since(tm)))
	}
	for _, meta := range readerMetaDatas {
		s.rowCount += int64(meta.blockRecordsCount)
		isExist, _ := s.filterReader.IsExist(meta.blockIndex, nil)
		if isExist {
			s.metaDataQueue.Push(meta)
		}
	}
	return true, nil
}

func (s *SegmentReader) GetRowCount() int64 {
	return s.rowCount
}

func (s *SegmentReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	s.tm = time.Now()
	s.span.CreateCounter(ReaderContentSpan, "")
	s.span.CreateCounter(ReaderReadContentSizeSpan, "")
	s.span.CreateCounter(ReaderReadContentDuration, "ns")
	s.span.CreateCounter(MetaReadNumSpan, "")
	s.span.CreateCounter(MetaReadDurationSpan, "ns")
	s.span.CreateCounter(ReaderContentSpan, "")
	s.span.Count(MetaReadNumSpan, s.blockCount/int64(META_STORE_N_BYTES))
	if s.filterReader != nil {
		s.filterReader.StartSpan(s.span)
	}
}

func (s *SegmentReader) initContentReader() (bool, error) {
	scanBlockCount := logstore.GetConstant(s.version).ScanBlockCnt
	offsets := make([]int64, 0, scanBlockCount)
	length := make([]int64, 0, scanBlockCount)
	blockIndex := make([]int64, 0, scanBlockCount)
	for {
		if len(offsets) >= scanBlockCount {
			break
		}
		if !s.metaDataQueue.IsEmpty() {
			meta, _ := s.metaDataQueue.Pop()
			m := meta.(*MetaData)
			if m.minTimestamp <= s.tr.Max && m.maxTimestamp >= s.tr.Min {
				offsets = append(offsets, m.contentBlockOffset)
				length = append(length, int64(m.contentBlockLength))
				blockIndex = append(blockIndex, m.GetBlockIndex())
			}
			continue
		} else {
			status, err := s.initMeta()
			if err != nil {
				return false, err
			}
			if !status {
				break
			}
		}
	}

	if len(offsets) == 0 && !s.isFirstQuery {
		return false, nil
	}
	var err error
	s.contentReader, err = NewContentReader(s.option, s.path, offsets, length, s.schema, s.isFirstQuery, s.blockCount/int64(META_STORE_N_BYTES), true)
	s.contentReader.StartSpan(s.span)
	s.curOffsets = offsets
	s.curLength = length
	s.curBlockIndex = blockIndex
	s.isFirstQuery = false
	return true, err
}

func (s *SegmentReader) UpdateTr(time int64) {
	if s.isAscending {
		s.tr.Max = time
	} else {
		s.tr.Min = time
	}
}

func (s *SegmentReader) SetTr(tr util.TimeRange) {
	s.tr = tr
}

func (s *SegmentReader) Close() {
	if s.filterReader != nil {
		s.filterReader.Close()
	}
	if s.contentReader != nil {
		s.contentReader.Close()
	}
}
