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
	"math"
	"sort"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/bloomfilter"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
)

type IndexReader struct {
	version              uint32
	blockCount           int64
	minTime              int64
	maxTime              int64
	currStartBlockOffset int64
	path                 string
	tr                   util.TimeRange
	metaReader           MetaReader
	filterReader         *bloomfilter.FilterReader
	opt                  hybridqp.Options
	option               *obs.ObsOptions
	span                 *tracing.Span
}

func NewIndexReader(option *obs.ObsOptions, version uint32, path string, tr util.TimeRange, opt hybridqp.Options) (*IndexReader, error) {
	s := &IndexReader{
		version:              version,
		path:                 path,
		minTime:              math.MaxInt64,
		maxTime:              math.MinInt64,
		tr:                   tr,
		opt:                  opt,
		option:               option,
		currStartBlockOffset: 0,
	}
	metaLen, err := GetMetaLen(option, path)
	if err != nil {
		return nil, err
	}
	s.blockCount = metaLen
	err = s.init()
	return s, err
}

func (s *IndexReader) init() (err error) {
	splitMap := make(map[string][]byte)
	splitMap[CONTENT_FIELD] = tokenizer.CONTENT_SPLIT_TABLE
	splitMap[TAG_FIELD] = tokenizer.TAGS_SPLIT_TABLE
	s.filterReader, err = bloomfilter.NewFilterReader(s.option, s.opt.GetCondition(), splitMap, true, true, s.version, s.path, s.path, logstore.FilterLogName, logstore.OBSVLMFileName)
	return
}

func (s *IndexReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	if s.filterReader != nil {
		s.filterReader.StartSpan(s.span)
	}
	s.span.CreateCounter(MetaReadNumSpan, "")
	s.span.CreateCounter(MetaReadDurationSpan, "ns")
	s.span.Count(MetaReadNumSpan, s.blockCount/int64(META_STORE_N_BYTES))
}

func (s *IndexReader) initMetaReader() (bool, error) {
	if s.currStartBlockOffset >= s.blockCount {
		return false, nil
	}
	var err error
	if s.blockCount-s.currStartBlockOffset > int64(MAX_SCAN_META_Length) {
		s.metaReader, err = NewMetaReader(s.option, s.path, s.currStartBlockOffset, int64(MAX_SCAN_META_Length), s.tr, true, true)
		s.currStartBlockOffset += int64(MAX_SCAN_META_Length)
	} else {
		s.metaReader, err = NewMetaReader(s.option, s.path, s.currStartBlockOffset, s.blockCount-s.currStartBlockOffset, s.tr, true, true)
		s.currStartBlockOffset = s.blockCount
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *IndexReader) GetMaxBlockId() int64 {
	return s.blockCount / int64(META_STORE_N_BYTES)
}

func (s *IndexReader) Get() ([]*MetaData, error) {
	metaDatas := make(MetaDatas, 0, MAX_SCAN_META_COUNT)
	readerMetaDatas := make(MetaDatas, 0, MAX_SCAN_META_COUNT)
	var tm time.Time
	for {
		if s.span != nil {
			tm = time.Now()
		}
		if s.metaReader == nil {
			if success, err := s.initMetaReader(); err != nil {
				return nil, err
			} else if !success {
				break
			}
		}
		currMetaData, err := s.metaReader.next()
		if err != nil {
			return nil, err
		}
		if currMetaData == nil {
			sort.Sort(readerMetaDatas)
			metaDatas = append(metaDatas, readerMetaDatas...)
			readerMetaDatas = make(MetaDatas, 0, MAX_SCAN_META_COUNT)
			s.metaReader.Close()
			s.metaReader = nil
			if s.span != nil {
				s.span.Count(MetaReadDurationSpan, int64(time.Since(tm)))
			}
			continue
		}
		isExist, _ := s.filterReader.IsExist(currMetaData.blockIndex, nil)
		if isExist {
			if currMetaData.maxTimestamp > s.maxTime {
				s.maxTime = currMetaData.maxTimestamp
			}
			if currMetaData.minTimestamp < s.minTime {
				s.minTime = currMetaData.minTimestamp
			}
			readerMetaDatas = append(readerMetaDatas, currMetaData)
		}
	}
	return metaDatas, nil
}

func (s *IndexReader) GetMinMaxTime() (int64, int64) {
	return s.minTime, s.maxTime
}

func (s *IndexReader) Close() {
	if s.filterReader != nil {
		s.filterReader.Close()
	}
}
