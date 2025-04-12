// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package immutable

import (
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func init() {
	RegistryDDLRespData(hybridqp.ShowTagValues, NewTagSets)
	RegistryDDLSequenceHandler(hybridqp.ShowTagValues, NewTagValuesIteratorHandler)
}

type IndexMergeSet interface {
	GetSeries(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error
	GetSeriesBytes(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesBytes)) error
	SearchSeriesKeys(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error)
}

type EngineShard interface {
	IsOpened() bool
	OpenAndEnable(client metaclient.MetaClient) error
	GetDataPath() string
	GetIdent() *meta.ShardIdentifier
}

type TagSets struct {
	//   map[tag key][tag value]struct{}
	sets       map[string]map[string]struct{}
	totalCount int
}

func NewTagSets() DDLRespData {
	return &TagSets{
		sets: make(map[string]map[string]struct{}),
	}
}

func (s *TagSets) Add(key, val string) {
	if valueSet, keyExist := s.sets[key]; !keyExist {
		s.sets[key] = map[string]struct{}{val: {}}
		s.totalCount++
	} else if _, valueExist := valueSet[val]; !valueExist {
		valueSet[val] = struct{}{}
		s.totalCount++
	}
}

func (s *TagSets) ForEach(process func(tagKey, tagValue string)) {
	for tagKey, tagValues := range s.sets {
		for tagValue := range tagValues {
			process(tagKey, tagValue)
		}
	}
}

func (s *TagSets) Count() int {
	return s.totalCount
}

type TagValuesIteratorHandler struct {
	idx IndexMergeSet

	limit       int
	tr          *util.TimeRange
	measurement string
	keys        [][]byte
	condition   influxql.Expr

	sets   *TagSets
	keySet map[string]struct{}
	sidSet map[uint64]struct{}
}

func NewTagValuesIteratorHandler(idx IndexMergeSet, condition influxql.Expr, tr *util.TimeRange, limit int) SequenceIteratorHandler {
	return &TagValuesIteratorHandler{
		idx:       idx,
		condition: condition,
		limit:     limit,
		tr:        tr,
		sidSet:    make(map[uint64]struct{}),
	}
}

const (
	InitParamKeyDst         string = "dst"
	InitParamKeyKeys        string = "keys"
	InitParamKeyMeasurement string = "measurement"
)

func (h *TagValuesIteratorHandler) Init(param map[string]interface{}) error {
	// dst *TagSets
	dst, ok := param[InitParamKeyDst]
	if !ok {
		return fmt.Errorf("TagValuesIteratorHandler init param invalid: %v", param)
	}
	sets, ok := dst.(*TagSets)
	if !ok {
		return fmt.Errorf("TagValuesIteratorHandler init param invalid: %v", param)
	}
	h.sets = sets

	// keys [][]byte
	ks, ok := param[InitParamKeyKeys]
	if !ok {
		return fmt.Errorf("TagValuesIteratorHandler init param invalid: %v", param)
	}
	keys, ok := ks.([][]byte)
	if !ok {
		return fmt.Errorf("TagValuesIteratorHandler init param invalid: %v", param)
	}
	h.keys = keys

	// measurement string
	mst, ok := param[InitParamKeyMeasurement]
	if !ok {
		return fmt.Errorf("TagValuesIteratorHandler init param invalid: %v", param)
	}
	measurement, ok := mst.(string)
	if !ok {
		return fmt.Errorf("TagValuesIteratorHandler init param invalid: %v", param)
	}
	h.measurement = measurement

	h.keySet = make(map[string]struct{})
	for _, keyBytes := range keys {
		h.keySet[string(keyBytes)] = struct{}{}
	}
	return nil
}

func (h *TagValuesIteratorHandler) Begin() {}

func (h *TagValuesIteratorHandler) NextFile(TSSPFile) {}

func (h *TagValuesIteratorHandler) NextChunkMeta(cm *ChunkMeta) error {
	// SeriesID may be duplicate in multiple TSSP files.
	if _, exists := h.sidSet[cm.sid]; exists {
		return nil
	}
	h.sidSet[cm.sid] = struct{}{}

	minTime, maxTime := cm.MinMaxTime()
	if !h.tr.Overlaps(minTime, maxTime) {
		return nil
	}

	err := h.idx.GetSeries(cm.sid, nil, h.condition, func(key *influx.SeriesKey) {
		if string(key.Measurement) != h.measurement {
			return
		}
		for _, tag := range key.TagSet {
			tagKey, tagValue := string(tag.Key), string(tag.Value)
			if _, ok := h.keySet[tagKey]; ok {
				h.sets.Add(tagKey, tagValue)
			}
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (h *TagValuesIteratorHandler) Limited() bool {
	return h.limit > 0 && h.sets.Count() >= h.limit
}

func (h *TagValuesIteratorHandler) Finish() {}
