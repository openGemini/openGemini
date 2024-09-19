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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type IndexMergeSet interface {
	GetSeries(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error
}

type ShowTagValuesPlan interface {
	Execute(dst map[string]*TagSets, tagKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) error
	Stop()
}

type EngineShard interface {
	IsOpened() bool
	OpenAndEnable(client metaclient.MetaClient) error
	GetDataPath() string
	GetIdent() *meta.ShardIdentifier
}

type showTagValuesPlan struct {
	idx   IndexMergeSet
	table TablesStore

	metaClient metaclient.MetaClient
	shard      EngineShard

	mu      sync.RWMutex
	handler SequenceIteratorHandler
	itr     SequenceIterator
	stopped bool

	logger *Log.Logger
}

func NewShowTagValuesPlan(table TablesStore, idx IndexMergeSet, logger *Log.Logger, sh EngineShard, client metaclient.MetaClient) ShowTagValuesPlan {
	return &showTagValuesPlan{
		table:      table,
		idx:        idx,
		logger:     logger,
		shard:      sh,
		metaClient: client,
	}
}

func (p *showTagValuesPlan) Stop() {
	p.mu.Lock()
	p.stopped = true
	if p.itr != nil {
		p.itr.Stop()
	}
	p.mu.Unlock()
}

func (p *showTagValuesPlan) Execute(dst map[string]*TagSets, tagKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	if p.handler == nil || p.itr == nil {
		p.handler = NewTagValuesIteratorHandler(p.idx, condition, &tr, limit)
		p.itr = NewSequenceIterator(p.handler, p.logger)
	}
	p.mu.Unlock()

	defer func() {
		p.itr.Release()
	}()

	// lazy shard open
	if !p.shard.IsOpened() {
		sh := p.shard
		start := time.Now()
		p.logger.Info("lazy shard open start", zap.String("path", sh.GetDataPath()), zap.Uint32("pt", sh.GetIdent().OwnerPt))
		if err := sh.OpenAndEnable(p.metaClient); err != nil {
			p.logger.Error("lazy shard open error", zap.Error(err), zap.Duration("duration", time.Since(start)))
			return err
		}
		p.logger.Info("lazy shard open end", zap.Duration("duration", time.Since(start)))
	}

	var err error
	var reachLimitCount int
	for mst, keys := range tagKeys {
		if tagSets, ok := dst[mst]; !ok {
			dst[mst] = NewTagSets()
		} else if limit > 0 && tagSets.TagKVCount() >= limit {
			reachLimitCount++
			continue
		}

		order, unordered, _ := p.table.GetBothFilesRef(mst, true, tr, nil)
		p.itr.AddFiles(order)
		p.itr.AddFiles(unordered)
		err = p.handler.Init(map[string]interface{}{
			InitParamKeyDst:         dst[mst],
			InitParamKeyKeys:        keys,
			InitParamKeyMeasurement: mst,
		})
		if err != nil {
			return err
		}

		err = p.itr.Run()
		if errors.Is(err, io.EOF) {
			// io.EOF represents that tagKV pair count reached the limit
			reachLimitCount++
			continue
		} else if err != nil {
			break
		}
		p.itr.Release()
	}

	// If all measurements reach the limit, return io.EOF
	if limit > 0 && reachLimitCount == len(dst) {
		return io.EOF
	}

	// Because `err` is reused, io.EOF indicates that part of measurements reaches limit.
	// Catch this exception.
	if errors.Is(err, io.EOF) {
		return nil
	} else {
		return err
	}
}

type TagValuesIteratorHandler struct {
	chunkMeta ChunkMeta
	idx       IndexMergeSet

	limit       int
	tr          *util.TimeRange
	measurement string
	keys        [][]byte
	condition   influxql.Expr

	sets   *TagSets
	keySet map[string]struct{}
	sidSet map[uint64]struct{}
}

func NewTagValuesIteratorHandler(idx IndexMergeSet, condition influxql.Expr, tr *util.TimeRange, limit int) *TagValuesIteratorHandler {
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

func (h *TagValuesIteratorHandler) NextChunkMeta(buf []byte) error {
	cm := &h.chunkMeta
	cm.reset()
	_, err := cm.UnmarshalWithColumns(buf, []string{"value"})
	if err != nil {
		return err
	}

	// SeriesID may be duplicate in multiple TSSP files.
	if _, exists := h.sidSet[cm.sid]; exists {
		return nil
	}
	h.sidSet[cm.sid] = struct{}{}

	minTime, maxTime := cm.MinMaxTime()
	if !h.tr.Overlaps(minTime, maxTime) {
		return nil
	}

	err = h.idx.GetSeries(cm.sid, buf[:0], h.condition, func(key *influx.SeriesKey) {
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
	return h.limit > 0 && h.sets.TagKVCount() >= h.limit
}

func (h *TagValuesIteratorHandler) Finish() {}
