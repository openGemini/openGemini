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

	"github.com/openGemini/openGemini/engine/hybridqp"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

func init() {
	RegistryDDLRespData(hybridqp.ShowSeries, NewSeriesKeys)
	RegistryDDLSequenceHandler(hybridqp.ShowSeries, NewSeriesKeysIteratorHandler)
}

// GenDDLRespDataFunc as a function to generate DDLRespData
type GenDDLRespDataFunc func() DDLRespData

// GenSequenceHandlerFunc as a function to generate SequenceHandler
type GenSequenceHandlerFunc func(idx IndexMergeSet, condition influxql.Expr, tr *util.TimeRange, limit int) SequenceIteratorHandler

// DDLRespDataFactory as a factory to use RespData
var DDLRespDataFactory = make(map[hybridqp.DDLType]GenDDLRespDataFunc)

func RegistryDDLRespData(ddl hybridqp.DDLType, f GenDDLRespDataFunc) {
	_, ok := DDLRespDataFactory[ddl]
	if ok {
		return
	}
	DDLRespDataFactory[ddl] = f
}

func GetDDLRespData(ddl hybridqp.DDLType) GenDDLRespDataFunc {
	return DDLRespDataFactory[ddl]
}

// DDLSequenceHandlerFactory as a factory to use SequenceHandler
var DDLSequenceHandlerFactory = make(map[hybridqp.DDLType]GenSequenceHandlerFunc)

func RegistryDDLSequenceHandler(ddl hybridqp.DDLType, f GenSequenceHandlerFunc) {
	_, ok := DDLSequenceHandlerFactory[ddl]
	if ok {
		return
	}
	DDLSequenceHandlerFactory[ddl] = f
}

func GetDDLDDLSequenceHandler(ddl hybridqp.DDLType) GenSequenceHandlerFunc {
	return DDLSequenceHandlerFactory[ddl]
}

// DDLBasePlan as an abstraction of the DDL base plan
type DDLBasePlan interface {
	Execute(dst map[string]DDLRespData, mstKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) error
	Stop()
}

// DDLRespData as an abstraction of the DDL response data
type DDLRespData interface {
	Add(key, value string)
	ForEach(process func(key, value string))
	Count() int
}

type SeriesKeys struct {
	sets       map[string]struct{} // map[series]struct{}
	totalCount int
}

func NewSeriesKeys() DDLRespData {
	return &SeriesKeys{
		sets: make(map[string]struct{}),
	}
}

func (s *SeriesKeys) Add(key, _ string) {
	if _, ok := s.sets[key]; !ok {
		s.sets[key] = struct{}{}
		s.totalCount++
	}
}

func (s *SeriesKeys) ForEach(process func(key, _ string)) {
	for key := range s.sets {
		process(key, key)
	}
}

func (s *SeriesKeys) Count() int {
	return s.totalCount
}

type ddlBasePlan struct {
	idx   IndexMergeSet
	table TablesStore

	metaClient metaclient.MetaClient
	shard      EngineShard

	mu      sync.RWMutex
	handler SequenceIteratorHandler
	itr     SequenceIterator
	stopped bool

	logger *Log.Logger

	fn GenDDLRespDataFunc
	fv GenSequenceHandlerFunc
}

func NewDDLBasePlan(table TablesStore, idx IndexMergeSet, logger *Log.Logger, sh EngineShard, client metaclient.MetaClient, ddl hybridqp.DDLType) DDLBasePlan {
	return &ddlBasePlan{
		table:      table,
		idx:        idx,
		logger:     logger,
		shard:      sh,
		metaClient: client,
		fn:         GetDDLRespData(ddl),
		fv:         GetDDLDDLSequenceHandler(ddl),
	}
}

func (p *ddlBasePlan) Stop() {
	p.mu.Lock()
	p.stopped = true
	if p.itr != nil {
		p.itr.Stop()
	}
	p.mu.Unlock()
}

func (p *ddlBasePlan) Execute(dst map[string]DDLRespData, mstKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	if p.handler == nil || p.itr == nil {
		p.handler = p.fv(p.idx, condition, &tr, limit)
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
	for mst, keys := range mstKeys {
		if seriesKeys, ok := dst[mst]; !ok {
			dst[mst] = p.fn()
		} else if limit > 0 && seriesKeys.Count() >= limit {
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

type SeriesKeysIteratorHandler struct {
	idx IndexMergeSet

	limit       int
	tr          *util.TimeRange
	measurement string
	condition   influxql.Expr

	sets   *SeriesKeys
	sidSet map[uint64]struct{}
}

func NewSeriesKeysIteratorHandler(idx IndexMergeSet, condition influxql.Expr, tr *util.TimeRange, limit int) SequenceIteratorHandler {
	return &SeriesKeysIteratorHandler{
		idx:       idx,
		condition: condition,
		limit:     limit,
		tr:        tr,
		sidSet:    make(map[uint64]struct{}),
	}
}

func (h *SeriesKeysIteratorHandler) Init(param map[string]interface{}) error {
	// dst *SeriesKeys
	dst, ok := param[InitParamKeyDst]
	if !ok {
		return fmt.Errorf("SeriesKeysIteratorHandler init param invalid: %v", param)
	}
	sets, ok := dst.(*SeriesKeys)
	if !ok {
		return fmt.Errorf("SeriesKeysIteratorHandler init param invalid: %v", param)
	}
	h.sets = sets

	mst, ok := param[InitParamKeyMeasurement]
	if !ok {
		return fmt.Errorf("SeriesKeysIteratorHandler init param invalid: %v", param)
	}
	measurement, ok := mst.(string)
	if !ok {
		return fmt.Errorf("SeriesKeysIteratorHandler init param invalid: %v", param)
	}
	h.measurement = measurement
	return nil
}

func (h *SeriesKeysIteratorHandler) Begin() {}

func (h *SeriesKeysIteratorHandler) NextFile(TSSPFile) {}

func (h *SeriesKeysIteratorHandler) NextChunkMeta(cm *ChunkMeta) error {
	// SeriesKeysID may be duplicate in multiple TSSP files.
	if _, exists := h.sidSet[cm.sid]; exists {
		return nil
	}
	h.sidSet[cm.sid] = struct{}{}

	minTime, maxTime := cm.MinMaxTime()
	if !h.tr.Overlaps(minTime, maxTime) {
		return nil
	}

	err := h.idx.GetSeriesBytes(cm.sid, nil, h.condition, func(key *influx.SeriesBytes) {
		if string(key.Measurement) != h.measurement {
			return
		}
		seriesKey := string(key.Series)
		h.sets.Add(seriesKey, seriesKey)
	})
	if err != nil {
		return err
	}

	return nil
}

func (h *SeriesKeysIteratorHandler) Limited() bool {
	return h.limit > 0 && h.sets.Count() >= h.limit
}

func (h *SeriesKeysIteratorHandler) Finish() {}
