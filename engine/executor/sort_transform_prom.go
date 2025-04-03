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

package executor

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

const (
	sortByValue = iota
	sortByLabel
)

type PromSortTransform struct {
	BaseProcessor
	input           *ChunkPort
	inputChunk      chan Chunk
	nextInputChunk  chan Semaphore
	output          *ChunkPort
	outputChunkPool *CircularChunkPool
	closedSignal    int32

	preChunk      Chunk
	preLabelKeys  []string
	preLabelVals  []string
	preTags       []byte
	preTimes      []int64
	preVals       []float64
	hasInit       bool
	sortGroups    PromSortGroups
	sortType      int
	sortFields    []string
	sortAscending bool

	schema *QuerySchema
	opt    *query.ProcessorOptions
	logger *logger.Logger
	span   *tracing.Span
	errs   errno.Errs
}

const (
	promSortTransform = "PromSortTransform"
)

var _ = RegistryTransformCreator(&LogicalPromSort{}, &PromSortTransformCreator{})

type PromSortTransformCreator struct {
}

func (c *PromSortTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	child := plan.Children()
	if len(child) != 1 {
		return nil, fmt.Errorf("PromSortTransformCreator doesn't support multiple input source")
	}
	p, err := NewPromSortTransform(
		child[0].RowDataType(),
		plan.RowDataType(),
		plan.Schema().(*QuerySchema),
		plan.(*LogicalPromSort).sortFields,
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func NewPromSortTransform(inRowDataType, outRowDataType hybridqp.RowDataType, s *QuerySchema, sortFields influxql.SortFields) (*PromSortTransform, error) {
	trans := &PromSortTransform{
		input:           NewChunkPort(inRowDataType),
		inputChunk:      make(chan Chunk, 1),
		nextInputChunk:  make(chan Semaphore),
		output:          NewChunkPort(outRowDataType),
		outputChunkPool: NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		sortGroups:      NewPromSortGroups(),
		schema:          s,
		opt:             s.opt.(*query.ProcessorOptions),
		logger:          logger.NewLogger(errno.ModuleQueryEngine),
	}
	err := trans.initSortFields(sortFields)
	if err != nil {
		return nil, err
	}
	return trans, nil
}

func (trans *PromSortTransform) initSortFields(sortFields influxql.SortFields) error {
	if len(sortFields) == 0 {
		return fmt.Errorf("PromSortTransform sortFields length check failed")
	}
	trans.sortAscending = sortFields[0].Ascending
	if len(sortFields) == 1 && sortFields[0].Name == "value" {
		trans.sortType = sortByValue
		return nil
	}
	trans.sortType = sortByLabel
	for _, field := range sortFields {
		trans.sortFields = append(trans.sortFields, field.Name)
		if field.Ascending != trans.sortAscending {
			return fmt.Errorf("PromSortTransform sortFields ascending check failed")
		}
	}
	return nil
}

func (trans *PromSortTransform) Name() string {
	return promSortTransform
}

func (trans *PromSortTransform) Explain() []ValuePair {
	return nil
}

func (trans *PromSortTransform) Close() {
	trans.Once(func() {
		atomic.AddInt32(&trans.closedSignal, 1)
		trans.output.Close()
	})
}

func (trans *PromSortTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[PromSortTransform] TotalWorkCost", false)
	trans.span = tracing.Start(span, "cost_for_prom_sort", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.span)
	}()
	errs := &trans.errs
	errs.Init(2, nil)
	go trans.runnable(ctx, errs)
	go trans.WorkHelper(ctx, errs)
	return errs.Err()
}

func (trans *PromSortTransform) runnable(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.logger.Error(err.Error(), zap.String("query", "PromSortTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		select {
		case c, ok := <-trans.input.State:
			if !ok {
				return
			}
			trans.inputChunk <- c
		case <-ctx.Done():
			atomic.AddInt32(&trans.closedSignal, 1)
			return
		}
	}
}

func (trans *PromSortTransform) WorkHelper(ctx context.Context, errs *errno.Errs) {
	defer func() {
		close(trans.nextInputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.logger.Error(err.Error(), zap.String("query", "PromSortTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()

	for {
		if atomic.LoadInt32(&trans.closedSignal) > 0 {
			return
		}
		c, ok := <-trans.inputChunk
		if !ok {
			trans.flushSortGroups()
			err := trans.workSort()
			if err != nil {
				errs.Dispatch(err)
			}
			trans.workOutput()
			return
		}
		trans.addChunk(c)
	}
}

func (trans *PromSortTransform) addChunk(c Chunk) {
	for i, tag := range c.Tags() {
		if trans.hasInit && !bytes.Equal(trans.preTags, tag.subset) {
			trans.flushSortGroups()
		}
		trans.preLabelKeys, trans.preLabelVals = tag.GetChunkTagAndValues()
		trans.hasInit = true
		trans.preTags = tag.subset
		col := c.Column(0)
		var endLoc int
		startLoc := c.TagIndex()[i]
		if i == c.TagLen()-1 {
			endLoc = c.Len()
		} else {
			endLoc = c.TagIndex()[i+1]
		}
		for ; startLoc < endLoc; startLoc++ {
			trans.preTimes = append(trans.preTimes, c.TimeByIndex(startLoc))
			trans.preVals = append(trans.preVals, col.FloatValue(col.GetValueIndexV2(startLoc)))
		}
	}
	trans.preChunk = c
}

func (trans *PromSortTransform) flushSortGroups() {
	trans.sortGroups.AppendMany(trans.preLabelKeys, trans.preLabelVals, trans.preVals, trans.preTimes)
	trans.preVals = trans.preVals[:0]
	trans.preTimes = trans.preTimes[:0]
}

func (trans *PromSortTransform) workSort() error {
	switch trans.sortType {
	case sortByValue:
		trans.sortGroups.sortByValue(trans.sortAscending)
	case sortByLabel:
		trans.sortGroups.sortByLabels(trans.sortFields, trans.sortAscending)
	default:
		return fmt.Errorf("unexpected PromSortTransform sortType: %v", trans.sortType)
	}
	return nil
}

func (trans *PromSortTransform) workOutput() {
	chunk := trans.outputChunkPool.GetChunk()
	chunk.SetName(trans.preChunk.Name())
	for _, row := range trans.sortGroups.inner {
		chunk.AppendTagsAndIndex(*NewChunkTagsByTagKVs(row.tagKey, row.tagValue), chunk.Len())
		for i, time := range row.timestamp {
			chunk.AppendTime(time)
			chunk.Column(0).AppendNotNil()
			chunk.Column(0).AppendFloatValue(row.value[i])
			if chunk.Len() >= trans.schema.GetOptions().ChunkSizeNum() {
				trans.sendChunk(chunk)
				chunk = trans.outputChunkPool.GetChunk()
				chunk.SetName(trans.preChunk.Name())
			}
		}
	}
	trans.sendChunk(chunk)
}

func (trans *PromSortTransform) sendChunk(c Chunk) {
	if c.Len() > 0 {
		trans.output.State <- c
	}
}

func (trans *PromSortTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *PromSortTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *PromSortTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *PromSortTransform) GetInputNumber(_ Port) int {
	return 0
}

type PromSortGroups struct {
	inner []*PromSortSeries
}

func NewPromSortGroups() PromSortGroups {
	return PromSortGroups{
		inner: make([]*PromSortSeries, 0),
	}
}

func (rows *PromSortGroups) AppendMany(tagKey, tagValue []string, value []float64, timestamp []int64) {
	rows.inner = append(rows.inner, NewPromSortSeries(tagKey, tagValue, value, timestamp))
}

func (rows *PromSortGroups) sortByValue(ascending bool) {
	slices.SortFunc(rows.inner, func(a, b *PromSortSeries) int {
		return a.compareByValue(b, ascending)
	})
}

func (rows *PromSortGroups) sortByLabels(fields []string, ascending bool) {
	slices.SortFunc(rows.inner, func(a, b *PromSortSeries) int {
		return a.compareByFields(b, fields, ascending)
	})
}

type PromSortSeries struct {
	tagKey    []string
	tagValue  []string
	value     []float64
	timestamp []int64
}

// As range query results always have a fixed output ordering, we don't care how values are comparing in such a situation.
// So we simply take the first sample value for each series.
func (r *PromSortSeries) Value() float64 {
	return r.value[0]
}

func NewPromSortSeries(tagKey, tagValue []string, value []float64, timestamp []int64) *PromSortSeries {
	return &PromSortSeries{
		tagKey:    tagKey,
		tagValue:  tagValue,
		value:     append([]float64{}, value...),
		timestamp: append([]int64{}, timestamp...),
	}
}

func (lhs *PromSortSeries) compareByValue(rhs *PromSortSeries, ascending bool) int {
	// Prometheus sort `NaN` timeseires after all other valid timeseires
	lNaN := math.IsNaN(lhs.Value())
	rNaN := math.IsNaN(rhs.Value())
	if lNaN && rNaN {
		return 0
	}
	if rNaN || lessHelper(lhs.Value(), rhs.Value(), ascending) {
		return -1
	}
	if lNaN || lessHelper(rhs.Value(), lhs.Value(), ascending) {
		return +1
	}
	return 0
}

func (lhs *PromSortSeries) compareByFields(rhs *PromSortSeries, fields []string, ascending bool) int {
	for _, field := range fields {
		result := natureCompare(lhs.getTagValueByKey(field), rhs.getTagValueByKey(field), ascending)
		if result != 0 {
			return result
		}
	}
	// Prometheus `sort_by_label` give a consistent result by sorting rest labels, just follow it
	return lhs.compareByAllTags(rhs)
}

func (lhs *PromSortSeries) compareByAllTags(rhs *PromSortSeries) int {
	lhsLen := len(lhs.tagKey)
	rhsLen := len(rhs.tagKey)

	commonLen := lhsLen
	if rhsLen < commonLen {
		commonLen = rhsLen
	}

	for i := range commonLen {
		if lhs.tagKey[i] != rhs.tagKey[i] {
			if lhs.tagKey[i] < rhs.tagKey[i] {
				return -1
			}
			return +1
		}
		if lhs.tagValue[i] != rhs.tagValue[i] {
			if lhs.tagValue[i] < rhs.tagValue[i] {
				return -1
			}
			return +1
		}
	}

	if lhsLen < rhsLen {
		return -1
	}
	return +1
}

func (r *PromSortSeries) getTagValueByKey(key string) string {
	for id, realKey := range r.tagKey {
		if realKey == key {
			return r.tagValue[id]
		}
	}
	return ""
}

var chunkifyRegexp = regexp.MustCompile(`(\d+|\D+)`)

func natureCompare(lhs, rhs string, ascending bool) int {
	lhsChunks := chunkifyRegexp.FindAllString(lhs, -1)
	rhsChunks := chunkifyRegexp.FindAllString(rhs, -1)

	lhsLen := len(lhsChunks)
	rhsLen := len(rhsChunks)

	for i := range lhsLen {
		if i >= rhsLen {
			return -1
		}

		a, errA := strconv.Atoi(lhsChunks[i])
		b, errB := strconv.Atoi(rhsChunks[i])

		if errA == nil && errB == nil {
			if a == b {
				continue
			}
			if lessHelper(a, b, ascending) {
				return -1
			}
			return +1
		}

		if lhsChunks[i] == rhsChunks[i] {
			continue
		}
		if lessHelper(lhsChunks[i], rhsChunks[i], ascending) {
			return -1
		}
		return +1
	}
	return 0
}

func lessHelper[T cmp.Ordered](a, b T, ascending bool) bool {
	if ascending {
		return a < b
	}
	return b < a
}
