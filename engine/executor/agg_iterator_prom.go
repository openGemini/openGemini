/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor

import (
	"math"
	"sort"
	"strconv"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
)

const leTagName = "le"

type FloatColFloatHistogramIterator struct {
	inOrdinal            int
	outOrdinal           int
	fn                   FloatColReduceHistogramReduce
	metricWithBucketsMap map[string]*metricWithBuckets
}

type bucket struct {
	upperBound float64
	count      float64
}

type buckets []bucket

func (b buckets) Len() int {
	return len(b)
}

func (b buckets) Less(i, j int) bool {
	return b[i].upperBound < b[j].upperBound
}

func (b buckets) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

type metricWithBuckets struct {
	name    string
	init    bool
	buckets []buckets
	times   []int64
}

func NewFloatColFloatHistogramIterator(fn FloatColReduceHistogramReduce, inOrdinal, outOrdinal int, rowDataType hybridqp.RowDataType) *FloatColFloatHistogramIterator {
	return &FloatColFloatHistogramIterator{
		fn:                   fn,
		inOrdinal:            inOrdinal,
		outOrdinal:           outOrdinal,
		metricWithBucketsMap: make(map[string]*metricWithBuckets, 0),
	}
}

func (r *FloatColFloatHistogramIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	var upperBound float64
	var insertIndex, tagInd int
	var metricName string
	for i, curIndex := range inChunk.IntervalIndex() {
		var err error
		if curIndex == inChunk.TagIndex()[tagInd] {
			insertIndex = 0
			// get bytes without le
			metricNameBytes, le := inChunk.Tags()[tagInd].DecodeTagsWithoutTag(leTagName)

			if tagInd < len(inChunk.TagIndex())-1 {
				tagInd++
			}
			upperBound, err = strconv.ParseFloat(le, 64)
			if err != nil {
				metricName = ""
				continue
			}

			metricName = string(metricNameBytes)

			if r.metricWithBucketsMap[metricName] == nil {
				r.metricWithBucketsMap[metricName] = &metricWithBuckets{
					name:    metricName,
					times:   make([]int64, 0),
					buckets: make([]buckets, 0),
					init:    true,
				}
			} else {
				r.metricWithBucketsMap[metricName].init = false
			}

		}
		curMetric := r.metricWithBucketsMap[metricName]
		if curMetric == nil {
			continue
		}
		time := inChunk.TimeByIndex(i)
		b := bucket{upperBound, inChunk.Column(r.inOrdinal).FloatValues()[curIndex]}
		if curMetric.init {
			curMetric.buckets = append(curMetric.buckets, []bucket{b})
			curMetric.times = append(curMetric.times, time)
		} else {
			insertBucket(curMetric, &insertIndex, time, b)
			insertIndex++

		}
	}
	if p.lastChunk {
		r.processBuckets(inChunk, outChunk)
	}
}

func insertBucket(curMetric *metricWithBuckets, insertIndex *int, time int64, b bucket) {
	if *insertIndex >= len(curMetric.times) {
		curMetric.times = append(curMetric.times, time)
		bs := []bucket{b}
		curMetric.buckets = append(curMetric.buckets, bs)
		return
	}
	if time == curMetric.times[*insertIndex] {
		curMetric.buckets[*insertIndex] = append(curMetric.buckets[*insertIndex], b)
		return
	}
	if time < curMetric.times[*insertIndex] {
		curMetric.times = append(curMetric.times[:*insertIndex], append([]int64{time}, curMetric.times[*insertIndex:]...)...)
		bs := []bucket{b}
		curMetric.buckets = append(curMetric.buckets[:*insertIndex], append([]buckets{bs}, curMetric.buckets[*insertIndex:]...)...)
	} else {
		*insertIndex++
		insertBucket(curMetric, insertIndex, time, b)
	}
}

func (r *FloatColFloatHistogramIterator) processBuckets(inChunk, outChunk Chunk) {
	if len(r.metricWithBucketsMap) == 0 {
		return
	}
	for name, m := range r.metricWithBucketsMap {
		chunkTag := NewChunkTagsByBytes([]byte(name))
		outChunk.AppendTagsAndIndex(*chunkTag, outChunk.Len())
		outColumn := outChunk.Column(r.outOrdinal)
		for i, buckets := range m.buckets {
			if len(buckets) > 0 {
				sort.Sort(buckets)
				buckets = coalesceBuckets(buckets)
				val := r.fn(buckets)

				outChunk.AppendTime(m.times[i])
				outChunk.AppendIntervalIndex(outChunk.Len() - 1)
				outColumn.AppendNotNil()
				outColumn.AppendFloatValue(val)
			}
		}

	}
	r.metricWithBucketsMap = make(map[string]*metricWithBuckets)
}

/*
Copyright 2015 The Prometheus Authors
This code is originally from: https://github.com/prometheus/prometheus/blob/main/promql/quantile.go
*/
// coalesceBuckets merges buckets with the same upper bound.
//
// The input buckets must be sorted.
func coalesceBuckets(buckets buckets) buckets {
	last := buckets[0]
	i := 0
	for _, b := range buckets[1:] {
		if b.upperBound == last.upperBound {
			last.count += b.count
		} else {
			buckets[i] = last
			last = b
			i++
		}
	}
	buckets[i] = last
	return buckets[:i+1]
}

type CountValuesIterator struct {
	inOrdinal     int
	outOrdinal    int
	tagName       string
	valueCountMap map[float64]*ValueCount
	mapSortKey    []float64
}

func NewCountValuesIterator(inOrdinal, outOrdinal int, tagName string) *CountValuesIterator {
	return &CountValuesIterator{
		inOrdinal:     inOrdinal,
		outOrdinal:    outOrdinal,
		tagName:       tagName,
		valueCountMap: make(map[float64]*ValueCount),
	}
}

type ValueCount struct {
	times  []int64
	counts []float64
}

func (r *CountValuesIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	firstIndex, lastIndex := 0, len(inChunk.IntervalIndex())-1
	var end, tagInd int
	for i, start := range inChunk.IntervalIndex() {
		if start == inChunk.TagIndex()[tagInd] {
			if start != firstIndex {
				r.processCounts(inChunk, outChunk, tagInd-1)
			}
			if tagInd < len(inChunk.TagIndex())-1 {
				tagInd++
			}
		}
		if i < lastIndex {
			end = inChunk.IntervalIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()
		}
		r.appendValueCount(start, end, inChunk)
	}
	if !p.sameTag {
		r.processCounts(inChunk, outChunk, tagInd)
	}
}

func (r *CountValuesIterator) appendValueCount(start, end int, inChunk Chunk) {
	time := inChunk.TimeByIndex(start)
	for j := start; j < end; j++ {
		value := inChunk.Column(r.inOrdinal).FloatValue(j)
		if valueCount, ok := r.valueCountMap[value]; ok {
			if valueCount.times[len(valueCount.times)-1] != time {
				valueCount.times = append(valueCount.times, time)
				valueCount.counts = append(valueCount.counts, 1)
			} else {
				valueCount.counts[len(valueCount.counts)-1]++
			}

		} else {
			r.mapSortKey = append(r.mapSortKey, value)
			r.valueCountMap[value] = &ValueCount{
				times:  []int64{time},
				counts: []float64{1},
			}
		}
	}
}

func (r *CountValuesIterator) processCounts(inChunk, outChunk Chunk, tagInd int) {
	tag := inChunk.Tags()[tagInd].RemoveKeys([]string{r.tagName})
	sort.Float64s(r.mapSortKey)
	column := outChunk.Column(r.outOrdinal)
	for _, value := range r.mapSortKey {
		valueCount := r.valueCountMap[value]
		tagK, tagV := tag.GetChunkTagAndValues()
		tagK = append(tagK, r.tagName)
		tagV = append(tagV, strconv.FormatFloat(value, 'f', -1, 64))

		chunkTag := NewChunkTagsByTagKVs(tagK, tagV)
		outChunk.AppendTagsAndIndex(*chunkTag, outChunk.Len())
		for i := 0; i < len(valueCount.times); i++ {
			outChunk.AppendTime(valueCount.times[i])
			outChunk.AppendIntervalIndex(outChunk.Len() - 1)
			column.AppendNotNil()
			column.AppendFloatValue(valueCount.counts[i])
		}
	}
	r.valueCountMap = make(map[float64]*ValueCount)
	r.mapSortKey = make([]float64, 0)
}

type ScalarBuf struct {
	times  []int64
	values []float64
}

type ScalarIterator struct {
	inOrdinal     int
	outOrdinal    int
	lastTag       string
	isMultiSeries bool
	buf           ScalarBuf
}

func NewScalarIterator(inOrdinal, outOrdinal int) *ScalarIterator {
	return &ScalarIterator{
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
	}
}

func (r *ScalarIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	inColumn := inChunk.Column(r.inOrdinal)

	if r.isMultiSeries {
		return
	}
	if inChunk.TagLen() > 1 || (r.lastTag != "" && r.lastTag != string(inChunk.Tags()[0].subset)) {
		r.isMultiSeries = true
		if inChunk.TagLen() > 1 {
			length := inChunk.TagIndex()[1]
			times := inChunk.Time()[:length]
			values := inColumn.FloatValues()[:length]
			r.buf.times = append(r.buf.times, times...)
			r.buf.values = append(r.buf.values, values...)
		}
		r.processBuffer(outChunk)
	} else {
		r.lastTag = string(inChunk.Tags()[0].subset)
	}

	if r.isMultiSeries {
		return
	}

	r.buf.times = append(r.buf.times, inChunk.Time()...)
	r.buf.values = append(r.buf.values, inColumn.FloatValues()...)

	if p.lastChunk {
		r.processBuffer(outChunk)
	}
}

func (r *ScalarIterator) processBuffer(outChunk Chunk) {
	column := outChunk.Column(r.outOrdinal)
	chunkTag := &ChunkTags{}
	outChunk.AppendTagsAndIndex(*chunkTag, 0)

	for i := 0; i < len(r.buf.times); i++ {
		val := r.buf.values[i]
		if r.isMultiSeries {
			val = math.NaN()
		}
		outChunk.AppendTime(r.buf.times[i])
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
		column.AppendNotNil()
		column.AppendFloatValue(val)
	}
}

type AbsentIterator struct {
	inOrdinal    int
	outOrdinal   int
	steps        int
	fullTimeFlag bool
	opt          *query.ProcessorOptions
	buf          *SliceItem[float64]
	timeMap      map[int64]struct{}
}

func NewAbsentIterator(inOrdinal, outOrdinal int, opt *query.ProcessorOptions) *AbsentIterator {
	var steps int
	if opt.IsPromInstantQuery() {
		steps = 1
	} else if opt.IsRangeVectorSelector() {
		steps = int((opt.EndTime-opt.StartTime-int64(opt.Range))/int64(opt.Step)) + 1
	} else {
		steps = int((opt.EndTime-opt.StartTime-int64(opt.LookBackDelta))/int64(opt.Step)) + 1
	}
	return &AbsentIterator{
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		steps:      steps,
		opt:        opt,
		timeMap:    make(map[int64]struct{}),
		buf:        NewSliceItem[float64](),
	}
}

func (r *AbsentIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	if r.fullTimeFlag || r.opt.IsPromInstantQuery() {
		return
	}
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	firstIndex, lastIndex := 0, len(inChunk.TagIndex())-1
	times := inChunk.Time()
	var end int
	for i, start := range inChunk.TagIndex() {
		if i < lastIndex {
			end = inChunk.TagIndex()[i+1]
		} else {
			end = inChunk.NumberOfRows()

		}

		if i == firstIndex {
			if r.buf.Len()+(end-start) == r.steps {
				r.buf.Reset()
				r.fullTimeFlag = true
				return
			}
			r.processFirstWindow(start, end, times)
		} else if i == lastIndex && p.sameTag {
			r.buf.time = times[start:end]
		} else {
			if end-start == r.steps {
				r.fullTimeFlag = true
				return
			}
			r.processMiddleWindow(start, end, times)
		}
		if len(r.timeMap) == r.steps {
			r.fullTimeFlag = true
			break
		}
	}
	if p.lastChunk {
		r.processTimeMap(outChunk)
		r.fullTimeFlag = false
		r.timeMap = make(map[int64]struct{})
	}
}

func (r *AbsentIterator) processFirstWindow(start, end int, times []int64) {
	for _, t := range r.buf.time {
		r.timeMap[t] = struct{}{}
	}
	for i := start; i < end; i++ {
		r.timeMap[times[i]] = struct{}{}
	}
	r.buf.Reset()
}

func (r *AbsentIterator) processMiddleWindow(start, end int, times []int64) {
	for i := start; i < end; i++ {
		r.timeMap[times[i]] = struct{}{}
	}
}

func (r *AbsentIterator) processTimeMap(outChunk Chunk) {
	outColumn := outChunk.Column(r.outOrdinal)
	keys := make([]string, 0)
	values := make([]string, 0)
	kvMap := make(map[string]string)
	kCountMap := make(map[string]int)
	createLabelsFromCondition(r.opt.Condition, kvMap, kCountMap)
	for k, v := range kvMap {
		if kCountMap[k] == 1 {
			keys = append(keys, k)
			values = append(values, v)
		}
	}
	chunkTags := NewChunkTagsByTagKVs(keys, values)
	outChunk.AppendTagsAndIndex(*chunkTags, outChunk.Len())
	startTime := r.opt.StartTime
	if r.opt.IsRangeVectorSelector() {
		startTime += int64(r.opt.Range)
	} else {
		startTime += int64(r.opt.LookBackDelta)
	}
	step := int64(r.opt.Step)
	for ts := startTime; ts <= r.opt.EndTime; ts += step {
		if _, ok := r.timeMap[ts]; !ok {
			outChunk.AppendTime(ts)
			outChunk.AppendIntervalIndex(outChunk.Len() - 1)
			outColumn.AppendNotNil()
			outColumn.AppendFloatValue(1)
		}
	}
}

func createLabelsFromCondition(expr influxql.Expr, kvMap map[string]string, kCountMap map[string]int) {
	e, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return
	}
	createLabelsFromCondition(e.LHS, kvMap, kCountMap)
	createLabelsFromCondition(e.RHS, kvMap, kCountMap)
	if e.Op != influxql.EQ {
		return
	}
	k, ok1 := e.LHS.(*influxql.VarRef)
	v, ok2 := e.RHS.(*influxql.StringLiteral)
	if ok1 && ok2 {
		if k.Val == promql2influxql.DefaultMetricKeyLabel {
			return
		}
		if _, ok := kCountMap[k.Val]; ok {
			kCountMap[k.Val]++
			return
		}
		kCountMap[k.Val] = 1
		kvMap[k.Val] = v.Val
	}

}

func AbsentWithOutDataAlive(opt *query.ProcessorOptions, outChunk Chunk) {
	outColumn := outChunk.Column(0)
	keys := make([]string, 0)
	values := make([]string, 0)
	kvMap := make(map[string]string)
	kCountMap := make(map[string]int)
	createLabelsFromCondition(opt.Condition, kvMap, kCountMap)
	for k, v := range kvMap {
		if kCountMap[k] == 1 {
			keys = append(keys, k)
			values = append(values, v)
		}
	}
	chunkTags := NewChunkTagsByTagKVs(keys, values)
	outChunk.AppendTagsAndIndex(*chunkTags, outChunk.Len())
	startTime := opt.StartTime
	if opt.IsRangeVectorSelector() {
		startTime += int64(opt.Range)
	} else {
		startTime += int64(opt.LookBackDelta)
	}
	if opt.IsPromInstantQuery() {
		outChunk.AppendTime(startTime)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
		outColumn.AppendNotNil()
		outColumn.AppendFloatValue(1)
		return
	}
	step := int64(opt.Step)
	for ts := startTime; ts <= opt.EndTime; ts += step {
		outChunk.AppendTime(ts)
		outChunk.AppendIntervalIndex(outChunk.Len() - 1)
		outColumn.AppendNotNil()
		outColumn.AppendFloatValue(1)
	}
}
