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
	"sort"
	"strconv"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util"
)

const leTagName = "le"

type FloatColFloatHistogramIterator struct {
	inOrdinal         int
	outOrdinal        int
	fn                FloatColReduceHistogramReduce
	metricWithBuckets *metricWithBuckets
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
	buckets []buckets
	times   []int64
}

func (b *metricWithBuckets) clear() {
	b.buckets = b.buckets[:0]
	b.times = b.times[:0]
}

func NewFloatColFloatHistogramIterator(fn FloatColReduceHistogramReduce, inOrdinal, outOrdinal int, rowDataType hybridqp.RowDataType) *FloatColFloatHistogramIterator {
	return &FloatColFloatHistogramIterator{
		fn:         fn,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		metricWithBuckets: &metricWithBuckets{
			times:   make([]int64, 0),
			buckets: make([]buckets, 0),
		},
	}
}

func (r *FloatColFloatHistogramIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	var upperBound float64
	var insertIndex, tagInd int
	var initBuckets bool
	var metricNameBytes []byte
	lastIndex := len(inChunk.IntervalIndex()) - 1
	for i, curIndex := range inChunk.IntervalIndex() {
		var err error
		if curIndex == inChunk.TagIndex()[tagInd] {
			insertIndex = 0
			var le string
			// get bytes without le
			metricNameBytes, le = inChunk.Tags()[tagInd].decodeTagsWithoutTag(leTagName)
			if len(metricNameBytes) == 0 {
				continue
			}
			upperBound, err = strconv.ParseFloat(le, 64)
			if err != nil {
				continue
			}

			if r.metricWithBuckets.name != util.Bytes2str(metricNameBytes) {
				initBuckets = true
				// process buckets
				r.processBuckets(inChunk, outChunk)
				r.metricWithBuckets.name = string(metricNameBytes)
			} else {
				initBuckets = false
			}
			if tagInd < len(inChunk.TagIndex())-1 {
				tagInd++
			}

		}
		time := inChunk.TimeByIndex(i)
		b := bucket{upperBound, inChunk.Column(r.inOrdinal).FloatValues()[curIndex]}
		if initBuckets {
			r.metricWithBuckets.buckets = append(r.metricWithBuckets.buckets, []bucket{b})
			r.metricWithBuckets.times = append(r.metricWithBuckets.times, time)
		} else {
			r.metricWithBuckets.buckets[insertIndex] = append(r.metricWithBuckets.buckets[insertIndex], b)
		}
		insertIndex++
		if p.lastChunk && i == lastIndex {
			r.processBuckets(inChunk, outChunk)
		}
	}
}

func (r *FloatColFloatHistogramIterator) processBuckets(inChunk, outChunk Chunk) {
	if len(r.metricWithBuckets.buckets) == 0 {
		return
	}
	chunkTag := NewChunkTagsByBytes([]byte(r.metricWithBuckets.name))
	outChunk.AppendTagsAndIndex(*chunkTag, outChunk.Len())
	for i, buckets := range r.metricWithBuckets.buckets {
		if len(buckets) > 0 {
			sort.Sort(buckets)
			buckets = coalesceBuckets(buckets)
			val := r.fn(buckets)

			outChunk.AppendTime(r.metricWithBuckets.times[i])
			outChunk.AppendIntervalIndex(outChunk.Len() - 1)
			outChunk.Column(r.outOrdinal).AppendNotNil()
			outChunk.Column(r.outOrdinal).AppendFloatValue(val)
		}
	}
	r.metricWithBuckets.clear()
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

func NewCountValuesIterator(inOrdinal, outOrdinal int, rowDataType hybridqp.RowDataType, tagName string) *CountValuesIterator {
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
	tag := inChunk.Tags()[tagInd]
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
