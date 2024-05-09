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
