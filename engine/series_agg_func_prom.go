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

package engine

import (
	"math"

	"github.com/openGemini/openGemini/lib/record"
)

func MinPromReduce(_ *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	minValue := values[start]
	for i := start + 1; i < end; i++ {
		if minValue > values[start] || math.IsNaN(minValue) {
			minValue = values[start]
		}
	}
	return start, minValue, false
}

func MinPromMerge(prevBuf, currBuf *floatColBuf) {
	if prevBuf.value > currBuf.value || math.IsNaN(prevBuf.value) {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func MaxPromReduce(_ *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	maxValue := values[start]
	for i := start + 1; i < end; i++ {
		if maxValue < values[start] || math.IsNaN(maxValue) {
			maxValue = values[start]
		}
	}
	return start, maxValue, false
}

func MaxPromMerge(prevBuf, currBuf *floatColBuf) {
	if prevBuf.value < currBuf.value || math.IsNaN(prevBuf.value) {
		prevBuf.index = currBuf.index
		prevBuf.time = currBuf.time
		prevBuf.value = currBuf.value
	}
}

func FloatCountPromReduce(cv *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	count := float64(cv.ValidCount(start, end))
	return start, count, count == 0
}

func FloatCountPromMerge(prevBuf, currBuf *floatColBuf) {
	prevBuf.value += currBuf.value
}
