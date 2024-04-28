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
)

func MinPromReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	minValue := c.Column(ordinal).FloatValue(start)
	for i := start + 1; i < end; i++ {
		v := c.Column(ordinal).FloatValue(i)
		if minValue > v || math.IsNaN(minValue) {
			minValue = v
		}
	}
	return start, minValue, false
}

func MinPromMerge(prevPoint, currPoint *FloatPoint) {
	if prevPoint.value > currPoint.value || math.IsNaN(prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func MaxPromReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	maxValue := c.Column(ordinal).FloatValue(start)
	for i := start + 1; i < end; i++ {
		v := c.Column(ordinal).FloatValue(i)
		if maxValue < v || math.IsNaN(maxValue) {
			maxValue = v
		}
	}
	return start, maxValue, false
}

func MaxPromMerge(prevPoint, currPoint *FloatPoint) {
	if prevPoint.value < currPoint.value || math.IsNaN(prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func FloatCountPromReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	count := float64(end - start)
	return start, count, count == 0
}

func FloatCountPromMerge(prevPoint, currPoint *FloatPoint) {
	prevPoint.value += currPoint.value
}
