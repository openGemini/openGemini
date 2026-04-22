// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import (
	"github.com/openGemini/openGemini/lib/util"
)

func (cv *ColVal) AppendIntegers(values ...int64) {
	appendValues(cv, values...)
}

func (cv *ColVal) AppendInteger(v int64) {
	appendValue(cv, v)
}

func (cv *ColVal) RemoveLastInteger() {
	removeLastValue[int64](cv)
}

func (cv *ColVal) AppendIntegerNulls(count int) {
	appendNulls(cv, count)
}

func (cv *ColVal) AppendIntegerNull() {
	appendNull(cv)
}

func (cv *ColVal) IntegerValues() []int64 {
	return values[int64](cv)
}

func (cv *ColVal) Int8Values() []int8 {
	return util.Bytes2Int8Slice(cv.Val)
}

func (cv *ColVal) SubIntegerValues(start, end int) []int64 {
	return subValues[int64](cv, start, end)
}

func (cv *ColVal) IntegerValue(i int) (int64, bool) {
	return value(cv, cv.IntegerValues(), i)
}

func (cv *ColVal) AppendIntegerNullReserve() {
	appendNullReserve[int64](cv)
}

func (cv *ColVal) UpdateIntegerValue(v int64, isNil bool, row int) {
	updateValue(cv, v, isNil, row)
}

func (cv *ColVal) IntegerValueWithNullReserve(index int) (int64, bool) {
	return cv.IntegerValues()[index], cv.IsNil(index)
}

func (cv *ColVal) MaxIntegerValue(values []int64, start, end int) (int64, int) {
	return maxValue(values, start, end, cv)
}

func (cv *ColVal) MinIntegerValue(values []int64, start, end int) (int64, int) {
	return minValue(values, start, end, cv)
}

func (cv *ColVal) FirstIntegerValue(values []int64, start, end int) (int64, int) {
	return firstValue(values, start, end, cv)
}

func (cv *ColVal) LastIntegerValue(values []int64, start, end int) (int64, int) {
	return lastValue(values, start, end, cv)
}

func (cv *ColVal) MaxIntegerValues(values []int64, start, end int) (int64, []int) {
	return maxValues(values, start, end, cv)
}

func (cv *ColVal) MinIntegerValues(values []int64, start, end int) (int64, []int) {
	return minValues(values, start, end, cv)
}
