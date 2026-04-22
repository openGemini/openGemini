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

func (cv *ColVal) AppendFloats(values ...float64) {
	appendValues(cv, values...)
}

func (cv *ColVal) AppendFloat(v float64) {
	appendValue(cv, v)
}

func (cv *ColVal) RemoveLastFloat() {
	removeLastValue[float64](cv)
}

func (cv *ColVal) AppendFloatNullReserve() {
	appendNullReserve[float64](cv)
}

func (cv *ColVal) AppendFloatNull() {
	appendNull(cv)
}

func (cv *ColVal) AppendFloatNulls(count int) {
	appendNulls(cv, count)
}

func (cv *ColVal) FloatValues() []float64 {
	return values[float64](cv)
}

func (cv *ColVal) FloatValue(i int) (float64, bool) {
	return value(cv, cv.FloatValues(), i)
}

func (cv *ColVal) SubFloatValues(start, end int) []float64 {
	return subValues[float64](cv, start, end)
}

func (cv *ColVal) UpdateFloatValue(v float64, isNil bool, row int) {
	updateValue(cv, v, isNil, row)
}

func (cv *ColVal) FloatValueWithNullReserve(index int) (float64, bool) {
	return values[float64](cv)[index], cv.IsNil(index)
}

func (cv *ColVal) MaxFloatValue(values []float64, start, end int) (float64, int) {
	return maxValue(values, start, end, cv)
}

func (cv *ColVal) MinFloatValue(values []float64, start, end int) (float64, int) {
	return minValue(values, start, end, cv)
}

func (cv *ColVal) FirstFloatValue(values []float64, start, end int) (float64, int) {
	return firstValue(values, start, end, cv)
}

func (cv *ColVal) LastFloatValue(values []float64, start, end int) (float64, int) {
	return lastValue(values, start, end, cv)
}

func (cv *ColVal) MaxFloatValues(values []float64, start, end int) (float64, []int) {
	return maxValues(values, start, end, cv)
}

func (cv *ColVal) MinFloatValues(values []float64, start, end int) (float64, []int) {
	return minValues(values, start, end, cv)
}
