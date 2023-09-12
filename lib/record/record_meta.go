/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

// nolint
package record

type RecMeta struct {
	IntervalIndex []int
	// Times used to store the time for
	// first/last aggregation
	Times    [][]int64
	tagIndex []int
	tags     []*[]byte
	ColMeta  []ColMeta // used for pre agg
}

func (r *RecMeta) IsEmpty() bool {
	for _, meta := range r.ColMeta {
		if !meta.IsEmpty() {
			return false
		}
	}
	return true
}

func (r *RecMeta) AssignRecMetaTimes(t [][]int64) {
	r.Times = t
}

func (r *RecMeta) Copy() *RecMeta {
	copyMeta := &RecMeta{}
	copyMeta.IntervalIndex = make([]int, len(r.IntervalIndex))
	copyMeta.tagIndex = make([]int, len(r.tagIndex))
	copyMeta.ColMeta = make([]ColMeta, len(r.ColMeta))
	copyMeta.tags = make([]*[]byte, len(r.tags))
	copyMeta.Times = make([][]int64, len(r.Times))

	copy(copyMeta.IntervalIndex, r.IntervalIndex)
	copy(copyMeta.tagIndex, r.tagIndex)
	for index, colM := range r.ColMeta {
		val, err := colM.Clone()
		if err != nil {
			copyMeta.ColMeta[index] = ColMeta{}
		} else {
			copyMeta.ColMeta[index] = val
		}

	}

	for index, tag := range r.tags {
		copyMeta.tags[index] = cloneBytes(*tag)
	}

	for i, times := range r.Times {
		copyMeta.Times[i] = make([]int64, len(times))
		copy(copyMeta.Times[i], times)
	}

	return copyMeta
}

func cloneBytes(v []byte) *[]byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return &clone
}

type ColMeta struct {
	isSetFlag bool // check whether ColMeta has been set value. default false.

	min     interface{}
	max     interface{}
	minTime int64
	maxTime int64

	first     interface{}
	last      interface{}
	firstTime int64
	lastTime  int64

	sum   interface{}
	count interface{}
}

func (m *ColMeta) Init() {
	m.isSetFlag = false

	m.min = nil
	m.max = nil
	m.minTime = 0
	m.maxTime = 0

	m.first = nil
	m.last = nil
	m.firstTime = 0
	m.lastTime = 0

	m.sum = nil
	m.count = nil
}

func (m *ColMeta) IsEmpty() bool {
	return !m.isSetFlag
}

func (m *ColMeta) Min() (interface{}, int64) {
	return m.min, m.minTime
}

func (m *ColMeta) Clone() (ColMeta, error) {
	if m == nil {
		return ColMeta{}, nil
	}
	var clone ColMeta

	clone.isSetFlag = m.isSetFlag
	clone.min = m.min
	clone.max = m.max
	clone.minTime = m.minTime
	clone.maxTime = m.maxTime

	clone.first = m.first
	clone.last = m.last
	clone.firstTime = m.firstTime
	clone.lastTime = m.lastTime

	clone.sum = m.sum
	clone.count = m.count
	return clone, nil
}

func (m *ColMeta) Max() (interface{}, int64) {
	return m.max, m.maxTime
}

func (m *ColMeta) First() (interface{}, int64) {
	return m.first, m.firstTime
}

func (m *ColMeta) Last() (interface{}, int64) {
	return m.last, m.lastTime
}

func (m *ColMeta) Sum() interface{} {
	return m.sum
}

func (m *ColMeta) Count() interface{} {
	return m.count
}

func (m *ColMeta) SetMin(min interface{}, minTime int64) {
	m.isSetFlag = true
	m.min = min
	m.minTime = minTime
}

func (m *ColMeta) SetMax(max interface{}, maxTime int64) {
	m.isSetFlag = true
	m.max = max
	m.maxTime = maxTime
}

func (m *ColMeta) SetFirst(first interface{}, firstTime int64) {
	m.isSetFlag = true
	m.first = first
	m.firstTime = firstTime
}

func (m *ColMeta) SetLast(last interface{}, lastTime int64) {
	m.isSetFlag = true
	m.last = last
	m.lastTime = lastTime
}

func (m *ColMeta) SetSum(sum interface{}) {
	m.isSetFlag = true
	m.sum = sum
}

func (m *ColMeta) SetCount(count interface{}) {
	m.isSetFlag = true
	m.count = count
}
