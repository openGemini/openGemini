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

type TCountMinSketch struct {
	Depth int
	Width int
	Table [][]TCounter
}

func NewTCountMinSketch(depth int, widthLog2 int) TCountMinSketch {
	re := TCountMinSketch{
		Depth: depth,
		Width: 1 << widthLog2,
		Table: make([][]TCounter, depth),
	}
	for i := 0; i < depth; i++ {
		re.Table[i] = make([]TCounter, re.Width)
	}
	return re
}

func (cms *TCountMinSketch) Get(indices []int) TCounter {
	result := cms.Table[0][indices[0]]
	for i := 1; i < cms.Depth; i++ {
		result = min(result, cms.Table[i][indices[i]])
	}
	return result
}

func (cms *TCountMinSketch) GetCounter(depth int, index int) TCounter {
	return cms.Table[depth][index]
}

func (cms *TCountMinSketch) Add(indices []int, value TCounter) {
	for i := 0; i < cms.Depth; i++ {
		cms.Table[i][indices[i]] += value
	}
}

func min(a, b TCounter) TCounter {
	if a < b {
		return a
	}
	return b
}
