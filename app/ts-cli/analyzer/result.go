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

package analyzer

type AnalyzeResult struct {
	seriesTotal  int
	segmentTotal int
	rowTotal     int
	columnTotal  int

	fileSize     int
	originSize   map[int]int // key=ref.Type
	compressSize map[int]int
	offsetSize   int // for string
	bitmapSize   int
	oneRowChunk  int

	preAggSize int

	timeCompressAlgo  map[string]*CompressAlgo
	intCompressAlgo   map[string]*CompressAlgo
	floatCompressAlgo map[string]*CompressAlgo

	allNilSegment int
	noNilSegment  int
}

func NewAnalyzeResult() *AnalyzeResult {
	return &AnalyzeResult{
		originSize:        make(map[int]int),
		compressSize:      make(map[int]int),
		timeCompressAlgo:  make(map[string]*CompressAlgo),
		intCompressAlgo:   make(map[string]*CompressAlgo),
		floatCompressAlgo: make(map[string]*CompressAlgo),
	}
}
