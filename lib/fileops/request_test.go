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

package fileops

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewObsReadRequestMerge(t *testing.T) {
	defaultObsRangeSize = 2
	offs := make([]int64, 0)
	offs = append(offs, 0, 2, 5, 8)
	sizes := make([]int64, 0)
	sizes = append(sizes, 2, 2, 2, 2)
	request, _ := NewObsReadRequest(offs, sizes, -1, -1)

	assert.Equal(t, 2, len(request))
	assert.Equal(t, "0-3,5-6", request[0].GetRangeString())
	assert.Equal(t, "8-9", request[1].GetRangeString())

	assert.Equal(t, 2, len(request[0].readMap))
	assert.Equal(t, 0, int(request[0].readMap[0][0].Offset))
	assert.Equal(t, 2, len(request[0].readMap[0][0].Content))
	assert.Equal(t, 2, int(request[0].readMap[0][1].Offset))
	assert.Equal(t, 2, len(request[0].readMap[0][1].Content))
	assert.Equal(t, 5, int(request[0].readMap[5][0].Offset))
	assert.Equal(t, 2, len(request[0].readMap[5][0].Content))

	assert.Equal(t, 1, len(request[1].readMap))
	assert.Equal(t, 8, int(request[1].readMap[8][0].Offset))
	assert.Equal(t, 2, len(request[1].readMap[8][0].Content))
}

func TestNewObsReadRequestSplit(t *testing.T) {
	ObsSingleRequestSize = 1000
	offs := make([]int64, 0)
	offs = append(offs, 0, 2000)
	sizes := make([]int64, 0)
	sizes = append(sizes, 2000, 2000)
	request, _ := NewObsReadRequest(offs, sizes, 1000, defaultObsRangeSize)

	assert.Equal(t, 4, len(request))
	assert.Equal(t, "0-999", request[0].GetRangeString())
	assert.Equal(t, "1000-1999", request[1].GetRangeString())
	assert.Equal(t, "2000-2999", request[2].GetRangeString())
	assert.Equal(t, "3000-3999", request[3].GetRangeString())

	assert.Equal(t, 1, len(request[0].readMap))
	assert.Equal(t, 0, int(request[0].readMap[0][0].Offset))
	assert.Equal(t, 1000, len(request[0].readMap[0][0].Content))

	assert.Equal(t, 1, len(request[1].readMap))
	assert.Equal(t, 1000, int(request[1].readMap[1000][0].Offset))
	assert.Equal(t, 1000, len(request[1].readMap[1000][0].Content))

	assert.Equal(t, 1, len(request[2].readMap))
	assert.Equal(t, 2000, int(request[2].readMap[2000][0].Offset))
	assert.Equal(t, 1000, len(request[2].readMap[2000][0].Content))

	assert.Equal(t, 1, len(request[3].readMap))
	assert.Equal(t, 3000, int(request[3].readMap[3000][0].Offset))
	assert.Equal(t, 1000, len(request[3].readMap[3000][0].Content))
}

func TestNewObsReadRequestSplitWithBlockSize(t *testing.T) {
	ObsSingleRequestSize = 9 * 1024
	offs := make([]int64, 0)
	offs = append(offs, 0)
	sizes := make([]int64, 0)
	sizes = append(sizes, 10*1024)
	request, _ := NewObsReadRequest(offs, sizes, 10, defaultObsRangeSize)

	assert.Equal(t, 2, len(request))
	assert.Equal(t, "0-9209", request[0].GetRangeString())
	assert.Equal(t, "9210-10239", request[1].GetRangeString())

	assert.Equal(t, 1, len(request[0].readMap))
	assert.Equal(t, 0, int(request[0].readMap[0][0].Offset))
	assert.Equal(t, 9210, len(request[0].readMap[0][0].Content))

	assert.Equal(t, 1, len(request[1].readMap))
	assert.Equal(t, 9210, int(request[1].readMap[9210][0].Offset))
	assert.Equal(t, 1030, len(request[1].readMap[9210][0].Content))
}
