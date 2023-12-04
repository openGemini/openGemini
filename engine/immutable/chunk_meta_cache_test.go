/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package immutable

import (
	"testing"

	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestChunkMetaCache(t *testing.T) {
	// Test the PutChunkMeta
	meta11 := &ChunkMeta{sid: 0, offset: 0, size: 100, columnCount: 1, segCount: 1,
		timeRange: []SegmentRange{[2]int64{0, 10}, [2]int64{10, 20}},
		colMeta:   []ColumnMeta{{name: "cpu", ty: influx.Field_Type_Float, preAgg: []byte{1, 2}, entries: []Segment{{offset: 0, size: 100}}}},
	}
	meta12 := &ChunkMeta{sid: 0, offset: 0, size: 100, columnCount: 1, segCount: 1,
		timeRange: []SegmentRange{[2]int64{0, 10}, [2]int64{10, 20}},
		colMeta:   []ColumnMeta{{name: "cpu", ty: influx.Field_Type_Float, preAgg: []byte{1, 2}, entries: []Segment{{offset: 0, size: 100}}}},
	}
	filePath11 := "/tmp/openGemini/db0/ptId/shardId/columnstore/dataFile1.tssp"
	filePath12 := "/tmp/openGemini/db0/ptId/shardId/columnstore/dataFile2.tssp"
	PutChunkMeta(filePath11, meta11)
	PutChunkMeta(filePath12, meta12)

	// Test the GetChunkMeta
	meta21, _ := GetChunkMeta(filePath11)
	assert.Equal(t, meta11, meta21)

	meta22, _ := GetChunkMeta(filePath12)
	assert.Equal(t, meta12, meta22)

	filePath3 := "/tmp/openGemini/db0/ptId/shardId/columnstore/dataFile3.tssp"
	_, ok := GetChunkMeta(filePath3)
	assert.Equal(t, ok, false)
}
