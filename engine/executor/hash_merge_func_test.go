// Copyright 2026 openGemini Authors.
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

package executor

import (
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestHashMergeStringColumnReusedInput(t *testing.T) {
	rt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "name", Type: influxql.String})
	input := NewChunkBuilder(rt).NewChunk("mst")
	input.AppendTimes([]int64{1, 2, 3})
	input.Column(0).AppendStringValues([]string{"tom", "jerry"})
	input.Column(0).AppendNilsV2(true, false, true)

	result := &HashMergeResult{cols: []HashMergeColumn{NewHashMergeStringColumn()}}
	result.AppendResult(input, 0, input.Len())

	// Simulate the upstream reader reusing the memory of a chunk it has already sent.
	values, _ := input.Column(0).GetStringBytes()
	for i := range values {
		values[i] = '#'
	}

	output := NewChunkBuilder(rt).NewChunk("mst").Column(0)
	for range result.time {
		result.cols[0].SetOutPut(output)
	}
	assert.Equal(t, []string{"tom", "jerry"}, output.StringValuesV2(nil))
	assert.Equal(t, 1, output.NilCount())
	assert.True(t, output.IsNilV2(1))
}
