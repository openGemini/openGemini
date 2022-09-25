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

package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func TestIntervalTransform(t *testing.T) {
	sourceChunk1, sourceChunk2 := buildSourceChunk1(), buildSourceChunk2()
	targetChunk1, targetChunk2 := buildTargetChunk1(), buildTargetChunk2()

	expectChunks := make([]executor.Chunk, 0, 2)
	expectChunks = append(expectChunks, targetChunk1)
	expectChunks = append(expectChunks, targetChunk2)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`count("value1")`), hybridqp.MustParseExpr(`min("value2")`)},
		Dimensions: []string{"name"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  10,
	}

	source := NewSourceFromMultiChunk(buildSourceRowDataType(), []executor.Chunk{sourceChunk1, sourceChunk2})
	trans1 := executor.NewIntervalTransform([]hybridqp.RowDataType{buildSourceRowDataType()}, []hybridqp.RowDataType{buildTargetRowDataType()}, opt)
	sink := NewNilSink(buildTargetRowDataType())

	executor.Connect(source.Output, trans1.Inputs[0])
	executor.Connect(trans1.Outputs[0], sink.Input)

	var processors executor.Processors

	processors = append(processors, source)
	processors = append(processors, trans1)
	processors = append(processors, sink)

	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()

	outputChunks := sink.Chunks
	if len(expectChunks) != len(outputChunks) {
		t.Fatalf("the chunk number is not the same as the expected: %d != %d\n", len(expectChunks), len(outputChunks))
	}
	for i := range outputChunks {
		assert.Equal(t, outputChunks[i].Name(), expectChunks[i].Name())
		assert.Equal(t, outputChunks[i].Tags(), expectChunks[i].Tags())
		assert.Equal(t, outputChunks[i].Time(), expectChunks[i].Time())
		assert.Equal(t, outputChunks[i].TagIndex(), expectChunks[i].TagIndex())
		assert.Equal(t, outputChunks[i].IntervalIndex(), expectChunks[i].IntervalIndex())
		for j := range outputChunks[i].Columns() {
			assert.Equal(t, outputChunks[i].Column(j), expectChunks[i].Column(j))
		}
	}
}
