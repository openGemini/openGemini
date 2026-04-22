// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func PromResultCompareSingleColumn(c1 executor.Chunk, c2 executor.Chunk, t *testing.T) {
	if c1 == nil && c2 == nil {
		return
	}
	assert.Equal(t, c1.TagLen(), c2.TagLen())
	for i, tag1s := range c1.Tags() {
		tag2s := c2.Tags()[i]
		assert.Equal(t, tag1s.GetTag(), tag2s.GetTag())
	}
	assert.Equal(t, c1.Time(), c2.Time())
	assert.Equal(t, 1, len(c1.Columns()))
	assert.Equal(t, 1, len(c2.Columns()))
	for i, f1 := range c1.Column(0).FloatValues() {
		f2 := c1.Column(0).FloatValues()[i]
		if math.IsNaN(f1) && math.IsNaN(f2) {
			continue
		}
		assert.Equal(t, f1, f2)
	}
}

func buildPromSortRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Integer},
	)
	return rowDataType
}

func buildSortChunk(labels []string, values []float64) executor.Chunk {
	rowDataType := buildPromSortRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("chunk")
	for i, tags := range labels {
		chunk.AppendTime(0)
		chunk.AppendTagsAndIndex(*ParseChunkTags(tags), i)
	}
	chunk.Column(0).AppendFloatValues(values)
	chunk.Column(0).AppendManyNotNil(len(values))
	return chunk
}

func buildSortInChunkData1() ([]string, []float64) {
	labels := []string{"rank=0", "rank=1", "rank=2", "rank=3", "rank=4", "rank=5"}
	values := []float64{3, 1, 2, math.NaN(), math.Inf(-1), math.Inf(+1)}
	return labels, values
}

func buildSortInChunkData2() ([]string, []float64) {
	labels := []string{"group=A1,rank=0", "group=A1,rank=2", "group=A1,rank=1", "group=A2,rank=3", "group=A2,rank=4", "group=A2,rank=5"}
	values := []float64{1, 4, 2, 8, 16, 32}
	return labels, values
}

func buildSortInChunkData3() ([]string, []float64) {
	labels := []string{"label1=1", "label1=1,label2=2", "label1=1,label2=2,label3=3", "label1=1,label2=2,label3=3,label4=4"}
	values := []float64{1, 4, 2, 8}
	return labels, values
}

func buildSortOutChunkData1() ([]string, []float64) {
	labels := []string{"rank=4", "rank=1", "rank=2", "rank=0", "rank=5", "rank=3"}
	values := []float64{math.Inf(-1), 1, 2, 3, math.Inf(+1), math.NaN()}
	return labels, values
}

func buildSortOutChunkData2() ([]string, []float64) {
	labels := []string{"rank=5", "rank=0", "rank=2", "rank=1", "rank=4", "rank=3"}
	values := []float64{math.Inf(+1), 3, 2, 1, math.Inf(-1), math.NaN()}
	return labels, values
}

func buildSortOutChunkData3() ([]string, []float64) {
	labels := []string{"rank=0", "rank=1", "rank=2", "rank=3", "rank=4", "rank=5"}
	values := []float64{3, 1, 2, math.NaN(), math.Inf(-1), math.Inf(+1)}
	return labels, values
}

func buildSortOutChunkData4() ([]string, []float64) {
	labels := []string{"rank=5", "rank=4", "rank=3", "rank=2", "rank=1", "rank=0"}
	values := []float64{math.Inf(+1), math.Inf(-1), math.NaN(), 2, 1, 3}
	return labels, values
}

func buildSortOutChunkData5() ([]string, []float64) {
	labels := []string{"group=A1,rank=0", "group=A1,rank=1", "group=A1,rank=2", "group=A2,rank=3", "group=A2,rank=4", "group=A2,rank=5"}
	values := []float64{1, 2, 4, 8, 16, 32}
	return labels, values
}

func buildSortOutChunkData6() ([]string, []float64) {
	labels := []string{"group=A2,rank=5", "group=A2,rank=4", "group=A2,rank=3", "group=A1,rank=2", "group=A1,rank=1", "group=A1,rank=0"}
	values := []float64{32, 16, 8, 4, 2, 1}
	return labels, values
}

func buildSortOutChunkData7() ([]string, []float64) {
	labels := []string{"label1=1,label2=2", "label1=1,label2=2,label3=3", "label1=1,label2=2,label3=3,label4=4", "label1=1"}
	values := []float64{4, 2, 8, 1}
	return labels, values
}

func buildPromSortSchema(sortFields influxql.SortFields) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:     1024,
		ChunkedSize:   10000,
		RowsChan:      outPutRowsChan,
		Dimensions:    make([]string, 0),
		SortFields:    sortFields,
		Ascending:     true,
		LookBackDelta: 2 * time.Millisecond,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildPromSortFields1() influxql.SortFields {
	return influxql.SortFields{
		&influxql.SortField{Name: "value", Ascending: true},
	}
}

func buildPromSortFields2() influxql.SortFields {
	return influxql.SortFields{
		&influxql.SortField{Name: "value", Ascending: false},
	}
}

func buildPromSortFields3() influxql.SortFields {
	return influxql.SortFields{
		&influxql.SortField{Name: "rank", Ascending: true},
	}
}

func buildPromSortFields4() influxql.SortFields {
	return influxql.SortFields{
		&influxql.SortField{Name: "rank", Ascending: false},
	}
}

func buildPromSortFields5() influxql.SortFields {
	return influxql.SortFields{
		&influxql.SortField{Name: "group", Ascending: true},
	}
}

func buildPromSortFields6() influxql.SortFields {
	return influxql.SortFields{
		&influxql.SortField{Name: "group", Ascending: false},
		&influxql.SortField{Name: "rank", Ascending: false},
	}
}

func buildPromSortFields7() influxql.SortFields {
	return influxql.SortFields{
		&influxql.SortField{Name: "label1", Ascending: true},
		&influxql.SortField{Name: "label2", Ascending: true},
	}
}

func PromSortTransformTestBase(t *testing.T, schema *executor.QuerySchema, chunks []executor.Chunk, expectedChunk executor.Chunk) {
	source := NewSourceFromMultiChunk(chunks[0].RowDataType(), chunks)
	inRowDataType := buildPromSortRowDataType()
	outRowDataType := buildPromSortRowDataType()

	var trans, err = executor.NewPromSortTransform(source.Output.RowDataType, outRowDataType, schema, schema.GetSortFields())
	assert.Equal(t, err, nil)
	assert.Equal(t, trans.Name(), "PromSortTransform")
	assert.Equal(t, len(trans.Explain()), 0)
	assert.Equal(t, trans.GetInputNumber(executor.NewChunkPort(inRowDataType)), 0)
	assert.Equal(t, trans.GetOutputNumber(executor.NewChunkPort(outRowDataType)), 0)
	checkResult := func(chunk executor.Chunk) error {
		PromResultCompareSingleColumn(expectedChunk, chunk, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestPromSortTransform(t *testing.T) {
	t.Parallel()
	t.Run("sortWithNaN", func(t *testing.T) {
		chunk := buildSortChunk(buildSortInChunkData1())
		schema := buildPromSortSchema(buildPromSortFields1())
		PromSortTransformTestBase(t, schema, []executor.Chunk{chunk}, buildSortChunk(buildSortOutChunkData1()))
	})
	t.Run("sortDescWithNaN", func(t *testing.T) {
		chunk := buildSortChunk(buildSortInChunkData1())
		schema := buildPromSortSchema(buildPromSortFields2())
		PromSortTransformTestBase(t, schema, []executor.Chunk{chunk}, buildSortChunk(buildSortOutChunkData2()))
	})
	t.Run("sortByLabel", func(t *testing.T) {
		chunk := buildSortChunk(buildSortInChunkData1())
		schema := buildPromSortSchema(buildPromSortFields3())
		PromSortTransformTestBase(t, schema, []executor.Chunk{chunk}, buildSortChunk(buildSortOutChunkData3()))
	})
	t.Run("sortByLabelDesc", func(t *testing.T) {
		chunk := buildSortChunk(buildSortInChunkData1())
		schema := buildPromSortSchema(buildPromSortFields4())
		PromSortTransformTestBase(t, schema, []executor.Chunk{chunk}, buildSortChunk(buildSortOutChunkData4()))
	})

	t.Run("sortBySinle", func(t *testing.T) {
		chunk := buildSortChunk(buildSortInChunkData2())
		schema := buildPromSortSchema(buildPromSortFields5())
		PromSortTransformTestBase(t, schema, []executor.Chunk{chunk}, buildSortChunk(buildSortOutChunkData5()))
	})
	t.Run("sortByMulti", func(t *testing.T) {
		chunk := buildSortChunk(buildSortInChunkData2())
		schema := buildPromSortSchema(buildPromSortFields6())
		PromSortTransformTestBase(t, schema, []executor.Chunk{chunk}, buildSortChunk(buildSortOutChunkData6()))
	})

	t.Run("sortWithSamePrefix", func(t *testing.T) {
		chunk := buildSortChunk(buildSortInChunkData3())
		schema := buildPromSortSchema(buildPromSortFields7())
		PromSortTransformTestBase(t, schema, []executor.Chunk{chunk}, buildSortChunk(buildSortOutChunkData7()))
	})
}
