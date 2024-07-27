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
package executor_test

import (
	"context"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
)

func PromResultCompare(c1 executor.Chunk, c2 executor.Chunk, t *testing.T) {
	if c1 == nil && c2 == nil {
		return
	}
	assert.Equal(t, c1.TagLen(), c2.TagLen())
	for i, tag1s := range c1.Tags() {
		tag2s := c2.Tags()[i]
		assert.Equal(t, tag1s.GetTag(), tag2s.GetTag())
	}
	assert.Equal(t, c1.Time(), c2.Time())
	assert.Equal(t, len(c1.Columns()), 1)
	assert.Equal(t, len(c2.Columns()), 1)
	assert.Equal(t, c1.Columns()[0].FloatValues(), c2.Columns()[0].FloatValues())
}

func PromBinOpTransformTestBase(t *testing.T, chunks1, chunks2 []executor.Chunk, para *influxql.BinOp, rChunk executor.Chunk) {
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	source2 := NewSourceFromMultiChunk(chunks2[0].RowDataType(), chunks2)
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	outRowDataType := buildPromBinOpOutputRowDataType()
	schema := buildPromBinOpSchema()
	trans, err := executor.NewBinOpTransform(inRowDataTypes, outRowDataType, schema, para)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		PromResultCompare(chunk, rChunk, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildBinOpInChunk1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=1,tk5=5"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=5"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=5,tk5=5"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk2() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=2,tk4=4"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=4"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=4,tk4=4"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk3() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=1,tk5=5"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=5"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=5,tk5=5"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk4() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=1,tk3=2,tk4=4"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=4"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=3,tk3=4,tk4=4"), 2)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=4,tk3=3,tk4=4"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.AddIntervalIndex(3)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1, 1})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildBinOpInChunk5() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=6,tk3=1,tk5=5"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=6,tk3=3,tk5=5"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=6,tk3=5,tk5=5"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk6() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=6,tk3=1,tk4=7"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=6,tk3=3,tk4=7"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=6,tk3=5,tk4=7"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk7() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=6,tk3=1,tk4=7,tk5=5"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=6,tk3=3,tk4=7,tk5=5"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=6,tk3=5,tk4=7,tk5=5"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk8() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=1,tk3=2,tk4=4"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=4"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=3,tk3=4,tk4=4"), 2)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=4,tk3=3,tk4=4"), 3)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.AddIntervalIndex(3)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1, 1})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildBinOpInChunk9() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=2,tk4=4"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=4"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=4,tk4=4"), 2)
	chunk.AddIntervalIndex(0)
	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 3.3, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpResult() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{4.4}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=4"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=4,tk3=3,tk4=4"), 1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=4,tk3=3"), 1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=7"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=4,tk3=3,tk4=7"), 1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=7,tk5=5"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=4,tk3=3,tk4=7,tk5=5"), 1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult5() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=6,tk2=2,tk3=3,tk4=7,tk5=5"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=6,tk2=4,tk3=3,tk4=7,tk5=5"), 1)
	AppendFloatValues(chunk1, 0, []float64{4, 8}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult6() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=5"), 0)
	AppendFloatValues(chunk1, 0, []float64{2.2}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult7() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=1,tk5=5"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=5"), 1)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=5,tk5=5"), 2)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3}, []bool{true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult8() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=1,tk5=5"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=5,tk5=5"), 1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 3.3}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult9() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 1, 1, 1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=1,tk5=5"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=5"), 1)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=5,tk5=5"), 2)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=2,tk4=4"), 3)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=3,tk4=4"), 4)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=4,tk4=4"), 5)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3, 1.1, 2.2, 3.3}, []bool{true, true, true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult10() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 1, 1, 1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=1,tk5=5"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=5"), 1)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=1,tk3=5,tk5=5"), 2)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=2,tk4=4"), 3)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk2=2,tk3=4,tk4=4"), 4)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3, 1.1, 3.3}, []bool{true, true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult11() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{2.2}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult12() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{1}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult13() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{0}, []bool{true})
	return []executor.Chunk{chunk1}
}

func buildPromBinOpOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)
	return rowDataType
}

func buildPromBinOpSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NullFill,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

// vector + vector -> no matchGroup -> nil
func TestPromBinOpTransform1(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.ADD,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// vector + on(tk3) vector -> 1 row
func TestPromBinOpTransform2(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.ADD,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult()[0])
}

// vector + ignore(tk1,tk5,tk2,tk4) vector -> 1 row
func TestPromBinOpTransform3(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.ADD,
		On:        false,
		MatchKeys: []string{"tk1", "tk5", "tk2", "tk4"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult()[0])
}

// vector + on(tk3) group_left vector -> primary dup err
func TestPromBinOpTransform4(t *testing.T) {
	chunk1 := BuildBinOpInChunk3()
	chunk2 := BuildBinOpInChunk4()
	para := &influxql.BinOp{
		OpType:    parser.ADD,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.ManyToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// vector - on(tk3) group_right vector -> 2 row
func TestPromBinOpTransform5(t *testing.T) {
	chunk1 := BuildBinOpInChunk3()
	chunk2 := BuildBinOpInChunk4()
	para := &influxql.BinOp{
		OpType:    parser.SUB,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToMany,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult1()[0])
}

// cover become del
// vector - on(tk3) group_right(tk2) vector -> result dup err
func TestPromBinOpTransform6(t *testing.T) {
	chunk1 := BuildBinOpInChunk3()
	chunk2 := BuildBinOpInChunk4()
	para := &influxql.BinOp{
		OpType:      parser.SUB,
		On:          true,
		MatchKeys:   []string{"tk3"},
		MatchCard:   influxql.OneToMany,
		IncludeKeys: []string{"tk2"},
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// cover become del
// vector - on(tk3) group_right(tk4) vector -> 2 row
func TestPromBinOpTransform7(t *testing.T) {
	chunk1 := BuildBinOpInChunk3()
	chunk2 := BuildBinOpInChunk4()
	para := &influxql.BinOp{
		OpType:      parser.SUB,
		On:          true,
		MatchKeys:   []string{"tk3"},
		MatchCard:   influxql.OneToMany,
		IncludeKeys: []string{"tk4"},
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult2()[0])
}

// has cover
// vector - on(tk3) group_right(tk2) vector -> result dup err
func TestPromBinOpTransform8(t *testing.T) {
	chunk1 := BuildBinOpInChunk5()
	chunk2 := BuildBinOpInChunk4()
	para := &influxql.BinOp{
		OpType:      parser.SUB,
		On:          true,
		MatchKeys:   []string{"tk3"},
		MatchCard:   influxql.OneToMany,
		IncludeKeys: []string{"tk2"},
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// (tk4):has cover
// vector - on(tk3) group_right(tk4) vector -> 2 row
func TestPromBinOpTransform9(t *testing.T) {
	chunk1 := BuildBinOpInChunk6()
	chunk2 := BuildBinOpInChunk4()
	para := &influxql.BinOp{
		OpType:      parser.SUB,
		On:          true,
		MatchKeys:   []string{"tk3"},
		MatchCard:   influxql.OneToMany,
		IncludeKeys: []string{"tk4"},
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult3()[0])
}

// (tk1):left include key is not in right; (tk5):left include key has last; (tk4):has cover
// vector * on(tk3) group_right(tk1,tk4,tk5) vector -> 2 row
func TestPromBinOpTransform10(t *testing.T) {
	chunk1 := BuildBinOpInChunk7()
	chunk2 := BuildBinOpInChunk8()
	para := &influxql.BinOp{
		OpType:      parser.MUL,
		On:          true,
		MatchKeys:   []string{"tk3"},
		MatchCard:   influxql.OneToMany,
		IncludeKeys: []string{"tk1", "tk4", "tk5"},
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult5()[0])
}

// vector LAND vector -> nil
func TestPromBinOpTransform11(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LAND,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// vector LAND on(tk3) vector -> 1 row
func TestPromBinOpTransform12(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LAND,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult6()[0])
}

// vector LAND ignore(tk1,tk5,tk2,tk4) vector -> 1 row
func TestPromBinOpTransform13(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LAND,
		On:        false,
		MatchKeys: []string{"tk1", "tk5", "tk2", "tk4"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult6()[0])
}

// vector LUNLESS vector -> nil
func TestPromBinOpTransform14(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LUNLESS,
		On:        true,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult7()[0])
}

// vector LUNLESS on(tk3) vector -> 1 row
func TestPromBinOpTransform15(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LUNLESS,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult8()[0])
}

// vector LUNLESS ignore(tk1,tk5,tk2,tk4) vector -> 1 row
func TestPromBinOpTransform16(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LUNLESS,
		On:        false,
		MatchKeys: []string{"tk1", "tk5", "tk2", "tk4"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult8()[0])
}

// vector LOR vector -> nil
func TestPromBinOpTransform17(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult9()[0])
}

// vector LOR on(tk3) vector -> 1 row
func TestPromBinOpTransform18(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult10()[0])
}

// vector LOR ignore(tk1,tk5,tk2,tk4) vector -> 1 row
func TestPromBinOpTransform19(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk2()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        false,
		MatchKeys: []string{"tk1", "tk5", "tk2", "tk4"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult10()[0])
}

// vector < on(tk3) vector -> 1 row
func TestPromBinOpTransform20(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:    parser.LSS,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult11()[0])
}

// vector <= on(tk3) vector -> 1 row
func TestPromBinOpTransform21(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:    parser.LTE,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult11()[0])
}

// vector > on(tk3) vector -> nil
func TestPromBinOpTransform22(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:    parser.GTR,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// vector >= on(tk3) vector -> nil
func TestPromBinOpTransform23(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:    parser.GTE,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// vector == on(tk3) vector -> nil
func TestPromBinOpTransform24(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:    parser.EQLC,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
}

// vector != on(tk3) vector -> nil
func TestPromBinOpTransform25(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:    parser.NEQ,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult11()[0])
}

// vector < bool on(tk3) vector -> 1 row
func TestPromBinOpTransform26(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:     parser.LSS,
		On:         true,
		MatchKeys:  []string{"tk3"},
		MatchCard:  influxql.OneToOne,
		ReturnBool: true,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult12()[0])
}

// vector > bool on(tk3) vector -> 1 row
func TestPromBinOpTransform27(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	chunk2 := BuildBinOpInChunk9()
	para := &influxql.BinOp{
		OpType:     parser.GTR,
		On:         true,
		MatchKeys:  []string{"tk3"},
		MatchCard:  influxql.OneToOne,
		ReturnBool: true,
	}
	PromBinOpTransformTestBase(t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult13()[0])
}

func PromBinOpTransformSingleInputTestBase(t *testing.T, chunks1 []executor.Chunk, para *influxql.BinOp, rChunk executor.Chunk) {
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	outRowDataType := buildPromBinOpOutputRowDataType()
	schema := buildPromBinOpSchema()
	trans, err := executor.NewBinOpTransform(inRowDataTypes, outRowDataType, schema, para)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		PromResultCompare(chunk, rChunk, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

// LNilMst LOR
func TestPromBinOpTransform28(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	para := &influxql.BinOp{
		OpType:     parser.LOR,
		On:         true,
		MatchKeys:  []string{"tk3"},
		MatchCard:  influxql.OneToOne,
		ReturnBool: true,
		NilMst:     influxql.LNilMst,
	}
	PromBinOpTransformSingleInputTestBase(t, []executor.Chunk{chunk1}, para, BuildBinOpInChunk1())
}

// LNilMst LUNLESS
func TestPromBinOpTransform29(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	para := &influxql.BinOp{
		OpType:     parser.LUNLESS,
		On:         true,
		MatchKeys:  []string{"tk3"},
		MatchCard:  influxql.OneToOne,
		ReturnBool: true,
		NilMst:     influxql.LNilMst,
	}
	PromBinOpTransformSingleInputTestBase(t, []executor.Chunk{chunk1}, para, nil)
}

// RNilMst LUNLESS
func TestPromBinOpTransform30(t *testing.T) {
	chunk1 := BuildBinOpInChunk1()
	para := &influxql.BinOp{
		OpType:     parser.LUNLESS,
		On:         true,
		MatchKeys:  []string{"tk3"},
		MatchCard:  influxql.OneToOne,
		ReturnBool: true,
		NilMst:     influxql.RNilMst,
	}
	PromBinOpTransformSingleInputTestBase(t, []executor.Chunk{chunk1}, para, BuildBinOpInChunk1())
}
