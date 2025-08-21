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
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
)

func PromResultCompareMultiCol(c1 executor.Chunk, c2 executor.Chunk) error {
	if c1 == nil && c2 == nil {
		return nil
	}
	if c1.TagLen() != c2.TagLen() {
		return fmt.Errorf("taglen not equal %d %d", c1.TagLen(), c2.TagLen())
	}
	for i, tag1s := range c1.Tags() {
		tag2s := c2.Tags()[i]
		if !bytes.Equal(tag1s.GetTag(), tag2s.GetTag()) {
			return fmt.Errorf("tags not equal %s %s", string(tag1s.GetTag()), string(tag2s.GetTag()))
		}
	}
	if !slices.Equal(c1.Time(), c2.Time()) {
		return fmt.Errorf("times not equal")
	}
	if len(c1.Columns()) != len(c2.Columns()) {
		return fmt.Errorf("col len not equal %d %d", len(c1.Columns()), len(c2.Columns()))
	}
	for i := range c1.Columns() {
		if !slices.Equal(c1.Columns()[i].FloatValues(), c2.Columns()[i].FloatValues()) {
			return fmt.Errorf("vals not equal")
		}
	}
	return nil
}

func PromResultCompareMultiColV2(c1 executor.Chunk, c2 executor.Chunk) error {
	if c1 == nil && c2 == nil {
		return nil
	}
	if c1.TagLen() != c2.TagLen() {
		return fmt.Errorf("taglen not equal %d %d", c1.TagLen(), c2.TagLen())
	}
	if c1.Len() != c2.Len() {
		return fmt.Errorf("len not equal %d %d", c1.Len(), c2.Len())
	}
	if len(c1.Columns()) != len(c2.Columns()) {
		return fmt.Errorf("col len not equal %d %d", len(c1.Columns()), len(c2.Columns()))
	}
	for i, tag1s := range c1.Tags() {
		find := false
		for j, tag2s := range c2.Tags() {
			if bytes.Equal(tag1s.GetTag(), tag2s.GetTag()) {
				s1 := c1.TagIndex()[i]
				e1 := 0
				if i == c1.TagLen()-1 {
					e1 = c1.Len()
				} else {
					e1 = c1.TagIndex()[i+1]
				}
				s2 := c2.TagIndex()[j]
				e2 := 0
				if j == c2.TagLen()-1 {
					e2 = c2.Len()
				} else {
					e2 = c2.TagIndex()[j+1]
				}
				vals1 := c1.Column(0).FloatValues()[s1:e1]
				vals2 := c2.Column(0).FloatValues()[s2:e2]
				if slices.Equal(vals1, vals2) {
					find = true
					break
				}
			}
		}
		if !find {
			return fmt.Errorf("not find tag1s in c2 %s", string(tag1s.GetTag()))
		}
	}
	return nil
}

func PromBinOpTransformTestBase(schema *executor.QuerySchema, compareFn func(c1 executor.Chunk, c2 executor.Chunk) error, t *testing.T, chunks1, chunks2 []executor.Chunk, para *influxql.BinOp, rChunk []executor.Chunk) {
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	source2 := NewSourceFromMultiChunk(chunks2[0].RowDataType(), chunks2)
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	outRowDataType := buildPromBinOpOutputRowDataType()
	trans, err := executor.NewBinOpTransform(inRowDataTypes, outRowDataType, schema, para, nil, nil)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		err := compareFn(chunk, rChunk[0])
		if err != nil && len(rChunk) > 1 {
			err = compareFn(chunk, rChunk[1])
		}
		assert.Equal(t, err, nil)
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
	addChunkTags([]string{"tk1=1,tk3=1,tk5=5", "tk1=1,tk3=3,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1, 2}, chunk)
	addIntervalIndexes([]int{0, 1, 2}, chunk)
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
	addChunkTags([]string{"tk2=2,tk3=2,tk4=4", "tk2=2,tk3=3,tk4=4", "tk2=2,tk3=4,tk4=4"}, []int{0, 1, 2}, chunk)
	addIntervalIndexes([]int{0, 1, 2}, chunk)
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
	addChunkTags([]string{"tk1=1,tk3=1,tk5=5", "tk1=1,tk3=3,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1, 2}, chunk)
	addIntervalIndexes([]int{0, 1, 2}, chunk)
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
	addChunkTags([]string{"tk2=1,tk3=2,tk4=4", "tk2=2,tk3=3,tk4=4", "tk2=3,tk3=4,tk4=4", "tk2=4,tk3=3,tk4=4"}, []int{0, 1, 2, 3}, chunk)
	addIntervalIndexes([]int{0, 1, 2, 3}, chunk)
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
	addChunkTags([]string{"tk2=6,tk3=1,tk5=5", "tk2=6,tk3=3,tk5=5", "tk2=6,tk3=5,tk5=5"}, []int{0, 1, 2}, chunk)
	addIntervalIndexes([]int{0, 1, 2}, chunk)
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
	addChunkTags([]string{"tk1=6,tk3=1,tk4=7", "tk1=6,tk3=3,tk4=7", "tk1=6,tk3=5,tk4=7"}, []int{0, 1, 2}, chunk)
	addIntervalIndexes([]int{0, 1, 2}, chunk)
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
	addChunkTags([]string{"tk1=6,tk3=1,tk4=7,tk5=5", "tk1=6,tk3=3,tk4=7,tk5=5", "tk1=6,tk3=5,tk4=7,tk5=5"}, []int{0, 1, 2}, chunk)
	addIntervalIndexes([]int{0, 1, 2}, chunk)
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
	addChunkTags([]string{"tk2=1,tk3=2,tk4=4", "tk2=2,tk3=3,tk4=4", "tk2=3,tk3=4,tk4=4", "tk2=4,tk3=3,tk4=4"}, []int{0, 1, 2, 3}, chunk)
	addIntervalIndexes([]int{0, 1, 2, 3}, chunk)
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
	addChunkTags([]string{"tk2=2,tk3=2,tk4=4", "tk2=2,tk3=3,tk4=4", "tk2=2,tk3=4,tk4=4"}, []int{0, 1, 2}, chunk)
	addIntervalIndexes([]int{0, 1, 2}, chunk)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 3.3, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 1})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk10() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 2})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2, 2})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildBinOpInChunk11() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 2, 1, 2})
	addChunkTags([]string{"tk1=2,tk2=1", "tk1=2,tk2=1", "tk1=2,tk2=2", "tk1=2,tk2=2"}, []int{0, 1, 2, 3}, chunk)
	addIntervalIndexes([]int{0, 1, 2, 3}, chunk)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2, 2, 3.3, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 1, 2})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildBinOpInChunk12() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 3.3, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildBinOpInChunk13() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 3})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 3})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildBinOpInChunk14() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func BuildBinOpInChunk15() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 1, 2, 2})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	return chunk
}

func addChunkTags(tags []string, locs []int, chunk executor.Chunk) {
	for i := range tags {
		chunk.AppendTagsAndIndex(*ParseChunkTags(tags[i]), locs[i])
	}
}

func addIntervalIndexes(indexs []int, chunk executor.Chunk) {
	for _, index := range indexs {
		chunk.AppendIntervalIndex(index)
	}
}

func BuildBinOpInChunk18() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1, 2, 6, 1, 3, 5, 1})
	addChunkTags([]string{"tk1=1,tk3=1,tk5=5", "tk1=1,tk3=3,tk5=4", "tk1=1,tk3=3,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1, 4, 7}, chunk)
	addIntervalIndexes([]int{0, 1, 4, 7}, chunk)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 1.1, 9.9, 6.6, 1.1, 3.3, 5.5, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1, 2, 6, 1, 3, 5, 1})
	chunk.Column(0).AppendManyNotNil(8)
	return chunk
}

func BuildBinOpInChunk19() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m2")
	chunk.AppendTimes([]int64{1, 2, 2, 4, 1})
	addChunkTags([]string{"tk2=2,tk3=2,tk4=4", "tk2=2,tk3=3,tk4=3", "tk2=2,tk3=3,tk4=4", "tk2=2,tk3=4,tk4=4"}, []int{0, 1, 2, 4}, chunk)
	addIntervalIndexes([]int{0, 1, 2, 4}, chunk)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2, 2.2, 4.4, 3.3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 2, 4, 1})
	chunk.Column(0).AppendManyNotNil(5)
	return chunk
}

func BuildBinOpInChunk20() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 3, 5, 7, 1})
	addChunkTags([]string{"tk1=1", "tk1=2"}, []int{0, 4}, chunk)
	addIntervalIndexes([]int{0, 4}, chunk)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 3.3, 5.5, 7.7, 1.1})
	chunk.Column(0).AppendColumnTimes([]int64{1, 3, 5, 7, 1})
	chunk.Column(0).AppendManyNotNil(5)
	return chunk
}

func BuildBinOpInChunk21() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{2, 3, 4})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{2.2, 9.9, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{2, 3, 4})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpInChunk22() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{2, 3, 4})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=1"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{2.2, 9.9, 4.4})
	chunk.Column(0).AppendColumnTimes([]int64{2, 3, 4})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func BuildBinOpResult() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{4.4}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult1() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	addChunkTags([]string{"tk2=2,tk3=3,tk4=4", "tk2=4,tk3=3,tk4=4"}, []int{0, 1}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	addChunkTags([]string{"tk2=2,tk3=3", "tk2=4,tk3=3"}, []int{0, 1}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	addChunkTags([]string{"tk2=2,tk3=3,tk4=7", "tk2=4,tk3=3,tk4=7"}, []int{0, 1}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	addChunkTags([]string{"tk2=2,tk3=3,tk4=7,tk5=5", "tk2=4,tk3=3,tk4=7,tk5=5"}, []int{0, 1}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{0, -2.2}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult5() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	addChunkTags([]string{"tk1=6,tk2=2,tk3=3,tk4=7,tk5=5", "tk1=6,tk2=4,tk3=3,tk4=7,tk5=5"}, []int{0, 1}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{4, 8}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult6() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=5"), 0)
	AppendFloatValues(chunk1, 0, []float64{2.2}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult7() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 1})
	addChunkTags([]string{"tk1=1,tk3=1,tk5=5", "tk1=1,tk3=3,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1, 2}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3}, []bool{true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult8() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1})
	addChunkTags([]string{"tk1=1,tk3=1,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 3.3}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult9() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 1, 1, 1, 1})
	addChunkTags([]string{"tk1=1,tk3=1,tk5=5", "tk1=1,tk3=3,tk5=5", "tk1=1,tk3=5,tk5=5", "tk2=2,tk3=2,tk4=4", "tk2=2,tk3=3,tk4=4", "tk2=2,tk3=4,tk4=4"}, []int{0, 1, 2, 3, 4, 5}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3, 1.1, 2.2, 3.3}, []bool{true, true, true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult10() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 1, 1, 1})
	addChunkTags([]string{"tk2=2,tk3=2,tk4=4", "tk1=1,tk3=3,tk5=5", "tk2=2,tk3=4,tk4=4", "tk1=1,tk3=1,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1, 2, 3, 4}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3, 1.1, 3.3}, []bool{true, true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult10_v4() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 2, 3, 4, 1, 3, 5, 7})
	addChunkTags([]string{"tk1=2", "tk1=1"}, []int{0, 4}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 9.9, 4.4, 1.1, 3.3, 5.5, 7.7}, []bool{true, true, true, true, true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult10_v5() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 2, 3, 4, 5, 7, 1})
	addChunkTags([]string{"tk1=1", "tk1=2"}, []int{0, 6}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3, 4.4, 5.5, 7.7, 1.1}, []bool{true, true, true, true, true, true, true})
	chunk2 := b.NewChunk("")
	chunk2.AppendTimes([]int64{1, 2, 3, 4, 1, 5, 7})
	addChunkTags([]string{"tk1=1", "tk1=2", "tk1=1"}, []int{0, 4, 5}, chunk2)
	AppendFloatValues(chunk2, 0, []float64{1.1, 2.2, 3.3, 4.4, 1.1, 5.5, 7.7}, []bool{true, true, true, true, true, true, true})
	return []executor.Chunk{chunk1, chunk2}
}

func BuildBinOpResult10_v2() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 1, 1, 1})
	addChunkTags([]string{"tk2=2,tk3=2,tk4=4", "tk1=1,tk3=3,tk5=5", "tk2=2,tk3=4,tk4=4", "tk1=1,tk3=5,tk5=5", "tk1=1,tk3=1,tk5=5"}, []int{0, 1, 2, 3, 4}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 2.2, 3.3, 3.3, 1.1}, []bool{true, true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult10_v3() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 1, 2, 4, 1, 1, 6, 1, 3, 5, 1})
	addChunkTags([]string{"tk2=2,tk3=2,tk4=4", "tk1=1,tk3=3,tk5=4", "tk2=2,tk3=3,tk4=4", "tk2=2,tk3=4,tk4=4", "tk1=1,tk3=1,tk5=5", "tk1=1,tk3=3,tk5=4", "tk1=1,tk3=3,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1, 3, 4, 5, 6, 7, 10}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1.1, 1.1, 9.9, 4.4, 3.3, 1.1, 6.6, 1.1, 3.3, 5.5, 3.3}, []bool{true, true, true, true, true, true, true, true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult11() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{2.2}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult12() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{1}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult13() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk3=3"), 0)
	AppendFloatValues(chunk1, 0, []float64{0}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult14() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 2, 1, 2})
	addChunkTags([]string{"tk1=2,tk2=1", "tk1=2,tk2=2"}, []int{0, 2}, chunk1)
	AppendFloatValues(chunk1, 0, []float64{1, 1, 1, 1}, []bool{true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult15() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 3})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	AppendFloatValues(chunk1, 0, []float64{2.2, 6.6}, []bool{true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult16() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{1, 2, 3, 4})
	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk1=2"), 0)
	AppendFloatValues(chunk1, 0, []float64{0, 0, 1, 0}, []bool{true, true, true, true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpInChunk17() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1})
	addChunkTags([]string{"tk1=1,tk2=1", "tk1=2,tk2=2"}, []int{0, 1}, chunk)
	addIntervalIndexes([]int{0, 1}, chunk)
	chunk.Column(0).AppendFloatValues([]float64{1.1, 2.2})
	chunk.Column(0).AppendColumnTimes([]int64{1, 1})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildBinOpResult17() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1})
	chunk.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1})
	chunk.Column(0).AppendColumnTimes([]int64{1})
	chunk.Column(0).AppendManyNotNil(1)
	return chunk
}

func BuildBinOpResult18() []executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("")
	chunk1.AppendTimes([]int64{2})

	chunk1.AppendTagsAndIndex(*ParseChunkTags("tk1=1,tk3=3,tk5=4"), 1)
	AppendFloatValues(chunk1, 0, []float64{9.9}, []bool{true})
	return []executor.Chunk{chunk1}
}

func BuildBinOpResult19() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{3})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tk1=1"), 0)
	AppendFloatValues(chunk, 0, []float64{3.3}, []bool{true})
	return chunk
}

func BuildBinOpResult20() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 1, 6, 1, 3, 5, 1})
	addChunkTags([]string{"tk1=1,tk3=1,tk5=5", "tk1=1,tk3=3,tk5=4", "tk1=1,tk3=3,tk5=5", "tk1=1,tk3=5,tk5=5"}, []int{0, 1, 3, 6}, chunk)
	AppendFloatValues(chunk, 0, []float64{1.1, 1.1, 6.6, 1.1, 3.3, 5.5, 3.3}, []bool{true, true, true, true, true, true, true})
	return chunk
}

func BuildBinOpResult21() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 5, 7, 1})
	addChunkTags([]string{"tk1=1", "tk1=2"}, []int{0, 3}, chunk)
	AppendFloatValues(chunk, 0, []float64{1.1, 5.5, 7.7, 1.1}, []bool{true, true, true, true})
	return chunk
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

func buildRangePromBinOpSchema() *executor.QuerySchema {
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
		Step:        1,
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult1())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult2())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult3())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult5())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult6())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult6())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult7())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult8())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult8())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiColV2, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult9())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiColV2, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult10())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiColV2, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult10_v2())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult11())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult11())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, nil)
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult11())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult12())
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
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult13())
}

func PromBinOpTransformSingleInputTestBase(t *testing.T, chunks1 []executor.Chunk, para *influxql.BinOp, rChunk executor.Chunk) {
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	outRowDataType := buildPromBinOpOutputRowDataType()
	schema := buildPromBinOpSchema()
	trans, err := executor.NewBinOpTransform(inRowDataTypes, outRowDataType, schema, para, nil, nil)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		PromResultCompareMultiCol(chunk, rChunk)
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

// test same PrimaryGroup reuse
func TestPromBinOpTransform31(t *testing.T) {
	chunk1 := BuildBinOpInChunk10()
	chunk2 := BuildBinOpInChunk11()
	para := &influxql.BinOp{
		OpType:     parser.ADD,
		On:         true,
		MatchKeys:  []string{"tk1"},
		MatchCard:  influxql.OneToMany,
		ReturnBool: true,
	}
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult14())
}

// test skip timestamp only one-side has, like "Short-circuit: nothing is going to match" in prom
func TestPromBinOpTransform32(t *testing.T) {
	chunk1 := BuildBinOpInChunk12()
	chunk2 := BuildBinOpInChunk13()
	para := &influxql.BinOp{
		OpType:    parser.ADD,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult15())

	chunk1 = BuildBinOpInChunk13()
	chunk2 = BuildBinOpInChunk12()
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult15())
}

// test mod op
func TestPromBinOpTransform33(t *testing.T) {
	chunk1 := BuildBinOpInChunk14()
	chunk2 := BuildBinOpInChunk15()
	para := &influxql.BinOp{
		OpType:    parser.MOD,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult16())
}

func PromBinOpTransformSingleInputTestBase1(t *testing.T, chunks1 []executor.Chunk, para *influxql.BinOp, rChunk executor.Chunk, lExpr, rExpr influxql.Expr) {
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	outRowDataType := buildPromBinOpOutputRowDataType()
	schema := buildPromBinOpSchema()
	trans, err := executor.NewBinOpTransform(inRowDataTypes, outRowDataType, schema, para, lExpr, rExpr)
	if err != nil {
		panic("")
	}
	checkResult := func(chunk executor.Chunk) error {
		err := PromResultCompareMultiCol(chunk, rChunk)
		assert.Equal(t, err, nil)
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

// mst AND vector(1)
func TestPromBinOpTransform34(t *testing.T) {
	chunk1 := BuildBinOpInChunk17()
	para := &influxql.BinOp{
		OpType:    parser.LAND,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
		RExpr:     &influxql.Call{Name: "vector_prom", Args: []influxql.Expr{&influxql.NumberLiteral{Val: 1}}},
	}
	PromBinOpTransformSingleInputTestBase1(t, []executor.Chunk{chunk1}, para, BuildBinOpInChunk17(), nil, para.RExpr)
}

// vector(1) AND mst
func TestPromBinOpTransform35(t *testing.T) {
	chunk1 := BuildBinOpInChunk17()
	para := &influxql.BinOp{
		OpType:    parser.LAND,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
		LExpr:     &influxql.Call{Name: "vector_prom", Args: []influxql.Expr{&influxql.NumberLiteral{Val: 1}}},
	}
	PromBinOpTransformSingleInputTestBase1(t, []executor.Chunk{chunk1}, para, BuildBinOpResult17(), para.LExpr, nil)
}

// mst AND vector(1)
func TestPromBinOpTransform36(t *testing.T) {
	chunk1 := BuildBinOpInChunk17()
	para := &influxql.BinOp{
		OpType:    parser.LUNLESS,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
		RExpr:     &influxql.Call{Name: "vector_prom", Args: []influxql.Expr{&influxql.NumberLiteral{Val: 1}}},
	}
	PromBinOpTransformSingleInputTestBase1(t, []executor.Chunk{chunk1}, para, nil, nil, para.RExpr)
}

func TestPromBinOpTransform37(t *testing.T) {
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
		RExpr:     &influxql.Call{Name: "vector_prom", Args: []influxql.Expr{&influxql.NumberLiteral{Val: 1}}},
	}
	rowDataType := buildPromBinOpOutputRowDataType()
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, rowDataType)
	schema := buildPromBinOpSchema()
	_, err := executor.NewBinOpTransform(inRowDataTypes, rowDataType, schema, para, nil, para.RExpr)
	assert.Equal(t, err.Error(), "unsupported")
}

// fix lor bug
func TestPromBinOpTransform38(t *testing.T) {
	chunk1 := BuildBinOpInChunk18()
	chunk2 := BuildBinOpInChunk19()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildRangePromBinOpSchema(), PromResultCompareMultiColV2, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult10_v3())
}

func TestPromBinOpTransform39(t *testing.T) {
	chunk1 := BuildBinOpInChunk20()
	chunk2 := BuildBinOpInChunk21()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildRangePromBinOpSchema(), PromResultCompareMultiColV2, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult10_v4())
}

func TestPromBinOpTransform40(t *testing.T) {
	chunk1 := BuildBinOpInChunk20()
	chunk2 := BuildBinOpInChunk22()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildRangePromBinOpSchema(), PromResultCompareMultiColV2, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult10_v5())
}

// fix land bug
func TestPromBinOpTransform41(t *testing.T) {
	chunk1 := BuildBinOpInChunk18()
	chunk2 := BuildBinOpInChunk19()
	para := &influxql.BinOp{
		OpType:    parser.LAND,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, BuildBinOpResult18())
}

func TestPromBinOpTransform42(t *testing.T) {
	chunk1 := BuildBinOpInChunk20()
	chunk2 := BuildBinOpInChunk22()
	para := &influxql.BinOp{
		OpType:    parser.LAND,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, []executor.Chunk{BuildBinOpResult19()})
}

// fix lunless bug
func TestPromBinOpTransform43(t *testing.T) {
	chunk1 := BuildBinOpInChunk18()
	chunk2 := BuildBinOpInChunk19()
	para := &influxql.BinOp{
		OpType:    parser.LUNLESS,
		On:        true,
		MatchKeys: []string{"tk3"},
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, []executor.Chunk{BuildBinOpResult20()})
}

func TestPromBinOpTransform44(t *testing.T) {
	chunk1 := BuildBinOpInChunk20()
	chunk2 := BuildBinOpInChunk22()
	para := &influxql.BinOp{
		OpType:    parser.LUNLESS,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildPromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, []executor.Chunk{BuildBinOpResult21()})
}

func BuildBinOpInChunkLorFix1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	var times []int64 = []int64{3, 4, 5, 12, 13, 14, 15, 22, 23, 24, 25, 32, 33, 34, 35, 42, 43, 44, 45, 52, 53, 54, 55}
	for i := 1; i <= 10; i++ {
		chunk.AppendTimes(times)
	}

	var tags []string
	var locs []int
	for i := 1; i <= 10; i++ {
		tags = append(tags, "tk1="+strconv.Itoa(i))
		locs = append(locs, 23*(i-1))
	}
	addChunkTags(tags, locs, chunk)
	var vals []float64 = []float64{101, 102, 103, 100, 101, 102, 103, 100, 101, 102, 103, 100, 101, 102, 103, 100, 101, 102, 103, 100, 101, 102, 103}
	for i := 1; i <= 10; i++ {
		chunk.Column(0).AppendFloatValues(vals)
		chunk.Column(0).AppendManyNotNil(23)
	}
	return chunk
}

func BuildBinOpInChunkLorFix2() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	var times []int64 = []int64{1, 2, 10, 11, 20, 21, 30, 31, 40, 41, 50, 51, 60, 61}
	for i := 1; i <= 10; i++ {
		chunk.AppendTimes(times)
	}

	var tags []string
	var locs []int
	for i := 1; i <= 10; i++ {
		tags = append(tags, "tk1="+strconv.Itoa(i))
		locs = append(locs, 14*(i-1))
	}
	addChunkTags(tags, locs, chunk)
	var vals []float64 = []float64{108, 108, 108, 109, 108, 109, 108, 109, 108, 109, 108, 109, 108, 108}
	for i := 1; i <= 10; i++ {
		chunk.Column(0).AppendFloatValues(vals)
		chunk.Column(0).AppendManyNotNil(14)
	}
	return chunk
}

func BuildBinOpResultLorFix1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	var times []int64 = []int64{1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 20, 21, 22, 23, 24, 25, 30, 31, 32, 33, 34, 35, 40, 41, 42, 43, 44, 45, 50, 51, 52, 53, 54, 55, 60, 61}
	for i := 1; i <= 10; i++ {
		chunk.AppendTimes(times)
	}

	var tags []string
	var locs []int
	for i := 1; i <= 10; i++ {
		tags = append(tags, "tk1="+strconv.Itoa(i))
		locs = append(locs, 37*(i-1))
	}
	addChunkTags(tags, locs, chunk)
	var vals []float64 = []float64{108, 108, 101, 102, 103, 108, 109, 100, 101, 102, 103, 108, 109, 100, 101, 102, 103, 108, 109, 100, 101, 102, 103, 108, 109, 100, 101, 102, 103, 108, 109, 100, 101, 102, 103, 108, 108}
	for i := 1; i <= 10; i++ {
		chunk.Column(0).AppendFloatValues(vals)
		chunk.Column(0).AppendManyNotNil(37)
	}
	return chunk
}

func TestPromBinOpTransformLorFix(t *testing.T) {
	chunk1 := BuildBinOpInChunkLorFix1()
	chunk2 := BuildBinOpInChunkLorFix2()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildRangePromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, []executor.Chunk{BuildBinOpResultLorFix1()})
}

func BuildBinOpInChunkLorFix1_1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	var times []int64 = []int64{1, 2, 3, 4, 11, 12, 13, 14, 21, 22, 23, 24, 31}
	for i := 1; i <= 10; i++ {
		chunk.AppendTimes(times)
	}

	var tags []string
	var locs []int
	for i := 1; i <= 10; i++ {
		tags = append(tags, "tk1="+strconv.Itoa(i))
		locs = append(locs, 13*(i-1))
	}
	addChunkTags(tags, locs, chunk)
	var vals []float64 = []float64{100, 101, 102, 103, 100, 101, 102, 103, 100, 101, 102, 103, 100}
	for i := 1; i <= 10; i++ {
		chunk.Column(0).AppendFloatValues(vals)
		chunk.Column(0).AppendManyNotNil(13)
	}
	return chunk
}

func BuildBinOpInChunkLorFix2_1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	var times []int64 = []int64{9, 10, 19, 20, 29, 30}
	for i := 1; i <= 10; i++ {
		chunk.AppendTimes(times)
	}

	var tags []string
	var locs []int
	for i := 1; i <= 10; i++ {
		tags = append(tags, "tk1="+strconv.Itoa(i))
		locs = append(locs, 6*(i-1))
	}
	addChunkTags(tags, locs, chunk)
	var vals []float64 = []float64{108, 109, 108, 109, 108, 109}
	for i := 1; i <= 10; i++ {
		chunk.Column(0).AppendFloatValues(vals)
		chunk.Column(0).AppendManyNotNil(6)
	}
	return chunk
}

func BuildBinOpResultLorFix1_1() executor.Chunk {
	rowDataType := buildPromBinOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	var times []int64 = []int64{1, 2, 3, 4, 9, 10, 11, 12, 13, 14, 19, 20, 21, 22, 23, 24, 29, 30, 31}
	for i := 1; i <= 10; i++ {
		chunk.AppendTimes(times)
	}

	var tags []string
	var locs []int
	for i := 1; i <= 10; i++ {
		tags = append(tags, "tk1="+strconv.Itoa(i))
		locs = append(locs, 19*(i-1))
	}
	addChunkTags(tags, locs, chunk)
	var vals []float64 = []float64{100, 101, 102, 103, 108, 109, 100, 101, 102, 103, 108, 109, 100, 101, 102, 103, 108, 109, 100}
	for i := 1; i <= 10; i++ {
		chunk.Column(0).AppendFloatValues(vals)
		chunk.Column(0).AppendManyNotNil(19)
	}
	return chunk
}

func TestPromBinOpTransformLorFix_1(t *testing.T) {
	chunk1 := BuildBinOpInChunkLorFix1_1()
	chunk2 := BuildBinOpInChunkLorFix2_1()
	para := &influxql.BinOp{
		OpType:    parser.LOR,
		On:        false,
		MatchCard: influxql.OneToOne,
	}
	PromBinOpTransformTestBase(buildRangePromBinOpSchema(), PromResultCompareMultiCol, t, []executor.Chunk{chunk1}, []executor.Chunk{chunk2}, para, []executor.Chunk{BuildBinOpResultLorFix1_1()})
}
