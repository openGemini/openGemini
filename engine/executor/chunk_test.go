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
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const chunkToRowStringColSep = " "

func appendRowToLine(c executor.Chunk, startloc int) []byte {
	var line []byte
	var l int
	for _, col := range c.Columns() {
		if col.IsNilV2(startloc) {
			line = append(line, chunkToRowStringColSep...)
			continue
		}
		l = col.GetValueIndexV2(startloc)
		switch col.DataType() {
		case influxql.Integer:
			line = append(line, strconv.FormatInt(col.IntegerValue(l), 10)...)
		case influxql.Float:
			line = append(line, strconv.FormatFloat(col.FloatValue(l), 'f', -1, 64)...)
		case influxql.Boolean:
			line = append(line, strconv.FormatBool(col.BooleanValue(l))...)
		case influxql.String, influxql.Tag:
			line = append(line, col.StringValue(l)...)
		}
		line = append(line, chunkToRowStringColSep...)
	}
	line = append(line, strconv.FormatInt(c.Time()[startloc], 10)...)
	return line
}

func StringToRows(c executor.Chunk) string {
	var buffer bytes.Buffer
	var schema []byte
	for _, rt := range c.RowDataType().Fields() {
		schema = append(schema, rt.Name()+chunkToRowStringColSep...)
	}
	schema = append(schema, "time\n"...)
	buffer.Write(schema)
	tagsLoc := 0
	startloc := 0
	endloc := c.Len()
	tagsLen := 1
	startTags := ""
	if c.Tags() != nil && len(c.Tags()) != 0 {
		tagsLen = len(c.Tags())
		startTags = string(c.Tags()[0].Subset(nil))
		if len(c.Tags()) > 1 {
			endloc = c.TagIndex()[1]
		}
	}
	for {
		buffer.WriteString(fmt.Sprintf("%s\n", startTags))
		for {
			if startloc >= endloc {
				break
			}
			line := appendRowToLine(c, startloc)
			buffer.WriteString(fmt.Sprintf("%s\n", line))
			startloc++
		}
		tagsLoc++
		if tagsLoc >= tagsLen {
			break
		}
		startloc = c.TagIndex()[tagsLoc]
		if tagsLoc == tagsLen-1 {
			endloc = c.Len()
		} else {
			endloc = c.TagIndex()[tagsLoc+1]
		}
		startTags = string(c.Tags()[tagsLoc].Subset(nil))
	}
	return buffer.String()
}

func buildChunkForPartialBlankRows() executor.Chunk {
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa")}, []int{0})
	chunk.AppendIntervalIndex(0)
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 4})
	chunk.Column(0).AppendNilsV2(true, false, false, true, false)

	chunk.Column(1).AppendFloatValues([]float64{2.2, 5.5})
	chunk.Column(1).AppendNilsV2(false, true, false, false, true)

	return chunk
}

func buildChunkForAllBlankRows() executor.Chunk {
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa")}, []int{0})
	chunk.AppendIntervalIndex(0)
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendManyNil(5)
	chunk.Column(1).AppendManyNil(5)

	return chunk
}

func buildChunkForOneBlankRow() executor.Chunk {
	rowDataType := buildSourceRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("name=aaa")}, []int{0})
	chunk.AppendIntervalIndex(0)
	chunk.AppendTimes([]int64{1})

	chunk.Column(0).AppendManyNil(1)
	chunk.Column(1).AppendManyNil(1)

	return chunk
}

func TestUnionColumns(t *testing.T) {
	// partial blank rows
	dst1 := []uint16{2}
	ck1 := buildChunkForPartialBlankRows()
	out1 := executor.UnionColumns(ck1.Columns()...)
	assert.Equal(t, dst1, out1)

	// all blank rows
	dst2 := []uint16{0, 1, 2, 3, 4}
	ck2 := buildChunkForAllBlankRows()
	out2 := executor.UnionColumns(ck2.Columns()...)
	assert.Equal(t, dst2, out2)

	// only blank rows
	dst3 := []uint16{0}
	ck3 := buildChunkForOneBlankRow()
	out3 := executor.UnionColumns(ck3.Columns()...)
	assert.Equal(t, dst3, out3)
}

func TestResetTagsAndIndexes(t *testing.T) {
	tags, tagIdx := []executor.ChunkTags{*ParseChunkTags("name=aaa")}, []int{0}
	chunk := executor.NewChunkBuilder(buildSourceRowDataType()).NewChunk("")
	chunk.ResetTagsAndIndexes(tags, tagIdx)
	assert.Equal(t, chunk.Tags(), tags)
	assert.Equal(t, chunk.TagIndex(), tagIdx)
}

func TestResetIntervalIndex(t *testing.T) {
	intervalIdx := []int{0}
	chunk := executor.NewChunkBuilder(buildSourceRowDataType()).NewChunk("")
	chunk.ResetIntervalIndex(intervalIdx...)
	assert.Equal(t, chunk.IntervalIndex(), intervalIdx)
}

func Test_GetColValsFn(t *testing.T) {
	type testCase struct {
		name                   string
		typ                    influxql.DataType
		appendFn               func(c executor.Column)
		bmStart, bmStop, ckLen int
		expect                 func(vals interface{}) bool
	}
	var testCases = []testCase{
		{
			name: "float64 column full data",
			typ:  influxql.Float,
			appendFn: func(c executor.Column) {
				c.AppendFloatValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0})
				c.AppendManyNotNil(5)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}) && success
				return success
			},
		},
		{
			name: "float64 column missing data",
			typ:  influxql.Float,
			appendFn: func(c executor.Column) {
				c.AppendFloatValues([]float64{1.0, 3.0, 5.0})
				c.AppendNilsV2(true, false, true, false, true)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{1.0, nil, 3.0, nil, 5.0}) && success
				return success
			},
		},
		{
			name: "int64 column full data",
			typ:  influxql.Integer,
			appendFn: func(c executor.Column) {
				c.AppendIntegerValues([]int64{1, 2, 3, 4, 5})
				c.AppendManyNotNil(5)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}) && success
				return success
			},
		},
		{
			name: "int64 column missing data",
			typ:  influxql.Integer,
			appendFn: func(c executor.Column) {
				c.AppendIntegerValues([]int64{2, 4})
				c.AppendNilsV2(false, true, false, true, false)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{nil, int64(2), nil, int64(4), nil}) && success
				return success
			},
		},
		{
			name: "bool column full data",
			typ:  influxql.Boolean,
			appendFn: func(c executor.Column) {
				c.AppendBooleanValues([]bool{true, false, true, false, true})
				c.AppendManyNotNil(5)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{true, false, true, false, true}) && success
				return success
			},
		},
		{
			name: "bool column missing data",
			typ:  influxql.Boolean,
			appendFn: func(c executor.Column) {
				c.AppendBooleanValues([]bool{true, true, true})
				c.AppendNilsV2(true, false, true, false, true)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{true, nil, true, nil, true}) && success
				return success
			},
		},
		{
			name: "string column full data",
			typ:  influxql.String,
			appendFn: func(c executor.Column) {
				strings := []string{"test1", "test2", "test3", "test4", "test5"}
				var sb []byte
				var off = []uint32{0}
				for _, s := range strings {
					sb = append(sb, util.Str2bytes(s)...)
					off = append(off, uint32(len(sb)))
				}

				c.AppendStringBytes(sb, off)
				c.AppendManyNotNil(5)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{"test1", "test2", "test3", "test4", "test5"}) && success
				return success
			},
		},
		{
			name: "string column missing data",
			typ:  influxql.String,
			appendFn: func(c executor.Column) {
				strings := []string{"test1", "test3", "test5"}
				var sb []byte
				var off = []uint32{0}
				for _, s := range strings {
					sb = append(sb, util.Str2bytes(s)...)
					off = append(off, uint32(len(sb)))
				}

				c.AppendStringBytes(sb, off)
				c.AppendNilsV2(true, false, true, false, true)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{"test1", nil, "test3", nil, "test5"}) && success
				return success
			},
		},
		{
			name: "tag column full data",
			typ:  influxql.Tag,
			appendFn: func(c executor.Column) {
				strings := []string{"test1", "test2", "test3", "test4", "test5"}
				var sb []byte
				var off = []uint32{0}
				for _, s := range strings {
					sb = append(sb, util.Str2bytes(s)...)
					off = append(off, uint32(len(sb)))
				}

				c.AppendStringBytes(sb, off)
				c.AppendManyNotNil(5)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{"test1", "test2", "test3", "test4", "test5"}) && success
				return success
			},
		},
		{
			name: "tag column missing data",
			typ:  influxql.Tag,
			appendFn: func(c executor.Column) {
				strings := []string{"test1", "test3", "test5"}
				var sb []byte
				var off = []uint32{0}
				for _, s := range strings {
					sb = append(sb, util.Str2bytes(s)...)
					off = append(off, uint32(len(sb)))
				}

				c.AppendStringBytes(sb, off)
				c.AppendNilsV2(true, false, true, false, true)
			},
			bmStart: 0,
			bmStop:  5,
			ckLen:   5,
			expect: func(vals interface{}) bool {
				success := true
				success = assert.Equal(t, vals, []interface{}{"test1", nil, "test3", nil, "test5"}) && success
				return success
			},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			column := executor.NewColumnImpl(tt.typ)
			tt.appendFn(column)
			vals := executor.GetColValsFn[tt.typ](column, tt.bmStart, tt.bmStop, tt.ckLen, nil)
			if !tt.expect(vals) {
				t.Fail()
			}
		})
	}

}

func Test_SlimChunk(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "id", Type: influxql.Integer},
		influxql.VarRef{Val: "value", Type: influxql.Float},
		influxql.VarRef{Val: "null", Type: influxql.String},
	)
	cb := executor.NewChunkBuilder(row)
	chunk := cb.NewChunk("mst")

	chunk.AppendTagsAndIndex(*executor.NewChunkTagsV2(nil), 0)
	chunk.AppendIntervalIndex(0)
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendIntegerValues([]int64{1, 3, 5})
	chunk.Column(0).AppendNilsV2(true, false, true, false, true)

	chunk.Column(1).AppendFloatValues([]float64{1.0, 3.0, 5.0})
	chunk.Column(1).AppendNilsV2(true, false, true, false, true)

	chunk.Column(2).AppendManyNil(5)

	chunk = chunk.SlimChunk([]int{2})
	assert.Equal(t, len(chunk.Columns()), 2)
	assert.Equal(t, chunk.Column(0).IntegerValues(), []int64{1, 3, 5})
	assert.Equal(t, chunk.Column(1).FloatValues(), []float64{1.0, 3.0, 5.0})

	chunk.CheckChunk()
}

func TestTargetTable(t *testing.T) {
	rowCap := 1
	tupleCap := 1
	table := executor.NewTargetTable(rowCap, tupleCap)
	table.Reset()

	checkAndAllocate := func(expect bool) {
		_, _, ok := table.CheckAndAllocate()
		assert.Equal(t, ok, expect)

		if ok {
			table.Commit()
		}
	}

	onlyAllocate := func() {
		row, tuple := table.Allocate()
		assert.NotEqual(t, row, nil)
		assert.NotEqual(t, tuple, nil)
		table.Commit()
	}

	checkAndAllocate(true)
	checkAndAllocate(false)
	onlyAllocate()
	onlyAllocate()
	checkAndAllocate(true)
	checkAndAllocate(false)
}

func BuildCopyByRowDataTypeSrcChunk(rt hybridqp.RowDataType) executor.Chunk {
	b := executor.NewChunkBuilder(rt)
	chunk := b.NewChunk("mst")
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(0).AppendManyNotNil(3)
	chunk.Column(1).AppendStringValues([]string{"f1", "f2", "f3"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(1).AppendManyNotNil(3)
	chunk.Column(2).AppendBooleanValues([]bool{true, true, true})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(2).AppendManyNotNil(3)
	chunk.Column(3).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(3).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(3).AppendManyNotNil(3)
	chunk.Column(4).AppendStringValues([]string{"f1", "f2", "f3"})
	chunk.Column(4).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(4).AppendManyNotNil(3)
	chunk.Column(5).AppendFloatTuple(*executor.NewfloatTuple([]float64{1, 1}))
	chunk.Column(5).AppendFloatTuple(*executor.NewfloatTuple([]float64{2, 2}))
	chunk.Column(5).AppendFloatTuple(*executor.NewfloatTuple([]float64{3, 3}))
	chunk.Column(5).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(5).AppendManyNotNil(3)
	return chunk
}

func buildCopyByRowDataTypeSrcRt() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Boolean},
		influxql.VarRef{Val: "val3", Type: influxql.Integer},
		influxql.VarRef{Val: "val4", Type: influxql.Tag},
		influxql.VarRef{Val: "val5", Type: influxql.FloatTuple},
	)
	return rowDataType
}

func BuildCopyByRowDataTypeDstChunk(rt hybridqp.RowDataType) executor.Chunk {
	b := executor.NewChunkBuilder(rt)
	chunk := b.NewChunk("mst")
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(1).AppendFloatValues([]float64{1, 2, 3})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(1).AppendManyNotNil(3)
	chunk.Column(0).AppendStringValues([]string{"f1", "f2", "f3"})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(0).AppendManyNotNil(3)
	chunk.Column(3).AppendBooleanValues([]bool{true, true, true})
	chunk.Column(3).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(3).AppendManyNotNil(3)
	chunk.Column(2).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(2).AppendManyNotNil(3)
	chunk.Column(5).AppendStringValues([]string{"f1", "f2", "f3"})
	chunk.Column(5).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(5).AppendManyNotNil(3)
	chunk.Column(4).AppendFloatTuple(*executor.NewfloatTuple([]float64{1, 1}))
	chunk.Column(4).AppendFloatTuple(*executor.NewfloatTuple([]float64{2, 2}))
	chunk.Column(4).AppendFloatTuple(*executor.NewfloatTuple([]float64{3, 3}))
	chunk.Column(4).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(4).AppendManyNotNil(3)
	return chunk
}

func buildCopyByRowDataTypeDstRt() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val0", Type: influxql.Float},
		influxql.VarRef{Val: "val3", Type: influxql.Integer},
		influxql.VarRef{Val: "val2", Type: influxql.Boolean},
		influxql.VarRef{Val: "val5", Type: influxql.FloatTuple},
		influxql.VarRef{Val: "val4", Type: influxql.Tag},
	)
	return rowDataType
}

func TestChunkCopyByRowDataType(t *testing.T) {
	srcRt := buildCopyByRowDataTypeSrcRt()
	srcChunk := BuildCopyByRowDataTypeSrcChunk(srcRt)
	dstRt := buildCopyByRowDataTypeDstRt()
	dstPreChunk := BuildCopyByRowDataTypeDstChunk(dstRt)
	b := executor.NewChunkBuilder(dstRt)
	dstChunk := b.NewChunk("")
	srcChunk.CopyByRowDataType(dstChunk, srcRt, dstRt)
	for i, col := range dstChunk.Columns() {
		if col.DataType() != dstPreChunk.Column(i).DataType() {
			t.Error("TestChunkCopyByRowDataType error")
		}
	}
}
