// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
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

func BuildCopyByRowDataTypeSrcChunk(rt hybridqp.RowDataType) executor.Chunk {
	b := executor.NewChunkBuilder(rt)
	chunk := b.NewChunk("mst")
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AppendIntervalIndex(0)
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
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AppendIntervalIndex(0)
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

func TestDecodeTagsWithoutTag(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		keys := []string{"a", "le", "z"}
		values := []string{"1", "+Inf", "2"}
		chunkTags := executor.NewChunkTagsByTagKVs(keys, values)
		expTags := executor.NewChunkTagsByTagKVs([]string{"a", "z"}, []string{"1", "2"}).GetTag()
		expLe := "+Inf"

		metricNameBytes, le := chunkTags.DecodeTagsWithoutTag("le")
		curTags := executor.NewChunkTagsByBytes(metricNameBytes).GetTag()
		if util.Bytes2str(expTags) != util.Bytes2str(curTags) {
			t.Fatal("not expect, exp metricNameBytes ", string(expTags), "current metricNameBytes", string(curTags))
		}
		if expLe != le {
			t.Fatal("not expect,exp le ", expLe, "current le", le)
		}
	})
	t.Run("2", func(t *testing.T) {
		keys := []string{"le", "z"}
		values := []string{"+Inf", "2"}
		chunkTags := executor.NewChunkTagsByTagKVs(keys, values)
		expTags := executor.NewChunkTagsByTagKVs([]string{"z"}, []string{"2"}).GetTag()
		expLe := "+Inf"

		metricNameBytes, le := chunkTags.DecodeTagsWithoutTag("le")
		curTags := executor.NewChunkTagsByBytes(metricNameBytes).GetTag()
		if util.Bytes2str(expTags) != util.Bytes2str(curTags) {
			t.Fatal("not expect, exp metricNameBytes ", string(expTags), "current metricNameBytes", string(metricNameBytes))
		}
		if expLe != le {
			t.Fatal("not expect,exp le ", expLe, "current le", le)
		}
	})
	t.Run("3", func(t *testing.T) {
		keys := []string{"le"}
		values := []string{"+Inf"}
		chunkTags := executor.NewChunkTagsByTagKVs(keys, values)
		expLe := "+Inf"

		metricNameBytes, le := chunkTags.DecodeTagsWithoutTag("le")
		if len(metricNameBytes) != 0 {
			t.Fatal("not expect, exp metricNameBytes ", nil)
		}
		if expLe != le {
			t.Fatal("not expect,exp le ", expLe, "current le", le)
		}
	})
}

func TestConvertToLabels(t *testing.T) {
	t.Run("TestConvertToLabels", func(t *testing.T) {
		// Test case 1: Normal case with multiple tags, no default metric key label
		pts := &influx.PointTags{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
		}
		dst := executor.ConvertToLabels(pts, nil)
		expected := "key1#$#value1|key2#$#value2"
		if string(dst) != expected {
			t.Errorf("Expected: %s, got: %s", expected, string(dst))
		}

		// Test case 2: Contains default metric key label
		pts = &influx.PointTags{
			{Key: promql2influxql.DefaultMetricKeyLabel, Value: "metric"},
			{Key: "key1", Value: "value1"},
		}
		dst = executor.ConvertToLabels(pts, nil)
		expected = "key1#$#value1"
		if string(dst) != expected {
			t.Errorf("Expected %s, got %s", expected, string(dst))
		}

		// Test case 3: Empty tags
		pts = &influx.PointTags{}
		dst = executor.ConvertToLabels(pts, nil)
		if len(dst) != 0 {
			t.Error("Expected empty byte slice, got non-empty")
		}

		// Test case 4: Single tag
		pts = &influx.PointTags{
			{Key: "key", Value: "value"},
		}
		dst = executor.ConvertToLabels(pts, nil)
		expected = "key#$#value"
		if string(dst) != expected {
			t.Errorf("Expected %s, got %s", expected, string(dst))
		}

		// Test case 5: Multiple tags, including default metric key label
		pts = &influx.PointTags{
			{Key: "key1", Value: "value1"},
			{Key: promql2influxql.DefaultMetricKeyLabel, Value: "metric"},
			{Key: "key2", Value: "value2"},
		}
		dst = executor.ConvertToLabels(pts, nil)
		expected = "key1#$#value1|key2#$#value2"
		if string(dst) != expected {
			t.Errorf("Expected %s, got %s", expected, string(dst))
		}
	})
}

func TestAuxTagKeyHandler(t *testing.T) {
	t.Run("TestAuxTagKeyHandler", func(t *testing.T) {
		// Test case 1: Normal case
		auxTagIdx := 0
		pts := &influx.PointTags{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
		}
		rec := &record.Record{
			ColVals: make([]record.ColVal, 1),
		}
		start := 0
		end := 2
		executor.AuxTagKeyHandler(auxTagIdx, pts, rec, start, end)
		expected := "key1#$#value1|key2#$#value2"
		got, _ := rec.ColVals[0].StringValueSafe(0)
		if got != expected {
			t.Errorf("Expected %s, got %s", expected, got)
		}

		// Test case 2: Empty label conversion result
		pts = &influx.PointTags{}
		rec = &record.Record{
			ColVals: make([]record.ColVal, 1),
		}
		start = 0
		end = 2
		executor.AuxTagKeyHandler(auxTagIdx, pts, rec, start, end)
		_, ok := rec.ColVals[0].StringValueSafe(0)
		if !ok {
			t.Error("Expected empty string, got non-empty")
		}

		// Test case 3: Different start and end values
		pts = &influx.PointTags{
			{Key: "key", Value: "value"},
		}
		rec = &record.Record{
			ColVals: make([]record.ColVal, 1),
		}
		start = 1
		end = 3
		executor.AuxTagKeyHandler(auxTagIdx, pts, rec, start, end)
		expected = "key#$#value"
		got, _ = rec.ColVals[0].StringValueSafe(0)
		if got != expected {
			t.Errorf("Expected %s at index 1, got %s", expected, got)
		}
	})
}

func TestNewChunkTags(t *testing.T) {
	testCases := []struct {
		name        string
		pts         influx.PointTags
		dimensions  []string
		expectedLen int
	}{
		{
			name:        "empty dimensions",
			pts:         influx.PointTags{},
			dimensions:  []string{},
			expectedLen: 0,
		},
		{
			name:        "single dimension",
			pts:         influx.PointTags{{Key: "key1", Value: "value1"}},
			dimensions:  []string{"key1"},
			expectedLen: 1,
		},
		{
			name:        "multiple dimensions",
			pts:         influx.PointTags{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}},
			dimensions:  []string{"key1", "key2"},
			expectedLen: 2,
		},
		{
			name:        "dimensions not present in pts",
			pts:         influx.PointTags{{Key: "__name__", Value: "value1"}, {Key: "key2", Value: "value2"}},
			dimensions:  []string{influxql.DefaultLabels},
			expectedLen: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunkTags := executor.NewChunkTags(tc.pts, tc.dimensions)
			if len(chunkTags.PointTags()) != tc.expectedLen {
				t.Errorf("expected %d tags, got %d", tc.expectedLen, len(chunkTags.PointTags()))
			}
		})
	}
}

func TestFilterRowsByIndexes(t *testing.T) {
	chunk := buildOriFilterChunk()
	fmt.Println("ori: ", chunk.String())

	indexes := []int{}
	outChunk := chunk.FilterRowsByIndexes(indexes...)
	if len(outChunk.Time()) != 0 {
		t.Fatal("the chunk show has no data")
	}

	indexes = []int{2}
	outChunk = chunk.FilterRowsByIndexes(indexes...)
	dstChunk1 := buildDstFilterChunk1()
	checkChunkEqual(dstChunk1, outChunk, t)

	indexes = []int{1, 3}
	outChunk = chunk.FilterRowsByIndexes(indexes...)
	dstChunk2 := buildDstFilterChunk2()
	checkChunkEqual(dstChunk2, outChunk, t)
}

func checkChunkEqual(dstChunk, outChunk executor.Chunk, t *testing.T) {
	fmt.Println("out: ", outChunk.String())
	assert.Equal(t, dstChunk.Name(), outChunk.Name())
	assert.Equal(t, dstChunk.Time(), outChunk.Time())
	for j, column := range outChunk.Columns() {
		assert.Equal(t, dstChunk.Column(j), column)
	}
	checkChunkTagAndIntervalEqual(dstChunk, outChunk, t)
}

func checkChunkTagAndIntervalEqual(dstChunk, outChunk executor.Chunk, t *testing.T) {
	fmt.Println("out: ", outChunk.String())
	assert.Equal(t, dstChunk.TagIndex(), outChunk.TagIndex())
	for k := range outChunk.Tags() {
		assert.Equal(t, dstChunk.Tags()[k].PointTags(), outChunk.Tags()[k].PointTags())
	}
	assert.Equal(t, dstChunk.IntervalIndex(), outChunk.IntervalIndex())
}

func buildOriFilterChunk() executor.Chunk {
	srcRt := buildCopyByRowDataTypeSrcRt()

	cb := executor.NewChunkBuilder(srcRt)
	chunk := cb.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tag1=a"), *ParseChunkTags("tag1=b"), *ParseChunkTags("tag1=c"),
	}, []int{0, 2, 4})
	chunk.AppendIntervalIndexes([]int{0, 1, 2, 4})
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})

	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4})
	chunk.Column(0).AppendNilsV2(true, true, true, true, false)

	chunk.Column(1).AppendStringValues([]string{"f1", "f2", "f4", "f5"})
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	chunk.Column(2).AppendBooleanValues([]bool{true, false, true, false})
	chunk.Column(2).AppendNilsV2(false, true, true, true, true)

	chunk.Column(3).AppendIntegerValues([]int64{4, 5})
	chunk.Column(3).AppendNilsV2(false, false, false, true, true)

	chunk.Column(4).AppendStringValues([]string{"t1", "t2", "t3"})
	chunk.Column(4).AppendNilsV2(true, true, true, false, false)

	chunk.Column(5).AppendFloatTuple(*executor.NewfloatTuple([]float64{1, 2}))
	chunk.Column(5).AppendFloatTuple(*executor.NewfloatTuple([]float64{3, 4}))
	chunk.Column(5).AppendFloatTuple(*executor.NewfloatTuple([]float64{1, 3}))
	chunk.Column(5).AppendNilsV2(true, true, false, false, true)

	return chunk
}

func buildDstFilterChunk1() executor.Chunk {
	srcRt := buildCopyByRowDataTypeSrcRt()

	cb := executor.NewChunkBuilder(srcRt)
	chunk := cb.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tag1=b"),
	}, []int{0})
	chunk.AppendIntervalIndexes([]int{0})
	chunk.AppendTimes([]int64{3})

	chunk.Column(0).AppendFloatValues([]float64{3})
	chunk.Column(0).AppendManyNotNil(1)

	chunk.Column(1).AppendNil()

	chunk.Column(2).AppendBooleanValues([]bool{false})
	chunk.Column(2).AppendManyNotNil(1)

	chunk.Column(3).AppendNil()

	chunk.Column(4).AppendStringValues([]string{"t3"})
	chunk.Column(4).AppendManyNotNil(1)
	chunk.Column(5).AppendNil()

	return chunk
}

func buildDstFilterChunk2() executor.Chunk {
	srcRt := buildCopyByRowDataTypeSrcRt()

	cb := executor.NewChunkBuilder(srcRt)
	chunk := cb.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tag1=a"), *ParseChunkTags("tag1=b"),
	}, []int{0, 1})
	chunk.AppendIntervalIndexes([]int{0, 1})
	chunk.AppendTimes([]int64{2, 4})

	chunk.Column(0).AppendFloatValues([]float64{2, 4})
	chunk.Column(0).AppendManyNotNil(2)

	chunk.Column(1).AppendStringValues([]string{"f2", "f4"})
	chunk.Column(1).AppendManyNotNil(2)

	chunk.Column(2).AppendBooleanValues([]bool{true, true})
	chunk.Column(2).AppendManyNotNil(2)

	chunk.Column(3).AppendIntegerValues([]int64{4})
	chunk.Column(3).AppendNilsV2(false, true)

	chunk.Column(4).AppendStringValues([]string{"t2"})
	chunk.Column(4).AppendNilsV2(true, false)

	chunk.Column(5).AppendFloatTuple(*executor.NewfloatTuple([]float64{3, 4}))
	chunk.Column(5).AppendNilsV2(true, false)

	return chunk
}

func TestFilterRowsByIndexesWithTagsAndInterval(t *testing.T) {
	srcRt := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
	)

	cb := executor.NewChunkBuilder(srcRt)
	chunk := cb.NewChunk("mst")

	chunk.AppendTimes([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	chunk.AppendTagsAndIndex(*executor.NewChunkTagsV2(nil), 0)
	chunk.AppendIntervalIndexes([]int{0})

	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	chunk.Column(0).AppendManyNotNil(10)

	dstChunk3 := buildDstFilterChunk3()

	indexes := []int{}
	outChunk := chunk.FilterRowsByIndexes(indexes...)
	checkChunkTagAndIntervalEqual(dstChunk3, outChunk, t)

	indexes = []int{1}
	outChunk = chunk.FilterRowsByIndexes(indexes...)
	checkChunkTagAndIntervalEqual(dstChunk3, outChunk, t)

	indexes = []int{1, 3, 5}
	outChunk = chunk.FilterRowsByIndexes(indexes...)
	checkChunkTagAndIntervalEqual(dstChunk3, outChunk, t)

	chunk.ResetTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tag1=a"), *ParseChunkTags("tag1=b"), *ParseChunkTags("tag1=c"),
	}, []int{0, 2, 4})
	chunk.ResetIntervalIndex([]int{0, 1, 2, 4}...)

	indexes = []int{}
	outChunk = chunk.FilterRowsByIndexes(indexes...)
	checkChunkTagAndIntervalEqual(dstChunk3, outChunk, t)

	dstChunk4 := buildDstFilterChunk4()
	indexes = []int{8}
	outChunk = chunk.FilterRowsByIndexes(indexes...)
	checkChunkTagAndIntervalEqual(dstChunk4, outChunk, t)

	dstChunk5 := buildDstFilterChunk5()
	indexes = []int{1, 3, 5}
	outChunk = chunk.FilterRowsByIndexes(indexes...)
	checkChunkTagAndIntervalEqual(dstChunk5, outChunk, t)
}

func buildDstFilterChunk3() executor.Chunk {
	srcRt := buildCopyByRowDataTypeSrcRt()

	cb := executor.NewChunkBuilder(srcRt)
	chunk := cb.NewChunk("mst")

	chunk.AppendTagsAndIndex(*executor.NewChunkTagsV2(nil), 0)
	chunk.AppendIntervalIndexes([]int{0})

	return chunk
}

func buildDstFilterChunk4() executor.Chunk {
	srcRt := buildCopyByRowDataTypeSrcRt()

	cb := executor.NewChunkBuilder(srcRt)
	chunk := cb.NewChunk("mst")
	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tag1=c"),
	}, []int{0})

	chunk.AppendIntervalIndexes([]int{0})

	return chunk
}

func buildDstFilterChunk5() executor.Chunk {
	srcRt := buildCopyByRowDataTypeSrcRt()

	cb := executor.NewChunkBuilder(srcRt)
	chunk := cb.NewChunk("mst")

	chunk.AppendTagsAndIndexes([]executor.ChunkTags{
		*ParseChunkTags("tag1=a"), *ParseChunkTags("tag1=b"), *ParseChunkTags("tag1=c"),
	}, []int{0, 1, 2})
	chunk.AppendIntervalIndexes([]int{0, 1, 2})

	return chunk
}

func TestGetChunkTagKVStrings(t *testing.T) {
	tests := []struct {
		name     string
		tags     influx.PointTags
		dims     []string
		expected string
	}{
		{
			name:     "empty tags",
			tags:     influx.PointTags{},
			dims:     []string{},
			expected: "",
		},
		{
			name: "single tag",
			tags: influx.PointTags{
				{Key: "host", Value: "server1"},
			},
			dims:     []string{"host"},
			expected: "host=server1",
		},
		{
			name: "multiple tags",
			tags: influx.PointTags{
				{Key: "host", Value: "server1"},
				{Key: "region", Value: "us-west"},
				{Key: "service", Value: "web"},
			},
			dims:     []string{"host", "region", "service"},
			expected: "host=server1,region=us-west,service=web",
		},
		{
			name: "default labels tag",
			tags: influx.PointTags{
				{Key: "__name__", Value: "cpu_usage"},
				{Key: "host", Value: "server1"},
			},
			dims:     []string{"__name__", "host"},
			expected: "__name__=cpu_usage,host=server1",
		},
		{
			name: "empty value tag",
			tags: influx.PointTags{
				{Key: "status", Value: ""},
				{Key: "host", Value: "server1"},
			},
			dims:     []string{"status", "host"},
			expected: "status=,host=server1",
		},
		{
			name: "numeric tag values",
			tags: influx.PointTags{
				{Key: "port", Value: "8080"},
				{Key: "timeout", Value: "30"},
			},
			dims:     []string{"port", "timeout"},
			expected: "port=8080,timeout=30",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkTags := executor.NewChunkTags(tt.tags, tt.dims)
			result := chunkTags.GetChunkTagKVStrings()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetChunkTagKVStringsWithNewChunkTagsByTagKVs(t *testing.T) {
	tests := []struct {
		name     string
		keys     []string
		values   []string
		expected string
	}{
		{
			name:     "empty keys and values",
			keys:     []string{},
			values:   []string{},
			expected: "",
		},
		{
			name:     "single key-value pair",
			keys:     []string{"host"},
			values:   []string{"server1"},
			expected: "host=server1",
		},
		{
			name:     "multiple key-value pairs",
			keys:     []string{"host", "region", "service"},
			values:   []string{"server1", "us-west", "web"},
			expected: "host=server1,region=us-west,service=web",
		},
		{
			name:     "keys and values of different lengths",
			keys:     []string{"host", "region"},
			values:   []string{"server1", "us-west", "service", "web"},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkTags := executor.NewChunkTagsByTagKVs(tt.keys, tt.values)
			result := chunkTags.GetChunkTagKVStrings()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetChunkTagKVStringsWithNewChunkTagsV2(t *testing.T) {
	// Test with empty subset
	chunkTags1 := executor.NewChunkTagsV2(nil)
	result1 := chunkTags1.GetChunkTagKVStrings()
	assert.Equal(t, "", result1)

	// Test with non-empty subset (encoded tags)
	tags := influx.PointTags{
		{Key: "host", Value: "server1"},
		{Key: "region", Value: "us-west"},
	}
	dims := []string{"host", "region"}
	expectedTags := executor.NewChunkTags(tags, dims)

	// Create a new ChunkTags with the subset from expected tags
	chunkTags2 := executor.NewChunkTagsV2(expectedTags.GetTag())
	result2 := chunkTags2.GetChunkTagKVStrings()
	assert.Equal(t, "host=server1,region=us-west", result2)
}
