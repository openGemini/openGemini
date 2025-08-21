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
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/services/castor"
)

type timeValTuple struct {
	timestamp int64
	value     interface{}
}
type timeValList []timeValTuple

func newTimeValTuple(timestamp []int64, values []interface{}) *timeValList {
	var ret timeValList
	dLen := len(timestamp)
	for i := 0; i < dLen; i++ {
		ret = append(ret, timeValTuple{
			timestamp: timestamp[i],
			value:     values[i],
		})
	}
	sort.Sort(ret)
	return &ret
}

func (t timeValList) Len() int           { return len(t) }
func (t timeValList) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t timeValList) Less(i, j int) bool { return t[i].timestamp < t[j].timestamp }
func matchContent(c executor.Chunk, recs []arrow.Record) bool {
	// timestamp store as a column in arrow.Record

	// one series one record
	if c.TagLen() != len(recs) {
		return false
	}

	// check number of rows and columns
	nRows := 0
	for _, r := range recs {
		nRows += int(r.NumRows())
		if int(r.NumCols())-1 != c.NumberOfCols() {
			return false
		}
	}
	if nRows != c.NumberOfRows() {
		return false
	}

	// check length and nils equality
	for i, cCol := range c.Columns() {
		rNilsCount := 0
		rColLen := 0
		for _, r := range recs {
			name := c.RowDataType().Field(i).Name()
			rFieldIdx := r.Schema().FieldIndices(name)
			if len(rFieldIdx) != 1 {
				return false
			}
			rCol := r.Column(rFieldIdx[0])
			rNilsCount += rCol.NullN()
			rColLen += rCol.Len()
		}
		if rColLen != cCol.Length() {
			return false
		}
		if rNilsCount != cCol.NilCount() {
			return false
		}
	}

	// check timestamp
	if !isTimeEqual(c, recs) {
		return false
	}

	// chunk tags should all in records' metadata
	if !isTagMatch(c, recs) {
		return false
	}

	recVals := make(map[string]interface{})
	for _, r := range recs {
		for i, rCol := range r.Columns() {
			name := r.Schema().Field(i).Name
			vals, exist := recVals[name]
			switch rCol.DataType().ID() {
			case arrow.FLOAT64:
				tmp := rCol.(*array.Float64).Float64Values()
				if !exist {
					vals = make([]float64, 0)
				}
				vals = append(vals.([]float64), tmp...)
				recVals[name] = vals
			case arrow.INT64:
				tmp := rCol.(*array.Int64).Int64Values()
				if !exist {
					vals = make([]int64, 0)
				}
				vals = append(vals.([]int64), tmp...)
				recVals[name] = vals
			case arrow.STRING:
				tmp := rCol.(*array.String).ValueBytes()
				if !exist {
					vals = make([]byte, 0)
				}
				vals = append(vals.([]byte), tmp...)
				recVals[name] = vals
			default:
				panic("type not support")
			}
		}
	}
	for i, cCol := range c.Columns() {
		name := c.RowDataType().Field(i).Name()
		tmp := recVals[name]

		switch cCol.DataType() {
		case influxql.Float:
			if cCol.NilCount() == 0 {
				if reflect.DeepEqual(tmp, cCol.FloatValues()) {
					continue
				}
				return false
			}
			for j, cVal := range cCol.FloatValues() {
				actualIdx := cCol.GetTimeIndex(j)
				rVals := tmp.([]float64)
				if rVals[actualIdx] != cVal {
					return false
				}
			}
		case influxql.Integer:
			if cCol.NilCount() == 0 {
				if reflect.DeepEqual(tmp, cCol.IntegerValues()) {
					continue
				}
				return false
			}
			for j, cVal := range cCol.IntegerValues() {
				actualIdx := cCol.GetTimeIndex(j)
				rVals := tmp.([]int64)
				if rVals[actualIdx] != cVal {
					return false
				}
			}
		case influxql.String:
			bytes := cCol.StringValue(0)
			if cCol.NilCount() == 0 {
				res := fmt.Sprintf("{\"%s\": \"%s\"}", name, tmp)
				if res != bytes {
					continue
				}
				return false
			}
			for j, cVal := range cCol.StringValuesV2([]string{}) {
				actualIdx := cCol.GetTimeIndex(j)
				rVals := tmp.([]string)
				if rVals[actualIdx] != cVal {
					return false
				}
			}
		default:
			panic("type not support")
		}
	}

	return true
}

func isTagMatch(c executor.Chunk, recs []arrow.Record) bool {
	rKeyVals := make(map[string]map[string]struct{})
	for _, r := range recs {
		for i, k := range r.Schema().Metadata().Keys() {
			tmp := rKeyVals[k]
			if tmp == nil {
				tmp = make(map[string]struct{})
			}
			tmp[r.Schema().Metadata().Values()[i]] = struct{}{}
			rKeyVals[k] = tmp
		}
	}

	for _, t := range c.Tags() {
		for k, v := range t.KeyValues() {
			rVals, exist := rKeyVals[k]
			if !exist {
				return false
			}
			_, exist = rVals[v]
			if !exist {
				return false
			}
		}
	}
	return true
}

func isTimeEqual(c executor.Chunk, recs []arrow.Record) bool {
	cTimes := c.Time()
	var rTimes []int64

	for _, r := range recs {
		times, ok := r.Column(int(r.NumCols()) - 1).(*array.Int64)
		if !ok {
			return false
		}
		rTimes = append(rTimes, times.Int64Values()...)
	}

	return reflect.DeepEqual(cTimes, rTimes)
}

func matchMultiContent(chunks []executor.Chunk, recs []arrow.Record) error {
	chunkVals := make(map[string][]interface{})
	chunkTimes := make(map[string][]int64)
	for _, c := range chunks {
		tagIdx := c.TagIndex()
		refs := c.RowDataType().MakeRefs()
		for i, t := range c.Tags() {
			cTagVal, exist := t.GetChunkTagValue("t")
			if !exist {
				return fmt.Errorf("tag not found")
			}
			cVals, exist := chunkVals[cTagVal]
			if !exist {
				cVals = []interface{}{}
			}
			start := tagIdx[i]
			var end int

			switch refs[0].Type {
			case influxql.Float:
				if i == len(tagIdx)-1 {
					end = len(c.Column(0).FloatValues())
				} else {
					end = tagIdx[i+1]
				}
				for _, v := range c.Column(0).FloatValues()[start:end] {
					cVals = append(cVals, v)
				}
			case influxql.Integer:
				if i == len(tagIdx)-1 {
					end = len(c.Column(0).IntegerValues())
				} else {
					end = tagIdx[i+1]
				}
				for _, v := range c.Column(0).IntegerValues()[start:end] {
					cVals = append(cVals, v)
				}
			default:
				return fmt.Errorf("only support integer\\float type")
			}

			chunkVals[cTagVal] = cVals
			cTimes, exist := chunkTimes[cTagVal]
			if !exist {
				cTimes = []int64{}
			}
			cTimes = append(cTimes, c.Time()[start:end]...)
			chunkTimes[cTagVal] = cTimes
		}
	}

	recVals := make(map[string][]interface{})
	recTimes := make(map[string][]int64)
	for _, r := range recs {
		rTagVal, err := castor.GetMetaValueFromRecord(r, "t")
		if err != nil {
			return err
		}
		vals, exist := recVals[rTagVal]
		if !exist {
			vals = []interface{}{}
		}

		switch r.Schema().Field(0).Type {
		case arrow.PrimitiveTypes.Float64:
			rCol, ok := r.Column(0).(*array.Float64)
			if !ok {
				return fmt.Errorf("dtype not correct")
			}
			for _, v := range rCol.Float64Values() {
				vals = append(vals, v)
			}
		case arrow.PrimitiveTypes.Int64:
			rCol, ok := r.Column(0).(*array.Int64)
			if !ok {
				return fmt.Errorf("dtype not correct")
			}
			for _, v := range rCol.Int64Values() {
				vals = append(vals, v)
			}
		}
		recVals[rTagVal] = vals

		rTime, ok := r.Column(1).(*array.Int64)
		if !ok {
			return fmt.Errorf("dtype not correct")
		}
		times, exist := recTimes[rTagVal]
		if !exist {
			times = []int64{}
		}
		times = append(times, rTime.Int64Values()...)
		recTimes[rTagVal] = times
	}

	if len(chunkVals) != len(recVals) {
		return fmt.Errorf("value size not equal")
	}
	if len(chunkTimes) != len(recTimes) {
		return fmt.Errorf("time size not equal")
	}

	for key, cTimes := range chunkTimes {
		cVals := chunkVals[key]
		rTimes, exist := recTimes[key]
		if !exist {
			return fmt.Errorf("series not found")
		}
		rVals := recVals[key]

		chunkData := newTimeValTuple(cTimes, cVals)
		recordData := newTimeValTuple(rTimes, rVals)
		if !reflect.DeepEqual(chunkData, recordData) {
			return fmt.Errorf("data not equal")
		}
	}

	return nil
}

func buildIntegerChunk() []executor.Chunk {
	row2 := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "f", Type: influxql.Integer})
	cb2 := executor.NewChunkBuilder(row2)
	ch2 := cb2.NewChunk("castor")
	timestamp2 := []int64{1, 2, 3, 4, 5, 6}
	ch2.AppendTimes(timestamp2)
	seriesIdx2 := []int{0, 3}
	ch2.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1"), *ParseChunkTags("t=2")}, seriesIdx2)
	ch2.AppendIntervalIndexes(seriesIdx2)
	ch2.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5, 6})
	ch2.Column(0).AppendNilsV2(true, true, true, true, true, true)
	ch2.CheckChunk()

	return []executor.Chunk{ch2}
}

func buildDiffDtypeChunk() []executor.Chunk {
	row := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "f", Type: influxql.Float})
	cb := executor.NewChunkBuilder(row)
	ch := cb.NewChunk("castor")
	ch.AppendTime(0)
	ch.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, []int{0})
	ch.AppendIntervalIndex(0)
	ch.Column(0).AppendFloatValue(0.0)
	ch.Column(0).AppendNotNil()
	ch.CheckChunk()

	row2 := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "f", Type: influxql.Integer})
	cb2 := executor.NewChunkBuilder(row2)
	ch2 := cb2.NewChunk("castor")
	ch2.AppendTime(0)
	ch2.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, []int{0})
	ch2.AppendIntervalIndex(0)
	ch2.Column(0).AppendIntegerValue(0)
	ch2.Column(0).AppendNotNil()
	ch2.CheckChunk()

	return []executor.Chunk{ch, ch2}
}

func buildUnsupportDtypeChunk() []executor.Chunk {
	row := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "f", Type: influxql.String})
	cb := executor.NewChunkBuilder(row)
	ch := cb.NewChunk("castor")
	ch.AppendTime(0)
	ch.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, []int{0})
	ch.AppendIntervalIndex(0)
	ch.Column(0).AppendStringValue("str")
	ch.Column(0).AppendNotNil()
	ch.CheckChunk()

	return []executor.Chunk{ch}
}

func buildMultiFloatChunk() []executor.Chunk {
	row1 := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "f", Type: influxql.Float})
	cb1 := executor.NewChunkBuilder(row1)
	ch1 := cb1.NewChunk("castor")
	timestamp1 := []int64{0, 1, 2, 3, 4, 5, 6}
	ch1.AppendTimes(timestamp1)
	seriesIdx := []int{0, 3}
	interval := []int{0, 1, 3, 4, 6}
	ch1.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1"), *ParseChunkTags("t=2")}, seriesIdx)
	ch1.AppendIntervalIndexes(interval)
	ch1.Column(0).AppendFloatValues([]float64{0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0})
	ch1.Column(0).AppendNilsV2(true, true, true, true, true, true, true)
	ch1.CheckChunk()

	row2 := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "f", Type: influxql.Float})
	cb2 := executor.NewChunkBuilder(row2)
	ch2 := cb2.NewChunk("castor")
	timestamp2 := []int64{7, 8, 9}
	ch2.AppendTimes(timestamp2)
	seriesIdx2 := []int{0}
	ch2.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, seriesIdx2)
	ch2.AppendIntervalIndexes(seriesIdx2)
	ch2.Column(0).AppendFloatValues([]float64{7.0, 8.0, 9.0})
	ch2.Column(0).AppendNilsV2(true, true, true)
	ch2.CheckChunk()

	row3 := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "f", Type: influxql.Float})
	cb3 := executor.NewChunkBuilder(row3)
	ch3 := cb3.NewChunk("castor")
	timestamp3 := []int64{7, 8, 9}
	ch3.AppendTimes(timestamp3)
	seriesIdx3 := []int{0}
	ch3.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=2")}, seriesIdx3)
	ch3.AppendIntervalIndexes(seriesIdx3)
	ch3.Column(0).AppendFloatValues([]float64{7.0, 8.0, 9.0})
	ch3.Column(0).AppendNilsV2(true, true, true)
	ch3.CheckChunk()

	return []executor.Chunk{ch1, ch2, ch3}
}

func Test_arrowRecordToChunk(t *testing.T) {
	rec := castor.BuildNumericRecord()
	baseSchema := rec.Schema()
	row, err := buildChunkSchema(baseSchema)
	if err != nil {
		t.Fatal(err)
	}
	cb := executor.NewChunkBuilder(row)
	chunk := cb.NewChunk("castor")
	if err := executor.CopyArrowRecordToChunk(rec, chunk, nil); err != nil {
		t.Fatal("convert pure numeric record fail")
	}
	if !matchContent(chunk, []arrow.Record{rec}) {
		t.Fatal("content not match")
	}
}

func Test_castorADArrowRecordToChunk(t *testing.T) {
	rec := castor.BuildStringRecord()
	baseSchema := rec.Schema()
	row, err := buildChunkSchema(baseSchema)
	if err != nil {
		t.Fatal(err)
	}
	cb := executor.NewChunkBuilder(row)
	chunk := cb.NewChunk("castor_ad")
	if err := executor.CopyCastorADArrowRecordToChunk(rec, chunk, nil); err != nil {
		t.Fatal("convert pure numeric record fail")
	}
	if !matchContent(chunk, []arrow.Record{rec}) {
		t.Fatal("content not match")
	}
}

func Test_castorADArrowRecordToChunk_Error(t *testing.T) {
	rec := castor.BuildNumericRecord()
	baseSchema := rec.Schema()
	row, err := buildChunkSchema(baseSchema)
	if err != nil {
		t.Fatal(err)
	}
	cb := executor.NewChunkBuilder(row)
	chunk := cb.NewChunk("castor_ad")
	if err := executor.CopyCastorADArrowRecordToChunk(rec, chunk, nil); err == nil {
		t.Fatal("expect CopyCastorADArrowRecordToChunk return error")
	}
}

// expect return records with distinct seires when multiple chunk as input
func Test_ChunkToArrowRecord_Float(t *testing.T) {
	chunks := buildMultiFloatChunk()
	args := []influxql.Expr{
		&influxql.StringLiteral{Val: "algo"},
		&influxql.StringLiteral{Val: "xx.conf"},
		&influxql.StringLiteral{Val: "detect"},
	}
	recs, err := executor.ChunkToArrowRecords(chunks, "123", args)
	if err != nil {
		t.Fatal("convert pure numeric chunk fail")
	}
	if err := matchMultiContent(chunks, recs); err != nil {
		t.Fatal("content not match")
	}
}

func Test_ChunkToArrowRecord_UDF_Detect(t *testing.T) {
	chunks := buildIntegerChunk()
	args := []influxql.Expr{
		&influxql.StringLiteral{Val: "algo"},
		&influxql.StringLiteral{Val: "xx.conf"},
		&influxql.StringLiteral{Val: "_udf_detect"},
		&influxql.StringLiteral{Val: "{}"},
	}
	recs, err := executor.ChunkToArrowRecords(chunks, "123", args)
	if err != nil {
		t.Fatal("convert pure numeric chunk fail")
	}
	if err := matchMultiContent(chunks, recs); err != nil {
		t.Fatal("content not match")
	}
}

func Test_ChunkToArrowRecord_UDF_Detect_Error(t *testing.T) {
	chunks := buildIntegerChunk()
	args := []influxql.Expr{
		&influxql.StringLiteral{Val: "algo"},
		&influxql.StringLiteral{Val: "xx.conf"},
		&influxql.StringLiteral{Val: "_udf_detect"},
		&influxql.TimeLiteral{Val: time.Now()},
	}
	_, err := executor.ChunkToArrowRecords(chunks, "123", args)
	if err == nil {
		t.Fatal("expect ChunkToArrowRecords return error")
	}

}

func Test_ChunkToArrowRecord_Int(t *testing.T) {
	chunks := buildIntegerChunk()
	args := []influxql.Expr{
		&influxql.StringLiteral{Val: "algo"},
		&influxql.StringLiteral{Val: "xx.conf"},
		&influxql.StringLiteral{Val: "detect"},
	}
	recs, err := executor.ChunkToArrowRecords(chunks, "123", args)
	if err != nil {
		t.Fatal("convert pure numeric chunk fail")
	}
	if err := matchMultiContent(chunks, recs); err != nil {
		t.Fatal("content not match")
	}
}

func Test_chunkToArrowRecords_NoneNumeric(t *testing.T) {
	chunks := buildUnsupportDtypeChunk()
	args := []influxql.Expr{
		&influxql.StringLiteral{Val: "algo"},
		&influxql.StringLiteral{Val: "xx.conf"},
		&influxql.StringLiteral{Val: "detect"},
	}
	_, err := executor.ChunkToArrowRecords(chunks, "123", args)
	if err == nil || !errno.Equal(err, errno.DtypeNotSupport) {
		t.Fatal("only support numeric data type, but no expected error return")
	}
}

func Test_chunkToArrowRecords_InvalidArgsType(t *testing.T) {
	chunks := buildIntegerChunk()
	args := []influxql.Expr{
		&influxql.IntegerLiteral{Val: 0},
		&influxql.IntegerLiteral{Val: 0},
		&influxql.IntegerLiteral{Val: 0},
	}
	_, err := executor.ChunkToArrowRecords(chunks, "123", args)
	if err == nil {
		t.Fatal("args should be string type, but no expected error return")
	}
}
func Test_getFieldInfo_TypeNotEqual(t *testing.T) {
	chunks := buildDiffDtypeChunk()
	if _, err := executor.GetFieldInfo(chunks); err == nil || !errno.Equal(err, errno.FieldTypeNotEqual) {
		t.Fatal("data type change in chunks but not detected")
	}
}

func buildChunkSchema(schema *arrow.Schema) (*hybridqp.RowDataTypeImpl, *errno.Error) {
	varRefs := make([]influxql.VarRef, len(schema.Fields())-1) // minus 1 for timestamp
	idx := 0
	for _, f := range schema.Fields() {
		if f.Name == string(castor.DataTime) || f.Name == string(castor.DataTimeStamp) {
			continue
		}
		switch f.Type.(type) {
		case *arrow.Float64Type:
			varRefs[idx] = influxql.VarRef{Val: f.Name, Type: influxql.Float}
		case *arrow.Int64Type:
			varRefs[idx] = influxql.VarRef{Val: f.Name, Type: influxql.Integer}
		case *arrow.StringType:
			varRefs[idx] = influxql.VarRef{Val: f.Name, Type: influxql.String}
		default:
			return nil, errno.NewError(errno.DtypeNotSupport)
		}
		idx++
	}
	row := hybridqp.NewRowDataTypeImpl(varRefs...)
	return row, nil
}
