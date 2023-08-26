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

package executor

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/influxdata/influxdb/uuid"
	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/castor"
)

type fieldInfo struct {
	name  string
	dType influxql.DataType
	idx   int
}

// GetFieldInfo return every series' field info in form of 2 level map. Level 1 key is series key, level 2 key is field name.
func GetFieldInfo(chunks []Chunk) (map[string]map[string]*fieldInfo, *errno.Error) {
	ret := make(map[string]map[string]*fieldInfo)
	for _, c := range chunks {
		tagIdx := c.TagIndex()
		tags := c.Tags()
		for i := 0; i < len(tagIdx); i++ {
			seriesKey := string(tags[i].subset)
			info, exist := ret[seriesKey]
			if !exist {
				info = make(map[string]*fieldInfo)
				ret[seriesKey] = info
			}
			for j, ref := range c.RowDataType().MakeRefs() {
				f, exist := info[ref.Val]
				if !exist {
					f = &fieldInfo{
						name:  ref.Val,
						dType: ref.Type,
						idx:   j,
					}
					info[ref.Val] = f
				}
				if ref.Type != f.dType {
					return nil, errno.NewError(errno.FieldTypeNotEqual)
				}
			}
		}
	}

	return ret, nil
}

// ChunkToArrowRecords must release record after use
func ChunkToArrowRecords(chunks []Chunk, taskId string, args []influxql.Expr) ([]array.Record, *errno.Error) {
	seriesFieldInfo, err := GetFieldInfo(chunks)
	if err != nil {
		return nil, err
	}

	var builderWithInterval []*array.RecordBuilder
	builderWithoutInterval := make(map[string]*array.RecordBuilder, len(chunks))

	for _, c := range chunks {
		if c.IntervalLen() <= 1 || c.IntervalLen() == c.TagLen() {
			if err = copyChunkToRecordWithoutInterval(c, builderWithoutInterval, seriesFieldInfo, taskId, args); err != nil {
				break
			}
		} else {
			var tmp []*array.RecordBuilder
			tmp, err = chunkToArrowRecordsWithInterval(c, seriesFieldInfo, taskId, args)
			builderWithInterval = append(builderWithInterval, tmp...)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		for _, b := range builderWithoutInterval {
			b.Release()
		}
		for _, b := range builderWithInterval {
			b.Release()
		}
		return nil, err
	}

	var ret []array.Record
	for _, b := range builderWithoutInterval {
		rec := b.NewRecord()
		ret = append(ret, rec)
		b.Release()
	}
	for _, b := range builderWithInterval {
		rec := b.NewRecord()
		ret = append(ret, rec)
		b.Release()
	}

	return ret, nil
}

func copyChunkToRecordWithoutInterval(
	c Chunk,
	builderSet map[string]*array.RecordBuilder,
	seriesFieldInfo map[string]map[string]*fieldInfo,
	taskId string,
	args []influxql.Expr) *errno.Error {

	// type assert already done in compile stage, minus 1 because field not in args
	algo, aOk := args[op.Algo-1].(*influxql.StringLiteral)
	cfg, cOk := args[op.Conf-1].(*influxql.StringLiteral)
	typeOfProcess, hOk := args[op.AlgoType-1].(*influxql.StringLiteral)
	if !aOk || !cOk || !hOk {
		return errno.NewError(errno.TypeAssertFail, influxql.String)
	}

	times := c.Time()
	tagIdx := c.TagIndex()
	tags := c.Tags()
	for i := 0; i < len(tagIdx); i++ {
		seriesStart := tagIdx[i]
		var seriesEnd int
		if i == len(tagIdx)-1 {
			seriesEnd = -1
		} else {
			seriesEnd = tagIdx[i+1]
		}

		seriesKey := string(tags[i].subset)
		b, exist := builderSet[seriesKey]
		if !exist {
			info, exist := seriesFieldInfo[seriesKey]
			if !exist {
				return errno.NewError(errno.FieldInfoNotFound)
			}
			aFields, err := buildRecordField(info)
			if err != nil {
				return err
			}
			metaData := buildRecordMetaData(tags[i], algo.Val, cfg.Val, typeOfProcess.Val, taskId)
			schema := arrow.NewSchema(aFields, &metaData)
			pool := memory.NewGoAllocator()
			b = array.NewRecordBuilder(pool, schema)
			builderSet[seriesKey] = b
		}

		if err := copyChunkToRecord(b, c, seriesStart, seriesEnd); err != nil {
			return err
		}

		// setup timestamp
		if seriesEnd == -1 {
			b.Field(c.NumberOfCols()).(*array.Int64Builder).AppendValues(times[seriesStart:], nil)
		} else {
			b.Field(c.NumberOfCols()).(*array.Int64Builder).AppendValues(times[seriesStart:seriesEnd], nil)
		}
	}
	return nil
}

func chunkToArrowRecordsWithInterval(
	c Chunk,
	seriesFieldInfo map[string]map[string]*fieldInfo,
	taskId string,
	args []influxql.Expr) ([]*array.RecordBuilder, *errno.Error) {

	// type assert already done in compile stage, minus 1 because field not in args
	algo, aOk := args[op.Algo-1].(*influxql.StringLiteral)
	cfg, cOk := args[op.Conf-1].(*influxql.StringLiteral)
	typeOfProcess, hOk := args[op.AlgoType-1].(*influxql.StringLiteral)
	if !aOk || !cOk || !hOk {
		return nil, errno.NewError(errno.TypeAssertFail, influxql.String)
	}

	var ret []*array.RecordBuilder
	times := c.Time()
	tagIdx := c.TagIndex()
	tags := c.Tags()
	intervalIdx := c.IntervalIndex()
	lastIntervalIdx := 0
	for i := 0; i < len(tagIdx); i++ {
		seriesStart := tagIdx[i]
		var seriesEnd int
		if i == len(tagIdx)-1 {
			seriesEnd = c.NumberOfRows() - 1
		} else {
			seriesEnd = tagIdx[i+1]
		}

		info, exist := seriesFieldInfo[string(tags[i].subset)]
		if !exist {
			return ret, errno.NewError(errno.FieldInfoNotFound)
		}
		aFields, err := buildRecordField(info)
		if err != nil {
			return ret, err
		}
		metaData := buildRecordMetaData(tags[i], algo.Val, cfg.Val, typeOfProcess.Val, taskId)
		schema := arrow.NewSchema(aFields, &metaData)

		for ; lastIntervalIdx < c.IntervalLen(); lastIntervalIdx++ {
			timeStart := intervalIdx[lastIntervalIdx]
			var timeEnd int
			if lastIntervalIdx == len(intervalIdx)-1 {
				timeEnd = -1
			} else {
				timeEnd = intervalIdx[lastIntervalIdx+1]
			}
			if !(timeStart >= seriesStart && timeStart <= seriesEnd && (timeEnd <= seriesEnd || timeEnd == -1)) {
				break
			}

			pool := memory.NewGoAllocator()
			b := array.NewRecordBuilder(pool, schema)
			if err := copyChunkToRecord(b, c, timeStart, timeEnd); err != nil {
				return ret, err
			}
			if timeEnd == -1 {
				b.Field(c.NumberOfCols()).(*array.Int64Builder).AppendValues(times[timeStart:], nil)
			} else {
				b.Field(c.NumberOfCols()).(*array.Int64Builder).AppendValues(times[timeStart:timeEnd], nil)
			}
			ret = append(ret, b)
		}
	}

	return ret, nil
}

func buildRecordMetaData(c ChunkTags, algo, cfg, typeOfProcess, taskId string) arrow.Metadata {
	metaKeys := make([]string, 0, c.Size()+8)
	metaVals := make([]string, 0, c.Size()+8)
	keys, vals := c.GetChunkTagAndValues()
	metaKeys = append(metaKeys, keys...)
	metaVals = append(metaVals, vals...)
	metaKeys = append(metaKeys, string(castor.Algorithm))
	metaVals = append(metaVals, algo)
	metaKeys = append(metaKeys, string(castor.ConfigFile))
	metaVals = append(metaVals, cfg)
	metaKeys = append(metaKeys, string(castor.ProcessType))
	metaVals = append(metaVals, typeOfProcess)

	// mark data with uuid and msgType which will be used to get corresponsive return
	metaKeys = append(metaKeys, string(castor.DataId))
	metaVals = append(metaVals, uuid.TimeUUID().String())
	metaKeys = append(metaKeys, string(castor.MessageType))
	metaVals = append(metaVals, string(castor.DATA))
	metaKeys = append(metaKeys, string(castor.TaskID))
	metaVals = append(metaVals, string(taskId))
	metaKeys = append(metaKeys, string(castor.QueryMode))
	metaVals = append(metaVals, string(castor.NormalQuery))
	metaKeys = append(metaKeys, string(castor.OutputInfo))
	metaVals = append(metaVals, string(castor.AnomalyLevel))

	metaData := arrow.NewMetadata(metaKeys, metaVals)
	return metaData
}

func buildRecordField(info map[string]*fieldInfo) ([]arrow.Field, *errno.Error) {
	aFields := make([]arrow.Field, len(info)+1) // add 1 for timestamp

	for _, f := range info {
		switch f.dType {
		case influxql.Float:
			aFields[f.idx] = arrow.Field{Name: f.name, Type: arrow.PrimitiveTypes.Float64}
		case influxql.Integer:
			aFields[f.idx] = arrow.Field{Name: f.name, Type: arrow.PrimitiveTypes.Int64}
		default:
			return nil, errno.NewError(errno.DtypeNotSupport)
		}
	}
	aFields[len(info)] = arrow.Field{Name: string(castor.DataTime), Type: arrow.PrimitiveTypes.Int64}
	return aFields, nil
}

func appendArrowFloat64(b *array.RecordBuilder, col Column, fieldIndex, seriesStart, seriesEnd int) {
	floatValues := col.FloatValues()
	if col.NilCount() == 0 {
		if seriesEnd == -1 {
			b.Field(fieldIndex).(*array.Float64Builder).AppendValues(floatValues[seriesStart:], nil)
		} else {
			b.Field(fieldIndex).(*array.Float64Builder).AppendValues(floatValues[seriesStart:seriesEnd], nil)
		}
		return
	}

	appendCnt := 0
	for j, val := range floatValues {
		timeIdx := col.GetTimeIndex(j)
		if timeIdx < seriesStart {
			continue
		}
		if seriesEnd != -1 && timeIdx >= seriesEnd {
			break
		}
		gap := timeIdx - seriesStart - b.Field(fieldIndex).Len()
		if gap > 0 {
			valid := make([]bool, gap+1) // default false
			valid[gap] = true
			v := make([]float64, gap+1)
			v[gap] = val
			b.Field(fieldIndex).(*array.Float64Builder).AppendValues(v, valid)
			appendCnt += len(v)
			continue
		}
		b.Field(fieldIndex).(*array.Float64Builder).Append(val)
		appendCnt += 1
	}

	var nRow int
	if seriesEnd == -1 {
		nRow = col.BitMap().length - seriesStart
	} else {
		nRow = seriesEnd - seriesStart
	}
	if appendCnt != nRow {
		gap := nRow - appendCnt
		valid := make([]bool, gap) // default false
		v := make([]float64, gap)
		b.Field(fieldIndex).(*array.Float64Builder).AppendValues(v, valid)
	}
}

func appendArrowInt64(b *array.RecordBuilder, col Column, fieldIndex, seriesStart, seriesEnd int) {
	integerValues := col.IntegerValues()
	if col.NilCount() == 0 {
		if seriesEnd == -1 {
			b.Field(fieldIndex).(*array.Int64Builder).AppendValues(integerValues[seriesStart:], nil)
		} else {
			b.Field(fieldIndex).(*array.Int64Builder).AppendValues(integerValues[seriesStart:seriesEnd], nil)
		}
		return
	}

	appendCnt := 0
	for j, val := range integerValues {
		timeIdx := col.GetTimeIndex(j)
		if timeIdx < seriesStart {
			continue
		}
		if seriesEnd != -1 && timeIdx >= seriesEnd {
			break
		}
		gap := timeIdx - seriesStart - b.Field(fieldIndex).Len()
		if gap > 0 {
			valid := make([]bool, gap+1) // default false
			valid[gap] = true
			v := make([]int64, gap+1)
			v[gap] = val
			b.Field(fieldIndex).(*array.Int64Builder).AppendValues(v, valid)
			appendCnt += len(v)
			continue
		}
		b.Field(fieldIndex).(*array.Int64Builder).Append(val)
		appendCnt += 1
	}

	var nRow int
	if seriesEnd == -1 {
		nRow = col.BitMap().length - seriesStart
	} else {
		nRow = seriesEnd - seriesStart
	}
	if appendCnt != nRow {
		gap := nRow - appendCnt
		valid := make([]bool, gap) // default false
		v := make([]int64, gap)
		b.Field(fieldIndex).(*array.Int64Builder).AppendValues(v, valid)
	}
}

func CopyArrowRecordToChunk(r array.Record, c Chunk, fields map[string]struct{}) *errno.Error {
	// check errInfo, if exist, just return it
	metaData := r.Schema().Metadata()
	errInfoIdx := metaData.FindKey(string(castor.ErrInfo))
	if errInfoIdx != -1 {
		return errno.NewThirdParty(fmt.Errorf(metaData.Values()[errInfoIdx]), errno.ModuleCastor)
	}

	// check number of anomaly
	nAnomaly, err := castor.GetMetaValueFromRecord(r, string(castor.AnomalyNum))
	if err != nil {
		return err
	}
	if nAnomaly == "0" {
		return nil
	}

	// time field count as one column in record
	if len(fields) == 0 && c.NumberOfCols() != int(r.NumCols()-1) {
		return errno.NewError(errno.NumOfFieldNotEqual)
	}

	// add interval
	c.AppendIntervalIndex(c.Len())

	// add chunk tags
	cts := buildChunkTagsWithFilter(metaData)
	c.AppendTagsAndIndex(*cts, c.Len())

	// add timestamp
	rTime, err := getTimestamp(r)
	if err != nil {
		return err
	}
	c.AppendTimes(rTime.Int64Values())

	// data type and order of column must strictly match between chunk and record
	// copy value to chunk
	if err := copyRecordToChunk(r, c, fields); err != nil {
		return err
	}

	return nil
}

func appendChunkFloat64(rCol *array.Float64, cCol Column) {
	valid := make([]bool, rCol.Len())
	isNilExist := false
	for j, val := range rCol.Float64Values() {
		if rCol.IsNull(j) {
			isNilExist = true
			continue
		}
		valid[j] = true
		cCol.AppendFloatValue(val)
	}
	if isNilExist {
		cCol.AppendNilsV2(valid...)
	} else {
		cCol.AppendManyNotNil(rCol.Len())
	}
}

func appendChunkInt64(rCol *array.Int64, cCol Column) {
	valid := make([]bool, rCol.Len())
	isNilExist := false
	for j, val := range rCol.Int64Values() {
		if rCol.IsNull(j) {
			isNilExist = true
			continue
		}
		valid[j] = true
		cCol.AppendIntegerValue(val)
	}
	if isNilExist {
		cCol.AppendNilsV2(valid...)
	} else {
		cCol.AppendManyNotNil(rCol.Len())
	}
}

func buildChunkTagsWithFilter(metaData arrow.Metadata) *ChunkTags {
	// combine tags from both record metadata and chunktags
	var newTags []influx.Tag
	var validKeys []string

	// build tag from castor algorithm output
	recTagLen := metaData.Len()
	recTagKeys := metaData.Keys()
	recTagVals := metaData.Values()
	for i := 0; i < recTagLen; i++ {
		if castor.IsInternalKey(recTagKeys[i]) {
			continue
		}
		newTags = append(newTags, influx.Tag{Key: recTagKeys[i], Value: recTagVals[i]})
		validKeys = append(validKeys, recTagKeys[i])
	}

	if len(newTags) == 0 {
		return &ChunkTags{}
	}

	return NewChunkTags(newTags, validKeys)
}

func getTimestamp(r array.Record) (*array.Int64, *errno.Error) {
	timeFieldIdx := r.Schema().FieldIndices(string(castor.DataTime))
	if len(timeFieldIdx) != 1 {
		return nil, errno.NewError(errno.TimestampNotFound)
	}
	rTime, ok := r.Column(timeFieldIdx[0]).(*array.Int64)
	if !ok {
		return nil, errno.NewError(errno.TypeAssertFail, arrow.PrimitiveTypes.Int64)
	}
	return rTime, nil
}

func copyRecordToChunk(r array.Record, c Chunk, fields map[string]struct{}) *errno.Error {
	schema := r.Schema()
	idx := 0
	for j, rCol := range r.Columns() {
		f := schema.Field(j)
		if f.Name == string(castor.DataTime) {
			continue
		}
		if len(fields) != 0 {
			if _, exist := fields[f.Name]; !exist {
				continue
			}
		}

		switch f.Type {
		case arrow.PrimitiveTypes.Float64:
			tmp, ok := rCol.(*array.Float64)
			if !ok {
				return errno.NewError(errno.TypeAssertFail, arrow.PrimitiveTypes.Float64)
			}
			if c.Column(idx).DataType() != influxql.Float {
				return errno.NewError(errno.DtypeNotMatch, influxql.Float, c.Column(idx).DataType())
			}
			appendChunkFloat64(tmp, c.Column(idx))
		case arrow.PrimitiveTypes.Int64:
			tmp, ok := rCol.(*array.Int64)
			if !ok {
				return errno.NewError(errno.TypeAssertFail, arrow.PrimitiveTypes.Int64)
			}
			if c.Column(idx).DataType() != influxql.Integer {
				return errno.NewError(errno.DtypeNotMatch, influxql.Integer, c.Column(idx).DataType())
			}
			appendChunkInt64(tmp, c.Column(idx))
		default:
			return errno.NewError(errno.DtypeNotSupport)
		}
		idx++
	}
	return nil
}

func copyChunkToRecord(b *array.RecordBuilder, c Chunk, seriesStart, seriesEnd int) *errno.Error {
	rdt := c.RowDataType()
	for i, col := range c.Columns() {
		fIdx := b.Schema().FieldIndices(rdt.Field(i).Name())
		if fIdx == nil {
			return errno.NewError(errno.FieldNotFound)
		}
		if len(fIdx) > 1 {
			return errno.NewError(errno.MultiFieldIndex)
		}
		switch col.DataType() {
		case influxql.Float:
			appendArrowFloat64(b, col, fIdx[0], seriesStart, seriesEnd)
		case influxql.Integer:
			appendArrowInt64(b, col, fIdx[0], seriesStart, seriesEnd)
		default:
			return errno.NewError(errno.DtypeNotSupport)
		}
	}
	return nil
}
