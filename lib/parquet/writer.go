// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquet

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const tmpsuffix = ".tmp"

type Writer struct {
	file          string
	arrowWriter   *pqarrow.FileWriter
	recordBuilder *array.RecordBuilder
	schema        *arrow.Schema
	fieldsInfo    map[string]FieldInfo
	WriteLines    uint64
	idx           uint64
	strSlice      []string
}

type FieldInfo struct {
	idx       int
	isWritten bool
	fileType  uint8
}

type MetaData struct {
	Mst     string
	Schemas map[string]uint8
}

const (
	Snappy = 0
	Zstd   = 1
)

const (
	InvalidTimeStampBuilderTemplate = "invalid builder , want TimestampBuilder, get %s"
	InvalidStringBuilderTemplate    = "invalid builder , want LargeStringBuilder, get %s"
	InvalidInt64BuilderTemplate     = "invalid builder , want Int64Builder, get %s"
	InvalidFloat64BuilderTemplate   = "invalid builder , want Float64Builder, get %s"
	InvalidBooleanBuilderTemplate   = "invalid builder , want BooleanBuilder, get %s"
)

func getCompressAlg(compressCode uint64) compress.Compression {
	switch compressCode {
	case Snappy:
		return compress.Codecs.Snappy
	case Zstd:
		return compress.Codecs.Zstd
	default:
		return compress.Codecs.Snappy
	}
}

func getDictCompress(dictCompressFlag uint64) bool {
	// 0 is disable, 1 is otherwise
	return dictCompressFlag == 1
}

func NewWriter(filename, lockPath string, metaData MetaData) (*Writer, error) {
	w := &Writer{file: filename, fieldsInfo: make(map[string]FieldInfo)}

	sh := arrow.NewSchema(
		w.buildSchema(metaData.Schemas),
		nil,
	)

	w.recordBuilder = array.NewRecordBuilder(memory.NewGoAllocator(), sh)
	w.schema = sh

	filename += tmpsuffix
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	f, err := fileops.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_TRUNC|os.O_RDWR, 0640, fileops.FileLockOption(lockPath), pri)
	if err != nil {
		return nil, err
	}
	conf := config.GetStoreConfig().ParquetTask
	writerProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(conf.MaxRowGroupLen)), parquet.WithDataPageSize(int64(conf.PageSize)),
		parquet.WithDictionaryPageSizeLimit(int64(conf.PageSize)), parquet.WithCompression(getCompressAlg(conf.CompressAlg)),
		parquet.WithBatchSize(int64(conf.WriteBatchSize)), parquet.WithDictionaryDefault(getDictCompress(conf.DictCompressEnable)), parquet.WithMaxStatsSize(conf.MaxStatsSize))

	arrowWritePros := pqarrow.NewArrowWriterProperties(pqarrow.WithCoerceTimestamps(arrow.Nanosecond))
	writer, err := pqarrow.NewFileWriter(w.schema, f, writerProps, arrowWritePros)
	if err != nil {
		util.MustClose(f)
		return w, err
	}
	w.arrowWriter = writer
	return w, nil
}

func (w *Writer) Close() {
	w.recordBuilder.Release()
	w.arrowWriter.Close()
}

func (w *Writer) updateFieldInfo(key string, isWritten bool, fieldType uint8) {
	ori := w.fieldsInfo[key]
	w.fieldsInfo[key] = FieldInfo{
		idx:       ori.idx,
		isWritten: isWritten,
		fileType:  fieldType,
	}
}

func (w *Writer) writeTsspRecord(series map[string]string, rec *record.Record) (lines int, nilFields map[string]FieldInfo, err error) {
	b := w.recordBuilder
	for i := range rec.Schema {
		col := &rec.ColVals[i]
		if lines < col.Len {
			lines = col.Len
		}

		builder := b.Field(w.fieldsInfo[rec.Schema[i].Name].idx)
		if col.NilCount == 0 {
			if err = w.appendFast(builder, rec.Schema[i].Type, col, rec.Schema[i].Name); err != nil {
				return
			}
		} else {
			if err = w.append(builder, uint8(rec.Schema[i].Type), col); err != nil {
				return
			}
		}
		w.updateFieldInfo(rec.Schema[i].Name, true, uint8(rec.Schema[i].Type))
	}

	for tagK, tagV := range series {
		if err = w.appendSeries(b.Field(w.fieldsInfo[tagK].idx), tagV, lines); err != nil {
			return
		}
		w.updateFieldInfo(tagK, true, influx.Field_Type_String)
	}

	// fill missing fields
	nilFields = make(map[string]FieldInfo, len(w.fieldsInfo))
	for k, v := range w.fieldsInfo {
		if !v.isWritten {
			nilFields[k] = FieldInfo{
				idx:       v.idx,
				isWritten: false,
				fileType:  v.fileType,
			}
		}
	}
	return
}

func (w *Writer) WriteRecord(series map[string]string, rec *record.Record) error {
	lines, nilFields, err := w.writeTsspRecord(series, rec)
	if err != nil {
		return err
	}

	// fill missing fields
	b := w.recordBuilder
	for k, v := range nilFields {
		if err := w.append(b.Field(w.fieldsInfo[k].idx), v.fileType, &record.ColVal{NilCount: lines, Len: lines}); err != nil {
			return err
		}
	}

	idx := atomic.AddUint64(&w.idx, 1)
	if idx%config.GetStoreConfig().ParquetTask.ItrBatchSize == 0 {
		if err := w.ForceFlush(); err != nil {
			return err
		}
	}

	atomic.AddUint64(&w.WriteLines, uint64(lines))
	for k, v := range w.fieldsInfo {
		w.updateFieldInfo(k, false, v.fileType)
	}
	return nil
}

func (w *Writer) ForceFlush() error {
	b := w.recordBuilder
	aw := w.arrowWriter
	arrowRecord := b.NewRecord()
	defer arrowRecord.Release()
	if arrowRecord.NumRows() == 0 {
		return nil
	}
	if err := aw.WriteBuffered(arrowRecord); err != nil {
		b.Release()
		return err
	}
	return nil
}

func (w *Writer) WriteStop() error {
	if w.arrowWriter == nil {
		return errors.New("nil arrow writer")
	}
	if err := w.arrowWriter.Close(); err != nil {
		return err
	}

	// only successfully write stop, the file can be regarded as ok
	return fileops.RenameFile(w.file+tmpsuffix, w.file)
}

func (w *Writer) transferInt64ToTimeStamp(vals []int64) []arrow.Timestamp {
	timeStamps := make([]arrow.Timestamp, 0, len(vals))
	for _, val := range vals {
		timeStamps = append(timeStamps, arrow.Timestamp(val))
	}
	return timeStamps
}

func (w *Writer) appendFast(b array.Builder, typ int, col *record.ColVal, name string) error {
	if name == record.TimeField {
		builder, ok := b.(*array.TimestampBuilder)
		if !ok {
			return fmt.Errorf(InvalidTimeStampBuilderTemplate, b)
		}
		builder.AppendValues(w.transferInt64ToTimeStamp(col.IntegerValues()), nil)
		return nil
	}

	switch typ {
	case influx.Field_Type_String:
		builder, ok := b.(*array.LargeStringBuilder)
		if !ok {
			return fmt.Errorf(InvalidStringBuilderTemplate, b.Type().String())
		}
		builder.AppendValues(col.StringValues(w.strSlice[:0]), nil)
	case influx.Field_Type_Int:
		builder, ok := b.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf(InvalidInt64BuilderTemplate, b.Type().String())
		}
		builder.AppendValues(col.IntegerValues(), nil)
	case influx.Field_Type_Float:
		builder, ok := b.(*array.Float64Builder)
		if !ok {
			return fmt.Errorf(InvalidFloat64BuilderTemplate, b.Type().String())
		}
		builder.AppendValues(col.FloatValues(), nil)
	case influx.Field_Type_Boolean:
		builder, ok := b.(*array.BooleanBuilder)
		if !ok {
			return fmt.Errorf(InvalidBooleanBuilderTemplate, b.Type().String())
		}
		builder.AppendValues(col.BooleanValues(), nil)
	}
	return nil
}

func iterateStringValue(callback func([]byte, bool), cv *record.ColVal) {
	for i := 0; i < cv.Len; i++ {
		callback(cv.StringValue(i))
	}
}

func iterateIntValue(callback func(int64, bool), cv *record.ColVal) {
	for i := 0; i < cv.Len; i++ {
		callback(cv.IntegerValue(i))
	}
}

func iterateBoolValue(callback func(bool, bool), cv *record.ColVal) {
	for i := 0; i < cv.Len; i++ {
		callback(cv.BooleanValue(i))
	}
}

func iterateFloatValue(callback func(float64, bool), cv *record.ColVal) {
	for i := 0; i < cv.Len; i++ {
		callback(cv.FloatValue(i))
	}
}

func (w *Writer) appendSeries(b array.Builder, series string, lines int) error {
	builder, ok := b.(*array.LargeStringBuilder)
	if !ok {
		return fmt.Errorf(InvalidStringBuilderTemplate, b)
	}
	for i := 0; i < lines; i++ {
		builder.Append(series)
	}
	return nil
}

func (w *Writer) append(b array.Builder, typ uint8, col *record.ColVal) error {
	switch typ {
	case influx.Field_Type_String:
		builder, ok := b.(*array.LargeStringBuilder)
		if !ok {
			return fmt.Errorf(InvalidStringBuilderTemplate, b.Type().String())
		}
		iterateStringValue(func(val []byte, isNil bool) {
			if isNil {
				builder.AppendNull()
			} else {
				builder.Append(string(val))
			}
		}, col)
	case influx.Field_Type_Int:
		builder, ok := b.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf(InvalidInt64BuilderTemplate, b.Type().String())
		}
		iterateIntValue(func(val int64, isNil bool) {
			if isNil {
				builder.AppendNull()
			} else {
				builder.Append(val)
			}
		}, col)
	case influx.Field_Type_Float:
		builder, ok := b.(*array.Float64Builder)
		if !ok {
			return fmt.Errorf(InvalidFloat64BuilderTemplate, b.Type().String())
		}
		iterateFloatValue(func(val float64, isNil bool) {
			if isNil {
				builder.AppendNull()
			} else {
				builder.Append(val)
			}
		}, col)
	case influx.Field_Type_Boolean:
		builder, ok := b.(*array.BooleanBuilder)
		if !ok {
			return fmt.Errorf(InvalidBooleanBuilderTemplate, b.Type().String())
		}
		iterateBoolValue(func(val bool, isNil bool) {
			if isNil {
				builder.AppendNull()
			} else {
				builder.Append(val)
			}
		}, col)
	}
	return nil
}

func (w *Writer) buildSchema(schema map[string]uint8) []arrow.Field {
	schemaLength := len(schema)
	fields := make([]arrow.Field, schemaLength)
	idx := -1
	for fieldName, fieldType := range schema {
		idx++
		fields[idx].Nullable = true
		fields[idx].Name = fieldName

		w.fieldsInfo[fieldName] = FieldInfo{
			idx:       idx,
			isWritten: false,
			fileType:  fieldType,
		}

		if fieldName == record.TimeField {
			fields[idx].Type = arrow.FixedWidthTypes.Timestamp_ns
			continue
		}

		switch fieldType {
		case influx.Field_Type_String:
			fields[idx].Type = arrow.BinaryTypes.LargeString
		case influx.Field_Type_Int:
			fields[idx].Type = arrow.PrimitiveTypes.Int64
		case influx.Field_Type_Float:
			fields[idx].Type = arrow.PrimitiveTypes.Float64
		case influx.Field_Type_Boolean:
			fields[idx].Type = arrow.FixedWidthTypes.Boolean
		}
	}
	return fields
}
