// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package sparseindex

import (
	"errors"
	"fmt"
	"math"
	"path"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/spf13/cast"
)

const (
	MinValueColumnName = "_min"
	MaxValueColumnName = "_max"
	InvalidIndex       = -1
	EquivalentAccuracy = 1e-7
)

var _ = RegistrySKFileReaderCreator(uint32(index.MinMax), &MinMaxReaderCreator{})

type MinMaxReaderCreator struct {
}

func (creator *MinMaxReaderCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewMinMaxIndexReader(rpnExpr, schema, option, isCache)
}

// MinMaxIndexReader:
//  1. the data record. the fragment size is 3.
//     f1 f2
//     1  4
//     2  5
//     3  6
//  2. the index record
//     1  4  -> min value
//     3  6  -> max value

type MinMaxIndexReader struct {
	isCache bool
	option  hybridqp.Options
	schema  record.Schemas
	// read the data of the index according to the file and index fields.
	ReadFunc func(file interface{}, rec *record.Record, isCache bool) (*record.Record, error)
	sk       SKCondition
	readers  *MinMaxFilterReaders
	span     *tracing.Span
}

func NewMinMaxIndexReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (*MinMaxIndexReader, error) {
	sk, err := NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}
	return &MinMaxIndexReader{
		schema:  schema,
		option:  option,
		isCache: isCache,
		sk:      sk,
	}, nil
}

func (r *MinMaxIndexReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.sk.IsExist(int64(fragId), r.readers)
}

func (r *MinMaxIndexReader) GetFragmentRowCount(fragId uint32) (int64, error) {
	return 0, nil
}

func (r *MinMaxIndexReader) StartSpan(span *tracing.Span) {
	r.span = span
}

func (r *MinMaxIndexReader) Close() error {
	return nil
}

type MinMaxWriter struct {
	*skipIndexWriter
	indexRecord  *record.Record
	indexBuilder *colstore.IndexBuilder
}

func NewMinMaxWriter(dir, msName, dataFilePath, lockPath string, tokens string) *MinMaxWriter {
	m := &MinMaxWriter{
		skipIndexWriter: newSkipIndexWriter(dir, msName, dataFilePath, lockPath, tokens),
	}

	indexFilePath := path.Join(m.dir, m.msName, colstore.AppendSecondaryIndexSuffix(m.dataFilePath, "", index.MinMax, 0))
	m.indexBuilder = colstore.NewIndexBuilder(&m.lockPath, indexFilePath+tmpFileSuffix)
	m.indexRecord = &record.Record{}
	return m
}

func (m *MinMaxWriter) Files() []string {
	return []string{m.indexBuilder.FilePath()}
}

func (m *MinMaxWriter) initMMIndex(field record.Field) (int, int) {
	// Consider the scenario where the same batch of data is divided into multiple segments based on primary keys.
	minIndex := m.indexRecord.Schema.FieldIndex(field.Name + MinValueColumnName)
	if minIndex != InvalidIndex {
		return minIndex, minIndex + 1
	}

	minIndex = m.indexRecord.Len()
	maxIndex := m.indexRecord.Len() + 1
	minMaxSchema := record.Schemas{
		{
			Name: field.Name + MinValueColumnName,
			Type: field.Type,
		},
		{
			Name: field.Name + MaxValueColumnName,
			Type: field.Type,
		},
	}
	m.indexRecord.Schema = append(m.indexRecord.Schema, minMaxSchema...)

	m.indexRecord.ReserveColVal(len(minMaxSchema))
	return minIndex, maxIndex
}

func (m *MinMaxWriter) Flush() error {
	return m.indexBuilder.WriteData(m.indexRecord, colstore.DefaultTCLocation)
}

func (m *MinMaxWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	for _, idx := range schemaIdx {
		field := writeRec.Schema[idx]
		minColIndex, maxColIndex := m.initMMIndex(field)
		for segIdx := 0; segIdx < len(rowsPerSegment); segIdx++ {
			startRow := 0
			if segIdx > 0 {
				startRow = rowsPerSegment[segIdx-1]
			}
			endRow := rowsPerSegment[segIdx]

			// Calculate minValue/maxValue for current segment
			minIndex, maxIndex, err := m.calculateSegmentMinMax(writeRec, idx, startRow, endRow)
			if err != nil {
				return err
			}

			if minIndex == InvalidIndex || maxIndex == InvalidIndex {
				if m.indexRecord.Schema[minColIndex].Type == influx.Field_Type_Int {
					m.indexRecord.ColVals[minColIndex].AppendInteger(math.MaxInt64)
					m.indexRecord.ColVals[maxColIndex].AppendInteger(math.MinInt64)
				} else {
					m.indexRecord.ColVals[minColIndex].AppendFloat(math.Inf(1))
					m.indexRecord.ColVals[maxColIndex].AppendFloat(math.Inf(-1))
				}
			} else {
				m.indexRecord.ColVals[minColIndex].AppendByteSlice(writeRec.ColVals[idx].Val[util.Int64SizeBytes*minIndex : util.Int64SizeBytes*(minIndex+1)])
				m.indexRecord.ColVals[maxColIndex].AppendByteSlice(writeRec.ColVals[idx].Val[util.Int64SizeBytes*maxIndex : util.Int64SizeBytes*(maxIndex+1)])
			}
		}

	}
	return nil
}

func (m *MinMaxWriter) calculateSegmentMinMax(writeRec *record.Record, fieldIdx, startRow, endRow int) (int, int, error) {
	colVal := writeRec.Column(fieldIdx)
	fieldType := writeRec.Schema[fieldIdx].Type

	var minIndex, maxIndex int
	switch fieldType {
	case influx.Field_Type_Int:
		values := colVal.IntegerValues()
		minIndex, maxIndex = calculateMinMaxIndices[int64](values, startRow, endRow, math.MaxInt64, math.MinInt64)
	case influx.Field_Type_Float:
		values := colVal.FloatValues()
		minIndex, maxIndex = calculateMinMaxIndices[float64](values, startRow, endRow, math.Inf(1), math.Inf(-1))
	default:
		return InvalidIndex, InvalidIndex, errno.NewError(errno.UnsupportedDataType, "writeRec.Schema[idx].Type")
	}
	return minIndex, maxIndex, nil
}

func calculateMinMaxIndices[T Number](values []T, startRow, endRow int, initialMin, initialMax T) (int, int) {
	minIndex := -1
	maxIndex := -1
	minValue := initialMin
	maxValue := initialMax

	loopEnd := endRow
	if loopEnd > len(values) {
		loopEnd = len(values)
	}

	for i := startRow; i < loopEnd; i++ {
		if values[i] < minValue {
			minValue = values[i]
			minIndex = i
		}
		if values[i] > maxValue {
			maxIndex = i
			maxValue = values[i]
		}
	}

	return minIndex, maxIndex
}

func (m *MinMaxWriter) Close() error {
	m.indexBuilder.Reset()
	return nil
}

type Number interface {
	~int64 | ~float64
}

// checkIsInMinMax checks if the min/max values match the condition
func checkIsInMinMax[T Number](elem *rpn.SKRPNElement, minValue, maxValue T) (bool, error) {
	value := cast.ToFloat64(elem.Value)
	minValueFloat := float64(minValue)
	maxValueFloat := float64(maxValue)

	switch elem.Op {
	case influxql.EQ:
		return compareValues(value, minValueFloat) >= 0 && compareValues(value, maxValueFloat) <= 0, nil
	case influxql.NEQ:
		if compareValues(value, minValueFloat) == 0 && compareValues(value, maxValueFloat) == 0 {
			return false, nil
		}
	case influxql.LT:
		if compareValues(value, minValueFloat) <= 0 {
			return false, nil
		}
	case influxql.LTE:
		return compareValues(value, minValueFloat) >= 0, nil
	case influxql.GTE:
		return compareValues(value, maxValueFloat) <= 0, nil
	case influxql.GT:
		if compareValues(value, maxValueFloat) >= 0 {
			return false, nil
		}
	default:
	}
	return true, nil
}

// compareValues compares two values of the same type
func compareValues(a, b float64) int {
	if math.Abs(a-b) <= EquivalentAccuracy {
		return 0
	} else if a < b {
		return -1
	} else {
		return 1
	}
}

// MinMaxFilterReaders manages multiple MinMaxFilterReaders for different fields
type MinMaxFilterReaders struct {
	path       string
	file       string
	ir         *influxql.IndexRelation
	schemas    record.Schemas
	indexCache map[string]*record.Record
	span       *tracing.Span
}

// NewMinMaxFilterReaders creates a new MinMaxFilterReaders
func NewMinMaxFilterReaders(path, file string, schemas record.Schemas, ir *influxql.IndexRelation) *MinMaxFilterReaders {
	return &MinMaxFilterReaders{
		path:       path,
		file:       file,
		ir:         ir,
		schemas:    schemas,
		indexCache: map[string]*record.Record{},
	}
}

// Close closes all readers
func (r *MinMaxFilterReaders) Close() error {
	return nil
}

// StartSpan starts a tracing span
func (r *MinMaxFilterReaders) StartSpan(span *tracing.Span) {
	r.span = span
}

// IsExist checks if the fragment might contain data matching the condition
func (r *MinMaxFilterReaders) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	rec, err := r.InitReaderAndQuery(elem)
	if err != nil {
		return false, err
	}
	minValueColumnName := elem.Key + MinValueColumnName
	minColIndex := rec.Schema.FieldIndex(minValueColumnName)

	if minColIndex == InvalidIndex {
		return false, nil
	}

	segId := int(blockId)
	maxColIndex := minColIndex + 1
	switch rec.Schema.Field(minColIndex).Type {
	case influx.Field_Type_Int:
		minValue, okMin := rec.Column(minColIndex).IntegerValue(segId)
		maxValue, okMax := rec.Column(maxColIndex).IntegerValue(segId)
		if okMin || okMax {
			return false, nil
		}
		return checkIsInMinMax(elem, minValue, maxValue)
	case influx.Field_Type_Float:
		minValue, okMin := rec.Column(minColIndex).FloatValue(segId)
		maxValue, okMax := rec.Column(maxColIndex).FloatValue(segId)
		if okMin || okMax {
			return false, nil
		}
		return checkIsInMinMax(elem, minValue, maxValue)
	default:
		return true, nil
	}
}

// GetRowCount returns the row count for the fragment
func (r *MinMaxFilterReaders) GetRowCount(blockId int64, elem *rpn.SKRPNElement) (int64, error) {
	return 0, nil
}

// InitReaderAndQuery initializes a reader and gets the query string
func (r *MinMaxFilterReaders) InitReaderAndQuery(elem *rpn.SKRPNElement) (*record.Record, error) {
	if elem == nil {
		return nil, errors.New("the input SKRPNElement is nil")
	}

	// Find the field index
	idx := r.schemas.FieldIndex(elem.Key)
	if idx < 0 {
		return nil, fmt.Errorf("cannot find the index for the field: %s", elem.Key)
	}

	// Create new reader
	fileName := colstore.AppendSecondaryIndexSuffix(r.file, "", index.MinMax, colstore.MinRowsForSeek)
	fullName := path.Join(r.path, fileName)
	// The same skipReader, with identical index files, is read only once.
	if v, ok := r.indexCache[fullName]; ok {
		return v, nil
	}

	f, err := colstore.NewPrimaryKeyReader(fullName, &r.path)

	if err != nil {
		return nil, err
	}
	defer f.Close()

	rec, _, err := f.ReadData()

	if err != nil {
		return nil, err
	}

	// Put into cache
	r.indexCache[fullName] = rec
	return rec, nil
}

func (r *MinMaxIndexReader) ReInit(file interface{}) (err error) {
	// Initialize readers with file path information
	var path, fileName string
	if f1, ok := file.(TsspFile); ok {
		index := strings.LastIndex(f1.Path(), "/")
		if index == -1 {
			path = "/"
		} else {
			path = f1.Path()[:index]
		}
		fileName = strings.Split(f1.Path()[index+1:], ".")[0]
	} else {
		return errors.New("file need be tssp type")
	}
	r.readers = NewMinMaxFilterReaders(path, fileName, r.schema, nil)

	return nil
}

func (m *MinMaxWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}
