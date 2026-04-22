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

package executor

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

//go:generate tmpl -data=@./tmpldata column.gen.go.tmpl

func init() {
	initColumnTypeFunc()
}

var (
	_ Chunk = (*ChunkImpl)(nil)
)

// Chunk consists of seven functionally distinct components, each responsible for
// managing metadata, tags, time, basic operations and serialization and Graph management.
type Chunk interface {
	ChunkMeta
	ChunkTag
	ChunkTime
	ChunkColumn
	ChunkOperator
	ChunkSerialization
	ChunkGraph
}

// ChunkMeta is data types and name interface.
type ChunkMeta interface {
	RowDataType() hybridqp.RowDataType
	SetRowDataType(hybridqp.RowDataType)
	CopyByRowDataType(c Chunk, fromRt hybridqp.RowDataType, dstRt hybridqp.RowDataType) error
	Name() string
	SetName(string)
}

// TagsManager is label-related interface.
type ChunkTag interface {
	Tags() []ChunkTags
	TagIndex() []int
	TagLen() int
	AppendTagsAndIndex(ChunkTags, int)
	AppendTagsAndIndexes([]ChunkTags, []int)
	ResetTagsAndIndexes(tags []ChunkTags, tagIndex []int)
}

// ChunkTime is a time window-related interface.
type ChunkTime interface {
	InitTimeWindow(minTime, maxTime, intervalTime int64, hasInterval, ascending bool, tag ChunkTags)
	Time() []int64
	TruncateTime(int)
	SetTime(time []int64)
	ResetTime(int, int64)
	AppendTime(...int64)
	AppendTimes([]int64)
	TimeByIndex(int) int64
	IntervalIndex() []int
	AppendIntervalIndexes([]int)
	AppendIntervalIndex(int)
	ResetIntervalIndex(...int)
	IntervalLen() int
}

// ChunkColumn is an interface related to columns and dimensions.
type ChunkColumn interface {
	Columns() []Column
	Column(int) Column
	Dims() []Column
	AddDims([]string)
	NewDims(size int)
	SetColumn(Column, int)
	AddColumn(...Column)
	Dim(int) Column
	AddDim(...Column)
}

// ChunkOperator is an interface related to data operations.
type ChunkOperator interface {
	IsNil() bool
	NumberOfRows() int
	NumberOfCols() int
	Len() int
	Reset()
	SlimChunk(ridIdx []int) Chunk
	Clone() Chunk
	CopyTo(Chunk)
	CheckChunk()
	GetRecord() *record.Record
	Append(ck Chunk, start, end int)
	String() string
}

// ChunkSerialization is an interface related to serialization and deserialization.
type ChunkSerialization interface {
	Marshal([]byte) ([]byte, error)
	Unmarshal([]byte) error
	Instance() transport.Codec
	Size() int
}

// ChunkGraph is a graph-related interface.
type ChunkGraph interface {
	GetGraph() IGraph
	SetGraph(g IGraph)
}

type ChunkWriter interface {
	Write(Chunk)
	Close()
}

// ChunkImpl DO NOT ADD ADDITIONAL FIELDS TO THIS STRUCT.
// Memory layout of ChunkImpl:
//
// +------------------------------------------------+
// | rowDataType: Data type of the row            |
// | - Description: Stores the data type of the row |
// | - Usage: Determines the type and structure of the data |
// +------------------------------------------------+
// | name: Measurement name                       |
// | - Description: Stores the name of the measurement |
// | - Usage: Identifies the measurement to which the data belongs |
// +------------------------------------------------+
// | tags: List of tag data                       |
// | - Description: Stores a list of tag data     |
// | - Usage: Manages tag information of the data  |
// +------------------------------------------------+
// | tagIndex: List of tag indices                |
// | - Description: Stores index information for tags |
// | - Usage:Quick access and locate tag data        |
// +------------------------------------------------+
// | time: List of timestamps                     |
// | - Description: Stores a list of timestamp data |
// | - Usage: Records and manages the time information of the data |
// +------------------------------------------------+
// | intervalIndex: List of interval indices      |
// | - Description: Stores a list of interval index data |
// | - Usage: Manages the time interval indices of the data |
// +------------------------------------------------+
// | columns: List of column data                 |
// | - Description: Stores a list of column data  |
// | - Usage: Manages the column information of the data |
// +------------------------------------------------+
// | dims: List of dimension columns              |
// | - Description: Stores a list of dimension column data |
// | - Usage: Manages the dimension information of the data |
// +------------------------------------------------+
// | Record: Record data                          |
// | - Description: Stores record data            |
// | - Usage: Manages the record information of the data |
// +------------------------------------------------+
// | graph: Graph data                            |
// | - Description: Stores graph data             |
// | - Usage: Manages the graph structure information of the data |
// +------------------------------------------------+
type ChunkImpl struct {
	rowDataType   hybridqp.RowDataType
	name          string
	tags          []ChunkTags
	tagIndex      []int
	time          []int64
	intervalIndex []int
	columns       []Column
	dims          []Column
	*record.Record
	graph IGraph
}

// NewChunkImpl FIXME: memory pool
func NewChunkImpl(rowDataType hybridqp.RowDataType, name string) *ChunkImpl {
	cb := &ChunkImpl{
		rowDataType: rowDataType,
		name:        name,
	}
	return cb
}

func (c *ChunkImpl) GetGraph() IGraph {
	return c.graph
}

func (c *ChunkImpl) SetGraph(g IGraph) {
	c.graph = g
}

func (c *ChunkImpl) NewDims(size int) {
	c.dims = make([]Column, 0, size)
	for i := 0; i < size; i++ {
		c.dims = append(c.dims, NewColumnImpl(influxql.String))
	}
}

func (c *ChunkImpl) AddDims(dimsVals []string) {
	for i, dimCol := range c.dims {
		dimCol.AppendStringValue(dimsVals[i])
		dimCol.AppendManyNotNil(1)
	}
}

func (c *ChunkImpl) Dims() []Column {
	return c.dims
}

func (c *ChunkImpl) Dim(i int) Column {
	return c.dims[i]
}

func (c *ChunkImpl) AddDim(cols ...Column) {
	c.dims = append(c.dims, cols...)
}

func (c *ChunkImpl) RowDataType() hybridqp.RowDataType {
	return c.rowDataType
}

func (c *ChunkImpl) SetRowDataType(r hybridqp.RowDataType) {
	c.rowDataType = r
}

func (c *ChunkImpl) Name() string {
	return c.name
}

func (c *ChunkImpl) SetName(name string) {
	c.name = name
}

func (c *ChunkImpl) Tags() []ChunkTags {
	return c.tags
}

func (c *ChunkImpl) TagIndex() []int {
	return c.tagIndex
}

func (c *ChunkImpl) TagLen() int {
	return len(c.tags)
}

func (c *ChunkImpl) AppendTagsAndIndex(tag ChunkTags, tagIndex int) {
	c.tags = append(c.tags, tag)
	c.tagIndex = append(c.tagIndex, tagIndex)
}

func (c *ChunkImpl) AppendTagsAndIndexes(tags []ChunkTags, tagIndex []int) {
	c.tags = append(c.tags, tags...)
	c.tagIndex = append(c.tagIndex, tagIndex...)
}

func (c *ChunkImpl) ResetTagsAndIndexes(tags []ChunkTags, tagIndex []int) {
	c.tags, c.tagIndex = c.tags[:0], c.tagIndex[:0]
	c.tags = append(c.tags, tags...)
	c.tagIndex = append(c.tagIndex, tagIndex...)
}

func (c *ChunkImpl) Time() []int64 {
	return c.time
}

func (c *ChunkImpl) TruncateTime(idx int) {
	c.time = c.time[:idx]
}

func (c *ChunkImpl) SetTime(time []int64) {
	c.time = time
}

func (c *ChunkImpl) ResetTime(idx int, time int64) {
	c.time[idx] = time
}

func (c *ChunkImpl) AppendTime(t ...int64) {
	c.time = append(c.time, t...)
}

// Note: This method will be deprecated; it is recommended to use AppendTime instead.
func (c *ChunkImpl) AppendTimes(ts []int64) {
	c.time = append(c.time, ts...)
}

func (c *ChunkImpl) TimeByIndex(i int) int64 {
	return c.time[i]
}

func (c *ChunkImpl) IntervalIndex() []int {
	return c.intervalIndex
}

func (c *ChunkImpl) AppendIntervalIndexes(intervalIndex []int) {
	c.intervalIndex = append(c.intervalIndex, intervalIndex...)
}

func (c *ChunkImpl) AppendIntervalIndex(intervalIndex int) {
	c.intervalIndex = append(c.intervalIndex, intervalIndex)
}

func (c *ChunkImpl) InitTimeWindow(minTime, maxTime, intervalTime int64, hasInterval, ascending bool, tag ChunkTags) {
	if !hasInterval {
		c.appendIntervalFullRows(0, intervalTime, 1, tag)
		return
	}
	num := (maxTime - minTime) / intervalTime
	if ascending {
		c.appendIntervalFullRows(minTime, intervalTime, int(num), tag)
	} else {
		c.appendIntervalFullRows(maxTime-intervalTime, -intervalTime, int(num), tag)
	}
}

func (c *ChunkImpl) appendIntervalFullRows(start, step int64, num int, tag ChunkTags) {
	// append time and interval index
	endLoc := len(c.time)
	for i := 0; i < num; i++ {
		c.time = append(c.time, start+step*int64(i))
		c.AppendIntervalIndex(endLoc + i)
	}
	// append data of column
	for i := 0; i < c.NumberOfCols(); i++ {
		switch c.rowDataType.Field(i).Expr.(*influxql.VarRef).Type {
		case influxql.Integer:
			c.Column(i).AppendIntegerValues(make([]int64, num))
		case influxql.Float:
			c.Column(i).AppendFloatValues(make([]float64, num))
		case influxql.Boolean:
			c.Column(i).AppendBooleanValues(make([]bool, num))
		default:
			panic("unsupported the data type for AppendIntervalFullRows")
		}
		c.Column(i).AppendManyNotNil(int(num))
	}
	// append tags and tagIndex
	c.AppendTagsAndIndex(tag, endLoc)
}

func (c *ChunkImpl) ResetIntervalIndex(intervalIndex ...int) {
	c.intervalIndex = c.intervalIndex[:0]
	c.intervalIndex = append(c.intervalIndex, intervalIndex...)
}

func (c *ChunkImpl) Columns() []Column {
	return c.columns
}

func (c *ChunkImpl) Column(i int) Column {
	return c.columns[i]
}

func (c *ChunkImpl) SetColumn(col Column, i int) {
	c.columns[i] = col
}

func (c *ChunkImpl) AddColumn(cols ...Column) {
	c.columns = append(c.columns, cols...)
}

// Deprecated: Do not use
func (c *ChunkImpl) IsNil() bool {
	if c == nil {
		return true
	}
	return len(c.time) == 0
}

func (c *ChunkImpl) NumberOfRows() int {
	if c == nil {
		return 0
	}
	return len(c.time)
}

func (c *ChunkImpl) NumberOfCols() int {
	if c == nil {
		return 0
	}
	return len(c.columns)
}

func (c *ChunkImpl) Len() int {
	return len(c.time)
}

func (c *ChunkImpl) Reset() {
	c.name = ""
	c.time = c.time[:0]
	c.tags = c.tags[:0]
	c.tagIndex = c.tagIndex[:0]
	c.intervalIndex = c.intervalIndex[:0]
	for i := range c.columns {
		c.Column(i).Reset()
	}
	for i := range c.dims {
		c.Dim(i).Reset()
	}
	c.graph = nil
}

// SlimChunk filter the ridIdx columns to slim chunk
func (c *ChunkImpl) SlimChunk(ridIdx []int) Chunk {
	for i, idx := range ridIdx {
		c.columns = append(c.columns[:idx-i], c.columns[idx-i+1:]...)
	}

	// TODO: rowDataType may reuse it
	rowDataType := hybridqp.RowDataTypeImpl{}
	c.rowDataType.CopyTo(&rowDataType)
	c.rowDataType = rowDataType.SlimFields(ridIdx)
	return c
}

func (c *ChunkImpl) IntervalLen() int {
	return len(c.intervalIndex)
}

func (c *ChunkImpl) GetRecord() *record.Record {
	return c.Record
}

func (c *ChunkImpl) copy(dst Chunk) Chunk {
	dst.SetGraph(c.graph)
	dst.AppendTagsAndIndexes(c.Tags(), c.TagIndex())
	dst.AppendIntervalIndexes(c.IntervalIndex())
	dst.AppendTimes(c.Time())

	for i := range dst.RowDataType().Fields() {
		dataType := dst.RowDataType().Field(i).Expr.(*influxql.VarRef).Type
		switch dataType {
		case influxql.Integer:
			dst.Column(i).AppendIntegerValues(c.Column(i).IntegerValues())
		case influxql.FloatTuple:
			dst.Column(i).AppendFloatTuples(c.Column(i).FloatTuples())
		case influxql.Float:
			dst.Column(i).AppendFloatValues(c.Column(i).FloatValues())
		case influxql.Boolean:
			dst.Column(i).AppendBooleanValues(c.Column(i).BooleanValues())
		case influxql.String, influxql.Tag:
			dst.Column(i).CloneStringValues(c.Column(i).GetStringBytes())
		}
		if len(c.Column(i).ColumnTimes()) != 0 {
			dst.Column(i).AppendColumnTimes(c.Column(i).ColumnTimes())
		}
		c.Column(i).NilsV2().CopyTo(dst.Column(i).NilsV2())
	}
	for i := range dst.Dims() {
		switch dst.Dim(i).DataType() {
		case influxql.Integer:
			dst.Dim(i).AppendIntegerValues(c.Dim(i).IntegerValues())
		case influxql.FloatTuple:
			dst.Dim(i).AppendFloatTuples(c.Dim(i).FloatTuples())
		case influxql.Float:
			dst.Dim(i).AppendFloatValues(c.Dim(i).FloatValues())
		case influxql.Boolean:
			dst.Dim(i).AppendBooleanValues(c.Dim(i).BooleanValues())
		case influxql.String, influxql.Tag:
			dst.Dim(i).CloneStringValues(c.Dim(i).GetStringBytes())
		}
		c.Dim(i).NilsV2().CopyTo(dst.Dim(i).NilsV2())
	}
	return dst
}

func (c *ChunkImpl) Clone() Chunk {
	clone := NewChunkBuilder(c.RowDataType()).NewChunk(c.Name())
	return c.copy(clone)
}

func (c *ChunkImpl) CopyTo(dstChunk Chunk) {
	dstChunk.SetName(c.Name())
	c.copy(dstChunk)
}

// Append used to append one chunk with the same number of columns and the same data type to other one
// based on the specified row interval.
func (c *ChunkImpl) Append(ck Chunk, start, end int) {
	c.time = append(c.time, ck.Time()[start:end]...)
	for i := range c.columns {
		vStart, vEnd := ck.Column(i).GetRangeValueIndexV2(start, end)
		switch c.columns[i].DataType() {
		case influxql.String, influxql.Tag:
			c.columns[i].AppendStringBytes(ck.Column(i).StringValuesWithOffset(vStart, vEnd, nil))
		case influxql.Float:
			c.columns[i].AppendFloatValues(ck.Column(i).FloatValues()[vStart:vEnd])
		case influxql.Integer:
			c.columns[i].AppendIntegerValues(ck.Column(i).IntegerValues()[vStart:vEnd])
		case influxql.Boolean:
			c.columns[i].AppendBooleanValues(ck.Column(i).BooleanValues()[vStart:vEnd])
		}
		for j := start; j < end; j++ {
			if ck.Column(i).IsNilV2(j) {
				c.columns[i].AppendNil()
			} else {
				c.columns[i].AppendNotNil()
			}
		}
	}
}

func (c *ChunkImpl) CopyByRowDataType(dst Chunk, fromRt hybridqp.RowDataType, dstRt hybridqp.RowDataType) error {
	dst.SetName(c.Name())
	dst.AppendTagsAndIndexes(c.Tags(), c.TagIndex())
	dst.AppendIntervalIndexes(c.IntervalIndex())
	dst.AppendTimes(c.Time())
	for k1, v1 := range fromRt.IndexByName() {
		if v2, ok := dstRt.IndexByName()[k1]; !ok {
			return fmt.Errorf("CopyByRowDataType fromRt and dstRt not match")
		} else {
			dataType := dst.Column(v2).DataType()
			switch dataType {
			case influxql.Integer:
				dst.Column(v2).AppendIntegerValues(c.Column(v1).IntegerValues())
			case influxql.FloatTuple:
				dst.Column(v2).AppendFloatTuples(c.Column(v1).FloatTuples())
			case influxql.Float:
				dst.Column(v2).AppendFloatValues(c.Column(v1).FloatValues())
			case influxql.Boolean:
				dst.Column(v2).AppendBooleanValues(c.Column(v1).BooleanValues())
			case influxql.String, influxql.Tag:
				dst.Column(v2).CloneStringValues(c.Column(v1).GetStringBytes())
			}
			if len(c.Column(v1).ColumnTimes()) != 0 {
				dst.Column(v2).AppendColumnTimes(c.Column(v1).ColumnTimes())
			}
			c.Column(v1).NilsV2().CopyTo(dst.Column(v2).NilsV2())
		}
	}
	return nil
}

func (c *ChunkImpl) CheckChunk() {
	if len(c.tagIndex) == 0 {
		panic("tagIndex at least one parameter:0!")
	}
	if len(c.intervalIndex) == 0 {
		panic("intervalIndex at least one parameter:0!")
	}
	m1 := make(map[int]int)
	m2 := make(map[int]int)
	for _, i := range c.intervalIndex {
		if _, ok := m1[i]; !ok {
			m1[i] = 1
		} else {
			panic("intervalIndex should be distinct, but it has same values!")
		}
	}
	for _, i := range c.tagIndex {
		if _, ok := m1[i]; !ok {
			panic("intervalIndex doesn't fit tagIndex: the tagIndex should be the subset of intervalIndex")
		}
		if _, ok := m2[i]; !ok {
			m2[i] = 1
		} else {
			panic("TagIndex should be distinct, but it has same values!")
		}
	}
	if len(c.tags) != len(c.tagIndex) {
		panic("chunk check failed: tagIndexes doesn't fit tags!")
	}
	for i := 0; i < len(c.tagIndex)-1; i++ {
		if c.tagIndex[i] >= c.tagIndex[i+1] {
			panic("chunk check failed: tagIndex out of order!")
		}
	}
	for _, col := range c.columns {
		col.CheckColumn(c.NumberOfRows())
	}

	for _, col := range c.dims {
		col.CheckColumn(c.NumberOfRows())
	}
}

func (c *ChunkImpl) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("name: %v\n", c.name))
	var tags []string
	for _, tag := range c.Tags() {
		tags = append(tags, string(tag.Subset(nil)))
	}
	buffer.WriteString(fmt.Sprintf("tag: %v\n", strings.Join(tags, "\t")))
	const padding = 1
	w := tabwriter.NewWriter(&buffer, 0, 0, padding, ' ', tabwriter.AlignRight|tabwriter.Debug)
	columns := []string{"time"}
	columns = append(columns, c.rowDataType.Fields().Names()...)
	_, err := fmt.Fprintln(w, strings.Join(columns, "\t"))
	if err != nil {
		return "raises error to println"
	}
	for l, t := range c.Time() {
		var line []string
		line = append(line, strconv.FormatInt(t, 10))
		for i := range c.Columns() {
			if c.Column(i).IsNilV2(l) {
				line = append(line, " ")
				continue
			}
			l = c.Column(i).GetValueIndexV2(l)
			switch c.Column(i).DataType() {
			case influxql.Integer:
				line = append(line, strconv.FormatInt(c.Column(i).IntegerValue(l), 10))
			case influxql.Float:
				line = append(line, strconv.FormatFloat(c.Column(i).FloatValue(l), 'f', -1, 64))
			case influxql.Boolean:
				line = append(line, strconv.FormatBool(c.Column(i).BooleanValue(l)))
			case influxql.String, influxql.Tag:
				line = append(line, c.Column(i).StringValue(l))
			}
		}
		_, err = fmt.Fprintln(w, strings.Join(line, "\t"))
		if err != nil {
			return "raises error to println"
		}
	}
	err = w.Flush()
	if err != nil {
		return "raises error to flush"
	}
	return buffer.String()
}

func IntervalIndexGen(ck Chunk, opt *query.ProcessorOptions) {
	chunk, ok := ck.(*ChunkImpl)
	if !ok {
		return
	}

	tagIndex := chunk.TagIndex()
	if opt.Interval.IsZero() {
		chunk.AppendIntervalIndexes(tagIndex)
		return
	}

	windowStopTime := influxql.MinTime
	ascending := opt.Ascending
	if ascending {
		windowStopTime = influxql.MaxTime
	}

	times := chunk.Time()
	tagIndexOffset := 0
	stopTime := opt.StopTime()

	for i := range times {
		// init first time stop window
		if windowStopTime == influxql.MaxTime {
			_, windowStopTime = opt.Window(times[i])
		} else if windowStopTime == influxql.MinTime {
			windowStopTime, _ = opt.Window(times[i])
		}
		if tagIndexOffset < len(tagIndex) && i == tagIndex[tagIndexOffset] {
			chunk.AppendIntervalIndex(i)
			if ascending {
				_, windowStopTime = opt.Window(times[i])
			} else {
				windowStopTime, _ = opt.Window(times[i])
			}

			tagIndexOffset++
			continue
		}

		if ascending && times[i] >= windowStopTime && times[i] <= stopTime {
			_, windowStopTime = opt.Window(times[i])
			chunk.AppendIntervalIndex(i)
		} else if !ascending && times[i] < windowStopTime && times[i] >= stopTime {
			windowStopTime, _ = opt.Window(times[i])
			chunk.AppendIntervalIndex(i)
		}
	}
}

func getBlankRowIdx(oriArr []uint16, timeLen int) []uint16 {
	if len(oriArr) == 0 {
		dst := make([]uint16, timeLen)
		for i := 0; i < timeLen; i++ {
			dst[i] = uint16(i)
		}
		return dst
	}
	var dst []uint16
	var idx uint16
	for i, v := range oriArr {
		if i == 0 {
			for idx < v {
				dst = append(dst, idx)
				idx++
			}
		} else {
			for v > idx && idx > oriArr[i-1] {
				dst = append(dst, idx)
				idx++
			}
		}
		idx++
	}

	idx = oriArr[len(oriArr)-1] + 1
	for int(idx) < timeLen {
		dst = append(dst, idx)
		idx++
	}
	return dst
}

func UnionColumns(cols ...Column) []uint16 {
	ori := make([]uint16, 0, cols[0].Length())
	for i := range cols {
		ori = UnionBitMapArray(cols[i].BitMap().array, ori)
	}

	blankRowIdx := getBlankRowIdx(ori, cols[0].Length())
	// fast path
	if len(blankRowIdx) == 0 || len(blankRowIdx) == cols[0].Length() {
		return blankRowIdx
	}
	// slow path to update the bitmap of each column
	nils := make([]bool, cols[0].Length()-len(blankRowIdx))
	for _, col := range cols {
		bit := col.BitMap()
		if len(bit.array) == 0 {
			bit.Clear()
			col.AppendManyNil(bit.length - len(blankRowIdx))
			continue
		}
		bit.length -= len(blankRowIdx)
		bit.nilCount -= len(blankRowIdx)
		var j int
		for i, b := range blankRowIdx {
			r := b - uint16(i)
			for r > bit.array[j] && j < len(bit.array) {
				j++
			}
			if j >= len(bit.array) {
				break
			}
			if r < bit.array[j] {
				for s := j; s < len(bit.array); s++ {
					bit.array[s]--
				}
			}
		}
		bit.UpdateBitWithArray(nils)
	}
	return blankRowIdx
}

var GetColValsFn map[influxql.DataType]func(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{}

func initColumnTypeFunc() {
	GetColValsFn = make(map[influxql.DataType]func(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{}, 5)

	GetColValsFn[influxql.Float] = initFloatColumnFunc

	GetColValsFn[influxql.Integer] = initIntegerColumnFunc

	GetColValsFn[influxql.Boolean] = initBooleanColumnFunc

	GetColValsFn[influxql.String] = initStringColumnFunc

	GetColValsFn[influxql.Tag] = initStringColumnFunc
}

func initFloatColumnFunc(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{} {
	// fast path
	if col.NilCount() == 0 {
		values := col.FloatValues()[bmStart:bmEnd]
		for _, v := range values {
			dst = append(dst, v)
		}
		return dst
	}

	// slow path
	for j := bmStart; j < bmEnd; j++ {
		if col.IsNilV2(j) {
			dst = append(dst, nil)
		} else {
			dst = append(dst, col.FloatValue(col.GetValueIndexV2(j)))
		}
	}
	return dst
}

func initIntegerColumnFunc(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{} {
	// fast path
	if col.NilCount() == 0 {
		values := col.IntegerValues()[bmStart:bmEnd]
		for _, v := range values {
			dst = append(dst, v)
		}
		return dst
	}

	// slow path
	for j := bmStart; j < bmEnd; j++ {
		if col.IsNilV2(j) {
			dst = append(dst, nil)
		} else {
			dst = append(dst, col.IntegerValue(col.GetValueIndexV2(j)))
		}
	}
	return dst
}

func initBooleanColumnFunc(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{} {
	// fast path
	if col.NilCount() == 0 {
		values := col.BooleanValues()[bmStart:bmEnd]
		for _, v := range values {
			dst = append(dst, v)
		}
		return dst
	}

	// slow path
	for j := bmStart; j < bmEnd; j++ {
		if col.IsNilV2(j) {
			dst = append(dst, nil)
		} else {
			dst = append(dst, col.BooleanValue(col.GetValueIndexV2(j)))
		}
	}
	return dst
}

func initStringColumnFunc(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{} {
	// fast path
	if col.NilCount() == 0 {
		for i := bmStart; i < bmEnd; i++ {
			oriStr := col.StringValue(i)
			newStr := make([]byte, len(oriStr))
			copy(newStr, oriStr)
			dst = append(dst, util.Bytes2str(newStr))
		}
		return dst
	}

	// slow path
	for j := bmStart; j < bmEnd; j++ {
		if col.IsNilV2(j) {
			dst = append(dst, nil)
		} else {
			oriStr := col.StringValue(col.GetValueIndexV2(j))
			newStr := make([]byte, len(oriStr))
			copy(newStr, oriStr)
			dst = append(dst, util.Bytes2str(newStr))
		}
	}
	return dst
}
