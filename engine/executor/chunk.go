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
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

//go:generate tmpl -data=@./tmpldata column.gen.go.tmpl

func init() {
	initColumnTypeFunc()
}

var GetColValsFn map[influxql.DataType]func(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{}

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
			dst = append(dst, record.Bytes2str(newStr))
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
			dst = append(dst, record.Bytes2str(newStr))
		}
	}
	return dst
}

func initTagColumnFunc(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{} {
	// fast path
	if col.NilCount() == 0 {
		for i := bmStart; i < bmEnd; i++ {
			oriStr := col.StringValue(i)
			newStr := make([]byte, len(oriStr))
			copy(newStr, oriStr)
			dst = append(dst, record.Bytes2str(newStr))
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
			dst = append(dst, record.Bytes2str(newStr))
		}
	}
	return dst
}

func initColumnTypeFunc() {
	GetColValsFn = make(map[influxql.DataType]func(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{}, 5)

	GetColValsFn[influxql.Float] = initFloatColumnFunc

	GetColValsFn[influxql.Integer] = initIntegerColumnFunc

	GetColValsFn[influxql.Boolean] = initBooleanColumnFunc

	GetColValsFn[influxql.String] = initStringColumnFunc

	GetColValsFn[influxql.Tag] = initTagColumnFunc
}

type Chunk interface {
	RowDataType() hybridqp.RowDataType
	SetRowDataType(hybridqp.RowDataType)
	Name() string
	SetName(string)
	Tags() []ChunkTags
	TagIndex() []int
	TagLen() int
	AppendTagsAndIndex(ChunkTags, int)
	AppendTagsAndIndexes([]ChunkTags, []int)
	Time() []int64
	TruncateTime(int)
	SetTime(time []int64)
	ResetTime(int, int64)
	AppendTime(...int64)
	TimeByIndex(int) int64
	IntervalIndex() []int
	AppendIntervalIndex(...int)
	ResetIntervalIndex(...int)
	Columns() []Column
	Column(int) Column
	SetColumn(Column, int)
	AddColumn(...Column)
	IsNil() bool
	NumberOfRows() int
	NumberOfCols() int
	Release()
	Len() int
	Reset()
	SlimChunk(ridIdx []int) Chunk
	IntervalLen() int
	AddTagAndIndex(tag ChunkTags, i int)
	ResetTagsAndIndexes(tags []ChunkTags, tagIndex []int)
	AddIntervalIndex(i int)
	Clone() Chunk
	// CheckChunk TODO: CheckChunk used to check the chunk's structure
	CheckChunk()
	String() string

	Marshal([]byte) ([]byte, error)
	Unmarshal([]byte) error
	Instance() transport.Codec
	Size() int
}

type ChunkWriter interface {
	Write(Chunk)
	Close()
}

// ChunkImpl DO NOT ADD ADDITIONAL FIELDS TO THIS STRUCT.
type ChunkImpl struct {
	rowDataType   hybridqp.RowDataType
	name          string
	tags          []ChunkTags
	tagIndex      []int
	time          []int64
	intervalIndex []int
	columns       []Column
	*record.Record
}

// NewChunkImpl FIXME: memory pool
func NewChunkImpl(rowDataType hybridqp.RowDataType, name string) *ChunkImpl {
	cb := &ChunkImpl{
		rowDataType: rowDataType,
		name:        name,
	}
	return cb
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

func (c *ChunkImpl) SetColumn(column Column, i int) {
	c.columns[i] = column
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

func (c *ChunkImpl) TimeByIndex(i int) int64 {
	return c.time[i]
}

func (c *ChunkImpl) IntervalIndex() []int {
	return c.intervalIndex
}

func (c *ChunkImpl) AppendIntervalIndex(intervalIndex ...int) {
	c.intervalIndex = append(c.intervalIndex, intervalIndex...)
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

func (c *ChunkImpl) Release() {

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

func (c *ChunkImpl) AddTagAndIndex(tag ChunkTags, i int) {
	c.tags = append(c.tags, tag)
	c.tagIndex = append(c.tagIndex, i)
}

func (c *ChunkImpl) AddIntervalIndex(i int) {
	c.intervalIndex = append(c.intervalIndex, i)
}

func (c *ChunkImpl) Clone() Chunk {
	clone := NewChunkBuilder(c.RowDataType()).NewChunk(c.Name())
	clone.AppendTagsAndIndexes(c.Tags(), c.TagIndex())
	clone.AppendIntervalIndex(c.IntervalIndex()...)
	clone.AppendTime(c.Time()...)

	for i := range clone.RowDataType().Fields() {
		dataType := clone.RowDataType().Field(i).Expr.(*influxql.VarRef).Type
		switch dataType {
		case influxql.Integer:
			clone.Column(i).AppendIntegerValues(c.Column(i).IntegerValues()...)
		case influxql.Float:
			clone.Column(i).AppendFloatValues(c.Column(i).FloatValues()...)
		case influxql.Boolean:
			clone.Column(i).AppendBooleanValues(c.Column(i).BooleanValues()...)
		case influxql.String, influxql.Tag:
			clone.Column(i).CloneStringValues(c.Column(i).GetStringBytes())
		}
		if len(c.Column(i).ColumnTimes()) != 0 {
			clone.Column(i).AppendColumnTimes(c.Column(i).ColumnTimes()...)
		}
		c.Column(i).NilsV2().CopyTo(clone.Column(i).NilsV2())
	}
	return clone
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
		col.CheckColumn(c.Len())
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

func IntervalIndexGen(chunk Chunk, opt query.ProcessorOptions) {
	var windowStopTime int64
	if opt.Ascending {
		windowStopTime = influxql.MaxTime
	} else {
		windowStopTime = influxql.MinTime
	}

	times := chunk.Time()
	tagIndexOffset := 0
	if !opt.Interval.IsZero() {
		for i := range times {
			// init first time stop window
			if windowStopTime == influxql.MaxTime {
				_, windowStopTime = opt.Window(times[i])
			} else if windowStopTime == influxql.MinTime {
				windowStopTime, _ = opt.Window(times[i])
			}
			if tagIndexOffset < len(chunk.TagIndex()) && i == chunk.TagIndex()[tagIndexOffset] {
				chunk.AppendIntervalIndex(i)
				_, windowStopTime = opt.Window(times[i])
				tagIndexOffset++
				continue
			}
			if opt.Ascending && times[i] >= windowStopTime && times[i] <= opt.StopTime() {
				_, windowStopTime = opt.Window(times[i])
				chunk.AppendIntervalIndex(i)
			} else if !opt.Ascending && times[i] < windowStopTime && times[i] >= opt.StopTime() {
				windowStopTime, _ = opt.Window(times[i])
				chunk.AppendIntervalIndex(i)
			}
		}
	} else {
		chunk.AppendIntervalIndex(chunk.TagIndex()...)
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

var (
	_ Chunk = (*ChunkImpl)(nil)
)
