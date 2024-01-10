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
	"sync"
	"text/tabwriter"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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

func initTagColumnFunc(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{} {
	return initStringColumnFunc(col, bmStart, bmEnd, ckLen, dst)
}

func initColumnTypeFunc() {
	GetColValsFn = make(map[influxql.DataType]func(col Column, bmStart, bmEnd int, ckLen int, dst []interface{}) []interface{}, 5)

	GetColValsFn[influxql.Float] = initFloatColumnFunc

	GetColValsFn[influxql.Integer] = initIntegerColumnFunc

	GetColValsFn[influxql.Boolean] = initBooleanColumnFunc

	GetColValsFn[influxql.String] = initStringColumnFunc

	GetColValsFn[influxql.Tag] = initTagColumnFunc
}

type PointRowIterator interface {
	GetNext(row *influx.Row, tuple *TargetTuple)
	HasMore() bool
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
	InitTimeWindow(minTime, maxTime, intervalTime int64, hasInterval, ascending bool, tag ChunkTags)
	Time() []int64
	TruncateTime(int)
	SetTime(time []int64)
	ResetTime(int, int64)
	AppendTime(int64)
	AppendTimes([]int64)
	TimeByIndex(int) int64
	IntervalIndex() []int
	AppendIntervalIndexes([]int)
	AppendIntervalIndex(int)
	ResetIntervalIndex(...int)
	Columns() []Column
	Column(int) Column
	Dims() []Column
	AddDims([]string)
	NewDims(size int)
	SetColumn(Column, int)
	AddColumn(...Column)
	Dim(int) Column
	SetDim(Column, int)
	AddDim(...Column)
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
	CopyTo(Chunk)
	// CheckChunk TODO: CheckChunk used to check the chunk's structure
	CheckChunk()
	GetRecord() *record.Record
	String() string

	Marshal([]byte) ([]byte, error)
	Unmarshal([]byte) error
	Instance() transport.Codec
	Size() int

	CreatePointRowIterator(string, *FieldsValuer) PointRowIterator

	CopyByRowDataType(c Chunk, fromRt hybridqp.RowDataType, dstRt hybridqp.RowDataType) error
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
	dims          []Column
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

func (c *ChunkImpl) SetDim(col Column, i int) {
	c.dims[i] = col
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

func (c *ChunkImpl) AppendTime(t int64) {
	c.time = append(c.time, t)
}

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
		c.AppendIntervalFullRows(0, intervalTime, 1, tag)
		return
	}
	num := (maxTime - minTime) / intervalTime
	if ascending {
		c.AppendIntervalFullRows(minTime, intervalTime, int(num), tag)
	} else {
		c.AppendIntervalFullRows(maxTime-intervalTime, -intervalTime, int(num), tag)
	}
}

func (c *ChunkImpl) AppendIntervalFullRows(start, step int64, num int, tag ChunkTags) {
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
	for i := range c.dims {
		c.Dim(i).Reset()
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

func (c *ChunkImpl) GetRecord() *record.Record {
	return c.Record
}

func (c *ChunkImpl) copy(dst Chunk) Chunk {
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
		col.CheckColumn(c.Len())
	}

	for _, col := range c.dims {
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

func (c *ChunkImpl) CreatePointRowIterator(name string, valuer *FieldsValuer) PointRowIterator {
	return NewChunkIteratorFromValuer(c, name, valuer)
}

type IntegerFieldValuer struct {
	key string
	typ int32
}

func (valuer *IntegerFieldValuer) At(col Column, pos int, field *influx.Field) bool {
	if col.IsNilV2(pos) {
		return false
	}

	valueIndex := col.GetValueIndexV2(pos)

	field.Key = valuer.key
	field.Type = valuer.typ
	field.NumValue = float64(col.IntegerValue(valueIndex))
	return true
}

type FieldValuer interface {
	At(Column, int, *influx.Field) bool
}

type FloatFieldValuer struct {
	key string
	typ int32
}

func (valuer *FloatFieldValuer) At(col Column, pos int, field *influx.Field) bool {
	if col.IsNilV2(pos) {
		return false
	}

	valueIndex := col.GetValueIndexV2(pos)

	field.Key = valuer.key
	field.Type = valuer.typ
	field.NumValue = col.FloatValue(valueIndex)
	return true
}

type StringFieldValuer struct {
	key string
	typ int32
}

func (valuer *StringFieldValuer) At(col Column, pos int, field *influx.Field) bool {
	if col.IsNilV2(pos) {
		return false
	}

	valueIndex := col.GetValueIndexV2(pos)

	field.Key = valuer.key
	field.Type = valuer.typ
	field.StrValue = col.StringValue(valueIndex)
	return true
}

type BooleanFieldValuer struct {
	key string
	typ int32
}

func (valuer *BooleanFieldValuer) At(col Column, pos int, field *influx.Field) bool {
	if col.IsNilV2(pos) {
		return false
	}

	valueIndex := col.GetValueIndexV2(pos)

	field.Key = valuer.key
	field.Type = valuer.typ
	field.StrValue = strconv.FormatBool(col.BooleanValue(valueIndex))
	return true
}

func NewFieldValuer(ref *influxql.VarRef) (FieldValuer, error) {
	switch ref.Type {
	case influxql.Integer:
		valuer := &IntegerFieldValuer{}
		valuer.key = ref.Val
		valuer.typ = influx.Field_Type_Int
		return valuer, nil
	case influxql.Float:
		valuer := &FloatFieldValuer{}
		valuer.key = ref.Val
		valuer.typ = influx.Field_Type_Float
		return valuer, nil
	case influxql.Tag:
		fallthrough
	case influxql.String:
		valuer := &StringFieldValuer{}
		valuer.key = ref.Val
		valuer.typ = influx.Field_Type_String
		return valuer, nil
	case influxql.Boolean:
		valuer := &BooleanFieldValuer{}
		valuer.key = ref.Val
		valuer.typ = influx.Field_Type_Boolean
		return valuer, nil
	default:
		return nil, fmt.Errorf("create field valuer on an unsupport type (%v)", ref.Type)
	}
}

type FieldsValuer struct {
	fvs []FieldValuer
}

func NewFieldsValuer(rdt hybridqp.RowDataType) (*FieldsValuer, error) {
	valuer := &FieldsValuer{
		fvs: make([]FieldValuer, rdt.NumColumn()),
	}

	for i, f := range rdt.Fields() {
		fv, err := NewFieldValuer(f.Expr.(*influxql.VarRef))
		if err != nil {
			return nil, err
		}
		valuer.fvs[i] = fv
	}
	return valuer, nil
}

func (valuer *FieldsValuer) At(chunk Chunk, pos int, tuple *TargetTuple) {
	for i, fv := range valuer.fvs {
		hasValue := fv.At(chunk.Column(i), pos, tuple.Allocate())
		if hasValue {
			tuple.Commit()
		}
	}
}

type TargetTuple struct {
	fields []influx.Field
	len    int
	cap    int
}

func NewTargetTuple(cap int) *TargetTuple {
	return &TargetTuple{
		fields: make([]influx.Field, cap),
		len:    0,
		cap:    cap,
	}
}

func (t *TargetTuple) Reset() {
	for _, field := range t.fields {
		field.Reset()
	}
	t.len = 0
}

func (t *TargetTuple) CheckAndAllocate() (*influx.Field, bool) {
	if t.len >= t.cap {
		return nil, false
	}
	return &t.fields[t.len], true
}

func (t *TargetTuple) Allocate() *influx.Field {
	return &t.fields[t.len]
}

func (t *TargetTuple) Commit() {
	t.len++
}

func (t *TargetTuple) Active() []influx.Field {
	return t.fields[0:t.len]
}

func (t *TargetTuple) Len() int {
	return t.len
}

// BatchRows is not thread safe. It can not be used in multi-threading model.
type TargetTable struct {
	rows     []influx.Row
	tuples   []*TargetTuple
	len      int
	cap      int
	tupleCap int
}

func NewTargetTable(rowCap int, tupleCap int) *TargetTable {
	tt := &TargetTable{
		rows:     make([]influx.Row, rowCap),
		tuples:   make([]*TargetTuple, rowCap),
		len:      0,
		cap:      rowCap,
		tupleCap: tupleCap,
	}

	tt.initTuples(tt.tuples)

	return tt
}

func (tt *TargetTable) initTuples(tuples []*TargetTuple) {
	for i := range tuples {
		tuples[i] = NewTargetTuple(tt.tupleCap)
	}
}

func (tt *TargetTable) Reset() {
	for i := range tt.rows {
		tt.rows[i].Reset()
	}

	for _, tuple := range tt.tuples {
		tuple.Reset()
	}

	tt.len = 0
}

func (tt *TargetTable) CheckAndAllocate() (*influx.Row, *TargetTuple, bool) {
	if tt.len >= tt.cap {
		return nil, nil, false
	}
	return &tt.rows[tt.len], tt.tuples[tt.len], true
}

func (tt *TargetTable) Allocate() (*influx.Row, *TargetTuple) {
	if tt.len >= tt.cap {
		tt.cap = tt.cap * 2

		rows := make([]influx.Row, tt.cap)
		copy(rows, tt.rows)
		tt.rows = rows

		tuples := make([]*TargetTuple, tt.cap)
		tt.initTuples(tuples[tt.len:])
		copy(tuples, tt.tuples)
		tt.tuples = tuples
	}

	tt.rows[tt.len].Reset()
	tt.tuples[tt.len].Reset()
	return &tt.rows[tt.len], tt.tuples[tt.len]
}

func (tt *TargetTable) Commit() {
	tt.len++
}

func (tt *TargetTable) Active() []influx.Row {
	for i := 0; i < tt.len; i++ {
		tt.rows[i].Fields = tt.tuples[i].Active()
	}
	return tt.rows[0:tt.len]
}

type TargetTablePool struct {
	sp       sync.Pool
	rowCap   int
	tupleCap int
}

func NewTargetTablePool(rowCap int, tupleCap int) *TargetTablePool {
	p := &TargetTablePool{
		sp: sync.Pool{
			New: func() interface{} {
				brs := NewTargetTable(rowCap, tupleCap)
				return brs
			},
		},
		rowCap:   rowCap,
		tupleCap: tupleCap,
	}

	return p
}

func (p *TargetTablePool) Get() *TargetTable {
	table, ok := p.sp.Get().(*TargetTable)
	if !ok {
		panic(fmt.Sprintf("%v is not a TargetTable type", table))
	}
	table.Reset()
	return table
}

func (p *TargetTablePool) Put(table *TargetTable) {
	p.sp.Put(table)
}

type ChunkIterator struct {
	chunk       *ChunkImpl
	name        string
	currTags    influx.PointTags
	currTagsPos int
	nextPos     int
	currPos     int
	valuer      *FieldsValuer
}

func NewChunkIteratorFromValuer(chunk *ChunkImpl, name string, valuer *FieldsValuer) *ChunkIterator {
	iter := &ChunkIterator{
		chunk:       chunk,
		name:        name,
		currTags:    nil,
		currTagsPos: -1,
		currPos:     0,
		valuer:      valuer,
	}

	if iter.currTagsPos+1 < len(iter.chunk.tags) {
		iter.nextPos = iter.chunk.tagIndex[iter.currTagsPos+1]
	}

	return iter
}

func (iter *ChunkIterator) advance() {
	iter.currTagsPos++
	iter.currTags = iter.chunk.tags[iter.currTagsPos].PointTags()
	if iter.currTagsPos+1 < len(iter.chunk.tags) {
		iter.nextPos = iter.chunk.tagIndex[iter.currTagsPos+1]
	}
}

func (iter *ChunkIterator) GetNext(row *influx.Row, tuple *TargetTuple) {
	if iter.currPos == iter.nextPos {
		iter.advance()
	}

	row.Name = iter.name
	row.Tags = iter.currTags
	iter.valuer.At(iter.chunk, iter.currPos, tuple)
	row.Timestamp = iter.chunk.time[iter.currPos]

	iter.currPos++
}

func (iter *ChunkIterator) HasMore() bool {
	return iter.currPos < iter.chunk.Len()
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

var (
	_ Chunk = (*ChunkImpl)(nil)
)
