// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"strconv"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	RowSizeBytes   int = 24 // Estimated size of each row
	TupleSizeBytes int = 16 // Estimated size of each tuple
)

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
		return nil, fmt.Errorf("create field valuer on an unsupported type (%v)", ref.Type)
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
	rowLen   int
	rowCap   int
	tupleCap int
}

func NewTargetTable(rowCap int, tupleCap int) *TargetTable {
	tt := &TargetTable{
		rows:     make([]influx.Row, rowCap),
		tuples:   make([]*TargetTuple, rowCap),
		rowLen:   0,
		rowCap:   rowCap,
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

	tt.rowLen = 0
}

func (tt *TargetTable) CheckAndAllocate() (*influx.Row, *TargetTuple, bool) {
	if tt.rowLen >= tt.rowCap {
		return nil, nil, false
	}
	return &tt.rows[tt.rowLen], tt.tuples[tt.rowLen], true
}

func (tt *TargetTable) Allocate() (*influx.Row, *TargetTuple) {
	if tt.rowLen >= tt.rowCap {
		tt.rowCap = tt.rowCap * 2

		rows := make([]influx.Row, tt.rowCap)
		copy(rows, tt.rows)
		tt.rows = rows

		tuples := make([]*TargetTuple, tt.rowCap)
		tt.initTuples(tuples[tt.rowLen:])
		copy(tuples, tt.tuples)
		tt.tuples = tuples
	}

	tt.rows[tt.rowLen].Reset()
	tt.tuples[tt.rowLen].Reset()
	return &tt.rows[tt.rowLen], tt.tuples[tt.rowLen]
}

func (tt *TargetTable) Commit() {
	tt.rowLen++
}

func (tt *TargetTable) Active() []influx.Row {
	for i := 0; i < tt.rowLen; i++ {
		tt.rows[i].Fields = tt.tuples[i].Active()
	}
	return tt.rows[0:tt.rowLen]
}

func (tt *TargetTable) MemSize() int {
	return util.Int64SizeBytes*3 + len(tt.rows)*RowSizeBytes + len(tt.tuples)*TupleSizeBytes
}

func (tt *TargetTable) Instance() *TargetTable {
	return tt
}

type TargetTablePool struct {
	sp       *pool.UnionPool[TargetTable]
	rowCap   int
	tupleCap int
}

func NewTargetTablePool(rowCap int, tupleCap int) *TargetTablePool {
	p := &TargetTablePool{
		sp:       pool.NewDefaultUnionPool[TargetTable](func() *TargetTable { return NewTargetTable(rowCap, tupleCap) }),
		rowCap:   rowCap,
		tupleCap: tupleCap,
	}
	return p
}

func (p *TargetTablePool) Get() *TargetTable {
	table := p.sp.Get()
	table.Reset()
	return table
}

func (p *TargetTablePool) Put(table *TargetTable) {
	p.sp.Put(table)
}

type PointRowIterator interface {
	GetNext(row *influx.Row, tuple *TargetTuple)
	HasMore() bool
}

type ChunkIterator struct {
	chunk       Chunk
	name        string
	currTags    influx.PointTags
	currTagsPos int
	nextPos     int
	currPos     int
	valuer      *FieldsValuer
}

func NewChunkIteratorFromValuer(chunk Chunk, name string, valuer *FieldsValuer) *ChunkIterator {
	iter := &ChunkIterator{
		chunk:       chunk,
		name:        name,
		currTags:    nil,
		currTagsPos: -1,
		currPos:     0,
		valuer:      valuer,
	}

	if iter.currTagsPos+1 < len(iter.chunk.Tags()) {
		iter.nextPos = iter.chunk.TagIndex()[iter.currTagsPos+1]
	}

	return iter
}

func (iter *ChunkIterator) advance() {
	iter.currTagsPos++
	iter.currTags = iter.chunk.Tags()[iter.currTagsPos].PointTags()
	if iter.currTagsPos+1 < len(iter.chunk.Tags()) {
		iter.nextPos = iter.chunk.TagIndex()[iter.currTagsPos+1]
	}
}

func (iter *ChunkIterator) GetNext(row *influx.Row, tuple *TargetTuple) {
	if iter.currPos == iter.nextPos {
		iter.advance()
	}

	row.Name = iter.name
	row.Tags = iter.currTags
	iter.valuer.At(iter.chunk, iter.currPos, tuple)
	row.Timestamp = iter.chunk.TimeByIndex(iter.currPos)

	iter.currPos++
}

func (iter *ChunkIterator) HasMore() bool {
	return iter.currPos < iter.chunk.NumberOfRows()
}
