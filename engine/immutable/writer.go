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

package immutable

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sync"
	"unsafe"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	minIndex = iota
	maxIndex
	minTIndex
	maxTIndex
	sumIndex
	countIndex
)

type PreAggBuilder interface {
	min() (interface{}, int64)
	max() (interface{}, int64)
	sum() interface{}
	count() int64
	marshal(dst []byte) []byte
	unmarshal(src []byte) ([]byte, error)
	addValues(values *record.ColVal, times []int64)
	size() int
	reset()
	release()
	addMin(float64, int64)
	addMax(float64, int64)
	addSum(float64)
	addCount(n int64)
}

var (
	integerPreAggPool = sync.Pool{}
	floatPreAggPool   = sync.Pool{}
	boolPreAggPool    = sync.Pool{}
	stringPreAggPool  = sync.Pool{}
	timePreAggPool    = sync.Pool{}

	MinMaxTimeLen    = int(unsafe.Sizeof(SegmentRange{}))
	SegmentLen       = (Segment{}).bytes()
	ColumnMetaLenMin = (ColumnMeta{}).bytes(1)
	ChunkMetaLen     = int(unsafe.Sizeof(ChunkMeta{})-24*2) + MinMaxTimeLen
	ChunkMetaMinLen  = ChunkMetaLen + ColumnMetaLenMin*2
	MetaIndexLen     = int(unsafe.Sizeof(MetaIndex{}))
)

type PreAggBuilders struct {
	intBuilder    PreAggBuilder
	floatBuilder  PreAggBuilder
	stringBuilder PreAggBuilder
	boolBuilder   PreAggBuilder
	timeBuilder   PreAggBuilder
}

func newPreAggBuilders() *PreAggBuilders {
	b := &PreAggBuilders{
		intBuilder:    acquireColumnBuilder(influx.Field_Type_Int),
		floatBuilder:  acquireColumnBuilder(influx.Field_Type_Float),
		stringBuilder: acquireColumnBuilder(influx.Field_Type_String),
		boolBuilder:   acquireColumnBuilder(influx.Field_Type_Boolean),
		timeBuilder:   acquireTimePreAggBuilder(),
	}
	b.reset()
	return b
}

func (b *PreAggBuilders) reset() {
	b.intBuilder.reset()
	b.floatBuilder.reset()
	b.stringBuilder.reset()
	b.boolBuilder.reset()
	b.timeBuilder.reset()
}

func (b *PreAggBuilders) Release() {
	if b == nil {
		return
	}

	ReleaseColumnBuilder(b.intBuilder)
	b.intBuilder = nil
	ReleaseColumnBuilder(b.floatBuilder)
	b.floatBuilder = nil
	ReleaseColumnBuilder(b.stringBuilder)
	b.stringBuilder = nil
	ReleaseColumnBuilder(b.boolBuilder)
	b.boolBuilder = nil
	ReleaseColumnBuilder(b.timeBuilder)
	b.timeBuilder = nil
}

func (b *PreAggBuilders) aggBuilder(ref *record.Field) PreAggBuilder {
	switch ref.Type {
	case influx.Field_Type_Int:
		if ref.Name == record.TimeField {
			return b.timeBuilder
		}
		return b.intBuilder
	case influx.Field_Type_Float:
		return b.floatBuilder
	case influx.Field_Type_String:
		return b.stringBuilder
	case influx.Field_Type_Boolean:
		return b.boolBuilder
	default:
		panic("unknown type")
	}
}

func ReleaseColumnBuilder(b PreAggBuilder) {
	if b == nil {
		return
	}
	b.release()
}

func acquireColumnBuilder(ty int) PreAggBuilder {
	switch ty {
	case influx.Field_Type_Int:
		v := integerPreAggPool.Get()
		if v == nil {
			return NewIntegerPreAgg()
		}
		return v.(*IntegerPreAgg)
	case influx.Field_Type_Float:
		v := floatPreAggPool.Get()
		if v == nil {
			return NewFloatPreAgg()
		}
		return v.(*FloatPreAgg)
	case influx.Field_Type_String:
		v := stringPreAggPool.Get()
		if v == nil {
			return NewStringPreAgg()
		}
		return v.(*StringPreAgg)
	case influx.Field_Type_Boolean:
		v := boolPreAggPool.Get()
		if v == nil {
			return NewBooleanPreAgg()
		}
		return v.(*BooleanPreAgg)
	default:
		panic("unknown type")
	}
}

func acquireTimePreAggBuilder() PreAggBuilder {
	v := timePreAggPool.Get()
	if v == nil {
		return NewTimePreAgg()
	}
	return v.(*TimePreAgg)
}

// IntegerPreAgg If you change the order of the elements in the structure,
// remember to modify marshal() and unmarshal() as well.
type IntegerPreAgg struct {
	values []int64 // min max minT maxT sum count
}

func NewIntegerPreAgg() *IntegerPreAgg {
	m := &IntegerPreAgg{values: make([]int64, countIndex+1)}
	m.reset()
	return m
}

func (m *IntegerPreAgg) size() int {
	return (countIndex + 1) * record.Int64SizeBytes
}

func (m *IntegerPreAgg) marshal(dst []byte) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&m.values))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * record.Int64SizeBytes
	s.Cap = h.Cap * record.Int64SizeBytes

	dst = append(dst, res...)
	return dst
}

func (m *IntegerPreAgg) unmarshal(src []byte) ([]byte, error) {
	if len(src) < m.size() {
		return nil, fmt.Errorf("too small data %v for ColumnMetaInteger", len(src))
	}

	var dstArr []int64
	h := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dst := (*reflect.SliceHeader)(unsafe.Pointer(&dstArr))
	dst.Data = h.Data
	dst.Cap = h.Cap / record.Int64SizeBytes
	dst.Len = h.Len / record.Int64SizeBytes
	copy(m.values, dstArr)

	return src[m.size():], nil
}

func (m *IntegerPreAgg) reset() {
	m.values[minIndex] = math.MaxInt64 // min
	m.values[maxIndex] = math.MinInt64 // max
	m.values[minTIndex] = 0            // minTime
	m.values[maxTIndex] = 0            // maxTime
	m.values[sumIndex] = 0             // sum
	m.values[countIndex] = 0           // count
}

func (m *IntegerPreAgg) min() (interface{}, int64) {
	return m.values[minIndex], m.values[minTIndex]
}

func (m *IntegerPreAgg) max() (interface{}, int64) {
	return m.values[maxIndex], m.values[maxTIndex]
}

func (m *IntegerPreAgg) count() int64 {
	return m.values[countIndex]
}

func (m *IntegerPreAgg) sum() interface{} {
	return m.values[sumIndex]
}

func (m *IntegerPreAgg) addValues(col *record.ColVal, times []int64) {
	values := col.IntegerValues()
	valLen := len(values)
	for i := 0; i < valLen; i++ {
		v := values[i]
		if m.values[minIndex] > v {
			m.values[minIndex] = v
			m.values[minTIndex] = times[i]
		}
		if m.values[maxIndex] < v {
			m.values[maxIndex] = v
			m.values[maxTIndex] = times[i]
		}

		m.values[sumIndex] += v
	}

	m.values[countIndex] += int64(valLen)
}

func (m *IntegerPreAgg) release() {
	m.reset()
	integerPreAggPool.Put(m)
}

func (m *IntegerPreAgg) addMin(value float64, tm int64) {
	v := int64(value)
	if v < m.values[minIndex] {
		m.values[minIndex] = v
		m.values[minTIndex] = tm
	} else if m.values[minIndex] == v {
		if tm < m.values[minTIndex] {
			m.values[minTIndex] = tm
		}
	}
}

func (m *IntegerPreAgg) addMax(value float64, tm int64) {
	v := int64(value)
	if v > m.values[maxIndex] {
		m.values[maxIndex] = v
		m.values[maxTIndex] = tm
	} else if m.values[maxIndex] == v {
		if tm < m.values[maxTIndex] {
			m.values[maxTIndex] = tm
		}
	}
}
func (m *IntegerPreAgg) addSum(v float64) { m.values[sumIndex] += int64(v) }
func (m *IntegerPreAgg) addCount(n int64) { m.values[countIndex] += n }

// FloatPreAgg If you change the order of the elements in the structure,
// remember to modify marshal() and unmarshal() as well.
type FloatPreAgg struct {
	minV    float64
	maxV    float64
	minTime int64
	maxTime int64
	sumV    float64
	countV  int64
}

func NewFloatPreAgg() *FloatPreAgg {
	m := &FloatPreAgg{}
	m.reset()
	return m
}

func (m *FloatPreAgg) size() int {
	return int(unsafe.Sizeof(*m))
}

func (m *FloatPreAgg) marshal(dst []byte) []byte {
	dst = numberenc.MarshalFloat64(dst, m.minV)
	dst = numberenc.MarshalFloat64(dst, m.maxV)
	dst = numberenc.MarshalInt64Append(dst, m.minTime)
	dst = numberenc.MarshalInt64Append(dst, m.maxTime)
	dst = numberenc.MarshalFloat64(dst, m.sumV)
	dst = numberenc.MarshalInt64Append(dst, m.countV)
	return dst
}

func (m *FloatPreAgg) unmarshal(src []byte) ([]byte, error) {
	if len(src) < m.size() {
		return nil, fmt.Errorf("too small data %v for ColumnMetaFloat", len(src))
	}

	m.minV, src = numberenc.UnmarshalFloat64(src), src[8:]
	m.maxV, src = numberenc.UnmarshalFloat64(src), src[8:]
	m.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.sumV, src = numberenc.UnmarshalFloat64(src), src[8:]
	m.countV, src = numberenc.UnmarshalInt64(src), src[8:]
	return src, nil
}

func (m *FloatPreAgg) reset() {
	m.minV = math.MaxFloat64  // min
	m.maxV = -math.MaxFloat64 // max
	m.minTime = 0             // minT
	m.maxTime = 0             // maxT
	m.sumV = 0                // sum
	m.countV = 0              // count
}

func (m *FloatPreAgg) min() (interface{}, int64) {
	return m.minV, m.minTime
}

func (m *FloatPreAgg) max() (interface{}, int64) {
	return m.maxV, m.maxTime
}

func (m *FloatPreAgg) count() int64 {
	return m.countV
}

func (m *FloatPreAgg) sum() interface{} {
	return m.sumV
}

func (m *FloatPreAgg) addValues(col *record.ColVal, times []int64) {
	values := col.FloatValues()
	valLen := len(values)
	for i := 0; i < valLen; i++ {
		v := values[i]
		if m.minV > v {
			m.minV = v
			m.minTime = times[i]
		}
		if m.maxV < v {
			m.maxV = v
			m.maxTime = times[i]
		}

		m.sumV += v
	}

	m.countV += int64(valLen)
}

func (m *FloatPreAgg) release() {
	m.reset()
	floatPreAggPool.Put(m)
}

func (m *FloatPreAgg) addMin(v float64, tm int64) {
	if m.minV < v {
		m.minV = v
		m.minTime = tm
	} else if m.minV == v {
		if tm < m.minTime {
			m.minTime = tm
		}
	}
}

func (m *FloatPreAgg) addMax(v float64, tm int64) {
	if m.maxV > v {
		m.maxV = v
		m.maxTime = tm
	} else if m.minV == v {
		if tm < m.minTime {
			m.minTime = tm
		}
	}
}

func (m *FloatPreAgg) addSum(v float64) { m.sumV += v }
func (m *FloatPreAgg) addCount(n int64) { m.countV += n }

// StringPreAgg If you change the order of the elements in the structure,
// remember to modify marshal() and unmarshal() as well.
type StringPreAgg struct {
	counts int64
}

func NewStringPreAgg() *StringPreAgg {
	return &StringPreAgg{}
}

func (m *StringPreAgg) size() int {
	return 8
}

func (m *StringPreAgg) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, m.counts)
	return dst
}

func (m *StringPreAgg) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("too small data (%v) for ColumnMetaString", len(src))
	}

	m.counts, src = numberenc.UnmarshalInt64(src), src[8:]
	return src, nil
}

func (m *StringPreAgg) reset() {
	m.counts = 0
}

func (m *StringPreAgg) min() (interface{}, int64) {
	panic("no min for string type")
}

func (m *StringPreAgg) max() (interface{}, int64) {
	panic("no max for string type")
}

func (m *StringPreAgg) count() int64 {
	return m.counts
}

func (m *StringPreAgg) sum() interface{} {
	return nil
}

func (m *StringPreAgg) addValues(col *record.ColVal, _ []int64) {
	m.counts += int64(col.Length() - col.NullN())
}

func (m *StringPreAgg) release() {
	m.counts = 0
	stringPreAggPool.Put(m)
}

func (m *StringPreAgg) addMin(float64, int64) {}
func (m *StringPreAgg) addMax(float64, int64) {}
func (m *StringPreAgg) addSum(float64)        {}
func (m *StringPreAgg) addCount(n int64)      { m.counts += n }

type BooleanPreAgg struct {
	counts     int64
	minTime    int64
	maxTime    int64
	minV, maxV int8
}

func (m *BooleanPreAgg) size() int {
	return 26
}

func (m *BooleanPreAgg) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, m.counts)
	dst = numberenc.MarshalInt64Append(dst, m.minTime)
	dst = numberenc.MarshalInt64Append(dst, m.maxTime)
	dst = append(dst, byte(m.minV))
	dst = append(dst, byte(m.maxV))
	return dst
}

func (m *BooleanPreAgg) unmarshal(src []byte) ([]byte, error) {
	if len(src) < m.size() {
		return nil, fmt.Errorf("too small data for ColumnMetaBoolean")
	}

	m.counts, src = numberenc.UnmarshalInt64(src), src[8:]
	m.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.minV, src = int8(src[0]), src[1:]
	m.maxV, src = int8(src[0]), src[1:]

	return src, nil
}

func (m *BooleanPreAgg) reset() {
	m.counts = 0
	m.minTime = 0
	m.maxTime = 0
	m.minV = 2
	m.maxV = -1
}

func NewBooleanPreAgg() *BooleanPreAgg {
	return &BooleanPreAgg{}
}

func (m *BooleanPreAgg) min() (interface{}, int64) {
	if m.minV == 1 {
		return true, m.minTime
	}
	return false, m.minTime
}

func (m *BooleanPreAgg) max() (interface{}, int64) {
	if m.maxV == 1 {
		return true, m.maxTime
	}
	return false, m.maxTime
}

func (m *BooleanPreAgg) count() int64 {
	return m.counts
}

func (m *BooleanPreAgg) sum() interface{} {
	return nil
}

func (m *BooleanPreAgg) addValues(col *record.ColVal, times []int64) {
	values := col.Int8Values()
	valLen := len(values)
	for i := 0; i < valLen; i++ {
		v := values[i]
		if m.minV > v {
			m.minV = v
			m.minTime = times[i]
		}
		if m.maxV < v {
			m.maxV = v
			m.maxTime = times[i]
		}
	}

	m.counts += int64(valLen)
}

func (m *BooleanPreAgg) release() {
	m.counts = 0
	boolPreAggPool.Put(m)
}

func (m *BooleanPreAgg) addMin(v float64, tm int64) {
	bv := int8(v)
	if bv < m.minV {
		m.minV = bv
		m.minTime = tm
	} else if m.minV == bv {
		if tm < m.minTime {
			m.minTime = tm
		}
	}
}

func (m *BooleanPreAgg) addMax(v float64, tm int64) {
	bv := int8(v)
	if bv > m.maxV {
		m.maxV = bv
		m.maxTime = tm
	} else if m.minV == bv {
		if tm < m.minTime {
			m.minTime = tm
		}
	}
}
func (m *BooleanPreAgg) addSum(float64)   {}
func (m *BooleanPreAgg) addCount(n int64) { m.counts += n }

type TimePreAgg struct {
	countV uint32
}

func NewTimePreAgg() *TimePreAgg {
	return &TimePreAgg{}
}

func (b *TimePreAgg) min() (interface{}, int64) {
	return 0, 0
}

func (b *TimePreAgg) max() (interface{}, int64) {
	return 0, 0
}

func (b *TimePreAgg) sum() interface{} {
	return 0
}

func (b *TimePreAgg) count() int64 {
	return int64(b.countV)
}

func (b *TimePreAgg) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint32Append(dst, b.countV)
	return dst
}

func (b *TimePreAgg) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf("too smaller data for time pre agg %v", len(src))
	}
	b.countV = numberenc.UnmarshalUint32(src)
	return src[4:], nil
}

func (b *TimePreAgg) addValues(_ *record.ColVal, times []int64) {
	b.countV += uint32(len(times))
}

func (b *TimePreAgg) size() int {
	return 4
}

func (b *TimePreAgg) reset() {
	b.countV = 0
}

func (b *TimePreAgg) release() {
	b.reset()
	timePreAggPool.Put(b)
}

func (b *TimePreAgg) addMin(float64, int64) {}
func (b *TimePreAgg) addMax(float64, int64) {}
func (b *TimePreAgg) addSum(float64)        {}
func (b *TimePreAgg) addCount(n int64)      { b.countV += uint32(n) }

// Segment offset/size/minT/maxT
type Segment struct {
	offset int64
	size   uint32
}

func (s Segment) bytes() int {
	return 8 + 4
}

func (s *Segment) reset() {
	s.offset = 0
	s.size = 0
}

func (s *Segment) setOffset(off int64) {
	s.offset = off
}

func (s *Segment) setSize(size uint32) {
	s.size = size
}

func (s *Segment) offsetSize() (int64, uint32) {
	return s.offset, s.size
}

func (s *Segment) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, s.offset)
	dst = numberenc.MarshalUint32Append(dst, s.size)
	return dst
}

func (s *Segment) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 8+4 {
		err := fmt.Errorf("too small data for segment %v < %v", len(src), 12)
		log.Error(err.Error())
		return src, err
	}
	s.offset, src = numberenc.UnmarshalInt64(src), src[8:]
	s.size = numberenc.UnmarshalUint32(src)
	return src[4:], nil
}

type SegmentRange [2]int64 // min/max

func (sr SegmentRange) bytes() int {
	return len(sr)
}

func (sr *SegmentRange) setMinTime(min int64) {
	sr[0] = min
}

func (sr *SegmentRange) setMaxTime(max int64) {
	sr[1] = max
}

func (sr *SegmentRange) minTime() int64 {
	return sr[0]
}

func (sr *SegmentRange) maxTime() int64 {
	return sr[1]
}

func (sr *SegmentRange) contains(tm int64) bool {
	return tm >= sr[0] && tm <= sr[1]
}

func (sr *SegmentRange) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, sr[0])
	dst = numberenc.MarshalInt64Append(dst, sr[1])
	return dst
}

func (sr *SegmentRange) unmarshal(src []byte) ([]byte, error) {
	if len(src) < sr.bytes() {
		return nil, fmt.Errorf("too smaller data for segment ragne %v", len(src))
	}

	sr[0], src = numberenc.UnmarshalInt64(src), src[8:]
	sr[1], src = numberenc.UnmarshalInt64(src), src[8:]
	return src, nil
}

type ColumnMeta struct {
	name    string
	ty      byte
	preAgg  []byte
	entries []Segment
}

func (m *ColumnMeta) reset() {
	if m == nil {
		return
	}
	m.name = ""
	m.ty = 0
	m.preAgg = m.preAgg[:0]
	for i := range m.entries {
		m.entries[i].reset()
	}
}

func (m *ColumnMeta) RowCount(ref *record.Field, decs *ReadContext) (int64, error) {
	if decs.preAggBuilders == nil {
		decs.preAggBuilders = newPreAggBuilders()
	}

	cb := decs.preAggBuilders.aggBuilder(ref)
	_, err := cb.unmarshal(m.preAgg)
	if err != nil {
		log.Error("unmarshal pre-agg data fail", zap.Error(err))
		return 0, err
	}

	return cb.count(), err
}

func (m *ChunkMeta) GetSid() (sid uint64) {
	return m.sid
}

func (m *ChunkMeta) TimeMeta() *ColumnMeta {
	return &m.colMeta[len(m.colMeta)-1]
}

func (m ColumnMeta) bytes(segs int) int {
	return 2 + len(m.name) + // name length + name
		1 + // type
		2 + // count
		2 + len(m.preAgg) + //preAgg length + preAggData
		SegmentLen*segs //segment meta
}

func (m *ColumnMeta) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint16Append(dst, uint16(len(m.name)))
	dst = append(dst, m.name...)
	dst = append(dst, m.ty)
	dst = numberenc.MarshalUint16Append(dst, uint16(len(m.preAgg)))
	dst = append(dst, m.preAgg...)
	for i := range m.entries {
		seg := m.entries[i]
		dst = seg.marshal(dst)
	}

	return dst
}

func (m *ChunkMeta) Reset() {
	m.sid = 0
	m.offset = 0
	m.size = 0
	m.segCount = 0
	m.columnCount = 0

	for i := range m.colMeta {
		m.colMeta[i].reset()
	}
	m.colMeta = m.colMeta[:0]
}

func (m *ColumnMeta) unmarshal(src []byte, segs int) ([]byte, error) {
	var err error
	if len(src) < 2 {
		err = fmt.Errorf("too smaller data for column name length,  %v < 2 ", len(src))
		log.Error(err.Error())
		return nil, err
	}

	l := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < l+5 {
		err = fmt.Errorf("too smaller data for column name,  %v < %v", len(src), l+5)
		log.Error(err.Error())
		return nil, err
	}
	m.name, src = string(src[:l]), src[l:]
	m.ty, src = src[0], src[1:]
	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	if len(src) < l {
		err = fmt.Errorf("too smaller data for preagg,  %v < %v", len(src), l)
		log.Error(err.Error())
		return nil, err
	}

	m.preAgg = append(m.preAgg[:0], src[:l]...)
	src = src[l:]
	segSize := segs * SegmentLen
	if len(src) < segSize {
		err = fmt.Errorf("too smaller data for segment meta,  %v < %v(%v)", len(src), segSize, segs)
		log.Error(err.Error())
		return nil, err
	}
	if cap(m.entries) < segs {
		delta := segs - cap(m.entries)
		m.entries = m.entries[:cap(m.entries)]
		m.entries = append(m.entries, make([]Segment, delta)...)
	}
	m.entries = m.entries[:segs]
	for i := range m.entries {
		segData := src[:SegmentLen]
		_, err = m.entries[i].unmarshal(segData)
		if err != nil {
			return nil, err
		}
		src = src[SegmentLen:]
	}

	return src, nil
}

func (m *ColumnMeta) growEntry() {
	if len(m.entries) < cap(m.entries) {
		m.entries = m.entries[:len(m.entries)+1]
	} else {
		m.entries = append(m.entries, Segment{})
	}
}

type ChunkMeta struct {
	sid         uint64
	offset      int64
	size        uint32
	columnCount uint32
	segCount    uint16
	timeRange   []SegmentRange
	colMeta     []ColumnMeta
}

func (m *ChunkMeta) segmentCount() int {
	return int(m.segCount)
}

func (m *ChunkMeta) timeMeta() *ColumnMeta {
	return &m.colMeta[len(m.colMeta)-1]
}

func (m *ChunkMeta) minTime() int64 {
	return m.timeRange[0].minTime()
}

func (m *ChunkMeta) maxTime() int64 {
	return m.timeRange[len(m.timeRange)-1].maxTime()
}

func (m *ChunkMeta) MinMaxTime() (min int64, max int64) {
	return m.minTime(), m.maxTime()
}

func (m *ChunkMeta) resize(columns int, segs int) {
	if cap(m.colMeta) < columns {
		delta := columns - cap(m.colMeta)
		m.colMeta = m.colMeta[:cap(m.colMeta)]
		m.colMeta = append(m.colMeta, make([]ColumnMeta, delta)...)
	}
	m.colMeta = m.colMeta[:columns]

	for i := range m.colMeta {
		cm := &m.colMeta[i]
		if cap(cm.entries) < segs {
			delta := segs - cap(cm.entries)
			cm.entries = cm.entries[:cap(cm.entries)]
			cm.entries = append(cm.entries, make([]Segment, delta)...)
		}
		cm.entries = cm.entries[:segs]
		cm.preAgg = cm.preAgg[:0]
	}

	if cap(m.timeRange) < segs {
		m.timeRange = m.timeRange[:cap(m.timeRange)]
		delta := segs - len(m.timeRange)
		m.timeRange = append(m.timeRange, make([]SegmentRange, delta)...)
	}
	m.timeRange = m.timeRange[:segs]
}

func (m *ChunkMeta) reset() {
	m.sid = 0
	m.offset = 0
	m.size = 0
	m.segCount = 0
	m.columnCount = 0

	for i := range m.colMeta {
		m.colMeta[i].reset()
	}
	m.colMeta = m.colMeta[:0]
}

func (m *ChunkMeta) Size() int {
	n := 8*3 + 2
	for i := range m.colMeta {
		n += m.colMeta[i].bytes(int(m.segCount))
	}

	return n
}

func (m *ChunkMeta) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint64Append(dst, m.sid)
	dst = numberenc.MarshalInt64Append(dst, m.offset)
	dst = numberenc.MarshalUint32Append(dst, m.size)
	dst = numberenc.MarshalUint32Append(dst, m.columnCount)
	dst = numberenc.MarshalUint16Append(dst, m.segCount)
	for i := range m.timeRange {
		tr := &m.timeRange[i]
		dst = tr.marshal(dst)
	}
	for i := range m.colMeta {
		dst = m.colMeta[i].marshal(dst)
	}

	return dst
}

func (m *ChunkMeta) unmarshal(src []byte) ([]byte, error) {
	var err error
	if len(src) < ChunkMetaMinLen {
		err = fmt.Errorf("to smaller data for unmarshal %v < 24", len(src))
		log.Error(err.Error())
		return nil, err
	}

	m.sid, src = numberenc.UnmarshalUint64(src), src[8:]
	m.offset, src = numberenc.UnmarshalInt64(src), src[8:]
	m.size, src = numberenc.UnmarshalUint32(src), src[4:]
	m.columnCount, src = numberenc.UnmarshalUint32(src), src[4:]
	m.segCount, src = numberenc.UnmarshalUint16(src), src[2:]

	m.resize(int(m.columnCount), int(m.segCount))
	for i := range m.timeRange {
		tr := &m.timeRange[i]
		src, err = tr.unmarshal(src)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
	}

	for i := range m.colMeta {
		src, err = m.colMeta[i].unmarshal(src, int(m.segCount))
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
	}

	return src, nil
}

func (m *ChunkMeta) columnIndex(ref *record.Field) int {
	for i := range m.colMeta {
		if ref.Name == m.colMeta[i].name && ref.Type == int(m.colMeta[i].ty) {
			return i
		}
	}
	return -1
}

func (m *ChunkMeta) allRowsInRange(tr record.TimeRange) bool {
	min, max := m.MinMaxTime()
	return tr.Min <= min && tr.Max >= max
}

func (m *ChunkMeta) Rows(ab PreAggBuilder) int {
	cm := &m.colMeta[len(m.colMeta)-1]
	ab.reset()
	_, err := ab.unmarshal(cm.preAgg)
	if err != nil {
		panic(err)
	}
	return int(ab.count())
}

func (m *ChunkMeta) growTimeRangeEntry() {
	if len(m.timeRange) < cap(m.timeRange) {
		m.timeRange = m.timeRange[:len(m.timeRange)+1]
	} else {
		m.timeRange = append(m.timeRange, SegmentRange{})
	}
}

// MetaIndex If you change the order of the elements in the structure,
// remember to modify marshal() and unmarshal() as well.
type MetaIndex struct {
	id      uint64
	minTime int64
	maxTime int64
	offset  int64  // chunkmeta block offset
	count   uint32 // number of ChunkMeta
	size    uint32 // chunkmeta block size
}

func (m *MetaIndex) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint64Append(dst, m.id)
	dst = numberenc.MarshalInt64Append(dst, m.minTime)
	dst = numberenc.MarshalInt64Append(dst, m.maxTime)
	dst = numberenc.MarshalInt64Append(dst, m.offset)
	dst = numberenc.MarshalUint32Append(dst, m.count)
	dst = numberenc.MarshalUint32Append(dst, m.size)

	return dst
}

func (m *MetaIndex) unmarshal(src []byte) ([]byte, error) {
	if len(src) < MetaIndexLen {
		return nil, fmt.Errorf("too small data (%v) for MetaIndex", len(src))
	}

	m.id, src = numberenc.UnmarshalUint64(src), src[8:]
	m.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.offset, src = numberenc.UnmarshalInt64(src), src[8:]
	m.count, src = numberenc.UnmarshalUint32(src), src[4:]
	m.size, src = numberenc.UnmarshalUint32(src), src[4:]

	return src, nil
}

func (m *MetaIndex) reset() {
	m.id = 0
	m.minTime = math.MaxInt64
	m.maxTime = math.MinInt64
	m.offset = 0
	m.size = 0
	m.count = 0
}

const (
	defaultBufferSize = 256 * 1024
	maxBufferSize     = 1024 * 1024
	minBufferSize     = 4 * 1024
)

type FileWriter interface {
	WriteData(b []byte) (int, error)
	WriteChunkMeta(b []byte) (int, error)
	Close() error
	DataSize() int64
	ChunkMetaSize() int64
	DataWriter() io.Writer
	AppendChunkMetaToData() error
	SwitchMetaBuffer()
	MetaDataBlocks(dst [][]byte) [][]byte
	Name() string
}

type fileWriter struct {
	fd    fileops.File
	dw    writer // data chunk writer
	dataN int64

	cmw *indexWriter // chunkmeta writer
	cmN int64
}

func newWriteLimiter(fd fileops.File, limitCompact bool) NameReadWriterCloser {
	var lw NameReadWriterCloser
	if limitCompact {
		lw = NewLimitWriter(fd, compWriteLimiter)
	} else {
		lw = fd
		if SnapshotLimit() {
			lw = NewLimitWriter(fd, snapshotWriteLimiter)
		}
	}
	return lw
}

func newFileWriter(fd fileops.File, cacheMeta bool, limitCompact bool) FileWriter {
	name := fd.Name()
	idxName := name[:len(name)-len(tmpTsspFileSuffix)] + ".index.init"

	lw := newWriteLimiter(fd, limitCompact)
	w := &fileWriter{
		fd: fd,
		dw: NewDiskWriter(lw, defaultBufferSize),
	}

	w.cmw = NewIndexWriter(idxName, cacheMeta, limitCompact)

	return w
}

func (w *fileWriter) DataWriter() io.Writer {
	return w.dw
}

func (w *fileWriter) DataSize() int64 {
	return w.dataN
}

func (w *fileWriter) ChunkMetaSize() int64 {
	return w.cmN
}

func (w *fileWriter) WriteData(b []byte) (int, error) {
	n, err := w.dw.Write(b)
	if err != nil {
		return 0, err
	}
	w.dataN += int64(n)

	return n, nil
}

func (w *fileWriter) Name() string {
	return w.fd.Name()
}

func (w *fileWriter) WriteChunkMeta(b []byte) (int, error) {
	n, err := w.cmw.Write(b)
	if err != nil {
		return 0, err
	}
	w.cmN += int64(n)

	return n, nil
}

func (w *fileWriter) Close() error {
	if w.dw != nil {
		if err := w.dw.Close(); err != nil {
			log.Error("close data writer fail", zap.Error(err))
			return err
		}
		w.dw = nil
	}

	if w.cmw != nil {
		if err := w.cmw.Close(); err != nil {
			log.Error("close chunk meta writer fail", zap.Error(err))
			return err
		}
		w.cmw = nil
	}

	if err := w.fd.Sync(); err != nil {
		log.Error("sync file fail", zap.String("file", w.fd.Name()), zap.Error(err))
		return err
	}
	w.fd = nil

	return nil
}

func (w *fileWriter) AppendChunkMetaToData() error {
	n, err := w.cmw.CopyTo(w.dw)
	if err != nil {
		log.Error("copy chunk meta fail", zap.Error(err))
		return err
	}

	if n != w.cmw.Size() {
		err = fmt.Errorf("copy chunkmeta to data fail, metasize:%v, copysize:%v", w.cmw.Size(), n)
		log.Error(err.Error())
		return err
	}

	w.dataN += int64(n)

	return err
}

func (w *fileWriter) SwitchMetaBuffer() {
	w.cmw.SwitchMetaBuffer()
}

func (w *fileWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	return w.cmw.MetaDataBlocks(dst)
}

type writer interface {
	Write(b []byte) (int, error)
	Close() error
	Size() int
	Reset(lw NameReadWriterCloser)
	Bytes() []byte
	CopyTo(w io.Writer) (int, error)
	SwitchMetaBuffer()
	MetaDataBlocks(dst [][]byte) [][]byte
}

type diskWriter struct {
	lw NameReadWriterCloser
	w  *bufio.Writer
	n  int
}

func NewDiskWriter(lw NameReadWriterCloser, bufferSize int) *diskWriter {
	if bufferSize == 0 {
		bufferSize = defaultBufferSize
	} else if bufferSize < minBufferSize {
		bufferSize = minBufferSize
	} else if bufferSize > maxBufferSize {
		bufferSize = maxBufferSize
	}

	var dw *diskWriter
	v := diskWriterPool.Get()
	if v == nil {
		dw = &diskWriter{
			w: bufio.NewWriterSize(lw, bufferSize),
		}
	} else {
		dw, _ = v.(*diskWriter)
	}

	dw.Reset(lw)
	dw.n = 0

	return dw
}

func (w *diskWriter) Write(b []byte) (int, error) {
	n, err := w.w.Write(b)
	if err != nil || n != len(b) {
		log.Error("write file fail", zap.String("file", w.lw.Name()),
			zap.Error(err), zap.Ints("write size", []int{n, len(b)}))
		return 0, err
	}
	w.n += n

	return n, nil
}

func (w *diskWriter) Close() error {
	if err := w.w.Flush(); err != nil {
		return err
	}

	w.lw = nil
	w.n = 0
	diskWriterPool.Put(w)

	return nil
}

func (w *diskWriter) Size() int {
	return w.n
}

func (w *diskWriter) Reset(lw NameReadWriterCloser) {
	w.lw = lw
	w.n = 0
	w.w.Reset(lw)
}

func (w *diskWriter) Bytes() []byte {
	return nil
}

func (w *diskWriter) CopyTo(to io.Writer) (int, error) {
	if err := w.w.Flush(); err != nil {
		log.Error("flush fail", zap.String("name", w.lw.Name()), zap.Error(err))
		return 0, err
	}

	name := w.lw.Name()
	if err := w.lw.Close(); err != nil {
		log.Error("close file fail", zap.String("file", name), zap.Error(err))
		return 0, err
	}

	lock := fileops.FileLockOption("")
	fd, err := fileops.Open(name, lock)
	if err != nil {
		log.Error("open file fail", zap.String("file", name), zap.Error(err))
		return 0, err
	}

	defer func(fn string) {
		if err = fd.Close(); err != nil {
			log.Error("close file fail", zap.String("file", fn), zap.Error(err))
		}
		if err = fileops.Remove(fn, fileops.FileLockOption("")); err != nil {
			log.Error("remove file fail", zap.String("file", fn), zap.Error(err))
		}
	}(name)

	buf := bufferpool.Get()
	defer bufferpool.Put(buf)
	buf = bufferpool.Resize(buf, defaultBufferSize)

	var wn int64
	wn, err = io.CopyBuffer(to, fd, buf)
	if err != nil {
		log.Error("copy file fail", zap.String("file", name), zap.Error(err))
		return 0, err
	}

	if wn != int64(w.n) {
		err = fmt.Errorf("copy file(%v) fail, file size:%v, copy size: %v", name, w.n, wn)
		return 0, err
	}

	return int(wn), err
}

func (w *diskWriter) SwitchMetaBuffer()                {}
func (w *diskWriter) MetaDataBlocks([][]byte) [][]byte { return nil }

var (
	diskWriterPool = sync.Pool{}
	metaBlkPool    = sync.Pool{}
)

func getMetaBlockBuffer(size int) []byte {
	v := metaBlkPool.Get()
	if v == nil {
		return make([]byte, 0, size)
	}
	buf, ok := v.([]byte)
	if !ok {
		buf = make([]byte, 0, size)
	}
	return buf[:0]
}

//nolint
func freeMetaBlockBuffer(b []byte) {
	metaBlkPool.Put(b[:0])
}

func freeMetaBlocks(buffers [][]byte) int {
	n := 0
	for i := range buffers {
		n += cap(buffers[i])
		freeMetaBlockBuffer(buffers[i])
	}
	return n
}

const (
	MB = 1024 * 1024
	GB = 1024 * MB
)

var (
	indexWriterPool = sync.Pool{New: func() interface{} { return &indexWriter{} }}
)

func indexWriterBufferSize(sizeBytes int64) int {
	if sizeBytes <= 0 {
		sizeBytes = 16 * GB
	}

	base := int(sizeBytes / GB / 4)
	if base < 1 {
		base = 1
	}

	if base > 64 {
		base = 64
	}

	return base * MB
}

type indexWriter struct {
	name string
	buf  []byte
	n    int
	wn   int
	lw   NameReadWriterCloser
	tmp  []byte

	blockSize    int
	cacheMeta    bool
	limitCompact bool
	metas        [][]byte
}

func NewIndexWriter(indexName string, cacheMeta bool, limitCompact bool) *indexWriter {
	v := indexWriterPool.Get()
	w := v.(*indexWriter)
	w.limitCompact = limitCompact
	w.reset(indexName, cacheMeta)
	if w.cacheMeta {
		w.buf = getMetaBlockBuffer(w.blockSize)
	} else {
		if cap(w.buf) >= w.blockSize {
			w.buf = w.buf[:w.blockSize]
		} else {
			w.buf = make([]byte, w.blockSize)
		}
	}

	return w
}

func (w *indexWriter) reset(name string, cacheMeta bool) {
	if w.lw != nil {
		_ = w.lw.Close()
	}

	bufSize := DefaultMaxChunkMetaItemSize
	if !cacheMeta {
		total, _ := memory.ReadSysMemory()
		total = total * kb
		bufSize = indexWriterBufferSize(total)
	}
	w.cacheMeta = cacheMeta
	w.blockSize = bufSize
	w.name = name
	w.metas = w.metas[:0]
	w.buf = w.buf[:0]
	w.n = 0
	w.wn = 0
	w.lw = nil
}

func (w *indexWriter) flush() error {
	if w.n == 0 {
		return nil
	}

	n, err := w.lw.Write(w.buf[0:w.n])
	if err != nil || n < w.n {
		return io.ErrShortWrite
	}
	w.n = 0

	return nil
}

func (w *indexWriter) available() int { return len(w.buf) - w.n }
func (w *indexWriter) buffered() int  { return w.n }

func (w *indexWriter) writeBuffer(p []byte) (int, error) {
	var wn int
	var err error
	for len(p) > w.available() && err == nil {
		if w.lw == nil {
			fd, err := fileops.OpenFile(w.name, os.O_CREATE|os.O_RDWR, 0640)
			if err != nil {
				log.Error("create file fail", zap.String("file", w.name), zap.Error(err))
				return 0, err
			}
			w.lw = newWriteLimiter(fd, w.limitCompact)
		}

		var n int
		if w.buffered() == 0 {
			// Big data block, write directly from p to avoid copy
			n, err = w.lw.Write(p)
		} else {
			n = copy(w.buf[w.n:], p)
			w.n += n
			err = w.flush()
		}

		wn += n
		p = p[n:]
	}
	if err != nil {
		return wn, err
	}

	n := copy(w.buf[w.n:], p)
	w.n += n

	wn += n
	w.wn += wn

	return wn, nil
}

func (w *indexWriter) writeCacheBlocks(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	w.wn += len(p)
	return len(p), nil
}

func (w *indexWriter) Write(p []byte) (nn int, err error) {
	if w.cacheMeta {
		return w.writeCacheBlocks(p)
	}

	return w.writeBuffer(p)
}

func (w *indexWriter) Close() error {
	if w.lw != nil {
		_ = w.lw.Close()
		_ = fileops.Remove(w.name)
		w.lw = nil
	}

	if w.cacheMeta {
		for _, b := range w.metas {
			freeMetaBlockBuffer(b)
		}
		if w.buf != nil {
			freeMetaBlockBuffer(w.buf)
		}
		w.buf = nil
	}

	w.reset("", false)
	indexWriterPool.Put(w)

	return nil
}

func (w *indexWriter) Size() int {
	return w.wn
}

func (w *indexWriter) bytes() []byte {
	return w.buf[:w.n]
}

func (w *indexWriter) copyFromCacheTo(to io.Writer) (int, error) {
	n := 0

	if len(w.buf) > 0 {
		w.metas = append(w.metas, w.buf)
		w.buf = nil
	}

	for i := range w.metas {
		if len(w.metas[i]) == 0 {
			continue
		}
		s, err := to.Write(w.metas[i])
		if err != nil {
			return n, err
		}
		n += s
	}

	return n, nil
}

func (w *indexWriter) allInBuffer() bool {
	return w.lw == nil && w.n <= w.wn
}

func (w *indexWriter) seekStart() error {
	err := w.lw.Close()
	if err != nil {
		return err
	}
	if w.lw, err = fileops.OpenFile(w.name, os.O_RDONLY, 0640); err != nil {
		return err
	}

	return nil
}

func (w *indexWriter) CopyTo(to io.Writer) (int, error) {
	if w.cacheMeta {
		return w.copyFromCacheTo(to)
	}

	if w.allInBuffer() {
		buf := w.bytes()
		return to.Write(buf)
	}

	if err := w.flush(); err != nil {
		return 0, err
	}

	// Seek file pos to start
	if err := w.seekStart(); err != nil {
		return 0, err
	}

	if len(w.tmp) <= 0 {
		w.tmp = bufferpool.Resize(w.tmp, defaultBufferSize)
	}
	n, err := io.CopyBuffer(to, w.lw, w.tmp)
	if err != nil {
		return 0, err
	}

	return int(n), nil
}

func (w *indexWriter) SwitchMetaBuffer() {
	if w.cacheMeta {
		w.metas = append(w.metas, w.buf)
		w.buf = getMetaBlockBuffer(w.blockSize)
	}
}

func (w *indexWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	dst = dst[:0]
	if w.cacheMeta {
		if len(w.buf) > 0 {
			w.metas = append(w.metas, w.buf)
			w.buf = nil
		}
		for i := range w.metas {
			if len(w.metas[i]) > 0 {
				dst = append(dst, w.metas[i])
			}
		}
		w.metas = w.metas[:0]
	}
	return dst
}
