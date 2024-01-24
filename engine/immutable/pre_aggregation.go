/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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
	SegmentLen        = (Segment{}).bytes()
	ColumnMetaLenMin  = (ColumnMeta{}).bytes(1)
	ChunkMetaMinLen   = ChunkMetaLen + ColumnMetaLenMin*2
)

const (
	MinMaxTimeLen        = int(unsafe.Sizeof(SegmentRange{}))
	ChunkMetaLen         = int(unsafe.Sizeof(ChunkMeta{})-24*2) + MinMaxTimeLen
	MetaIndexLen         = int(unsafe.Sizeof(MetaIndex{}))
	DetachedMetaIndexLen = int(unsafe.Sizeof(MetaIndex{}) - 4) //count not use
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

func (b *PreAggBuilders) FloatBuilder() *FloatPreAgg {
	builder, ok := b.floatBuilder.(*FloatPreAgg)
	if !ok || builder == nil {
		builder = &FloatPreAgg{}
	}
	return builder
}

func (b *PreAggBuilders) IntegerBuilder() *IntegerPreAgg {
	builder, ok := b.intBuilder.(*IntegerPreAgg)
	if !ok || builder == nil {
		builder = &IntegerPreAgg{}
	}
	return builder
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
	return (countIndex + 1) * util.Int64SizeBytes
}

func (m *IntegerPreAgg) marshal(dst []byte) []byte {
	if m.values[countIndex] == 1 {
		dst = numberenc.MarshalInt64Append(dst, m.values[minIndex])
		dst = numberenc.MarshalInt64Append(dst, m.values[minTIndex])
		return dst
	}

	for _, val := range m.values {
		dst = numberenc.MarshalInt64Append(dst, val)
	}
	return dst
}

func (m *IntegerPreAgg) unmarshal(src []byte) ([]byte, error) {
	if PreAggOnlyOneRow(src) {
		// only one row of data
		m.values[minIndex], src = numberenc.UnmarshalInt64(src), src[8:]
		m.values[minTIndex], src = numberenc.UnmarshalInt64(src), src[8:]
		m.values[maxIndex] = m.values[minIndex]
		m.values[maxTIndex] = m.values[minTIndex]
		m.values[sumIndex] = m.values[minIndex]
		m.values[countIndex] = 1
		return src, nil
	}

	if len(src) < m.size() {
		return nil, fmt.Errorf("too small data %v for ColumnMetaInteger", len(src))
	}

	for i := range m.values {
		m.values[i] = numberenc.UnmarshalInt64(src[:8])
		src = src[8:]
	}

	return src, nil
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
	agg := m.values
	for i, j := 0, 0; i < col.Len; i++ {
		if col.NilCount > 0 && col.IsNil(i) {
			continue
		}

		v := values[j]
		j++
		if agg[minIndex] > v {
			agg[minIndex] = v
			agg[minTIndex] = times[i]
		}
		if agg[maxIndex] < v {
			agg[maxIndex] = v
			agg[maxTIndex] = times[i]
		}

		agg[sumIndex] += v
	}

	agg[countIndex] += int64(valLen)
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

func (m *IntegerPreAgg) merge(other *IntegerPreAgg) {
	m.addMin(float64(other.values[minIndex]), other.values[minTIndex])
	m.addMax(float64(other.values[maxIndex]), other.values[maxTIndex])
	m.values[sumIndex] += other.values[sumIndex]
	m.values[countIndex] += other.values[countIndex]
}

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
	if m.countV == 1 {
		dst = numberenc.MarshalFloat64(dst, m.minV)
		dst = numberenc.MarshalInt64Append(dst, m.minTime)
		return dst
	}

	dst = numberenc.MarshalFloat64(dst, m.minV)
	dst = numberenc.MarshalFloat64(dst, m.maxV)
	dst = numberenc.MarshalInt64Append(dst, m.minTime)
	dst = numberenc.MarshalInt64Append(dst, m.maxTime)
	dst = numberenc.MarshalFloat64(dst, m.sumV)
	dst = numberenc.MarshalInt64Append(dst, m.countV)
	return dst
}

func (m *FloatPreAgg) unmarshal(src []byte) ([]byte, error) {
	if PreAggOnlyOneRow(src) {
		// only one row of data
		m.minV, src = numberenc.UnmarshalFloat64(src), src[8:]
		m.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
		m.maxV = m.minV
		m.maxTime = m.minTime
		m.sumV = m.minV
		m.countV = 1
		return src, nil
	}

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
	for i, j := 0, 0; i < col.Len; i++ {
		if col.NilCount > 0 && col.IsNil(i) {
			continue
		}

		v := values[j]
		j++
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

func (m *FloatPreAgg) merge(other *FloatPreAgg) {
	m.addMin(other.minV, other.minTime)
	m.addMax(other.maxV, other.maxTime)
	m.addSum(other.sumV)
	m.addCount(other.countV)
}

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

func PreAggOnlyOneRow(buf []byte) bool {
	// If a Chunk contains only one row of data,
	// the pre-aggregation data retains only the minimum value and the corresponding time
	// Therefore, the length of the pre-aggregated data is 16 bytes.
	return len(buf) == 16
}
