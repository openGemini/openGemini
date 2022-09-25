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
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	tableMagic = "53ac2021"
	version    = uint64(2)

	trailerSize           = int(unsafe.Sizeof(Trailer{})) - 24*2 + (2 + 0) + (2 + 1)
	fileHeaderSize        = len(tableMagic) + int(unsafe.Sizeof(version))
	kb                    = 1024
	maxImmTablePercentage = 85
)

var (
	falsePositive         = 0.08
	nodeImmTableSizeLimit = int64(20 * 1024 * 1024 * 1024)
	nodeImmTableSizeUsed  = int64(0)
	loadSizeLimit         = nodeImmTableSizeLimit
)

func SetImmTableMaxMemoryPercentage(sysTotalMem, percentage int) {
	if percentage > maxImmTablePercentage {
		percentage = maxImmTablePercentage
	}
	nodeImmTableSizeLimit = int64(sysTotalMem * percentage / 100)
	logger.GetLogger().Info("Set imm table max memory percentage", zap.Int64("nodeImmTableSizeLimit", nodeImmTableSizeLimit),
		zap.Int64("sysTotalMem", int64(sysTotalMem)), zap.Int64("percentage", int64(percentage)))
	loadSizeLimit = nodeImmTableSizeLimit
}

type TableStat struct {
	idCount          int64
	minId, maxId     uint64
	minTime, maxTime int64
	metaIndexItemNum int64
	bloomM, bloomK   uint64
	data             []byte // reserved for future use
	name             []byte // measurement name
}

func (stat *TableStat) marshalStat(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, stat.idCount)
	dst = numberenc.MarshalUint64Append(dst, stat.minId)
	dst = numberenc.MarshalUint64Append(dst, stat.maxId)
	dst = numberenc.MarshalInt64Append(dst, stat.minTime)
	dst = numberenc.MarshalInt64Append(dst, stat.maxTime)
	dst = numberenc.MarshalInt64Append(dst, stat.metaIndexItemNum)
	dst = numberenc.MarshalUint64Append(dst, stat.bloomM)
	dst = numberenc.MarshalUint64Append(dst, stat.bloomK)

	dst = numberenc.MarshalUint16Append(dst, uint16(len(stat.data)))
	dst = append(dst, stat.data...)
	dst = numberenc.MarshalUint16Append(dst, uint16(len(stat.name)))
	dst = append(dst, stat.name...)

	return dst
}

func (stat *TableStat) unmarshalStat(src []byte) ([]byte, error) {
	stat.idCount, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.minId, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.maxId, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.metaIndexItemNum, src = numberenc.UnmarshalInt64(src), src[8:]
	stat.bloomM, src = numberenc.UnmarshalUint64(src), src[8:]
	stat.bloomK, src = numberenc.UnmarshalUint64(src), src[8:]

	if len(src) < 2 {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer data, expect(2)", len(src))
	}
	dLen := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < dLen+2 {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer data, expect(%v)", len(src), dLen)
	}

	if len(stat.data) < dLen {
		stat.data = make([]byte, dLen)
	}
	stat.data = stat.data[:dLen]
	copy(stat.data, src[:dLen])
	src = src[dLen:]

	nameLen := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < nameLen {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer name, expect(%v)", len(src), nameLen)
	}

	if len(stat.name) < nameLen {
		stat.name = make([]byte, nameLen)
	}
	stat.name = stat.name[:nameLen]
	copy(stat.name, src[:nameLen])
	src = src[nameLen:]

	return src, nil
}

type Trailer struct {
	dataOffset    int64
	dataSize      int64
	indexSize     int64
	metaIndexSize int64
	bloomSize     int64
	idTimeSize    int64
	TableStat
}

func (t *Trailer) reset() {
	t.dataOffset = 0
	t.dataSize = 0
	t.indexSize = 0
	t.metaIndexSize = 0
	t.bloomSize = 0
	t.idTimeSize = 0
	t.idCount = 0
	t.minId = 0
	t.maxId = 0
	t.minTime = 0
	t.maxTime = 0
	t.metaIndexItemNum = 0
	t.bloomM = 0
	t.bloomK = 0

	t.data = t.data[:0]
	t.name = t.name[:0]
}

func (t *Trailer) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, t.dataOffset)
	dst = numberenc.MarshalInt64Append(dst, t.dataSize)
	dst = numberenc.MarshalInt64Append(dst, t.indexSize)
	dst = numberenc.MarshalInt64Append(dst, t.metaIndexSize)
	dst = numberenc.MarshalInt64Append(dst, t.bloomSize)
	dst = numberenc.MarshalInt64Append(dst, t.idTimeSize)

	return t.marshalStat(dst)
}

func (t *Trailer) unmarshal(src []byte) ([]byte, error) {
	if len(src) < trailerSize {
		return nil, fmt.Errorf("tool small data (%v) for unmarshal file Trailer", len(src))
	}

	t.dataOffset, src = numberenc.UnmarshalInt64(src), src[8:]
	t.dataSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.indexSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.metaIndexSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.bloomSize, src = numberenc.UnmarshalInt64(src), src[8:]
	t.idTimeSize, src = numberenc.UnmarshalInt64(src), src[8:]
	return t.unmarshalStat(src)
}

func (t *Trailer) ContainsId(id uint64) bool {
	if id >= t.minId && id <= t.maxId {
		return true
	}
	return false
}

func (t *Trailer) ContainsTime(tm record.TimeRange) bool {
	return tm.Overlaps(t.minTime, t.maxTime)
}

func (t *Trailer) copyTo(tr *Trailer) {
	tr.dataOffset = t.dataOffset
	tr.dataSize = t.dataSize
	tr.indexSize = t.indexSize
	tr.metaIndexSize = t.metaIndexSize
	tr.bloomSize = t.bloomSize
	tr.idCount = t.idCount
	tr.minId, tr.maxId = t.minId, t.maxId
	tr.minTime, tr.maxTime = t.minTime, t.maxTime
	tr.metaIndexItemNum = t.metaIndexItemNum
	tr.bloomM, tr.bloomK = t.bloomM, t.bloomK
	if cap(tr.data) < len(t.data) {
		tr.data = make([]byte, len(t.data))
	} else {
		tr.data = tr.data[:len(t.data)]
	}
	copy(tr.data, t.data)

	if cap(tr.name) < len(t.name) {
		tr.name = make([]byte, len(t.name))
	} else {
		tr.name = tr.name[:len(t.name)]
	}
	copy(tr.name, t.name)
}

func (t *Trailer) metaOffsetSize() (int64, int64) {
	return t.dataOffset + t.dataSize, t.indexSize
}

func (t *Trailer) metaIndexOffsetSize() (int64, int64) {
	return t.dataOffset + t.dataSize + t.indexSize, t.metaIndexSize
}

func (t *Trailer) idTimeOffsetSize() (int64, int64) {
	idTimeOff := t.dataOffset + t.dataSize + t.indexSize + t.metaIndexSize + t.bloomSize
	return idTimeOff, t.idTimeSize
}

type TableData struct {
	// id bloom filter data
	bloomFilter []byte
	// include version | each section offset | measurement name | key and time range etc...
	trailerData    []byte
	metaIndexItems []MetaIndex

	inMemBlock MemoryReader
}

func (t *TableData) reset() {
	t.trailerData = t.trailerData[:0]
	t.bloomFilter = t.bloomFilter[:0]
	t.metaIndexItems = t.metaIndexItems[:0]
	if t.inMemBlock != nil {
		t.inMemBlock.Reset()
	}
}

func minTableSize() int64 {
	return int64(len(tableMagic)+trailerSize) + 8 + 8
}

type ColumnBuilder struct {
	data    []byte
	cm      *ChunkMeta
	colMeta *ColumnMeta
	segCol  []record.ColVal

	intPreAggBuilder    PreAggBuilder
	floatPreAggBuilder  PreAggBuilder
	stringPreAggBuilder PreAggBuilder
	boolPreAggBuilder   PreAggBuilder
	timePreAggBuilder   PreAggBuilder

	coder *CoderContext
	log   *Log.Logger
}

func (b *ColumnBuilder) resetPreAgg() {
	if b.timePreAggBuilder != nil {
		b.timePreAggBuilder.reset()
	}
	if b.stringPreAggBuilder != nil {
		b.stringPreAggBuilder.reset()
	}
	if b.intPreAggBuilder != nil {
		b.intPreAggBuilder.reset()
	}
	if b.floatPreAggBuilder != nil {
		b.floatPreAggBuilder.reset()
	}
	if b.boolPreAggBuilder != nil {
		b.boolPreAggBuilder.reset()
	}
}

func (b *ColumnBuilder) initEncoder(ref record.Field) error {
	if ref.Name == record.TimeField && ref.Type == influx.Field_Type_Int {
		if b.coder.timeCoder == nil {
			b.coder.timeCoder = GetTimeCoder()
		}
		if b.timePreAggBuilder == nil {
			b.timePreAggBuilder = acquireTimePreAggBuilder()
		}
		b.timePreAggBuilder.reset()
		return nil
	}

	switch ref.Type {
	case influx.Field_Type_Int:
		if b.coder.intCoder == nil {
			b.coder.intCoder = GetInterCoder()
		}
		if b.intPreAggBuilder == nil {
			b.intPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Int)
		}
		b.intPreAggBuilder.reset()
		return nil
	case influx.Field_Type_Float:
		if b.coder.floatCoder == nil {
			b.coder.floatCoder = GetFloatCoder()
		}
		if b.floatPreAggBuilder == nil {
			b.floatPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Float)
		}
		b.floatPreAggBuilder.reset()
		return nil
	case influx.Field_Type_String:
		if b.coder.stringCoder == nil {
			b.coder.stringCoder = GetStringCoder()
		}
		if b.stringPreAggBuilder == nil {
			b.stringPreAggBuilder = acquireColumnBuilder(influx.Field_Type_String)
		}
		b.stringPreAggBuilder.reset()
		return nil
	case influx.Field_Type_Boolean:
		if b.coder.boolCoder == nil {
			b.coder.boolCoder = GetBoolCoder()
		}
		if b.boolPreAggBuilder == nil {
			b.boolPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Boolean)
		}
		b.boolPreAggBuilder.reset()
		return nil
	default:
		err := fmt.Errorf("unknown column data type %v", ref.String())
		b.log.Error(err.Error())
		return err
	}
}

func (b *ColumnBuilder) encIntegerColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.intPreAggBuilder == nil {
		b.intPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Int)
	}
	b.intPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.intPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockInteger)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		b.data, err = EncodeIntegerBlock(segCol.Val, b.data, b.coder)
		if err != nil {
			b.log.Error("encode integer value fail", zap.Error(err))
			return err
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)
		offset += int64(size)
	}

	b.colMeta.preAgg = b.intPreAggBuilder.marshal(b.colMeta.preAgg[:0])

	return err
}

func (b *ColumnBuilder) encFloatColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.floatPreAggBuilder == nil {
		b.floatPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Float)
	}
	b.floatPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.floatPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockFloat64)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		b.data, err = EncodeFloatBlock(segCol.Val, b.data, b.coder)
		if err != nil {
			b.log.Error("encode float value fail", zap.Error(err))
			return err
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)

		offset += int64(size)
	}

	b.colMeta.preAgg = b.floatPreAggBuilder.marshal(b.colMeta.preAgg[:0])

	return err
}

func (b *ColumnBuilder) encStringColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.stringPreAggBuilder == nil {
		b.stringPreAggBuilder = acquireColumnBuilder(influx.Field_Type_String)
	}
	b.stringPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.stringPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockString)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		b.data, err = EncodeStringBlock(segCol.Val, segCol.Offset, b.data, b.coder)
		if err != nil {
			b.log.Error("encode string value fail", zap.Error(err))
			return err
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)

		offset += int64(size)
	}

	b.colMeta.preAgg = b.stringPreAggBuilder.marshal(b.colMeta.preAgg[:0])
	return err
}

func (b *ColumnBuilder) encBooleanColumn(timeCols []record.ColVal, segCols []record.ColVal, offset int64) error {
	var err error
	if b.boolPreAggBuilder == nil {
		b.boolPreAggBuilder = acquireColumnBuilder(influx.Field_Type_Boolean)
	}
	b.boolPreAggBuilder.reset()

	for i := range segCols {
		segCol := &segCols[i]
		tmCol := timeCols[i]
		if segCol.Length() != tmCol.Length() {
			err = fmt.Errorf("%v column rows not equal time rows, %v != %v", b.colMeta.name, segCol.Length(), tmCol.Length())
			b.log.Error(err.Error())
			panic(err)
		}

		times := tmCol.IntegerValues()
		b.boolPreAggBuilder.addValues(segCol, times)
		m := &b.colMeta.entries[i]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, BlockBoolean)
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))

		b.data, err = EncodeBooleanBlock(segCol.Val, b.data, b.coder)
		if err != nil {
			b.log.Error("encode boolean value fail", zap.Error(err))
			return err
		}

		size := uint32(len(b.data) - pos)
		m.setSize(size)

		offset += int64(size)
	}

	b.colMeta.preAgg = b.boolPreAggBuilder.marshal(b.colMeta.preAgg[:0])
	return err
}

func (b *ColumnBuilder) EncodeColumn(ref record.Field, col *record.ColVal, timeCols []record.ColVal, segRowsLimit int, dataOffset int64) ([]byte, error) {
	var err error
	b.segCol = col.Split(b.segCol[:0], segRowsLimit, ref.Type)
	b.colMeta.name = ref.Name
	b.colMeta.ty = byte(ref.Type)

	if len(b.segCol) != len(timeCols) {
		err = fmt.Errorf("%v segment not equal time segment, %v != %v", ref.Name, len(b.segCol), len(timeCols))
		b.log.Error(err.Error())
		panic(err)
	}

	switch ref.Type {
	case influx.Field_Type_Int:
		err = b.encIntegerColumn(timeCols, b.segCol, dataOffset)
	case influx.Field_Type_Float:
		err = b.encFloatColumn(timeCols, b.segCol, dataOffset)
	case influx.Field_Type_String:
		err = b.encStringColumn(timeCols, b.segCol, dataOffset)
	case influx.Field_Type_Boolean:
		err = b.encBooleanColumn(timeCols, b.segCol, dataOffset)
	}

	if err != nil {
		return nil, err
	}

	return b.data, nil
}

func (b *ColumnBuilder) set(dst []byte, m *ColumnMeta) {
	if b == nil {
		return
	}
	b.data = dst
	b.colMeta = m
}

func NewColumnBuilder() *ColumnBuilder {
	return &ColumnBuilder{coder: NewCoderContext()}
}

type ChunkDataBuilder struct {
	segmentLimit int
	maxRowsLimit int // must be multiple of 8
	chunk        []byte
	chunkMeta    *ChunkMeta

	colBuilder *ColumnBuilder
	timeCols   []record.ColVal
	log        *Log.Logger
}

func NewChunkDataBuilder(maxRowsPerSegment, maxSegmentLimit int) *ChunkDataBuilder {
	return &ChunkDataBuilder{
		segmentLimit: maxSegmentLimit,
		maxRowsLimit: maxRowsPerSegment,
		colBuilder:   NewColumnBuilder(),
	}
}

func (b *ChunkDataBuilder) setChunkMeta(cm *ChunkMeta) {
	b.chunkMeta = cm
}

func (b *ChunkDataBuilder) reset(dst []byte) {
	b.chunkMeta.reset()
	b.chunk = dst
}

func (b *ChunkDataBuilder) EncodeTime(offset int64) error {
	var err error
	if b.colBuilder.coder.timeCoder == nil {
		b.colBuilder.coder.timeCoder = GetTimeCoder()
	}

	if b.colBuilder.timePreAggBuilder == nil {
		b.colBuilder.timePreAggBuilder = acquireTimePreAggBuilder()
	}
	tb := b.colBuilder.timePreAggBuilder
	tb.reset()
	tm := b.chunkMeta.timeMeta()

	tm.name = record.TimeField
	tm.ty = influx.Field_Type_Int
	for i, col := range b.timeCols {
		values := col.IntegerValues()
		tb.addValues(nil, values)
		m := &tm.entries[i]

		pos := len(b.chunk)
		b.chunk = append(b.chunk, BlockInteger)
		nilBitMap, bitmapOffset := col.SubBitmapBytes()
		b.chunk = numberenc.MarshalUint32Append(b.chunk, uint32(len(nilBitMap)))
		b.chunk = append(b.chunk, nilBitMap...)
		b.chunk = numberenc.MarshalUint32Append(b.chunk, uint32(bitmapOffset))
		b.chunk = numberenc.MarshalUint32Append(b.chunk, uint32(col.NilCount))
		b.chunk, err = EncodeTimestampBlock(col.Val, b.chunk, b.colBuilder.coder)
		if err != nil {
			b.log.Error("encode integer value fail", zap.Error(err))
			return err
		}

		m.setOffset(offset)
		size := uint32(len(b.chunk) - pos)
		m.setSize(size)
		b.chunkMeta.timeRange[i].setMinTime(values[0])
		b.chunkMeta.timeRange[i].setMaxTime(values[len(values)-1])
		offset += int64(size)
		b.chunkMeta.size += size
	}
	tm.preAgg = tb.marshal(tm.preAgg[:0])

	return nil
}

func (b *ChunkDataBuilder) EncodeChunk(id uint64, offset int64, rec *record.Record, dst []byte) ([]byte, error) {
	var err error
	b.reset(dst)
	b.chunkMeta.sid = id
	b.chunkMeta.offset = offset
	b.chunkMeta.columnCount = uint32(rec.ColNums())
	if rec.RowNums() > b.maxRowsLimit*b.segmentLimit {
		return nil, fmt.Errorf("max rows %v for record greater than %v", rec.RowNums(), b.maxRowsLimit*b.segmentLimit)
	}
	timeCol := rec.TimeColumn()
	b.timeCols = timeCol.Split(b.timeCols[:0], b.maxRowsLimit, influx.Field_Type_Int)
	b.chunkMeta.segCount = uint16(len(b.timeCols))
	b.chunkMeta.resize(int(b.chunkMeta.columnCount), len(b.timeCols))

	for i := range rec.Schema[:len(rec.Schema)-1] {
		ref := rec.Schema[i]
		col := rec.Column(i)
		cm := &b.chunkMeta.colMeta[i]
		pos := len(b.chunk)
		b.chunk = numberenc.MarshalUint32Append(b.chunk, 0) // reserve crc32
		b.colBuilder.set(b.chunk, cm)
		offset += 4
		b.chunkMeta.size += 4
		if b.chunk, err = b.colBuilder.EncodeColumn(ref, col, b.timeCols, b.maxRowsLimit, offset); err != nil {
			b.log.Error("encode column fail", zap.Error(err))
			return nil, err
		}
		crc := crc32.ChecksumIEEE(b.chunk[pos+4:])
		numberenc.MarshalUint32Copy(b.chunk[pos:pos+4], crc)

		size := uint32(len(b.chunk) - pos - 4)
		b.chunkMeta.size += size
		offset += int64(size)
	}

	pos := len(b.chunk)
	b.chunk = numberenc.MarshalUint32Append(b.chunk, 0)
	offset += 4
	b.chunkMeta.size += 4
	if err = b.EncodeTime(offset); err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(b.chunk[pos+4:])
	numberenc.MarshalUint32Copy(b.chunk[pos:pos+4], crc)

	return b.chunk, nil
}

type MsBuilder struct {
	ref  int64
	Path string
	TableData
	Conf         *Config
	chunkBuilder *ChunkDataBuilder
	mIndex       MetaIndex
	trailer      *Trailer
	keys         map[uint64]struct{}
	bf           *bloom.Filter
	dataOffset   int64
	MaxIds       int

	trailerOffset     int64
	fd                fileops.File
	fileSize          int64
	diskFileWriter    FileWriter
	cmOffset          []uint32
	preCmOff          int64
	encodeChunk       []byte
	encChunkMeta      []byte
	encChunkIndexMeta []byte
	chunkMetaBlocks   [][]byte
	encIdTime         []byte
	inited            bool
	blockSizeIndex    int
	pair              IdTimePairs
	sequencer         *Sequencer
	msName            string
	tier              uint64
	cm                *ChunkMeta

	Files    []TSSPFile
	FileName TSSPFileName
	recs     []record.Record
	log      *Log.Logger
}

type MsBuilderPool struct {
	cache chan *MsBuilder
	pool  *sync.Pool
}

func NewMsBuilderPool() *MsBuilderPool {
	n := cpu.GetCpuNum()
	if n < 2 {
		n = 2
	}
	if n > 32 {
		n = 32
	}
	return &MsBuilderPool{
		cache: make(chan *MsBuilder, n),
		pool:  &sync.Pool{},
	}
}

/*func (p *MsBuilderPool) put(m *MsBuilder) {
	if atomic.AddInt64(&m.ref, -1) != 0 {
		panic(m)
	}
	select {
	case p.cache <- m:
	default:
		p.pool.Put(m)
	}
}*/

func (p *MsBuilderPool) get() *MsBuilder {
	select {
	case r := <-p.cache:
		if atomic.AddInt64(&r.ref, 1) > 1 {
			panic(r)
		}
		return r
	default:
		if v := p.pool.Get(); v != nil {
			r, ok := v.(*MsBuilder)
			if !ok {
				logger.GetLogger().Error("MsBuilderPool pool Get type isn't *MsBuilder")
				panic(r)
			}
			if atomic.AddInt64(&r.ref, 1) > 1 {
				panic(r)
			}
			return r
		}
		return &MsBuilder{ref: 1}
	}
}

var msBuilderPool = NewMsBuilderPool()

func GetMsBuilder(dir, name string, fileName TSSPFileName, conf *Config,
	sequencer *Sequencer, tier uint64, estimateSize int) *MsBuilder {
	msBuilder := msBuilderPool.get()

	if msBuilder.chunkBuilder == nil {
		msBuilder.chunkBuilder = NewChunkDataBuilder(conf.maxRowsPerSegment, conf.maxSegmentLimit)
	} else {
		msBuilder.chunkBuilder.maxRowsLimit = conf.maxRowsPerSegment
		msBuilder.chunkBuilder.colBuilder = NewColumnBuilder()
	}

	if msBuilder.trailer == nil {
		msBuilder.trailer = &Trailer{}
	}

	msBuilder.log = Log.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	msBuilder.tier = tier
	msBuilder.Conf = conf
	msBuilder.trailer.name = append(msBuilder.trailer.name[:0], name...)
	msBuilder.Path = dir
	msBuilder.inMemBlock = emptyMemReader
	if msBuilder.cacheDataInMemory() || msBuilder.cacheMetaInMemory() {
		idx := calcBlockIndex(estimateSize)
		msBuilder.inMemBlock = NewMemoryReader(blockSize[idx])
	}

	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	dir = filepath.Join(dir, name)
	_ = fileops.MkdirAll(dir, 0750, lock)
	filePath := fileName.Path(dir, true)
	_, err := fileops.Stat(filePath)
	if err == nil {
		panic(fmt.Sprintf("file(%v) exist", filePath))
	}
	msBuilder.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		panic(err)
	}

	limit := fileName.level > 0
	if msBuilder.cacheMetaInMemory() {
		msBuilder.diskFileWriter = newFileWriter(msBuilder.fd, true, limit)
	} else {
		msBuilder.diskFileWriter = newFileWriter(msBuilder.fd, false, limit)
	}
	msBuilder.keys = make(map[uint64]struct{}, 256)
	msBuilder.sequencer = sequencer
	msBuilder.msName = name
	msBuilder.Files = msBuilder.Files[:0]
	msBuilder.FileName = fileName

	return msBuilder
}

func PutMsBuilder(msBuilder *MsBuilder) {
	//msBuilder.Reset()
	//msBuilderPool.put(msBuilder)
}

func (b *MsBuilder) MaxRowsPerSegment() int {
	return b.Conf.maxRowsPerSegment
}

func (b *MsBuilder) WithLog(log *Log.Logger) {
	b.log = log
	if b.chunkBuilder != nil {
		b.chunkBuilder.log = log
		if b.chunkBuilder.colBuilder != nil {
			b.chunkBuilder.colBuilder.log = log
		}
	}
}

func (b *MsBuilder) Reset() {
	b.reset()

	b.dataOffset = 0
	b.mIndex.reset()
	b.trailer.reset()
	b.Path = ""
	b.releaseEncoders()
	b.encodeChunk = b.encodeChunk[:0]
	b.inited = false
	if b.fd != nil {
		_ = b.fd.Close()
		b.fd = nil
	}
	if b.diskFileWriter != nil {
		_ = b.diskFileWriter.Close()
	}
	b.fileSize = 0
	b.Files = b.Files[:0]
}

func (b *MsBuilder) writeToDisk(rowCounts int64) error {
	wn, err := b.diskFileWriter.WriteData(b.encodeChunk)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk data fail", zap.Error(err))
		return err
	}
	if wn != len(b.encodeChunk) {
		b.log.Error("write chunk data fail", zap.String("file", b.fd.Name()),
			zap.Ints("size", []int{len(b.encodeChunk), wn}))
		return io.ErrShortWrite
	}

	cm := b.cm
	minT, maxT := cm.MinMaxTime()
	if b.mIndex.count == 0 {
		b.mIndex.size = 0
		b.mIndex.id = cm.sid
		b.mIndex.minTime = minT
		b.mIndex.maxTime = maxT
		b.mIndex.offset = b.diskFileWriter.ChunkMetaSize()
	}

	if b.FileName.order {
		b.pair.Add(cm.sid, maxT)
	}
	b.pair.AddRowCounts(rowCounts)

	b.encChunkMeta = cm.marshal(b.encChunkMeta[:0])
	cmOff := b.diskFileWriter.ChunkMetaSize()
	b.cmOffset = append(b.cmOffset, uint32(cmOff-b.preCmOff))
	wn, err = b.diskFileWriter.WriteChunkMeta(b.encChunkMeta)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk meta fail", zap.Error(err))
		return err
	}
	if wn != len(b.encChunkMeta) {
		b.log.Error("write chunk meta fail", zap.String("file", b.fd.Name()), zap.Ints("size", []int{len(b.encChunkMeta), wn}))
		return io.ErrShortWrite
	}

	b.mIndex.size += uint32(wn) + 4
	b.mIndex.count++
	if b.mIndex.minTime > minT {
		b.mIndex.minTime = minT
	}
	if b.mIndex.maxTime < maxT {
		b.mIndex.maxTime = maxT
	}

	if b.mIndex.size >= uint32(b.Conf.maxChunkMetaItemSize) || b.mIndex.count >= uint32(b.Conf.maxChunkMetaItemCount) {
		offBytes := record.Uint32Slice2byte(b.cmOffset)
		_, err = b.diskFileWriter.WriteChunkMeta(offBytes)
		if err != nil {
			err = errWriteFail(b.diskFileWriter.Name(), err)
			b.log.Error("write chunk meta fail", zap.Error(err))
			return err
		}
		b.metaIndexItems = append(b.metaIndexItems, b.mIndex)
		b.mIndex.reset()
		b.cmOffset = b.cmOffset[:0]
		b.preCmOff = b.diskFileWriter.ChunkMetaSize()
		b.diskFileWriter.SwitchMetaBuffer()
	}

	return nil
}

func (b *MsBuilder) WriteRecord(id uint64, data *record.Record, nextFile func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16)) (*MsBuilder, error) {
	rowsLimit := b.Conf.maxRowsPerSegment * b.Conf.maxSegmentLimit
	msb := b
	msb.recs = data.Split(msb.recs[:0], rowsLimit)
	recs := msb.recs
	msb.recs = nil
	for i := range recs {
		if err := msb.WriteData(id, &recs[i]); err != nil {
			msb.log.Error("write data record fail", zap.String("file", msb.fd.Name()), zap.Error(err))
			return msb, err
		}

		fSize := msb.Size()
		if i < len(recs)-1 || fSize >= b.Conf.fileSizeLimit {
			f, err := msb.NewTSSPFile(true)
			if err != nil {
				msb.log.Error("new file fail", zap.String("file", msb.fd.Name()), zap.Error(err))
				return msb, err
			}

			msb.log.Info("switch tssp file",
				zap.String("file", f.Path()),
				zap.Int("rowsLimit", rowsLimit),
				zap.Int("rows", data.RowNums()),
				zap.Int64("fileSize", fSize),
				zap.Int64("sizeLimit", b.Conf.fileSizeLimit))

			msb.Files = append(msb.Files, f)
			seq, lv, merge, ext := nextFile(msb.FileName)
			msb.FileName.SetSeq(seq)
			msb.FileName.SetMerge(merge)
			msb.FileName.SetExtend(ext)
			msb.FileName.SetLevel(lv)
			n := msb.MaxIds
			builder := AllocMsBuilder(msb.Path, msb.Name(), msb.Conf, n, msb.FileName, msb.tier, msb.sequencer, recs[i].Len())
			builder.Files = append(builder.Files, msb.Files...)
			builder.WithLog(msb.log)
			PutMsBuilder(msb)
			msb = builder
		}
	}
	msb.recs = recs
	return msb, nil
}

func (b *MsBuilder) NewTSSPFile(tmp bool) (TSSPFile, error) {
	if err := b.Flush(); err != nil {
		if err == errEmptyFile {
			b.removeEmptyFile()
			return nil, nil
		}
		b.log.Error("flush error", zap.String("name", b.fd.Name()), zap.Error(err))
		return nil, err
	}

	dr, err := CreateTSSPFileReader(b.fileSize, b.fd, b.trailer, &b.TableData, b.FileVersion(), tmp)
	if err != nil {
		b.log.Error("create tssp file reader fail", zap.String("name", dr.FileName()), zap.Error(err))
		return nil, err
	}

	dr.avgChunkRows = 0
	dr.maxChunkRows = 0
	for _, rows := range b.pair.Rows {
		n := int(rows)
		if dr.maxChunkRows < n {
			dr.maxChunkRows = n
		}
		dr.avgChunkRows += n
	}
	dr.avgChunkRows /= len(b.pair.Rows)

	size := dr.InMemSize()
	if b.FileName.order {
		addMemSize(levelName(b.FileName.level), size, size, 0)
	} else {
		addMemSize(levelName(b.FileName.level), size, 0, size)
	}

	///todo for test check, delete after the version is stable
	validateFileName(b.FileName, dr.FileName())
	return &tsspFile{
		name:   b.FileName,
		reader: dr,
		ref:    1,
	}, nil
}

func validateFileName(msbFileName TSSPFileName, filePath string) {
	var fName TSSPFileName
	if err := fName.ParseFileName(filePath); err != nil {
		panic(err)
	}
	order := strings.Contains(filePath, "out-of-order")
	fName.SetOrder(!order)
	if fName != msbFileName {
		panic(fmt.Sprintf("fName:%v, bFName:%v", fName, msbFileName))
	}
}

func (b *MsBuilder) WriteData(id uint64, data *record.Record) error {
	record.CheckRecord(data)

	var err error
	if b.trailer.maxId != 0 {
		if id <= b.trailer.maxId {
			err = fmt.Errorf("file(%v) series id(%d) must be greater than %d", b.fd.Name(), id, b.trailer.maxId)
			b.log.Error("Invalid series id", zap.Error(err))
			return err
		}
	}

	b.encodeChunk = b.encodeChunk[:0]
	if !b.inited {
		b.pair.Reset(b.msName)
		b.inited = true
		b.cmOffset = b.cmOffset[:0]
		b.preCmOff = 0
		if b.cacheDataInMemory() {
			size := EstimateBufferSize(data.Size(), b.MaxIds)
			b.blockSizeIndex = calcBlockIndex(size)
		}
		b.encodeChunk = append(b.encodeChunk, tableMagic...)
		b.encodeChunk = numberenc.MarshalUint64Append(b.encodeChunk, version)
		b.dataOffset = int64(len(b.encodeChunk))
		if b.cm == nil {
			b.cm = &ChunkMeta{}
		}
	}

	b.chunkBuilder.setChunkMeta(b.cm)
	b.encodeChunk, err = b.chunkBuilder.EncodeChunk(id, b.dataOffset, data, b.encodeChunk)
	if err != nil {
		b.log.Error("encode chunk fail", zap.Error(err))
		return err
	}
	b.dataOffset += int64(b.cm.size)

	if err = b.writeToDisk(int64(data.RowNums())); err != nil {
		return err
	}

	minTime, maxTime := b.cm.MinMaxTime()
	if b.trailer.idCount == 0 {
		b.trailer.minTime = minTime
		b.trailer.maxTime = maxTime
		b.trailer.minId = id
	}

	b.trailer.idCount++
	b.trailer.maxId = id
	if b.trailer.minTime > minTime {
		b.trailer.minTime = minTime
	}
	if b.trailer.maxTime < maxTime {
		b.trailer.maxTime = maxTime
	}

	b.keys[id] = struct{}{}

	if !b.cacheDataInMemory() {
		return nil
	}

	b.inMemBlock.AppendDataBlock(b.encodeChunk)

	return nil
}

func (b *MsBuilder) releaseEncoders() {
	if b.chunkBuilder != nil {
		if b.chunkBuilder.colBuilder != nil {
			colBuilder := b.chunkBuilder.colBuilder
			if colBuilder != nil {
				if colBuilder.intPreAggBuilder != nil {
					colBuilder.intPreAggBuilder.release()
					colBuilder.intPreAggBuilder = nil
				}

				if colBuilder.floatPreAggBuilder != nil {
					colBuilder.floatPreAggBuilder.release()
					colBuilder.floatPreAggBuilder = nil
				}

				if colBuilder.stringPreAggBuilder != nil {
					colBuilder.stringPreAggBuilder.release()
					colBuilder.stringPreAggBuilder = nil
				}

				if colBuilder.boolPreAggBuilder != nil {
					colBuilder.boolPreAggBuilder.release()
					colBuilder.boolPreAggBuilder = nil
				}

				colBuilder.coder.Release()
			}
		}
	}
}

func (b *MsBuilder) genBloomFilter() {
	bm, bk := bloom.Estimate(uint64(len(b.keys)), falsePositive)
	bmBytes := pow2((bm + 7) / 8)
	if uint64(cap(b.bloomFilter)) < bmBytes {
		b.bloomFilter = make([]byte, bmBytes)
	} else {
		b.bloomFilter = b.bloomFilter[:bmBytes]
		record.MemorySet(b.bloomFilter)
	}
	b.trailer.bloomM = bm
	b.trailer.bloomK = bk
	b.bf, _ = bloom.NewFilterBuffer(b.bloomFilter, bk)
	for id := range b.keys {
		b.bf.Insert(record.Uint64ToBytes(id))
	}
}

func (b *MsBuilder) Flush() error {
	if b.diskFileWriter.DataSize() <= 16 {
		return errEmptyFile
	}

	if b.mIndex.count > 0 {
		offBytes := record.Uint32Slice2byte(b.cmOffset)
		_, err := b.diskFileWriter.WriteChunkMeta(offBytes)
		if err != nil {
			b.log.Error("write chunk meta fail", zap.String("name", b.fd.Name()), zap.Error(err))
			return err
		}
		b.metaIndexItems = append(b.metaIndexItems, b.mIndex)
	}

	b.genBloomFilter()
	b.trailer.dataOffset = int64(len(tableMagic) + int(unsafe.Sizeof(version)))
	b.trailer.dataSize = b.diskFileWriter.DataSize() - b.trailer.dataOffset
	metaOff := b.diskFileWriter.DataSize()
	b.trailer.indexSize = b.diskFileWriter.ChunkMetaSize()

	if err := b.diskFileWriter.AppendChunkMetaToData(); err != nil {
		b.log.Error("copy chunk meta fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}
	if b.cacheMetaInMemory() {
		b.chunkMetaBlocks = b.diskFileWriter.MetaDataBlocks(b.chunkMetaBlocks[:0])
		b.inMemBlock.SetMetaBlocks(b.chunkMetaBlocks)
	}

	miOff := b.diskFileWriter.DataSize()
	for i := range b.metaIndexItems {
		m := &b.metaIndexItems[i]
		m.offset += metaOff
		b.encChunkIndexMeta = m.marshal(b.encChunkIndexMeta[:0])
		_, err := b.diskFileWriter.WriteData(b.encChunkIndexMeta)
		if err != nil {
			b.log.Error("write meta index fail", zap.String("name", b.fd.Name()), zap.Error(err))
			return err
		}
	}

	b.trailer.metaIndexSize = b.diskFileWriter.DataSize() - miOff
	b.trailer.metaIndexItemNum = int64(len(b.metaIndexItems))
	b.trailer.bloomSize = int64(len(b.bloomFilter))

	if _, err := b.diskFileWriter.WriteData(b.bloomFilter); err != nil {
		b.log.Error("write bloom filter fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}

	if b.sequencer != nil {
		b.sequencer.BatchUpdate(&b.pair)
	}

	b.encIdTime = b.pair.Marshal(b.FileName.order, b.encIdTime[:0], b.chunkBuilder.colBuilder.coder)
	b.trailer.idTimeSize = int64(len(b.encIdTime))
	if _, err := b.diskFileWriter.WriteData(b.encIdTime); err != nil {
		b.log.Error("write id time data fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}
	b.trailerData = b.trailer.marshal(b.trailerData[:0])

	b.trailerOffset = b.diskFileWriter.DataSize()
	if _, err := b.diskFileWriter.WriteData(b.trailerData); err != nil {
		b.log.Error("write trailer fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}

	var footer [8]byte
	fb := numberenc.MarshalInt64Append(footer[:0], b.trailerOffset)
	if _, err := b.diskFileWriter.WriteData(fb); err != nil {
		b.log.Error("write footer fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}

	b.fileSize = b.diskFileWriter.DataSize()
	if err := b.diskFileWriter.Close(); err != nil {
		b.log.Error("close file fail", zap.String("name", b.fd.Name()), zap.Error(err))
	}
	b.diskFileWriter = nil

	return nil
}

func (b *MsBuilder) Name() string {
	return string(b.trailer.name)
}

func (b *MsBuilder) Size() int64 {
	if b.diskFileWriter.DataSize() <= 16 {
		return 0
	}
	n := b.diskFileWriter.DataSize()
	n += b.diskFileWriter.ChunkMetaSize()
	n += int64(len(b.metaIndexItems) * MetaIndexLen)
	bm, _ := bloom.Estimate(uint64(len(b.keys)), falsePositive)
	bmBytes := pow2((bm + 7) / 8)
	n += int64(bmBytes) + int64(trailerSize+len(b.Name()))
	n += int64(b.pair.Len()*3*8) / 2 // assuming the compression ratio is 50%
	return n
}

func (b *MsBuilder) removeEmptyFile() {
	if b.diskFileWriter != nil && b.fd != nil {
		_ = b.diskFileWriter.Close()
		name := b.fd.Name()
		_ = b.fd.Close()
		_ = fileops.Remove(name)
		b.fd = nil
		b.diskFileWriter = nil
	}
}

func (b *MsBuilder) FileVersion() uint64 {
	return version ///todo ???
}

func (b *MsBuilder) cacheDataInMemory() bool {
	if b.tier == meta.Hot {
		return b.Conf.cacheDataBlock
	} else if b.tier == meta.Warm {
		return false
	}

	return b.Conf.cacheDataBlock
}

func (b *MsBuilder) cacheMetaInMemory() bool {
	if b.tier == meta.Hot {
		return b.Conf.cacheMetaData
	} else if b.tier == meta.Warm {
		return false
	}

	return b.Conf.cacheMetaData
}

func AllocMsBuilder(dir string, name string, conf *Config, idCount int, fileName TSSPFileName,
	tier uint64, sequencer *Sequencer, estimateSize int) *MsBuilder {
	builder := GetMsBuilder(dir, name, fileName, conf, sequencer, tier, estimateSize)
	builder.MaxIds = idCount
	if builder.cacheMetaInMemory() {
		n := idCount/DefaultMaxChunkMetaItemCount + 1
		builder.inMemBlock.ReserveMetaBlock(n)
	} else {
		if builder.cm == nil {
			builder.cm = &ChunkMeta{}
		}
	}
	builder.bf = nil

	return builder
}

func pow2(v uint64) uint64 {
	for i := uint64(8); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

var (
	blockSize     = []int{2 * kb, 4 * kb, 8 * kb, 16 * kb, 32 * kb, 64 * kb, 128 * kb, 256 * kb, 512 * kb, 1024 * kb, 2048 * kb, 4096 * kb}
	dataBlockPool = make([]sync.Pool, len(blockSize))
)

func calcBlockIndex(size int) int {
	for i := range blockSize {
		if size <= blockSize[i] {
			return i
		}
	}

	return len(blockSize) - 1
}

func getDataBlockBuffer(size int) []byte {
	idx := calcBlockIndex(size)
	size = blockSize[idx]
	v := dataBlockPool[idx].Get()
	if v == nil {
		return make([]byte, 0, size)
	}

	return v.([]byte)
}

// nolint
func putDataBlockBuffer(b []byte) {
	if cap(b) == 0 {
		return
	}

	b = b[:0]
	idx := calcBlockIndex(cap(b))
	dataBlockPool[idx].Put(b)
}

func freeDataBlocks(buffers [][]byte) int {
	n := 0
	for i := range buffers {
		n += cap(buffers[i])
		putDataBlockBuffer(buffers[i])
		buffers[i] = nil
	}
	return n
}

func EstimateBufferSize(recSize int, rows int) int {
	return rows * recSize
}
