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

	"github.com/openGemini/openGemini/lib/encoding"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const crcSize = 4

type ChunkDataBuilder struct {
	segmentLimit     int
	maxRowsLimit     int // must be multiple of 8
	position         int // segment entries position
	chunk            []byte
	chunkMeta        *ChunkMeta
	preChunkMetaSize uint32

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
	if b.colBuilder.coder.GetTimeCoder() == nil {
		b.colBuilder.coder.SetTimeCoder(encoding.GetTimeCoder())
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
		m := &tm.entries[i+b.position]

		pos := len(b.chunk)
		b.chunk = append(b.chunk, encoding.BlockInteger)
		nilBitMap, bitmapOffset := col.SubBitmapBytes()
		b.chunk = numberenc.MarshalUint32Append(b.chunk, uint32(len(nilBitMap)))
		b.chunk = append(b.chunk, nilBitMap...)
		b.chunk = numberenc.MarshalUint32Append(b.chunk, uint32(bitmapOffset))
		b.chunk = numberenc.MarshalUint32Append(b.chunk, uint32(col.NilCount))
		b.chunk, err = encoding.EncodeTimestampBlock(col.Val, b.chunk, b.colBuilder.coder)
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
	b.chunkMeta.segCount = uint32(len(b.timeCols))
	b.chunkMeta.resize(int(b.chunkMeta.columnCount), len(b.timeCols))

	for i := range rec.Schema[:len(rec.Schema)-1] {
		ref := rec.Schema[i]
		col := rec.Column(i)
		cm := &b.chunkMeta.colMeta[i]
		pos := len(b.chunk)
		b.chunk = numberenc.MarshalUint32Append(b.chunk, 0) // reserve crc32
		b.colBuilder.set(b.chunk, cm)
		offset += crcSize
		b.chunkMeta.size += crcSize
		if b.chunk, err = b.colBuilder.EncodeColumn(ref, col, b.timeCols, b.maxRowsLimit, offset); err != nil {
			b.log.Error("encode column fail", zap.Error(err))
			return nil, err
		}
		crc := crc32.ChecksumIEEE(b.chunk[pos+crcSize:])
		numberenc.MarshalUint32Copy(b.chunk[pos:pos+crcSize], crc)

		size := uint32(len(b.chunk) - pos - crcSize)
		b.chunkMeta.size += size
		offset += int64(size)
	}

	pos := len(b.chunk)
	b.chunk = numberenc.MarshalUint32Append(b.chunk, 0)
	offset += crcSize
	b.chunkMeta.size += crcSize
	if err = b.EncodeTime(offset); err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(b.chunk[pos+crcSize:])
	numberenc.MarshalUint32Copy(b.chunk[pos:pos+crcSize], crc)

	return b.chunk, nil
}

func (b *ChunkDataBuilder) EncodeChunkForColumnStore(offset int64, rec *record.Record, dst []byte) ([]byte, error) {
	var err error
	b.chunk = dst
	timeCol := rec.TimeColumn()
	b.timeCols = timeCol.Split(b.timeCols[:0], b.maxRowsLimit, influx.Field_Type_Int)
	b.chunkMeta.segCount += uint32(len(b.timeCols))
	var entriesCount int
	if len(b.chunkMeta.colMeta) != 0 {
		entriesCount = len(b.chunkMeta.timeMeta().entries)
	}
	b.chunkMeta.resize(int(b.chunkMeta.columnCount), len(b.timeCols)+entriesCount)
	b.colBuilder.position = b.position
	b.preChunkMetaSize = b.chunkMeta.size

	for i := range rec.Schema[:len(rec.Schema)-1] {
		ref := rec.Schema[i]
		col := rec.Column(i)
		cm := &b.chunkMeta.colMeta[i]
		pos := len(b.chunk)
		b.chunk = numberenc.MarshalUint32Append(b.chunk, 0) // reserve crc32
		b.colBuilder.set(b.chunk, cm)
		offset += crcSize
		b.chunkMeta.size += crcSize
		if b.chunk, err = b.colBuilder.EncodeColumn(ref, col, b.timeCols, b.maxRowsLimit, offset); err != nil {
			b.log.Error("encode column fail", zap.Error(err))
			return nil, err
		}
		crc := crc32.ChecksumIEEE(b.chunk[pos+crcSize:])
		numberenc.MarshalUint32Copy(b.chunk[pos:pos+crcSize], crc)

		size := uint32(len(b.chunk) - pos - crcSize)
		b.chunkMeta.size += size
		offset += int64(size)
	}

	pos := len(b.chunk)
	b.chunk = numberenc.MarshalUint32Append(b.chunk, 0)
	offset += crcSize
	b.chunkMeta.size += crcSize
	if err = b.EncodeTime(offset); err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(b.chunk[pos+crcSize:])
	numberenc.MarshalUint32Copy(b.chunk[pos:pos+crcSize], crc)
	b.position += len(b.timeCols)
	return b.chunk, nil
}
