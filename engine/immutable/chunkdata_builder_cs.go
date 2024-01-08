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
	"hash/crc32"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

type CsChunkDataImp struct {
	accumulateRowsIndex []int
	writeDetached       bool
}

func (c *CsChunkDataImp) EncodeChunk(b *ChunkDataBuilder, id uint64, offset int64, rec *record.Record, dst []byte, timeSorted bool) ([]byte, error) {
	b.reset(dst)
	b.chunkMeta.sid = id
	b.chunkMeta.offset = offset
	b.chunkMeta.columnCount = uint32(rec.ColNums())
	if rec.RowNums() > b.maxRowsLimit*b.segmentLimit {
		return nil, fmt.Errorf("max rows %v for record greater than %v", rec.RowNums(), b.maxRowsLimit*b.segmentLimit)
	}
	timeCol := rec.TimeColumn()
	b.timeCols = timeCol.SplitColBySize(b.timeCols[:0], c.accumulateRowsIndex, influx.Field_Type_Int)
	b.chunkMeta.segCount = uint32(len(b.timeCols))
	b.chunkMeta.resize(int(b.chunkMeta.columnCount), len(b.timeCols))

	b.colBuilder.SetEncodeMode(c.writeDetached)
	if c.writeDetached {
		return c.encodeChunkDetached(b, offset, rec, timeSorted)
	} else {
		return c.encodeChunk(b, offset, rec, timeSorted)
	}
}

func (c *CsChunkDataImp) encodeChunkDetached(b *ChunkDataBuilder, offset int64, rec *record.Record, timeSorted bool) ([]byte, error) {
	var err error
	for i := range rec.Schema[:len(rec.Schema)-1] {
		ref := rec.Schema[i]
		col := rec.Column(i)
		cm := &b.chunkMeta.colMeta[i]
		pos := len(b.chunk)
		b.colBuilder.set(b.chunk, cm)
		if b.chunk, err = b.colBuilder.EncodeColumnBySize(ref, col, b.timeCols, c.accumulateRowsIndex, offset); err != nil {
			b.log.Error("encode column fail", zap.Error(err))
			return nil, err
		}
		size := uint32(len(b.chunk) - pos)
		b.chunkMeta.size += size
		offset += int64(size)
	}

	if err = b.EncodeTime(offset, timeSorted); err != nil {
		return nil, err
	}
	return b.chunk, nil
}

func (c *CsChunkDataImp) encodeChunk(b *ChunkDataBuilder, offset int64, rec *record.Record, timeSorted bool) ([]byte, error) {
	var err error
	for i := range rec.Schema[:len(rec.Schema)-1] {
		ref := rec.Schema[i]
		col := rec.Column(i)
		cm := &b.chunkMeta.colMeta[i]
		pos := len(b.chunk)
		b.chunk = numberenc.MarshalUint32Append(b.chunk, 0) // reserve crc32
		b.colBuilder.set(b.chunk, cm)
		offset += crcSize
		b.chunkMeta.size += crcSize
		if b.chunk, err = b.colBuilder.EncodeColumnBySize(ref, col, b.timeCols, c.accumulateRowsIndex, offset); err != nil {
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
	if err = b.EncodeTime(offset, timeSorted); err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(b.chunk[pos+crcSize:])
	numberenc.MarshalUint32Copy(b.chunk[pos:pos+crcSize], crc)
	return b.chunk, nil
}

func (c *CsChunkDataImp) EncodeChunkForCompaction(b *ChunkDataBuilder, offset int64, rec *record.Record, dst []byte, accumulateRowsIndex []int, timeSorted bool) ([]byte, error) {
	b.chunk = dst
	timeCol := rec.TimeColumn()
	//split ColVal by size or fixNums
	if len(accumulateRowsIndex) > 0 {
		b.timeCols = timeCol.SplitColBySize(b.timeCols[:0], accumulateRowsIndex, influx.Field_Type_Int)
	} else {
		b.timeCols = timeCol.Split(b.timeCols[:0], b.maxRowsLimit, influx.Field_Type_Int)
	}

	b.chunkMeta.segCount += uint32(len(b.timeCols))
	var entriesCount int
	if len(b.chunkMeta.colMeta) != 0 {
		entriesCount = len(b.chunkMeta.timeMeta().entries)
	}
	b.chunkMeta.resize(int(b.chunkMeta.columnCount), len(b.timeCols)+entriesCount)
	b.colBuilder.position = b.position
	b.preChunkMetaSize = b.chunkMeta.size
	b.colBuilder.SetEncodeMode(c.writeDetached)
	if c.writeDetached {
		return c.encodeChunkDetachedForCompact(b, offset, rec, accumulateRowsIndex, timeSorted)
	} else {
		return c.encodeChunkAttachedForCompact(b, offset, rec, accumulateRowsIndex, timeSorted)
	}
}

func (c *CsChunkDataImp) encodeChunkAttachedForCompact(b *ChunkDataBuilder, offset int64, rec *record.Record, accumulateRowsIndex []int, timeSorted bool) ([]byte, error) {
	var err error
	for i := range rec.Schema[:len(rec.Schema)-1] {
		ref := rec.Schema[i]
		col := rec.Column(i)
		cm := &b.chunkMeta.colMeta[i]
		pos := len(b.chunk)
		b.chunk = numberenc.MarshalUint32Append(b.chunk, 0) // reserve crc32
		b.colBuilder.set(b.chunk, cm)
		offset += crcSize
		b.chunkMeta.size += crcSize
		if len(accumulateRowsIndex) > 0 {
			b.chunk, err = b.colBuilder.EncodeColumnBySize(ref, col, b.timeCols, accumulateRowsIndex, offset)
		} else {
			b.chunk, err = b.colBuilder.EncodeColumn(ref, col, b.timeCols, b.maxRowsLimit, offset)
		}
		if err != nil {
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
	if err = b.EncodeTime(offset, timeSorted); err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(b.chunk[pos+crcSize:])
	numberenc.MarshalUint32Copy(b.chunk[pos:pos+crcSize], crc)
	b.position += len(b.timeCols)
	return b.chunk, nil
}

func (c *CsChunkDataImp) encodeChunkDetachedForCompact(b *ChunkDataBuilder, offset int64, rec *record.Record, accumulateRowsIndex []int, timeSorted bool) ([]byte, error) {
	var err error
	for i := range rec.Schema[:len(rec.Schema)-1] {
		ref := rec.Schema[i]
		col := rec.Column(i)
		cm := &b.chunkMeta.colMeta[i]
		pos := len(b.chunk)
		b.colBuilder.set(b.chunk, cm)
		if len(accumulateRowsIndex) > 0 {
			b.chunk, err = b.colBuilder.EncodeColumnBySize(ref, col, b.timeCols, accumulateRowsIndex, offset)
		} else {
			b.chunk, err = b.colBuilder.EncodeColumn(ref, col, b.timeCols, b.maxRowsLimit, offset)
		}
		if err != nil {
			b.log.Error("encode column fail", zap.Error(err))
			return nil, err
		}
		size := uint32(len(b.chunk) - pos)
		b.chunkMeta.size += size
		offset += int64(size)
	}

	if err = b.EncodeTime(offset, timeSorted); err != nil {
		return nil, err
	}
	b.position += len(b.timeCols)
	return b.chunk, nil
}

func (c *CsChunkDataImp) SetAccumulateRowsIndex(accumulateRowsIndex []int) {
	c.accumulateRowsIndex = accumulateRowsIndex
}

func (c *CsChunkDataImp) SetDetachedInfo(writeDetached bool) {
	c.writeDetached = writeDetached
}
