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
	"math"

	"github.com/openGemini/openGemini/lib/encoding"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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
	minT, maxT       int64

	colBuilder *ColumnBuilder
	timeCols   []record.ColVal
	log        *Log.Logger
}

func NewChunkDataBuilder(maxRowsPerSegment, maxSegmentLimit int) *ChunkDataBuilder {
	return &ChunkDataBuilder{
		minT:         math.MaxInt64,
		maxT:         math.MinInt64,
		segmentLimit: maxSegmentLimit,
		maxRowsLimit: maxRowsPerSegment,
		colBuilder:   NewColumnBuilder(),
	}
}

func (b *ChunkDataBuilder) reset(dst []byte) {
	b.chunkMeta.reset()
	b.chunk = dst
}

func (b *ChunkDataBuilder) getMinMaxTime(timeSorted bool) (int64, int64) {
	if timeSorted {
		return b.chunkMeta.MinMaxTime()
	}
	return b.minT, b.maxT
}

func (b *ChunkDataBuilder) EncodeTime(offset int64, timeSorted bool) error {
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
		m.setOffset(offset)
		if b.colBuilder.encodeMode != nil {
			b.chunk = b.colBuilder.encodeMode.reserveCrc(b.chunk)
		}

		if CanEncodeOneRowMode(&col) {
			b.chunk = append(b.chunk, encoding.BlockIntegerOne)
			b.chunk = append(b.chunk, col.Val...)
		} else {
			b.chunk = EncodeColumnHeader(&col, b.chunk, encoding.BlockInteger)
			b.chunk, err = encoding.EncodeTimestampBlock(col.Val, b.chunk, b.colBuilder.coder)
			if err != nil {
				b.log.Error("encode integer value fail", zap.Error(err))
				return err
			}
		}

		if b.colBuilder.encodeMode != nil {
			b.chunk = b.colBuilder.encodeMode.setCrc(b.chunk, pos)
		}
		size := uint32(len(b.chunk) - pos)
		m.setSize(size)
		b.updateTimeRange(i, values, timeSorted)
		offset += int64(size)
		b.chunkMeta.size += size
	}
	tm.preAgg = tb.marshal(tm.preAgg[:0])

	return nil
}

func (b *ChunkDataBuilder) updateTimeRange(idx int, values []int64, timeSorted bool) {
	minT, maxT := values[0], values[len(values)-1]
	if !timeSorted {
		for i := range values {
			if maxT < values[i] {
				maxT = values[i]
			}

			if minT > values[i] {
				minT = values[i]
			}
		}
	}

	if b.maxT < maxT {
		b.maxT = maxT
	}
	if b.minT > minT {
		b.minT = minT
	}

	b.chunkMeta.timeRange[idx+b.position].setMinTime(minT)
	b.chunkMeta.timeRange[idx+b.position].setMaxTime(maxT)
}
