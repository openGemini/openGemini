// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

var firstLastReaderPool sync.Pool

func readFirstOrLast(cm *ChunkMeta, ref *record.Field, dst *record.Record, ctx *ReadContext, cr ColumnReader, copied, first bool, ioPriority int) error {
	reader, ok := firstLastReaderPool.Get().(*FirstLastReader)
	if !ok || reader == nil {
		reader = &FirstLastReader{}
	}
	defer reader.Release()

	reader.Init(cm, cr, ref, dst, first)
	return reader.Read(ctx, copied, ioPriority)
}

type FirstLastReader struct {
	first bool
	cr    ColumnReader

	ref     *record.Field
	cm      *ChunkMeta
	dst     *record.Record
	timeCol *record.ColVal
	dataCol *record.ColVal
	meta    *record.ColMeta

	timeBuf []byte
	dataBuf []byte

	segIndex    int
	floatPreAgg *FloatPreAgg
	intPreAgg   *IntegerPreAgg
}

func (r *FirstLastReader) Init(cm *ChunkMeta, cr ColumnReader, ref *record.Field, dst *record.Record, first bool) *FirstLastReader {
	r.first = first
	r.ref = ref
	r.cr = cr
	r.cm = cm
	r.dst = dst

	idx := dst.Schema.FieldIndex(ref.Name)
	if idx < 0 {
		panic(fmt.Sprintf("column(%v) not find in %v", ref.String(), dst.Schema.String()))
	}
	r.dataCol = dst.Column(idx)
	r.timeCol = dst.TimeColumn()
	r.meta = &dst.ColMeta[idx]

	r.segIndex = -1
	if !first {
		r.segIndex = int(cm.segCount)
	}

	return r
}

func (r *FirstLastReader) Read(ctx *ReadContext, copied bool, ioPriority int) error {
	idx := r.cm.columnIndex(r.ref)
	if idx < 0 {
		return nil
	}
	colMeta := &r.cm.colMeta[idx]
	tmMeta := r.cm.timeMeta()

	var err error
	for r.next() {
		minMaxSeg := &r.cm.timeRange[r.segIndex]
		if !ctx.tr.Overlaps(minMaxSeg.minTime(), minMaxSeg.maxTime()) {
			continue
		}

		// in some scenarios, the first value is exactly the min value or
		// the last value is exactly the max value.
		// can directly use the min/max value in the pre-aggregated data as the first/last
		val, tm, ok := r.readFirstOrLastFromPreAgg(ctx, minMaxSeg, colMeta)
		if ok {
			rowIndex := 0
			if !r.first {
				rowIndex = int(numberenc.UnmarshalUint32(r.cm.timeMeta().preAgg)) - 1
			}
			err = r.after(val, tm, rowIndex, ctx, copied, ioPriority)
			break
		}

		if err := r.readDataColVal(ctx, &colMeta.entries[r.segIndex], copied, ioPriority); err != nil {
			return err
		}

		var rowIndex int
		if r.first && r.dataCol.NilCount == 0 && minMaxSeg.minTime() >= ctx.tr.Min {
			// query time range:   --------------
			// segment time range:     ---------------
			// If there is no null value, the first row of data is the result
			tm = r.cm.minTime()
			rowIndex = 0
		} else if !r.first && r.dataCol.NilCount == 0 && minMaxSeg.maxTime() <= ctx.tr.Max {
			// query time range:        --------------
			// segment time range: ---------------
			// If there is no null value, the last row of data is the result
			tm = r.cm.maxTime()
			rowIndex = r.dataCol.Len - 1
		} else {
			if err := r.readTimeColVal(ctx, &tmMeta.entries[r.segIndex], copied, ioPriority); err != nil {
				return err
			}

			rowIndex = r.readRowIndex(ctx)
			if rowIndex >= r.timeCol.Length() {
				continue
			}
			tm, _ = r.timeCol.IntegerValue(rowIndex)
		}

		val = getColumnValue(r.ref, r.dataCol, rowIndex)
		err = r.after(val, tm, rowIndex, ctx, copied, ioPriority)
		break
	}

	return err
}

func (r *FirstLastReader) Release() {
	r.cr = nil
	r.ref = nil
	r.cm = nil
	r.dst = nil
	r.timeCol = nil
	r.dataCol = nil
	r.meta = nil
	firstLastReaderPool.Put(r)
}

func (r *FirstLastReader) after(val interface{}, tm int64, rowIndex int, ctx *ReadContext, copied bool, ioPriority int) error {
	if r.first {
		r.meta.SetFirst(val, tm)
	} else {
		r.meta.SetLast(val, tm)
	}

	setColumnDefaultValue(r.ref, r.dataCol)
	r.timeCol.Init()
	r.timeCol.AppendInteger(tm)

	if r.dst.Schema.Len() > 2 && len(ctx.ops) == 1 {
		err := readAuxData(r.cm, r.segIndex, rowIndex, r.dst, ctx, r.cr, copied, ioPriority)
		if err != nil {
			log.Error("read aux data column fail", zap.Error(err))
			return err
		}
	}
	return nil
}

func (r *FirstLastReader) readRowIndex(ctx *ReadContext) int {
	if r.first {
		return readFirstRowIndex(r.timeCol, r.dataCol, ctx.tr)
	}
	return readLastRowIndex(r.timeCol, r.dataCol, ctx.tr)
}

func (r *FirstLastReader) next() bool {
	if r.first {
		r.segIndex++
		return r.segIndex < int(r.cm.segCount)
	}

	r.segIndex--
	return r.segIndex >= 0
}

func (r *FirstLastReader) readColVal(seg *Segment, buf *[]byte, hook func(buf []byte) error, ioPriority int) error {
	offset, size := seg.offsetSize()
	data, cachePage, err := r.cr.ReadDataBlock(offset, size, buf, ioPriority)
	defer r.cr.UnrefCachePage(cachePage)
	if err != nil {
		return err
	}

	return hook(data)
}

func (r *FirstLastReader) readTimeColVal(ctx *ReadContext, seg *Segment, copied bool, ioPriority int) error {
	r.timeBuf = r.timeBuf[:0]
	return r.readColVal(seg, &r.timeBuf, func(buf []byte) error {
		return appendTimeColumnData(buf, r.timeCol, ctx, copied)
	}, ioPriority)
}

func (r *FirstLastReader) readDataColVal(ctx *ReadContext, seg *Segment, copied bool, ioPriority int) error {
	r.dataBuf = r.dataBuf[:0]
	return r.readColVal(seg, &r.dataBuf, func(buf []byte) error {
		return decodeColumnData(r.ref, buf, r.dataCol, ctx, copied)
	}, ioPriority)
}

func (r *FirstLastReader) readFirstOrLastFromPreAgg(ctx *ReadContext, sr *SegmentRange, colMeta *ColumnMeta) (interface{}, int64, bool) {
	if r.first && ctx.tr.Min <= sr.minTime() {
		val, tm, ok := r.ReadMinFromPreAgg(colMeta)

		return val, tm, ok && tm == sr.minTime()
	}

	if !r.first && ctx.tr.Max >= sr.maxTime() {
		val, tm, ok := r.ReadMaxFromPreAgg(colMeta)

		return val, tm, ok && tm == sr.maxTime()
	}

	return 0, 0, false
}

func (r *FirstLastReader) ReadMinFromPreAgg(colMeta *ColumnMeta) (interface{}, int64, bool) {
	ab, ok := r.unmarshalPreAgg(colMeta)
	if !ok {
		return 0, 0, false
	}

	val, tm := ab.min()
	return val, tm, true
}

func (r *FirstLastReader) ReadMaxFromPreAgg(colMeta *ColumnMeta) (interface{}, int64, bool) {
	ab, ok := r.unmarshalPreAgg(colMeta)
	if !ok {
		return 0, 0, false
	}

	val, tm := ab.max()
	return val, tm, true
}

func (r *FirstLastReader) getIntPreAgg() PreAggBuilder {
	if r.intPreAgg == nil {
		r.intPreAgg = NewIntegerPreAgg()
	}
	return r.intPreAgg
}

func (r *FirstLastReader) getFloatPreAgg() PreAggBuilder {
	if r.floatPreAgg == nil {
		r.floatPreAgg = NewFloatPreAgg()
	}
	return r.floatPreAgg
}

func (r *FirstLastReader) unmarshalPreAgg(col *ColumnMeta) (PreAggBuilder, bool) {
	var ab PreAggBuilder
	switch col.ty {
	case influx.Field_Type_Int:
		ab = r.getIntPreAgg()
	case influx.Field_Type_Float:
		ab = r.getFloatPreAgg()
	default:
		return nil, false
	}

	_, err := ab.unmarshal(col.preAgg)
	if err != nil {
		logger.GetLogger().Error("failed to unmarshal pre agg",
			zap.Binary("data", col.preAgg), zap.Error(err))
		return nil, false
	}
	return ab, true
}
