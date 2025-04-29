// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"encoding/binary"
	"fmt"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

type ChunkMetaHeader struct {
	values []string
}

func (h *ChunkMetaHeader) AppendValue(val string) {
	h.values = append(h.values, val)
}

func (h *ChunkMetaHeader) Len() int {
	return len(h.values)
}

func (h *ChunkMetaHeader) GetValue(idx int) string {
	if idx >= len(h.values) {
		return ""
	}
	return h.values[idx]
}

func (h *ChunkMetaHeader) Marshal(dst []byte) []byte {
	for i := range h.values {
		dst = codec.AppendString(dst, h.values[i])
	}
	return dst
}

func (h *ChunkMetaHeader) Unmarshal(buf []byte) {
	dec := codec.NewBinaryDecoder(buf)

	h.values = h.values[:0]
	for dec.RemainSize() > 0 {
		h.values = append(h.values, dec.String())
	}
}

func (h *ChunkMetaHeader) CopyTo(dst *ChunkMetaHeader) {
	dst.values = append(dst.values[:0], h.values...)
}

func (h *ChunkMetaHeader) Reset() {
	if h == nil {
		return
	}
	h.values = h.values[:0]
}

type ChunkMetaCodecCtx struct {
	header  *ChunkMetaHeader // used to marshal ChunkMeta
	trailer *Trailer         // used to unmarshal ChunkMeta

	dict    map[string]uint64
	int64s  []int64
	int64ps []*int64
	binDec  codec.BinaryDecoder
}

var codecCtxPool = pool.NewDefaultUnionPool[ChunkMetaCodecCtx](func() *ChunkMetaCodecCtx {
	return &ChunkMetaCodecCtx{
		header: &ChunkMetaHeader{},
		dict:   make(map[string]uint64),
	}
})

func GetChunkMetaCodecCtx() *ChunkMetaCodecCtx {
	return codecCtxPool.Get()
}

func (ctx *ChunkMetaCodecCtx) GetHeader() *ChunkMetaHeader {
	if ctx == nil {
		return nil
	}
	return ctx.header
}

func (ctx *ChunkMetaCodecCtx) SetTrailer(trailer *Trailer) {
	ctx.trailer = trailer
}

func (ctx *ChunkMetaCodecCtx) GetIndex(val string) uint64 {
	if idx, ok := ctx.dict[val]; ok {
		return idx
	}

	ctx.header.AppendValue(val)
	idx := ctx.header.Len() - 1
	ctx.dict[val] = uint64(idx)
	return uint64(idx)
}

func (ctx *ChunkMetaCodecCtx) GetValue(idx int) string {
	return ctx.trailer.ChunkMetaHeader.GetValue(idx)
}

func (ctx *ChunkMetaCodecCtx) MemSize() int {
	return (cap(ctx.int64s) + cap(ctx.int64ps)) * util.Int64SizeBytes
}

func (ctx *ChunkMetaCodecCtx) Instance() *ChunkMetaCodecCtx {
	return ctx
}

func (ctx *ChunkMetaCodecCtx) Release() {
	ctx.header.Reset()
	ctx.trailer = nil
	ctx.binDec.Reset(nil)
	for k := range ctx.dict {
		delete(ctx.dict, k)
	}
	codecCtxPool.Put(ctx)
}

func MarshalChunkMeta(ctx *ChunkMetaCodecCtx, cm *ChunkMeta, dst []byte) ([]byte, error) {
	if !IsChunkMetaCompressSelf() {
		return cm.marshal(dst), nil
	}

	dst = binary.BigEndian.AppendUint64(dst, cm.sid)
	dst = binary.AppendUvarint(dst, uint64(cm.offset))
	dst = binary.AppendUvarint(dst, uint64(cm.size))
	dst = binary.AppendUvarint(dst, uint64(cm.columnCount))
	dst = binary.AppendUvarint(dst, uint64(cm.segCount))

	var err error
	dst = MarshalTimeRange(ctx, cm.timeRange, dst)

	for i := range cm.colMeta {
		dst = MarshalColumnMeta(ctx, &cm.colMeta[i], dst)
	}
	return dst, err
}

func UnmarshalChunkMeta(ctx *ChunkMetaCodecCtx, cm *ChunkMeta, buf []byte) ([]byte, error) {
	buf, err := UnmarshalChunkMetaBaseAttr(ctx, cm, buf)
	if err != nil {
		return buf, err
	}

	columnCount := int(cm.columnCount)
	segCount := int(cm.segCount)

	if cap(cm.colMeta) < columnCount {
		cm.colMeta = make([]ColumnMeta, columnCount)
	}

	cm.colMeta = cm.colMeta[:columnCount]
	for i := range cm.colMeta {
		buf, err = UnmarshalColumnMeta(ctx, segCount, &cm.colMeta[i], buf)
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}

func UnmarshalChunkMetaWithColumns(ctx *ChunkMetaCodecCtx, cm *ChunkMeta, columns []string, buf []byte) ([]byte, error) {
	if len(columns) == 0 || columns[0] == record.TimeField {
		return UnmarshalChunkMeta(ctx, cm, buf)
	}

	buf, err := UnmarshalChunkMetaBaseAttr(ctx, cm, buf)
	if err != nil {
		return buf, err
	}

	columnCount := len(columns)
	cm.columnCount = uint32(len(columns))
	segCount := int(cm.segCount)
	if cap(cm.colMeta) < columnCount {
		cm.colMeta = make([]ColumnMeta, columnCount)
	}
	cm.colMeta = cm.colMeta[:columnCount]

	j := 0
	ok := false
	for i := range columns {
		if i > 0 && columns[i] == columns[i-1] {
			continue
		}

		buf, ok, err = unmarshalColumnMetaWithName(ctx, &cm.colMeta[j], columns[i], segCount, buf)
		if err != nil {
			return nil, err
		}
		if ok {
			j++
		}
	}

	// j is actual number of columns
	cm.colMeta = cm.colMeta[:j]
	cm.columnCount = uint32(j)
	return buf, nil
}

func unmarshalColumnMetaWithName(ctx *ChunkMetaCodecCtx, col *ColumnMeta, column string, segCount int, buf []byte) ([]byte, bool, error) {
	var name string
	var err error
	orig := buf

	for len(buf) > 0 {
		buf, name = UnmarshalColumnName(ctx, buf)
		if name == column {
			col.name = name
			buf, err = UnmarshalColumnMetaWithoutName(ctx, segCount, col, buf)
			break
		}

		if (name > column && column != record.TimeField) || name == record.TimeField {
			return orig, false, nil
		}

		// skip current column
		n := columnMetaMinSize(segCount) + int(buf[1])
		buf = buf[n:]
	}
	return buf, true, err
}

func UnmarshalChunkMetaBaseAttr(ctx *ChunkMetaCodecCtx, cm *ChunkMeta, buf []byte) ([]byte, error) {
	dec := &ctx.binDec
	dec.Reset(buf)

	cm.sid = dec.Uint64()

	offset, ok := dec.Uvarint()
	if !ok {
		return buf, fmt.Errorf("invalid data offset")
	}
	cm.offset = int64(offset)

	size, ok := dec.Uvarint()
	if !ok {
		return buf, fmt.Errorf("invalid data size")
	}
	cm.size = uint32(size)

	columnCount, ok := dec.Uvarint()
	if !ok {
		return buf, fmt.Errorf("invalid column count")
	}
	cm.columnCount = uint32(columnCount)

	segCount, ok := dec.Uvarint()
	if !ok {
		return buf, fmt.Errorf("invalid segment count")
	}
	cm.segCount = uint32(segCount)

	buf = dec.Remain()
	buf, ok = UnmarshalTimeRange(ctx, cm, buf)
	if !ok {
		return buf, fmt.Errorf("invalid time range")
	}

	return buf, nil
}

func MarshalTimeRange(ctx *ChunkMetaCodecCtx, sr []SegmentRange, dst []byte) []byte {
	int64s := ctx.int64s[:0]
	for i := range sr {
		int64s = append(int64s, sr[i][0], sr[i][1])
	}

	dst = codec.EncodeInt64sWithScale(dst, int64s)
	ctx.int64s = int64s
	return dst
}

func UnmarshalTimeRange(ctx *ChunkMetaCodecCtx, cm *ChunkMeta, buf []byte) ([]byte, bool) {
	segCount := int(cm.segCount)
	if cap(cm.timeRange) < segCount {
		cm.timeRange = make([]SegmentRange, segCount)
	}
	cm.timeRange = cm.timeRange[:segCount]

	int64ps := ctx.int64ps[:0]
	sr := cm.timeRange[:segCount]
	for i := range sr {
		int64ps = append(int64ps, &sr[i][0], &sr[i][1])
	}
	ctx.int64ps = int64ps

	return codec.DecodeInt64sWithScale(buf, int64ps...)
}

func MarshalColumnMeta(ctx *ChunkMetaCodecCtx, col *ColumnMeta, dst []byte) []byte {
	dst = binary.AppendUvarint(dst, ctx.GetIndex(col.name))
	dst = append(dst, col.ty)

	if config.GetCommon().PreAggEnabled || col.name == record.TimeField {
		dst = append(dst, uint8(len(col.preAgg)))
		dst = append(dst, col.preAgg...)
	} else {
		dst = append(dst, 0)
	}

	dst = numberenc.MarshalUint64Append(dst, uint64(col.entries[0].offset))
	for i := range col.entries {
		dst = numberenc.MarshalUint32Append(dst, col.entries[i].size)
	}
	return dst
}

func columnMetaMinSize(segCount int) int {
	return 0 +
		1 + // column type
		1 + // pre agg data length
		util.Uint64SizeBytes + // segment data offset
		segCount*util.Uint32SizeBytes // size of each segment
}

func newSmallerError(name string, got int, min int) error {
	return fmt.Errorf("too smaller data for %s %d < %d", name, got, min)
}

func UnmarshalColumnMeta(ctx *ChunkMetaCodecCtx, segCount int, col *ColumnMeta, buf []byte) ([]byte, error) {
	buf, col.name = UnmarshalColumnName(ctx, buf)
	if col.name == "" {
		return nil, fmt.Errorf("invalid column name")
	}
	return UnmarshalColumnMetaWithoutName(ctx, segCount, col, buf)
}

func UnmarshalColumnMetaWithoutName(ctx *ChunkMetaCodecCtx, segCount int, col *ColumnMeta, buf []byte) ([]byte, error) {
	minSize := columnMetaMinSize(segCount)
	if len(buf) < minSize {
		return nil, newSmallerError("segment", len(buf), minSize)
	}

	dec := &ctx.binDec
	dec.Reset(buf)

	col.ty = dec.Uint8()

	preAggSize := dec.Uint8()
	if preAggSize > 0 {
		col.preAgg = dec.BytesN(col.preAgg[:0], int(preAggSize))
	} else {
		col.preAgg = append(col.preAgg[:0], zeroPreAgg...)
	}

	if cap(col.entries) < segCount {
		col.entries = make([]Segment, segCount)
	}
	col.entries = col.entries[:segCount]

	offset := int64(dec.Uint64())
	for i := range col.entries {
		size := dec.Uint32()
		col.entries[i].offset = offset
		col.entries[i].size = size
		offset += int64(size)
	}
	return dec.Remain(), nil
}

func UnmarshalColumnName(ctx *ChunkMetaCodecCtx, buf []byte) ([]byte, string) {
	idx, n := binary.Uvarint(buf)
	if n <= 0 {
		return buf, ""
	}
	return buf[n:], ctx.GetValue(int(idx))
}

func UnmarshalChunkMetaAdaptive(ctx *ChunkMetaCodecCtx, cm *ChunkMeta, columns []string, buf []byte) ([]byte, error) {
	if ctx != nil && ctx.trailer.ChunkMetaCompressFlag == ChunkMetaCompressSelf {
		return UnmarshalChunkMetaWithColumns(ctx, cm, columns, buf)
	}

	return cm.UnmarshalWithColumns(buf, columns)
}
