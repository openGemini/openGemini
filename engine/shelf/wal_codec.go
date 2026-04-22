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

package shelf

import (
	"encoding/binary"
	"slices"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/compress/dict"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	RecordEncodeModeOneRow = 1
	RecordEncodeModeFull   = 2
)

type WalRecordDecoder struct {
	dec codec.BinaryDecoder
}

func NewWalRecordDecoder() *WalRecordDecoder {
	return &WalRecordDecoder{}
}

func (c *WalRecordDecoder) Reset(buf []byte) {
	c.dec.Reset(buf)
}

func (c *WalRecordDecoder) Decode(rec *record.Record, buf []byte) error {
	var err error
	c.Reset(buf)

	header := c.dec.Uint32()
	fieldLen := int((header << 8) >> 8)
	mode := header >> 24

	rec.Schema = slices.Grow(rec.Schema[:0], fieldLen)[:fieldLen]
	rec.ColVals = slices.Grow(rec.ColVals[:0], fieldLen)[:fieldLen]

	if mode == RecordEncodeModeOneRow {
		for i := range rec.Schema {
			schema := &rec.Schema[i]
			c.DecodeColumnSchema(schema)
			c.DecodeColValOneRowMode(schema.Type, &rec.ColVals[i])
		}
		return nil
	}

	for i := range rec.Schema {
		schema := &rec.Schema[i]
		c.DecodeColumnSchema(schema)
		err = c.DecodeColVal(schema.Type, &rec.ColVals[i])
		if err != nil {
			return err
		}
	}

	if c.dec.RemainSize() > 0 {
		logger.GetLogger().Warn("[BUG] error may occur in decoding the record",
			zap.Int("src binary size", len(buf)),
			zap.Int("remain size", c.dec.RemainSize()),
			zap.Uint8s("src", buf))
	}
	return nil
}

func (c *WalRecordDecoder) DecodeColumnSchema(dst *record.Field) {
	id := c.dec.Uint32()
	if id>>31 == 1 {
		// dictionary encoding is not used
		dst.Name = stringinterner.InternSafe(util.Bytes2str(c.dec.BytesNoCopyN(int(id & 0xFFFF))))
	} else {
		dst.Name = dict.DefaultDict().GetValue(int(id))
	}

	dst.Type = int(c.dec.Uint8())
}

func (c *WalRecordDecoder) DecodeColValOneRowMode(typ int, col *record.ColVal) {
	col.Len = 1
	col.NilCount = 0
	col.BitMapOffset = 0
	col.Bitmap = append(col.Bitmap[:0], 1)
	col.Offset = col.Offset[:0]

	if typ == influx.Field_Type_String {
		col.Offset = append(col.Offset, 0)
		col.Val = c.dec.BytesNoCopy()
		return
	}

	col.Val = c.dec.BytesNoCopyN(record.GetTypeSize(typ))
}

func (c *WalRecordDecoder) DecodeColVal(typ int, col *record.ColVal) error {
	var ok bool
	var u uint64
	dec := &c.dec

	u, ok = dec.Uvarint()
	if !ok {
		return errno.NewError(errno.TooSmallOrOverflow, "ColVal.Len")
	}
	col.Len = int(u)

	u, ok = dec.Uvarint()
	if !ok {
		return errno.NewError(errno.TooSmallOrOverflow, "ColVal.NilCount")
	}
	col.NilCount = int(u)

	col.BitMapOffset = 0
	if col.NilCount > 0 {
		col.Bitmap = dec.BytesNoCopyN(util.DivisionCeil(col.Len, util.Uint64SizeBytes))
	} else {
		var fill uint8 = 0xFF
		if col.NilCount == col.Len {
			fill = 0
		}
		col.FillBitmap(fill)
	}

	if typ == influx.Field_Type_String {
		col.Val = dec.BytesNoCopy()

		size := col.Len * util.Uint32SizeBytes
		if dec.RemainSize() < size {
			return errno.NewError(errno.TooSmallData, "ColVal.Offset", size, dec.RemainSize())
		}

		col.Offset = util.Bytes2Uint32Slice(dec.BytesNoCopyN(size))
	} else {
		size := (col.Len - col.NilCount) * record.GetTypeSize(typ)
		if dec.RemainSize() < size {
			return errno.NewError(errno.TooSmallData, "ColVal.Val", size, dec.RemainSize())
		}
		col.Val = dec.BytesNoCopyN(size)
		col.Offset = col.Offset[:0]
	}

	return nil
}

func EncodeRecordRow(dst []byte, rec *record.Record, rowIdx int) []byte {
	ofs := len(dst)
	dst = codec.AppendUint32(dst, 0)

	var fieldLen uint32
	for i := range rec.Len() {
		if rec.ColVals[i].IsNil(rowIdx) {
			continue
		}

		fieldLen++
		dst = EncodeColumnSchema(dst, &rec.Schema[i])
		dst = EncodeColValRow(dst, rec.Schema[i].Type, rec.Column(i), rowIdx)
	}

	binary.BigEndian.PutUint32(dst[ofs:], RecordEncodeModeOneRow<<24|fieldLen)
	return dst
}

func EncodeRecord(dst []byte, rec *record.Record) []byte {
	dst = codec.AppendUint32(dst, RecordEncodeModeFull<<24|uint32(rec.Len()))

	for i := range rec.Len() {
		dst = EncodeColumnSchema(dst, &rec.Schema[i])
		dst = EncodeColVal(dst, rec.Column(i))
	}

	return dst
}

func EncodeColumnSchema(dst []byte, schema *record.Field) []byte {
	id := dict.DefaultDict().GetID(schema.Name)
	if id < 0 {
		// not in the dictionary
		dst = codec.AppendUint16(dst, uint16(1<<15))
		dst = codec.AppendString(dst, schema.Name)
	} else {
		dst = codec.AppendUint32(dst, uint32(id))
	}

	return append(dst, uint8(schema.Type))
}

func EncodeColValRow(dst []byte, typ int, cv *record.ColVal, rowIdx int) []byte {
	// Because there is only one row and it is not empty, you only need to serialize cv.Val
	if typ == influx.Field_Type_String {
		val, _ := cv.StringValue(rowIdx)
		dst = codec.AppendBytes(dst, val)
		return dst
	}

	pos := rowIdx
	if cv.NilCount > 0 {
		pos = record.ValueIndexRangeWithSingle(cv.Bitmap, cv.BitMapOffset, cv.BitMapOffset+rowIdx)
	}

	size := record.GetTypeSize(typ)
	dst = append(dst, cv.Val[pos*size:(pos+1)*size]...)

	return dst
}

func EncodeColVal(dst []byte, cv *record.ColVal) []byte {
	dst = binary.AppendUvarint(dst, uint64(cv.Len))
	dst = binary.AppendUvarint(dst, uint64(cv.NilCount))

	if cv.NilCount > 0 {
		// bitmap does not need to be serialized if there is no nil row
		// The length of cv.Bitmap is: util.DivisionCeil(cv.Len, util.Uint64SizeBytes)
		dst = append(dst, cv.Bitmap...)
	}

	if len(cv.Offset) > 0 {
		// type String
		dst = codec.AppendBytes(dst, cv.Val)
		dst = append(dst, util.Uint32Slice2byte(cv.Offset)...)
	} else {
		// The length of cv.Val is: (cv.Len - cv.NilCount) * record.GetTypeSize(typ)
		dst = append(dst, cv.Val...)
	}

	return dst
}
