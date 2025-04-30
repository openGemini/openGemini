// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import (
	"github.com/openGemini/openGemini/lib/codec"
)

func (cv *ColVal) Marshal(buf []byte) []byte {
	buf = codec.AppendInt(buf, cv.Len)
	buf = codec.AppendInt(buf, cv.NilCount)
	buf = codec.AppendInt(buf, cv.BitMapOffset)
	buf = codec.AppendBytes(buf, cv.Val)
	buf = codec.AppendBytes(buf, cv.Bitmap)
	buf = codec.AppendUint32SliceSafe(buf, cv.Offset)
	return buf
}

func (cv *ColVal) Unmarshal(buf []byte) {
	if len(buf) == 0 {
		return
	}
	dec := codec.NewBinaryDecoder(buf)
	cv.Len = dec.Int()
	cv.NilCount = dec.Int()
	cv.BitMapOffset = dec.Int()
	cv.Val = append(cv.Val[:0], dec.BytesNoCopy()...)
	cv.Bitmap = append(cv.Bitmap[:0], dec.BytesNoCopy()...)
	cv.Offset = dec.Uint32SliceLE(cv.Offset[:0])
}

func (cv *ColVal) Size() int {
	size := 0
	size += codec.SizeOfInt()                  // Len
	size += codec.SizeOfInt()                  // NilCount
	size += codec.SizeOfInt()                  // BitMapOffset
	size += codec.SizeOfByteSlice(cv.Val)      // Val
	size += codec.SizeOfByteSlice(cv.Bitmap)   // Bitmap
	size += codec.SizeOfUint32Slice(cv.Offset) // Offset
	return size
}
