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

package bitmap

import "github.com/openGemini/openGemini/lib/record"

type FilterBitmap struct {
	Bitmap [][]byte
}

func NewFilterBitmap(length int) *FilterBitmap {
	f := &FilterBitmap{}
	f.Bitmap = make([][]byte, length)
	for i := range f.Bitmap {
		f.Bitmap[i] = []byte{}
	}
	return f
}

func (f *FilterBitmap) Reset() {
	for i := range f.Bitmap {
		f.Bitmap[i] = f.Bitmap[i][:0]
	}
}

func IsNil(bitMap []byte, idx int) bool {
	if (bitMap[idx>>3] & record.BitMask[idx&0x07]) == 0 {
		return true
	}
	return false
}

func SetBitMap(bitMap []byte, idx int) {
	bitMap[idx>>3] &= record.FlippedBitMask[idx&0x07]
}

func GetValWithOrOp(bitmap [][]byte, resIdx, i, j int) [][]byte {
	// Get target idx bitmap value under or operation
	bitmap[resIdx][j] = bitmap[resIdx][j] | bitmap[i][j]
	return bitmap
}
