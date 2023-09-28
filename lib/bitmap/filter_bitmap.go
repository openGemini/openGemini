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

const RowsNumPerFragment int = 8192

type FilterBitmap struct {
	ReserveId []int
	Bitmap    [][]byte
}

func NewFilterBitmap(length int) *FilterBitmap {
	f := &FilterBitmap{}
	f.ReserveId = make([]int, 0, RowsNumPerFragment)
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
	f.ReserveId = f.ReserveId[:0]
}

func IsNil(bitMap []byte, idx int) bool {
	return (bitMap[idx>>3] & record.BitMask[idx&0x07]) == 0
}

func SetBitMap(bitMap []byte, idx int) {
	bitMap[idx>>3] &= record.FlippedBitMask[idx&0x07]
}

func GetValWithOrOp(bitmap [][]byte, lastIdx int) [][]byte {
	for i := 0; i < len(bitmap)-1; i++ {
		for j := range bitmap[i] {
			// Get target idx bitmap value under or operation
			bitmap[lastIdx][j] = bitmap[lastIdx][j] | bitmap[i][j]
		}
	}
	return bitmap
}
