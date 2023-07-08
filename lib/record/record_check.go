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

package record

import (
	"fmt"

	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func CheckRecord(rec *Record) {
	colN := len(rec.Schema)
	if rec.Schema[colN-1].Name != TimeField {
		panic(fmt.Sprintf("schema:%v", rec.Schema))
	}

	if rec.ColVals[colN-1].NilCount != 0 {
		panic(rec.String())
	}

	for i := 1; i < colN; i++ {
		if rec.Schema[i].Name == rec.Schema[i-1].Name {
			panic(fmt.Sprintf("same schema; idx: %d, name: %v", i, rec.Schema[i].Name))
		}
	}

	for i := 0; i < colN-1; i++ {
		f := &rec.Schema[i]
		col1, col2 := &rec.ColVals[i], &rec.ColVals[i+1]

		if col1.Len != col2.Len {
			panic(rec.String())
		}

		// check string data length
		if f.Type == influx.Field_Type_String {
			continue
		}

		// check data length
		expLen := typeSize[f.Type] * (col1.Len - col1.NilCount)
		if expLen != len(col1.Val) {
			fmt.Println(rec.String())
			err := fmt.Sprintf("the length of rec.ColVals[%d].val is incorrect. exp: %d, got: %d",
				i, expLen, len(col1.Val))
			panic(err)
		}
	}
}

func CheckTimes(times []int64) {
	if len(times) < 2 {
		return
	}
	for i := 0; i < len(times)-1; i++ {
		if times[i+1] <= times[i] {
			fmt.Println(i+1, "=>", times[i+1], "; ", i, "=>", times[i])
			panic("the time column is not ordered")
		}
	}
}

func CheckCol(col *ColVal, typ int) {
	if col == nil {
		return
	}

	if len(col.Offset) > 0 && !col.ValidString() {
		panic("Offset is invalid")
	}

	if col.NilCount < 0 {
		panic("NilCount is less than 0")
	}

	if col.ValidCount(0, col.Len) != (col.Len - col.NilCount) {
		panic("NilCount is invalid")
	}

	if typ != influx.Field_Type_String {
		expSize := typeSize[typ] * (col.Len - col.NilCount)
		if expSize != len(col.Val) {
			panic("invalid val size")
		}
	}

	bitmapLen := (col.Len+col.BitMapOffset)/8 + 1
	if (col.Len+col.BitMapOffset)%8 == 0 {
		bitmapLen--
	}
	if len(col.Bitmap) != bitmapLen {
		fmt.Println(len(col.Bitmap), bitmapLen, col.Len, col.BitMapOffset)
		panic("Bitmap is invalid")
	}
}
