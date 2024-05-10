/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor

import "math"

func UpdateHashInterSumSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	srcColumn, dstColumn := srcChunk.Column(srcCol), dstChunk.Column(dstCol)
	isNil := srcColumn.IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcColumn.GetValueIndexV2(srcRow)
	sv := srcColumn.IntegerValue(srcRow)
	dv := dstColumn.IntegerValue(dstRow)
	dstColumn.UpdateIntegerValueFast(sv+dv, dstRow)
}

func UpdateHashInterSumFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	dstColumn := dstChunk.Column(dstCol)
	sv := srcChunk.Column(srcCol).IntegerValue(srcRow)
	dv := dstColumn.IntegerValue(dstRow)
	dstColumn.UpdateIntegerValueFast(sv+dv, dstRow)
}

func UpdateHashFloatSumSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	srcColumn, dstColumn := srcChunk.Column(srcCol), dstChunk.Column(dstCol)
	isNil := srcColumn.IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcColumn.GetValueIndexV2(srcRow)
	sv := srcColumn.FloatValue(srcRow)
	dv := dstColumn.FloatValue(dstRow)
	dstColumn.UpdateFloatValueFast(sv+dv, dstRow)
}

func UpdateHashFloatSumFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	dstColumn := dstChunk.Column(dstCol)
	sv := srcChunk.Column(srcCol).FloatValue(srcRow)
	dv := dstColumn.FloatValue(dstRow)
	dstColumn.UpdateFloatValueFast(sv+dv, dstRow)
}

func UpdateHashInterMinSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	srcColumn, dstColumn := srcChunk.Column(srcCol), dstChunk.Column(dstCol)
	isNil := srcColumn.IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcColumn.GetValueIndexV2(srcRow)
	sv := srcColumn.IntegerValue(srcRow)
	dv := dstColumn.IntegerValue(dstRow)
	minV := sv
	if dv < sv {
		minV = dv
	}
	dstColumn.UpdateIntegerValueFast(minV, dstRow)
}

func UpdateHashInterMinFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	dstColumn := dstChunk.Column(dstCol)
	sv := srcChunk.Column(srcCol).IntegerValue(srcRow)
	dv := dstColumn.IntegerValue(dstRow)
	minV := sv
	if dv < sv {
		minV = dv
	}
	dstColumn.UpdateIntegerValueFast(minV, dstRow)
}

func UpdateHashFloatMinSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	srcColumn, dstColumn := srcChunk.Column(srcCol), dstChunk.Column(dstCol)
	isNil := srcColumn.IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcColumn.GetValueIndexV2(srcRow)
	sv := srcColumn.FloatValue(srcRow)
	dv := dstColumn.FloatValue(dstRow)
	minV := sv
	if dv < sv {
		minV = dv
	}
	dstColumn.UpdateFloatValueFast(minV, dstRow)
}

func UpdateHashFloatMinFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	dstColumn := dstChunk.Column(dstCol)
	sv := srcChunk.Column(srcCol).FloatValue(srcRow)
	dv := dstColumn.FloatValue(dstRow)
	minV := sv
	if dv < sv {
		minV = dv
	}
	dstColumn.UpdateFloatValueFast(minV, dstRow)
}

func rewriteInterMinColumnFunc(chunk Chunk, srcCol, lastNum int) {
	column := chunk.Column(srcCol)
	lastIndex := len(column.IntegerValues()) - 1
	firstIndex := lastIndex - lastNum + 1
	if firstIndex < 0 {
		return
	}
	for i := lastIndex; i >= firstIndex; i-- {
		column.UpdateIntegerValueFast(math.MaxInt64, i)
	}
}

func rewriteFloatMinColumnFunc(chunk Chunk, srcCol, lastNum int) {
	column := chunk.Column(srcCol)
	lastIndex := len(column.FloatValues()) - 1
	firstIndex := lastIndex - lastNum + 1
	if firstIndex < 0 {
		return
	}
	for i := lastIndex; i >= firstIndex; i-- {
		column.UpdateFloatValueFast(math.MaxFloat64, i)
	}
}

func UpdateHashInterMaxSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	srcColumn, dstColumn := srcChunk.Column(srcCol), dstChunk.Column(dstCol)
	isNil := srcColumn.IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcColumn.GetValueIndexV2(srcRow)
	sv := srcColumn.IntegerValue(srcRow)
	dv := dstColumn.IntegerValue(dstRow)
	maxV := sv
	if dv > sv {
		maxV = dv
	}
	dstColumn.UpdateIntegerValueFast(maxV, dstRow)
}

func UpdateHashInterMaxFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	dstColumn := dstChunk.Column(dstCol)
	sv := srcChunk.Column(srcCol).IntegerValue(srcRow)
	dv := dstColumn.IntegerValue(dstRow)
	maxV := sv
	if dv > sv {
		maxV = dv
	}
	dstColumn.UpdateIntegerValueFast(maxV, dstRow)
}

func UpdateHashFloatMaxSlow(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	srcColumn, dstColumn := srcChunk.Column(srcCol), dstChunk.Column(dstCol)
	isNil := srcColumn.IsNilV2(srcRow)
	if isNil {
		return
	}
	srcRow = srcColumn.GetValueIndexV2(srcRow)
	sv := srcColumn.FloatValue(srcRow)
	dv := dstColumn.FloatValue(dstRow)
	maxV := sv
	if dv > sv {
		maxV = dv
	}
	dstColumn.UpdateFloatValueFast(maxV, dstRow)
}

func UpdateHashFloatMaxFast(dstChunk, srcChunk Chunk, dstCol, srcCol, dstRow, srcRow int) {
	dstColumn := dstChunk.Column(dstCol)
	sv := srcChunk.Column(srcCol).FloatValue(srcRow)
	dv := dstColumn.FloatValue(dstRow)
	maxV := sv
	if dv > sv {
		maxV = dv
	}
	dstColumn.UpdateFloatValueFast(maxV, dstRow)
}

func rewriteInterMaxColumnFunc(chunk Chunk, srcCol, lastNum int) {
	column := chunk.Column(srcCol)
	lastIndex := len(column.IntegerValues()) - 1
	firstIndex := lastIndex - lastNum + 1
	if firstIndex < 0 {
		return
	}
	for i := lastIndex; i >= firstIndex; i-- {
		column.UpdateIntegerValueFast(-math.MaxInt64, i)
	}
}

func rewriteFloatMaxColumnFunc(chunk Chunk, srcCol, lastNum int) {
	column := chunk.Column(srcCol)
	lastIndex := len(column.FloatValues()) - 1
	firstIndex := lastIndex - lastNum + 1
	if firstIndex < 0 {
		return
	}
	for i := lastIndex; i >= firstIndex; i-- {
		column.UpdateFloatValueFast(-math.MaxFloat64, i)
	}
}
