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

package textindex

// #cgo CPPFLAGS: -O3
// #cgo CXXFLAGS: -std=c++11
// #include "textbuilder_c.h"
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"
)

type FullTextIndexBuilder struct {
	builder C.TextIndexBuilder
}

type InvertMemElement struct {
	memElement C.MemElement
}

func (b *FullTextIndexBuilder) AddDocument(val []byte, offset []uint32, startRow int, endRow int) (*InvertMemElement, error) {
	valHeader := (*reflect.SliceHeader)(unsafe.Pointer(&val))
	valBuf := (*C.char)(unsafe.Pointer(valHeader.Data))

	offsetHeader := (*reflect.SliceHeader)(unsafe.Pointer(&offset))
	offsetBuf := (*C.uint32_t)(unsafe.Pointer(offsetHeader.Data))

	memElement := C.AddDocument(b.builder, valBuf, C.uint32_t(len(val)), offsetBuf, C.uint32_t(len(offset)), C.uint32_t(startRow), C.uint32_t(endRow))
	if memElement == nil {
		return nil, fmt.Errorf("add document to full text index failed")
	}
	return &InvertMemElement{memElement: memElement}, nil
}

// res[0]: first token start-offset
// res[1]: first token end-offset
// res[2]: last token start-offset
// res[3]: last token end-offset
// res[4]: keys size
// res[5]: keys total size = keys size + keys offs size
// res[6]: data size
// res[7]: data total size = data size + data offs size
// res[8]: items count
func RetrievePostingList(memElement *InvertMemElement, data *BlockData, bh *BlockHeader) bool {
	keysHeader := (*reflect.SliceHeader)(unsafe.Pointer(&data.Keys))
	keysBuf := (*C.char)(unsafe.Pointer(keysHeader.Data))

	datasHeader := (*reflect.SliceHeader)(unsafe.Pointer(&data.Data))
	datasBuf := (*C.char)(unsafe.Pointer(datasHeader.Data))

	res := make([]uint32, 9)
	resHeader := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	resBuf := (*C.uint32_t)(unsafe.Pointer(resHeader.Data))

	//(MemElement memElement, )
	readEnd := C.NextData(memElement.memElement, keysBuf, C.uint32_t(cap(data.Keys)), datasBuf, C.uint32_t(cap(data.Data)), resBuf)
	bh.KeysSize = res[4]
	bh.KeysUnpackSize = res[5]
	bh.PostSize = res[6]
	bh.PostUnpackSize = res[7]
	bh.ItemsCount = res[8]
	// modify the length of data.Keys and data.Data
	if int(bh.PostUnpackSize) > cap(data.Data) || int(bh.KeysUnpackSize) > cap(data.Keys) {
		panic(fmt.Errorf("the return length greater than the cap:[%d,%d][%d,%d]", bh.PostUnpackSize, cap(data.Data), bh.KeysUnpackSize, cap(data.Keys)))
	}
	keysHeader.Len = int(bh.KeysUnpackSize)
	datasHeader.Len = int(bh.PostUnpackSize)
	bh.FirstItem = data.Keys[res[0]:res[1]]
	bh.LastItem = data.Keys[res[2]:res[3]]
	return bool(readEnd)
}

func NewFullTextIndexBuilder(splitChars string, hasChin bool) *FullTextIndexBuilder {
	splitHeader := (*reflect.StringHeader)(unsafe.Pointer(&splitChars))
	splitStr := (*C.char)(unsafe.Pointer(splitHeader.Data))

	builder := C.NewTextIndexBuilder(splitStr, C.uint32_t(len(splitChars)), C._Bool(hasChin))
	return &FullTextIndexBuilder{builder: builder}
}

func FreeFullTextIndexBuilder(builder *FullTextIndexBuilder) {
	C.FreeTextIndexBuilder(builder.builder)
}

func PutInvertMemElement(ele *InvertMemElement) {
	C.PutMemElement(ele.memElement)
}

func GetMemElement(groupSize uint32) *InvertMemElement {
	memElement := C.GetMemElement(C.uint32_t(groupSize))
	if memElement == nil {
		return nil
	}
	return &InvertMemElement{memElement: memElement}
}

func AddPostingToMem(memElement *InvertMemElement, key []byte, rowId uint32) bool {
	keyHeader := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	keyBuf := (*C.char)(unsafe.Pointer(keyHeader.Data))

	isSuccess := C.AddPostingToMem(memElement.memElement, keyBuf, C.uint32_t(len(key)), C.uint32_t(rowId))
	return bool(isSuccess)
}
