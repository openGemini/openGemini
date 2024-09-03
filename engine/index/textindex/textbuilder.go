//go:build !linux || !amd64 || !cgo

package textindex

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

type FullTextIndexBuilder struct {
}

type InvertMemElement struct {
}

func (b *FullTextIndexBuilder) AddDocument(val []byte, offset []uint32, startRow int, endRow int) (*InvertMemElement, error) {
	return nil, nil
}

func RetrievePostingList(memElement *InvertMemElement, data *BlockData, bh *BlockHeader) bool {
	return false
}

func NewFullTextIndexBuilder(splitChars string, hasChin bool) *FullTextIndexBuilder {
	return nil
}

func FreeFullTextIndexBuilder(builder *FullTextIndexBuilder) {
}

func PutInvertMemElement(ele *InvertMemElement) {
}

func GetMemElement(groupSize uint32) *InvertMemElement {
	return &InvertMemElement{}
}

func AddPostingToMem(memElement *InvertMemElement, key []byte, rowId uint32) bool {
	return true
}
