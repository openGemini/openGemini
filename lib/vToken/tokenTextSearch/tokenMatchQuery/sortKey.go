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
package tokenMatchQuery

import "github.com/openGemini/openGemini/lib/vToken/tokenIndex"

type SortKey struct {
	offset             uint16
	sizeOfInvertedList int
	tokenArr           []string
	invertedIndex      tokenIndex.Inverted_index
}

func (s *SortKey) Offset() uint16 {
	return s.offset
}

func (s *SortKey) SetOffset(offset uint16) {
	s.offset = offset
}

func (s *SortKey) SizeOfInvertedList() int {
	return s.sizeOfInvertedList
}

func (s *SortKey) SetSizeOfInvertedList(sizeOfInvertedList int) {
	s.sizeOfInvertedList = sizeOfInvertedList
}

func (s *SortKey) TokenArr() []string {
	return s.tokenArr
}

func (s *SortKey) SetTokenArr(tokenArr []string) {
	s.tokenArr = tokenArr
}

func (s *SortKey) InvertedIndex() tokenIndex.Inverted_index {
	return s.invertedIndex
}

func (s *SortKey) SetInvertedIndex(invertedIndex tokenIndex.Inverted_index) {
	s.invertedIndex = invertedIndex
}

func NewSortKey(offset uint16, pos int, tokenArr []string, invertIndex tokenIndex.Inverted_index) SortKey {
	return SortKey{
		offset:             offset,
		sizeOfInvertedList: pos,
		tokenArr:           tokenArr,
		invertedIndex:      invertIndex,
	}
}
