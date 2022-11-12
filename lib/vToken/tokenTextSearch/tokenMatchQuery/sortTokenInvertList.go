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

type SortTokenInvertList struct {
	tokenArr  []string
	indexList tokenIndex.Inverted_index
}

func (s *SortTokenInvertList) TokenArr() []string {
	return s.tokenArr
}

func (s *SortTokenInvertList) SetTokenArr(tokenArr []string) {
	s.tokenArr = tokenArr
}

func (s *SortTokenInvertList) IndexList() tokenIndex.Inverted_index {
	return s.indexList
}

func (s *SortTokenInvertList) SetIndexList(indexList tokenIndex.Inverted_index) {
	s.indexList = indexList
}

func NewSortTokenInvertList(tokenArr []string, indexList tokenIndex.Inverted_index) SortTokenInvertList {
	return SortTokenInvertList{
		tokenArr:  tokenArr,
		indexList: indexList,
	}
}
