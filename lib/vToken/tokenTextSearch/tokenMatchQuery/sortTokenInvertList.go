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
