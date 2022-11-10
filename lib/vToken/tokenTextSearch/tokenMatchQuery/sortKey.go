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
