package gramMatchQuery

import "github.com/openGemini/openGemini/lib/vGram/gramIndex"

type SortKey struct {
	offset             uint16
	sizeOfInvertedList int
	gram               string
	invertedIndex      gramIndex.Inverted_index
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

func (s *SortKey) Gram() string {
	return s.gram
}

func (s *SortKey) SetGram(gram string) {
	s.gram = gram
}

func (s *SortKey) InvertedIndex() gramIndex.Inverted_index {
	return s.invertedIndex
}

func (s *SortKey) SetInvertedIndex(invertedIndex gramIndex.Inverted_index) {
	s.invertedIndex = invertedIndex
}

func NewSortKey(offset uint16, pos int, gram string, invertIndex gramIndex.Inverted_index) SortKey {
	return SortKey{
		offset:             offset,
		sizeOfInvertedList: pos,
		gram:               gram,
		invertedIndex:      invertIndex,
	}
}
