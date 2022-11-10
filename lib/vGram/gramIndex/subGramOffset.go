package gramIndex

type SubGramOffset struct {
	subGram string
	offset  uint16
}

func (s *SubGramOffset) SubGram() string {
	return s.subGram
}

func (s *SubGramOffset) SetSubGram(subGram string) {
	s.subGram = subGram
}

func (s *SubGramOffset) Offset() uint16 {
	return s.offset
}

func (s *SubGramOffset) SetOffset(offset uint16) {
	s.offset = offset
}

func NewSubGramOffset(subGram string, offset uint16) SubGramOffset {
	return SubGramOffset{
		subGram: subGram,
		offset:  offset,
	}
}
