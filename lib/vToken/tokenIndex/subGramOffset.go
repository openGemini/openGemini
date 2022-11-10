package tokenIndex

type SubTokenOffset struct {
	subToken []string
	offset   uint16
}

func (s *SubTokenOffset) SubToken() []string {
	return s.subToken
}

func (s *SubTokenOffset) SetSubToken(subToken []string) {
	s.subToken = subToken
}

func (s *SubTokenOffset) Offset() uint16 {
	return s.offset
}

func (s *SubTokenOffset) SetOffset(offset uint16) {
	s.offset = offset
}

func NewSubGramOffset(subToken []string, offset uint16) SubTokenOffset {
	return SubTokenOffset{
		subToken: subToken,
		offset:   offset,
	}
}
