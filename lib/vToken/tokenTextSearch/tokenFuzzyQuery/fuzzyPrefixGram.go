package tokenFuzzyQuery

type FuzzyPrefixGram struct {
	gram string
	pos  int
}

func (p *FuzzyPrefixGram) Gram() string {
	return p.gram
}

func (p *FuzzyPrefixGram) SetGram(gram string) {
	p.gram = gram
}

func (p *FuzzyPrefixGram) Pos() int {
	return p.pos
}

func (p *FuzzyPrefixGram) SetPos(pos int) {
	p.pos = pos
}
func NewFuzzyPrefixGram(gram string, pos int) FuzzyPrefixGram {
	return FuzzyPrefixGram{
		gram: gram,
		pos:  pos,
	}
}
