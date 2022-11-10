package gramRegexQuery

type Parameters struct {
	defaultGramLen int
	alphabet       []int
}

func NewParameters() *Parameters {
	alphabet := make([]int, 127)
	for i := 0; i < 127; i++ {
		alphabet[i] = 1
	}
	return &Parameters{
		defaultGramLen: 3,
		alphabet:       alphabet,
	}
}
