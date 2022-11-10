package gramClvc

type FreqList struct {
	char string
	freq int
}

func (f *FreqList) Char() string {
	return f.char
}

func (f *FreqList) SetChar(char string) {
	f.char = char
}

func (f *FreqList) Freq() int {
	return f.freq
}

func (f *FreqList) SetFreq(freq int) {
	f.freq = freq
}

func NewFreqList(char string, freq int) *FreqList {
	return &FreqList{
		char: char,
		freq: freq,
	}
}
