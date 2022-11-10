package tokenClvc

type FreqList struct {
	token string
	freq  int
}

func (f *FreqList) Token() string {
	return f.token
}

func (f *FreqList) SetToken(token string) {
	f.token = token
}

func (f *FreqList) Freq() int {
	return f.freq
}

func (f *FreqList) SetFreq(freq int) {
	f.freq = freq
}

func NewFreqList(token string, freq int) *FreqList {
	return &FreqList{
		token: token,
		freq:  freq,
	}
}
