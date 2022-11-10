package vToken

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvl"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenFuzzyQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenRegexQuery"
	"testing"
	"time"
)

//todo
const (
	QMIN      = 2
	QMAX      = 12
	THRESHOLD = 3
)

func TestCLVCMatchSearch(t *testing.T) {
	clvcDicTrie := CreateCLVCDic()
	IndexTrie := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())

	var tests = []struct {
		input string
		want  int
	}{
		{"GET /images/", 1},
		{"HTTP/1.0", 6},
		{"hotel.jpg", 0},
		{"GET /english", 8},
		{"imageses", 0},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenMatchQuery.MatchSearch(test.input, clvcDicTrie.Root(), IndexTrie.Root(), clvcDicTrie.Qmin()))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}
}

func TestCLVCFuzzySearchDistanceEqualOne(t *testing.T) {
	clvcDicTrie := CreateCLVCDic()
	IndexTrie := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"immages", 10},
		{"httpp", 10},
		{"historyd", 4},
		{"parisa", 1},
		{"english", 8},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenFuzzyQuery.FuzzySearchComparedWithES(test.input, IndexTrie.Root(), 1))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVCFuzzySearchDistanceEqualTwo(t *testing.T) {
	clvcDicTrie := CreateCLVCDic()
	IndexTrie := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"immagesm", 10},
		{"htpp", 10},
		{"historid", 4},
		{"parisae", 1},
		{"englisho", 8},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenFuzzyQuery.FuzzySearchComparedWithES(test.input, IndexTrie.Root(), 2))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVCRegexSearch(t *testing.T) {
	clvcDicTrie := CreateCLVCDic()
	IndexTrie := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"en.+ish", 8},
		{"ven.*paris", 1},
		{"fren.h", 1},
		{"histo.*", 4},
		{"ima.es", 10},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenRegexQuery.RegexSearch(test.input, IndexTrie))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVLMatchSearch(t *testing.T) {
	clvlDicTrie := CreateCLVLDic()
	IndexTrie := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"GET /images/", 1},
		{"HTTP/1.0", 6},
		{"hotel.jpg", 0},
		{"GET /english", 8},
		{"imageses", 0},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenMatchQuery.MatchSearch(test.input, clvlDicTrie.Root(), IndexTrie.Root(), clvlDicTrie.Qmin()))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVLFuzzySearchDistanceEqualOne(t *testing.T) {
	clvlDicTrie := CreateCLVLDic()
	IndexTrie := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"immages", 10},
		{"httpp", 10},
		{"historyd", 4},
		{"parisa", 1},
		{"english", 8},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenFuzzyQuery.FuzzySearchComparedWithES(test.input, IndexTrie.Root(), 1))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVLFuzzySearchDistanceEqualTwo(t *testing.T) {
	clvlDicTrie := CreateCLVLDic()
	IndexTrie := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"immagesm", 10},
		{"htpp", 10},
		{"historid", 4},
		{"parisae", 1},
		{"englisho", 8},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenFuzzyQuery.FuzzySearchComparedWithES(test.input, IndexTrie.Root(), 2))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVLRegexSearch(t *testing.T) {
	clvlDicTrie := CreateCLVLDic()
	IndexTrie := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"en.+ish", 8},
		{"ven.*paris", 1},
		{"fren.h", 1},
		{"histo.*", 4},
		{"ima.es", 10},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(tokenRegexQuery.RegexSearch(test.input, IndexTrie))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func CreateCLVCDic() *tokenClvc.TrieTree {
	strDic := []utils.LogSeries{
		utils.LogSeries{
			"GET /images/space.gif HTTP/1.0",
			1,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/tickets/images/ticket_header.gif HTTP/1.1",
			2,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/tickets/images/hm_f98_top.gif HTTP/1.1",
			3,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/venues/cities/images/paris/venue_paris_header.jpg HTTP/1.0",
			4,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /images/mondiresa_hotel.jpg HTTP/1.1",
			5,
			time.Now().Unix(),
		},
	}
	clvcdic := tokenClvc.NewCLVCDic(QMIN, QMAX)
	clvcdic.GenerateClvcDictionaryTree(strDic, QMIN, QMAX, THRESHOLD)
	return clvcdic.TrieTree
}

func ArrToMap(arr []utils.LogSeries) map[utils.SeriesId]string {
	logMap := make(map[utils.SeriesId]string)
	for i := 0; i < len(arr); i++ {
		sid := utils.NewSeriesId(arr[i].Tsid, arr[i].TimeStamp)
		logMap[sid] = arr[i].Log
	}
	return logMap
}

func CreateCLVLDic() *tokenClvc.TrieTree {
	strDic := []utils.LogSeries{
		utils.LogSeries{
			"GET /images/space.gif HTTP/1.0",
			1,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/tickets/images/ticket_header.gif HTTP/1.1",
			2,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/tickets/images/hm_f98_top.gif HTTP/1.1",
			3,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/venues/cities/images/paris/venue_paris_header.jpg HTTP/1.0",
			4,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /images/mondiresa_hotel.jpg HTTP/1.1",
			5,
			time.Now().Unix(),
		},
	}
	strQueryWorkload := []utils.LogSeries{
		strDic[0],
		strDic[1],
	}
	clvldic := tokenClvl.NewCLVLDic(QMIN, 0)
	clvldic.GenerateClvlDictionaryTree(ArrToMap(strDic), QMIN, ArrToMap(strQueryWorkload))
	return clvldic.TrieTree
}

func CreateIndex(dicTrie tokenClvc.TrieTree, qmin int, qmax int) *tokenIndex.IndexTree {
	strIndex := []utils.LogSeries{
		utils.LogSeries{
			"GET /french/venues/images/venue_bu_another_off.gif HTTP/1.0",
			1,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/tickets/images/ticket_header.gif HTTP/1.1",
			2,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/tickets/images/hm_f98_top.gif HTTP/1.1",
			3,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/venues/cities/images/paris/venue_paris_header.jpg HTTP/1.0",
			4,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/images/news_btn_letter_off.gif HTTP/1.1",
			5,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /images/mondiresa_hotel.jpg HTTP/1.1",
			6,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/history/past_cups/images/past_bu_30_off.gif HTTP/1.0",
			7,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/history/past_cups/images/past_bracket_bot.gif HTTP/1.0",
			8,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/history/past_cups/images/posters/uruguay30.gif HTTP/1.0",
			9,
			time.Now().Unix(),
		}, utils.LogSeries{
			"GET /english/history/history_of/images/cup/share.gif HTTP/1.0",
			10,
			time.Now().Unix(),
		},
	}
	indexTrie, _ := tokenIndex.GenerateIndexTree(strIndex, qmin, qmax, dicTrie.Root())
	return indexTrie
}
