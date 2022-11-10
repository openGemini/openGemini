package vGram

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvl"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramFuzzyQuery"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramRegexQuery"
	"testing"
	"time"
)


const (
	QMIN      = 2
	QMAX      = 12
	THRESHOLD = 3
	LOGTREEMAX = 12
)

func TestCLVCMatchSearch(t *testing.T) {
	clvcDicTrie := CreateCLVCDic()
	IndexTrie, _ := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"GET /images/", 1},
		{"HTTP/1.0", 6},
		{"hotel.jpg", 1},
		{"GET /english", 8},
		{"imageses", 0},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramMatchQuery.MatchSearch(test.input, clvcDicTrie.Root(), IndexTrie.Root(), clvcDicTrie.Qmin()))
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
	IndexTrie, LogTree := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"GET /imeges/", 1},
		{"HTTP/1.0", 10},
		{"hotels.jpg", 1},
		{"GET english", 8},
		{"imageses", 0},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramFuzzyQuery.FuzzyQueryGramQmaxTrie(LogTree.Root(), test.input, clvcDicTrie.Root(), IndexTrie.Root(), clvcDicTrie.Qmin(), clvcDicTrie.Qmax(), 1))
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
	IndexTrie, LogTree := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"GT /imeges/", 1},
		{"HTTP/1.0", 10},
		{"hotels.jpeg", 1},
		{"GET englosh", 8},
		{"imageses", 10},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramFuzzyQuery.FuzzyQueryGramQmaxTrie(LogTree.Root(), test.input, clvcDicTrie.Root(), IndexTrie.Root(), clvcDicTrie.Qmin(), clvcDicTrie.Qmax(), 2))
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
	IndexTrie, _ := CreateIndex(*clvcDicTrie, clvcDicTrie.Qmin(), clvcDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"en.+ish", 8},
		{"ven.*paris", 1},
		{"tickets", 2},
		{"GET /e.+glish", 8},
		{"ima.es", 10},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramRegexQuery.RegexSearch(test.input, clvcDicTrie, IndexTrie))
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
	IndexTrie, _ := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"GET /images/", 1},
		{"HTTP/1.0", 6},
		{"hotel.jpg", 1},
		{"GET /english", 8},
		{"imageses", 0},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramMatchQuery.MatchSearch(test.input, clvlDicTrie.Root(), IndexTrie.Root(), clvlDicTrie.Qmin()))
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
	IndexTrie, LogTree := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"GET /imeges/", 1},
		{"HTTP/1.0", 10},
		{"hotels.jpg", 1},
		{"GET english", 8},
		{"imageses", 0},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramFuzzyQuery.FuzzyQueryGramQmaxTrie(LogTree.Root(), test.input, clvlDicTrie.Root(), IndexTrie.Root(), clvlDicTrie.Qmin(), clvlDicTrie.Qmax(), 1))
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
	IndexTrie, LogTree := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"GT /imeges/", 1},
		{"HTTP/1.0", 10},
		{"hotels.jpeg", 1},
		{"GET englosh", 8},
		{"imageses", 10},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramFuzzyQuery.FuzzyQueryGramQmaxTrie(LogTree.Root(), test.input, clvlDicTrie.Root(), IndexTrie.Root(), clvlDicTrie.Qmin(), clvlDicTrie.Qmax(), 2))
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
	IndexTrie, _ := CreateIndex(*clvlDicTrie, clvlDicTrie.Qmin(), clvlDicTrie.Qmax())
	var tests = []struct {
		input string
		want  int
	}{
		{"en.+ish", 8},
		{"ven.*paris", 1},
		{"tickets", 2},
		{"GET /e.+glish", 8},
		{"ima.es", 10},
	}
	flagPass := true
	for i, test := range tests {
		resLen := len(gramRegexQuery.RegexSearch(test.input, clvlDicTrie, IndexTrie))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		fmt.Println("测试用例全部通过")
	}

}

func CreateCLVCDic() *gramClvc.TrieTree {
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
	clvcdic := gramClvc.NewCLVCDic(QMIN, QMAX)
	clvcdic.GenerateClvcDictionaryTree(strDic, QMIN, QMAX, THRESHOLD)
	return clvcdic.TrieTree
}

func ArrToMap(arr []utils.LogSeries) map[utils.SeriesId]string{
	logMap:=make(map[utils.SeriesId]string)
	for i:=0;i<len(arr);i++{
		sid:=utils.NewSeriesId(arr[i].Tsid,arr[i].TimeStamp)
		logMap[sid]=arr[i].Log
	}
	return logMap
}

func CreateCLVLDic() *gramClvc.TrieTree {
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
	clvldic := gramClvl.NewCLVLDic(QMIN, 0)
	clvldic.GenerateClvlDictionaryTree(ArrToMap(strDic), QMIN, ArrToMap(strQueryWorkload))
	return clvldic.TrieTree
}

func CreateIndex(dicTrie gramClvc.TrieTree, qmin int, qmax int) (*gramIndex.IndexTree, *gramIndex.LogTree) {
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
	indexTrie, _, logTree := gramIndex.GenerateIndexTree(strIndex, qmin, qmax, LOGTREEMAX, dicTrie.Root())
	return indexTrie, logTree
}
