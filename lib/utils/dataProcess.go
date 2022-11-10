package utils

import (
	"github.com/clipperhouse/uax29/words"
	"math/rand"
	"strings"
	"time"
)

type LogSeries struct {
	Log       string
	Tsid      uint64
	TimeStamp int64
}

func DataProcess(row string) ([]string, string) {
	tokenArray := make([]string, 0)
	res := ""
	lowdata := strings.ToLower(row)
	text := []byte(lowdata)
	segments := words.NewSegmenter(text) //原始数据分词
	for segments.Next() {
		s := string(segments.Bytes())
		if (s != " " && (len(s) != 1)) || (len(s) == 1 && (s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= '0' && s[0] <= '9')) {
			tokenArray = append(tokenArray, s)
			res += s + " "
		}
	}
	res = strings.TrimSpace(res)

	return tokenArray, res
}

func HasSample(buffDicStrings []LogSeries, wlen int) map[SeriesId]string {
	res := make(map[SeriesId]string)
	randnum := genRandNum(wlen, len(buffDicStrings))
	for _, num := range randnum {
		queryWorkLoadId:=NewSeriesId(buffDicStrings[num].Tsid,buffDicStrings[num].TimeStamp)
		res[queryWorkLoadId]=buffDicStrings[num].Log
	}
	return res
}

func genRandNum(wlen int, rge int) []int {
	mp := make(map[int]bool, 0)
	res := make([]int, 0)
	for i := 0; i < wlen; {
		rand.Seed(int64(i) + time.Now().UnixNano())
		num := rand.Intn(rge)
		if _, ok := mp[num]; !ok {
			mp[num] = true
			res = append(res, num)
			i++
		} else {
			continue
		}
	}
	return res
}

func LogSeriesToMap(buffDicStrings []LogSeries)map[SeriesId]string{
	res := make(map[SeriesId]string)
	for i:=0;i<len(buffDicStrings);i++{
		logseries := buffDicStrings[i]
		sid:=NewSeriesId(logseries.Tsid,logseries.TimeStamp)
		res[sid]=logseries.Log
	}
	return res
}
