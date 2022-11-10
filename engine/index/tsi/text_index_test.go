/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package tsi

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/clvIndex"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func CreateIndexData() []*influx.Row {
	rows := make([]*influx.Row, 10)
	for i, _ := range rows {
		rows[i] = new(influx.Row)
	}
	rows[0].Name = "clvTable"
	rows[0].Fields = make([]influx.Field, 1)
	rows[0].Fields[0].Key = "logs"
	rows[0].Fields[0].StrValue = "GET /french/venues/images/venue_bu_another_off.gif HTTP/1.0"
	rows[0].Timestamp = time.Now().Unix()
	rows[0].SeriesId = 1

	rows[1].Name = "clvTable"
	rows[1].Fields = make([]influx.Field, 1)
	rows[1].Fields[0].Key = "logs"
	rows[1].Fields[0].StrValue = "GET /english/tickets/images/ticket_header.gif HTTP/1.1"
	rows[1].Timestamp = time.Now().Unix()
	rows[1].SeriesId = 2

	rows[2].Name = "clvTable"
	rows[2].Fields = make([]influx.Field, 1)
	rows[2].Fields[0].Key = "logs"
	rows[2].Fields[0].StrValue = "GET /english/tickets/images/hm_f98_top.gif HTTP/1.1"
	rows[2].Timestamp = time.Now().Unix()
	rows[2].SeriesId = 3

	rows[3].Name = "clvTable"
	rows[3].Fields = make([]influx.Field, 1)
	rows[3].Fields[0].Key = "logs"
	rows[3].Fields[0].StrValue = "GET /english/venues/cities/images/paris/venue_paris_header.jpg HTTP/1.0"
	rows[3].Timestamp = time.Now().Unix()
	rows[3].SeriesId = 4

	rows[4].Name = "clvTable"
	rows[4].Fields = make([]influx.Field, 1)
	rows[4].Fields[0].Key = "logs"
	rows[4].Fields[0].StrValue = "GET /english/images/news_btn_letter_off.gif HTTP/1.1"
	rows[4].Timestamp = time.Now().Unix()
	rows[4].SeriesId = 5

	rows[5].Name = "clvTable"
	rows[5].Fields = make([]influx.Field, 1)
	rows[5].Fields[0].Key = "logs"
	rows[5].Fields[0].StrValue = "GET /images/mondiresa_hotel.jpg HTTP/1.1"
	rows[5].Timestamp = time.Now().Unix()
	rows[5].SeriesId = 6

	rows[6].Name = "clvTable"
	rows[6].Fields = make([]influx.Field, 1)
	rows[6].Fields[0].Key = "logs"
	rows[6].Fields[0].StrValue = "GET /english/history/past_cups/images/past_bu_30_off.gif HTTP/1.0"
	rows[6].Timestamp = time.Now().Unix()
	rows[6].SeriesId = 7

	rows[7].Name = "clvTable"
	rows[7].Fields = make([]influx.Field, 1)
	rows[7].Fields[0].Key = "logs"
	rows[7].Fields[0].StrValue = "GET /english/history/past_cups/images/past_bracket_bot.gif HTTP/1.0"
	rows[7].Timestamp = time.Now().Unix()
	rows[7].SeriesId = 8

	rows[8].Name = "clvTable"
	rows[8].Fields = make([]influx.Field, 1)
	rows[8].Fields[0].Key = "logs"
	rows[8].Fields[0].StrValue = "GET /english/history/past_cups/images/posters/uruguay30.gif HTTP/1.0"
	rows[8].Timestamp = time.Now().Unix()
	rows[8].SeriesId = 9

	rows[9].Name = "clvTable"
	rows[9].Fields = make([]influx.Field, 1)
	rows[9].Fields[0].Key = "logs"
	rows[9].Fields[0].StrValue = "GET /english/history/history_of/images/cup/share.gif HTTP/1.0"
	rows[9].Timestamp = time.Now().Unix()
	rows[9].SeriesId = 10

	return rows

}

func TestCreateCLVIndex(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

}

//gram search
/*
func TestCLVGramMatchSearch(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.MATCHSEARCH, tests[i].input, 0))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVGramFuzzySearchDistanceEqualOne(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input, 1))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVGramFuzzySearchDistanceEqualTwo(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input, 2))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVGramRegexSearch(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.REGEXSEARCH, tests[i].input, 0))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}*/

//token search
/*
func TestCLVTokenMatchSearch(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.MATCHSEARCH, tests[i].input, 0))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}
func TestCLVTokenFuzzySearchDistanceEqualOne(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input, 1))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVTokenFuzzySearchDistanceEqualTwo(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input, 2))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVTokenRegexSearch(t *testing.T) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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
		resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.REGEXSEARCH, tests[i].input, 0))
		if resLen != test.want {
			t.Errorf("第%d个测试用例未通过", i)
			flagPass = false
		}
	}
	if flagPass == true {
		t.Logf("测试用例全部通过")
		fmt.Println("测试用例全部通过")
	}

}*/

// Benchmark
func BenchmarkNewCLVIndex(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx, _ := NewTextIndex(nil)
		for i := range rows {
			idx.CreateIndexIfNotExists(nil, rows[i], 0)
		}
	}
	b.StopTimer()
}

// Benchmark token search
func BenchmarkCLVTokenMatchSearch(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.MATCHSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}

func BenchmarkCLVTokenFuzzySearchDistanceEqualOne(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}

func BenchmarkCLVTokenFuzzySearchDistanceEqualTwo(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}

func BenchmarkCLVTokenRegexSearch(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.REGEXSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}

// Benchmark gram search
func BenchmarkCLVGramMatchSearch(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.MATCHSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}

func BenchmarkCLVGramFuzzySearchDistanceEqualOne(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}

func BenchmarkCLVGramFuzzySearchDistanceEqualTwo(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.FUZZYSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}

func BenchmarkCLVGramRegexSearch(b *testing.B) {
	rows := CreateIndexData()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}

	idx, _ := NewTextIndex(nil)
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}

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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		flagPass := true
		for i, test := range tests {
			resLen := len(idx.ClvIndex.CLVSearch("clvTable", "logs", clvIndex.REGEXSEARCH, tests[i].input))
			if resLen != test.want {
				flagPass = false
			}
		}
		if flagPass == true {
			fmt.Println("测试用例全部通过")
		}
	}
	b.StopTimer()
}
