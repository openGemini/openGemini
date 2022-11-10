package tsi

import (
	"bufio"
	"fmt"
	"github.com/openGemini/openGemini/lib/clvIndex"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"os"
	"testing"
	"time"
)

func ReadIndexDataFromFile() []*influx.Row {
	filename := "500000Index.txt"
	var id uint64 = 0
	rows := make([]*influx.Row, 0)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		logValue := scanner.Text() //string
		row := new(influx.Row)
		row.Name = "clvTable"
		row.Fields = make([]influx.Field, 1)
		row.Fields[0].Key = "logs"
		row.Fields[0].StrValue = logValue
		row.Timestamp = time.Now().Unix()
		id = id + 1
		row.SeriesId = id
		rows = append(rows, row)
	}
	file.Close()
	return rows

}

func TestCreateCLVIndexFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
	FieldKeys := make(map[string][]string)
	FieldKeys["clvTable"] = []string{
		"logs",
	}
	idx, _ := NewTextIndex(nil)
	start := time.Now().UnixNano() / 1e9
	for i := range rows {
		idx.CreateIndexIfNotExists(nil, rows[i], 0)
	}
	end := time.Now().UnixNano() / 1e9
	fmt.Println("时间s", end-start)
}

//gram search
/*
func TestCLVGramMatchSearchFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{"GET", 498326},
		{"tickets", 10541},
		{"playing", 41531},
		{"GET /images/", 248532},
		{"home_sponsor.gif", 3979},
		{"/cup/15txt.gif", 106},
		{"11187", 43},
		{"french", 39463},
		{"/history/past_cups/images/", 6635},
		{"comp_bu_stage1n.gif", 856},
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
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVGramFuzzySearchDistanceEqualOneFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{"imagese", 438347},
		{"tickete", 10541},
		{"bio138.html", 64},
		{"ipta1934.gif", 142},
		{"ench/nwws/", 956},
		{"historm", 19021},
		{"indivduals", 1610},
		{"posterse", 1079},
		{"trivial", 8452},
		{"_quests_", 379},
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
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVGramFuzzySearchDistanceEqualTwoFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{"imaoese", 438347},
		{"ticklete", 10541},
		{"bio18.html", 541},
		{"ipt1934.gif", 142},
		{"enh/nwws/", 956},
		{"hiostorm", 19021},
		{"inpivduals", 1610},
		{"posoterse", 1079},
		{"truvial", 8453},
		{"_qyuests_", 379},
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
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVGramRegexSearchFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{"en.+ish", 203592},
		{"im.ges", 438347},
		{"s10.327", 2079},
		{"hm_a.*", 12639},
		{"his.*ry", 18954},
		{"team.*io138", 64},
		{"GET /fr.*", 39402},
		{"past_(bracket|bu)", 5094},
		{"hm_(day|anime)", 9860},
		{"(hm_brdl|info).gif", 9193},
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
		fmt.Println("测试用例全部通过")
	}

}*/

//token search
/*
func TestCLVTokenMatchSearchFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{"GET", 498326},
		{"GET /english/images/", 100860},
		{"GET /images/", 248532},
		{"GET /english/images/team_hm_header_shad.gif HTTP/1.0", 941},
		{"GET /images/s102325.gif HTTP/1.0", 1580},
		{"GET /english/history/history_of/images/cup/", 1196},
		{"/images/space.gif", 10143},
		{"GET / HTTP/1.0", 4911},
		{"11187", 43},
		{"french", 39421},
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
		fmt.Println("测试用例全部通过")
	}

}
func TestCLVTokenFuzzySearchDistanceEqualOneFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{"httpp", 500000},
		{"1.00", 394806},
		{"englis", 203477},
		{"ticketts", 5500},
		{"ticket_header.gof", 377},
		{"1.2", 499974},
		{"hm._f98_top.gif", 3184},
		{"mondirsa_logo.gif", 88},
		{"moniresa_hotel.jpg", 96},
		{"playingk", 41531},
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
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVTokenFuzzySearchDistanceEqualTwoFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{"httppr", 500000},
		{"1.00r", 394808},
		{"englirs", 203477},
		{"ticketrts", 5500},
		{"ticketr_header.gof", 377},
		{"1.r2", 499974},
		{"hm.r_f98_top.gif", 3184},
		{"mondrirsa_logo.gif", 88},
		{"monirresa_hotel.jpg", 96},
		{"playinrgk", 41531},
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
		fmt.Println("测试用例全部通过")
	}

}

func TestCLVTokenRegexSearchFromFile(t *testing.T) {
	rows := ReadIndexDataFromFile()
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
		{".*gif", 398240},
		{"body.*", 3515},
		{"nav_home.*gif", 5074},
		{"team_group.*gif", 11555},
		{"s1023(36|25)", 4248},
		{"home(_intro.anim.gif|_fr_button.gif)", 10178},
		{"(paris_i|quizbg).gif", 457},
		{"(02how|04luck).gif", 622},
		{"ligne(b01|01)", 3190},
		{"team.*io138", 64},
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
		fmt.Println("测试用例全部通过")
	}

}*/

//Benchmark
func BenchmarkNewCLVIndexFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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

//Benchmark gram search
func BenchmarkCLVGramMatchSearchFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{"GET", 498326},
		{"tickets", 10541},
		{"playing", 41531},
		{"GET /images/", 248532},
		{"home_sponsor.gif", 3979},
		{"/cup/15txt.gif", 106},
		{"11187", 43},
		{"french", 39463},
		{"/history/past_cups/images/", 6635},
		{"comp_bu_stage1n.gif", 856},
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

func BenchmarkCLVGramFuzzySearchDistanceEqualOneFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{"imagese", 438347},
		{"tickete", 10541},
		{"bio138.html", 64},
		{"ipta1934.gif", 142},
		{"ench/nwws/", 956},
		{"historm", 19021},
		{"indivduals", 1610},
		{"posterse", 1079},
		{"trivial", 8452},
		{"_quests_", 379},
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

func BenchmarkCLVGramFuzzySearchDistanceEqualTwoFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{"imaoese", 438347},
		{"ticklete", 10541},
		{"bio18.html", 541},
		{"ipt1934.gif", 142},
		{"enh/nwws/", 956},
		{"hiostorm", 19021},
		{"inpivduals", 1610},
		{"posoterse", 1079},
		{"truvial", 8453},
		{"_qyuests_", 379},
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

func BenchmarkCLVGramRegexSearchFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{"en.+ish", 203592},
		{"im.ges", 438347},
		{"s10.327", 2079},
		{"hm_a.*", 12639},
		{"his.*ry", 18954},
		{"team.*io138", 64},
		{"GET /fr.*", 39402},
		{"past_(bracket|bu)", 5094},
		{"hm_(day|anime)", 9860},
		{"(hm_brdl|info).gif", 9193},
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

//Benchmark token search
func BenchmarkCLVTokenMatchSearchFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{"GET", 498326},
		{"GET /english/images/", 100860},
		{"GET /images/", 248532},
		{"GET /english/images/team_hm_header_shad.gif HTTP/1.0", 941},
		{"GET /images/s102325.gif HTTP/1.0", 1580},
		{"GET /english/history/history_of/images/cup/", 1196},
		{"/images/space.gif", 10143},
		{"GET / HTTP/1.0", 4911},
		{"11187", 43},
		{"french", 39421},
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

func BenchmarkCLVTokenFuzzySearchDistanceEqualOneFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{"httpp", 500000},
		{"1.00", 394806},
		{"englis", 203477},
		{"ticketts", 5500},
		{"ticket_header.gof", 377},
		{"1.2", 499974},
		{"hm._f98_top.gif", 3184},
		{"mondirsa_logo.gif", 88},
		{"moniresa_hotel.jpg", 96},
		{"playingk", 41531},
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

func BenchmarkCLVTokenFuzzySearchDistanceEqualTwoFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{"httppr", 500000},
		{"1.00r", 394808},
		{"englirs", 203477},
		{"ticketrts", 5500},
		{"ticketr_header.gof", 377},
		{"1.r2", 499974},
		{"hm.r_f98_top.gif", 3184},
		{"mondrirsa_logo.gif", 88},
		{"monirresa_hotel.jpg", 96},
		{"playinrgk", 41531},
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

func BenchmarkCLVTokenRegexSearchFromFile(b *testing.B) {
	rows := ReadIndexDataFromFile()
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
		{".*gif", 398240},
		{"body.*", 3515},
		{"nav_home.*gif", 5074},
		{"team_group.*gif", 11555},
		{"s1023(36|25)", 4248},
		{"home(_intro.anim.gif|_fr_button.gif)", 10178},
		{"(paris_i|quizbg).gif", 457},
		{"(02how|04luck).gif", 622},
		{"ligne(b01|01)", 3190},
		{"team.*io138", 64},
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
