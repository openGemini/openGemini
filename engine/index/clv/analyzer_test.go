/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package clv

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
)

func getLogContent() []string {
	logStrs := []string{
		"get french index.html http 1.0",
		"get images tck_pkit_fx_a.jpg http 1.1",
		"get french competition flashed_stage1.htm http 1.0",
		"get images ps_bdr_r.gif http 1.1",
		"get english images hm_official.gif http 1.1",
		"get japanese nav_inet.html http 1.0",
	}
	return logStrs
}

func TestDefaultAnalyzer(t *testing.T) {
	path := "/tmp/clv_analyzer"
	a := newAnalyzer(path, "logmst", "content", 0)

	logStr := "GET /french/index.html HTTP/1.0"
	oriVtokens := []VToken{
		{[]string{"GET"}, 0, 0},
		{[]string{"french"}, 0, 1},
		{[]string{"index.html"}, 0, 2},
		{[]string{"HTTP"}, 0, 3},
		{[]string{"1.0"}, 0, 4},
	}
	vtokens, err := a.Analyze([]byte(logStr))
	if err != nil {
		t.Fatalf("use default analyzer to analyze token failed")
	}
	if !reflect.DeepEqual(oriVtokens, vtokens) {
		t.Fatalf("use default analyzer to analyze token to get incorrect results")
	}
}

func TestCreateLearningAnalyzerAndAnalyze(t *testing.T) {
	path := "/tmp/clv_analyzer"
	a := newAnalyzer(path, "logmst", "content", 0)

	logStrs := getLogContent()
	for i := 0; i < len(logStrs); i++ {
		a.InsertToDictionary(logStrs[i])
	}

	if a.dictionary == nil {
		t.Fatalf("insert tokens to dictionary failed")
	}

	err := a.AssignId()
	if err != nil {
		t.Fatalf("assign ID for tokenizer dictionary failed: %v", err)
	}

	if a.dicMap == nil {
		t.Fatalf("assign ID for tokenizer dictionary failed")
	}

	logStr := "get /french/index.html http/1.0"
	oriVtokens := []VToken{
		{[]string{"get", "french", "index.html", "http", "1.0"}, 10, 0},
	}
	vtokens, err := a.Analyze([]byte(logStr))
	if err != nil {
		t.Fatalf("use learning analyzer to analyze token failed")
	}
	if !reflect.DeepEqual(oriVtokens, vtokens) {
		t.Fatalf("use learning analyzer to analyze failed, exp: %v, get:%v", oriVtokens, vtokens)
	}
}

func TestFindVtokenByID(t *testing.T) {
	path := "/tmp/clv_analyzer"
	a := newAnalyzer(path, "logmst", "content", 0)

	logStrs := getLogContent()
	for i := 0; i < len(logStrs); i++ {
		a.InsertToDictionary(logStrs[i])
	}
	err := a.AssignId()
	if err != nil {
		t.Fatalf("assign id for analyzer failed: %v", err)
	}

	oriVtokens := "get french index.html http 1.0 "
	vtokens := a.FindVtokenByID(10)

	if vtokens != oriVtokens {
		t.Fatalf("get vtokens by ID failed")
	}
}

func TestAnalyzeCache(t *testing.T) {
	path := "/tmp/clv_analyzer"
	a1 := newAnalyzer(path, "logmst", "content", 1)
	a2 := newAnalyzer(path, "logmst", "content", 2)

	cache.init()
	// save invalid analyzer
	cache.saveAnalyzer(nil)
	// save valid analyzer
	cache.saveAnalyzer(a1)
	// save duplicate analyzer
	cache.saveAnalyzer(a1)
	// save different version's analyzer
	cache.saveAnalyzer(a2)

	// get the analyzer by path+dbname+mstname+filedname+version
	// wrong filedname
	ca := cache.getAnalyzer(path, "logmst", "content0", 1)
	if ca != nil {
		t.Fatal()
	}

	ca = cache.getAnalyzer(path, "logmst", "content", 1)
	if ca == nil {
		t.Fatal()
	}
	if !reflect.DeepEqual(ca, a1) {
		t.Fatal()
	}

	ca = cache.getAnalyzer(path, "logmst", "content", 2)
	if ca == nil {
		t.Fatal()
	}
	if !reflect.DeepEqual(ca, a2) {
		t.Fatal()
	}
}

func TestGetCollectorFromAnalyzeCache(t *testing.T) {
	path := "/tmp/clv_analyzer"
	cache.init()

	c := cache.getCollector(path, "logmst", "content")
	if c == nil {
		t.Fatalf("get a collector for analyzer leanrning failed")
	}
	// get collector aggin
	c = cache.getCollector(path, "logmst", "content")
	if c == nil {
		t.Fatalf("get a collector for analyzer leanrning failed")
	}
}

func TestCollectLogAndGetAnalyzer(t *testing.T) {
	InitConfig(&config.ClvConfig{QMax: 7, Threshold: 2, DocCount: 5, Enabled: true})
	defer InitConfig(&config.ClvConfig{QMax: 7, Threshold: 100, DocCount: 500000, Enabled: true})
	path := "/tmp/clv_analyzer"
	os.RemoveAll(path)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	cache.init()
	// get a default analyzer first.
	a, err := GetAnalyzer(path, "logmst", "content", Unknown)
	if err != nil || a == nil {
		t.Fatalf("get the default analyzer failed")
	}
	if a.collector == nil {
		t.Fatalf("the default analyzer do not have collector")
	}

	// save the learn-type dictionary to mergeset.
	logStrs := getLogContent()
	for i := 0; i < len(logStrs); i++ {
		_, err = a.Analyze([]byte(logStrs[i]))
		if err != nil {
			t.Fatalf("analyze logstr failed: %v", err)
		}
	}
	// wait for the learn-type dictionary to be compeletely flushed to the disk
	time.Sleep(2 * time.Second)

	// re-obtain the analyzer from mergeset
	a, err = GetAnalyzer(path, "logmst", "content", Unknown)
	if err != nil || a == nil {
		t.Fatalf("get the default analyzer failed")
	}
	//default version is 1, learn-type dic's version >= 2
	if a.version < 2 {
		t.Fatalf("do's get learn-type analyzer, version:%d", a.version)
	}

	// re-obtain the analyzer from cache
	a, err = GetAnalyzer(path, "logmst", "content", Unknown)
	if err != nil || a == nil {
		t.Fatalf("get the default analyzer failed")
	}

	a1, err0 := loadAnalyzer(path, "logmst", "content", a.version)
	if err0 != nil || a1 == nil {
		t.Fatalf("get the default analyzer failed")
	}
	if !reflect.DeepEqual(a.dicMap, a1.dicMap) {
		t.Fatalf("the analyzer read from cache is not equal to the analyzer read from disk")
	}
}
