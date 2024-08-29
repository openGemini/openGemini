// Copyright Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/rand"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func RandInt() int {
	return rand.Intn(10000)
}

func RandFloat(min, max float64) float64 {
	var res float64
	res = min + rand.Float64()*(max-min)
	return res
}

func RandBool() string {
	if rand.Intn(2)%2 == 0 {
		return "true"
	}
	return "false"
}

func TestServer_Query_Consistency(t *testing.T) {
	t.Parallel()
	if url := os.Getenv("TSDBURL"); url == "" {
		t.Skip("Skipping.  DO not run TestServer_Query_Consistency1")
	}

	seriesCountTotal := 10000
	pointCountTotal := 4000
	for i := 1; i <= seriesCountTotal; i++ {
		for j := 1; j <= pointCountTotal; j++ {
			psServer := OpenDefaultServer(NewParseConfig(testCfgPath))
			defer psServer.Close()

			tsdbServer := OpenTSDBServer(NewParseConfig(testCfgPath))
			defer tsdbServer.Close()

			test := tests.load(t, "consistency_check")
			test.writes = Generate_Data_For_Consistency(i, j)

			for iQuery, query := range test.queries {
				result := t.Run(query.name, func(t *testing.T) {
					if iQuery == 0 {
						if err := test.init(tsdbServer); err != nil {
							t.Fatalf("test tsdb init failed: %s", err)
						}

						test.initialized = false

						if err := test.init(psServer); err != nil {
							t.Fatalf("test gemini tsdb init failed: %s", err)
						}
					}
					if query.skip {
						t.Skipf("SKIP:: %s", query.name)
					}

					tsdbResult, err := query.ExecuteResult(tsdbServer)
					if err != nil {
						t.Errorf("[TSDB query failed]SeriesCount:%d, PointCount:%d, error:%s", i, j, query.Error(err))
					}
					actResult, err := query.ExecuteResult(psServer)
					if err != nil {
						t.Errorf("[Gemini TSDB query failed]SeriesCount:%d, PointCount:%d, error:%s", i, j, query.Error(err))
					}

					if tsdbResult != actResult {
						if notEqual(tsdbResult, actResult) {
							t.Errorf("not equal SeriesCount:%d, PointCount:%d", i, j)
							t.Errorf("TSDB result:%s\n", tsdbResult)
							t.Errorf("Gemini TSDB result:%s\n", actResult)
						}
					}
				})
				if !result {
					return
				}
			}
		}
	}
}

func Generate_Data_For_Consistency(seriesCount, pointCount int) Writes {
	genWrites := make(Writes, 0, seriesCount)
	var index int64
	for i := 1; i <= seriesCount; i++ {
		writes := make([]string, 0, pointCount)
		for j := 1; j <= pointCount; j++ {
			writes = append(writes, fmt.Sprintf(`cpu,lisnId=lisnId-%d,server_az=az-%d,hostId=cn-north-%d,elb_name=public-%d actConn=%d,totalConn=%d,used=%s,vname="%s" %d`,
				i, i, i, i, RandInt(), RandInt(), RandBool(), RandStringRunes(10), mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()+index*1000000000))
			index++
		}
		genWrites = append(genWrites, &Write{data: strings.Join(writes, "\n")})
	}
	return genWrites
}

func notEqual(s1, s2 string) bool {
	for i := 0; i < len(s1)-6; i++ {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}
