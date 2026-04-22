// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package influxql_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

type DepthTestCase struct {
	name, prefix, left, middle, right, suffix string
}

var simpleCases []DepthTestCase
var simpleSteps []int
var complexCases []DepthTestCase
var complexSteps []int

var enormousCase = DepthTestCase{
	"DeepExprLeft", "select value", "+1", "", "", " from test..sensor"}

func init() {
	simpleSteps = []int{0, 1000, 1000, 3000, 5000, 10000} // for 50k add 30000; for 100k, add: 50000
	simpleCases = []DepthTestCase{
		{"DeepBracketsExpr", "select value+", "(", "1", ")", " from test..sensor"},
		{"DeepExprBrLeft", "select ", "(", "value", "+1)", " from test..sensor"},
		{"DeepExprBrRight", "select ", "(1+", "value", ")", " from test..sensor"},
		{"DeepExprLeft", "select value", "+1", "", "", " from test..sensor"},
		{"DeepExprRight", "select ", "1+", "value", "", " from test..sensor"},
		{"DeepExprMix", "select value", "+1*1", "", "", " from test..sensor"},
		{"Column", "select value from test..sensor where value in (", "1,", "", "", "1)"},
		{"CaseWhen", "select case when value > 1 then 1 ", "when value > 1 then 1 ", "else 10 ", "", "end from test..sensor"},
		{"IdentsIn", "select value from test..sensor where value in (", "value,", "value)", "", ""},
		{"IdentsNotIn", "select value from test..sensor where value not in (", "value,", "value)", "", ""},
		{"Dimensions", "select value from test..sensor group by ", "value,", "value", "", ""},
		{"Sortfields", "select value from test..sensor order by ", "value,", "value", "", ""},
		{"Indexlist", "create measurement test..XX (i int64 field) WITH INDEXTYPE field INDEXLIST ", "i,", "i", "", ""},
		{"IndextypeCS", "create measurement test..XX (i int64 field) WITH ENGINETYPE=COLUMNSTORE INDEXTYPE ", "minmax INDEXLIST i ", "", "", ""},
		{"IndextypeTS", "create measurement test..XX (i int64 field) WITH INDEXTYPE ", "field INDEXLIST i ", "", "", ""},
		{"PropCS", "create measurement test..XX (i int64 field) WITH ENGINETYPE=COLUMNSTORE PROPERTY ", "x=y,", "x=y", "", ""},
		{"DownsampleSample", "create downsample (avg()) WITH duration 1m sampleinterval (1m", ",1m", "", "", ") timeinterval (1m)"},
		{"DownsampleTime", "create downsample (avg()) WITH duration 1m sampleinterval (1m) timeinterval(1m", ",1m", "", "", ")"},
		{"Alldest", "create subscription xxx on xxx destinations any ", "x,", "x", "", ""},
		{"Withsel", "with ", "xx as (select 1), ", "xx as (select 1)", "", "select value from test..sensor"},
		{"FromList", "select value from ", "test..sensor, ", "test..sensor", "", ""},
		{"FromSubqueryList", "select value from ", "(select value from test..sensor), ", "test..sensor", "", ""},
		{"FromDeepParent", "select value from (", "(", "select value from test..sensor", ")", ")"},
	}
	complexSteps = []int{0, 1000, 1000, 3000, 5000}
	complexCases = []DepthTestCase{
		{"FromDeepSubquery", "select value from ", "(select * from ", "a..a", ")", ""},
		{"FromJoinSubquery", "select value from ", "(select * from ", "a..a", " full join a..a on (a=a))", ""},
		{"FromJoinChained", "select value from a..a ", "left outer join a..a on (a=a) ", "", "", ""},
	}
}

func (c *DepthTestCase) GetSql(size int) string {
	return strings.Join([]string{c.prefix, strings.Repeat(c.left, size), c.middle, strings.Repeat(c.right, size), c.suffix}, "")
}

func DepthTester(N int, test string) error {
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
	}

	for i := 0; i < N; i++ {
		YyParser.Query = influxql.Query{}
		YyParser.Scanner = influxql.NewScanner(strings.NewReader(test))
		YyParser.ParseTokens()
		_, err := YyParser.GetQuery()
		if err != nil {
			return err
		}
	}
	return nil
}

func TestCalcDepth(t *testing.T) {
	sizes := []int{0, 10}
	for _, cs := range slices.Concat(simpleCases, complexCases) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%s-%d", cs.name, size), func(t *testing.T) {
				test := cs.GetSql(size)

				YyParser1 := &influxql.YyParser{
					Query:   influxql.Query{},
					Scanner: influxql.NewScanner(strings.NewReader(test)),
				}
				YyParser1.ParseTokens()
				q1, err := YyParser1.GetQuery()
				if err != nil {
					t.Error(err.Error())
				}

				YyParser2 := &influxql.YyParser{
					Query:   influxql.Query{},
					Scanner: influxql.NewScanner(strings.NewReader(test)),
				}
				YyParser2.ParseTokens()
				q2, err := YyParser2.GetQuery()
				if err != nil {
					t.Error(err.Error())
				}
				q2.UpdateDepthForTests()

				assert.Equal(t, q2, q1)
				if !reflect.DeepEqual(q1, q2) {
					t.Errorf("Updated Depth is not equal for sql: %s", test)

					fmt.Printf("===Current:[%d]%v\n", q1.Depth(), q1)
					influxql.WalkFunc(q1, func(n influxql.Node) { fmt.Printf("  %#v\n", n) })
					fmt.Printf("===Correct:[%d]%v\n", q2.Depth(), q2)
					influxql.WalkFunc(q2, func(n influxql.Node) { fmt.Printf("  %#v\n", n) })
				}
			})
		}
	}
}

func TestDepth(t *testing.T) {
	DoTestDepth(t, simpleCases, simpleSteps)
	DoTestDepth(t, complexCases, complexSteps)
}

func DoTestDepth(t *testing.T, testCases []DepthTestCase, testSteps []int) {
	for _, cs := range testCases {
		size := 0
		for _, step := range testSteps {
			size += step
			t.Run(fmt.Sprintf("%s-%d", cs.name, size), func(t *testing.T) {
				test := cs.GetSql(size)
				err := DepthTester(1, test)
				if err != nil {
					t.Errorf("%s", err.Error())
				}
			})
		}
	}
}

func TestDepthFail(t *testing.T) {
	for _, cs := range slices.Concat(simpleCases, complexCases) {
		size := 100000
		t.Run(fmt.Sprintf("%s-%d", cs.name, size), func(t *testing.T) {
			test := cs.GetSql(size)
			err := DepthTester(1, test)
			if err == nil {
				t.Errorf("Should fail due to complexity or stack limit")
			}
		})
	}
}

func TestDepthEnormous(t *testing.T) {
	cs := enormousCase
	size := 10_000_000
	t.Run(fmt.Sprintf("%s-%d", cs.name, size), func(t *testing.T) {
		test := cs.GetSql(size)
		err := DepthTester(1, test)
		if err == nil {
			t.Errorf("Should fail due to complexity or stack limit")
		}
	})
}

func BenchmarkDepth(b *testing.B) {
	DoBenchmarkDepth(b, simpleCases, simpleSteps)
	DoBenchmarkDepth(b, complexCases, complexSteps)
}

func DoBenchmarkDepth(b *testing.B, testCases []DepthTestCase, testSteps []int) {
	for _, cs := range testCases {
		size := 0
		for _, step := range testSteps {
			size += step
			b.Run(fmt.Sprintf("%s-%d", cs.name, size), func(b *testing.B) {
				test := cs.GetSql(size)
				b.ResetTimer()
				err := DepthTester(b.N, test)
				if err != nil {
					b.Errorf("%s", err.Error())
				}
			})
		}
	}
}
