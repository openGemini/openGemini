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

package logparser

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type logTest struct {
	log  string
	expr influxql.BinaryExpr
}

func TestLogParserForSpecialType(t *testing.T) {
	parser := &YyParser{Query: influxql.Query{}}
	testLogs := []logTest{
		{
			log: "127.0.0.10",
			expr: influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "content"},
				RHS: &influxql.StringLiteral{Val: "127.0.0.10"},
			},
		},
		{
			log: "2023-06-13",
			expr: influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "content"},
				RHS: &influxql.StringLiteral{Val: "2023-06-13"},
			},
		},
		{
			log: "10:00:00",
			expr: influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "content"},
				RHS: &influxql.StringLiteral{Val: "10:00:00"},
			},
		},
	}

	for i, testLog := range testLogs {
		parser.Scanner = NewScanner(strings.NewReader(testLog.log))
		parser.ParseTokens()
		q, err := parser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "parse %d with sql: %s, fai", i, q.String())
			break
		}
		condStatement, ok := q.Statements[i].(*influxql.LogPipeStatement)
		if !ok {
			t.Fatal()
		}
		if !reflect.DeepEqual(&testLog.expr, condStatement.Cond) {
			t.Fatalf("[%s] result err, expect:%+v, real: %+v", testLog.log, testLog.expr, condStatement.Cond)
		}
		fmt.Println(q.Statements[i].String())
	}
}

func TestLogParserForFieldAndType(t *testing.T) {
	parser := &YyParser{Query: influxql.Query{}}
	testLogs := []logTest{
		{
			log: "host: 127.0.0.10",
			expr: influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "host"},
				RHS: &influxql.StringLiteral{Val: "127.0.0.10"},
			},
		},

		{
			log: "date: 2023-06-13",
			expr: influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "date"},
				RHS: &influxql.StringLiteral{Val: "2023-06-13"},
			},
		},
		{
			log: "path:/var/log/messages/",
			expr: influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "path"},
				RHS: &influxql.StringLiteral{Val: "/var/log/messages/"},
			},
		},
	}

	for i, testLog := range testLogs {
		parser.Scanner = NewScanner(strings.NewReader(testLog.log))
		parser.ParseTokens()
		q, err := parser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "parse %d with sql: %s, fai", i, q.String())
			break
		}
		condStatement, ok := q.Statements[i].(*influxql.LogPipeStatement)
		if !ok {
			t.Fatal()
		}
		if !reflect.DeepEqual(&testLog.expr, condStatement.Cond) {
			t.Fatalf("[%s] result err, expect:%+v, real: %+v", testLog.log, testLog.expr, condStatement.Cond)
		}
		fmt.Println(q.Statements[i].String())
	}
}

type logTermTest struct {
	log    string
	expect string
}

func TestLogParserForMultiSpecialType(t *testing.T) {
	parser := &YyParser{Query: influxql.Query{}}
	testLogs := []logTermTest{
		{
			log:    "(2023-06-13 OR 127.0.0.10) AND time:10:00:00",
			expect: "(content MATCHPHRASE '2023-06-13' OR content MATCHPHRASE '127.0.0.10') AND time MATCHPHRASE '10:00:00'",
		},
		{
			log:    "time AND host:127.0.0.10 AND host:127.0.0.11",
			expect: "content MATCHPHRASE 'time' AND host MATCHPHRASE '127.0.0.10' AND host MATCHPHRASE '127.0.0.11'",
		},
		{
			log:    "(/var/log/messages OR path:/var/log/messages) AND ip:127.0.0.10",
			expect: "(content MATCHPHRASE '/var/log/messages' OR path MATCHPHRASE '/var/log/messages') AND ip MATCHPHRASE '127.0.0.10'",
		},
		{
			log:    "123456789",
			expect: "content MATCHPHRASE '123456789'",
		},
		{
			log:    "request:123456789",
			expect: "request MATCHPHRASE '123456789'",
		},
		{
			log:    "get iamges or request:process",
			expect: "content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges' OR request MATCHPHRASE 'process'",
		},
	}

	for i, testLog := range testLogs {
		parser.Scanner = NewScanner(strings.NewReader(testLog.log))
		parser.ParseTokens()
		q, err := parser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "parse %d with sql: %s, fai", i, q.String())
			break
		}
		_, ok := q.Statements[i].(*influxql.LogPipeStatement)
		if !ok {
			t.Fatal()
		}
		get := q.Statements[i].String()
		if testLog.expect != get {
			t.Fatalf("[%s] result err, \nexpect:%s, \nreal: %s", testLog.log, testLog.expect, get)
		}
		fmt.Println(testLog.log, " : ", q.Statements[i].String())
	}
}

func TestLogParserForMultiTerm(t *testing.T) {
	parser := &YyParser{Query: influxql.Query{}}
	testLogs := []logTermTest{
		{
			log:    "get iamges and process",
			expect: "content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges' AND content MATCHPHRASE 'process'",
		},
		{
			log:    "iamges OR simulating process",
			expect: "content MATCHPHRASE 'iamges' OR content MATCHPHRASE 'simulating' AND content MATCHPHRASE 'process'",
		},
		{
			log:    "get iamges or request:process",
			expect: "content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges' OR request MATCHPHRASE 'process'",
		},
		{
			log:    "\"get iamges\" or request:process",
			expect: "content MATCHPHRASE 'get iamges' OR request MATCHPHRASE 'process'",
		},
		{
			log:    "request:process AND \"get iamges\"",
			expect: "request MATCHPHRASE 'process' AND content MATCHPHRASE 'get iamges'",
		},
		{
			log:    "request:process OR get iamges",
			expect: "request MATCHPHRASE 'process' OR content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges'",
		},
		{
			log:    "request:process OR request:iamges",
			expect: "request MATCHPHRASE 'process' OR request MATCHPHRASE 'iamges'",
		},
		{
			log:    "get iamges \"HTTP 1.0\"",
			expect: "content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges' AND content MATCHPHRASE 'HTTP 1.0'",
		},
		{
			log:    "request:simulating process OR get iamges",
			expect: "request MATCHPHRASE 'simulating' AND content MATCHPHRASE 'process' OR content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges'",
		},
	}

	for i, testLog := range testLogs {
		parser.Scanner = NewScanner(strings.NewReader(testLog.log))
		parser.ParseTokens()
		q, err := parser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "parse %d with sql: %s, fai", i, q.String())
			break
		}
		_, ok := q.Statements[i].(*influxql.LogPipeStatement)
		if !ok {
			t.Fatal()
		}
		get := q.Statements[i].String()
		if testLog.expect != get {
			t.Fatalf("[%s] result err, \nexpect:%s, \nreal: %s", testLog.log, testLog.expect, get)
		}
		fmt.Println(testLog.log, " : ", q.Statements[i].String())
	}
}

func TestLogParserForExtract(t *testing.T) {
	parser := &YyParser{Query: influxql.Query{}}
	testLogs := []logTermTest{
		{
			log:    "get iamges|EXTRACT(tags:\"([a-z]+):([a-z]+)\") AS(key1,   value1)|key1:http",
			expect: "content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges' AND key1 MATCHPHRASE 'http'|UNNEST(match_all(\"([a-z]+):([a-z]+)\", tags)) AS(key1, value1)",
		},
		{
			log:    "get iamges|EXTRACT(\"([a-z]+):([a-z]+)\") AS(key1,   value1)|key1:http",
			expect: "content MATCHPHRASE 'get' AND content MATCHPHRASE 'iamges' AND key1 MATCHPHRASE 'http'|UNNEST(match_all(\"([a-z]+):([a-z]+)\", content)) AS(key1, value1)",
		},
		{
			log:    "get|EXTRACT(\"([a-z]+)\") AS(key1)",
			expect: "content MATCHPHRASE 'get'|UNNEST(match_all(\"([a-z]+)\", content)) AS(key1)",
		},
		{
			log:    "*|EXTRACT(\"([a-z]+)\") AS(key1)",
			expect: "content != ''|UNNEST(match_all(\"([a-z]+)\", content)) AS(key1)",
		},
		{
			log:    "EXTRACT(\"([a-z]+)\") AS(key1)",
			expect: "|UNNEST(match_all(\"([a-z]+)\", content)) AS(key1)",
		},
	}

	for i, testLog := range testLogs {
		parser.Scanner = NewScanner(strings.NewReader(testLog.log))
		parser.ParseTokens()
		q, err := parser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "parse %d with sql: %s, fai", i, q.String())
			break
		}
		_, ok := q.Statements[i].(*influxql.LogPipeStatement)
		if !ok {
			t.Fatal()
		}
		get := q.Statements[i].String()
		if testLog.expect != get {
			t.Fatalf("[%s] result err, \nexpect:%s, \nreal: %s", testLog.log, testLog.expect, get)
		}
		fmt.Println(testLog.log, " : ", q.Statements[i].String())
	}
}

func TestLogParserForWildCard(t *testing.T) {
	parser := &YyParser{Query: influxql.Query{}}
	testLogs := []logTermTest{
		{
			log:    "content: *",
			expect: "content != ''",
		},
	}

	for i, testLog := range testLogs {
		parser.Scanner = NewScanner(strings.NewReader(testLog.log))
		parser.ParseTokens()
		q, err := parser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "parse %d with sql: %s, fai", i, q.String())
			break
		}
		_, ok := q.Statements[i].(*influxql.LogPipeStatement)
		if !ok {
			t.Fatal()
		}
		get := q.Statements[i].String()
		if testLog.expect != get {
			t.Fatalf("[%s] result err, \nexpect:%s, \nreal: %s", testLog.log, testLog.expect, get)
		}
		fmt.Println(testLog.log, " : ", q.Statements[i].String())
	}
}

func TestLogParserForRangeExpr(t *testing.T) {
	parser := &YyParser{Query: influxql.Query{}}
	testLogs := []logTermTest{
		{
			log:    "field in (10 100)",
			expect: "\"field\" > '10' AND \"field\" < '100'",
		},
		{
			log:    "field in (10 100]",
			expect: "\"field\" > '10' AND \"field\" <= '100'",
		},
		{
			log:    "field in [10 100)",
			expect: "\"field\" >= '10' AND \"field\" < '100'",
		},
		{
			log:    "field in [10 100]",
			expect: "\"field\" >= '10' AND \"field\" <= '100'",
		},
		{
			log:    "field in [10 100] and a<100",
			expect: "\"field\" >= '10' AND \"field\" <= '100' AND a < '100'",
		},
	}

	for i, testLog := range testLogs {
		parser.Scanner = NewScanner(strings.NewReader(testLog.log))
		parser.ParseTokens()
		q, err := parser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "parse %d with sql: %s, fai", i, q.String())
			break
		}
		_, ok := q.Statements[i].(*influxql.LogPipeStatement)
		if !ok {
			t.Fatal()
		}
		get := q.Statements[i].String()
		if testLog.expect != get {
			t.Fatalf("[%s] result err, \nexpect:%s, \nreal: %s", testLog.log, testLog.expect, get)
		}
		fmt.Println(testLog.log, " : ", q.Statements[i].String())
	}
}
