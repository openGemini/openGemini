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

package yacc

import (
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

type YyParser struct {
	Query   influxql.Query
	Scanner *influxql.Scanner
	error   YyParserError
}

type YyParserError string

func (p YyParserError) Error() string {
	return string(p)
}

func NewYyParser(s *influxql.Scanner) YyParser {
	return YyParser{Scanner: s}
}

func (p *YyParser) ParseTokens() {
	yyParse(p)
}

func (p *YyParser) SetScanner(s *influxql.Scanner) {
	p.Scanner = s
}
func (p *YyParser) GetQuery() (*influxql.Query, error) {
	if len(p.error) > 0 {
		return &p.Query, p.error
	}
	return &p.Query, nil
}

func (p *YyParser) Lex(lval *yySymType) int {
	var typ influxql.Token
	var val string

	for {
		typ, _, val = p.Scanner.Scan()
		switch typ {
		case influxql.ILLEGAL:
			p.Error("unexpected " + string(val) + ", it's ILLEGAL")
		case influxql.EOF:
			return 0
		case influxql.MUL:
			{
				lval.int = int(influxql.ILLEGAL)
			}
		case influxql.NUMBER:
			{
				lval.float64, _ = strconv.ParseFloat(val, 64)
			}
		case influxql.INTEGER:
			{
				lval.int64, _ = strconv.ParseInt(val, 10, 64)
			}
		case influxql.DURATIONVAL:
			{
				time, err := influxql.ParseDuration(val)
				if err == nil {
					lval.tdur = time
				} else {
					p.Error("invalid duration")
				}
			}
		case influxql.DESC:
			{
				lval.bool = false
			}
		case influxql.AND:
			{
				lval.int = int(influxql.AND)
			}
		case influxql.OR:
			{
				lval.int = int(influxql.OR)
			}
		case influxql.ASC:
			{
				lval.bool = true
			}
		case influxql.HINT:
			{
				hitLit := val[1:]
				hitLit = influxql.RemoveExtraSpace(hitLit)
				hitLits := strings.Split(hitLit, " ")

				var hints influxql.Hints
				for _, l := range hitLits {
					support, ok := influxql.SupportHit[l]
					if support && ok {
						val := &influxql.StringLiteral{Val: l}
						hints = append(hints, &influxql.Hint{Expr: val})
					}
				}
				lval.hints = hints
			}
		}
		if typ >= influxql.EQ && typ <= influxql.GTE {
			lval.int = int(typ)
		}
		if typ != influxql.WS {
			break
		}
	}
	lval.str = val
	return int(typ)
}
func (p *YyParser) Error(err string) {
	p.error = YyParserError(err)
}

type GroupByCondition struct {
	Dimensions   influxql.Dimensions
	TimeInterval *influxql.DurationLiteral
}

type Durations struct {
	ShardGroupDuration time.Duration
	HotDuration        time.Duration
	WarmDuration       time.Duration
	IndexGroupDuration time.Duration

	PolicyDuration *time.Duration
	Replication    *int
	PolicyName     string
	ReplicaNum     uint32
	rpdefault      bool
	ShardKey       []string
}

type IndexType struct {
	types []string
	lists [][]string
}
