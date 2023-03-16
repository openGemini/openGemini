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

package influxql

import (
	"strconv"
	"strings"
	"time"
)

type YyParser struct {
	Query   Query
	Scanner *Scanner
	error   YyParserError
}

type YyParserError string

func (p YyParserError) Error() string {
	return string(p)
}

func NewYyParser(s *Scanner) YyParser {
	return YyParser{Scanner: s}
}

func (p *YyParser) ParseTokens() {
	yyParse(p)
}

func (p *YyParser) SetScanner(s *Scanner) {
	p.Scanner = s
}
func (p *YyParser) GetQuery() (*Query, error) {
	if len(p.error) > 0 {
		return &p.Query, p.error
	}
	return &p.Query, nil
}

func (p *YyParser) Lex(lval *yySymType) int {
	var typ Token
	var val string

	for {
		typ, _, val = p.Scanner.Scan()
		switch typ {
		case ILLEGAL:
			p.Error("unexpected " + string(val) + ", it's ILLEGAL")
		case EOF:
			return 0
		case MUL:
			{
				lval.int = int(ILLEGAL)
			}
		case NUMBER:
			{
				lval.float64, _ = strconv.ParseFloat(val, 64)
			}
		case INTEGER:
			{
				lval.int64, _ = strconv.ParseInt(val, 10, 64)
			}
		case DURATIONVAL:
			{
				time, err := ParseDuration(val)
				if err == nil {
					lval.tdur = time
				} else {
					p.Error("invalid duration")
				}
			}
		case DESC:
			{
				lval.bool = false
			}
		case AND:
			{
				lval.int = int(AND)
			}
		case OR:
			{
				lval.int = int(OR)
			}
		case ASC:
			{
				lval.bool = true
			}
		case HINT:
			{
				hitLit := val[1:]
				hitLit = RemoveExtraSpace(hitLit)
				hitLits := strings.Split(hitLit, " ")

				var hints Hints
				for _, l := range hitLits {
					support, ok := SupportHit[l]
					if support && ok {
						val := &StringLiteral{Val: l}
						hints = append(hints, &Hint{Expr: val})
					}
				}
				lval.hints = hints
			}
		}
		if typ >= EQ && typ <= GTE {
			lval.int = int(typ)
		}
		if typ != WS {
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
	Dimensions   Dimensions
	TimeInterval *DurationLiteral
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

type cqSamplePolicyInfo struct {
	ResampleEvery time.Duration
	ResampleFor   time.Duration
}
type fieldList struct {
	fieldName  string
	fieldType  string
	tagOrField string
}
