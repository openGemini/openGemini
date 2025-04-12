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
package tokenizer

import (
	"strings"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type TokenFinder interface {
	Next() bool
	CurrentOffset() int
}

type SimpleTokenFinder struct {
	splitTable []byte
	content    []byte
	token      []byte
	fromIndex  int
	end        int
	offset     int
}

func NewSimpleTokenFinder(split []byte) *SimpleTokenFinder {
	return &SimpleTokenFinder{splitTable: split, content: nil, token: nil, fromIndex: 0, end: 0, offset: -1}
}

func (t *SimpleTokenFinder) InitInput(content []byte, token []byte) {
	t.content = content
	t.token = token
	t.fromIndex = 0
	t.end = len(content)
	t.offset = -1
}

func (t *SimpleTokenFinder) Next() bool {
	for {
		if len(t.token) == 0 && len(t.content) == 0 {
			return true
		}
		found := indexOf(t.content, t.token, t.fromIndex)
		if found < 0 {
			return false
		}

		t.fromIndex = found + len(t.token)
		first := found
		last := found + len(t.token) - 1
		pre := first - 1
		post := last + 1
		preValid := pre < 0 || t.isSplit(pre) || t.isSplit(first)
		postValid := post >= t.end || t.isSplit(post) || t.isSplit(last)
		if preValid && postValid {
			t.offset = found
			break
		}
	}
	return true
}

func (t *SimpleTokenFinder) CurrentOffset() int {
	return t.offset
}

func (t *SimpleTokenFinder) isSplit(pos int) bool {
	b := int8(t.content[pos])
	return b < 0 || t.splitTable[b] > 0
}

func indexOf(source []byte, target []byte, from int) int {
	sourceCount := len(source)
	targetCount := len(target)

	if from >= sourceCount {
		if targetCount == 0 {
			return sourceCount
		} else {
			return -1
		}
	}
	if from < 0 {
		from = 0
	}
	if targetCount == 0 {
		return -1
	}

	max := sourceCount - targetCount
	i := from
	for i <= max {
		if source[i] != target[0] {
			i++
			for i <= max && source[i] != target[0] {
				i++
			}
		}

		if i <= max {
			j := i + 1
			k := 1
			end := j + targetCount - 1
			for j < end && source[j] == target[k] {
				j++
				k++
			}
			if j == end {
				return i
			}
		}
		i++
	}
	return -1
}

type TokenFilter struct {
	schemas    record.Schemas
	expr       influxql.Expr
	tokenizers map[string]*SimpleTokenFinder
}

func NewTokenFilter(schemas record.Schemas, expr influxql.Expr, split map[string][]byte) *TokenFilter {
	t := &TokenFilter{
		schemas:    schemas,
		expr:       expr,
		tokenizers: make(map[string]*SimpleTokenFinder),
	}
	for k, v := range split {
		t.tokenizers[k] = NewSimpleTokenFinder(v)
	}

	return t
}

func (t *TokenFilter) Filter(words [][]byte) bool {
	return t.FilterRowByField(words, t.expr)
}

func (t *TokenFilter) FilterRowByField(words [][]byte, expr influxql.Expr) bool {
	switch n := expr.(type) {
	case *influxql.ParenExpr:
		return t.FilterRowByField(words, n.Expr)
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.AND:
			return t.FilterRowByField(words, n.LHS) && t.FilterRowByField(words, n.RHS)
		case influxql.OR:
			return t.FilterRowByField(words, n.LHS) || t.FilterRowByField(words, n.RHS)
		case influxql.EQ:
			for i := 0; i < t.schemas.Len(); i++ {
				if n.LHS.(*influxql.VarRef).Val == t.schemas[i].Name {
					if n.RHS.(*influxql.StringLiteral).Val == "" {
						return len(words[i]) == 0
					} else {
						colName := t.schemas[i].Name
						if tokenizer, ok := t.tokenizers[colName]; ok {
							tokenizer.InitInput(words[i], []byte(n.RHS.(*influxql.StringLiteral).Val))
							return tokenizer.Next()
						} else {
							return strings.Contains(string(words[i]), n.RHS.(*influxql.StringLiteral).Val)
						}
					}
				}
			}
			return false
		}
	default:
		return false
	}
	return false
}
