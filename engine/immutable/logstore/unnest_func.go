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

package logstore

import (
	"fmt"
	"regexp"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

type UnnestMatch interface {
	Get([][]byte) [][]byte
}

func GetUnnestFunc(unnest *influxql.Unnest, schema record.Schemas) (UnnestMatch, error) {
	call := unnest.Expr.(*influxql.Call)
	switch call.Name {
	case "match_all":
		return NewRegexpMatchAll(unnest, schema), nil
	default:
		return nil, fmt.Errorf("unnest not support %s", call.Name)
	}
}

type RegexpMatchAll struct {
	tagIndex          int
	contentIndex      int
	tagOrContentIndex int //0: tag, 1: content
	fieldIndex        map[int]int
	schema            record.Schemas
	re                *regexp.Regexp
}

func NewRegexpMatchAll(unnest *influxql.Unnest, schema record.Schemas) UnnestMatch {
	call := unnest.Expr.(*influxql.Call)
	fieldName := unnest.Expr.(*influxql.Call).Args[1].(*influxql.VarRef).Val
	r := &RegexpMatchAll{
		schema:     schema,
		fieldIndex: make(map[int]int),
		re:         regexp.MustCompile(call.Args[0].(*influxql.VarRef).Val),
	}
	for k, v := range unnest.Aliases {
		for ks, vs := range schema {
			if v == vs.Name {
				r.fieldIndex[k] = ks
				break
			} else {
				r.fieldIndex[k] = -1
			}
		}
	}
	if fieldName == "tag" {
		r.tagOrContentIndex = 0
	} else if fieldName == "content" {
		r.tagOrContentIndex = 1
	}
	for k, v := range schema {
		if v.Name == "tag" {
			r.tagIndex = k
		} else if v.Name == "content" {
			r.contentIndex = k
		}
	}
	return r
}

func (r *RegexpMatchAll) Get(data [][]byte) [][]byte {
	result := make([][]byte, r.schema.Len())
	result[r.tagIndex] = data[0]
	result[r.contentIndex] = data[1]
	result[r.schema.Len()-1] = data[2]

	matchesAll := r.re.FindStringSubmatch(string(data[r.tagOrContentIndex]))
	if matchesAll == nil {
		return result
	}

	if len(matchesAll) != len(r.fieldIndex)+1 {
		return result
	}
	for i := 1; i < len(matchesAll); i++ {
		if r.fieldIndex[i-1] == -1 {
			continue
		}
		result[r.fieldIndex[i-1]] = []byte(matchesAll[i])
	}
	return result
}

type UnnestMatchAll struct {
	unnest *influxql.Unnest
	re     *regexp.Regexp
}

func NewUnnestMatchAll(unnest *influxql.Unnest) (*UnnestMatchAll, error) {
	unnestExpr, ok := unnest.Expr.(*influxql.Call)
	if !ok {
		return nil, nil
	}
	var err error
	reg, err := regexp.Compile(unnestExpr.Args[0].(*influxql.VarRef).Val)
	if err != nil {
		return nil, err
	}
	r := &UnnestMatchAll{
		unnest: unnest,
		re:     reg,
	}

	return r, nil
}

func (r *UnnestMatchAll) Get(s string) map[string]string {
	result := make(map[string]string)
	for _, v := range r.unnest.Aliases {
		result[v] = ""
	}
	matchesAll := r.re.FindStringSubmatch(s)
	if matchesAll == nil {
		return result
	}
	for k, v := range r.unnest.Aliases {
		result[v] = matchesAll[k+1]
	}
	return result
}

type UnnestOperator interface {
	Compute(rec *record.Record)
}

type MatchAllOperator struct {
	schemas   record.Schemas
	srcField  string
	srcIndex  int
	dstFields []string
	dstIndex  []int
	re        *regexp.Regexp
}

func NewMatchAllOperator(unnest *influxql.Unnest, schemas record.Schemas) UnnestOperator {
	call := unnest.Expr.(*influxql.Call)
	srcField := unnest.Expr.(*influxql.Call).Args[1].(*influxql.VarRef).Val
	r := &MatchAllOperator{
		schemas:   schemas,
		srcField:  srcField,
		srcIndex:  -1,
		dstFields: unnest.Aliases,
		dstIndex:  make([]int, len(unnest.Aliases)),
		re:        regexp.MustCompile(call.Args[0].(*influxql.VarRef).Val),
	}

	for i, field := range schemas {
		if field.Name == srcField {
			r.srcIndex = i
		}
	}

	for i, dstField := range r.dstFields {
		r.dstIndex[i] = -1
		for j, field := range schemas {
			if field.Name == dstField {
				r.dstIndex[i] = j
			}
		}
	}

	return r
}

func (r *MatchAllOperator) appendDstNil(rec *record.Record) {
	for _, i := range r.dstIndex {
		rec.ColVals[i].AppendStringNull()
	}
}

func (r *MatchAllOperator) reset(rec *record.Record) {
	for _, i := range r.dstIndex {
		rec.ColVals[i].Init()
	}
}

func (r *MatchAllOperator) Compute(rec *record.Record) {
	r.reset(rec)
	rowsNums := rec.RowNums()
	for i := 0; i < rowsNums; i++ {
		srcString, isNil := rec.ColVals[r.srcIndex].StringValueUnsafe(i)
		if isNil {
			r.appendDstNil(rec)
			continue
		}
		ret := r.re.FindStringSubmatch(srcString)
		if len(ret) != len(r.dstIndex)+1 {
			r.appendDstNil(rec)
			continue
		}
		for j, k := range r.dstIndex {
			rec.ColVals[k].AppendString(ret[j+1])
		}
	}
}

func GetUnnestFuncOperator(unnest *influxql.Unnest, schema record.Schemas) (UnnestOperator, error) {
	call := unnest.Expr.(*influxql.Call)
	switch call.Name {
	case "match_all":
		return NewMatchAllOperator(unnest, schema), nil
	default:
		return nil, fmt.Errorf("unnest not support %s", call.Name)
	}
}
