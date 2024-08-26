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

package immutable

import (
	"fmt"
	"regexp"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

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
	call, ok := unnest.Expr.(*influxql.Call)
	if !ok {
		panic(fmt.Errorf("the type of %s is not *influxql.Call", unnest.Expr))
	}
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
	call, ok := unnest.Expr.(*influxql.Call)
	if !ok {
		panic(fmt.Errorf("the type of %s is not *influxql.Call", unnest.Expr))
	}
	switch call.Name {
	case "match_all":
		return NewMatchAllOperator(unnest, schema), nil
	default:
		return nil, fmt.Errorf("unnest not support %s", call.Name)
	}
}
