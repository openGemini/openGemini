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

package handler

import (
	"regexp"
	"strings"
	"time"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func rewriteBinary(expr *influxql.BinaryExpr) {
	switch expr.RHS.(type) {
	case *influxql.IntegerLiteral, *influxql.NumberLiteral,
		*influxql.BooleanLiteral, *influxql.UnsignedLiteral:
		expr.RHS = &influxql.StringLiteral{Val: expr.RHS.String()}
	case *influxql.Wildcard:
		val, _ := regexp.Compile(".*")
		expr.RHS = &influxql.RegexLiteral{Val: val}

		if expr.Op == influxql.EQ {
			expr.Op = influxql.EQREGEX
		} else if expr.Op == influxql.NEQ {
			expr.Op = influxql.NEQREGEX
		}
	}
}

func parseTagKeyCondition(cond string) (influxql.Expr, influxql.TimeRange, error) {
	var expr influxql.Expr
	if cond == "" {
		return expr, influxql.TimeRange{}, nil
	}
	p := influxql.NewParser(strings.NewReader(cond))
	expr, err := p.ParseExpr()
	if err != nil {
		p.Release()
		return nil, influxql.TimeRange{}, err
	}
	p.Release()

	valuer := influxql.NowValuer{Now: time.Now()}
	e, tr, err := influxql.ConditionExpr(expr, &valuer)
	if err != nil {
		return e, tr, err
	}

	// defaults to influxql.Tag for all types
	influxql.WalkFunc(e, func(node influxql.Node) {
		switch ref := node.(type) {
		case *influxql.VarRef:
			ref.Type = influxql.Tag
		case *influxql.BinaryExpr:
			rewriteBinary(ref)
		}
	})
	return e, tr, nil
}
