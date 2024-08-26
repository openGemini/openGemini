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

package coordinator

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

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

func getExpResults() netstorage.TableTagKeys {
	return netstorage.TableTagKeys{
		netstorage.TagKeys{
			Name: "mst",
			Keys: []string{"tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tk1", "tk10", "tk2", "tk3"},
		},
	}
}

func TestShowTagKeysExecutor(t *testing.T) {
	e := NewShowTagKeysExecutor(logger.NewLogger(errno.ModuleUnknown),
		&mockMC{}, &mockME{}, &mockNS{})
	smt := &influxql.ShowTagKeysStatement{
		Database: "db0",
		Sources:  append(influxql.Sources{}, &influxql.Measurement{}),
	}
	cond, _, er := parseTagKeyCondition("tag1 = 'a1'")
	assert.NoError(t, er)

	smt.Condition = cond

	rows, err := e.Execute(smt)
	assert.NoError(t, err)
	assert.Equal(t, rows, getExpResults())
}
