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

package rpn_test

import (
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestConvertToRPNExpr(t *testing.T) {
	condition := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.AND,
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "direction"}, RHS: &influxql.StringLiteral{Val: "out"}},
			LHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "campus"}, RHS: &influxql.StringLiteral{Val: "广州1"}}},
		RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "net_export_name"}, RHS: &influxql.StringLiteral{Val: "华南-广州_PNI_广州移动"}},
	}
	rpnExpr := rpn.ConvertToRPNExpr(condition)

	var b = &strings.Builder{}
	for _, v := range rpnExpr.Val {
		switch v := v.(type) {
		case *influxql.VarRef:
			b.WriteString(v.String())

		case *influxql.StringLiteral:
			b.WriteString(v.String())
		case *influxql.IntegerLiteral:
			b.WriteString(v.String())
		case *influxql.NumberLiteral:
			b.WriteString(v.String())
		case *influxql.BooleanLiteral:
			b.WriteString(v.String())
		case influxql.Token:
			b.WriteString(v.String() + " | ")
		default:
		}
		b.WriteString(" ")
	}
	expected := "campus '广州1' = |  direction 'out' = |  AND |  net_export_name '华南-广州_PNI_广州移动' = |  AND |  "
	assert.Equal(t, expected, b.String())
}
