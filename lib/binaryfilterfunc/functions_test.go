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

package binaryfilterfunc

import (
	"testing"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestRotate(t *testing.T) {
	root := &influxql.BinaryExpr{
		Op: influxql.AND,
		RHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "f1"},
			Op:  influxql.EQ,
			RHS: &influxql.IntegerLiteral{Val: 2},
		},
		LHS: &influxql.BinaryExpr{
			Op: influxql.OR,
			RHS: &influxql.BinaryExpr{
				LHS: &influxql.VarRef{Val: "f2"},
				Op:  influxql.EQ,
				RHS: &influxql.IntegerLiteral{Val: 4},
			},
			LHS: &influxql.BinaryExpr{
				Op: influxql.AND,
				LHS: &influxql.BinaryExpr{
					Op: influxql.OR,
					LHS: &influxql.BinaryExpr{
						LHS: &influxql.VarRef{Val: "f1"},
						Op:  influxql.EQ,
						RHS: &influxql.IntegerLiteral{Val: 1},
					},
					RHS: &influxql.BinaryExpr{
						LHS: &influxql.VarRef{Val: "f2"},
						Op:  influxql.EQ,
						RHS: &influxql.IntegerLiteral{Val: 2},
					},
				},
				RHS: &influxql.BinaryExpr{
					LHS: &influxql.VarRef{Val: "f2"},
					Op:  influxql.EQ,
					RHS: &influxql.IntegerLiteral{Val: 2},
				},
			},
		},
	}
	expExpr := "f1 = 1 OR f2 = 2 AND f2 = 2 OR f2 = 4 AND f1 = 2"
	expParts := 3
	newRoot := MoveOrOpToRoot(root)
	if newRoot.String() != expExpr {
		t.Fatal()
	}
	splitRoots := SplitWithOrOperation(newRoot)
	if len(splitRoots) != expParts {
		t.Fatal()
	}

}

func TestRotateRewriteTimeCompareVal(t *testing.T) {
	root := &influxql.BinaryExpr{
		Op: influxql.AND,
		RHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.GTE,
			RHS: &influxql.StringLiteral{Val: "2023-06-19T00:00:00Z"},
		},
		LHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.LTE,
			RHS: &influxql.StringLiteral{Val: "2023-06-19T09:00:00Z"},
		},
	}
	RewriteTimeCompareVal(root)
	expected := &influxql.BinaryExpr{
		Op: influxql.AND,
		RHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.GTE,
			RHS: &influxql.IntegerLiteral{Val: 1687132800000000000},
		},
		LHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.LTE,
			RHS: &influxql.IntegerLiteral{Val: 1687165200000000000},
		},
	}
	assert.Equal(t, root, expected)
}
