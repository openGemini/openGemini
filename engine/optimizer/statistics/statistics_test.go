// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package statistics

import (
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

// TestEstimateSelectivity tests the EstimateSelectivity method with various expressions.
func TestEstimateSelectivity(t *testing.T) {
	h := &NumericHistogram{
		Name: "time",
		Bins: []NumericBin{
			{Min: 0, Max: 10, Count: 5, Density: 0.5},
			{Min: 10, Max: 20, Count: 5, Density: 0.5},
		},
		TotalCount: 10,
	}
	emptyHis := &NumericHistogram{Name: "time"}
	tests := []struct {
		name     string
		expr     influxql.Expr
		his      Histogram
		expected float64
	}{
		{
			name: "LT condition within bin",
			expr: &influxql.BinaryExpr{
				Op: influxql.LT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 5},
			},
			his:      h,
			expected: 0.25,
		},
		{
			name: "LT condition out of bin",
			expr: &influxql.BinaryExpr{
				Op: influxql.LT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 25},
			},
			his:      h,
			expected: 1.0,
		},
		{
			name: "LT condition with unexpected VarRef",
			expr: &influxql.BinaryExpr{
				Op:  influxql.LT,
				LHS: &influxql.NumberLiteral{Val: 5},
				RHS: &influxql.VarRef{
					Val: "time",
				},
			},
			his:      h,
			expected: DefaultSelectivity,
		},
		{
			name: "LT condition with unexpected NumberLiteral",
			expr: &influxql.BinaryExpr{
				Op: influxql.LT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.StringLiteral{},
			},
			his:      h,
			expected: DefaultSelectivity,
		},
		{
			name: "LT condition with empty histogram",
			expr: &influxql.BinaryExpr{
				Op: influxql.LT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 5},
			},
			his:      emptyHis,
			expected: DefaultSelectivity,
		},
		{
			name: "LTE condition within bin",
			expr: &influxql.BinaryExpr{
				Op: influxql.LTE,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 10},
			},
			his:      h,
			expected: 0.5,
		},
		{
			name: "GT condition within bin",
			expr: &influxql.BinaryExpr{
				Op: influxql.GT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 15},
			},
			his:      h,
			expected: 0.25,
		},
		{
			name: "GT condition out of bin",
			expr: &influxql.BinaryExpr{
				Op: influxql.GT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 25},
			},
			his:      h,
			expected: 0.0,
		},
		{
			name: "GT condition with unexpected VarRef",
			expr: &influxql.BinaryExpr{
				Op:  influxql.GT,
				LHS: &influxql.NumberLiteral{Val: 15},
				RHS: &influxql.VarRef{
					Val: "time",
				},
			},
			his:      h,
			expected: DefaultSelectivity,
		},
		{
			name: "GT condition with unexpected NumberLiteral",
			expr: &influxql.BinaryExpr{
				Op: influxql.GT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.StringLiteral{},
			},
			his:      h,
			expected: DefaultSelectivity,
		},
		{
			name: "GT condition with empty histogram",
			expr: &influxql.BinaryExpr{
				Op: influxql.GT,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 10},
			},
			his:      emptyHis,
			expected: DefaultSelectivity,
		},
		{
			name: "GTE condition within bin",
			expr: &influxql.BinaryExpr{
				Op: influxql.GTE,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 10},
			},
			his:      h,
			expected: 0.5,
		},
		{
			name:     "Invalid expression type",
			expr:     &influxql.VarRef{Val: "time"},
			his:      h,
			expected: DefaultSelectivity,
		},
		{
			name: "Unsupported expression op",
			expr: &influxql.BinaryExpr{
				Op: influxql.EQ,
				LHS: &influxql.VarRef{
					Val: "time",
				},
				RHS: &influxql.NumberLiteral{Val: 10},
			},
			his:      h,
			expected: DefaultSelectivity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.his.EstimateSelectivity(tt.expr)
			if result != tt.expected {
				t.Errorf("EstimateSelectivity() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestUpdate tests the Update method with various values.
func TestUpdate(t *testing.T) {
	h := &NumericHistogram{
		Name: "time",
		Bins: []NumericBin{
			{Min: 0, Max: 10, Count: 0, Density: 0},
			{Min: 10, Max: 20, Count: 0, Density: 0},
		},
		TotalCount: 0,
	}

	values := []interface{}{
		float64(5),
		float64(15),
		"invalid", // non-float64 value
	}

	h.Update(values...)

	if h.Bins[0].Count != 1 || h.Bins[1].Count != 1 || h.TotalCount != 2 {
		t.Errorf("Update() resulted in incorrect counts: %v, %v, %v", h.Bins[0].Count, h.Bins[1].Count, h.TotalCount)
	}
	h = &NumericHistogram{}
	h.Update([]int64{0})
	assert.Equal(t, h.TotalCount, uint64(0))
}

// TestSerializeDeserialize tests the Serialize and Deserialize methods.
func TestSerializeDeserialize(t *testing.T) {
	h := &NumericHistogram{
		Name: "time",
		Bins: []NumericBin{
			{Min: 0, Max: 10, Count: 5, Density: 0.5},
			{Min: 10, Max: 20, Count: 5, Density: 0.5},
		},
		TotalCount: 10,
	}

	data, err := h.Serialize()
	if err != nil {
		t.Fatalf("Serialize() failed: %v", err)
	}

	newH := &NumericHistogram{}
	err = newH.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize() failed: %v", err)
	}

	if newH.Name != h.Name || newH.TotalCount != h.TotalCount || len(newH.Bins) != len(h.Bins) {
		t.Errorf("Deserialize() resulted in incorrect histogram: %v", newH)
	}
}
