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

package query

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/require"
)

func TestIsOnlyCountTime(t *testing.T) {
	// Test case 1: All conditions are met, should return true
	test1 := &ProcessorOptions{
		Exprs: []influxql.Expr{countTimeCall()},
	}
	require.True(t, test1.IsOnlyCountTime())

	// Test case 2: HintType is ExactStatisticQuery, should return false
	test2 := &ProcessorOptions{
		HintType: hybridqp.ExactStatisticQuery,
	}
	require.False(t, test2.IsOnlyCountTime())

	// Test case 3: No call, should return false
	test3 := &ProcessorOptions{}
	require.False(t, test3.IsOnlyCountTime())

	// Test case 4: Exprs length is not 1, should return false
	test4 := &ProcessorOptions{
		Exprs: []influxql.Expr{countTimeCall(), countTimeCall()},
	}
	require.False(t, test4.IsOnlyCountTime())

	// Test case 5: Call is not count(time), should return false
	test5 := &ProcessorOptions{
		Exprs: []influxql.Expr{countOtherCall()},
	}
	require.False(t, test5.IsOnlyCountTime())

	// Test case 6: Condition is not nil, should return false
	test6 := &ProcessorOptions{
		Exprs:     []influxql.Expr{countTimeCall()},
		Condition: &influxql.BinaryExpr{},
	}
	require.True(t, test6.IsOnlyCountTime())

	// Test case 7: HasInterval is true, should return false
	test7 := &ProcessorOptions{
		Exprs:    []influxql.Expr{countTimeCall()},
		Interval: hybridqp.Interval{Duration: time.Second},
	}
	require.False(t, test7.IsOnlyCountTime())

	// Test case 8: OptDimension is not empty, should return false
	test8 := &ProcessorOptions{
		Exprs:      []influxql.Expr{countTimeCall()},
		Dimensions: []string{"tag"},
	}
	require.False(t, test8.IsOnlyCountTime())
}

func TestIsOnlyCsStoreForOpt(t *testing.T) {
	tests := []struct {
		name     string
		sources  influxql.Sources
		expected bool
	}{
		{
			name: "only column store",
			sources: influxql.Sources{
				&influxql.Measurement{
					Name:       "m1",
					EngineType: config.COLUMNSTORE,
				},
			},
			expected: true,
		},
		{
			name: "only ts store",
			sources: influxql.Sources{
				&influxql.Measurement{
					Name:       "m2",
					EngineType: config.TSSTORE,
				},
			},
			expected: false,
		},
		{
			name: "multi measurements",
			sources: influxql.Sources{
				&influxql.Measurement{
					Name:       "m1",
					EngineType: config.COLUMNSTORE,
				},
				&influxql.Measurement{
					Name:       "m2",
					EngineType: config.TSSTORE,
				},
			},
			expected: false,
		},
		{
			name: "subquery with cs_store",
			sources: influxql.Sources{
				&influxql.SubQuery{
					Alias: "a",
					Statement: &influxql.SelectStatement{
						Sources: influxql.Sources{
							&influxql.Measurement{
								Name:       "m3",
								EngineType: config.COLUMNSTORE,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "subquery with ts_store",
			sources: influxql.Sources{
				&influxql.SubQuery{
					Alias: "a",
					Statement: &influxql.SelectStatement{
						Sources: influxql.Sources{
							&influxql.Measurement{
								Name:       "m4",
								EngineType: config.TSSTORE,
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "multi measurement and subquery",
			sources: influxql.Sources{
				&influxql.Measurement{
					Name:       "m1",
					EngineType: config.COLUMNSTORE,
				},
				&influxql.SubQuery{
					Alias: "a",
					Statement: &influxql.SelectStatement{
						Sources: influxql.Sources{
							&influxql.Measurement{
								Name:       "m3",
								EngineType: config.COLUMNSTORE,
							},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &ProcessorOptions{
				Sources: tt.sources,
			}

			result := opt.HaveOnlyCSStore()
			require.Equal(t, tt.expected, result)
		})
	}
}

// Helper function to create a count(time) call
func countTimeCall() influxql.Expr {
	return &influxql.Call{
		Name: "count",
		Args: []influxql.Expr{
			&influxql.VarRef{Val: "time"},
		},
	}
}

// Helper function to create a count(other) call
func countOtherCall() influxql.Expr {
	return &influxql.Call{
		Name: "count",
		Args: []influxql.Expr{
			&influxql.VarRef{Val: "other"},
		},
	}
}
