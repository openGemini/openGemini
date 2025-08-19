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

package executor

import (
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestMatchSortMergeJoin(t *testing.T) {
	type args struct {
		join   *influxql.Join
		schema hybridqp.Catalog
	}
	tests := []struct {
		name string
		args args
		want bool
		err  error
	}{
		{
			name: "sort merge join",
			args: args{
				join: &influxql.Join{
					LSrc: &influxql.SubQuery{
						Alias: "a",
						Statement: &influxql.SelectStatement{
							Dimensions: []*influxql.Dimension{
								{Expr: &influxql.VarRef{Val: "id"}},
							},
						},
					},
					RSrc: &influxql.SubQuery{
						Alias: "b",
						Statement: &influxql.SelectStatement{
							Dimensions: []*influxql.Dimension{
								{Expr: &influxql.VarRef{Val: "id"}},
							},
						},
					},
					Condition: &influxql.BinaryExpr{
						Op:  influxql.EQ,
						LHS: &influxql.VarRef{Val: "a.id"},
						RHS: &influxql.VarRef{Val: "b.id"},
					},
				},
				schema: NewQuerySchema(nil, nil, &query.ProcessorOptions{Dimensions: []string{"id"}}, nil),
			},
			want: true,
		},
		{
			name: "hash join",
			args: args{
				join: &influxql.Join{
					LSrc: &influxql.SubQuery{
						Alias:     "a",
						Statement: &influxql.SelectStatement{},
					},
					RSrc: &influxql.SubQuery{
						Alias:     "b",
						Statement: &influxql.SelectStatement{},
					},
					Condition: &influxql.BinaryExpr{
						Op: influxql.AND,
						LHS: &influxql.BinaryExpr{
							Op:  influxql.EQ,
							LHS: &influxql.VarRef{Val: "a.id"},
							RHS: &influxql.VarRef{Val: "b.id"},
						},
						RHS: &influxql.BinaryExpr{
							Op:  influxql.EQ,
							LHS: &influxql.VarRef{Val: "a.time"},
							RHS: &influxql.VarRef{Val: "b.time"},
						},
					},
				},
				schema: NewQuerySchema(nil, nil, &query.ProcessorOptions{Dimensions: []string{"id"}}, nil),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MatchSortMergeJoin(tt.args.join, tt.args.schema)
			if tt.err != nil {
				assert.Equalf(t, tt.err, err, "MatchSortMergeJoin(%v, %v)", tt.args.join, tt.args.schema)
			}
			assert.Equalf(t, tt.want, got, "MatchSortMergeJoin(%v, %v)", tt.args.join, tt.args.schema)
		})
	}
}
