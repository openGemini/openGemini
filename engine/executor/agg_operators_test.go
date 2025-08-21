/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor

import (
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestADRmseExtOp_CreateRoutine(t *testing.T) {
	type args struct {
		params *AggCallFuncParams
	}
	tests := []struct {
		name string
		args args
		err  error
	}{
		{
			name: "ad_rmse_ext: SchemaNotAligned",
			args: args{params: &AggCallFuncParams{
				InRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.Float},
				),
				OutRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.Float},
				),
				ExprOpt: hybridqp.ExprOptions{
					Expr: &influxql.Call{Name: "ad_rmse_ext", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
					Ref:  influxql.VarRef{Val: `value`, Type: influxql.Float},
				},
			}},
			err: errno.NewError(errno.SchemaNotAligned, "ad_rmse_ext", "input and output schemas are not aligned"),
		},
		{
			name: "ad_rmse_ext: UnsupportedDataType",
			args: args{params: &AggCallFuncParams{
				InRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.String},
				),
				OutRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.Float},
				),
				ExprOpt: hybridqp.ExprOptions{
					Expr: &influxql.Call{Name: "ad_rmse_ext", Args: []influxql.Expr{hybridqp.MustParseExpr("value")}},
					Ref:  influxql.VarRef{Val: `value`, Type: influxql.Float},
				},
			}},
			err: errno.NewError(errno.UnsupportedDataType, "ad_rmse_ext", influxql.String.String()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ADRmseExtOp{}
			_, err := c.CreateRoutine(tt.args.params)
			assert.ErrorContains(t, err, tt.err.Error(), "CreateRoutine(%v)", tt.args.params)
		})
	}
}

func TestRegrSlopeOp_CreateRoutine(t *testing.T) {
	type args struct {
		params *AggCallFuncParams
	}
	tests := []struct {
		name string
		args args
		err  error
	}{
		{
			name: "regr_slope: SchemaNotAligned",
			args: args{params: &AggCallFuncParams{
				InRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.Float},
				),
				OutRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.Float},
				),
				ExprOpt: hybridqp.ExprOptions{
					Expr: &influxql.Call{Name: "regr_slope", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
					Ref:  influxql.VarRef{Val: `value`, Type: influxql.Float},
				},
			}},
			err: errno.NewError(errno.SchemaNotAligned, "regr_slope", "input and output schemas are not aligned"),
		},
		{
			name: "regr_slope: UnsupportedDataType",
			args: args{params: &AggCallFuncParams{
				InRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.String},
				),
				OutRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "value", Type: influxql.Float},
				),
				ExprOpt: hybridqp.ExprOptions{
					Expr: &influxql.Call{Name: "regr_slope", Args: []influxql.Expr{hybridqp.MustParseExpr("value")}},
					Ref:  influxql.VarRef{Val: `value`, Type: influxql.Float},
				},
			}},
			err: errno.NewError(errno.UnsupportedDataType, "regr_slope", influxql.String.String()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &RegrSlopeOp{}
			_, err := c.CreateRoutine(tt.args.params)
			assert.Error(t, err, tt.err.Error(), "CreateRoutine(%v)", tt.args.params)
		})
	}
}
