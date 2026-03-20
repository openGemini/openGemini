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
	"github.com/stretchr/testify/assert"
)

func Test_compiledStatement_compileFields(t *testing.T) {
	type args struct {
		stmt *influxql.SelectStatement
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
		err     error
	}{
		{
			name: "raise err without alias",
			args: args{
				stmt: &influxql.SelectStatement{
					Fields: []*influxql.Field{{Expr: &influxql.StringLiteral{Val: "event"}}},
				},
			},
			wantErr: assert.NoError,
			err:     nil,
		},
		{
			name: "query normal with alias",
			args: args{
				stmt: &influxql.SelectStatement{
					Fields: []*influxql.Field{{Expr: &influxql.StringLiteral{Val: "event"}, Alias: "type"}},
				},
			},
			wantErr: assert.NoError,
			err:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compiledStatement{
				stmt: tt.args.stmt,
			}
			tt.wantErr(t, c.compileFields(tt.args.stmt), tt.err)
		})
	}
}

func TestIsOutOfTimeProtection(t *testing.T) {
	type args struct {
		stmt      *influxql.SelectStatement
		opt       ProcessorOptions
		timeRange influxql.TimeRange
	}
	TimeFilterProtection = true
	defer func() { TimeFilterProtection = false }()
	fields := influxql.Fields{&influxql.Field{
		Expr: &influxql.Call{
			Name: "last_row",
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  "val1",
					Type: influxql.Float,
				},
			},
		},
	},
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "with_hint_type",
			args: args{
				stmt: &influxql.SelectStatement{Fields: fields, IsCreateStream: false},
				opt: ProcessorOptions{
					HintType:       hybridqp.FullSeriesQuery,
					GroupByAllDims: true,
					Sources:        []influxql.Source{&influxql.Measurement{Name: "m", EngineType: config.TSSTORE}},
				},
				timeRange: influxql.TimeRange{Min: time.Unix(0, influxql.MinTime), Max: time.Unix(0, influxql.MaxTime)},
			},
			want: false,
		},
		{
			name: "without_hint_type",
			args: args{
				stmt: &influxql.SelectStatement{Fields: fields, IsCreateStream: false},
				opt: ProcessorOptions{
					GroupByAllDims: true,
					Sources:        []influxql.Source{&influxql.Measurement{Name: "m", EngineType: config.TSSTORE}},
				},
				timeRange: influxql.TimeRange{Min: time.Unix(0, influxql.MinTime), Max: time.Unix(0, influxql.MaxTime)},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, IsOutOfTimeProtection(tt.args.stmt, tt.args.opt, tt.args.timeRange), tt.want)
		})
	}
}

func Test_compiledStatement_validateFields(t *testing.T) {
	type args struct {
		fields                     []*compiledField
		functionCalls              []*influxql.Call
		topBottomFunction          string
		percentileOGSketchFunction string
		hasAuxiliaryFields         bool
		hasDistinct                bool
		fillOption                 influxql.FillOption
		interval                   hybridqp.Interval
		inheritedInterval          bool
		stmt                       *influxql.SelectStatement
		OnlySelectors              bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errMsg  string
	}{
		{
			name: "empty fields should error",
			args: args{
				fields:        []*compiledField{},
				functionCalls: []*influxql.Call{},
				stmt:          &influxql.SelectStatement{},
			},
			wantErr: true,
			errMsg:  "at least 1 non-time field must be queried",
		},
		{
			name: "multiple functions with top/bottom should error",
			args: args{
				fields:            []*compiledField{{}},
				functionCalls:     []*influxql.Call{{Name: "max"}, {Name: "min"}},
				topBottomFunction: "top",
				stmt:              &influxql.SelectStatement{},
			},
			wantErr: true,
			errMsg:  "selector function top() cannot be combined with other functions",
		},
		{
			name: "fill(none) without function should error",
			args: args{
				fields:        []*compiledField{{}},
				functionCalls: []*influxql.Call{},
				fillOption:    influxql.NoFill,
				stmt:          &influxql.SelectStatement{},
			},
			wantErr: true,
			errMsg:  "fill(none) must be used with a function",
		},
		{
			name: "fill(linear) without function should error",
			args: args{
				fields:        []*compiledField{{}},
				functionCalls: []*influxql.Call{},
				fillOption:    influxql.LinearFill,
				stmt:          &influxql.SelectStatement{},
			},
			wantErr: true,
			errMsg:  "fill(linear) must be used with a function",
		},
		{
			name: "GROUP BY without aggregate function should error",
			args: args{
				fields:            []*compiledField{{}},
				functionCalls:     []*influxql.Call{},
				interval:          hybridqp.Interval{Duration: time.Hour},
				inheritedInterval: false,
				stmt:              &influxql.SelectStatement{},
			},
			wantErr: true,
			errMsg:  "GROUP BY requires at least one aggregate function",
		},
		{
			name: "distinct with multiple functions should error",
			args: args{
				fields:        []*compiledField{{}},
				functionCalls: []*influxql.Call{{Name: "distinct"}, {Name: "max"}},
				hasDistinct:   true,
				stmt:          &influxql.SelectStatement{DistinctFields: nil},
			},
			wantErr: true,
			errMsg:  "aggregate function distinct() cannot be combined with other functions or fields",
		},
		{
			name: "distinct with auxiliary fields should error",
			args: args{
				fields:             []*compiledField{{}},
				functionCalls:      []*influxql.Call{{Name: "distinct"}},
				hasDistinct:        true,
				hasAuxiliaryFields: true,
				stmt:               &influxql.SelectStatement{DistinctFields: nil},
			},
			wantErr: true,
			errMsg:  "aggregate function distinct() cannot be combined with other functions or fields",
		},
		{
			name: "multiple functions with percentile_ogsketch should error",
			args: args{
				fields:                     []*compiledField{{}},
				functionCalls:              []*influxql.Call{{Name: "percentile_ogsketch"}, {Name: "max"}},
				percentileOGSketchFunction: "percentile_ogsketch",
				stmt:                       &influxql.SelectStatement{},
			},
			wantErr: true,
			errMsg:  "selector function percentile_ogsketch() cannot be combined with other functions",
		},
		{
			name: "mixing aggregate and non-aggregate should error",
			args: args{
				fields:             []*compiledField{{}},
				functionCalls:      []*influxql.Call{{Name: "max"}},
				hasAuxiliaryFields: true,
				stmt:               &influxql.SelectStatement{},
			},
			wantErr: true,
			errMsg:  "mixing aggregate and non-aggregate queries is not supported",
		},
		{
			name: "multiple selector functions with auxiliary fields should error",
			args: args{
				fields:             []*compiledField{{}},
				functionCalls:      []*influxql.Call{{Name: "max"}, {Name: "min"}},
				hasAuxiliaryFields: true,
				stmt:               &influxql.SelectStatement{},
				OnlySelectors:      true,
			},
			wantErr: true,
			errMsg:  "mixing multiple selector functions with tags or fields is not supported",
		},
		{
			name: "valid single field should pass",
			args: args{
				fields:        []*compiledField{{}},
				functionCalls: []*influxql.Call{},
				stmt:          &influxql.SelectStatement{},
			},
			wantErr: false,
		},
		{
			name: "valid single function should pass",
			args: args{
				fields:        []*compiledField{{}},
				functionCalls: []*influxql.Call{{Name: "max"}},
				stmt:          &influxql.SelectStatement{},
			},
			wantErr: false,
		},
		{
			name: "valid distinct should pass",
			args: args{
				fields:        []*compiledField{{}},
				functionCalls: []*influxql.Call{{Name: "distinct"}},
				hasDistinct:   true,
				stmt:          &influxql.SelectStatement{DistinctFields: nil},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compiledStatement{
				Fields:                     tt.args.fields,
				FunctionCalls:              tt.args.functionCalls,
				TopBottomFunction:          tt.args.topBottomFunction,
				PercentileOGSketchFunction: tt.args.percentileOGSketchFunction,
				HasAuxiliaryFields:         tt.args.hasAuxiliaryFields,
				HasDistinct:                tt.args.hasDistinct,
				FillOption:                 tt.args.fillOption,
				Interval:                   tt.args.interval,
				InheritedInterval:          tt.args.inheritedInterval,
				OnlySelectors:              tt.args.OnlySelectors,
				stmt:                       tt.args.stmt,
			}
			err := c.validateFields()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.errMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_compiledStatement_validateCSAuxiliaryFields(t *testing.T) {
	type args struct {
		hasAuxiliaryFields bool
		functionCalls      []*influxql.Call
		onlySelectors      bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errMsg  string
	}{
		{
			name: "no auxiliary fields should pass",
			args: args{
				hasAuxiliaryFields: false,
				functionCalls:      []*influxql.Call{},
				onlySelectors:      true,
			},
			wantErr: false,
		},
		{
			name: "single selector function with auxiliary fields should pass",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "top"}},
				onlySelectors:      true,
			},
			wantErr: false,
		},
		{
			name: "single bottom function with auxiliary fields should pass",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "bottom"}},
				onlySelectors:      true,
			},
			wantErr: false,
		},
		{
			name: "single max function with auxiliary fields should error",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "max"}},
				onlySelectors:      true,
			},
			wantErr: true,
			errMsg:  "mixing column and max(column) queries for column store engine is not supported",
		},
		{
			name: "single min function with auxiliary fields should error",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "min"}},
				onlySelectors:      true,
			},
			wantErr: true,
			errMsg:  "mixing column and min(column) queries for column store engine is not supported",
		},
		{
			name: "single first function with auxiliary fields should error",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "first"}},
				onlySelectors:      true,
			},
			wantErr: true,
			errMsg:  "mixing column and first(column) queries for column store engine is not supported",
		},
		{
			name: "single last function with auxiliary fields should error",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "last"}},
				onlySelectors:      true,
			},
			wantErr: true,
			errMsg:  "mixing column and last(column) queries for column store engine is not supported",
		},
		{
			name: "single percentile function with auxiliary fields should error",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "percentile"}},
				onlySelectors:      true,
			},
			wantErr: true,
			errMsg:  "mixing column and percentile(column) queries for column store engine is not supported",
		},
		{
			name: "non selector function with auxiliary fields should pass",
			args: args{
				hasAuxiliaryFields: true,
				functionCalls:      []*influxql.Call{{Name: "count"}},
				onlySelectors:      false,
			},
			wantErr: false, // This should not error since OnlySelectors is false
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compiledStatement{
				HasAuxiliaryFields: tt.args.hasAuxiliaryFields,
				FunctionCalls:      tt.args.functionCalls,
				OnlySelectors:      tt.args.onlySelectors,
			}
			err := c.validateCSAuxiliaryFields()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.errMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
