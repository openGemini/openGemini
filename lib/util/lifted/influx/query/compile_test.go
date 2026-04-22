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

	"github.com/openGemini/openGemini/lib/errno"
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
			wantErr: assert.Error,
			err:     errno.NewError(errno.FieldIsLiteral),
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
