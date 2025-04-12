// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"testing"

	assert2 "github.com/stretchr/testify/assert"
)

func Test_floatBuffer_updateValue(t *testing.T) {
	type fields struct {
		times  []int64
		values []float64
		s      int
		e      int
	}
	type args struct {
		times    []int64
		values   []float64
		param    *ReducerParams
		rangeEnd int64
		start    int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name: "1",
			fields: fields{
				times:  []int64{1, 2, 3},
				values: []float64{1, 2, 3},
				s:      1,
				e:      3,
			},
			args: args{
				times:    []int64{4, 5, 6},
				values:   []float64{4, 5, 6},
				param:    &ReducerParams{sameWindow: true, step: 2, rangeDuration: 3},
				rangeEnd: 6,
				start:    0,
			},
			want: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &floatBuffer{
				times:  tt.fields.times,
				values: tt.fields.values,
				s:      tt.fields.s,
				e:      tt.fields.e,
			}
			b.updateValue(tt.args.times, tt.args.values, tt.args.param, tt.args.rangeEnd, tt.args.start)
			assert2.Equal(t, tt.want, len(b.values))
		})
	}
}
