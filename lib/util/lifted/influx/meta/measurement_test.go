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

package meta

import (
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	assert1 "github.com/stretchr/testify/assert"
)

func TestMeasurementInfo_FilterTagKeys(t *testing.T) {
	type fields struct {
		Name   string
		Schema CleanSchema
	}
	mst := "mst"
	ret := make(map[string]map[string]struct{}, 1)
	type args struct {
		cond influxql.Expr
		tr   influxql.TimeRangeValue
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name: "schema clean off",
			fields: fields{
				Name: mst,
				Schema: map[string]SchemaVal{
					"tk1": {Typ: influx.Field_Type_Tag, EndTime: 1000},
				},
			},
			args: args{
				cond: nil,
				tr:   influxql.TimeRangeValue{Min: 4294967296001, Max: influxql.MaxTime},
			},
			want: 0,
		},
		{
			name: "schema clean on",
			fields: fields{
				Name: mst,
				Schema: map[string]SchemaVal{
					"tk1": {Typ: influx.Field_Type_Tag, EndTime: 0},
				},
			},
			args: args{
				cond: nil,
				tr:   influxql.TimeRangeValue{Min: 4294967296001, Max: influxql.MaxTime},
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msti := &MeasurementInfo{
				Name:   tt.fields.Name,
				Schema: &tt.fields.Schema,
			}
			ret[tt.fields.Name] = make(map[string]struct{})
			msti.FilterTagKeys(tt.args.cond, tt.args.tr, ret)
			assert1.Equal(t, tt.want, len(ret[tt.fields.Name]))
		})
	}
}

func TestMeasurementInfo_UnmarshalBinary_EmptyShardIdxes(t *testing.T) {
	mst := &MeasurementInfo{
		Name:        "mst_0000",
		originName:  "mst",
		ShardIdexes: make(map[uint64][]int),
	}
	binary, err := mst.MarshalBinary()
	assert1.NoError(t, err)
	mstUnmarshal := &MeasurementInfo{}
	err = mstUnmarshal.UnmarshalBinary(binary)
	assert1.NoError(t, err)
	assert1.NotNil(t, mstUnmarshal.ShardIdexes)
}
