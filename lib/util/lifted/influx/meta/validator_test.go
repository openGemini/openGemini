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
	assert2 "github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

func TestValidSecondaryIndex(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *influxql.CreateMeasurementStatement
		wantErr bool
		errMsg  string
	}{
		{
			name: "invalid index info 1",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{},
				IndexParams: [][]influxql.Expr{},
			},
			wantErr: true,
			errMsg:  "the IndexType does not match the number of index fields",
		},
		{
			name: "invalid index info 2",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{},
			},
			wantErr: true,
			errMsg:  "the IndexParams don't match the number of index fields",
		},
		{
			name: "invalid index column",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{{"f2"}},
				IndexParams: [][]influxql.Expr{{}},
				Tags:        map[string]int32{},
				Fields:      map[string]int32{"f1": influx.Field_Type_Int},
			},
			wantErr: true,
			errMsg:  "minmax index: f2 is not a tag or a field",
		},
		{
			name: "primarykey is the secondary index",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{"f1"},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{{}},
				Tags:        map[string]int32{},
				Fields:      map[string]int32{"f1": influx.Field_Type_Int},
			},
			wantErr: true,
			errMsg:  "the primary key column: f1 can't be the minmax index",
		},
		{
			name: "valid text index: tag && string field",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"text"},
				IndexList:   [][]string{{"t1", "f1"}},
				IndexParams: [][]influxql.Expr{{}},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_String},
			},
			wantErr: false,
		},
		{
			name: "invalid text index1: int field",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{"t1"},
				IndexType:   []string{"text"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{{}},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_Int},
			},
			wantErr: true,
			errMsg:  "text index only support Tag or String field, but the column: f1 is Integer field",
		},
		{
			name: "invalid text index2: float field",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{"t1"},
				IndexType:   []string{"text"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{{}},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_Float},
			},
			wantErr: true,
			errMsg:  "text index only support Tag or String field, but the column: f1 is Float field",
		},
		{
			name: "valid bloomfilter_universal index1: tag && string/int/float field, no indexParam",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"bloomfilter_universal"},
				IndexList:   [][]string{{"t1", "f1", "f2", "f3"}},
				IndexParams: [][]influxql.Expr{nil},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_String, "f2": influx.Field_Type_Int, "f3": influx.Field_Type_Float},
			},
			wantErr: false,
		},
		{
			name: "valid bloomfilter_universal index2: tag && string/int/float field, with indexParam",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"bloomfilter_universal"},
				IndexList:   [][]string{{"t1", "f1", "f2", "f3"}},
				IndexParams: [][]influxql.Expr{{&influxql.NumberLiteral{Val: 0.2}}},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_String, "f2": influx.Field_Type_Int, "f3": influx.Field_Type_Float},
			},
			wantErr: false,
		},
		{
			name: "invalid bloomfilter_universal index1: boolean field",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{"t1"},
				IndexType:   []string{"bloomfilter_universal"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{nil},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_Boolean},
			},
			wantErr: true,
			errMsg:  "bloomfilter_universal index does not support Boolean field, but the column: f1 is Boolean field",
		},
		{
			name: "invalid indexParam for bloomfilter_universal index: more params than one",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{"t1"},
				IndexType:   []string{"bloomfilter_universal"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{{&influxql.NumberLiteral{Val: 0.1}, &influxql.NumberLiteral{Val: 0.2}}},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_Float},
			},
			wantErr: true,
			errMsg:  "only expect one parameter for bloomfilter_universal",
		},
		{
			name: "invalid indexParam for bloomfilter_universal index: not NumberLiteral",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{"t1"},
				IndexType:   []string{"bloomfilter_universal"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{{&influxql.StringLiteral{Val: "0.1"}}},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_Float},
			},
			wantErr: true,
			errMsg:  "expect one Float value between (0,1) for bloomfilter_universal",
		},
		{
			name: "invalid indexParam for bloomfilter_universal index: not between (0,1)",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{"t1"},
				IndexType:   []string{"bloomfilter_universal"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{{&influxql.NumberLiteral{Val: 1.1}}},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_Float},
			},
			wantErr: true,
			errMsg:  "expect one Float value between (0,1) for bloomfilter_universal",
		},
		{
			name: "valid minmax index: int field && float field",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{{"f2", "f3"}},
				IndexParams: [][]influxql.Expr{nil},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_String, "f2": influx.Field_Type_Int, "f3": influx.Field_Type_Float},
			},
			wantErr: false,
		},
		{
			name: "invalid minmax index1: tag",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{{"t1"}},
				IndexParams: [][]influxql.Expr{nil},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_String, "f2": influx.Field_Type_Int, "f3": influx.Field_Type_Float},
			},
			wantErr: true,
			errMsg:  "minmax index only support Integer field or Float field, but the column: t1 is Tag",
		},
		{
			name: "invalid minmax index2: string field",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{nil},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_String, "f2": influx.Field_Type_Int, "f3": influx.Field_Type_Float},
			},
			wantErr: true,
			errMsg:  "minmax index only support Integer field or Float field, but the column: f1 is String",
		},
		{
			name: "valid minmax index: int field && float field",
			stmt: &influxql.CreateMeasurementStatement{
				PrimaryKey:  []string{},
				IndexType:   []string{"minmax"},
				IndexList:   [][]string{{"f1"}},
				IndexParams: [][]influxql.Expr{nil},
				Tags:        map[string]int32{"t1": influx.Field_Type_Tag},
				Fields:      map[string]int32{"f1": influx.Field_Type_Boolean, "f2": influx.Field_Type_Int, "f3": influx.Field_Type_Float},
			},
			wantErr: true,
			errMsg:  "minmax index only support Integer field or Float field, but the column: f1 is Boolean",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &influxql.CreateMeasurementStatement{
				PrimaryKey:  tt.stmt.PrimaryKey,
				IndexType:   tt.stmt.IndexType,
				IndexList:   tt.stmt.IndexList,
				IndexParams: tt.stmt.IndexParams,
				Tags:        tt.stmt.Tags,
				Fields:      tt.stmt.Fields,
			}
			err := ValidSecondaryIndex(c)
			if tt.wantErr {
				assert2.Error(t, err)
				assert2.Equal(t, tt.errMsg, err.Error())
			} else {
				assert2.NoError(t, err)
			}
		})
	}
}

func TestValidIndexList(t *testing.T) {
	tests := []struct {
		name      string
		indexList [][]string
		wantErr   bool
	}{
		{
			name:      "no duplicates",
			indexList: [][]string{{"a", "b", "c"}, {"d", "e", "f"}},
			wantErr:   false,
		},
		{
			name:      "duplicates in same index",
			indexList: [][]string{{"a", "b", "a"}, {"d", "e", "f"}},
			wantErr:   true,
		},
		{
			name:      "duplicates in different indexes",
			indexList: [][]string{{"a", "b", "c"}, {"d", "e", "a"}},
			wantErr:   false,
		},
		{
			name:      "empty index list",
			indexList: [][]string{},
			wantErr:   false,
		},
		{
			name:      "empty index",
			indexList: [][]string{{}},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validIndexList(tt.indexList); (err != nil) != tt.wantErr {
				t.Errorf("ValidIndexList() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
