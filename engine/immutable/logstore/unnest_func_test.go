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

package logstore

import (
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestUnnestMatchAll(t *testing.T) {
	unnest := &influxql.Unnest{
		Expr: &influxql.Call{
			Name: "match_all",
			Args: []influxql.Expr{&influxql.VarRef{Val: "location:([a-z]+)"}, &influxql.VarRef{Val: "content", Type: influxql.String}},
		},
		Aliases: []string{"key1"},
		DstType: []influxql.DataType{influxql.String},
	}
	match, _ := NewUnnestMatchAll(unnest)
	result := match.Get("location:test")
	v, ok := result["key1"]
	if !ok || v != "test" {
		t.Fatal("get result error")
	}
	result = match.Get("type:test")
	v, ok = result["key1"]
	if !ok || v != "" {
		t.Fatal("get nil result error")
	}

	matchNil, _ := NewUnnestMatchAll(&influxql.Unnest{
		Expr:    &influxql.VarRef{},
		Aliases: []string{"key1"},
		DstType: []influxql.DataType{influxql.String},
	})
	if matchNil != nil {
		t.Fatal("get nil match error")
	}

	_, err := NewUnnestMatchAll(&influxql.Unnest{
		Expr: &influxql.Call{
			Name: "match_all",
			Args: []influxql.Expr{&influxql.VarRef{Val: "* | EXTRACT(content:\"type:(a-z:0-9]+\") AS(key1) | select count (key1) group by key1"}, &influxql.VarRef{Val: "content", Type: influxql.String}},
		},
		Aliases: []string{"key1"},
		DstType: []influxql.DataType{influxql.String},
	})
	if err == nil {
		t.Fatal("get err match")
	}
}

func TestUnnestMatchAllOperator(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "content"},
		record.Field{Type: influx.Field_Type_String, Name: "key1"},
		record.Field{Type: influx.Field_Type_String, Name: "value1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	unnest := &influxql.Unnest{
		Expr: &influxql.Call{
			Name: "match_all",
			Args: []influxql.Expr{
				&influxql.VarRef{Val: "([a-z]+),([0-9]+)", Type: influxql.String},
				&influxql.VarRef{Val: "content", Type: influxql.String},
			},
		},
		Aliases: []string{"key1", "value1"},
		DstType: []influxql.DataType{influxql.String, influxql.String},
	}

	matchAllOperator, err := GetUnnestFuncOperator(unnest, schema)
	if err != nil {
		t.Fatalf("GetUnnestFuncOperator error: %+v", err)
	}

	contents := []string{"shenzhen,1", "", "", "shanghai,2"}
	rec := record.NewRecord(schema, true)
	for i := 0; i < rec.ColNums(); i++ {
		switch rec.Schema.Field(i).Name {
		case "content":
			for j := 0; j < len(contents); j++ {
				rec.Column(i).AppendString(contents[j])
			}
		case "key1":
			for j := 0; j < len(contents); j++ {
				rec.Column(i).AppendStringNull()
			}
		case "value1":
			for j := 0; j < len(contents); j++ {
				rec.Column(i).AppendStringNull()
			}
		case "time":
			for j := 0; j < len(contents); j++ {
				rec.Column(i).AppendInteger(int64(1695461186000000000))
			}
		}
	}
	matchAllOperator.Compute(rec)
	if rec.Column(1).Len != len(contents) {
		t.Fatalf("matchAllOperator.Compute error, expected len: %v, real len:%v", len(contents), rec.Column(1).Len)
	}
}
