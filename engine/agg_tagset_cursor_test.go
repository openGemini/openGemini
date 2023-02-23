/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package engine

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func Test_fileLoopCursor_SinkPlan(t *testing.T) {
	ctx := &idKeyCursorContext{
		schema: record.Schemas{{Name: "value", Type: influx.Field_Type_Int}, {Name: "time", Type: influx.Field_Type_Int}},
	}
	tagSet := tsi.NewTagSetInfo()
	cursor := NewFileLoopCursor(ctx, nil, nil, tagSet, 0, 1, nil)

	opt := query.ProcessorOptions{}
	fields := []*influxql.Field{
		{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "value",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	}
	schema := executor.NewQuerySchema(fields, []string{"id", "value", "alive", "name"}, &opt)
	series := executor.NewLogicalSeries(schema)
	plan := executor.NewLogicalReader(series, schema)
	cursor.SinkPlan(plan)
	_, _, _ = cursor.Next()
	require.Equal(t, 1, len(cursor.ridIdx))
	if _, ok := cursor.ridIdx[0]; !ok {
		t.Fatal("column 0 should rid out")
	}
	require.Equal(t, true, cursor.isCutSchema)
}
