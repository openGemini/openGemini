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

package executor_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func createQuerySchema() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createFields(), createColumnNames(), &opt)
	m := createMeasurement()
	schema.AddTable(m, schema.MakeRefs())

	return schema
}

func IsAuxExprOptions(ExprOptions []hybridqp.ExprOptions) bool {
	for _, option := range ExprOptions {
		switch option.Expr.(type) {
		case *influxql.VarRef:
			continue
		default:
			return false
		}
	}

	return true
}

func TestLogicalPlan(t *testing.T) {
	schema := createQuerySchema()

	series := executor.NewLogicalSeries(schema)
	reader := executor.NewLogicalReader(series, schema)

	if len(reader.RowExprOptions()) != 4 {
		t.Errorf("call options of reader must be 4, but %v", len(reader.RowExprOptions()))
	}

	if !IsAuxExprOptions(reader.RowExprOptions()) {
		t.Error("call options of reader must be aux call")
	}

	agg := executor.NewLogicalAggregate(reader, schema)

	if len(agg.RowExprOptions()) != 4 {
		t.Errorf("call options of agg must be 4, but %v", len(agg.RowExprOptions()))
	}

	if !IsAuxExprOptions(agg.RowExprOptions()) {
		t.Error("call options of agg must be aux call")
	}
}

func createFieldsFilterBlank() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "non_negative_difference",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "id",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "non_negative_difference",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "score",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
	)

	return fields
}

func createColumnNamesFilterBlank() []string {
	return []string{"id", "score"}
}

func createQuerySchemaFilterBlank() *executor.QuerySchema {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createFieldsFilterBlank(), createColumnNamesFilterBlank(), &opt)
	m := createMeasurement()
	schema.AddTable(m, schema.MakeRefs())

	return schema
}

func TestLogicalPlanFilterBlank(t *testing.T) {
	schema := createQuerySchemaFilterBlank()

	series := executor.NewLogicalSeries(schema)
	reader := executor.NewLogicalReader(series, schema)

	if len(reader.RowExprOptions()) != 2 {
		t.Errorf("call options of reader must be 2, but %v", len(reader.RowExprOptions()))
	}

	agg := executor.NewLogicalAggregate(reader, schema)

	if len(agg.RowExprOptions()) != 2 {
		t.Errorf("call options of agg must be 2, but %v", len(agg.RowExprOptions()))
	}
	if !schema.HasBlankRowCall() {
		t.Errorf("Options have calls that maybe generate blank row")
	}

	filterBlank := executor.NewLogicalFilterBlank(agg, schema)

	if !IsAuxExprOptions(filterBlank.RowExprOptions()) {
		t.Error("call options of agg must be aux call")
	}
}
