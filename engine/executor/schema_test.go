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

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func createRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Float},
		influxql.VarRef{Val: "val3", Type: influxql.Boolean},
	)

	return rowDataType
}

func createColumnNames() []string {
	return []string{"id", "name", "score", "good"}
}

func createFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "id",
				Type: influxql.Integer,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "name",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "score",
				Type: influxql.Float,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "good",
				Type: influxql.Boolean,
			},
			Alias: "",
		},
	)

	return fields
}

func createVarRefsFromFields() influxql.VarRefs {
	fields := createFields()

	refs := make(influxql.VarRefs, 0, len(fields))
	for _, f := range fields {
		refs = append(refs, *(f.Expr.(*influxql.VarRef)))
	}

	return refs
}

func createMeasurement() *influxql.Measurement {
	return &influxql.Measurement{Name: "students"}
}

func createUniqueRowDataTypeOfFields() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Float},
		influxql.VarRef{Val: "val3", Type: influxql.Boolean},
	)

	return rowDataType
}

func createRowDataTypeOfFields() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "id", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
		influxql.VarRef{Val: "score", Type: influxql.Float},
		influxql.VarRef{Val: "good", Type: influxql.Boolean},
	)

	return rowDataType
}

func TestRowDataType(t *testing.T) {
	rt1 := createRowDataType()
	rt2 := createRowDataType()

	if !rt1.Equal(rt2) {
		t.Errorf("two row datatype aren't same, they are (%v) and (%v)", rt1, rt2)
	}

	if rt1.NumColumn() != 4 {
		t.Errorf("row datatype must have 4 columns, but %v", rt1.NumColumn())
	}

	if rt1.FieldIndex("val0") != 0 {
		t.Errorf("column val0 must at indice 0, but %v", rt1.FieldIndex("val0"))
	}

	if rt1.FieldIndex("val1") != 1 {
		t.Errorf("column val1 must at indice 1, but %v", rt1.FieldIndex("val0"))
	}

	if rt1.FieldIndex("val2") != 2 {
		t.Errorf("column val2 must at indice 2, but %v", rt1.FieldIndex("val0"))
	}

	if rt1.FieldIndex("val3") != 3 {
		t.Errorf("column val3 must at indice 3, but %v", rt1.FieldIndex("val0"))
	}
}

func TestQuerySchema(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createFields(), createColumnNames(), &opt)

	if len(schema.Calls()) != 0 {
		t.Errorf("length of calls must be 0, but %v", len(schema.Calls()))
	}

	if len(schema.Binarys()) != 0 {
		t.Errorf("length of binarys must be 0, but %v", len(schema.Binarys()))
	}

	if len(schema.Refs()) != 4 {
		t.Errorf("length of refs must be 4, but %v", len(schema.Refs()))
	}

	if len(schema.Symbols()) != 4 {
		t.Errorf("length of symbols must be 4, but %v", len(schema.Symbols()))
	}

	if len(schema.Mapping()) != 4 {
		t.Errorf("length of mapping must be 4, but %v", len(schema.Mapping()))
	}
}

func TestQuerySchemaHasSeriesAgg(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createCallFields(), createCallColumnNames(), &opt)
	assert.Equal(t, schema.HasInSeriesAgg(), false)
	schema = executor.NewQuerySchema(createPercentileCallFields(), createCallColumnNames(), &opt)
	assert.Equal(t, schema.HasInSeriesAgg(), false)
}

func createPercentileCallFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 2)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "percentile",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "age",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "rate",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "name",
						Type: influxql.String,
					},
					&influxql.StringLiteral{Val: "XiaoMing"},
				},
			},
			Alias: "",
		},
	)

	return fields
}

func createCallFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 2)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "abs",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "age",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "str",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "name",
						Type: influxql.String,
					},
					&influxql.StringLiteral{Val: "XiaoMing"},
				},
			},
			Alias: "",
		},
	)

	return fields
}

func createCallColumnNames() []string {
	return []string{"age", "name"}
}

func TestHasMathAndString(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createCallFields(), createCallColumnNames(), &opt)
	assert.Equal(t, schema.HasMath(), true)
	assert.Equal(t, schema.HasString(), true)
}

func TestHasAuxTags(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createCallFields(), createCallColumnNames(), &opt)
	schema.Refs()
	assert.Equal(t, schema.HasMath(), true)
	assert.Equal(t, schema.HasString(), true)
}

func TestGetFieldType(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(influxql.Fields{&influxql.Field{
		Expr: &influxql.Call{
			Name: "sum",
			Args: []influxql.Expr{&influxql.VarRef{Val: "age", Type: influxql.Integer}},
		},
	}}, []string{"sum_age"}, &opt)
	if i, err := schema.GetFieldType(0); i != int64(influxql.Integer) || err != nil {
		t.Fatal()
	}
}
