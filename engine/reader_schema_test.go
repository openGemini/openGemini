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
package engine_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func createColumnNames() []string {
	return []string{"id", "name"}
}

func createFieldsAndTags() influxql.Fields {
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
				Type: influxql.Tag,
			},
			Alias: "",
		},
	)

	return fields
}

func createFilterCondition() []*influxql.VarRef {
	filters := make([]*influxql.VarRef, 0, 2)
	filters = append(filters,
		&influxql.VarRef{
			Val:  "score",
			Type: influxql.Float,
		},
		&influxql.VarRef{
			Val:  "country",
			Type: influxql.Tag,
		},
	)
	return filters
}

func TestNewRecordSchema(t *testing.T) {
	querySchema := executor.NewQuerySchema(createFieldsAndTags(), createColumnNames(), &query.ProcessorOptions{}, nil)
	var schema record.Schemas
	_, dstSchema := engine.NewRecordSchema(querySchema, nil, schema, createFilterCondition(), config.COLUMNSTORE)
	expSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "country"},
		record.Field{Type: influx.Field_Type_Int, Name: "id"},
		record.Field{Type: influx.Field_Type_String, Name: "name"},
		record.Field{Type: influx.Field_Type_Float, Name: "score"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	assert.Equal(t, expSchema, dstSchema)

	_, dstSchema = engine.NewRecordSchema(querySchema, nil, nil, nil, config.COLUMNSTORE)
	expSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "id"},
		record.Field{Type: influx.Field_Type_String, Name: "name"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	assert.Equal(t, expSchema, dstSchema)

	_, dstSchema = engine.NewRecordSchema(querySchema, nil, schema, createFilterCondition(), config.TSSTORE)
	expSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "id"},
		record.Field{Type: influx.Field_Type_Float, Name: "score"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	assert.Equal(t, expSchema, dstSchema)

	_, dstSchema = engine.NewRecordSchema(querySchema, nil, nil, nil, config.TSSTORE)
	expSchema = record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "id"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	assert.Equal(t, expSchema, dstSchema)
}
