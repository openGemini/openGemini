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
	"sort"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

func NewRecordSchema(querySchema *executor.QuerySchema, auxTags []string, schema record.Schemas, filterConditions []*influxql.VarRef) ([]string, record.Schemas) {
	fieldMap := dictpool.Dict{}
	for key, ref := range querySchema.Refs() {
		switch ref.Type {
		case influxql.Integer, influxql.String, influxql.Boolean, influxql.Float:
			{
				fieldType := ref.Type
				v := fieldMap.Get(key)
				if v == nil {
					schema = append(schema, record.Field{Name: ref.Val, Type: record.ToModelTypes(fieldType)})
					fieldMap.Set(key, fieldType)
				}
			}
		case influxql.Tag:
			v := fieldMap.Get(key)
			if v == nil {
				fieldMap.Set(key, ref.Type)
				auxTags = append(auxTags, ref.Val)
			}
		default:
			log.Warn("unknown column field type", zap.String("field", ref.String()))
		}
	}

	for _, cond := range filterConditions {
		switch cond.Type {
		case influxql.Integer, influxql.String, influxql.Boolean, influxql.Float:
			{
				fieldType := cond.Type
				v := fieldMap.Get(cond.String())
				if v == nil {
					schema = append(schema, record.Field{Name: cond.Val, Type: record.ToModelTypes(fieldType)})
					fieldMap.Set(cond.String(), fieldType)
				}
			}
		case influxql.Tag:
			v := fieldMap.Get(cond.String())
			if v == nil {
				fieldMap.Set(cond.String(), cond.Type)
			}
		default:
			log.Warn("unknown column field type", zap.String("field", cond.String()))
		}
	}
	sort.Sort(schema)
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	return auxTags, schema
}
