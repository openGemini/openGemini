// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"sort"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
	"go.uber.org/zap"
)

func NewRecordSchema(querySchema *executor.QuerySchema, auxTags []string, schema record.Schemas, filterConditions []*influxql.VarRef, engineType config.EngineType) ([]string, record.Schemas) {
	fieldMap := dictpool.Dict{}
	for key, ref := range querySchema.Refs() {
		genSchemaByRef(&fieldMap, key, ref, &schema, &auxTags, engineType, true)
	}
	for _, cond := range filterConditions {
		genSchemaByRef(&fieldMap, cond.String(), cond, &schema, &auxTags, engineType, false)
	}
	sort.Sort(schema)
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	return auxTags, schema
}

func genSchemaByRef(fieldMap *dictpool.Dict, key string, ref *influxql.VarRef, schema *record.Schemas, auxTags *[]string, engineType config.EngineType, queryRef bool) {
	switch ref.Type {
	case influxql.Integer, influxql.String, influxql.Boolean, influxql.Float:
		{
			fieldType := ref.Type
			v := fieldMap.Get(key)
			if v == nil {
				*schema = append(*schema, record.Field{Name: ref.Val, Type: record.ToModelTypes(fieldType)})
				fieldMap.Set(key, fieldType)
			}
		}
	case influxql.Tag:
		v := fieldMap.Get(key)
		if v == nil {
			fieldMap.Set(key, ref.Type)
			if engineType == config.COLUMNSTORE {
				*schema = append(*schema, record.Field{Name: ref.Val, Type: record.ToModelTypes(ref.Type)})
			} else if queryRef {
				*auxTags = append(*auxTags, ref.Val)
			}
		}
	default:
		log.Warn("unknown column field type", zap.String("field", ref.String()))
	}
}
