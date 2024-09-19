// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package downsample

import (
	"math"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func downSampleQuerySchemaGen(sinfo *meta.ShardDownSamplePolicyInfo, infos *meta.RpMeasurementsFieldsInfo, policy *meta.DownSamplePolicyInfo) [][]hybridqp.Catalog {
	downSampleLevel := sinfo.DownSamplePolicyLevel
	currLvl := sinfo.Ident.DownSampleLevel
	var schemas [][]hybridqp.Catalog
	schemas = initDownSampleSchema(sinfo, infos, policy)
	if currLvl == 0 {
		return schemas
	}
	schemas = genNextLevelSchemas(schemas, downSampleLevel, policy.DownSamplePolicies)
	return schemas
}

func initDownSampleSchema(sinfo *meta.ShardDownSamplePolicyInfo, infos *meta.RpMeasurementsFieldsInfo, policy *meta.DownSamplePolicyInfo) [][]hybridqp.Catalog {
	downSampleLevel := sinfo.DownSamplePolicyLevel
	currLvl := sinfo.Ident.DownSampleLevel
	schemas := make([][]hybridqp.Catalog, downSampleLevel)
	var start int
	for i := range schemas {
		schemas[i] = make([]hybridqp.Catalog, 0, len(infos.MeasurementInfos))
	}
	if currLvl == 0 {
		start = downSampleLevel
	} else {
		start = currLvl
	}
	for i := 0; i < start-1; i++ {
		schemas[i] = append(schemas[i], make([]hybridqp.Catalog, len(infos.MeasurementInfos))...)
	}
	for _, info := range infos.MeasurementInfos {
		opt := genDefaultOpt(info.MstName)
		sources := []influxql.Source{
			&influxql.Measurement{
				Database:        sinfo.DbName,
				RetentionPolicy: sinfo.RpName,
				Name:            info.MstName,
			},
		}
		fields := make([]*influxql.Field, 0)
		columnNames := make([]string, 0)
		calls := policy.GetCalls()

		for _, f := range info.TypeFields {
			call, ok := calls[f.Type]
			if !ok {
				continue
			}

			field, columnName := downSampleExprGen(call, f.Fields, f.Type)
			columnNames = append(columnNames, columnName...)
			fields = append(fields, field...)
		}

		opt.Interval = hybridqp.Interval{
			Duration: policy.DownSamplePolicies[downSampleLevel-1].TimeInterval,
		}
		schemas[start-1] = append(schemas[start-1], executor.NewQuerySchemaWithSources(fields, sources, columnNames, opt, nil))
	}
	return schemas
}

func downSampleExprGen(calls []string, columns []string, dataType int64) (influxql.Fields, []string) {
	fields := make([]*influxql.Field, 0, len(calls)*len(columns))
	columnNames := make([]string, 0, len(calls)*len(columns))
	for _, c := range calls {
		for _, name := range columns {
			expr := &influxql.Call{
				Name: c,
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  name,
						Type: influxql.DataType(dataType),
					},
				},
			}
			fields = append(fields, &influxql.Field{
				Expr: expr,
			})
			columnNames = append(columnNames, c+"_"+name)
		}
	}
	return fields, columnNames
}

func genNextLevelSchemas(s [][]hybridqp.Catalog, downSampleLevel int, policy []*meta.DownSamplePolicy) [][]hybridqp.Catalog {
	schemas := make([][]hybridqp.Catalog, downSampleLevel)
	for i := range schemas {
		schemas[i] = make([]hybridqp.Catalog, 0, len(s[0]))
	}
	var start = downSampleLevel
	for i := 0; i < downSampleLevel; i++ {
		if i >= len(s) || len(s[i]) == 0 {
			start = i
			break
		}
		schemas[i] = append(schemas[i], s[i]...)
	}
	for i := start; i < downSampleLevel; i++ {
		for j := range s[i-1] {
			schemas[i] = append(schemas[i], genNextLevelSchema(s[i-1][j], policy[i-1].TimeInterval))
		}
	}
	return schemas
}

func genDefaultOpt(mstName string) *query.ProcessorOptions {
	return &query.ProcessorOptions{
		Name:           mstName,
		GroupByAllDims: true,
		Ascending:      true,
		ChunkSize:      httpd.DefaultInnerChunkSize,
		StartTime:      math.MinInt64,
		EndTime:        math.MaxInt64,
		HintType:       hybridqp.ExactStatisticQuery,
	}
}

func genNextLevelSchema(s hybridqp.Catalog, timeInterval time.Duration) hybridqp.Catalog {
	fields := s.GetQueryFields()
	renameFields := make([]*influxql.Field, len(fields))
	for i := range renameFields {
		f, _ := influxql.CloneExpr(fields[i].Expr).(*influxql.Call)
		callName := f.Name
		if f.Name == "count" {
			f.Name = "sum"
		}
		rewriteField(f, callName)
		renameFields[i] = &influxql.Field{
			Expr: f,
		}
	}
	columnNames := s.GetColumnNames()
	opt := genDefaultOpt(s.Options().OptionsName())
	opt.Interval = hybridqp.Interval{
		Duration: timeInterval,
	}
	return executor.NewQuerySchemaWithSources(renameFields, s.Sources(), columnNames, opt, nil)
}

func rewriteField(expr influxql.Expr, callName string) {
	c, ok := expr.(*influxql.Call)
	if !ok {
		return
	}
	for i := range c.Args {
		v, ok := c.Args[i].(*influxql.VarRef)
		if !ok {
			return
		}
		v.Val = callName + "_" + v.Val
		if callName == "count" {
			v.Type = influxql.Integer
		}
	}
}
