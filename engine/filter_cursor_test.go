// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package engine

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type cursor struct {
	closeErr         bool
	nextAggData      *record.Record
	plan             hybridqp.QueryNode
	ops              []*comm.CallOption
	schemas          record.Schemas
	nextAggDataErr   error
	notReturnAggData bool
}

func (c *cursor) StartSpan(span *tracing.Span) {
}

func (c *cursor) EndSpan() {
}

func (c *cursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if c.notReturnAggData {
		return nil, nil, nil
	} else {
		c.notReturnAggData = true
		return c.nextAggData, nil, c.nextAggDataErr
	}
}

func (c *cursor) SetOps(ops []*comm.CallOption) {
	c.ops = ops
}

func (c *cursor) SinkPlan(plan hybridqp.QueryNode) {
	c.plan = plan
}

func (c *cursor) GetSchema() record.Schemas {
	return c.schemas
}

func (c *cursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return nil, nil, nil
}

func (c *cursor) Name() string {
	return ""
}

func (c *cursor) Close() error {
	if !c.closeErr {
		return nil
	}
	return fmt.Errorf("cursor close err")
}

func Test_NewFilterCursor(t *testing.T) {
	var tagSets []tsi.TagSet
	for i := 0; i < 3; i++ {
		tagSetInfo := &tsi.TagSetInfo{}
		tagSets = append(tagSets, tagSetInfo)
	}
	opt := query.ProcessorOptions{
		ValueCondition: &influxql.BinaryExpr{
			Op: influxql.LTE,
			LHS: &influxql.VarRef{
				Val:  "value",
				Type: influxql.Float,
			},
			RHS: &influxql.NumberLiteral{
				Val: 1,
			},
		},
	}
	querySchema := executor.NewQuerySchema(nil, []string{"a"}, &opt, nil)
	schemas := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "value"},
	}
	filterCursor, err := NewFilterCursor(querySchema, tagSets, &cursor{}, schemas, immutable.BaseFilterOptions{})

	if filterCursor == nil || err != nil {
		t.Errorf("NewFilterCursor fail")
	}
	//schemas field name not in condition
	schemas1 := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "value1"},
	}
	filterCursor1, err := NewFilterCursor(querySchema, tagSets, &cursor{}, schemas1, immutable.BaseFilterOptions{})
	if filterCursor1 != nil {
		t.Errorf("NewFilterCursor should fail")
	}
}

func Test_FilterCursor_NextAggData(t *testing.T) {
	msNames := []string{"cpu"}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "field1_string"},
		record.Field{Type: influx.Field_Type_Int, Name: "field2_int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "field3_bool"},
		record.Field{Type: influx.Field_Type_Float, Name: "field4_float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 20, 19})
	minTime, maxTime := int64(0), int64(23)

	cursor1 := &cursor{}
	fc1 := &FilterCursor{cursor: cursor1}
	nextAggData1, info1, err1 := fc1.NextAggData()
	if nextAggData1 != nil || info1 != nil || err1 != nil {
		t.Errorf("FilterCursor NextAggData should return nil nil nil")
	}

	cursor2 := &cursor{}
	cursor2.nextAggDataErr = errors.New("nextAggDataErr")
	cursor2.nextAggData = rec
	fc2 := &FilterCursor{cursor: cursor2}

	nextAggData2, info2, err2 := fc2.NextAggData()
	if nextAggData2 != nil || info2 != nil || err2 == nil {
		t.Errorf("FilterCursor NextAggData with FilterCursor.cursor.NextAggData() error")
	}

	for nameIdx := range msNames {
		cases := []TestCase{
			{"AllFieldLTCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int < 1100 AND field1_string < 'hb' OR field4_float < 1002", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTCondition01", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field2_int >=1000 OR field1_string > 'ha' AND field4_float < 1002", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"AllFieldLTCondition02", influxql.MinTime, influxql.MaxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int < 1100 AND field1_string < 'hb' OR field4_float < 1002", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTCondition03", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field2_int < 1100 OR field1_string > 'ha' AND field2_int <= 1000", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 0},
					[]int{1, 1}, []float64{1002.4, 1002.4},
					[]int{1, 1}, []string{"hb", "hc"},
					[]int{1, 1}, []bool{true, false},
					[]int64{21, 20})},
			{"AllFieldLTCondition04", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field2_int < 1100 OR field1_string > 'ha' AND field2_int <= 1000", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 0},
					[]int{1, 1}, []float64{1002.4, 1002.4},
					[]int{1, 1}, []string{"hb", "hc"},
					[]int{1, 1}, []bool{true, false},
					[]int64{21, 20})},
			{"AllFieldLTCondition05", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field4_float > 1000 OR field1_string > 'hb' AND field4_float >= 1000", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 0},
					[]int{1, 1}, []float64{1002.4, 1002.4},
					[]int{1, 1}, []string{"hb", "hc"},
					[]int{1, 1}, []bool{true, false},
					[]int64{21, 20})},
			{"AllFieldLTConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1000 < field2_int AND ('hb' < field1_string OR 1002 < field4_float)", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"AllFieldLTECondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int <= 1100 AND field1_string <= 'hb' OR field4_float <= 1002.4", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
					[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
					[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
					[]int{1, 1, 1, 1}, []bool{true, true, false, false},
					[]int64{22, 21, 20, 19})},
			{"AllFieldLTEConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1000 <= field2_int AND 'hb' <= field1_string OR  1002.4 <= field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{1, 1, 1}, []float64{1002.4, 1002.4, 0},
					[]int{1, 1, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
			{"AllFieldGTCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int > 1100 AND field1_string > 'hb' OR field4_float > 1002.4", nil, false, nil},
			{"AllFieldGTConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1100 > field2_int AND 'hb' > field1_string OR 1002.4 > field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldGTECondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int >= 1100 AND field1_string >= 'hb' OR field4_float >= 1002.4", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{1, 1, 1}, []float64{1002.4, 1002.4, 0},
					[]int{1, 1, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
			{"AllFieldGTEConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"1100 >= field2_int AND 'hb' >= field1_string OR 1002.4 >= field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
					[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
					[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
					[]int{1, 1, 1, 1}, []bool{true, true, false, false},
					[]int64{22, 21, 20, 19})},
			{"AllFieldEQCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int = 0 AND field1_string = 'hb' OR field4_float = 1002.4 AND field3_bool = true", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{0},
					[]int{1}, []float64{1002.4},
					[]int{1}, []string{"hb"},
					[]int{1}, []bool{true},
					[]int64{21})},
			{"AllFieldNEQCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int != 1000 AND field1_string != 'hd' OR field4_float != 1002.4 AND field3_bool != true", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{1, 1, 1}, []float64{1002.4, 1002.4, 0},
					[]int{1, 1, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
			{"IntFieldEQFloatCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int = 1000.1", nil, false,
				genRowRec(schema,
					[]int{}, []int64{},
					[]int{}, []float64{},
					[]int{}, []string{},
					[]int{}, []bool{},
					[]int64{})},
			{"IntFieldEQFloatCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int != 1000.1", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
					[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
					[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
					[]int{1, 1, 1, 1}, []bool{true, true, false, false},
					[]int64{22, 21, 20, 19})},
			{"IntFieldGTEFloatCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int >= 1000.1", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"IntFieldLTEFloatCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int <= 1000.1", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{1000, 0, 0},
					[]int{1, 1, 1}, []float64{1001.3, 1002.4, 1002.4},
					[]int{1, 1, 1}, []string{"ha", "hb", "hc"},
					[]int{1, 1, 1}, []bool{true, true, false},
					[]int64{22, 21, 20})},
			{"FloatFieldEQIntCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field4_float = 0", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"FloatFieldGTIntCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field4_float > 0", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{1000, 0, 0},
					[]int{1, 1, 1}, []float64{1001.3, 1002.4, 1002.4},
					[]int{1, 1, 1}, []string{"ha", "hb", "hc"},
					[]int{1, 1, 1}, []bool{true, true, false},
					[]int64{22, 21, 20})},
			{"FloatFieldLTIntCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field4_float <1000", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
		}
		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				var filterConditions []*influxql.VarRef
				var err error
				filterConditions, err = getFilterFieldsByExpr(querySchema.Options().GetCondition(), filterConditions[:0])
				if err != nil {
					t.Fatal(err)
				}
				queryCtx := &QueryCtx{}
				queryCtx.auxTags, queryCtx.schema = NewRecordSchema(querySchema, queryCtx.auxTags[:0], queryCtx.schema[:0], filterConditions, config.TSSTORE)
				if queryCtx.auxTags == nil && queryCtx.schema.Len() <= 1 {
					return
				}
				queryCtx.filterFieldsIdx = queryCtx.filterFieldsIdx[:0]
				queryCtx.filterTags = queryCtx.filterTags[:0]
				for _, f := range filterConditions {
					idx := queryCtx.schema.FieldIndex(f.Val)
					if idx >= 0 && f.Type != influxql.Unknown {
						queryCtx.filterFieldsIdx = append(queryCtx.filterFieldsIdx, idx)
					} else if f.Type != influxql.Unknown {
						queryCtx.filterTags = append(queryCtx.filterTags, f.Val)
					}
				}

				var tagSets []tsi.TagSet
				for i := 0; i < 3; i++ {
					tagSetInfoItems := make([]tsi.TagSetInfoItem, 0)
					item := tsi.TagSetInfoItem{
						Filter: querySchema.Options().GetCondition(),
					}
					tagSetInfoItems = append(tagSetInfoItems, item)
					tagSetInfo := &tsi.TagSetInfo{
						TagSetInfoItems: tagSetInfoItems,
					}
					tagSets = append(tagSets, tagSetInfo)
				}
				querySchema.Options().SetValueCondition(querySchema.Options().GetCondition())
				filterOption := &immutable.BaseFilterOptions{}
				filterCursor, err := NewFilterCursor(querySchema, tagSets, &cursor{nextAggData: rec}, queryCtx.schema, *filterOption)
				filterCursor.recPool = record.NewCircularRecordPool(SeriesLoopPool, seriesLoopCursorRecordNum, queryCtx.schema, false)

				if filterCursor == nil || err != nil {
					t.Errorf("NewFilterCursor fail")
				}

				newRecord, _, err := filterCursor.NextAggData()

				if err != nil {
					t.Fatal("error filterCursor NextAggData")
				}

				if newRecord != nil && c.expRecord != nil {
					if !testRecsEqual(newRecord, c.expRecord) {
						t.Fatal("error result")
					}
				}
			})
		}
	}
}

func Test_FilterCursor_StartSpan(t *testing.T) {
	fc1 := &FilterCursor{cursor: &cursor{}}
	span := &tracing.Span{}
	fc1.StartSpan(span)
	if fc1.span != span {
		t.Errorf("Expected span to be set, but it was not")
	}
	fc2 := &FilterCursor{cursor: &cursor{}}
	fc2.StartSpan(nil)
	if fc2.span != nil {
		t.Errorf("Expected span is nil, but it was not")
	}
}

func Test_FilterCursor_SetOps(t *testing.T) {
	cursor := &cursor{}
	fc := &FilterCursor{cursor: cursor}
	ops := []*comm.CallOption{{}, {}}
	fc.SetOps(ops)
	if !reflect.DeepEqual(cursor.ops, ops) {
		t.Errorf("Expected ops to be set, but it was not")
	}
}

func Test_FilterCursor_SinkPlan(t *testing.T) {
	cursor := &cursor{}
	fc := &FilterCursor{cursor: cursor}
	plan := &executor.LogicalSeries{}
	fc.SinkPlan(plan)
	if cursor.plan != plan {
		t.Errorf("Expected plan to be sink, but it was not")
	}
}

func Test_FilterCursor_Next(t *testing.T) {
	fc := &FilterCursor{}
	rec, series, err := fc.Next()

	if rec != nil || series != nil || err != nil {
		t.Errorf("FilterCursor Fail")
	}
}

func Test_FilterCursor_Name(t *testing.T) {
	fc := &FilterCursor{}
	if fc.Name() != "filterCursor" {
		t.Errorf("Expected name to be 'filterCursor', but it was not")
	}
}

func TestFilterCursorClose(t *testing.T) {
	cursor := &cursor{closeErr: false}
	fc := &FilterCursor{cursor: cursor}
	err := fc.Close()
	if err != nil {
		t.Errorf("Expected no error, but got one")
	}
}

func Test_FilterCursor_GetSchema(t *testing.T) {
	schemas := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: record.TimeField},
		record.Field{Type: influx.Field_Type_String, Name: "value"},
	}
	cursor := &cursor{schemas: schemas}
	fc := &FilterCursor{cursor: cursor}
	filterSchemas := fc.GetSchema()
	if len(filterSchemas) != len(cursor.schemas) {
		t.Errorf("Expected schema to be get, but it was not")
	}
	for i, field := range schemas {
		filterSchemaField := filterSchemas[i]
		if filterSchemaField.Type != field.Type || filterSchemaField.Name != field.Name {
			t.Errorf("Expected schema to be get, but it was not")
		}
	}
}
