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

package executor_test

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	schema := executor.NewQuerySchema(createFields(), createColumnNames(), &opt, nil)

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
	schema := executor.NewQuerySchema(createCallFields(), createCallColumnNames(), &opt, nil)
	assert.Equal(t, schema.HasInSeriesAgg(), false)
	schema = executor.NewQuerySchema(createPercentileCallFields(), createCallColumnNames(), &opt, nil)
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
	schema := executor.NewQuerySchema(createCallFields(), createCallColumnNames(), &opt, nil)
	assert.Equal(t, schema.HasMath(), true)
	assert.Equal(t, schema.HasString(), true)
}

func TestHasAuxTags(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(createCallFields(), createCallColumnNames(), &opt, nil)
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
	}}, []string{"sum_age"}, &opt, nil)
	if i, err := schema.GetFieldType(0); i != int64(influxql.Integer) || err != nil {
		t.Fatal()
	}
}

func TestGetSortFields(t *testing.T) {
	schema := &executor.QuerySchema{}
	assert.Equal(t, len(schema.GetSortFields()), 0)

	opt := query.ProcessorOptions{}
	schema = executor.NewQuerySchema(nil, nil, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, len(schema.GetSortFields()), 1)
}

func createCall(call string) influxql.Fields {
	fields := make(influxql.Fields, 0, 1)
	fields = append(fields,
		&influxql.Field{Expr: &influxql.Call{
			Name: call,
			Args: []influxql.Expr{
				&influxql.VarRef{
					Val:  "time",
					Type: influxql.Integer,
				},
			},
		},
		},
	)
	return fields
}

func TestHasRowCount(t *testing.T) {
	fields := make(meta.CleanSchema)
	fields["pk_field"] = meta.SchemaVal{Typ: influx.Field_Type_String}
	fields["f2_int"] = meta.SchemaVal{Typ: influx.Field_Type_Int}
	mi := &meta.MeasurementInfo{
		Name:       "test_mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:          []string{"pk_field"},
			SortKey:             []string{},
			TimeClusterDuration: 0,
		},
		Schema: &fields,
	}
	ident := util.NewMeasurementIdent("db0", "rp0")
	ident.SetName("mst")
	colstore.MstManagerIns().Add(ident, mi)
	defer colstore.MstManagerIns().Clear()
	mockMst, _ := colstore.MstManagerIns().GetByIdent(ident)

	patch := gomonkey.ApplyMethodFunc(colstore.MstManagerIns(), "Get",
		func(db, rp, name string) (*colstore.Measurement, bool) {
			return mockMst, true
		})
	defer patch.Reset()

	opt := query.ProcessorOptions{HintType: hybridqp.ExactStatisticQuery}
	schema := executor.NewQuerySchema(nil, nil, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, schema.HasRowCount(), false)

	opt = query.ProcessorOptions{HintType: hybridqp.DefaultNoHint}
	schema = executor.NewQuerySchema(createCall("sum"), []string{"time"}, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, schema.HasRowCount(), false)

	opt = query.ProcessorOptions{HintType: hybridqp.DefaultNoHint, Condition: &influxql.BinaryExpr{}}
	schema = executor.NewQuerySchema(createCall("count"), []string{"time"}, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, schema.HasRowCount(), false)

	opt = query.ProcessorOptions{HintType: hybridqp.DefaultNoHint, Interval: hybridqp.Interval{Duration: 1 * time.Hour}}
	schema = executor.NewQuerySchema(createCall("count"), []string{"time"}, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, schema.HasRowCount(), false)

	opt = query.ProcessorOptions{HintType: hybridqp.DefaultNoHint, Dimensions: []string{"a", "b"}}
	schema = executor.NewQuerySchema(createCall("count"), []string{"time"}, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, schema.HasRowCount(), false)

	colMst := &influxql.Measurement{
		EngineType: config.COLUMNSTORE,
	}
	opt = query.ProcessorOptions{Sources: influxql.Sources{colMst}, HintType: hybridqp.DefaultNoHint, StartTime: influxql.MinTime, EndTime: influxql.MaxTime}
	schema = executor.NewQuerySchema(createCall("count"), []string{"time"}, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, schema.HasRowCount(), true)

	opt = query.ProcessorOptions{HintType: hybridqp.DefaultNoHint}
	opt.StartTime = 0
	schema = executor.NewQuerySchema(createCall("count"), []string{"time"}, &opt, []*influxql.SortField{{Name: "a"}})
	assert.Equal(t, schema.HasRowCount(), false)
}

func TestMeanAsSubCall(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(influxql.Fields{&influxql.Field{
		Expr: &influxql.ParenExpr{
			Expr: &influxql.Call{
				Name: "floor",
				Args: []influxql.Expr{&influxql.Call{Name: "mean", Args: []influxql.Expr{&influxql.VarRef{Val: "f1", Type: influxql.Integer}}}},
			},
		},
	}}, []string{"floor_val"}, &opt, nil)
	hasSum := false
	for k, _ := range schema.Mapping() {
		if c, ok := k.(*influxql.Call); ok {
			if c.Name == "sum" {
				hasSum = true
				break
			}
		}
	}
	if !hasSum {
		t.Fatal("TestMeanAsSubCall fail")
	}
}

func TestGetTimeRangeByTC(t *testing.T) {
	indexR := influxql.NewIndexRelation()

	// default interval is 1h
	opt := query.ProcessorOptions{Sources: []influxql.Source{&influxql.Measurement{Name: "mst", IndexRelation: indexR}}, StartTime: influxql.MinTime, EndTime: 5001}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	assert.Equal(t, schema.GetTimeRangeByTC(), util.TimeRange{Min: influxql.MinTime, Max: 0})

	opt = query.ProcessorOptions{Sources: []influxql.Source{&influxql.Measurement{Name: "mst", IndexRelation: indexR}}, StartTime: int64(colstore.DefaultTCDuration) - 1, EndTime: int64(colstore.DefaultTCDuration) + 1}
	schema = executor.NewQuerySchema(nil, nil, &opt, nil)
	assert.Equal(t, schema.GetTimeRangeByTC(), util.TimeRange{Min: 0, Max: int64(colstore.DefaultTCDuration)})

	indexR.Oids = append(indexR.Oids, uint32(index.TimeCluster))
	indexR.IndexOptions = append(indexR.IndexOptions, &influxql.IndexOptions{
		Options: []*influxql.IndexOption{
			{TimeClusterDuration: time.Duration(5000)},
		},
	})

	// min interval is 10mins
	opt = query.ProcessorOptions{Sources: []influxql.Source{&influxql.Measurement{Name: "mst", IndexRelation: indexR}}, StartTime: int64(colstore.MinTCDuration)*3 + 2, EndTime: int64(colstore.MinTCDuration)*8 - 1}
	schema = executor.NewQuerySchema(nil, nil, &opt, nil)
	assert.Equal(t, schema.GetTimeRangeByTC(), util.TimeRange{Min: int64(colstore.MinTCDuration) * 3, Max: int64(colstore.MinTCDuration) * 7})

	// test cluster time: 15m20s
	testInterval := int64(time.Minute*15 + time.Second*20)
	indexR.IndexOptions[0] = &influxql.IndexOptions{
		Options: []*influxql.IndexOption{
			{TimeClusterDuration: time.Duration(testInterval)},
		},
	}
	opt = query.ProcessorOptions{Sources: []influxql.Source{&influxql.Measurement{Name: "mst", IndexRelation: indexR}}, StartTime: testInterval * 18, EndTime: testInterval*180 + int64(time.Minute*15)}
	schema = executor.NewQuerySchema(nil, nil, &opt, nil)
	assert.Equal(t, schema.GetTimeRangeByTC(), util.TimeRange{Min: testInterval * 18, Max: testInterval * 180})
}

func TestCanSeqAggPushDown(t *testing.T) {
	opt := query.ProcessorOptions{Sources: []influxql.Source{&influxql.Measurement{Name: "mst"}}, StartTime: influxql.MinTime, EndTime: 5001}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	assert.Equal(t, schema.CanSeqAggPushDown(), false)
}

func TestIsPromAbsentCall(t *testing.T) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(influxql.Fields{&influxql.Field{
		Expr: &influxql.ParenExpr{
			Expr: &influxql.Call{
				Name: "absent_prom",
				Args: []influxql.Expr{&influxql.Call{Name: "absent_prom", Args: []influxql.Expr{}}},
			},
		},
	}}, []string{"metric"}, &opt, nil)
	assert.Equal(t, schema.IsPromAbsentCall(), true)
}

func TestIsColumnStoreCount(t *testing.T) {
	countTimeCall := &influxql.Call{
		Name: "count",
		Args: []influxql.Expr{&influxql.VarRef{Val: "time"}},
	}
	countTimeExpr := &influxql.Field{
		Expr: countTimeCall,
	}
	sumTimeCall := &influxql.Call{
		Name: "sum",
		Args: []influxql.Expr{&influxql.VarRef{Val: "time"}},
	}
	sumTimeExpr := &influxql.Field{
		Expr: sumTimeCall,
	}
	fieldMatchCondition := &influxql.BinaryExpr{
		Op:  influxql.MATCH,
		LHS: &influxql.VarRef{Val: "field"},
	}
	fieldTextMatchCondition := &influxql.BinaryExpr{
		Op:  influxql.MATCH,
		LHS: &influxql.VarRef{Val: "field_text"},
	}
	andCondition := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: fieldMatchCondition,
		RHS: fieldTextMatchCondition,
	}

	for _, test := range []struct {
		name       string
		engineType config.EngineType
		fields     influxql.Fields
		call       *influxql.Call
		condition  influxql.Expr
		dimensions []string
		expected   bool
	}{
		{
			name:       "select count(time) from ts",
			engineType: config.TSSTORE,
			fields:     influxql.Fields{countTimeExpr},
			call:       countTimeCall,
			condition:  nil,
			dimensions: nil,
			expected:   false,
		},
		{
			name:       "select sum(time) from ts",
			engineType: config.TSSTORE,
			fields:     influxql.Fields{sumTimeExpr},
			call: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{&influxql.VarRef{Val: "time"}},
			},
			condition:  nil,
			dimensions: nil,
			expected:   false,
		},
		{
			name:       "select count(time), sum(time) from ts",
			engineType: config.TSSTORE,
			fields:     influxql.Fields{countTimeExpr, sumTimeExpr},
			call:       sumTimeCall,
			condition:  nil,
			dimensions: nil,
			expected:   false,
		},
		{
			name:       "select count(time) from cs",
			engineType: config.COLUMNSTORE,
			fields:     influxql.Fields{countTimeExpr},
			call:       countTimeCall,
			condition:  nil,
			dimensions: nil,
			expected:   false,
		},
		{
			name:       "select count(time) from cs group by field",
			engineType: config.COLUMNSTORE,
			fields:     influxql.Fields{countTimeExpr},
			call:       countTimeCall,
			condition:  nil,
			dimensions: []string{"field"},
			expected:   false,
		},
		{
			name:       "select count(time) from cs where match(field, 'xxx')",
			engineType: config.COLUMNSTORE,
			fields:     influxql.Fields{countTimeExpr},
			call:       countTimeCall,
			condition:  fieldMatchCondition,
			dimensions: nil,
			expected:   false,
		},
		{
			name:       "select count(time) from cs where match(field_text, 'xxx')",
			engineType: config.COLUMNSTORE,
			fields:     influxql.Fields{countTimeExpr},
			call:       countTimeCall,
			condition:  fieldTextMatchCondition,
			expected:   true,
		},
		{
			name:       "select count(time) from cs where match(field, 'xxx') and match(field_text, 'xxx')",
			engineType: config.COLUMNSTORE,
			fields:     influxql.Fields{countTimeExpr},
			call:       countTimeCall,
			condition:  andCondition,
			dimensions: nil,
			expected:   false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mst := &influxql.Measurement{
				EngineType: test.engineType,
				IndexRelation: &influxql.IndexRelation{
					IndexNames: []string{"text"},
					IndexList:  []*influxql.IndexList{{IList: []string{"field_text"}}},
				},
			}
			opt := query.ProcessorOptions{
				Condition:  test.condition,
				Sources:    influxql.Sources{mst},
				Dimensions: test.dimensions,
			}
			schema := executor.NewQuerySchema(test.fields, []string{"field", "field_text"}, &opt, nil)
			schema.RecordExpr(test.call)
			schema.SetSources(influxql.Sources{mst})

			res := schema.IsColumnStoreCount()
			if test.expected != res {
				t.Errorf("`%s` failed, expect %v, but got %v", test.name, test.expected, res)
			}
		})
	}
}

// TestSetResource tests the SetResource/GetResource method with various scenarios.
func TestSetResource(t *testing.T) {
	testCases := []struct {
		name            string
		initialResource map[string]map[string]string
		resourceName    string
		resource        map[string]string
	}{
		{
			name:            "Test when resource is empty",
			initialResource: make(map[string]map[string]string),
			resourceName:    "test",
			resource:        make(map[string]string),
		},
		{
			name:            "Test when resource is non-empty",
			initialResource: make(map[string]map[string]string),
			resourceName:    "test",
			resource:        map[string]string{"key": "value"},
		},
		{
			name:            "Test when key already exists",
			initialResource: map[string]map[string]string{"test": {"key": "value"}},
			resourceName:    "test",
			resource:        map[string]string{"newKey": "newValue"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qs := &executor.QuerySchema{}
			qs.SetResource(tc.resourceName, tc.resource)

			// Verify the resource was set correctly
			assert.Equal(t, qs.GetResource(tc.resourceName), tc.resource)
		})
	}
}

func testIsLastBaseQuery(t *testing.T, fields influxql.Fields, f func(schema hybridqp.Catalog) bool) {
	opt := query.ProcessorOptions{}
	schema := executor.NewQuerySchema(nil, []string{"value"}, &opt, nil)
	assert.Equal(t, f(schema), false)

	schema.Options().(*query.ProcessorOptions).HintType = hybridqp.FullSeriesQuery
	assert.Equal(t, f(schema), false)

	opt = query.ProcessorOptions{
		HintType: hybridqp.FullSeriesQuery,
		Interval: hybridqp.Interval{Duration: time.Hour},
	}
	schema = executor.NewQuerySchema(fields, []string{"value"}, &opt, nil)
	assert.Equal(t, f(schema), false)

	schema.Options().(*query.ProcessorOptions).Interval = hybridqp.Interval{}
	assert.Equal(t, f(schema), false)

	schema.Options().(*query.ProcessorOptions).Condition = &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Float},
		RHS: &influxql.NumberLiteral{},
	}
	assert.Equal(t, f(schema), false)

	schema.Options().(*query.ProcessorOptions).Condition = nil
	assert.Equal(t, f(schema), false)

	schema.Options().(*query.ProcessorOptions).GroupByAllDims = true
	assert.Equal(t, f(schema), true)
}

func TestIsLastRowQuery(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "last_row",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "val1",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	f := func(catalog hybridqp.Catalog) bool {
		return catalog.IsLastRowQuery()
	}
	testIsLastBaseQuery(t, fields, f)
}

func TestIsLastFieldQuery(t *testing.T) {
	fields := influxql.Fields{
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "last",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "val1",
						Type: influxql.Float,
					},
				},
			},
		},
	}
	f := func(catalog hybridqp.Catalog) bool {
		return catalog.IsLastFieldQuery()
	}
	testIsLastBaseQuery(t, fields, f)
}

// TestIsOnlyCSPreAgg tests the IsOnlyCSPreAgg method which checks if a query meets the conditions for column store pre-aggregation.
func TestIsOnlyCSPreAgg(t *testing.T) {
	fields := make(meta.CleanSchema)
	fields["pk_field"] = meta.SchemaVal{Typ: influx.Field_Type_String}
	fields["f2_int"] = meta.SchemaVal{Typ: influx.Field_Type_Int}
	mi := &meta.MeasurementInfo{
		Name:       "test_mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:          []string{"pk_field"},
			SortKey:             []string{},
			TimeClusterDuration: 0,
		},
		Schema: &fields,
	}
	mi.ColStoreInfo.BuildProperty()
	ident := util.NewMeasurementIdent("db0", "rp0")
	ident.SetName("mst")
	colstore.MstManagerIns().Add(ident, mi)
	defer colstore.MstManagerIns().Clear()
	mockMst, _ := colstore.MstManagerIns().GetByIdent(ident)

	patch := gomonkey.ApplyMethodFunc(colstore.MstManagerIns(), "Get",
		func(db, rp, name string) (*colstore.Measurement, bool) {
			return mockMst, true
		})
	defer patch.Reset()

	tests := []struct {
		name        string
		opt         *query.ProcessorOptions
		expected    bool
		fields      influxql.Fields
		columnNames []string
	}{
		{
			name: "ExactStatisticQuery",
			opt: &query.ProcessorOptions{
				HintType: hybridqp.ExactStatisticQuery,
			},
			expected: false,
		},
		{
			name: "Invalid sources number",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{},
					&influxql.Measurement{},
				},
			},
			expected: false,
		},
		{
			name: "Invalid engine type (not column store)",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{EngineType: config.TSSTORE},
				},
			},
			expected: false,
		},
		{
			name: "Invalid schema with dimensions",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{EngineType: config.COLUMNSTORE},
				},
				Dimensions: []string{"t1"},
			},
			expected: false,
		},
		{
			name: "Invalid schema with interval",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{EngineType: config.COLUMNSTORE},
				},
				Interval: hybridqp.Interval{Duration: 1},
			},
			expected: false,
		},
		{
			name: "Invalid schema with invalid call",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{EngineType: config.COLUMNSTORE},
				},
			},
			fields: []*influxql.Field{
				{
					Expr: &influxql.Call{
						Name: "first",
						Args: []influxql.Expr{
							&influxql.VarRef{
								Val:  "f1",
								Type: influxql.Integer,
							},
						},
					},
					Alias: "",
				},
			},
			columnNames: []string{"f1"},
			expected:    false,
		},
		{
			name: "Valid schema with invalid call",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{EngineType: config.COLUMNSTORE},
				},
			},
			fields: []*influxql.Field{
				{
					Expr: &influxql.Call{
						Name: "min",
						Args: []influxql.Expr{
							&influxql.VarRef{
								Val:  "f1",
								Type: influxql.Integer,
							},
						},
					},
					Alias: "",
				},
				{
					Expr: &influxql.Call{
						Name: "max",
						Args: []influxql.Expr{
							&influxql.VarRef{
								Val:  "f2",
								Type: influxql.Integer,
							},
						},
					},
					Alias: "",
				},
			},
			columnNames: []string{"f1", "f2"},
			expected:    false,
		},
		{
			name: "Invalid schema with invalid condition",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{EngineType: config.COLUMNSTORE},
				},
				Condition: &influxql.BinaryExpr{
					LHS: &influxql.VarRef{Val: "A", Type: influxql.String},
					RHS: &influxql.StringLiteral{Val: "a"},
					Op:  influxql.EQ},
			},
			fields: []*influxql.Field{
				{
					Expr: &influxql.Call{
						Name: "count",
						Args: []influxql.Expr{
							&influxql.VarRef{
								Val:  "f1",
								Type: influxql.String,
							},
						},
					},
					Alias: "",
				},
			},
			columnNames: []string{"f1"},
			expected:    false,
		},
		{
			name: "Valid schema with invalid only pk condition",
			opt: &query.ProcessorOptions{
				Sources: []influxql.Source{
					&influxql.Measurement{EngineType: config.COLUMNSTORE},
				},
				Condition: &influxql.BinaryExpr{
					LHS: &influxql.VarRef{Val: "pk_field", Type: influxql.String},
					RHS: &influxql.StringLiteral{Val: "a"},
					Op:  influxql.EQ},
				StartTime: influxql.MinTime, EndTime: influxql.MaxTime,
			},
			fields: []*influxql.Field{
				{
					Expr: &influxql.Call{
						Name: "count",
						Args: []influxql.Expr{
							&influxql.VarRef{
								Val:  "pk_field",
								Type: influxql.String,
							},
						},
					},
					Alias: "",
				},
			},
			columnNames: []string{"pk_field"},
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a minimal QuerySchema for testing
			qs := executor.NewQuerySchema(tt.fields, tt.columnNames, tt.opt, nil)

			result := qs.IsOnlyCSPreAgg()
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestCalculateQueryTagFromSymbol tests the CalculateQueryTagFromSymbol method.
func TestCalculateQueryTagFromSymbol(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(*executor.QuerySchema) *executor.QuerySchema
		symbol       string
		expected     string
		expectError  bool
		errorMessage string
	}{
		{
			name: "Valid symbol with aggregation function",
			setup: func(qs *executor.QuerySchema) *executor.QuerySchema {
				// Set up a mapping with a call expression
				expr1 := &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Float}}}
				ref1 := &influxql.VarRef{Val: "val0", Type: influxql.Float}
				expr2 := &influxql.VarRef{Val: "f1", Type: influxql.Float}
				ref2 := &influxql.VarRef{Val: "val1", Type: influxql.Float}
				qs.Mapping()[expr1] = *ref1
				qs.Mapping()[expr2] = *ref2
				return qs
			},
			symbol:       "val0",
			expected:     "f1_sum",
			expectError:  false,
			errorMessage: "",
		},
		{
			name: "Valid symbol with simple variable reference",
			setup: func(qs *executor.QuerySchema) *executor.QuerySchema {
				// Set up a mapping with a variable reference
				expr := &influxql.VarRef{Val: "value", Type: influxql.Float}
				ref := &influxql.VarRef{Val: "val0", Type: influxql.Float}
				qs.Mapping()[expr] = *ref
				return qs
			},
			symbol:       "val0",
			expected:     "value",
			expectError:  false,
			errorMessage: "",
		},
		{
			name: "Symbol not found in mapping",
			setup: func(qs *executor.QuerySchema) *executor.QuerySchema {
				// Set up an empty mapping
				return qs
			},
			symbol:       "nonexistent",
			expected:     "",
			expectError:  true,
			errorMessage: "symbol 'nonexistent' not found in query schema mapping",
		},
		{
			name: "Call expression with wrong argument count",
			setup: func(qs *executor.QuerySchema) *executor.QuerySchema {
				// Set up a mapping with a call that has wrong number of arguments
				expr := &influxql.Call{Name: "sum", Args: []influxql.Expr{
					&influxql.VarRef{Val: "value", Type: influxql.Float},
					&influxql.VarRef{Val: "value2", Type: influxql.Float},
				}}
				ref := &influxql.VarRef{Val: "val0", Type: influxql.Float}
				qs.Mapping()[expr] = *ref
				return qs
			},
			symbol:       "val0",
			expected:     "",
			expectError:  true,
			errorMessage: "call expression for symbol 'val0' has 2 arguments, expected exactly 1",
		},
		{
			name: "Call argument is not a variable reference",
			setup: func(qs *executor.QuerySchema) *executor.QuerySchema {
				// Set up a mapping with a call that has a non-VarRef argument
				expr := &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.StringLiteral{Val: "not_a_var"}}}
				ref := &influxql.VarRef{Val: "val0", Type: influxql.Float}
				qs.Mapping()[expr] = *ref
				return qs
			},
			symbol:       "val0",
			expected:     "",
			expectError:  true,
			errorMessage: "the argument of call expression for symbol 'val0' is not a variable reference, got *influxql.StringLiteral",
		},
		{
			name: "Referenced symbol not found",
			setup: func(qs *executor.QuerySchema) *executor.QuerySchema {
				// Set up a mapping with a call that references a non-existent symbol
				expr := &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "value", Type: influxql.Float}}}
				ref := &influxql.VarRef{Val: "val0", Type: influxql.Float}
				qs.Mapping()[expr] = *ref
				qs.Symbols()["val0"] = *ref
				// Intentionally don't add "value" to the symbol-to-expr mapping
				return qs
			},
			symbol:       "val0",
			expected:     "",
			expectError:  true,
			errorMessage: "referenced symbol 'value' not found in query schema mapping",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a minimal QuerySchema for testing
			opt := &query.ProcessorOptions{}
			qs := executor.NewQuerySchema(nil, nil, opt, nil)

			// Apply test-specific setup
			qs = tt.setup(qs)

			result, err := qs.CalculateQueryTagFromSymbol(tt.symbol)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
