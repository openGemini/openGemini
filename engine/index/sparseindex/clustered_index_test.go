// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package sparseindex

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	ci := NewClusterIndex()

	// Test single field indexing
	fields := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Alice2",
		},
	}
	segNumID := uint32(1)
	err := ci.Write(segNumID, fields)
	require.NoError(t, err)

	fields = map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Alice1",
		},
	}
	segNumID = uint32(2)
	err = ci.Write(segNumID, fields)
	require.NoError(t, err)

	// Verify whether the index has been correctly added.
	fieldCluster, exists := ci.fieldClusters["name"]
	assert.True(t, exists)

	segments, exists := fieldCluster.ValueToSegments["Alice2"]
	assert.True(t, exists)
	assert.True(t, segments.Contains(uint32(1)))

	segments, exists = fieldCluster.ValueToSegments["Alice1"]
	assert.True(t, exists)
	assert.True(t, segments.Contains(uint32(2)))

	expectSortValues := []interface{}{"Alice1", "Alice2"}
	assert.Equal(t, expectSortValues, fieldCluster.SortedValues, "the value of SortedValues is not expected")
}

func TestQuery(t *testing.T) {
	ci := NewClusterIndex()

	// Add Test Data
	fields0 := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "David",
		},
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(35),
		},
	}
	assert.NoError(t, ci.Write(0, fields0))

	fields1 := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Alice",
		},
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(30),
		},
	}
	assert.NoError(t, ci.Write(1, fields1))

	fields2 := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Bob",
		},
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(25),
		},
	}
	assert.NoError(t, ci.Write(2, fields2))

	fields3 := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Bob",
		},
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(20),
		},
	}
	assert.NoError(t, ci.Write(3, fields3))

	fields4 := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Casey",
		},
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(20),
		},
	}
	assert.NoError(t, ci.Write(4, fields4))

	fields5 := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Alice",
		},
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(25),
		},
	}
	assert.NoError(t, ci.Write(5, fields5))

	// Test Equivalent Query
	expr := influxql.MustParseExpr("\"name\" = 'Alice'")
	result, err := ci.Query(expr)
	require.NoError(t, err)
	expectSegIDs := []uint32{1, 5}
	assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where name='Alice' should be {1, 5}")

	expr = influxql.MustParseExpr("age = 21")
	result, err = ci.Query(expr)
	require.NoError(t, err)
	expectSegIDs = []uint32{}
	assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where age = 21 should be {}")

	expr = influxql.MustParseExpr("age = 20")
	result, err = ci.Query(expr)
	require.NoError(t, err)
	expectSegIDs = []uint32{3, 4}
	assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where age = 20 should be {3, 4}")

	expr = influxql.MustParseExpr("age != 25")
	result, err = ci.Query(expr)
	require.NoError(t, err)
	expectSegIDs = []uint32{0, 1, 3, 4}
	assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where != 25 should be {0, 1, 3, 4}")

	expr = influxql.MustParseExpr("\"name\" >= 'Bob'")
	result, err = ci.Query(expr)
	require.NoError(t, err)
	expectSegIDs = []uint32{0, 2, 3, 4}
	assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where name>='Bob' should be {0, 2, 3, 4}")

	expr = influxql.MustParseExpr("age < 25")
	result, err = ci.Query(expr)
	require.NoError(t, err)
	expectSegIDs = []uint32{3, 4}
	assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where age < 25 should be {3, 4}")

	// Test AND Query
	exprAnd := influxql.MustParseExpr("\"name\" = 'Alice' AND age = 30")
	resultAnd, err := ci.Query(exprAnd)
	require.NoError(t, err)
	expectSegIDs = []uint32{1}
	assert.Equal(t, expectSegIDs, resultAnd.ToArray(), "the segNumID where name= 'Alice' AND age = 30 should be {1}")

	exprAnd = influxql.MustParseExpr("\"name\" > 'Alice' AND age = 25")
	resultAnd, err = ci.Query(exprAnd)
	require.NoError(t, err)
	expectSegIDs = []uint32{2}
	assert.Equal(t, expectSegIDs, resultAnd.ToArray(), "the segNumID where name > 'Alice' AND age = 25 should be {2}")

	exprAnd = influxql.MustParseExpr("\"name\" = 'Alice' AND age <= 25")
	resultAnd, err = ci.Query(exprAnd)
	require.NoError(t, err)
	expectSegIDs = []uint32{5}
	assert.Equal(t, expectSegIDs, resultAnd.ToArray(), "the segNumID where name = 'Alice' AND age <= 25 should be {5}")

	exprAnd = influxql.MustParseExpr("\"name\" >= 'Bob' AND age <= 25")
	resultAnd, err = ci.Query(exprAnd)
	require.NoError(t, err)
	expectSegIDs = []uint32{2, 3, 4}
	assert.Equal(t, expectSegIDs, resultAnd.ToArray(), "the segNumID where name >= 'Bob' AND age <= 25 should be {2, 3, 4}")

	exprAnd = influxql.MustParseExpr("\"name\" != 'Bob' AND age != 25")
	resultAnd, err = ci.Query(exprAnd)
	require.NoError(t, err)
	expectSegIDs = []uint32{0, 1, 4}
	assert.Equal(t, expectSegIDs, resultAnd.ToArray(), "the segNumID where name != 'Bob' AND age != 25 should be {0, 1, 4}")

	// Test OR query
	exprOr := influxql.MustParseExpr("\"name\" = 'Alice' OR \"name\" = 'Bob'")
	resultOr, err := ci.Query(exprOr)
	require.NoError(t, err)
	expectSegIDs = []uint32{1, 2, 3, 5}
	assert.Equal(t, expectSegIDs, resultOr.ToArray(), "the segNumID where name = 'Alice' OR name = 'Bob' should be {1, 2, 3, 5}")

	exprOr = influxql.MustParseExpr("\"name\" = 'Alice' OR age = 20")
	resultOr, err = ci.Query(exprOr)
	require.NoError(t, err)
	expectSegIDs = []uint32{1, 3, 4, 5}
	assert.Equal(t, expectSegIDs, resultOr.ToArray(), "the segNumID where name = 'Alice' OR age = 20 should be {1, 3, 4, 5}")

	exprOr = influxql.MustParseExpr("\"name\" > 'B' OR age <= 20")
	resultOr, err = ci.Query(exprOr)
	require.NoError(t, err)
	expectSegIDs = []uint32{0, 2, 3, 4}
	assert.Equal(t, expectSegIDs, resultOr.ToArray(), "the segNumID where name > 'B' OR age <= 20 should be {0, 2, 3, 4}")

	exprOr = influxql.MustParseExpr("age <=20 OR age > 25")
	resultOr, err = ci.Query(exprOr)
	require.NoError(t, err)
	expectSegIDs = []uint32{0, 1, 3, 4}
	assert.Equal(t, expectSegIDs, resultOr.ToArray(), "the segNumID where age <=20 OR age > 25 should be {0, 1, 3, 4}")

	exprOr = influxql.MustParseExpr("\"name\" <> 'Alice' OR age != 20")
	resultOr, err = ci.Query(exprOr)
	require.NoError(t, err)
	expectSegIDs = []uint32{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectSegIDs, resultOr.ToArray(), "the segNumID where name <> 'Alice' OR age != 20 should be {0, 1, 2, 3, 4, 5}")
}

func TestEvaluateRPN(t *testing.T) {
	ci := NewClusterIndex()

	//Preparing Test Data
	fields := map[string]*FieldToWrite{
		"name": {
			fType:  influx.Field_Type_String,
			fValue: "Alice",
		},
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(30),
		},
	}
	assert.NoError(t, ci.Write(0, fields))

	// Test a simple RPN expression
	rpn := []interface{}{
		&Condition{Field: "age", Op: influxql.EQ, Value: int64(30)},
	}
	result, err := ci.evaluateRPN(rpn)
	require.NoError(t, err)
	assert.True(t, result.Contains(0))

	// Testing the AND operation
	rpnAnd := []interface{}{
		&Condition{Field: "age", Op: influxql.EQ, Value: int64(30)},
		&Condition{Field: "name", Op: influxql.EQ, Value: "Alice"},
		influxql.Token(influxql.AND),
	}
	resultAnd, err := ci.evaluateRPN(rpnAnd)
	require.NoError(t, err)
	assert.True(t, resultAnd.Contains(0))

	// Test invalid RPN expressions
	rpnInvalid := []interface{}{
		influxql.AND,
	}
	_, err = ci.evaluateRPN(rpnInvalid)
	assert.Error(t, err)
}

func TestEvaluateCondition(t *testing.T) {
	ci := NewClusterIndex()

	// Preparing Test Data
	fields := map[string]*FieldToWrite{
		"age": {
			fType:  influx.Field_Type_Int,
			fValue: int64(30),
		},
	}
	assert.NoError(t, ci.Write(0, fields))

	// Test Equivalent Conditions
	cond := &Condition{Field: "age", Op: influxql.EQ, Value: int64(30)}
	result, err := ci.evaluateCondition(cond)
	require.NoError(t, err)
	assert.True(t, result.Contains(0))

	// Testing for Inequivalence Conditions
	condNEQ := &Condition{Field: "age", Op: influxql.NEQ, Value: int64(30)}
	resultNEQ, err := ci.evaluateCondition(condNEQ)
	require.NoError(t, err)
	assert.False(t, resultNEQ.Contains(0))
}

func TestConvertToRPN(t *testing.T) {
	// Testing Simple Expressions
	expr := influxql.MustParseExpr("\"name\" = 'Alice'")
	rpn, err := ConvertToRPN(expr)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		&Condition{
			Op:    influxql.EQ,
			Field: "name",
			Value: "Alice",
		},
	}, rpn)

	// Test expressions containing AND and OR
	exprComplex := influxql.MustParseExpr("(\"name\" = 'Alice' AND age > 25) OR (\"name\" = 'Bob')")
	rpnComplex, err := ConvertToRPN(exprComplex)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		&Condition{
			Op:    influxql.EQ,
			Field: "name",
			Value: "Alice",
		},
		&Condition{
			Op:    influxql.GT,
			Field: "age",
			Value: int64(25),
		},
		influxql.Token(influxql.AND),
		&Condition{
			Op:    influxql.EQ,
			Field: "name",
			Value: "Bob",
		},
		influxql.Token(influxql.OR),
	}, rpnComplex)

	// Testing unsupported expression types
	exprUnsupported := influxql.MustParseExpr("time > now()")
	_, err = ConvertToRPN(exprUnsupported)
	assert.Error(t, err)
}

func TestClusterIndex_WritePkRec(t *testing.T) {
	schema := []record.Field{
		{Name: "str", Type: influx.Field_Type_String},
		{Name: "int64", Type: influx.Field_Type_Int},
		{Name: "float64", Type: influx.Field_Type_Float},
		{Name: "bool", Type: influx.Field_Type_Boolean},
		{Name: record.FragmentField, Type: influx.Field_Type_Int},
	}

	t.Run("empty pk record", func(t *testing.T) {
		ci := NewClusterIndex()
		pk := record.NewRecord(schema, false)
		assert.NoError(t, ci.WritePkRec(pk))
	})

	t.Run("no fragment field", func(t *testing.T) {
		ci := NewClusterIndex()
		pk := record.NewRecord(schema[:len(schema)-1], false)
		pk.ColVals[len(pk.ColVals)-1].AppendBooleans(true, false)
		assert.Error(t, ci.WritePkRec(pk), "invalid __fragment__")
	})

	t.Run("normal pk record", func(t *testing.T) {
		ci := NewClusterIndex()
		pk := record.NewRecord(schema, false)
		pk.ColVals[0].AppendStrings("a", "b", "c", "d")
		pk.ColVals[1].AppendIntegers(1, 2, 3, 4)
		pk.ColVals[2].AppendFloats(1.0, 2.0, 3.0, 4.0)
		pk.ColVals[3].AppendBooleans(true, false, true, false)
		pk.ColVals[4].AppendIntegers(2, 4, 6, 8)
		assert.NoError(t, ci.WritePkRec(pk))

		exprComplex := influxql.MustParseExpr("\"str\" = 'a' AND \"int64\" = 1 AND \"float64\" = 1.0 AND \"bool\" = true")
		result, err := ci.Query(exprComplex)
		assert.NoError(t, err)
		expectSegIDs := []uint32{0, 1}
		assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where \"str\" = 'a' AND \"int64\" = 1 AND \"float64\" = 1.0 AND \"bool\" = true should be {0, 1}")

		exprComplex = influxql.MustParseExpr("\"str\" > 'a' AND \"int64\" <= 3")
		result, err = ci.Query(exprComplex)
		assert.NoError(t, err)
		expectSegIDs = []uint32{2, 3, 4, 5}
		assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where \"str\" > 'a' AND \"int64\" <= 3 should be {2, 3, 4, 5}")

		exprComplex = influxql.MustParseExpr("\"int64\" != 3 AND \"float64\" > 3.0 AND \"bool\" = true")
		result, err = ci.Query(exprComplex)
		assert.NoError(t, err)
		expectSegIDs = []uint32{}
		assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where \"int64\" != 3 AND \"float64\" > 3.0 AND \"bool\" = true should be {}")

		exprComplex = influxql.MustParseExpr("\"str\" < 'b' OR \"bool\" = true")
		result, err = ci.Query(exprComplex)
		assert.NoError(t, err)
		expectSegIDs = []uint32{0, 1, 4, 5}
		assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where \"str\" < 'b' OR \"bool\" = true should be {0, 1, 4, 5}")

		exprComplex = influxql.MustParseExpr("\"str\" != 'b' OR \"int64\" != 1")
		result, err = ci.Query(exprComplex)
		assert.NoError(t, err)
		expectSegIDs = []uint32{0, 1, 2, 3, 4, 5, 6, 7}
		assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where \"str\" != 'b' OR \"int64\" != 1 should be {0, 1, 2, 3, 4, 5, 6, 7}")

		exprComplex = influxql.MustParseExpr("fieldNotExist=1")
		result, err = ci.Query(exprComplex)
		assert.NoError(t, err)
		expectSegIDs = []uint32{0, 1, 2, 3, 4, 5, 6, 7}
		assert.Equal(t, expectSegIDs, result.ToArray(), "the segNumID where fieldNotExist=1 should be {0, 1, 2, 3, 4, 5, 6, 7}")

		exprComplex = influxql.MustParseExpr("\"str\" > 2")
		_, err = ci.Query(exprComplex)
		assert.EqualError(t, err, "operand type clash: column[str] String>Integer", "error message is not expected")

		exprComplex = influxql.MustParseExpr("\"int64\" = true")
		_, err = ci.Query(exprComplex)
		assert.EqualError(t, err, "operand type clash: column[int64] Integer=Boolean", "error message is not expected")

		exprComplex = influxql.MustParseExpr("\"float64\" != '2'")
		_, err = ci.Query(exprComplex)
		assert.EqualError(t, err, "operand type clash: column[float64] Float!=String", "error message is not expected")

		exprComplex = influxql.MustParseExpr("\"bool\" < 2")
		_, err = ci.Query(exprComplex)
		assert.EqualError(t, err, "operand type clash: column[bool] Boolean<Integer", "error message is not expected")

		exprComplex = &influxql.BinaryExpr{
			Op:  influxql.IN,
			LHS: &influxql.VarRef{Val: "str", Type: influx.Field_Type_String},
			RHS: &influxql.SetLiteral{Vals: map[interface{}]bool{"b": true, "e": true}},
		}
		result, err = ci.Query(exprComplex)
		assert.NoError(t, err)
		expectSegIDs = []uint32{2, 3}
		assert.Equal(t, expectSegIDs, result.ToArray())
	})
}

func TestBitmapTrans(t *testing.T) {
	bm := roaring.New()

	indices := BitmapToIDs(bm)
	expectedIndices := []int{}
	assert.Equal(t, expectedIndices, indices)

	ranges := BitmapToRanges(bm)
	exptectedRanges := fragment.FragmentRanges(nil)
	assert.Equal(t, exptectedRanges, ranges)

	bm.Add(1)

	indices = BitmapToIDs(bm)
	expectedIndices = []int{1}
	assert.Equal(t, expectedIndices, indices)

	ranges = BitmapToRanges(bm)
	exptectedRanges = fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 2}}
	assert.Equal(t, exptectedRanges, ranges)

	bm.AddRange(5, 9)

	indices = BitmapToIDs(bm)
	expectedIndices = []int{1, 5, 6, 7, 8}
	assert.Equal(t, expectedIndices, indices)

	ranges = BitmapToRanges(bm)
	exptectedRanges = fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 2}, &fragment.FragmentRange{Start: 5, End: 9}}
	assert.Equal(t, exptectedRanges, ranges)

	bm.AddRange(9, 12)

	indices = BitmapToIDs(bm)
	expectedIndices = []int{1, 5, 6, 7, 8, 9, 10, 11}
	assert.Equal(t, expectedIndices, indices)

	ranges = BitmapToRanges(bm)
	exptectedRanges = fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 2}, &fragment.FragmentRange{Start: 5, End: 12}}
	assert.Equal(t, exptectedRanges, ranges)

	bm.Remove(7)

	indices = BitmapToIDs(bm)
	expectedIndices = []int{1, 5, 6, 8, 9, 10, 11}
	assert.Equal(t, expectedIndices, indices)

	ranges = BitmapToRanges(bm)
	exptectedRanges = fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 2}, &fragment.FragmentRange{Start: 5, End: 7}, &fragment.FragmentRange{Start: 8, End: 12}}
	assert.Equal(t, exptectedRanges, ranges)
}

func BenchmarkBitmapTrans(b *testing.B) {
	bm := roaring.New()
	bm.AddRange(0, 10000)

	b.Run("ToIDsThenRanges", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			indices := BitmapToIDs(bm)
			fragment.CalcFragmentRanges(indices)
		}
	})

	b.Run("ToRanges", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			BitmapToRanges(bm)
		}
	})
}

func BenchmarkBitmapTransManyRanges(b *testing.B) {
	bm := roaring.New()
	var i uint64
	for i = 0; i <= 10000; i++ {
		bm.AddRange(i*2, i*2+1)
	}

	b.Run("ToIDsThenRanges", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			indices := BitmapToIDs(bm)
			fragment.CalcFragmentRanges(indices)
		}
	})

	b.Run("ToRanges", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			BitmapToRanges(bm)
		}
	})
}
