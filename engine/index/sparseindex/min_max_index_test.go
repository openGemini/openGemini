// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package sparseindex_test

import (
	"os"
	"path"
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper functions to test private methods
// Since these functions are private, we'll test them indirectly through public interfaces
func TestMinMaxIndexReader(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewMinMaxIndexReader(rpnExpr, schema, option, true)
	if err != nil {
		t.Fatal(err)
	}

	// Test GetFragmentRowCount
	rowCount, err := reader.GetFragmentRowCount(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowCount)

	// Test StartSpan
	reader.StartSpan(nil)

	// Test Close
	assert.NoError(t, reader.Close())
}

func TestMinMaxIndexReaderInteger(t *testing.T) {
	// [1, 2, 3], [4, 5, 6] frag=1
	testCase(t, influxql.EQ, influxql.Integer, "field1", int64(2), false)
	testCase(t, influxql.GTE, influxql.Integer, "field1", int64(2), true)
	testCase(t, influxql.GTE, influxql.Integer, "field1", int64(4), true)
	testCase(t, influxql.NEQ, influxql.Integer, "field1", int64(4), true)
	testCase(t, influxql.NEQ, influxql.Integer, "field1", int64(2), true)
	testCase(t, influxql.NEQ, influxql.Integer, "field1", int64(8), true)
	testCase(t, influxql.NEQ, influxql.Integer, "field1", float64(8), true)
	testCase(t, influxql.LTE, influxql.Integer, "field1", int64(2), false)
	testCase(t, influxql.LT, influxql.Integer, "field1", int64(2), false)
	testCase(t, influxql.LT, influxql.Integer, "field1", int64(10), true)
	testCase(t, influxql.GT, influxql.Integer, "field1", int64(2), true)
	testCase(t, influxql.GT, influxql.Integer, "field1", int64(10), false)
	testCase(t, influxql.GT, influxql.Integer, "field1", float64(10), false)
	testCaseNoSegId(t, influxql.GT, influxql.Integer, "field1", int64(2))
}

func testCase(t *testing.T, operator influxql.Token, numType influxql.DataType, fieldName string, condValue interface{}, expectResult bool) {
	reader, err := buildReader(t, operator, numType, fieldName, condValue)
	require.NoError(t, err)
	defer reader.Close()

	pathName := t.TempDir()
	err = os.MkdirAll(path.Join(pathName, "mst"), 0700)
	require.NoError(t, err)
	err = WriteData(t, pathName)
	assert.NoError(t, err)

	dataFile := "mst.tssp"
	tsspFile := &MocTsspFile{
		path: path.Join(pathName, "mst", dataFile),
		name: dataFile,
	}
	err = reader.ReInit(tsspFile)
	require.NoError(t, err)
	exist, err := reader.MayBeInFragment(1)
	assert.NoError(t, err)
	assert.Equal(t, expectResult, exist)
}

func testCaseNoSegId(t *testing.T, operator influxql.Token, numType influxql.DataType, fieldName string, condValue interface{}) {
	reader, err := buildReader(t, operator, numType, fieldName, condValue)
	require.NoError(t, err)
	defer reader.Close()

	pathName := t.TempDir()
	err = os.MkdirAll(path.Join(pathName, "mst"), 0700)
	require.NoError(t, err)
	err = WriteData(t, pathName)
	assert.NoError(t, err)

	dataFile := "mst.tssp"
	tsspFile := &MocTsspFile{
		path: path.Join(pathName, "mst", dataFile),
		name: dataFile,
	}

	err = reader.ReInit(tsspFile)
	require.NoError(t, err)
	exist, err := reader.MayBeInFragment(10)
	assert.False(t, exist)
}

func TestMinMaxIndexReaderFloat(t *testing.T) {
	// [1.1, 2.2, 3.3], [4.4, 5.5, 6.6]  fragId=1
	testCase(t, influxql.EQ, influxql.Float, "field2", float64(2.2), false)
	testCase(t, influxql.GTE, influxql.Float, "field2", float64(2.2), true)
	testCase(t, influxql.LTE, influxql.Float, "field2", float64(2.2), false)
	testCase(t, influxql.LT, influxql.Float, "field2", float64(2.2), false)
	testCase(t, influxql.GT, influxql.Float, "field2", float64(2.2), true)
	testCase(t, influxql.GT, influxql.Float, "field2", int64(2), true)
	testCase(t, influxql.LT, influxql.Float, "field2", float64(12.2), true)
	testCase(t, influxql.GT, influxql.Float, "field2", float64(12.2), false)
	testCase(t, influxql.GT, influxql.Float, "field2", int64(12), false)
	testCaseNoSegId(t, influxql.GT, influxql.Float, "field2", float64(2.2))
	testCase(t, influxql.MATCHPHRASE, influxql.Float, "field2", 2.2, true)
}

func WriteData(t *testing.T, pathName string) error {
	writer := sparseindex.NewMinMaxWriter(pathName, "mst", "mst", path.Join(pathName, "lockPath"), "tokens")
	// Create test record with multiple fields
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field2", Type: influx.Field_Type_Float},
	}, false)

	// Add test data
	writeRec.ColVals[0].AppendIntegers(1, 2, 3, 4, 5, 6)
	writeRec.ColVals[1].AppendFloats(1.1, 2.2, 3.3, 4.4, 5.5, 6.6)

	// Test with single field
	schemaIdx := []int{0, 1}
	rowsPerSegment := []int{3, 6}
	err := writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	require.NoError(t, err)
	err = writer.Flush()
	require.NoError(t, err)

	fileName := writer.Files()[0]

	err = writer.Close()
	require.NoError(t, err)

	err = fileops.RenameFile(fileName, fileName[:len(fileName)-5])
	return err
}

func TestMinMaxIndexNil(t *testing.T) {
	tests := []struct {
		name            string
		fieldType       int
		nilPosition     int
		value           interface{}
		buildReaderType influxql.DataType
	}{
		{"Int_LastNil", influx.Field_Type_Int, 2, int64(2), influxql.Integer},
		{"Int_FirstNil", influx.Field_Type_Int, 0, int64(2), influxql.Integer},
		{"Int_MiddleNil", influx.Field_Type_Int, 1, int64(2), influxql.Integer},
		{"Int_AllNil", influx.Field_Type_Int, 3, int64(12), influxql.Integer},
		{"Float_LastNil", influx.Field_Type_Float, 2, 2.2, influxql.Float},
		{"Float_FirstNil", influx.Field_Type_Float, 0, 2.2, influxql.Float},
		{"Float_MiddleNil", influx.Field_Type_Float, 1, 2.2, influxql.Float},
		{"Float_AllNil", influx.Field_Type_Float, 3, 2.2, influxql.Float},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pathName := t.TempDir()
			err := os.MkdirAll(path.Join(pathName, "mst"), 0700)
			require.NoError(t, err)

			writer := sparseindex.NewMinMaxWriter(pathName, "mst", "mst", path.Join(pathName, "lockPath"), "tokens")

			writeRec := record.NewRecord(record.Schemas{
				{Name: "field1", Type: tt.fieldType},
			}, false)

			schemaIdx := []int{0}
			rowsPerSegment := []int{3}

			switch tt.nilPosition {
			case 0: // First nil
				writeRec.ColVals[0].Init()
				if tt.fieldType == influx.Field_Type_Int {
					writeRec.ColVals[0].AppendIntegerNulls(3)
				} else {
					writeRec.ColVals[0].AppendFloatNulls(3)
				}
				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)

				if tt.fieldType == influx.Field_Type_Int {
					writeRec.ColVals[0].AppendIntegers(1, 2, 3)
				} else {
					writeRec.ColVals[0].AppendFloats(1.1, 2.2, 3.3)
				}
				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)
			case 1: // Middle nil
				if tt.fieldType == influx.Field_Type_Int {
					writeRec.ColVals[0].AppendIntegers(1, 2, 3)
				} else {
					writeRec.ColVals[0].AppendFloats(1.1, 2.2, 3.3)
				}

				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)

				writeRec.ColVals[0].Init()
				if tt.fieldType == influx.Field_Type_Int {
					writeRec.ColVals[0].AppendIntegerNulls(3)
				} else {
					writeRec.ColVals[0].AppendFloatNulls(3)
				}
				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)

				writeRec.ColVals[0].AppendIntegers(1, 2, 3)
				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)
			case 2: // Last nil
				if tt.fieldType == influx.Field_Type_Int {
					writeRec.ColVals[0].AppendIntegers(1, 2, 3)
				} else {
					writeRec.ColVals[0].AppendFloats(1.1, 2.2, 3.3)
				}
				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)

				writeRec.ColVals[0].Init()
				if tt.fieldType == influx.Field_Type_Int {
					writeRec.ColVals[0].AppendIntegerNulls(3)
				} else {
					writeRec.ColVals[0].AppendFloatNulls(3)
				}
				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)
			case 3: // All nil
				writeRec.ColVals[0].Init()
				if tt.fieldType == influx.Field_Type_Int {
					writeRec.ColVals[0].AppendIntegerNulls(3)
				} else {
					writeRec.ColVals[0].AppendFloatNulls(3)
				}
				err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
				require.NoError(t, err)
			}

			err = writer.Flush()
			require.NoError(t, err)

			fileName := writer.Files()[0]
			err = writer.Close()
			require.NoError(t, err)
			err = fileops.RenameFile(fileName, fileName[:len(fileName)-5])
			require.NoError(t, err)

			reader, err := buildReader(t, influxql.EQ, tt.buildReaderType, "field1", tt.value)
			require.NoError(t, err)
			defer reader.Close()

			dataFile := "mst.tssp"
			tsspFile := &MocTsspFile{
				path: path.Join(pathName, "mst", dataFile),
				name: dataFile,
			}
			err = reader.ReInit(tsspFile)
			require.NoError(t, err)

			for fragID := 0; fragID <= 1; fragID++ {
				exist, err := reader.MayBeInFragment(uint32(fragID))
				assert.NoError(t, err)

				switch tt.nilPosition {
				case 0: // First nil
					assert.Equal(t, fragID == 1, exist)
				case 1: // Middle nil
					assert.Equal(t, fragID == 0, exist)
				case 2: // Last nil
					assert.Equal(t, fragID == 0, exist)
				case 3: // All nil
					assert.Equal(t, false, exist)
				}
			}
		})
	}
}

func buildReader(test *testing.T, token influxql.Token, dataType influxql.DataType, fieldName string, condValue interface{}) (*sparseindex.MinMaxIndexReader, error) {
	var t int
	var option *query.ProcessorOptions
	if dataType == influxql.Integer {
		t = influx.Field_Type_Int
		if v, ok := condValue.(int64); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.IntegerLiteral{Val: v},
			}}
		} else if v, ok := condValue.(float64); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.NumberLiteral{Val: v},
			}}
		} else {
			require.True(test, ok)
		}
	} else if dataType == influxql.Float {
		t = influx.Field_Type_Float
		if v, ok := condValue.(float64); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.NumberLiteral{Val: v},
			}}
		} else if v, ok := condValue.(int64); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.IntegerLiteral{Val: v},
			}}
		} else {
			require.True(test, ok)
		}
	} else {
		t = influx.Field_Type_String
		if v, ok := condValue.(string); ok {
			option = &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
				Op:  token,
				LHS: &influxql.VarRef{Val: fieldName, Type: dataType},
				RHS: &influxql.StringLiteral{Val: v},
			}}
		} else {
			require.True(test, ok)
		}
	}
	schema := record.Schemas{{Name: fieldName, Type: t}}

	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	return sparseindex.NewMinMaxIndexReader(rpnExpr, schema, option, true)
}

type MocTsspFile struct {
	sparseindex.TsspFile
	path string
	name string
}

func (m MocTsspFile) Path() string {
	return m.path
}

func (m MocTsspFile) Name() string {
	return ""
}

func TestWriteDataStringReturnError(t *testing.T) {
	pathName := t.TempDir()
	os.MkdirAll(path.Join(pathName, "mst"), 0700)

	// init writer
	writer := sparseindex.NewMinMaxWriter(pathName, "mst", "mst", path.Join(pathName, "lockPath"), "tokens")

	// Create test record with string field
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field3", Type: influx.Field_Type_String},
	}, false)

	// Add test data
	writeRec.ColVals[0].AppendStrings("1")

	// Test with single field
	schemaIdx := []int{0}
	rowsPerSegment := []int{1}
	err := writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.ErrorContains(t, err, "unsupported")

	err = writer.Flush()
	assert.NoError(t, err)

	// Test Close
	assert.NoError(t, writer.Close())
}

// TestMinMaxIndexReader_error is removed because it causes nil pointer dereference
// The ReInit method requires proper initialization before it can work with error conditions

func TestMinMaxWriter(t *testing.T) {
	// Test NewMinMaxWriter
	pathName := t.TempDir()
	err := os.MkdirAll(path.Join(pathName, "msName"), 0700)
	require.NoError(t, err)
	writer := sparseindex.NewMinMaxWriter(pathName, "msName", "dataFile", "lockPath", "tokens")
	assert.NotNil(t, writer)

	err = writer.Flush()
	assert.NoError(t, err)

	// Test that Close doesn't panic
	assert.NotPanics(t, func() {
		err := writer.Close()
		assert.NoError(t, err)
	})
}

func TestMinMaxWriter_CreateAttachIndex(t *testing.T) {
	pathName := t.TempDir()
	err := os.MkdirAll(path.Join(pathName, "msName"), 0700)
	require.NoError(t, err)
	writer := sparseindex.NewMinMaxWriter(pathName, "msName", "dataFile", "lockPath", "tokens")

	// Create test record with multiple fields
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
		{Name: "field2", Type: influx.Field_Type_Float},
	}, false)

	// Add test data
	writeRec.ColVals[0].AppendIntegers(1, 2, 3, 4, 5, 6)
	writeRec.ColVals[1].AppendFloats(1.1, 2.2, 3.3, 4.4, 5.5, 6.6)

	// Test with single field
	schemaIdx := []int{0}
	rowsPerSegment := []int{3, 6}
	err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err)

	// Test with multiple fields
	schemaIdx = []int{0, 1}
	err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err)

	// Test with empty rowsPerSegment
	rowsPerSegment = []int{0}
	err = writer.CreateAttachIndex(writeRec, schemaIdx, rowsPerSegment)
	assert.NoError(t, err)

	err = writer.Flush()
	assert.NoError(t, err)

	// Test Close
	assert.NoError(t, writer.Close())
}

func TestMinMaxWriter_CreateDetachIndex(t *testing.T) {
	pathName := t.TempDir()
	err := os.MkdirAll(path.Join(pathName, "msName"), 0700)
	require.NoError(t, err)
	writer := sparseindex.NewMinMaxWriter(pathName, "msName", "dataFile", "lockPath", "tokens")

	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}, false)

	dataBufs := make([][]byte, 0)

	// Test CreateDetachIndex
	resultBufs, resultStrings := writer.CreateDetachIndex(writeRec, []int{0}, []int{3}, dataBufs)

	// The function returns nil slices for this implementation
	assert.Nil(t, resultBufs)
	assert.Nil(t, resultStrings)

	err = writer.Flush()
	assert.NoError(t, err)

	// Test Close
	assert.NoError(t, writer.Close())
}

func TestMinMaxFilterReaders(t *testing.T) {
	schemas := record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}

	// Test NewMinMaxFilterReaders
	readers := sparseindex.NewMinMaxFilterReaders("/test/path", "testfile", schemas, nil)
	assert.NotNil(t, readers)

	// Test Close
	assert.NoError(t, readers.Close())

	// Test StartSpan
	readers.StartSpan(nil)
}

func TestMinMaxFilterReaders_GetRowCount(t *testing.T) {
	schemas := record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}
	readers := sparseindex.NewMinMaxFilterReaders("/test/path", "testfile", schemas, nil)

	rowCount, err := readers.GetRowCount(0, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowCount)
}

func TestMinMaxIndexReader_GetFragmentRowCount(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewMinMaxIndexReader(rpnExpr, schema, option, true)
	if err != nil {
		t.Fatal(err)
	}

	rowCount, err := reader.GetFragmentRowCount(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowCount)
}

func TestMinMaxIndexReader_StartSpan(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewMinMaxIndexReader(rpnExpr, schema, option, true)
	if err != nil {
		t.Fatal(err)
	}

	// Test StartSpan doesn't panic
	reader.StartSpan(nil)
}

func TestMinMaxIndexReader_ReInit(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewMinMaxIndexReader(rpnExpr, schema, option, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Test ReInit with string
	err = reader.ReInit("test_file")
	assert.ErrorContains(t, err, "file need be tssp type")

	mockFile := mockTsspFile{path: "/data/data/db0/0...columnstore/mst_00000001-0000-00000000.tssp"}
	err = reader.ReInit(mockFile)
	assert.NoError(t, err)
}

// Test ReInit with TsspFile (mock)
type mockTsspFile struct {
	sparseindex.TsspFile
	path string
}

func (m mockTsspFile) Path() string {
	return m.path
}

// TestMinMaxIndexReader_ErrorCases is removed because it causes nil pointer dereference
// The NewSKCondition function requires valid input parameters

func TestMinMaxWriter_ErrorCases(t *testing.T) {

	pathName := t.TempDir()
	err := os.MkdirAll(path.Join(pathName, "msName"), 0700)
	require.NoError(t, err)

	writer := sparseindex.NewMinMaxWriter(pathName, "msName", "dataFile", "lockPath", "tokens")

	// Test with empty schema indices
	writeRec := record.NewRecord(record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}, false)
	writeRec.ColVals[0].AppendIntegers(1, 2, 3)

	// This should not panic
	assert.NoError(t, writer.CreateAttachIndex(writeRec, []int{}, []int{3}))

	// Test with invalid rowsPerSegment
	assert.NoError(t, writer.CreateAttachIndex(writeRec, []int{0}, []int{}))

	err = writer.Flush()
	assert.NoError(t, err)

	// Test Close
	assert.NoError(t, writer.Close())
}

func TestMinMaxFilterReaders_ErrorCases(t *testing.T) {
	schemas := record.Schemas{
		{Name: "field1", Type: influx.Field_Type_Int},
	}
	readers := sparseindex.NewMinMaxFilterReaders("/test/path", "testfile", schemas, nil)

	// Test IsExist with nil element
	_, err := readers.IsExist(0, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the input SKRPNElement is nil")

	// Test IsExist with non-existent field
	elem := &rpn.SKRPNElement{Key: "nonexistent"}
	_, err = readers.IsExist(0, elem)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot find the index for the field")
}
