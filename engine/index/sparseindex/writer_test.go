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

package sparseindex_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func buildPKSchema() record.Schemas {
	var schema record.Schemas
	schema = append(schema,
		record.Field{Name: "UserID", Type: influx.Field_Type_String},
		record.Field{Name: "URL", Type: influx.Field_Type_String})
	return schema
}

func buildDataRecord() *record.Record {
	schema := buildPKSchema()
	rec := record.NewRecord(schema, false)
	rec.Column(0).AppendStrings("U1", "U1", "U1", "U1", "U1", "U1", "U1", "U1")
	rec.Column(1).AppendStrings("W1", "W2", "W2", "W3", "W4", "W4", "W5", "W6")
	return rec
}

func buildIndexFragmentWithVariable() fragment.IndexFragment {
	accumulateRowCount := []uint64{2, 4, 6, 8}
	mark := fragment.NewIndexFragmentVariable(accumulateRowCount)
	return mark
}

func buildPKRecord() *record.Record {
	schema := buildPKSchema()
	rec := record.NewRecord(schema, false)
	rec.Column(0).AppendStrings("U1", "U1", "U1", "U1", "U1")
	rec.Column(1).AppendStrings("W1", "W2", "W4", "W5", "W6")
	rec.Column(0).Bitmap = []byte{255}
	rec.Column(1).Bitmap = []byte{255}
	return rec
}

func buildIndexFragmentWithFixedSize() fragment.IndexFragment {
	mark := fragment.NewIndexFragmentFixedSize(4, 2)
	return mark
}

func TestCreatePrimaryIndexWithStringType(t *testing.T) {
	indexWriter := sparseindex.NewIndexWriter()
	FragmentSize := 2
	pkSchema := buildPKSchema()
	srcRec1 := buildDataRecord()

	sparseindex.InitIndexFragmentFixedSize = true
	pkRec, pkMark, err := indexWriter.CreatePrimaryIndex(srcRec1, pkSchema, FragmentSize)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, buildPKRecord(), pkRec)
	assert.Equal(t, buildIndexFragmentWithFixedSize(), pkMark)

	sparseindex.InitIndexFragmentFixedSize = false
	pkRec, pkMark, err = indexWriter.CreatePrimaryIndex(srcRec1, pkSchema, FragmentSize)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, buildPKRecord(), pkRec)
	assert.Equal(t, buildIndexFragmentWithVariable(), pkMark)
}

func buildPKSchemaAll() record.Schemas {
	var schema record.Schemas
	schema = append(schema,
		record.Field{Name: "stringKey", Type: influx.Field_Type_String},
		record.Field{Name: "boolKey", Type: influx.Field_Type_Boolean},
		record.Field{Name: "intKey", Type: influx.Field_Type_Int},
		record.Field{Name: "floatKey", Type: influx.Field_Type_Float})
	return schema
}

func buildDataRecordAll() *record.Record {
	schema := buildPKSchemaAll()
	rec := record.NewRecord(schema, false)
	rec.Column(0).AppendStrings("W1", "W1", "W1", "W1", "W1", "W1", "W1", "W1")
	rec.Column(1).AppendBooleans(true, true, true, true, false, false, false, false)
	rec.Column(2).AppendIntegers(1, 2, 1, 2, 1, 2, 1, 2)
	rec.Column(3).AppendFloats(1, 2, 3, 4, 5, 6, 7, 8)
	return rec
}

func buildPKRecordAllFinal() *record.Record {
	schema := buildPKSchemaAll()
	rec := record.NewRecord(schema, false)
	rec.Column(0).AppendStrings("W1", "W1", "W1", "W1", "W1")
	rec.Column(1).AppendBooleans(true, true, false, false, false)
	rec.Column(2).AppendIntegers(1, 1, 1, 1, 2)
	rec.Column(3).AppendFloats(1, 3, 5, 7, 8)
	rec.Column(0).Bitmap = []byte{255}
	rec.Column(1).Bitmap = []byte{255}
	rec.Column(2).Bitmap = []byte{255}
	rec.Column(3).Bitmap = []byte{255}
	return rec
}

func buildIndexFragmentAllWithVariable() fragment.IndexFragment {
	accumulateRowCount := []uint64{2, 4, 6, 8}
	mark := fragment.NewIndexFragmentVariable(accumulateRowCount)
	return mark
}

func buildIndexFragmentAllWtiFixedSize() fragment.IndexFragment {
	mark := fragment.NewIndexFragmentFixedSize(4, 2)
	return mark
}

func TestCreatePrimaryIndexAllDataType(t *testing.T) {
	FragmentSize := 2
	indexWriter := sparseindex.NewIndexWriter()
	pkSchema := buildPKSchemaAll()
	srcRec1 := buildDataRecordAll()

	sparseindex.InitIndexFragmentFixedSize = true
	pkRec, pkMark, err := indexWriter.CreatePrimaryIndex(srcRec1, pkSchema, FragmentSize)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, buildPKRecordAllFinal(), pkRec)
	assert.Equal(t, buildIndexFragmentAllWtiFixedSize(), pkMark)

	sparseindex.InitIndexFragmentFixedSize = false
	pkRec, pkMark, err = indexWriter.CreatePrimaryIndex(srcRec1, pkSchema, FragmentSize)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, buildPKRecordAllFinal(), pkRec)
	assert.Equal(t, buildIndexFragmentAllWithVariable(), pkMark)
}
