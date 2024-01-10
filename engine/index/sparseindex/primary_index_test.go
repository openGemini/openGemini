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
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
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

func TestBuildWithStringType(t *testing.T) {
	indexWriter := sparseindex.NewPKIndexWriter()
	FragmentSize := 2
	pkSchema := buildPKSchema()
	srcRec1 := buildDataRecord()

	fixRowsPerSegment := immutable.GenFixRowsPerSegment(srcRec1, FragmentSize)
	sparseindex.InitIndexFragmentFixedSize = true
	pkRec, pkMark, err := indexWriter.Build(srcRec1, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, FragmentSize)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, buildPKRecord(), pkRec)
	assert.Equal(t, buildIndexFragmentWithFixedSize(), pkMark)

	sparseindex.InitIndexFragmentFixedSize = false
	pkRec, pkMark, err = indexWriter.Build(srcRec1, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, 0)
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

func TestBuildAllDataType(t *testing.T) {
	FragmentSize := 2
	indexWriter := sparseindex.NewPKIndexWriter()
	pkSchema := buildPKSchemaAll()
	srcRec1 := buildDataRecordAll()

	fixRowsPerSegment := immutable.GenFixRowsPerSegment(srcRec1, FragmentSize)
	sparseindex.InitIndexFragmentFixedSize = true
	pkRec, pkMark, err := indexWriter.Build(srcRec1, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, FragmentSize)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, buildPKRecordAllFinal(), pkRec)
	assert.Equal(t, buildIndexFragmentAllWtiFixedSize(), pkMark)

	sparseindex.InitIndexFragmentFixedSize = false
	pkRec, pkMark, err = indexWriter.Build(srcRec1, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, buildPKRecordAllFinal(), pkRec)
	assert.Equal(t, buildIndexFragmentAllWithVariable(), pkMark)
}

var (
	fieldMap = map[string]influxql.DataType{
		"UserID":    influxql.String,
		"URL":       influxql.String,
		"stringKey": influxql.String,
		"boolKey":   influxql.Boolean,
		"intKey":    influxql.Integer,
		"floatKey":  influxql.Float,
	}
)

// MustParseExpr parses an expression. Panic on error.
func MustParseExpr(s string) influxql.Expr {
	p := influxql.NewParser(strings.NewReader(s))
	defer p.Release()
	expr, err := p.ParseExpr()
	if err != nil {
		panic(err)
	}
	influxql.WalkFunc(expr, func(n influxql.Node) {
		ref, ok := n.(*influxql.VarRef)
		if !ok {
			return
		}
		ty, ok := fieldMap[ref.Val]
		if ok {
			ref.Type = ty
		} else {
			ref.Type = influxql.Tag
		}
	})
	return expr
}

func buildPkRecordComplex() *record.Record {
	schema := buildPKSchema()
	rec := record.NewRecord(schema, false)
	rec.Column(0).AppendStrings("U1", "U2", "U3", "U4", "U4")
	rec.Column(1).AppendStrings("W1", "W2", "W2", "W1", "W2")
	return rec
}

func buildIndexFragmentVariable() fragment.IndexFragment {
	return fragment.NewIndexFragmentVariable([]uint64{2, 4, 6, 8})
}

func buildIndexFragmentFixedSize() fragment.IndexFragment {
	return fragment.NewIndexFragmentFixedSize(4, 2)
}

func TestScanWithSimple(t *testing.T) {
	pkFile := "00000001-0001-00000000.idx"
	indexReader := sparseindex.NewPKIndexReader(2, 2, 0)
	f := func(
		pkFile string,
		pkMark fragment.IndexFragment,
		pkRec *record.Record,
		expectFragmentRanges fragment.FragmentRanges,
		conStr string,
	) {
		pkSchema := pkRec.Schema
		KeyConditionImpl, err := sparseindex.NewKeyCondition(nil, MustParseExpr(conStr), pkSchema)
		if err != nil {
			t.Fatal(err)
		}
		outputFragmentRanges, err := indexReader.Scan(pkFile, pkRec, pkMark, KeyConditionImpl)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, expectFragmentRanges, outputFragmentRanges)
	}
	t.Run("exclusion search EQ11", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 2)},
			"URL='W3'",
		)
	})

	t.Run("binary search EQ11", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID='U1'",
		)
	})

	t.Run("exclusion search EQ AND EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 2)},
			"UserID='U1' and URL='W3'",
		)
	})

	t.Run("exclusion search EQ OR EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID='U1' or URL='W3'",
		)
	})

	t.Run("binary search LT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID<'U2'",
		)
	})

	t.Run("binary search LTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID<='U2'",
		)
	})

	t.Run("binary search GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			nil,
			"UserID>'U2'",
		)
	})

	t.Run("binary search GTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			nil,
			"UserID>='U2'",
		)
	})

	t.Run("exclusion search LT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 1)},
			"URL<'W2'",
		)
	})

	t.Run("exclusion search LTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 2)},
			"URL<='W2'",
		)
	})

	t.Run("exclusion search GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 4)},
			"URL>'W2'",
		)
	})

	t.Run("exclusion search GTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"URL>='W2'",
		)
	})

	t.Run("binary search NTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID != 'U2'",
		)
	})

	t.Run("exclusion search NTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPKRecord(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"URL != 'W2'",
		)
	})

}

func TestScanWithComplex(t *testing.T) {
	pkFile := "00000001-0001-00000000.idx"
	indexReader := sparseindex.NewPKIndexReader(2, 2, 0)
	f := func(
		pkFile string,
		pkMark fragment.IndexFragment,
		pkRec *record.Record,
		expectFragmentRanges fragment.FragmentRanges,
		conStr string,
	) {
		pkSchema := pkRec.Schema
		KeyConditionImpl, err := sparseindex.NewKeyCondition(nil, MustParseExpr(conStr), pkSchema)
		if err != nil {
			t.Fatal(err)
		}
		outputFragmentRanges, err := indexReader.Scan(pkFile, pkRec, pkMark, KeyConditionImpl)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, expectFragmentRanges, outputFragmentRanges)
	}
	t.Run("exclusion search EQ11", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 3)},
			"URL='W3'",
		)
	})

	t.Run("binary search EQ11", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 1)},
			"UserID='U1'",
		)
	})

	t.Run("exclusion search EQ AND EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 1)},
			"UserID='U1' and URL='W3'",
		)
	})

	t.Run("exclusion search EQ AND EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(3, 4)},
			"UserID='U4' and URL='W2'",
		)
	})

	t.Run("exclusion search EQ OR EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 3)},
			"UserID='U1' or URL='W3'",
		)
	})

	t.Run("binary search LT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 1)},
			"UserID<'U2'",
		)
	})

	t.Run("binary search LTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 2)},
			"UserID<='U2'",
		)
	})

	t.Run("binary search GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 4)},
			"UserID>'U2'",
		)
	})

	t.Run("binary search GTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID>='U2'",
		)
	})

	t.Run("exclusion search LT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"URL<'W2'",
		)
	})

	t.Run("exclusion search LTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"URL<='W2'",
		)
	})

	t.Run("exclusion search GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 3)},
			"URL>'W2'",
		)
	})

	t.Run("exclusion search GTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"URL>='W2'",
		)
	})

	t.Run("binary search NTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID != 'U2'",
		)
	})

	t.Run("exclusion search NTE", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"URL != 'W2'",
		)
	})

	t.Run("exclusion search LT OR GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"UserID > 'U2' OR URL > 'W2'",
		)
	})

	t.Run("exclusion search LT and GT or LT and GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 3)},
			"UserID > 'U2' and URL > 'W2' or height = 180 and age = 18",
		)
	})

	t.Run("exclusion search LT and GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 3)},
			"UserID > 'U2' and UserID < 'U4'",
		)
	})
}

func TestScanWithAllType(t *testing.T) {
	pkFile := "00000001-0001-00000000.idx"
	indexReader := sparseindex.NewPKIndexReader(2, 2, 0)
	f := func(
		pkFile string,
		pkMark fragment.IndexFragment,
		pkRec *record.Record,
		expectFragmentRanges fragment.FragmentRanges,
		conStr string,
	) {
		pkSchema := pkRec.Schema
		KeyConditionImpl, err := sparseindex.NewKeyCondition(nil, MustParseExpr(conStr), pkSchema)
		if err != nil {
			t.Fatal(err)
		}
		outputFragmentRanges, err := indexReader.Scan(pkFile, pkRec, pkMark, KeyConditionImpl)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, expectFragmentRanges, outputFragmentRanges)
	}
	t.Run("Integer exclusion search EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentFixedSize(),
			buildPKRecordAllFinal(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"intKey=1",
		)
	})

	t.Run("Integer exclusion search LT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentFixedSize(),
			buildPKRecordAllFinal(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 2)},
			"intKey < 1",
		)
	})

	t.Run("Integer exclusion search GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentFixedSize(),
			buildPKRecordAllFinal(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 2), fragment.NewFragmentRange(3, 4)},
			"intKey > 1",
		)
	})

	t.Run("Float exclusion search EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentFixedSize(),
			buildPKRecordAllFinal(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 2), fragment.NewFragmentRange(3, 4)},
			"floatKey=1.0",
		)
	})

	t.Run("Float exclusion search LT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentFixedSize(),
			buildPKRecordAllFinal(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(1, 2), fragment.NewFragmentRange(3, 4)},
			"floatKey < 1.0",
		)
	})

	t.Run("Float exclusion search GT", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentFixedSize(),
			buildPKRecordAllFinal(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"floatKey > 1.0",
		)
	})
}
