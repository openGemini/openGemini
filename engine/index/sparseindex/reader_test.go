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

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/stretchr/testify/assert"
)

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

func buildDataRecordComplex() *record.Record {
	schema := buildPKSchema()
	rec := record.NewRecord(schema, false)
	rec.Column(0).AppendStrings("U1", "U2", "U2", "U3", "U3", "U4", "U4", "U4")
	rec.Column(1).AppendStrings("W1", "W2", "W2", "W1", "W2", "W1", "W1", "W2")
	return rec
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
	indexReader := sparseindex.NewIndexReader(2, 2, 0)
	f := func(
		pkFile string,
		pkMark fragment.IndexFragment,
		pkRec *record.Record,
		expectFragmentRanges fragment.FragmentRanges,
		conStr string,
	) {
		pkSchema := pkRec.Schema
		KeyConditionImpl := sparseindex.NewKeyCondition(MustParseExpr(conStr), pkSchema)
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
	indexReader := sparseindex.NewIndexReader(2, 2, 0)
	f := func(
		pkFile string,
		pkMark fragment.IndexFragment,
		pkRec *record.Record,
		expectFragmentRanges fragment.FragmentRanges,
		conStr string,
	) {
		pkSchema := pkRec.Schema
		KeyConditionImpl := sparseindex.NewKeyCondition(MustParseExpr(conStr), pkSchema)
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

	// TODO: check
	t.Run("exclusion search EQ AND EQ", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentVariable(),
			buildPkRecordComplex(),
			nil,
			"UserID='U1' and URL='W3'",
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
}

func TestScanWithAllType(t *testing.T) {
	pkFile := "00000001-0001-00000000.idx"
	indexReader := sparseindex.NewIndexReader(2, 2, 0)
	f := func(
		pkFile string,
		pkMark fragment.IndexFragment,
		pkRec *record.Record,
		expectFragmentRanges fragment.FragmentRanges,
		conStr string,
	) {
		pkSchema := pkRec.Schema
		KeyConditionImpl := sparseindex.NewKeyCondition(MustParseExpr(conStr), pkSchema)
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

	t.Run("Float exclusion search IN", func(t *testing.T) {
		f(
			pkFile,
			buildIndexFragmentFixedSize(),
			buildPKRecordAllFinal(),
			[]*fragment.FragmentRange{fragment.NewFragmentRange(0, 4)},
			"floatKey in [1.0]",
		)
	})
}
