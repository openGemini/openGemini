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

package binaryfilterfunc

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestRotate(t *testing.T) {
	root := &influxql.BinaryExpr{
		Op: influxql.AND,
		RHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "f1"},
			Op:  influxql.EQ,
			RHS: &influxql.IntegerLiteral{Val: 2},
		},
		LHS: &influxql.BinaryExpr{
			Op: influxql.OR,
			RHS: &influxql.BinaryExpr{
				LHS: &influxql.VarRef{Val: "f2"},
				Op:  influxql.EQ,
				RHS: &influxql.IntegerLiteral{Val: 4},
			},
			LHS: &influxql.BinaryExpr{
				Op: influxql.AND,
				LHS: &influxql.BinaryExpr{
					Op: influxql.OR,
					LHS: &influxql.BinaryExpr{
						LHS: &influxql.VarRef{Val: "f1"},
						Op:  influxql.EQ,
						RHS: &influxql.IntegerLiteral{Val: 1},
					},
					RHS: &influxql.BinaryExpr{
						LHS: &influxql.VarRef{Val: "f2"},
						Op:  influxql.EQ,
						RHS: &influxql.IntegerLiteral{Val: 2},
					},
				},
				RHS: &influxql.BinaryExpr{
					LHS: &influxql.VarRef{Val: "f2"},
					Op:  influxql.EQ,
					RHS: &influxql.IntegerLiteral{Val: 2},
				},
			},
		},
	}
	expExpr := "f1 = 1 OR f2 = 2 AND f2 = 2 OR f2 = 4 AND f1 = 2"
	expParts := 3
	newRoot := MoveOrOpToRoot(root)
	if newRoot.String() != expExpr {
		t.Fatal()
	}
	splitRoots := SplitWithOrOperation(newRoot)
	if len(splitRoots) != expParts {
		t.Fatal()
	}

}

func TestGetStringMatchPhraseConditionBitMap(t *testing.T) {
	col, bitMap := prepareStringColValue(1, 8192)
	params := &TypeFunParams{
		col:     col,
		compare: RandomString + "-4096",
		bitMap:  col.Bitmap,
		pos:     bitMap,
		offset:  0,
		opt:     &query.ProcessorOptions{},
	}
	results := GetStringMatchPhraseConditionBitMap(params)
	emptyCount := 0
	for _, v := range results {
		if v == 0 {
			emptyCount += 1
		}
	}
	assert.Equal(t, 1023, emptyCount)

	results = GetStringMatchPhraseConditionBitMapWithNull(params)
	emptyCount = 0
	for _, v := range results {
		if v == 0 {
			emptyCount += 1
		}
	}
	assert.Equal(t, 1023, emptyCount)
}

func TestRotateRewriteTimeCompareVal(t *testing.T) {
	root := &influxql.BinaryExpr{
		Op: influxql.AND,
		RHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.GTE,
			RHS: &influxql.StringLiteral{Val: "2023-06-19T00:00:00Z"},
		},
		LHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.LTE,
			RHS: &influxql.StringLiteral{Val: "2023-06-19T09:00:00Z"},
		},
	}
	valuer := influxql.NowValuer{Now: time.Now(), Location: nil}
	RewriteTimeCompareVal(root, &valuer)
	expected := &influxql.BinaryExpr{
		Op: influxql.AND,
		RHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.GTE,
			RHS: &influxql.IntegerLiteral{Val: 1687132800000000000},
		},
		LHS: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			Op:  influxql.LTE,
			RHS: &influxql.IntegerLiteral{Val: 1687165200000000000},
		},
	}
	assert.Equal(t, root, expected)

	root = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.Call{Name: "now"},
	}
	valuer = influxql.NowValuer{Now: time.Now(), Location: nil}
	now := time.Now()
	RewriteTimeCompareVal(root, &valuer)
	expected = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.IntegerLiteral{Val: 1687132800000000000},
	}

	root = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.TimeLiteral{Val: time.Unix(0, 1687132800000000000)},
	}
	valuer = influxql.NowValuer{Now: now, Location: nil}
	RewriteTimeCompareVal(root, &valuer)
	expected = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.IntegerLiteral{Val: 1687132800000000000},
	}

	root = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.DurationLiteral{Val: 1687132800000000000},
	}
	valuer = influxql.NowValuer{Now: now, Location: nil}
	RewriteTimeCompareVal(root, &valuer)
	expected = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.IntegerLiteral{Val: 1687132800000000000},
	}

	root = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.NumberLiteral{Val: 1687132800000000000},
	}
	valuer = influxql.NowValuer{Now: now, Location: nil}
	RewriteTimeCompareVal(root, &valuer)
	expected = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.IntegerLiteral{Val: 1687132800000000000},
	}
	assert.Equal(t, root, expected)

	defer func() {
		if err := recover(); err != nil {
			assert.Equal(t, strings.Contains(err.(string), "unsupported data type for time filter"), true)
		}
	}()
	root = &influxql.BinaryExpr{
		LHS: &influxql.VarRef{Val: "time"},
		Op:  influxql.GTE,
		RHS: &influxql.BooleanLiteral{Val: false},
	}
	valuer = influxql.NowValuer{Now: now, Location: nil}
	RewriteTimeCompareVal(root, &valuer)
}

var (
	fieldMap = map[string]influxql.DataType{
		"country": influxql.String,
		"name":    influxql.String,
		"age":     influxql.Integer,
		"height":  influxql.Float,
		"address": influxql.String,
		"alive":   influxql.Boolean,
		"time":    influxql.Integer,
	}
	inSchema = record.Schemas{
		record.Field{Name: "country", Type: influx.Field_Type_String},
		record.Field{Name: "name", Type: influx.Field_Type_String},
		record.Field{Name: "age", Type: influx.Field_Type_Int},
		record.Field{Name: "height", Type: influx.Field_Type_Float},
		record.Field{Name: "address", Type: influx.Field_Type_String},
		record.Field{Name: "alive", Type: influx.Field_Type_Boolean},
		record.Field{Name: "time", Type: influx.Field_Type_Int},
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

func getTypeString(ty int) string {
	switch ty {
	case influx.Field_Type_String:
		return "string"
	case influx.Field_Type_Int:
		return "integer"
	case influx.Field_Type_Float:
		return "float"
	case influx.Field_Type_Boolean:
		return "bool"
	default:
		return ""
	}
}

func getElemString(e *RPNElement) string {
	switch e.op {
	case rpn.InRange:
		if inSchema.Field(e.rg.Idx).Type == influx.Field_Type_String {
			return fmt.Sprintf("%s::%s %s '%v'",
				inSchema.Field(e.rg.Idx).Name,
				getTypeString(inSchema.Field(e.rg.Idx).Type),
				e.rg.Op.String(),
				e.rg.Compare)
		}
		return fmt.Sprintf("%s::%s %s %v",
			inSchema.Field(e.rg.Idx).Name,
			getTypeString(inSchema.Field(e.rg.Idx).Type),
			e.rg.Op.String(),
			e.rg.Compare)
	case rpn.AND:
		return "and"
	case rpn.OR:
		return "or"
	default:
		return ""
	}
}

func getCondString(c *ConditionImpl) string {
	var b = strings.Builder{}
	for i, elem := range c.rpn {
		b.WriteString(getElemString(elem))
		if i < len(c.rpn)-1 {
			b.WriteString(" | ")
		}
	}
	return b.String()
}

func TestCondition(t *testing.T) {
	condStr := "__log___ = 1"
	fieldMap["__log__"] = influxql.Integer
	condExpr := MustParseExpr(condStr)
	opt := query.ProcessorOptions{
		Sources: []influxql.Source{
			&influxql.Measurement{
				Name: "students",
				IndexRelation: &influxql.IndexRelation{IndexNames: []string{index.BloomFilterFullTextIndex},
					Oids:      []uint32{uint32(index.BloomFilterFullText)},
					IndexList: []*influxql.IndexList{{IList: []string{"country"}}},
				},
			},
		},
	}
	_, err := NewCondition(nil, condExpr, inSchema, &opt)
	assert.True(t, errno.Equal(err, errno.ErrValueTypeFullTextIndex))
}

func TestConditionToRPN(t *testing.T) {
	f := func(
		timeStr string,
		condStr string,
		expected string,
	) {
		var timeExpr influxql.Expr
		if len(timeStr) > 0 {
			timeExpr = MustParseExpr(timeStr)
		}
		var condExpr influxql.Expr
		if len(condStr) > 0 {
			condExpr = MustParseExpr(condStr)
		}
		opt := query.ProcessorOptions{
			Sources: []influxql.Source{
				&influxql.Measurement{
					Name: "students",
					IndexRelation: &influxql.IndexRelation{IndexNames: []string{index.BloomFilterFullTextIndex},
						Oids:      []uint32{uint32(index.BloomFilterFullText)},
						IndexList: []*influxql.IndexList{&influxql.IndexList{IList: []string{"country"}}},
					},
				},
			},
		}
		condition, err := NewCondition(timeExpr, condExpr, inSchema, &opt)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, expected, getCondString(condition))
	}

	t.Run("1", func(t *testing.T) {
		// 1 and 2 and 3 and 4 => 1 2 3 4 and and and
		f(
			"",
			"country = 'china' and country  = 'american' and address = 'shenzhen' and address = 'shanghai'",
			"country::string = 'china' | country::string = 'american' | and | address::string = 'shenzhen' | and | address::string = 'shanghai' | and",
		)
	})

	t.Run("2", func(t *testing.T) {
		// time and 1 and 2 and (3 or 4) => time 1 2 3 4 or and and and
		f(
			"time >= 1 and time <= 2",
			"country = 'china' and country = 'american' and (address = 'shenzhen' or address = 'shanghai')",
			"time::integer >= 1 | time::integer <= 2 | and | country::string = 'china' | country::string = 'american' | and | address::string = 'shenzhen' | address::string = 'shanghai' | or | and | and",
		)
	})

	t.Run("3", func(t *testing.T) {
		// 1 and 2 and (3 or 4)  = >  1 2 3 4 or and and
		f(
			"",
			"country = 'china' and country  = 'american' and (address = 'shenzhen' or address = 'shanghai')",
			"country::string = 'china' | country::string = 'american' | and | address::string = 'shenzhen' | address::string = 'shanghai' | or | and",
		)
	})

	t.Run("4", func(t *testing.T) {
		// 1 and (2 and (3 or 4)) => 1 2 3 4 or and and
		f(
			"",
			"country = 'china' and (country  = 'american' and (address = 'shenzhen' or address = 'shanghai'))",
			"country::string = 'china' | country::string = 'american' | address::string = 'shenzhen' | address::string = 'shanghai' | or | and | and",
		)
	})

	t.Run("5", func(t *testing.T) {
		// (1 or 2 ) and (3 or 4) => 1 2 or 3 4 or and
		f(
			"",
			"(country = 'china' or country  = 'american') and (address = 'shenzhen' or address = 'shanghai')",
			"country::string = 'china' | country::string = 'american' | or | address::string = 'shenzhen' | address::string = 'shanghai' | or | and",
		)
	})

	t.Run("6", func(t *testing.T) {
		// 1 and 2 and 3 or 4 => 1 2 and 3 and 4 or
		f(
			"",
			"country = 'china' and country  = 'american' and address = 'shenzhen' or address = 'shanghai'",
			"country::string = 'china' | country::string = 'american' | and | address::string = 'shenzhen' | and | address::string = 'shanghai' | or",
		)
	})

	t.Run("7", func(t *testing.T) {
		// (1 and 2) and 3 or 4  = >  1 2 and 3 and 4 or
		f(
			"",
			"(country = 'china' and country  = 'american') and address = 'shenzhen' or address = 'shanghai'",
			"country::string = 'china' | country::string = 'american' | and | address::string = 'shenzhen' | and | address::string = 'shanghai' | or",
		)
	})

	t.Run("8", func(t *testing.T) {
		// ((1 and 2) and 3) or 4 => 1 2 and 3 and 4 or
		f(
			"",
			"((country = 'china' and country  = 'american') and address = 'shenzhen') or address = 'shanghai'",
			"country::string = 'china' | country::string = 'american' | and | address::string = 'shenzhen' | and | address::string = 'shanghai' | or",
		)
	})
	t.Run("9", func(t *testing.T) {
		// 1 and 2 and 3 and 4 => 1 2 3 4 and and and
		f(
			"",
			"__log___ = 'shanghai'",
			"country::string MATCHPHRASE 'shanghai'",
		)
	})
}

const RandomString = "aaaaabbbbbcccccdddddeeeeefffff"

func prepareStringColValue(colNum, colSize int) (*record.ColVal, []byte) {
	col := &record.ColVal{}
	for i := 0; i < colNum; i++ {
		for j := 0; j < colSize; j++ {
			col.AppendString(fmt.Sprintf("%s-%d", RandomString, j))
		}
	}
	var bitMap []byte
	bitNum, bitRemain := (colNum*colSize)/8, (colNum*colSize)%8
	if bitRemain > 0 {
		bitNum++
	}
	for i := 0; i < bitNum; i++ {
		bitMap = append(bitMap, byte(255))
	}
	return col, bitMap
}

func prepareIntegerColValue(colNum, colSize int) (*record.ColVal, []byte) {
	col := &record.ColVal{}
	for i := 0; i < colNum; i++ {
		for j := 0; j < colSize; j++ {
			col.AppendInteger(int64(i*colSize + j))
		}
	}
	var bitMap []byte
	bitNum, bitRemain := (colNum*colSize)/8, (colNum*colSize)%8
	if bitRemain > 0 {
		bitNum++
	}
	for i := 0; i < bitNum; i++ {
		bitMap = append(bitMap, byte(255))
	}
	return col, bitMap
}

func GetStringLTConditionBitMapByBytes(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var idx, index int
	cmpData, _ := compare.(string)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			index++
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, index)
			continue
		}

		if index == len(col.Offset)-1 {
			if util.Bytes2str(col.Val[col.Offset[index]:]) > cmpData {
				bitmap.SetBitMap(pos, index)
			}
		} else {
			if util.Bytes2str(col.Val[col.Offset[index]:col.Offset[index+1]]) > cmpData {
				bitmap.SetBitMap(pos, index)
			}
		}
		index++
	}
	return pos
}

func GetStringLTConditionBitMapByStrings(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var idx, index int
	cmpData, _ := compare.(string)
	values := col.StringValues(nil)

	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			index++
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, index)
			continue
		}
		if values[index] > cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func TestGetIntegerLTEConditionBitMapWithoutNull(t *testing.T) {
	schema := []record.Field{
		{Name: "cpu", Type: influx.Field_Type_Int},
		{Name: "time", Type: influx.Field_Type_Int}}
	rec := record.NewRecordBuilder(schema)
	rec.ColVals[0].AppendIntegers(1, 2, 3, 4)
	rec.AppendTime(1, 2, 3, 4)
	dstRec := record.NewRecord(schema, false)
	dstRec.SliceFromRecord(rec, 3, 4)
	var bitMap []byte
	col := &dstRec.ColVals[1]
	bitMap = append(bitMap, dstRec.ColVals[1].Bitmap...)
	p := &TypeFunParams{
		col:     col,
		compare: int64(3),
		pos:     bitMap,
		bitMap:  col.Bitmap,
		offset:  col.BitMapOffset,
		opt:     &query.ProcessorOptions{},
	}
	GetIntegerLTEConditionBitMapWithoutNull(p)
	assert.Equal(t, bitMap, []uint8{0x7})
}

func TestGetStringIPInRangeBitMapWithoutNull(t *testing.T) {
	schema := []record.Field{
		{Name: "srcIp", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int}}
	rec := record.NewRecordBuilder(schema)
	rec.ColVals[0].AppendStrings("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4")
	rec.AppendTime(1, 2, 3, 4)
	dstRec := record.NewRecord(schema, false)
	dstRec.SliceFromRecord(rec, 0, 4)
	var bitMap []byte
	col := &dstRec.ColVals[0]
	bitMap = append(bitMap, dstRec.ColVals[0].Bitmap...)
	p := &TypeFunParams{
		col:     col,
		compare: "3.3.3.0/24",
		pos:     bitMap,
		bitMap:  col.Bitmap,
		offset:  col.BitMapOffset,
		opt:     &query.ProcessorOptions{},
	}
	GetStringIPInRangeBitMap(p)
	assert.Equal(t, bitMap, []uint8{0x4})
}

func TestGetStringIPInRangeBitMapWithNull(t *testing.T) {
	schema := []record.Field{
		{Name: "srcIp", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int}}
	rec := record.NewRecordBuilder(schema)
	rec.ColVals[0].AppendStrings("1.1.1.1", "3.3.3.3", "4.4.4.4")
	rec.ColVals[0].AppendStringNull()
	rec.AppendTime(1, 2, 3, 4)
	dstRec := record.NewRecord(schema, false)
	dstRec.SliceFromRecord(rec, 0, 4)
	var bitMap []byte
	col := &dstRec.ColVals[0]
	bitMap = append(bitMap, dstRec.ColVals[0].Bitmap...)
	p := &TypeFunParams{
		col:     col,
		compare: "3.3.3.0/24",
		pos:     bitMap,
		bitMap:  col.Bitmap,
		offset:  col.BitMapOffset,
		opt:     &query.ProcessorOptions{},
	}
	GetStringIPInRangeBitMap(p)
	assert.Equal(t, bitMap, []uint8{0x2})
}

func TestIsIpInRange(t *testing.T) {
	type args struct {
		ipStr     string
		subnetStr string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test success",
			args: args{
				ipStr:     "1.1.1.1",
				subnetStr: "1.1.1.0/24",
			},
			want: true,
		},
		{
			name: "test nil ip",
			args: args{
				ipStr:     "1.1.1.",
				subnetStr: "1.1.1.1/24",
			},
			want: false,
		},
		{
			name: "test success",
			args: args{
				ipStr:     "1.1.1.1",
				subnetStr: "1.1.1.1",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsIpInRange(tt.args.ipStr, tt.args.subnetStr), "IsIpInRange(%v, %v)", tt.args.ipStr, tt.args.subnetStr)
		})
	}
}

func BenchmarkStringCompareByBytes(b *testing.B) {
	col, bitMap := prepareStringColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	params := &TypeFunParams{
		col:     col,
		compare: RandomString + "-4096",
		bitMap:  col.Bitmap,
		pos:     bitMap,
		offset:  0,
	}
	for i := 0; i < b.N; i++ {
		GetStringLTConditionBitMap(params)
	}
}

func BenchmarkStringCompareByString(b *testing.B) {
	col, bitMap := prepareStringColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetStringLTConditionBitMapByBytes(col, RandomString+"-4096", col.Bitmap, bitMap, 0)
	}
}

func BenchmarkStringCompareByStrings(b *testing.B) {
	col, bitMap := prepareStringColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetStringLTConditionBitMapByStrings(col, RandomString+"-4096", col.Bitmap, bitMap, 0)
	}
}

func BenchmarkGetStringEQConditionBitMap(b *testing.B) {
	col, bitMap := prepareStringColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	params := &TypeFunParams{
		col:     col,
		compare: RandomString + "-4096",
		bitMap:  col.Bitmap,
		pos:     bitMap,
		offset:  0,
	}
	for i := 0; i < b.N; i++ {
		GetStringEQConditionBitMap(params)
	}
}

func BenchmarkGetStringEQConditionBitMapWithNull(b *testing.B) {
	col, bitMap := prepareStringColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	params := &TypeFunParams{
		col:     col,
		compare: RandomString + "-4096",
		bitMap:  col.Bitmap,
		pos:     bitMap,
		offset:  0,
	}
	for i := 0; i < b.N; i++ {
		GetStringEQConditionBitMapWithNull(params)
	}
}

func BenchmarkGetIntegerLTEConditionBitMap(b *testing.B) {
	col, bitMap := prepareIntegerColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	params := &TypeFunParams{
		col:     col,
		compare: 4096,
		bitMap:  col.Bitmap,
		pos:     bitMap,
		offset:  0,
	}
	for i := 0; i < b.N; i++ {
		GetIntegerLTEConditionBitMap(params)
	}
}

func BenchmarkGetIntegerLTEConditionBitMapWithNull(b *testing.B) {
	col, bitMap := prepareIntegerColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	params := &TypeFunParams{
		col:     col,
		compare: 4096,
		bitMap:  col.Bitmap,
		pos:     bitMap,
		offset:  0,
	}
	for i := 0; i < b.N; i++ {
		GetIntegerLTEConditionBitMapWithNull(params)
	}
}
