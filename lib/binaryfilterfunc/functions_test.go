/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package binaryfilterfunc

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
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

func BenchmarkStringCompareByBytes(b *testing.B) {
	col, bitMap := prepareStringColValue(1, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetStringLTConditionBitMap(col, RandomString+"-4096", col.Bitmap, bitMap, 0)
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
