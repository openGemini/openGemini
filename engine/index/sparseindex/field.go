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

package sparseindex

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var NEGATIVE_INFINITY = &FieldRef{row: math.MinInt64}

var POSITIVE_INFINITY = &FieldRef{row: math.MaxInt64}

type FieldRef struct {
	column int
	row    int
	cols   []*ColumnRef
}

func NewFieldRef(cols []*ColumnRef, column int, row int) *FieldRef {
	return &FieldRef{
		cols:   cols,
		column: column,
		row:    row,
	}
}

func (f *FieldRef) Equals(rhs *FieldRef) bool {
	if f.IsPositiveInfinity() || f.IsNegativeInfinity() || rhs.IsPositiveInfinity() || rhs.IsNegativeInfinity() {
		return (f.IsPositiveInfinity() && rhs.IsPositiveInfinity()) || (f.IsNegativeInfinity() && rhs.IsNegativeInfinity())
	}

	if f.cols[f.column].dataType != rhs.cols[rhs.column].dataType {
		panic("the data type of lhs and rhs in a range is different")
	}

	switch f.cols[f.column].dataType {
	case influx.Field_Type_Int:
		lhsVal, isNil1 := f.cols[f.column].column.IntegerValue(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.IntegerValue(rhs.row)
		return (!isNil1 && !isNil2 && lhsVal == rhsVal) || (isNil1 && isNil2)
	case influx.Field_Type_Float:
		lhsVal, isNil1 := f.cols[f.column].column.FloatValue(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.FloatValue(rhs.row)
		return (!isNil1 && !isNil2 && lhsVal == rhsVal) || (isNil1 && isNil2)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		lhsVal, isNil1 := f.cols[f.column].column.BytesUnsafe(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.BytesUnsafe(rhs.row)
		return (!isNil1 && !isNil2 && bytes.Equal(lhsVal, rhsVal)) || (isNil1 && isNil2)
	case influx.Field_Type_Boolean:
		lhsVal, isNil1 := f.cols[f.column].column.BooleanValue(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.BooleanValue(rhs.row)
		return (!isNil1 && !isNil2 && lhsVal == rhsVal) || (isNil1 && isNil2)
	default:
		panic("not support the data type")
	}
}

func (f *FieldRef) MatchedIndices(ColVal *record.ColVal, compOp influxql.Token, isFirstPK bool) ([]int, error) {
	// f.cols[f.column] is valid
	switch f.cols[f.column].dataType {
	case influx.Field_Type_String:
		threshold, _ := f.cols[f.column].column.StringValueSafe(f.row)
		pkValues := ColVal.StringValues(nil)
		if isFirstPK {
			return matchedIndicesWithBinarySearch(pkValues, compOp, threshold)
		} else {
			return matchedIndicesWithLinearSearch(pkValues, compOp, threshold)
		}
	case influx.Field_Type_Int:
		threshold, _ := f.cols[f.column].column.IntegerValue(f.row)
		pkValues := ColVal.IntegerValues()
		if isFirstPK {
			return matchedIndicesWithBinarySearch(pkValues, compOp, threshold)
		} else {
			return matchedIndicesWithLinearSearch(pkValues, compOp, threshold)
		}
	case influx.Field_Type_Float:
		threshold, _ := f.cols[f.column].column.FloatValue(f.row)
		pkValues := ColVal.FloatValues()
		if isFirstPK {
			return matchedIndicesWithBinarySearch(pkValues, compOp, threshold)
		} else {
			return matchedIndicesWithLinearSearch(pkValues, compOp, threshold)
		}
	case influx.Field_Type_Boolean:
		threshold, _ := f.cols[f.column].column.BooleanValue(f.row)
		pkValues := ColVal.BooleanValues()
		thresholdInt64 := util.Bool2Int64(threshold)
		pkValuesInt64 := make([]int64, len(pkValues))
		for i, val := range pkValues {
			pkValuesInt64[i] = util.Bool2Int64(val)
		}
		if isFirstPK {
			return matchedIndicesWithBinarySearch(pkValuesInt64, compOp, thresholdInt64)
		} else {
			return matchedIndicesWithLinearSearch(pkValuesInt64, compOp, thresholdInt64)
		}
	default:
		return nil, fmt.Errorf("unsupported primary key data type: %d", f.cols[f.column].dataType)
	}
}

// the first PK column is sorted
func matchedIndicesWithBinarySearch[T util.ExceptBool](pkValues []T, compOp influxql.Token, threshold T) ([]int, error) {
	n := len(pkValues)
	var indices []int
	switch compOp {
	case influxql.EQ:
		first := sort.Search(n, func(i int) bool { return pkValues[i] >= threshold })
		if first < n && pkValues[first] == threshold {
			last := sort.Search(n, func(i int) bool { return pkValues[i] > threshold }) - 1
			indices = makeRange(first, last)
		}
	case influxql.LT:
		idx := sort.Search(n, func(i int) bool { return pkValues[i] >= threshold })
		indices = makeRange(0, idx-1)
	case influxql.LTE:
		idx := sort.Search(n, func(i int) bool { return pkValues[i] > threshold })
		indices = makeRange(0, idx-1)
	case influxql.GT:
		idx := sort.Search(n, func(i int) bool { return pkValues[i] > threshold })
		indices = makeRange(idx, n-1)
	case influxql.GTE:
		idx := sort.Search(n, func(i int) bool { return pkValues[i] >= threshold })
		indices = makeRange(idx, n-1)
	default:
		return nil, ErrUnknownOperator(compOp.String())
	}
	return indices, nil
}

func makeRange(start, end int) []int {
	if start > end {
		return nil
	}
	rangeLen := end - start + 1
	rangeIndices := make([]int, rangeLen)
	for i := range rangeLen {
		rangeIndices[i] = start + i
	}
	return rangeIndices
}

func matchedIndicesWithLinearSearch[T util.ExceptBool](pkValues []T, compOp influxql.Token, threshold T) ([]int, error) {
	var indices []int
	for i, s := range pkValues {
		isOK, err := compareVal(compOp, s, threshold)
		if err != nil {
			return nil, err
		}
		if isOK {
			indices = append(indices, i)
		}
	}
	return indices, nil
}

func compareVal[T util.ExceptBool](compOp influxql.Token, val1, val2 T) (bool, error) {
	switch compOp {
	case influxql.EQ:
		return val1 == val2, nil
	case influxql.LT:
		return val1 < val2, nil
	case influxql.LTE:
		return val1 <= val2, nil
	case influxql.GT:
		return val1 > val2, nil
	case influxql.GTE:
		return val1 >= val2, nil
	default:
		return false, ErrUnknownOperator(compOp.String())
	}
}

func (f *FieldRef) Less(rhs *FieldRef) bool {
	if f.IsPositiveInfinity() || f.IsNegativeInfinity() || rhs.IsPositiveInfinity() || rhs.IsNegativeInfinity() {
		res := (f.IsNegativeInfinity() && !rhs.IsNegativeInfinity()) || (!f.IsPositiveInfinity() && rhs.IsPositiveInfinity())
		return res
	}
	if f.cols[f.column].dataType != rhs.cols[rhs.column].dataType {
		panic("the data type of lhs and rhs in a range is different")
	}

	switch f.cols[f.column].dataType {
	case influx.Field_Type_Int:
		lhsVal, isNil1 := f.cols[f.column].column.IntegerValue(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.IntegerValue(rhs.row)
		return (!isNil1 && !isNil2 && lhsVal < rhsVal) || (isNil1 && !isNil2)
	case influx.Field_Type_Float:
		lhsVal, isNil1 := f.cols[f.column].column.FloatValue(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.FloatValue(rhs.row)
		return (!isNil1 && !isNil2 && lhsVal < rhsVal) || (isNil1 && !isNil2)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		lhsVal, isNil1 := f.cols[f.column].column.BytesUnsafe(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.BytesUnsafe(rhs.row)
		return (!isNil1 && !isNil2 && bytes.Compare(lhsVal, rhsVal) < 0) || (isNil1 && !isNil2)
	case influx.Field_Type_Boolean:
		lhsVal, isNil1 := f.cols[f.column].column.BooleanValue(f.row)
		rhsVal, isNil2 := rhs.cols[rhs.column].column.BooleanValue(rhs.row)
		return (!isNil1 && !isNil2 && !lhsVal && rhsVal) || (isNil1 && !isNil2)
	default:
		panic("not support the data type")
	}
}

func (f *FieldRef) Set(cols []*ColumnRef, column, row int) {
	f.cols = cols
	f.column = column
	f.row = row
}

func (f *FieldRef) IsNull() bool {
	return f.cols[f.column].column.IsNil(f.row)
}

func (f *FieldRef) SetPositiveInfinity() {
	f.row = math.MaxInt64
}

func (f *FieldRef) SetNegativeInfinity() {
	f.row = math.MinInt64
}

func (f *FieldRef) IsPositiveInfinity() bool {
	return f.row == math.MaxInt64
}

func (f *FieldRef) IsNegativeInfinity() bool {
	return f.row == math.MinInt64
}
