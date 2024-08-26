// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"math"

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
