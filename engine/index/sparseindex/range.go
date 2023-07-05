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

package sparseindex

import (
	"math"

	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

// Range means that the range with open or closed ends, possibly unbounded.
type Range struct {
	left          *FieldRef
	right         *FieldRef
	leftIncluded  bool
	rightIncluded bool
}

func NewRange(left, right *FieldRef, li, ri bool) *Range {
	return &Range{
		left:          left,
		right:         right,
		leftIncluded:  li,
		rightIncluded: ri,
	}
}

// turnOpenRangeIntoClosed convert an open range to a closed range. for example, turn (0, 3) into [1, 2].
func (r *Range) turnOpenRangeIntoClosed() {
	if len(r.left.cols) > 0 && !r.leftIncluded && r.left.cols[r.left.column].dataType == influx.Field_Type_Int {
		if val, _ := r.left.cols[r.left.column].column.IntegerValue(r.left.row); val != math.MaxInt64 {
			r.left.cols[r.left.column].column.UpdateIntegerValue(val+1, false, r.left.row)
			r.leftIncluded = true
		}
	}
	if len(r.right.cols) > 0 && !r.rightIncluded && r.right.cols[r.right.column].dataType == influx.Field_Type_Int {
		if val, _ := r.right.cols[r.right.column].column.IntegerValue(r.right.row); val != math.MinInt64 {
			r.right.cols[r.right.column].column.UpdateIntegerValue(val-1, false, r.right.row)
			r.rightIncluded = true
		}
	}
}

func (r *Range) empty() bool {
	return r.right.Less(r.left) || ((!r.leftIncluded || !r.rightIncluded) && !r.left.Less(r.right))
}

func (r *Range) contains(x *FieldRef) bool {
	return !r.rightGEQ(x) && !r.leftLEQ(x)
}

// leftLEQ x is to the right for the left point of the range.
func (r *Range) leftLEQ(x *FieldRef) bool {
	return r.left.Less(x) || (r.leftIncluded && x.Equals(r.left))
}

// rightGEQ x is to the left for the right point of the range.
func (r *Range) rightGEQ(x *FieldRef) bool {
	return x.Less(r.right) || (r.rightIncluded && x.Equals(r.right))
}

// rightLQ nr is to the right for the range.
func (r *Range) rightLQ(nr *Range) bool {
	return r.right.Less(nr.left) || ((!r.rightIncluded || !nr.leftIncluded) && nr.left.Equals(r.right))
}

func (r *Range) intersectsRange(nr *Range) bool {
	return !(r.rightLQ(nr) || nr.rightLQ(r))
}

func (r *Range) containsRange(nr *Range) bool {
	return r.leftLEQ(nr.left) && r.rightGEQ(nr.right)
}

// createWholeRangeIncludeBound create an infinite range, including boundaries.
func createWholeRangeIncludeBound() *Range {
	return NewRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, true, true)
}

// createWholeRangeWithoutBound create an infinite range, excluding boundaries.
func createWholeRangeWithoutBound() *Range {
	return NewRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, false, false)
}

func createRightBounded(right *FieldRef, ri bool, isInfinityIncluded bool) *Range {
	var nr *Range
	if isInfinityIncluded {
		nr = createWholeRangeIncludeBound()
	} else {
		nr = createWholeRangeWithoutBound()
	}
	nr.right = right
	nr.rightIncluded = ri
	nr.turnOpenRangeIntoClosed()
	// Special case for [-Inf, -Inf]
	if nr.right.IsNegativeInfinity() && ri {
		nr.leftIncluded = true
	}
	return nr
}

func createLeftBounded(left *FieldRef, li bool, isInfinityIncluded bool) *Range {
	var nr *Range
	if isInfinityIncluded {
		nr = createWholeRangeIncludeBound()
	} else {
		nr = createWholeRangeWithoutBound()
	}
	nr.left = left
	nr.leftIncluded = li
	nr.turnOpenRangeIntoClosed()
	// Special case for [+Inf, +Inf]
	if nr.left.IsPositiveInfinity() && li {
		nr.rightIncluded = true
	}
	return nr
}
