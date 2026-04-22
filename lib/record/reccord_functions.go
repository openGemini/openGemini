// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import "math"

func booleanCompareLessEqual(a, b bool) bool {
	if a == b {
		return true
	}
	return !a
}

func booleanCompareLess(a, b bool) bool {
	if a == b {
		return false
	}
	return !a
}

func booleanCompareGreaterEqual(a, b bool) bool {
	if a == b {
		return true
	}
	return a
}

func booleanCompareGreater(a, b bool) bool {
	if a == b {
		return false
	}
	return a
}

func updateIntegerFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, isFast bool, compare func(t1, t2 int64) bool) {
	var v int64
	if isFast {
		v = rec.ColVals[recColumn].IntegerValues()[recRow]
	} else {
		var isNil bool
		v, isNil = rec.ColVals[recColumn].IntegerValue(recRow)
		if isNil {
			return
		}
	}

	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if compare(t1, t2) {
		iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	if !isSrcNil && compare(t2, t1) {
		return
	}
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateIntegerFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateIntegerFirstFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateIntegerLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func UpdateIntegerLastFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func updateFloatFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, isFast bool, compare func(t1, t2 int64) bool) {
	var v float64
	if isFast {
		v = rec.ColVals[recColumn].FloatValues()[recRow]
	} else {
		var isNil bool
		v, isNil = rec.ColVals[recColumn].FloatValue(recRow)
		if isNil {
			return
		}
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if compare(t1, t2) {
		iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if !isSrcNil && compare(t2, t1) {
		return
	}
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateFloatFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateFloatFirstFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateFloatLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func UpdateFloatLastFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func updateBooleanFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, isFast bool, compare func(t1, t2 int64) bool) {
	var v bool
	if isFast {
		v = rec.ColVals[recColumn].BooleanValues()[recRow]
	} else {
		var isNil bool
		v, isNil = rec.ColVals[recColumn].BooleanValue(recRow)
		if isNil {
			return
		}
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if compare(t1, t2) {
		iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].BooleanValueWithNullReserve(iRecRow)
	if !isSrcNil && compare(t2, t1) {
		return
	}
	if booleanCompareGreaterEqual(srcVal, v) && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateBooleanFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateBooleanFirstFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateBooleanLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func UpdateBooleanLastFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func updateStringFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, compare func(t1, t2 int64) bool) {
	v, isNil := rec.ColVals[recColumn].StringValueUnsafe(recRow)
	if isNil {
		return
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if compare(t1, t2) {
		iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].StringValueUnsafe(iRecRow)
	if !isSrcNil && compare(t2, t1) {
		return
	}
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateStringFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateStringFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateStringLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateStringFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func updateIntegerColumnFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, isFast bool, compare func(t1, t2 int64) bool) {
	var v int64
	if isFast {
		v = rec.ColVals[recColumn].IntegerValues()[recRow]
	} else {
		var isNil bool
		v, isNil = rec.ColVals[recColumn].IntegerValue(recRow)
		if isNil {
			return
		}
	}
	if compare(iRec.RecMeta.Times[iRecColumn][iRecRow], rec.RecMeta.Times[recColumn][recRow]) {
		iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
		iRec.ColVals[iRecColumn].UpdateIntegerValue(v, false, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	if !isSrcNil && compare(rec.RecMeta.Times[recColumn][recRow], iRec.RecMeta.Times[iRecColumn][iRecRow]) {
		return
	}
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateIntegerValue(v, false, iRecRow)
	iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
}

func UpdateIntegerColumnFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateIntegerColumnFirstFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateIntegerColumnLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func UpdateIntegerColumnLastFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateIntegerColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func updateFloatColumnFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, isFast bool, compare func(t1, t2 int64) bool) {
	var v float64
	if isFast {
		v = rec.ColVals[recColumn].FloatValues()[recRow]
	} else {
		var isNil bool
		v, isNil = rec.ColVals[recColumn].FloatValue(recRow)
		if isNil {
			return
		}
	}

	if compare(iRec.RecMeta.Times[iRecColumn][iRecRow], rec.RecMeta.Times[recColumn][recRow]) {
		iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
		iRec.ColVals[iRecColumn].UpdateFloatValue(v, false, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if !isSrcNil && compare(rec.RecMeta.Times[recColumn][recRow], iRec.RecMeta.Times[iRecColumn][iRecRow]) {
		return
	}
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateFloatValue(v, false, iRecRow)
	iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
}

func UpdateFloatColumnFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateFloatColumnFirstFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateFloatColumnLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func UpdateFloatColumnLastFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func updateBooleanColumnFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, isFast bool, compare func(t1, t2 int64) bool) {
	var v bool
	if isFast {
		v = rec.ColVals[recColumn].BooleanValues()[recRow]
	} else {
		var isNil bool
		v, isNil = rec.ColVals[recColumn].BooleanValue(recRow)
		if isNil {
			return
		}
	}

	if compare(iRec.RecMeta.Times[iRecColumn][iRecRow], rec.RecMeta.Times[recColumn][recRow]) {
		iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
		iRec.ColVals[iRecColumn].UpdateBooleanValue(v, false, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].BooleanValueWithNullReserve(iRecRow)
	if !isSrcNil && compare(rec.RecMeta.Times[recColumn][recRow], iRec.RecMeta.Times[iRecColumn][iRecRow]) {
		return
	}
	if booleanCompareGreaterEqual(srcVal, v) && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateBooleanValue(v, false, iRecRow)
	iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
}

func UpdateBooleanColumnFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateBooleanColumnFirstFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateBooleanColumnLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, false, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func UpdateBooleanColumnLastFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateBooleanColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, true, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func updateStringColumnFirstLastImp(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int, compare func(t1, t2 int64) bool) {
	v, isNil := rec.ColVals[recColumn].StringValueSafe(recRow)
	if isNil {
		return
	}
	if compare(iRec.RecMeta.Times[iRecColumn][iRecRow], rec.RecMeta.Times[recColumn][recRow]) {
		iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
		iRec.ColVals[iRecColumn].UpdateStringValue(v, false, iRecRow)
		return
	}
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].StringValueUnsafe(iRecRow)
	if !isSrcNil && compare(rec.RecMeta.Times[recColumn][recRow], iRec.RecMeta.Times[iRecColumn][iRecRow]) {
		return
	}
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateStringValue(v, false, iRecRow)
	iRec.RecMeta.Times[iRecColumn][iRecRow] = rec.RecMeta.Times[recColumn][recRow]
}

func UpdateStringColumnFirst(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateStringColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, func(t1, t2 int64) bool {
		return t1 > t2
	})
}

func UpdateStringColumnLast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateStringColumnFirstLastImp(iRec, rec, recColumn, iRecColumn, recRow, iRecRow, func(t1, t2 int64) bool {
		return t1 < t2
	})
}

func UpdateIntegerMin(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].IntegerValue(recRow)
	if isNil {
		return
	}
	updateIntegerMinImpl(v, iRec, rec, iRecColumn, recRow, iRecRow)
}

func updateIntegerMinImpl(v int64, iRec, rec *Record, iRecColumn, recRow, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	if srcVal < v && !isSrcNil {
		return
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if srcVal == v && t1 <= t2 && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateIntegerMinFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].IntegerValues()[recRow]
	updateIntegerMinImpl(v, iRec, rec, iRecColumn, recRow, iRecRow)
}

func UpdateIntegerMax(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].IntegerValue(recRow)
	if isNil {
		return
	}
	updateIntegerMaxImpl(v, iRec, rec, iRecColumn, recRow, iRecRow)
}

func updateIntegerMaxImpl(v int64, iRec, rec *Record, iRecColumn, recRow, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	if srcVal > v && !isSrcNil {
		return
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if srcVal == v && t1 <= t2 && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateIntegerMaxFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].IntegerValues()[recRow]
	updateIntegerMaxImpl(v, iRec, rec, iRecColumn, recRow, iRecRow)
}

func UpdateFloatMin(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].FloatValue(recRow)
	if isNil {
		return
	}
	updateFloatMinImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func updateFloatMinImpl(v float64, iRec, rec *Record, recRow, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if srcVal < v && !isSrcNil {
		return
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].FloatValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].FloatValue(recRow)
	if srcVal == v && t1 <= t2 && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateFloatMinFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	updateFloatMinImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func UpdateFloatMax(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].FloatValue(recRow)
	if isNil {
		return
	}
	updateFloatMaxImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func updateFloatMaxImpl(v float64, iRec, rec *Record, recRow, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if srcVal > v && !isSrcNil {
		return
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].FloatValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].FloatValue(recRow)
	if srcVal == v && t1 <= t2 && !isSrcNil {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateFloatMaxFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	updateFloatMaxImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func UpdateBooleanMin(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].BooleanValue(recRow)
	if isNil {
		return
	}
	updateBooleanMinImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func updateBooleanMinImpl(v bool, iRec, rec *Record, recRow, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].BooleanValueWithNullReserve(iRecRow)
	if booleanCompareLess(srcVal, v) && !isSrcNil {
		return
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if t1 <= t2 && !isSrcNil && srcVal == v {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateBooleanMinFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].BooleanValues()[recRow]
	updateBooleanMinImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func UpdateBooleanMax(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].BooleanValue(recRow)
	if isNil {
		return
	}
	updateBooleanMaxImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func updateBooleanMaxImpl(v bool, iRec, rec *Record, recRow, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].BooleanValueWithNullReserve(iRecRow)
	if booleanCompareGreater(srcVal, v) && !isSrcNil {
		return
	}
	t1, _ := iRec.ColVals[len(iRec.Schema)-1].IntegerValueWithNullReserve(iRecRow)
	t2, _ := rec.ColVals[len(rec.Schema)-1].IntegerValue(recRow)
	if t1 <= t2 && !isSrcNil && srcVal == v {
		return
	}
	iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
}

func UpdateBooleanMaxFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].BooleanValues()[recRow]
	updateBooleanMaxImpl(v, iRec, rec, recRow, iRecColumn, iRecRow)
}

func UpdateIntegerColumnMin(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].IntegerValue(recRow)
	if isNil {
		return
	}
	updateIntegerColumnMinImpl(v, iRec, iRecColumn, iRecRow)
}

func updateIntegerColumnMinImpl(v int64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	if srcVal <= v && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateIntegerValue(v, false, iRecRow)
}

func UpdateIntegerColumnMinFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].IntegerValues()[recRow]
	updateIntegerColumnMinImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateIntegerColumnMax(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].IntegerValue(recRow)
	if isNil {
		return
	}
	updateIntegerColumnMaxImpl(v, iRec, iRecColumn, iRecRow)
}

func updateIntegerColumnMaxImpl(v int64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateIntegerValue(v, false, iRecRow)
}

func UpdateIntegerColumnMaxFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].IntegerValues()[recRow]
	updateIntegerColumnMaxImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateFloatColumnMin(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].FloatValue(recRow)
	if isNil {
		return
	}
	updateFloatColumnMinImpl(v, iRec, iRecColumn, iRecRow)
}

func updateFloatColumnMinImpl(v float64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if srcVal <= v && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateFloatValue(v, false, iRecRow)
}

func UpdateFloatColumnMinFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	updateFloatColumnMinImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateFloatColumnMax(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].FloatValue(recRow)
	if isNil {
		return
	}
	updateFloatColumnMaxImpl(v, iRec, iRecColumn, iRecRow)
}

func updateFloatColumnMaxImpl(v float64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if srcVal >= v && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateFloatValue(v, false, iRecRow)
}

func UpdateFloatColumnMaxFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	updateFloatColumnMaxImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateBooleanColumnMin(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].BooleanValue(recRow)
	if isNil {
		return
	}
	updateBooleanColumnMinImpl(v, iRec, iRecColumn, iRecRow)
}

func updateBooleanColumnMinImpl(v bool, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].BooleanValueWithNullReserve(iRecRow)
	if booleanCompareLessEqual(srcVal, v) && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateBooleanValue(v, false, iRecRow)
}

func UpdateBooleanColumnMinFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].BooleanValues()[recRow]
	updateBooleanColumnMinImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateBooleanColumnMax(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].BooleanValue(recRow)
	if isNil {
		return
	}
	updateBooleanColumnMaxImpl(v, iRec, iRecColumn, iRecRow)
}

func updateBooleanColumnMaxImpl(v bool, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].BooleanValueWithNullReserve(iRecRow)
	if booleanCompareGreaterEqual(srcVal, v) && !isSrcNil {
		return
	}
	iRec.ColVals[iRecColumn].UpdateBooleanValue(v, false, iRecRow)
}

func UpdateBooleanColumnMaxFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].BooleanValues()[recRow]
	updateBooleanColumnMaxImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateIntegerSum(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].IntegerValue(recRow)
	if isNil {
		return
	}
	updateIntegerSumImpl(v, iRec, iRecColumn, iRecRow)
}

func updateIntegerSumImpl(v int64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, _ := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	iRec.ColVals[iRecColumn].UpdateIntegerValue(v+srcVal, false, iRecRow)
}

func UpdateIntegerSumFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].IntegerValues()[recRow]
	updateIntegerSumImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateFloatSum(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].FloatValue(recRow)
	if isNil {
		return
	}
	updateFloatSumImpl(v, iRec, iRecColumn, iRecRow)
}

func updateFloatSumImpl(v float64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal := iRec.ColVals[iRecColumn].FloatValues()[iRecRow]
	iRec.ColVals[iRecColumn].UpdateFloatValue(v+srcVal, false, iRecRow)
}

func UpdateFloatSumFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	updateFloatSumImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateCount(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v, isNil := rec.ColVals[recColumn].IntegerValue(recRow)
	if isNil {
		return
	}
	updateCountImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateFloatCountProm(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	updateFloatCountPromImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateFloatCountOriginProm(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	updateFloatCountPromImpl(1.0, iRec, iRecColumn, iRecRow)
}

func updateCountImpl(v int64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, _ := iRec.ColVals[iRecColumn].IntegerValueWithNullReserve(iRecRow)
	iRec.ColVals[iRecColumn].UpdateIntegerValue(v+srcVal, false, iRecRow)
}

func updateFloatCountPromImpl(v float64, iRec *Record, iRecColumn, iRecRow int) {
	srcVal, _ := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	iRec.ColVals[iRecColumn].UpdateFloatValue(v+srcVal, false, iRecRow)
}

func UpdateCountFast(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].IntegerValues()[recRow]
	updateCountImpl(v, iRec, iRecColumn, iRecRow)
}

func UpdateMinProm(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if isSrcNil || srcVal > v || math.IsNaN(srcVal) {
		iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
	}
}

func UpdateMaxProm(iRec, rec *Record, recColumn, iRecColumn, recRow, iRecRow int) {
	v := rec.ColVals[recColumn].FloatValues()[recRow]
	srcVal, isSrcNil := iRec.ColVals[iRecColumn].FloatValueWithNullReserve(iRecRow)
	if isSrcNil || srcVal < v || math.IsNaN(srcVal) {
		iRec.UpdateIntervalRecRow(rec, recRow, iRecRow)
	}
}
