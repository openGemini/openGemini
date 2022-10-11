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

package record

func booleanCompareLess(a, b bool) bool {
	if a == b {
		return false
	}
	return !a
}

func getIntegerFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.Times()
	var row int
	var value int64
	var t int64
	defer func() {
		dscRe.AppendRecForAggTagSet(re, row, row+1)
	}()
	values := re.ColVals[index].IntegerValues()
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			if v, isNil := re.ColVals[index].IntegerValue(i); !isNil {
				row = i
				value = v
				t = times[i]
				break
			}
		}

		start := row
		for i := start; i < re.RowNums(); i++ {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				if compare(times[i], t) || (times[i] == t && value < v) {
					row = i
					value = v
					t = times[i]
				}
			}
		}
		return
	}
	t = times[0]
	value = values[0]
	row = 0
	for i := 1; i < re.RowNums(); i++ {
		if compare(times[i], t) || (times[i] == t && value < values[i]) {
			row = i
			value = values[i]
			t = times[i]
		}
	}
}

func getFloatFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.Times()
	var row int
	var value float64
	var t int64
	defer func() {
		dscRe.AppendRecForAggTagSet(re, row, row+1)
	}()
	values := re.ColVals[index].FloatValues()
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			if v, isNil := re.ColVals[index].FloatValue(i); !isNil {
				row = i
				value = v
				t = times[i]
				break
			}
		}

		start := row
		for i := start; i < re.RowNums(); i++ {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				if compare(times[i], t) || (times[i] == t && value < v) {
					row = i
					value = v
					t = times[i]
				}
			}
		}
		return
	}
	t = times[0]
	value = values[0]
	row = 0
	for i := 1; i < re.RowNums(); i++ {
		if compare(times[i], t) || (times[i] == t && value < values[i]) {
			row = i
			value = values[i]
			t = times[i]
		}
	}
}

func getBooleanFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.Times()
	var row int
	var value bool
	var t int64
	defer func() {
		dscRe.AppendRecForAggTagSet(re, row, row+1)
	}()
	values := re.ColVals[index].BooleanValues()
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			if v, isNil := re.ColVals[index].BooleanValue(i); !isNil {
				row = i
				value = v
				t = times[i]
				break
			}
		}

		start := row
		for i := start; i < re.RowNums(); i++ {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				if compare(times[i], t) || (times[i] == t && booleanCompareLess(value, v)) {
					row = i
					value = v
					t = times[i]
				}
			}
		}
		return
	}
	row = 0
	t = times[row]
	//value = values[row]
	for i := 1; i < re.RowNums(); i++ {
		if compare(times[i], t) || (times[i] == t && values[i]) {
			row = i
			//value = values[i]
			t = times[i]
		}
	}
}

func getStringFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.Times()
	var row int
	var value string
	var t int64
	defer func() {
		dscRe.AppendRecForAggTagSet(re, row, row+1)
	}()
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			if v, isNil := re.ColVals[index].StringValueSafe(i); !isNil {
				row = i
				value = v
				t = times[i]
				break
			}
		}

		start := row
		for i := start; i < re.RowNums(); i++ {
			v, isNil := re.ColVals[index].StringValueSafe(i)
			if (compare(times[i], t) && !isNil) || (times[i] == t && !isNil && value < v) {
				row = i
				value = v
				t = times[i]
			}
		}
		return
	}
	t = times[0]
	value, _ = re.ColVals[index].StringValueSafe(0)
	row = 0
	for i := 1; i < re.RowNums(); i++ {
		v, _ := re.ColVals[index].StringValueSafe(i)
		if compare(times[i], t) || (times[i] == t && value < v) {
			row = i
			value = v
			t = times[i]
		}
	}
}

func GetRecordIntegerLast(dscRe, re *Record, index int) {
	getIntegerFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}

func GetRecordFloatLast(dscRe, re *Record, index int) {
	getFloatFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}

func GetRecordBooleanLast(dscRe, re *Record, index int) {
	getBooleanFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}
func GetRecordStringLast(dscRe, re *Record, index int) {
	getStringFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}

func GetRecordIntegerFirst(dscRe, re *Record, index int) {
	getIntegerFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}

func GetRecordFloatFirst(dscRe, re *Record, index int) {
	getFloatFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}
func GetRecordBooleanFirst(dscRe, re *Record, index int) {
	getBooleanFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}
func GetRecordStringFirst(dscRe, re *Record, index int) {
	getStringFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}

func getColumnIntegerFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.RecMeta.Times[index]
	var t int64
	defer func() {
		dscRe.RecMeta.Times[index] = append(dscRe.RecMeta.Times[index], t)
	}()
	values := re.ColVals[index].IntegerValues()
	var value int64
	var start int
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				value = v
				t = times[i]
				start = i
				break
			}
			if i == len(times)-1 {
				dscRe.ColVals[index].AppendIntegerNull()
				return
			}
		}
		for i := start; i < re.RowNums(); i++ {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				if compare(times[i], t) || (times[i] == t && value < v) {
					value = v
					t = times[i]
				}
			}
		}
		dscRe.ColVals[index].AppendInteger(value)
		return
	}

	t = times[0]
	value = values[0]
	for i := 1; i < re.RowNums(); i++ {
		if compare(times[i], t) || (times[i] == t && value < values[i]) {
			value = values[i]
			t = times[i]
		}
	}
	dscRe.ColVals[index].AppendInteger(value)
}

func getColumnFloatFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.RecMeta.Times[index]
	var t int64
	defer func() {
		dscRe.RecMeta.Times[index] = append(dscRe.RecMeta.Times[index], t)
	}()
	values := re.ColVals[index].FloatValues()
	var value float64
	var start int
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				value = v
				t = times[i]
				start = i
				break
			}
			if i == len(times)-1 {
				dscRe.ColVals[index].AppendFloatNull()
				return
			}
		}

		for i := start; i < re.RowNums(); i++ {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				if compare(times[i], t) || (times[i] == t && value < v) {
					value = v
					t = times[i]
				}
			}
		}
		dscRe.ColVals[index].AppendFloat(value)
		return
	}

	t = times[0]
	value = values[0]
	for i := 1; i < re.RowNums(); i++ {
		if compare(times[i], t) || (times[i] == t && value < values[i]) {
			value = values[i]
			t = times[i]
		}
	}
	dscRe.ColVals[index].AppendFloat(value)
}

func getColumnBooleanFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.RecMeta.Times[index]
	var t int64
	defer func() {
		dscRe.RecMeta.Times[index] = append(dscRe.RecMeta.Times[index], t)
	}()
	values := re.ColVals[index].BooleanValues()
	var value bool
	var start int
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			isNil := re.ColVals[index].IsNil(i)

			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				value = v
				t = times[i]
				start = i
				break
			}
			if i == len(times)-1 {
				dscRe.ColVals[index].AppendBooleanNull()
				return
			}
		}

		for i := start; i < re.RowNums(); i++ {
			isNil := re.ColVals[index].IsNil(i)
			if !isNil {
				v := values[re.ColVals[index].ValidCount(0, i)]
				if compare(times[i], t) || (times[i] == t && booleanCompareLess(value, v)) {
					value = v
					t = times[i]
				}
			}
		}
		dscRe.ColVals[index].AppendBoolean(value)
		return
	}
	t = times[0]
	value = values[0]
	for i := 1; i < re.RowNums(); i++ {
		if compare(times[i], t) || (times[i] == t && values[i]) {
			value = values[i]
			t = times[i]
		}
	}
	dscRe.ColVals[index].AppendBoolean(value)
}

func getColumnStringFirstLastImp(dscRe, re *Record, index int, compare func(a, b int64) bool) {
	times := re.RecMeta.Times[index]
	var t int64
	defer func() {
		dscRe.RecMeta.Times[index] = append(dscRe.RecMeta.Times[index], t)
	}()
	var value string
	var start int
	if re.ColVals[index].NilCount != 0 {
		for i := range times {
			if v, isNil := re.ColVals[index].StringValueSafe(i); !isNil {
				value = v
				t = times[i]
				start = i
				break
			}
			if i == len(times)-1 {
				dscRe.ColVals[index].AppendStringNull()
				return
			}
		}

		for i := start; i < re.RowNums(); i++ {
			v, isNil := re.ColVals[index].StringValueSafe(i)
			if (compare(times[i], t) && !isNil) || (times[i] == t && !isNil && value < v) {
				value = v
				t = times[i]
			}
		}
		dscRe.ColVals[index].AppendString(value)
		return
	}
	t = times[0]
	value, _ = re.ColVals[index].StringValueSafe(0)
	for i := 1; i < re.RowNums(); i++ {
		v, _ := re.ColVals[index].StringValueSafe(i)
		if compare(times[i], t) || (times[i] == t && value < v) {
			value = v
			t = times[i]
		}
	}
	dscRe.ColVals[index].AppendString(value)
}

func GetRecordColumnIntegerFirst(dscRe, re *Record, index int) {
	getColumnIntegerFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}
func GetRecordColumnFloatFirst(dscRe, re *Record, index int) {
	getColumnFloatFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}
func GetRecordColumnBooleanFirst(dscRe, re *Record, index int) {
	getColumnBooleanFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}
func GetRecordColumnStringFirst(dscRe, re *Record, index int) {
	getColumnStringFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a < b
	})
}

func GetRecordColumnIntegerLast(dscRe, re *Record, index int) {
	getColumnIntegerFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}
func GetRecordColumnFloatLast(dscRe, re *Record, index int) {
	getColumnFloatFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}
func GetRecordColumnBooleanLast(dscRe, re *Record, index int) {
	getColumnBooleanFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}
func GetRecordColumnStringLast(dscRe, re *Record, index int) {
	getColumnStringFirstLastImp(dscRe, re, index, func(a, b int64) bool {
		return a > b
	})
}

func GetRecordColumnIntegerMin(dstRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	v, row := re.ColVals[index].MinIntegerValue(values, 0, re.RowNums())
	if row == -1 {
		dstRe.ColVals[index].AppendIntegerNull()
		return
	}
	dstRe.ColVals[index].AppendInteger(v)
}

func GetRecordColumnFloatMin(dstRe, re *Record, index int) {
	values := re.ColVals[index].FloatValues()
	v, row := re.ColVals[index].MinFloatValue(values, 0, re.RowNums())
	if row == -1 {
		dstRe.ColVals[index].AppendFloatNull()
		return
	}
	dstRe.ColVals[index].AppendFloat(v)
}

func GetRecordColumnBooleanMin(dstRe, re *Record, index int) {
	values := re.ColVals[index].BooleanValues()
	v, row := re.ColVals[index].MinBooleanValue(values, 0, re.RowNums())
	if row == -1 {
		dstRe.ColVals[index].AppendBooleanNull()
		return
	}
	dstRe.ColVals[index].AppendBoolean(v)
}

func GetRecordColumnIntegerMax(dstRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	v, row := re.ColVals[index].MaxIntegerValue(values, 0, re.RowNums())
	if row == -1 {
		dstRe.ColVals[index].AppendIntegerNull()
		return
	}
	dstRe.ColVals[index].AppendInteger(v)
}
func GetRecordColumnFloatMax(dstRe, re *Record, index int) {
	values := re.ColVals[index].FloatValues()
	v, row := re.ColVals[index].MaxFloatValue(values, 0, re.RowNums())
	if row == -1 {
		dstRe.ColVals[index].AppendFloatNull()
		return
	}
	dstRe.ColVals[index].AppendFloat(v)
}
func GetRecordColumnBooleanMax(dstRe, re *Record, index int) {
	values := re.ColVals[index].BooleanValues()
	v, row := re.ColVals[index].MaxBooleanValue(values, 0, re.RowNums())
	if row == -1 {
		dstRe.ColVals[index].AppendBooleanNull()
		return
	}
	dstRe.ColVals[index].AppendBoolean(v)
}

func getRecordMinMaxImp(dscRe, re *Record, rows []int) {
	var row int
	rowNum := len(rows)
	if rowNum == 0 {
		dscRe.ColumnAppendNull(1)
		return
	}
	defer func() {
		r := re
		dscRe.AppendRecForAggTagSet(r, row, row+1)
	}()
	if rowNum == 1 {
		row = rows[0]
		return
	}
	var t int64
	t = -1
	times := re.Times()
	for i := range rows {
		if t < 0 || t > times[i] {
			t = times[i]
			row = rows[i]
		}
	}
}

func GetRecordIntegerMin(dscRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	_, rows := re.ColVals[index].MinIntegerValues(values, 0, re.RowNums())
	getRecordMinMaxImp(dscRe, re, rows)
}

func GetRecordFloatMin(dscRe, re *Record, index int) {
	values := re.ColVals[index].FloatValues()
	_, rows := re.ColVals[index].MinFloatValues(values, 0, re.RowNums())
	getRecordMinMaxImp(dscRe, re, rows)
}

func GetRecordBooleanMin(dscRe, re *Record, index int) {
	values := re.ColVals[index].BooleanValues()
	_, rows := re.ColVals[index].MinBooleanValues(values, 0, re.RowNums())
	getRecordMinMaxImp(dscRe, re, rows)
}

func GetRecordIntegerMax(dscRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	_, rows := re.ColVals[index].MaxIntegerValues(values, 0, re.RowNums())
	getRecordMinMaxImp(dscRe, re, rows)
}

func GetRecordFloatMax(dscRe, re *Record, index int) {
	values := re.ColVals[index].FloatValues()
	_, rows := re.ColVals[index].MaxFloatValues(values, 0, re.RowNums())
	getRecordMinMaxImp(dscRe, re, rows)
}
func GetRecordBooleanMax(dscRe, re *Record, index int) {
	values := re.ColVals[index].BooleanValues()
	_, rows := re.ColVals[index].MaxBooleanValues(values, 0, re.RowNums())
	getRecordMinMaxImp(dscRe, re, rows)
}

func GetRecordColumnIntegerSum(dstRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	v := integerSum(values)
	dstRe.ColVals[index].AppendInteger(v)
}

func GetRecordColumnFloatSum(dstRe, re *Record, index int) {
	values := re.ColVals[index].FloatValues()
	v := floatSum(values)
	dstRe.ColVals[index].AppendFloat(v)
}

func GetRecordColumnCount(dstRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	v := integerSum(values)
	if v == 0 {
		dstRe.ColVals[index].AppendIntegerNull()
		return
	}
	dstRe.ColVals[index].AppendInteger(v)
}

func GetRecordIntegerSum(dscRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	v := integerSum(values)
	dscRe.ColVals[index].AppendInteger(v)
	dscRe.AppendTime(re.Time(0))
}
func GetRecordFloatSum(dscRe, re *Record, index int) {
	values := re.ColVals[index].FloatValues()
	v := floatSum(values)
	dscRe.ColVals[index].AppendFloat(v)
	dscRe.AppendTime(re.Time(0))
}

// GetRecordCount used for count aggregated call
// only for multiple series record which has been count with method count += 1.
func GetRecordCount(dscRe, re *Record, index int) {
	values := re.ColVals[index].IntegerValues()
	v := integerSum(values)
	if v == 0 {
		dscRe.ColVals[index].AppendIntegerNull()
	} else {
		dscRe.ColVals[index].AppendInteger(v)
	}
	dscRe.AppendTime(re.Time(0))

}

func GetRecordDefault(dscRe, re *Record, index int) {
	dscRe.AppendRecForAggTagSet(re, 0, re.RowNums())
}

func floatSum(values []float64) float64 {
	var value float64
	for _, v := range values {
		value += v
	}
	return value
}

func integerSum(values []int64) int64 {
	var value int64
	for _, v := range values {
		value += v
	}
	return value
}
