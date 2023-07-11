// Generated by tmpl
// https://github.com/benbjohnson/tmpl
//
// DO NOT EDIT!
// Source: eval_generator.gen.go.tmpl

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

package binaryfilterfunc

import (
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/record"
)

func GetFloatLTConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.FloatValues()
	var idx int
	var index int
	cmpData, _ := compare.(float64)

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

		if values[index] >= cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetIntegerLTConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)

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

		if values[index] >= cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetStringLTConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var values []string
	values = col.StringValues(values)
	var idx int
	var index int
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

		if values[index] >= cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetFloatLTEConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.FloatValues()
	var idx int
	var index int
	cmpData, _ := compare.(float64)

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

func GetIntegerLTEConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)

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

func GetStringLTEConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var values []string
	values = col.StringValues(values)
	var idx int
	var index int
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

		if values[index] > cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetFloatGTConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.FloatValues()
	var idx int
	var index int
	cmpData, _ := compare.(float64)

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

		if values[index] <= cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetIntegerGTConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)

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

		if values[index] <= cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetStringGTConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var values []string
	values = col.StringValues(values)
	var idx int
	var index int
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

		if values[index] <= cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetFloatGTEConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.FloatValues()
	var idx int
	var index int
	cmpData, _ := compare.(float64)

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

		if values[index] < cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetIntegerGTEConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)

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

		if values[index] < cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetStringGTEConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var values []string
	values = col.StringValues(values)
	var idx int
	var index int
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

		if values[index] < cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetFloatEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.FloatValues()
	var idx int
	var index int
	cmpData, _ := compare.(float64)

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

		if values[index] != cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetIntegerEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)

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

		if values[index] != cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetStringEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var values []string
	values = col.StringValues(values)
	var idx int
	var index int
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

		if values[index] != cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetBooleanEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.BooleanValues()
	var idx int
	var index int
	cmpData, _ := compare.(bool)

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

		if values[index] != cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetFloatNEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.FloatValues()
	var idx int
	var index int
	cmpData, _ := compare.(float64)

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

		if values[index] == cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetIntegerNEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)

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

		if values[index] == cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetStringNEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	var values []string
	values = col.StringValues(values)
	var idx int
	var index int
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

		if values[index] == cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}

func GetBooleanNEQConditionBitMap(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
	values := col.BooleanValues()
	var idx int
	var index int
	cmpData, _ := compare.(bool)

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

		if values[index] == cmpData {
			bitmap.SetBitMap(pos, index)
		}
		index++
	}
	return pos
}
