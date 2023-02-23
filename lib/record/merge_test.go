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

package record_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

type mergeItem struct {
	order      []float64
	orderTimes []int64

	unordered      []float64
	unorderedTimes []int64

	expTimes []int64
	expData  []float64
	expLen   int
	expNil   int
}

func appendData(col *record.ColVal, val []float64) {
	for _, v := range val {
		if v == -1 {
			col.AppendFloatNull()
			continue
		}

		col.AppendFloat(v)
	}
}

func TestHelper_MergeCol(t *testing.T) {
	items := []mergeItem{
		{
			order:          []float64{1.1, 1.2, 1.3, 1.4, 1.5},
			unordered:      []float64{2.1, 2.2, 2.3, 2.4, 2.5},
			orderTimes:     []int64{1, 2, 6, 7, 12},
			unorderedTimes: []int64{3, 4, 6, 9, 10},
			expTimes:       []int64{1, 2, 3, 4, 6, 7, 9, 10, 12},
			expData:        []float64{1.1, 1.2, 2.1, 2.2, 2.3, 1.4, 2.4, 2.5, 1.5},
			expLen:         9,
			expNil:         0,
		},
		{
			order:          []float64{1.1, -1, 1.3, -1, 1.5},
			unordered:      []float64{2.1, 2.2, -1, -1, 2.5},
			orderTimes:     []int64{1, 4, 6, 7, 12},
			unorderedTimes: []int64{3, 4, 6, 9, 10},
			expTimes:       []int64{1, 3, 4, 6, 7, 9, 10, 12},
			expData:        []float64{1.1, 2.1, 2.2, 1.3, 2.5, 1.5},
			expLen:         8,
			expNil:         2,
		},
		{
			order:          []float64{1.1, -1, 1.3, -1, 1.5},
			unordered:      []float64{2.1, 2.2, -1, -1, 2.5},
			orderTimes:     []int64{1, 2, 3, 4, 5},
			unorderedTimes: []int64{11, 12, 13, 14, 15},
			expTimes:       []int64{1, 2, 3, 4, 5, 11, 12, 13, 14, 15},
			expData:        []float64{1.1, 1.3, 1.5, 2.1, 2.2, 2.5},
			expLen:         10,
			expNil:         4,
		},
	}

	hlp := record.NewMergeHelper()
	for _, item := range items {
		order := &record.ColVal{}
		appendData(order, item.order)
		unordered := &record.ColVal{}
		appendData(unordered, item.unordered)

		hlp.AddUnorderedCol(unordered, item.unorderedTimes, influx.Field_Type_Float)

		merged, mergedTimes, err := hlp.Merge(order, item.orderTimes, influx.Field_Type_Float)
		if !assert.NoError(t, err) {
			return
		}

		if !assert.Equal(t, item.expTimes, mergedTimes) {
			return
		}
		if !assert.Equal(t, item.expData, merged.FloatValues()) {
			return
		}
		if !assert.Equal(t, item.expLen, merged.Length()) {
			return
		}
		if !assert.Equal(t, item.expNil, merged.NilCount) {
			return
		}
	}

}
