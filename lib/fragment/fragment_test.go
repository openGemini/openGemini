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

package fragment_test

import (
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/assert"
)

func TestFragmentRange(t *testing.T) {
	fr1 := fragment.NewFragmentRange(0, 2)
	fr2 := fragment.NewFragmentRange(2, 3)
	fr3 := fragment.NewFragmentRange(0, 1)

	assert.Equal(t, fr1.Equal(fr2), false)

	less, err := fr1.Less(fr2)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, less, true)

	less, err = fr1.Less(fr3)
	if err != nil {
		assert.Equal(t, strings.Contains(err.Error(), "Intersecting mark ranges are not allowed"), true)
	}
	assert.Equal(t, less, false)
}

func TestFragmentRanges(t *testing.T) {
	var frs fragment.FragmentRanges
	fr1 := fragment.NewFragmentRange(0, 2)
	fr2 := fragment.NewFragmentRange(2, 3)
	fr3 := fragment.NewFragmentRange(0, 1)

	frs = append(frs, fr1)
	frs = append(frs, fr2)
	frs = append(frs, fr3)

	assert.Equal(t, frs.String(), "(0, 2), (2, 3), (0, 1)")
	assert.Equal(t, frs.GetLastFragment(), uint32(3))
}

func TestIndexFragment(t *testing.T) {
	var frags []uint64
	rowCount := []uint32{2, 4, 6, 8}
	segCount := []uint32{1, 2, 3, 4}
	for i := 0; i < len(rowCount); i++ {
		frags = append(frags, uint64(util.MergeToInt64(segCount[i], rowCount[i])))
	}
	f := fragment.NewIndexFragmentVariable(frags)
	assert.Equal(t, f.GetFragmentCount(), uint32(4))
	assert.Equal(t, f.GetRowsCountInRange(0, 1), uint64(2))
	assert.Equal(t, f.GetTotalRowsCount(), uint64(8))
	assert.Equal(t, f.(*fragment.IndexFragmentVariableImpl).GetLastFragmentRows(), uint64(2))
	assert.Equal(t, f.Empty(), false)
	f.(*fragment.IndexFragmentVariableImpl).AppendFragment(2)
	assert.Equal(t, f.GetTotalRowsCount(), uint64(10))
	f.(*fragment.IndexFragmentVariableImpl).AddRowsToLastFragment(2)
	assert.Equal(t, f.GetTotalRowsCount(), uint64(12))
	f.PopFragment()
	assert.Equal(t, f.GetTotalRowsCount(), uint64(8))

	f1 := fragment.NewIndexFragmentFixedSize(uint32(2), uint64(2))
	assert.Equal(t, f1.GetFragmentCount(), uint32(2))
	assert.Equal(t, f1.GetRowsCountInRange(0, 1), uint64(2))
	assert.Equal(t, f1.GetTotalRowsCount(), uint64(4))
	f1.PopFragment()
	assert.Equal(t, f1.GetTotalRowsCount(), uint64(2))
	assert.Equal(t, f1.Empty(), false)
}

func TestIntersectAndUnion(t *testing.T) {
	frs := fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 5}}

	other1 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 5}}
	result1 := frs.Intersect(other1)
	expect1 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 5}}
	assert.Equal(t, expect1, result1)

	resultUnion1 := frs.Union(other1)
	expectUnion1 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 5}}
	assert.Equal(t, expectUnion1, resultUnion1)

	other2 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 3, End: 7}}
	result2 := frs.Intersect(other2)
	expect2 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 3, End: 5}}
	assert.Equal(t, expect2, result2)

	resultUnion2 := frs.Union(other2)
	expectUnion2 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 7}}
	assert.Equal(t, expectUnion2, resultUnion2)

	other3 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 6, End: 10}}
	result3 := frs.Intersect(other3)
	expect3 := fragment.FragmentRanges(nil)
	assert.Equal(t, expect3, result3)

	resultUnion3 := frs.Union(other3)
	expectUnion3 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 5}, &fragment.FragmentRange{Start: 6, End: 10}}
	assert.Equal(t, expectUnion3, resultUnion3)

	other4 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 4}}
	result4 := frs.Intersect(other4)
	expect4 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 4}}
	assert.Equal(t, expect4, result4)

	resultUnion4 := frs.Union(other4)
	expectUnion4 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 5}}
	assert.Equal(t, expectUnion4, resultUnion4)

	frs5 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 4}, &fragment.FragmentRange{Start: 6, End: 8}, &fragment.FragmentRange{Start: 10, End: 20}}
	other5 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 4, End: 5}, &fragment.FragmentRange{Start: 7, End: 12}}
	result5 := frs5.Intersect(other5)
	expect5 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 7, End: 8}, &fragment.FragmentRange{Start: 10, End: 12}}
	assert.Equal(t, expect5, result5)

	resultUnion5 := frs5.Union(other5)
	expectUnion5 := fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 5}, &fragment.FragmentRange{Start: 6, End: 20}}
	assert.Equal(t, expectUnion5, resultUnion5)
}

func TestComplementRanges(t *testing.T) {
	frs := fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 10}}
	subset := fragment.FragmentRanges(nil)
	result := frs.Complement(subset)
	expect := fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 10}}
	assert.Equal(t, expect, result)

	subset = fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 10}}
	result = frs.Complement(subset)
	expect = fragment.FragmentRanges(nil)
	assert.Equal(t, expect, result)

	subset = fragment.FragmentRanges{&fragment.FragmentRange{Start: 3, End: 6}}
	result = frs.Complement(subset)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 3}, &fragment.FragmentRange{Start: 6, End: 10}}
	assert.Equal(t, expect, result)

	subset = fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 2}, &fragment.FragmentRange{Start: 4, End: 6}}
	result = frs.Complement(subset)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 4}, &fragment.FragmentRange{Start: 6, End: 10}}
	assert.Equal(t, expect, result)
}

func TestCalcFragmentRanges(t *testing.T) {
	indices := []int{}
	result := fragment.CalcFragmentRanges(indices)
	expect := fragment.FragmentRanges(nil)
	assert.Equal(t, expect, result)

	indices = []int{0, 1, 2, 3, 4}
	result = fragment.CalcFragmentRanges(indices)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 5}}
	assert.Equal(t, expect, result)

	indices = []int{2, 3, 4}
	result = fragment.CalcFragmentRanges(indices)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 5}}
	assert.Equal(t, expect, result)

	indices = []int{2, 4, 5}
	result = fragment.CalcFragmentRanges(indices)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 3}, &fragment.FragmentRange{Start: 4, End: 6}}
	assert.Equal(t, expect, result)
}

func TestConvertToSegmentRanges(t *testing.T) {
	// segment layout：
	// 0：(0,1)
	// 1：(2)
	// 2：(3)
	// 3：(4,5)
	// 4：(6)
	// 5：(7)
	segmentIDsLayout := []int64{2, 3, 4, 6, 7, 8}

	frs := fragment.FragmentRanges{&fragment.FragmentRange{Start: 3, End: 3}}
	_, err := frs.ConvertToSegmentRanges(segmentIDsLayout)
	assert.Error(t, err, "invalid fragment ranges when calculate segemnt ranges from fragment ranges")

	frs = fragment.FragmentRanges{&fragment.FragmentRange{Start: 3, End: 7}}
	_, err = frs.ConvertToSegmentRanges(segmentIDsLayout)
	assert.Error(t, err, "invalid fragment ranges or segment ID`s layout when calculate segemnt ranges from fragment ranges")

	frs = fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 6}}
	result, _ := frs.ConvertToSegmentRanges(segmentIDsLayout)
	expect := fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 8}}
	assert.Equal(t, expect, result)

	frs = fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 4}}
	result, _ = frs.ConvertToSegmentRanges(segmentIDsLayout)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 0, End: 6}}
	assert.Equal(t, expect, result)

	frs = fragment.FragmentRanges{&fragment.FragmentRange{Start: 3, End: 6}}
	result, _ = frs.ConvertToSegmentRanges(segmentIDsLayout)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 4, End: 8}}
	assert.Equal(t, expect, result)

	frs = fragment.FragmentRanges{&fragment.FragmentRange{Start: 1, End: 2}, &fragment.FragmentRange{Start: 3, End: 5}}
	result, _ = frs.ConvertToSegmentRanges(segmentIDsLayout)
	expect = fragment.FragmentRanges{&fragment.FragmentRange{Start: 2, End: 3}, &fragment.FragmentRange{Start: 4, End: 7}}
	assert.Equal(t, expect, result)
}
