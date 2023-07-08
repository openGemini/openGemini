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

package fragment_test

import (
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/fragment"
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

	assert.Equal(t, frs.String(), "(\x00, \x02), (\x02, \x03), (\x00, \x01)")
	assert.Equal(t, frs.GetLastFragment(), uint32(3))
}

func TestIndexFragment(t *testing.T) {
	f := fragment.NewIndexFragmentVariable([]uint64{2, 4, 6, 8})
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
