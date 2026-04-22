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

package immutable

import (
	"testing"

	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/stretchr/testify/assert"
)

func TestNextSegment(t *testing.T) {
	f := func(
		loc *Location,
		rgs []*fragment.FragmentRange,
		expected []int,
	) {
		var actual []int
		loc.meta = &ChunkMeta{}
		loc.SetFragmentRanges(rgs)
		for loc.hasNext() {
			actual = append(actual, loc.segPos)
			loc.nextSegment(false)
		}
		assert.Equal(t, expected, actual)
	}
	t.Run("tsstore 1", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(true)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}},
			[]int{0, 1},
		)
	})

	t.Run("tsstore 2", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(false)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}},
			[]int{1, 0},
		)
	})

	t.Run("tsstore 3", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(true)),
			[]*fragment.FragmentRange{{Start: 1, End: 3}},
			[]int{1, 2},
		)
	})

	t.Run("tsstore 4", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(false)),
			[]*fragment.FragmentRange{{Start: 1, End: 3}},
			[]int{2, 1},
		)
	})

	t.Run("csstore 1", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(true)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}, {Start: 3, End: 5}},
			[]int{0, 1, 3, 4},
		)
	})

	t.Run("csstore 2", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(false)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}, {Start: 3, End: 5}},
			[]int{4, 3, 1, 0},
		)
	})

	t.Run("csstore 3", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(true)),
			[]*fragment.FragmentRange{{Start: 1, End: 3}},
			[]int{1, 2},
		)
	})

	t.Run("csstore 4", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(false)),
			[]*fragment.FragmentRange{{Start: 1, End: 3}},
			[]int{2, 1},
		)
	})

	t.Run("csstore 5", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(true)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}, {Start: 3, End: 5}, {Start: 7, End: 9}},
			[]int{0, 1, 3, 4, 7, 8},
		)
	})

	t.Run("csstore 5", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(false)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}, {Start: 3, End: 5}, {Start: 7, End: 9}},
			[]int{8, 7, 4, 3, 1, 0},
		)
	})
}

func TestNextSegmentLast(t *testing.T) {
	f := func(
		loc *Location,
		rgs []*fragment.FragmentRange,
		expected []int,
	) {
		var actual []int
		loc.meta = &ChunkMeta{}
		loc.SetFragmentRanges(rgs)
		for loc.hasNext() {
			actual = append(actual, loc.segPos)
			loc.nextSegment(true)
		}
		assert.Equal(t, expected, actual)
	}
	t.Run("tsstore 1", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(true)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}},
			[]int{0},
		)
	})

	t.Run("tsstore 2", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(false)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}},
			[]int{1},
		)
	})

	t.Run("csstore 1", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(true)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}, {Start: 3, End: 5}},
			[]int{0},
		)
	})

	t.Run("csstore 2", func(t *testing.T) {
		f(
			NewLocation(nil, NewReadContext(false)),
			[]*fragment.FragmentRange{{Start: 0, End: 2}, {Start: 3, End: 5}},
			[]int{4},
		)
	})
}

func TestReadCtx(t *testing.T) {
	readCtx := NewReadContext(true)
	assert.False(t, readCtx.IsAborted())
	s := false
	closedSignal := &s
	readCtx.SetClosedSignal(closedSignal)
	*closedSignal = true
	assert.True(t, readCtx.IsAborted())

	loc := NewLocation(nil, readCtx)
	loc.ctx.tr.Min, loc.ctx.tr.Max = 0, 1
	loc.SetFragmentRanges([]*fragment.FragmentRange{{Start: 0, End: 1}})
	loc.SetChunkMeta(&ChunkMeta{timeRange: []SegmentRange{[2]int64{0, 1}}})
	_, _, err := loc.readData(nil, nil, nil, nil, nil)
	assert.Equal(t, err, nil)
}
