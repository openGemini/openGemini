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

package fragment

import (
	"errors"
	"fmt"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
)

// FragmentRange means a fragment range of a data file. The range is left closed and right open. Such as [Start, end).
// A fragment is a logical data block based of row. It is used for data block filtering of primary index and skip index.
// Currently, fragment and segment are one-to-one and can be expanded to one-to-N in the future.
type FragmentRange struct {
	Start uint32
	End   uint32
}

func NewFragmentRange(start, end uint32) *FragmentRange {
	return &FragmentRange{Start: start, End: end}
}

// Equal is used to determine that two fragment ranges are the same.
func (fr *FragmentRange) Equal(other *FragmentRange) bool {
	return fr.Start == other.Start && fr.End == other.End
}

// Less is used to determine that the former fragment range is on the left of the latter. The horizontal axis starts from 0.
func (fr *FragmentRange) Less(other *FragmentRange) (bool, error) {
	isInterSection := (fr.Start <= other.Start && other.Start < fr.End) || (other.Start <= fr.Start && fr.Start < other.End)
	if isInterSection {
		return false, errno.NewError(errno.ErrMarkRangeInserting)
	}
	return fr.Start < other.Start && fr.End <= other.End, nil
}

func (fr *FragmentRange) NumberOfSegments() int {
	return int(fr.End - fr.Start)
}

type FragmentRanges []*FragmentRange

func (frs FragmentRanges) Empty() bool {
	return len(frs) == 0
}

func (frs FragmentRanges) String() string {
	var res string
	for _, fr := range frs {
		if len(res) > 0 {
			res += ", "
		}
		res += fmt.Sprintf("(%d, %d)", fr.Start, fr.End)
	}
	return res
}

// Convention: The range intervals represented by elements in 'frs' and 'other' are each sorted in ascending order and non-overlapping
func (frs FragmentRanges) Intersect(other FragmentRanges) FragmentRanges {
	var result FragmentRanges
	i, j := 0, 0

	for i < len(frs) && j < len(other) {
		a := frs[i]
		b := other[j]

		start := max(a.Start, b.Start)
		end := min(a.End, b.End)

		if start < end {
			result = append(result, &FragmentRange{Start: start, End: end})
		}

		if a.End < b.End {
			i++
		} else {
			j++
		}
	}

	return result
}

// Convention: The range intervals represented by elements in 'frs' and 'other' are each sorted in ascending order and non-overlapping
func (frs FragmentRanges) Union(other FragmentRanges) FragmentRanges {
	var result FragmentRanges
	i, j := 0, 0

	for i < len(frs) || j < len(other) {
		if i < len(frs) && (j >= len(other) || frs[i].Start < other[j].Start) {
			addRange(&result, frs[i])
			i++
		} else {
			addRange(&result, other[j])
			j++
		}
	}
	return result
}

func addRange(result *FragmentRanges, current *FragmentRange) {
	addCur := &FragmentRange{Start: current.Start, End: current.End}
	if len(*result) == 0 {
		*result = append(*result, addCur)
	} else {
		last := (*result)[len(*result)-1]
		if current.Start <= last.End {
			last.End = max(last.End, current.End)
		} else {
			*result = append(*result, addCur)
		}
	}
}

// Convention: 'frs' represents the full range set; 'subset' is a subset where the range intervals represented by its elements are sorted in ascending order and non-overlapping
func (frs FragmentRanges) Complement(subset FragmentRanges) FragmentRanges {
	if len(frs) == 0 {
		return FragmentRanges{}
	}

	var result FragmentRanges

	motherStart := frs[0].Start
	motherEnd := frs[0].End

	if len(subset) == 0 {
		result = append(result, NewFragmentRange(motherStart, motherEnd))
		return result
	}

	if subset[0].Start > motherStart {
		result = append(result, NewFragmentRange(motherStart, subset[0].Start))
	}

	for i := 0; i < len(subset)-1; i++ {
		current := subset[i]
		next := subset[i+1]
		if current.End < next.Start {
			result = append(result, NewFragmentRange(current.End, next.Start))
		}
	}

	last := subset[len(subset)-1]
	if last.End < motherEnd {
		result = append(result, NewFragmentRange(last.End, motherEnd))
	}

	return result
}

// Convention: The indices parameter is a list of valid range IDs in ascending order
func CalcFragmentRanges(indices []int) FragmentRanges {
	var ranges FragmentRanges
	if len(indices) == 0 {
		return ranges
	}

	currentStart := indices[0]
	for i := 1; i < len(indices); i++ {
		if indices[i] != indices[i-1]+1 {
			ranges = append(ranges, NewFragmentRange(uint32(currentStart), uint32(indices[i-1]+1)))
			currentStart = indices[i]
		}
	}

	ranges = append(ranges, NewFragmentRange(uint32(currentStart), uint32(indices[len(indices)-1]+1)))

	return ranges
}

func (frs FragmentRanges) GetLastFragment() uint32 {
	currentTaskLastFragment := uint32(0)
	for _, fr := range frs {
		currentTaskLastFragment = util.MaxUint32(currentTaskLastFragment, fr.End)
	}
	return currentTaskLastFragment
}

// Generate segment ranges from fragment ranges
func (frs FragmentRanges) ConvertToSegmentRanges(segmentIDsLayout []int64) (FragmentRanges, error) {
	segmentRanges := make(FragmentRanges, 0, len(frs))
	layoutLength := uint32(len(segmentIDsLayout))

	var segStart, segEnd uint32
	for _, fr := range frs {
		if fr.Start >= fr.End {
			return nil, errors.New("invalid fragment ranges when calculate segemnt ranges from fragment ranges")
		}
		if fr.End > layoutLength {
			return nil, errors.New("invalid fragment ranges or segment ID`s layout when calculate segemnt ranges from fragment ranges")
		}
		if fr.Start == 0 {
			segStart = 0
		} else {
			segStart = uint32(segmentIDsLayout[fr.Start-1])
		}
		segEnd = uint32(segmentIDsLayout[fr.End-1])
		segmentRanges = append(segmentRanges, &FragmentRange{
			Start: segStart,
			End:   segEnd,
		})
	}
	return segmentRanges, nil
}

type IndexFragment interface {
	GetFragmentCount() uint32
	GetSegmentsFromFragmentRange() FragmentRanges
	GetRowsCountInRange(start int, end int) uint64
	GetTotalRowsCount() uint64
	PopFragment()
	Empty() bool
}

// IndexFragmentVariableImpl indicates that the size of fragment is variable and the last row of data is added to the index by default.
type IndexFragmentVariableImpl struct {
	accumulateRowCount []uint64
	fragmentRanges     FragmentRanges
}

func NewIndexFragmentVariable(segment []uint64) IndexFragment {
	n := len(segment)
	f := &IndexFragmentVariableImpl{
		accumulateRowCount: make([]uint64, n),
		fragmentRanges:     make(FragmentRanges, n),
	}

	frs := make([]FragmentRange, n)
	var start uint32 = 0
	segmentOffset := util.Bytes2Int64Slice(util.Uint64Slice2byte(segment))
	for i := range segmentOffset {
		segmentCount, rowCount := util.SplitInt64(segmentOffset[i])
		fr := &frs[i]
		fr.Start = start
		fr.End = segmentCount
		f.fragmentRanges[i] = fr
		f.accumulateRowCount[i] = uint64(rowCount)
		start = fr.End
	}

	return f
}

func (f *IndexFragmentVariableImpl) GetFragmentCount() uint32 {
	return uint32(len(f.accumulateRowCount))
}

func (f *IndexFragmentVariableImpl) GetSegmentsFromFragmentRange() FragmentRanges {
	return f.fragmentRanges
}

func (f *IndexFragmentVariableImpl) GetRowsCountInRange(start int, end int) uint64 {
	var subtrahend uint64
	if start != 0 {
		subtrahend = f.accumulateRowCount[start-1]
	}
	return f.accumulateRowCount[end-1] - subtrahend
}

func (f *IndexFragmentVariableImpl) GetTotalRowsCount() uint64 {
	if len(f.accumulateRowCount) == 0 {
		return 0
	}
	return f.accumulateRowCount[len(f.accumulateRowCount)-1]
}

func (f *IndexFragmentVariableImpl) GetFragmentStartingRow(markIndex int) uint64 {
	if len(f.accumulateRowCount) == 0 {
		return 0
	}
	if markIndex == 0 {
		return f.accumulateRowCount[0]
	}
	return f.accumulateRowCount[markIndex] - f.accumulateRowCount[markIndex-1]
}

func (f *IndexFragmentVariableImpl) GetLastFragmentRows() uint64 {
	return f.GetFragmentStartingRow(len(f.accumulateRowCount) - 1)
}

func (f *IndexFragmentVariableImpl) Empty() bool {
	return len(f.accumulateRowCount) == 0
}

func (f *IndexFragmentVariableImpl) AppendFragment(rowsCount uint64) {
	if len(f.accumulateRowCount) == 0 {
		f.accumulateRowCount = append(f.accumulateRowCount, rowsCount)
	} else {
		f.accumulateRowCount = append(f.accumulateRowCount, rowsCount+f.accumulateRowCount[len(f.accumulateRowCount)-1])
	}
}

func (f *IndexFragmentVariableImpl) AddRowsToLastFragment(rowsCount uint64) {
	if len(f.accumulateRowCount) == 0 {
		f.accumulateRowCount = append(f.accumulateRowCount, rowsCount)
	} else {
		f.accumulateRowCount[len(f.accumulateRowCount)-1] += rowsCount
	}
}

func (f *IndexFragmentVariableImpl) PopFragment() {
	if len(f.accumulateRowCount) != 0 {
		f.accumulateRowCount = f.accumulateRowCount[:len(f.accumulateRowCount)-1]
	}
}

// IndexFragmentFixedSizeImpl indicates that the size of fragment is fixed and the last row of data is added to the index by default.
type IndexFragmentFixedSizeImpl struct {
	rowCountPerFragment uint64
	fragmentCount       uint32
}

func NewIndexFragmentFixedSize(fragmentCount uint32, rowCountPerFragment uint64) IndexFragment {
	return &IndexFragmentFixedSizeImpl{
		fragmentCount:       fragmentCount,
		rowCountPerFragment: rowCountPerFragment}
}

func (f *IndexFragmentFixedSizeImpl) GetFragmentCount() uint32 {
	return f.fragmentCount
}

func (f *IndexFragmentFixedSizeImpl) GetSegmentsFromFragmentRange() FragmentRanges {
	return []*FragmentRange{{Start: 0, End: f.fragmentCount}}
}

func (f *IndexFragmentFixedSizeImpl) GetRowsCountInRange(start int, end int) uint64 {
	return uint64(end-start) * f.rowCountPerFragment
}

func (f *IndexFragmentFixedSizeImpl) GetTotalRowsCount() uint64 {
	return f.rowCountPerFragment * uint64(f.fragmentCount)
}

func (f *IndexFragmentFixedSizeImpl) PopFragment() {
	f.fragmentCount -= 1
}

func (f *IndexFragmentFixedSizeImpl) Empty() bool {
	return f.fragmentCount == 0
}

type FragmentRangeDetail struct {
	Count int64
}

type FragmentRangeDetails []*FragmentRangeDetail
