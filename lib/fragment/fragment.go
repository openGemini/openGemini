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

package fragment

import (
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

func (frs FragmentRanges) GetLastFragment() uint32 {
	currentTaskLastFragment := uint32(0)
	for _, fr := range frs {
		currentTaskLastFragment = util.MaxUint32(currentTaskLastFragment, fr.End)
	}
	return currentTaskLastFragment
}

type IndexFragment interface {
	GetFragmentCount() uint32
	GetRowsCountInRange(start int, end int) uint64
	GetTotalRowsCount() uint64
	PopFragment()
	Empty() bool
}

// IndexFragmentVariableImpl indicates that the size of fragment is variable and the last row of data is added to the index by default.
type IndexFragmentVariableImpl struct {
	accumulateRowCount []uint64
}

func NewIndexFragmentVariable(accumulateRowCount []uint64) IndexFragment {
	f := &IndexFragmentVariableImpl{}
	f.accumulateRowCount = append(f.accumulateRowCount, accumulateRowCount...)
	return f
}

func (f *IndexFragmentVariableImpl) GetFragmentCount() uint32 {
	return uint32(len(f.accumulateRowCount))
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
