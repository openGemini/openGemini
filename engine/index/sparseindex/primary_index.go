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

package sparseindex

import (
	"errors"
	"fmt"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type PKIndexReader interface {
	Scan(pkFile string,
		pkRec *record.Record,
		pkMark fragment.IndexFragment,
		keyCondition KeyCondition,
	) (fragment.FragmentRanges, error)
	Close() error
}

type PKIndexWriter interface {
	Build(srcRec *record.Record,
		pkSchema record.Schemas,
		rowsNumPerFragment []int,
		tcLocation int8,
		fixRowsPerSegment int,
	) (
		*record.Record, fragment.IndexFragment, error,
	)
	Close() error
}

type PKIndexReaderImpl struct {
	property *IndexProperty
	logger   *logger.Logger
}

func NewPKIndexReader(rowsNumPerFragment int, coarseIndexFragment int, minRowsForSeek int) *PKIndexReaderImpl {
	return &PKIndexReaderImpl{
		property: NewIndexProperty(rowsNumPerFragment, coarseIndexFragment, minRowsForSeek),
		logger:   logger.NewLogger(errno.ModuleIndex),
	}
}

// Scan is used to filter fragment ranges based on the primary key in the condition,
// and it determines whether to do binary search or exclusion search according to the sequence of keys in the primary key.
// give a specific example to illustrate the usage of scan.
//
//  1. origin record:
//     x -> [1, 2, 1, 2, 1, 2, 2, 1]
//     y -> [1, 1, 3, 4, 2, 2, 3, 4]
//
//  2. sorted record(sorted by x, y):
//     x -> [1, 1, 1, 1, 2, 2, 2, 2]
//     y -> [1, 2, 3, 4, 1, 2, 3, 4]
//
//  3. primary index record(fragment size is 2):
//     x ->  [1, 1, 2, 2, 2]
//     y ->  [1, 3, 1, 3, 4]
//     fragment index -> [0, 1, 2, 3]
//
//  4. key condition:
//     x > 1 and y < 3
//
//  5. scan results:
//     fragment range -> [1, 3)
func (r *PKIndexReaderImpl) Scan(
	pkFile string,
	pkRec *record.Record,
	pkMark fragment.IndexFragment,
	keyCondition KeyCondition,
) (fragment.FragmentRanges, error) {
	var res fragment.FragmentRanges
	var createFieldRef func(int, int, *FieldRef)

	index := pkRec
	pkSchema := pkRec.Schema
	fragmentCount := pkMark.GetFragmentCount()

	// if index is not used
	if !keyCondition.HavePrimaryKey() {
		res = append(res, fragment.NewFragmentRange(0, fragmentCount))
		return res, nil
	}

	usedKeySize := keyCondition.GetMaxKeyIndex() + 1
	primaryKey := pkSchema
	pkTypes := getDataTypesFromPk(primaryKey)
	createFieldRef = r.createFieldRefFunc(index, primaryKey, usedKeySize)

	indexLeft := make([]*FieldRef, usedKeySize)
	indexRight := make([]*FieldRef, usedKeySize)
	for i := 0; i < usedKeySize; i++ {
		indexLeft[i] = &FieldRef{}
		indexRight[i] = &FieldRef{}
	}

	checkInRange := func(mr *fragment.FragmentRange) (bool, error) {
		for i := 0; i < usedKeySize; i++ {
			createFieldRef(int(mr.Start), i, indexLeft[i])
			createFieldRef(int(mr.End), i, indexRight[i])
		}

		return keyCondition.MayBeInRange(usedKeySize, indexLeft, indexRight, pkTypes)
	}

	if !keyCondition.CanDoBinarySearch() {
		return r.doExclusionSearch(fragmentCount, pkFile, checkInRange)
	}
	return r.doBinarySearch(fragmentCount, pkFile, checkInRange)
}

func (r *PKIndexReaderImpl) createFieldRefFunc(
	index *record.Record,
	primaryKey record.Schemas,
	usedKeySize int,
) (createFieldRef func(int, int, *FieldRef)) {
	doCreateFieldRef := func(row int, column int, field *FieldRef, cols []*ColumnRef) {
		field.Set(cols, column, row)
		if field.IsNull() {
			field.SetPositiveInfinity()
		}
	}

	indexPartColumns := make([]*ColumnRef, usedKeySize)
	for i := 0; i < usedKeySize; i++ {
		indexPartColumns[i] = &ColumnRef{
			column:   index.Column(i),
			name:     primaryKey[i].Name,
			dataType: primaryKey[i].Type}
	}
	createFieldRef = func(row int, column int, field *FieldRef) {
		doCreateFieldRef(row, column, field, indexPartColumns)
	}
	return
}

// doBinarySearch does binary search to get the target ranges
func (r *PKIndexReaderImpl) doBinarySearch(
	fragmentCount uint32,
	fileName string,
	checkInRange func(mr *fragment.FragmentRange) (bool, error),
) (fragment.FragmentRanges, error) {
	r.logger.Debug("Running binary search on index range for", zap.String("datafile", fileName), zap.Uint32("masks", fragmentCount))

	var res fragment.FragmentRanges
	steps, left, right := uint32(0), uint32(0), fragmentCount
	resRange := fragment.NewFragmentRange(0, fragmentCount)
	for left+1 < right {
		middle := (left + right) / 2
		mr := fragment.NewFragmentRange(0, middle)
		inRange, err := checkInRange(mr)
		if err != nil {
			return nil, err
		}
		if inRange {
			right = middle
		} else {
			left = middle
		}
		steps++
	}
	resRange.Start = left
	r.logger.Debug("Found (LEFT) boundary", zap.Uint32("mark", left))

	right = fragmentCount
	for left+1 < right {
		middle := (left + right) / 2
		mr := fragment.NewFragmentRange(middle, fragmentCount)
		inRange, err := checkInRange(mr)
		if err != nil {
			return nil, err
		}
		if inRange {
			left = middle
		} else {
			right = middle
		}
		steps++
	}
	resRange.End = right
	r.logger.Debug("Found (RIGHT) boundary", zap.Uint32("mark", right))

	if resRange.Start < resRange.End {
		inRange, err := checkInRange(resRange)
		if err != nil {
			return nil, err
		}
		if inRange {
			res = append(res, resRange)
		}
	}

	r.logger.Debug("Found", zap.String("range", getSearchStatus(res)), zap.Uint32("steps", steps))
	return res, nil
}

// doExclusionSearch does exclusion search, where we drop ranges that do not match
func (r *PKIndexReaderImpl) doExclusionSearch(
	fragmentCount uint32,
	fileName string,
	checkInRange func(mr *fragment.FragmentRange) (bool, error),
) (fragment.FragmentRanges, error) {
	r.logger.Debug("Running exclusion search on index range for", zap.String("datafile", fileName), zap.Uint32("masks", fragmentCount))
	if r.property.CoarseIndexFragment <= 1 {
		return nil, errno.NewError(errno.ErrCoarseIndexFragment)
	}

	var res fragment.FragmentRanges
	steps := uint32(0)
	minMarksForSeek := (r.property.MinRowsForSeek + r.property.RowsNumPerFragment - 1) / r.property.RowsNumPerFragment
	rangesStack := []*fragment.FragmentRange{{Start: 0, End: fragmentCount}}
	for len(rangesStack) > 0 {
		mr := rangesStack[len(rangesStack)-1]
		rangesStack = rangesStack[:len(rangesStack)-1]
		steps++
		inRange, err := checkInRange(mr)
		if err != nil {
			return nil, err
		}
		if !inRange {
			continue
		}

		if mr.End == mr.Start+1 {
			// We saw a useful gap between neighboring marks. Either add it to the last range, or start a new range.
			if len(res) == 0 || int(mr.Start-res[len(res)-1].End) > minMarksForSeek {
				res = append(res, mr)
			} else {
				res[len(res)-1].End = mr.End
			}
		} else {
			step := (mr.End-mr.Start-1)/uint32(r.property.CoarseIndexFragment) + 1
			var end uint32
			for end = mr.End; end > mr.Start+step; end -= step {
				rangesStack = append(rangesStack, fragment.NewFragmentRange(end-step, end))
			}
			rangesStack = append(rangesStack, fragment.NewFragmentRange(mr.Start, end))
		}
	}
	r.logger.Debug("Used generic exclusion search over index for", zap.String("datafile", fileName), zap.Uint32("steps", steps))
	return res, nil
}

func (r *PKIndexReaderImpl) Close() error {
	return nil
}

var (
	// InitIndexFragmentFixedSize means that each fragment is fixed in size except the last fragment.
	InitIndexFragmentFixedSize = true
)

type PKIndexWriterImpl struct {
}

func NewPKIndexWriter() *PKIndexWriterImpl {
	return &PKIndexWriterImpl{}
}

// Build generates sparse primary index based on sorted data to be flushed to disks.
func (w *PKIndexWriterImpl) Build(
	srcRec *record.Record,
	pkSchema record.Schemas,
	rowsNumPerFragment []int,
	tcLocation int8,
	fixRowsPerSegment int,
) (
	*record.Record,
	fragment.IndexFragment,
	error,
) {
	numFragment := len(rowsNumPerFragment)
	dstRec, err := w.buildData(srcRec, pkSchema, rowsNumPerFragment, numFragment, tcLocation)
	if err != nil {
		return nil, nil, err
	}

	indexFragment := w.buildFragment(rowsNumPerFragment, numFragment, fixRowsPerSegment)
	return dstRec, indexFragment, nil
}

// buildData generates sparse primary index data based on sorted data to be flushed to disks.
func (w *PKIndexWriterImpl) buildData(
	srcRec *record.Record,
	pkSchema record.Schemas,
	rowsNumPerFragment []int,
	numFragment int,
	tcLocation int8,
) (
	*record.Record,
	error,
) {
	if tcLocation > colstore.DefaultTCLocation {
		field := record.Field{Type: influx.Field_Type_Int, Name: record.TimeClusterCol}
		pkSchema = append([]record.Field{field}, pkSchema...)
	}
	dstRec := record.NewRecord(pkSchema, false)
	for i := 0; i < pkSchema.Len(); i++ {
		if idx := srcRec.Schema.FieldIndex(pkSchema.Field(i).Name); idx >= 0 {
			switch pkSchema.Field(i).Type {
			case influx.Field_Type_String, influx.Field_Type_Tag:
				w.generateColumn(srcRec, dstRec, rowsNumPerFragment, numFragment, pkSchema.Field(i).Type, idx, i)
			case influx.Field_Type_Int:
				w.generateColumn(srcRec, dstRec, rowsNumPerFragment, numFragment, influx.Field_Type_Int, idx, i)
			case influx.Field_Type_Float:
				w.generateColumn(srcRec, dstRec, rowsNumPerFragment, numFragment, influx.Field_Type_Float, idx, i)
			case influx.Field_Type_Boolean:
				w.generateColumn(srcRec, dstRec, rowsNumPerFragment, numFragment, influx.Field_Type_Boolean, idx, i)
			default:
				return nil, errors.New("unsupported data type")
			}
		} else {
			return nil, fmt.Errorf("the table does not have a primary key field, %s", pkSchema.Field(i).Name)
		}
	}
	return dstRec, nil
}

func (w *PKIndexWriterImpl) generateColumn(
	srcRec *record.Record,
	dstRec *record.Record,
	rowsNumPerFragment []int,
	numFragment int,
	dataType int,
	srcColIdx int,
	dstColIdx int,
) {
	// eg: rowsNumPerFragment -> [8192,8192*2,8192*3....,len(srcRec) - 8192*n]  or  [7000,13847,21420,...,len(srcRec)-1]
	start := 0
	dstRec.ColVals[dstColIdx].AppendColVal(&srcRec.ColVals[srcColIdx], dataType, start, start+1)
	for j := 0; j < numFragment; j++ {
		dstRec.ColVals[dstColIdx].AppendColVal(&srcRec.ColVals[srcColIdx], dataType, rowsNumPerFragment[j], rowsNumPerFragment[j]+1)
	}
}

// buildFragment generates sparse primary index fragment based on sorted data to be flushed to disks.
func (w *PKIndexWriterImpl) buildFragment(
	rowsNumPerFragment []int,
	numFragment int,
	fixRowsPerSegment int,
) fragment.IndexFragment {
	if fixRowsPerSegment != 0 {
		return fragment.NewIndexFragmentFixedSize(uint32(numFragment), uint64(fixRowsPerSegment))
	}

	accumulateRowCount := make([]uint64, numFragment)
	for i := 0; i < numFragment-1; i++ {
		accumulateRowCount[i] = uint64(rowsNumPerFragment[i])
	}
	accumulateRowCount[numFragment-1] = uint64(rowsNumPerFragment[numFragment-1] + 1)
	return fragment.NewIndexFragmentVariable(accumulateRowCount)
}

func (w *PKIndexWriterImpl) Close() error {
	return nil
}
