/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package sparseindex

import (
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"go.uber.org/zap"
)

type IndexReader interface {
	Scan(pkFile string,
		pkRec *record.Record,
		pkMark fragment.IndexFragment,
		keyCondition KeyCondition,
	) (fragment.FragmentRanges, error)
	Close() error
}

type IndexReaderImpl struct {
	property *IndexProperty
	logger   *logger.Logger
}

func NewIndexReader(rowsNumPerFragment int, coarseIndexFragment int, minRowsForSeek int) *IndexReaderImpl {
	return &IndexReaderImpl{
		property: NewIndexProperty(rowsNumPerFragment, coarseIndexFragment, minRowsForSeek),
		logger:   logger.NewLogger(errno.ModuleIndex),
	}
}

// Scan is used to filter fragment ranges based on the primary key in the condition,
// and it determines whether to do binary search or exclusion search according to the sequence of keys in the primary key.
// give a specific example to illustrate the usage of scan.
// 1. origin record:
//	x -> [1, 2, 1, 2, 1, 2, 2, 1]
//	y -> [1, 1, 3, 4, 2, 2, 3, 4]
//
// 2. sorted record(sorted by x, y):
//	x -> [1, 1, 1, 1, 2, 2, 2, 2]
//	y -> [1, 2, 3, 4, 1, 2, 3, 4]
//
// 3. primary index record(fragment size is 2):
//   x ->  [1, 1, 2, 2, 2]
//   y ->  [1, 3, 1, 3, 4]
//   fragment index -> [0, 1, 2, 3]
//
// 4. key condition:
//   x > 1 and y < 3
//
// 5. scan results:
//   fragment range -> [1, 3)
func (s *IndexReaderImpl) Scan(
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
	createFieldRef = s.createFieldRefFunc(keyCondition, index, primaryKey, usedKeySize)

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
		return s.doExclusionSearch(fragmentCount, pkFile, checkInRange)
	}
	return s.doBinarySearch(fragmentCount, pkFile, checkInRange)
}

func (s *IndexReaderImpl) createFieldRefFunc(
	keyCondition KeyCondition,
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
	if keyCondition.IsFirstPrimaryKey() {
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
	} else {
		createFieldRef = func(row int, column int, field *FieldRef) {
			doCreateFieldRef(row, column, field, genIndexColumnsByRec(index))
		}
	}
	return
}

// doBinarySearch does binary search to get the target ranges
func (s *IndexReaderImpl) doBinarySearch(
	fragmentCount uint32,
	fileName string,
	checkInRange func(mr *fragment.FragmentRange) (bool, error),
) (fragment.FragmentRanges, error) {
	s.logger.Debug("Running binary search on index range for", zap.String("datafile", fileName), zap.Uint32("masks", fragmentCount))

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
	s.logger.Debug("Found (LEFT) boundary", zap.Uint32("mark", left))

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
	s.logger.Debug("Found (RIGHT) boundary", zap.Uint32("mark", right))

	if resRange.Start < resRange.End {
		inRange, err := checkInRange(resRange)
		if err != nil {
			return nil, err
		}
		if inRange {
			res = append(res, resRange)
		}
	}

	s.logger.Debug("Found", zap.String("range", getSearchStatus(res)), zap.Uint32("steps", steps))
	return res, nil
}

// doExclusionSearch does exclusion search, where we drop ranges that do not match
func (s *IndexReaderImpl) doExclusionSearch(
	fragmentCount uint32,
	fileName string,
	checkInRange func(mr *fragment.FragmentRange) (bool, error),
) (fragment.FragmentRanges, error) {
	s.logger.Debug("Running exclusion search on index range for", zap.String("datafile", fileName), zap.Uint32("masks", fragmentCount))
	if s.property.CoarseIndexFragment <= 1 {
		return nil, errno.NewError(errno.ErrCoarseIndexFragment)
	}

	var res fragment.FragmentRanges
	steps := uint32(0)
	minMarksForSeek := (s.property.MinRowsForSeek + s.property.RowsNumPerFragment - 1) / s.property.RowsNumPerFragment
	rangesStack := []*fragment.FragmentRange{{0, fragmentCount}}
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
			step := (mr.End-mr.Start-1)/uint32(s.property.CoarseIndexFragment) + 1
			var end uint32
			for end = mr.End; end > mr.Start+step; end -= step {
				rangesStack = append(rangesStack, fragment.NewFragmentRange(end-step, end))
			}
			rangesStack = append(rangesStack, fragment.NewFragmentRange(mr.Start, end))
		}
	}
	s.logger.Debug("Used generic exclusion search over index for", zap.String("datafile", fileName), zap.Uint32("steps", steps))
	return res, nil
}

func (s *IndexReaderImpl) Close() error {
	return nil
}
