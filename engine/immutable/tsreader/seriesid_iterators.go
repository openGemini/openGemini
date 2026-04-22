// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package tsreader

import (
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/index"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type SeriesIdIterator struct {
	order     []immutable.TSSPFile
	unordered []immutable.TSSPFile

	seriesItr index.SeriesIDIterator
	tr        util.TimeRange
	schema    record.Schemas

	rec        *record.Record
	swap       *record.Record
	sortHelper *record.ColumnSortHelper
	metaCtx    *immutable.ChunkMetaContext
	readCtx    *immutable.ReadContext

	filterOptions *immutable.BaseFilterOptions
	filterBitmap  *bitmap.FilterBitmap
}

func NewSeriesIdIterator(order, unordered []immutable.TSSPFile, seriesItr index.SeriesIDIterator, tr util.TimeRange,
	schema record.Schemas) *SeriesIdIterator {
	fieldsIdx := make([]int, schema.Len()-1)
	for i := range fieldsIdx {
		fieldsIdx[i] = i
	}
	filterOption := &immutable.BaseFilterOptions{
		CondFunctions: &binaryfilterfunc.ConditionImpl{},
		FieldsIdx:     fieldsIdx,
		RedIdxMap:     map[int]struct{}{},
		FiltersMap:    make(map[string]*influxql.FilterMapValue),
	}

	return &SeriesIdIterator{
		order:         order,
		unordered:     unordered,
		seriesItr:     seriesItr,
		tr:            tr,
		schema:        schema,
		rec:           &record.Record{},
		swap:          &record.Record{},
		sortHelper:    record.NewColumnSortHelper(),
		metaCtx:       immutable.NewChunkMetaContext(schema),
		readCtx:       immutable.NewReadContext(true),
		filterOptions: filterOption,
	}
}

func (itr *SeriesIdIterator) Close() {
	itr.seriesItr.Close()
	itr.readCtx.Release()
	itr.metaCtx.Release()
	itr.sortHelper.Release()

	itr.readCtx = nil
	itr.metaCtx = nil
	itr.sortHelper = nil
	itr.order = nil
	itr.unordered = nil
}

func (itr *SeriesIdIterator) Next() (uint64, *record.Record, error) {
	seriesIDElem, err := itr.seriesItr.Next()
	if err != nil {
		return 0, nil, err
	}
	sid := seriesIDElem.SeriesID
	if seriesIDElem.SeriesID == 0 {
		return 0, nil, nil
	}

	conditionFunction, err := binaryfilterfunc.NewCondition(nil, seriesIDElem.Expr, itr.schema, nil)
	if err != nil {
		return 0, nil, err
	}
	filterBitmap := bitmap.NewFilterBitmap(conditionFunction.NumFilter())
	itr.filterOptions.CondFunctions = conditionFunction
	itr.filterBitmap = filterBitmap

	rec, err := itr.readRecord(sid)
	if err != nil {
		return 0, nil, err
	}

	return sid, rec, nil
}

func (itr *SeriesIdIterator) readRecord(sid uint64) (*record.Record, error) {
	dst := itr.rec
	dst.ResetWithSchema(itr.schema)

	for _, r := range itr.unordered {
		err := itr.readRecordFromTSSPFile(r, dst, sid)
		if err != nil {
			return nil, err
		}
	}
	needSort := dst.RowNums() > 0

	for _, r := range itr.order {
		err := itr.readRecordFromTSSPFile(r, dst, sid)
		if err != nil {
			return nil, err
		}
	}

	if needSort {
		dst = itr.sortHelper.Sort(dst)
		itr.rec = dst
	}

	return dst, nil
}

func (itr *SeriesIdIterator) readRecordFromTSSPFile(reader immutable.TSSPFile, dst *record.Record, sid uint64) error {
	idx, metaIndex, err := reader.MetaIndex(sid, itr.tr)
	if err != nil {
		return err
	}
	if metaIndex == nil {
		return nil
	}

	ioPriority := fileops.IO_PRIORITY_ULTRA_HIGH
	meta, err := reader.ChunkMeta(sid, metaIndex.GetOffset(), metaIndex.GetSize(), metaIndex.GetCount(), idx, itr.metaCtx, ioPriority)
	if err != nil {
		return err
	}
	if meta == nil {
		return nil
	}
	if !itr.tr.Overlaps(meta.MinMaxTime()) {
		return nil
	}

	swap := itr.swap
	swap.ResetWithSchema(itr.schema)

	for i := range meta.SegmentCount() {
		swap, err = reader.ReadAt(meta, i, swap, itr.readCtx, ioPriority)
		if err != nil {
			return err
		}
		if swap.RowNums() == 0 {
			return nil
		}

		// filter with time
		swap = immutable.FilterByTime(swap, itr.tr)

		// filter with condition, only when current sid has filter expr
		if len(itr.filterBitmap.Bitmap) > 0 {
			swap = immutable.FilterByFieldFuncs(swap, nil, itr.filterOptions, itr.filterBitmap)
		}
		if swap == nil || swap.RowNums() == 0 {
			return nil
		}
		dst.Merge(swap)
	}
	return nil
}
