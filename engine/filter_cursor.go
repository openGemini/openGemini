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

package engine

import (
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
)

type FilterCursor struct {
	options      *immutable.BaseFilterOptions
	schema       *executor.QuerySchema
	tagSets      []tsi.TagSet
	cursor       comm.KeyCursor
	filterBitmap *bitmap.FilterBitmap
	span         *tracing.Span
	recPool      *record.CircularRecordPool
}

func NewFilterCursor(schema *executor.QuerySchema, tagSets []tsi.TagSet, itr comm.KeyCursor, schemas record.Schemas,
	filterOption immutable.BaseFilterOptions) (*FilterCursor, error) {
	filterCursor := &FilterCursor{
		options: &filterOption,
		schema:  schema,
		tagSets: tagSets,
		cursor:  itr,
	}
	conditionFunction, err := binaryfilterfunc.NewCondition(nil, schema.Options().GetValueCondition(), schemas, schema.Options())
	if err != nil {
		return nil, err
	}
	filterCursor.options.CondFunctions = conditionFunction
	filterBitmap := bitmap.NewFilterBitmap(conditionFunction.NumFilter())
	filterCursor.filterBitmap = filterBitmap
	return filterCursor, nil
}

func (fc *FilterCursor) StartSpan(span *tracing.Span) {
	if span != nil {
		fc.span = span
		fc.cursor.StartSpan(fc.span)
	}
}

func (fc *FilterCursor) EndSpan() {
}

func (fc *FilterCursor) SetOps(ops []*comm.CallOption) {
	fc.cursor.SetOps(ops)
}

func (fc *FilterCursor) SinkPlan(plan hybridqp.QueryNode) {
	fc.cursor.SinkPlan(plan)
	fc.recPool = record.NewCircularRecordPool(FilterCursorPool, filterCursorRecordNum, fc.GetSchema(), false)
}

func (fc *FilterCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return nil, nil, nil
}

func (fc *FilterCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	for {
		rec, fileInfo, err := fc.cursor.NextAggData()
		if err != nil {
			return nil, nil, err
		}
		if rec == nil {
			return nil, fileInfo, err
		}
		filterRec := fc.recPool.Get()
		filterRec = immutable.FilterByFieldFuncs(rec, filterRec, fc.options, fc.filterBitmap)
		if filterRec != nil && filterRec.RowNums() > 0 {
			return filterRec, fileInfo, nil
		}
	}
}

func (fc *FilterCursor) Name() string {
	return "filterCursor"
}

func (fc *FilterCursor) Close() error {
	return fc.cursor.Close()
}

func (fc *FilterCursor) GetSchema() record.Schemas {
	return fc.cursor.GetSchema()
}
