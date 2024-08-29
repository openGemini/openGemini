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

package engine

import (
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
)

type limitCursor struct {
	count           int
	schema          *executor.QuerySchema
	cursor          comm.KeyCursor
	span            *tracing.Span
	nextFunctions   func() (*record.Record, comm.SeriesInfoIntf, error)
	nextHelper      func() (*record.Record, *comm.FileInfo, error)
	RecordCutHelper func(start, end int, src, des *record.Record)
}

func (lc *limitCursor) StartSpan(span *tracing.Span) {
	if span != nil {
		lc.span = span
		lc.cursor.StartSpan(lc.span)
	}
}

func (lc *limitCursor) EndSpan() {
}

func (lc *limitCursor) SetOps(ops []*comm.CallOption) {
	lc.cursor.SetOps(ops)
}

func (lc *limitCursor) SinkPlan(plan hybridqp.QueryNode) {
	if plan.Children() != nil {
		lc.cursor.SinkPlan(plan.Children()[0])
	}
}

func (lc *limitCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return lc.nextFunctions()
}

func (lc *limitCursor) limitHelperForPassThrough() (*record.Record, comm.SeriesInfoIntf, error) {
	return lc.cursor.Next()
}

func (lc *limitCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return lc.nextHelper()
}

func (lc *limitCursor) limitHelperForPassThroughNew() (*record.Record, *comm.FileInfo, error) {
	return lc.cursor.NextAggData()
}

func (lc *limitCursor) limitNextHelperForMultipleRows() (*record.Record, *comm.FileInfo, error) {
	opt := lc.schema.Options()
	if lc.count >= opt.GetLimit()+opt.GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.NextAggData()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := len(rec.IntervalIndex)
	lc.count += numRow
	if lc.count < opt.GetLimit()+opt.GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, rec.IntervalIndex[lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow], rec, re)
	return re, info, nil
}

func (lc *limitCursor) limitNextHelperForSingleRow() (*record.Record, *comm.FileInfo, error) {
	opt := lc.schema.Options()
	if lc.count >= opt.GetLimit()+opt.GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.NextAggData()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := rec.RowNums()
	lc.count += numRow
	if lc.count < opt.GetLimit()+opt.GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow, rec, re)
	return re, info, nil
}

func (lc *limitCursor) limitHelperForSingleRow() (*record.Record, comm.SeriesInfoIntf, error) {
	if lc.count >= lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.Next()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := rec.RowNums()
	lc.count += numRow
	if lc.count < lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow, rec, re)
	return re, info, nil
}

func (lc *limitCursor) limitHelperForMultipleRows() (*record.Record, comm.SeriesInfoIntf, error) {
	if lc.count >= lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return nil, nil, nil
	}
	rec, info, err := lc.cursor.Next()
	if err != nil {
		return nil, nil, err
	}
	if rec.RowNums() == 0 {
		return nil, nil, nil
	}
	numRow := len(rec.IntervalIndex)
	lc.count += numRow
	if lc.count < lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset() {
		return rec, info, nil
	}
	re := record.NewRecordBuilder(rec.Schema)
	lc.RecordCutHelper(0, rec.IntervalIndex[lc.schema.Options().GetLimit()+lc.schema.Options().GetOffset()-lc.count+numRow], rec, re)
	return re, info, nil
}

func (lc *limitCursor) Name() string {
	return "limitCursor"
}

func (lc *limitCursor) Close() error {
	return lc.cursor.Close()
}

func (lc *limitCursor) GetSchema() record.Schemas {
	return lc.cursor.GetSchema()
}

func NewLimitCursor(schema *executor.QuerySchema, helper func(start, end int, src, des *record.Record)) *limitCursor {
	limitCursor := &limitCursor{
		count:           0,
		schema:          schema,
		RecordCutHelper: helper,
	}
	if CanNotAggOnSeriesFunc(schema.Calls()) {
		limitCursor.nextFunctions = limitCursor.limitHelperForPassThrough
		limitCursor.nextHelper = limitCursor.limitHelperForPassThroughNew
	} else {
		limitCursor.FindLimitHelper()
	}
	if limitCursor.nextFunctions == nil {
		panic("CurSorLimit hasn't get any limit function")
	}
	return limitCursor
}

func (lc *limitCursor) SetCursor(keyCursor comm.KeyCursor) {
	lc.cursor = keyCursor
}

func (lc *limitCursor) FindLimitHelper() {
	switch lc.schema.LimitType() {
	case hybridqp.SingleRowIgnoreTagLimit:
		lc.nextFunctions = lc.limitHelperForSingleRow
		lc.nextHelper = lc.limitNextHelperForSingleRow
	case hybridqp.MultipleRowsIgnoreTagLimit:
		lc.nextFunctions = lc.limitHelperForMultipleRows
		lc.nextHelper = lc.limitNextHelperForMultipleRows
	}
}
