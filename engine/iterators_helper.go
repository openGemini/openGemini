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

package engine

import (
	"bytes"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

func newCursorSchema(ctx *idKeyCursorContext, schema *executor.QuerySchema) error {
	var err error
	var filterConditions []*influxql.VarRef
	filterConditions, err = getFilterFieldsByExpr(schema.Options().GetCondition(), filterConditions[:0])
	if err != nil {
		log.Error("get field filter fail", zap.Error(err))
		return err
	}
	ctx.auxTags, ctx.schema = NewRecordSchema(schema, ctx.auxTags[:0], ctx.schema[:0], filterConditions, ctx.engineType)
	if ctx.auxTags == nil && ctx.schema.Len() <= 1 {
		return nil
	}
	ctx.filterOption.FieldsIdx = ctx.filterOption.FieldsIdx[:0]
	ctx.filterOption.FilterTags = ctx.filterOption.FilterTags[:0]
	for _, f := range filterConditions {
		idx := ctx.schema.FieldIndex(f.Val)
		if idx >= 0 && f.Type != influxql.Unknown {
			ctx.filterOption.FieldsIdx = append(ctx.filterOption.FieldsIdx, idx)
		} else if f.Type != influxql.Unknown {
			ctx.filterOption.FilterTags = append(ctx.filterOption.FilterTags, f.Val)
		}
	}

	return nil
}

func getFilterFieldsByExpr(expr influxql.Expr, dst []*influxql.VarRef) ([]*influxql.VarRef, error) {
	if expr == nil {
		return dst, nil
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		dst, _ = getFilterFieldsByExpr(expr.LHS, dst)
		dst, _ = getFilterFieldsByExpr(expr.RHS, dst)
		return dst, nil
	case *influxql.ParenExpr:
		return getFilterFieldsByExpr(expr.Expr, dst)
	case *influxql.BooleanLiteral:
		return dst, nil
	case *influxql.VarRef:
		dst = append(dst, expr)
		return dst, nil
	default:
	}
	return dst, nil
}

func hasMultipleColumnsWithFirst(schema *executor.QuerySchema) bool {
	if len(schema.Calls()) > 1 {
		for _, c := range schema.Calls() {
			if c.Name == "first" || c.Name == "last" {
				return true
			}
		}
	}
	return false
}

func matchPreAgg(schema *executor.QuerySchema, ctx *idKeyCursorContext) bool {
	if !config.GetCommon().PreAggEnabled {
		return false
	}

	if !schema.HasCall() {
		return false
	}

	if schema.HasNonPreCall() {
		return false
	}

	if schema.HasInterval() {
		return false
	}

	if ctx.hasFieldCondition() || schema.HasFieldCondition() {
		return false
	}

	if schema.Options().GetHintType() == hybridqp.ExactStatisticQuery {
		return false
	}
	return true
}

func getFirstTime(orderFirstTime, unorderFirstTime int64, ascending bool) int64 {
	var limitFirstTime int64
	if orderFirstTime == 0 || orderFirstTime == -1 {
		limitFirstTime = unorderFirstTime
	} else if unorderFirstTime == 0 || unorderFirstTime == -1 {
		limitFirstTime = orderFirstTime
	} else if !ascending {
		if orderFirstTime > unorderFirstTime {
			limitFirstTime = orderFirstTime
		} else {
			limitFirstTime = unorderFirstTime
		}
	} else {
		if orderFirstTime < unorderFirstTime {
			limitFirstTime = orderFirstTime
		} else {
			limitFirstTime = unorderFirstTime
		}
	}
	return limitFirstTime
}

func CanNotAggOnSeriesFunc(m map[string]*influxql.Call) bool {
	for _, call := range m {
		if executor.NotAggOnSeries[call.Name] {
			return true
		}
	}
	return false
}

func GetMinTime(minTime int64, rec *record.Record, isAscending bool) int64 {
	min := rec.MinTime(isAscending)
	if minTime < 0 || min < minTime {
		minTime = min
	}
	return minTime
}

func GetMaxTime(maxTime int64, rec *record.Record, isAscending bool) int64 {
	max := rec.MaxTime(isAscending)
	if maxTime < 0 || max > maxTime {
		maxTime = max
	}
	return maxTime
}

func compareTags(pt1, pt2 []byte, ascending bool) bool {
	if ascending {
		return bytes.Compare(pt1, pt2) <= 0
	}
	return bytes.Compare(pt1, pt2) >= 0
}

type recordIter struct {
	record      *record.Record
	pos, rowCnt int
}

func (r *recordIter) init(rec *record.Record) {
	r.record = rec
	r.pos = 0
	if rec != nil {
		r.rowCnt = rec.RowNums()
	} else {
		r.rowCnt = 0
	}
}

func (r *recordIter) reset() {
	r.pos = 0
	r.rowCnt = 0
	r.record = nil
}

func (r *recordIter) hasRemainData() bool {
	return r.pos < r.rowCnt
}

func (r *recordIter) updatePos(newPos int) {
	r.pos = newPos
}

// cur a new record from r.record, start from r.pos and limit num is maxRow
func (r *recordIter) cutRecord(maxRow int) *record.Record {
	var rec record.Record
	remain := r.rowCnt - r.pos
	if remain > maxRow {
		remain = maxRow
	}
	rec.SliceFromRecord(r.record, r.pos, r.pos+remain)
	r.pos += remain
	return &rec
}

func (r *recordIter) readMemTableMetaRecord(ops []*comm.CallOption) {
	if r.record == nil {
		return
	}
	schema := r.record.Schema

	if r.record.RecMeta == nil {
		r.record.RecMeta = &record.RecMeta{}
	}

	if cap(r.record.ColMeta) < len(schema)-1 {
		r.record.ColMeta = make([]record.ColMeta, len(schema)-1)
	}

	timeCol := r.record.TimeColumn()

	for _, call := range ops {
		if r.record == nil {
			return
		}
		idx := r.record.Schema.FieldIndex(call.Ref.Val)
		if idx < 0 {
			continue
		}

		switch r.record.Schema[idx].Type {
		case influx.Field_Type_Int:
			r.setIntColumnMeta(timeCol, idx, r.record, ops)
		case influx.Field_Type_String, influx.Field_Type_Tag:
			r.setStringColumnMeta(timeCol, idx, r.record, ops)
		case influx.Field_Type_Float:
			r.setFloatColumnMeta(timeCol, idx, r.record, ops)
		case influx.Field_Type_Boolean:
			r.setBoolColumnMeta(timeCol, idx, r.record, ops)
		default:
			return
		}
	}
}

func (r *recordIter) setIntColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.IntegerValues()
	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var minV, maxV, minVTime, maxVTime, sumV, countV int64
	var colIndex, lastIndex, firstIndex, minIndex, maxIndex int
	nilCount := 0
	colIndex = -1
	lastIndex, firstIndex, minIndex, maxIndex = -1, -1, -1, -1
	firstInit := false
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		if !firstInit {
			minV = cols[index-nilCount]
			minVTime = timeCol
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			firstIndex, minIndex, maxIndex = index, index, index
			firstInit = true
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
			firstIndex = index
		}
		if cols[index-nilCount] < minV || (cols[index-nilCount] == minV && minVTime > timeCol) {
			minV = cols[index-nilCount]
			minVTime = timeCol
			minIndex = index
		}

		if cols[index-nilCount] > maxV || (cols[index-nilCount] == maxV && maxVTime > timeCol) {
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			maxIndex = index
		}

		sumV += cols[index-nilCount]
		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetMin(minV, minVTime)
	rec.ColMeta[idx].SetMax(maxV, maxVTime)
	rec.ColMeta[idx].SetCount(countV)
	rec.ColMeta[idx].SetSum(sumV)

	setColValInAux(timeColVals, idx, ops, rec, minIndex, firstIndex, maxIndex, lastIndex)
}

func (r *recordIter) setBoolColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.BooleanValues()

	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var minVTime, maxVTime, countV int64
	var minV, maxV bool
	var colIndex, lastIndex, firstIndex, minIndex, maxIndex int
	nilCount := 0
	lastIndex, firstIndex, minIndex, maxIndex = -1, -1, -1, -1

	countV = 0
	colIndex = -1
	firstInit := false
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		if !firstInit {
			minV = cols[index-nilCount]
			minVTime = timeCol
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			firstInit = true
			firstIndex, minIndex, maxIndex = index, index, index
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
		}
		if minV && !cols[index-nilCount] {
			minV = cols[index-nilCount]
			minVTime = timeCol
			minIndex = index
		}

		if !maxV && cols[index-nilCount] {
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			maxIndex = index
		}
		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetMin(minV, minVTime)
	rec.ColMeta[idx].SetMax(maxV, maxVTime)
	rec.ColMeta[idx].SetCount(countV)

	setColValInAux(timeColVals, idx, ops, rec, minIndex, firstIndex, maxIndex, lastIndex)
}

func (r *recordIter) setFloatColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.FloatValues()

	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var minVTime, maxVTime, countV int64
	var minV, maxV, sumV float64
	var colIndex, lastIndex, firstIndex, minIndex, maxIndex int
	nilCount := 0
	colIndex = -1
	lastIndex, firstIndex, minIndex, maxIndex = -1, -1, -1, -1
	sumV = 0
	countV = 0
	firstInit := false
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		if !firstInit {
			minV = cols[index-nilCount]
			minVTime = timeCol
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			firstInit = true
			firstIndex, minIndex, maxIndex = index, index, index
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
		}
		if cols[index-nilCount] < minV || (cols[index-nilCount] == minV && minVTime > timeCol) {
			minV = cols[index-nilCount]
			minVTime = timeCol
			minIndex = index
		}

		if cols[index-nilCount] > maxV || (cols[index-nilCount] == maxV && maxVTime > timeCol) {
			maxV = cols[index-nilCount]
			maxVTime = timeCol
			maxIndex = index
		}

		sumV += cols[index-nilCount]
		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetMin(minV, minVTime)
	rec.ColMeta[idx].SetMax(maxV, maxVTime)
	rec.ColMeta[idx].SetCount(countV)
	rec.ColMeta[idx].SetSum(sumV)

	setColValInAux(timeColVals, idx, ops, rec, minIndex, firstIndex, maxIndex, lastIndex)
}

func (r *recordIter) setStringColumnMeta(timeColVals *record.ColVal, idx int, rec *record.Record, ops []*comm.CallOption) {
	timeCols := timeColVals.IntegerValues()
	colVals := rec.ColVals[idx]
	cols := colVals.StringValues(nil)

	if cols == nil {
		if len(ops) == 1 {
			r.reset()
		}
		return
	}

	var colIndex, lastIndex, firstIndex int
	nilCount := 0
	colIndex = -1
	lastIndex, firstIndex = -1, -1
	var countV int64
	countV = 0
	for index, timeCol := range timeCols {
		if colVals.IsNil(index) {
			nilCount += 1
			continue
		}
		countV += 1
		colIndex += 1
		if colIndex == 0 {
			firstIndex = index
			rec.ColMeta[idx].SetFirst(cols[index-nilCount], timeCol)
		}

		lastIndex = colIndex
	}

	rec.ColMeta[idx].SetLast(cols[lastIndex], timeCols[len(timeCols)-1])
	rec.ColMeta[idx].SetCount(countV)
	setColValInAux(timeColVals, idx, ops, rec, -1, firstIndex, -1, lastIndex)
}

// mergeData is used for merge two record iter data(eg, mem table and immutable or order and out order in immutable)
// both scenery need merge data since data in two iters may overlap or duplicate in time, so we abstract this method
// newRecIter hold the newer data, like mem table or out of order data, and it's rows may exceed maxRow limit
// baseRecIter hold the older data, which rows will not exceed maxRow limit, and return record's row should not
// exceed maxRow limit
func mergeData(newRecIter, baseRecIter *recordIter, maxRow int, ascending bool) *record.Record {
	if newRecIter == nil || baseRecIter == nil {
		return nil
	}

	if newRecIter.record == nil && baseRecIter.record == nil {
		return nil
	}

	if baseRecIter.hasRemainData() && newRecIter.hasRemainData() {
		var mergeRec record.Record
		newPos, oldPos := mergeRec.MergeRecordByMaxTimeOfOldRec(newRecIter.record, baseRecIter.record,
			newRecIter.pos, baseRecIter.pos, maxRow, ascending)
		newRecIter.updatePos(newPos)
		baseRecIter.updatePos(oldPos)
		return &mergeRec
	} else if baseRecIter.hasRemainData() {
		// oldIter record's row will not exceed maxRow, so we return record directly
		if baseRecIter.pos == 0 {
			rec := baseRecIter.record
			baseRecIter.reset()
			return rec
		}
		// pos not zero, need generate a new record from pos to end
		return baseRecIter.cutRecord(maxRow)
	} else if newRecIter.hasRemainData() {
		return newRecIter.cutRecord(maxRow)
	}

	return nil
}

func setSchemaColVal(field *record.Field, col *record.ColVal, rowIndex int) {
	switch field.Type {
	case influx.Field_Type_Float:
		value, isNil := col.FloatValue(rowIndex)
		if !isNil {
			col.Init()
			col.AppendFloat(value)
		}
	case influx.Field_Type_Int:
		value, isNil := col.IntegerValue(rowIndex)
		if !isNil {
			col.Init()
			col.AppendInteger(value)
		}
	case influx.Field_Type_String:
		value, isNil := col.StringValueSafe(rowIndex)
		if !isNil {
			col.Init()
			col.AppendString(value)
		}
	case influx.Field_Type_Boolean:
		value, isNil := col.BooleanValue(rowIndex)
		if !isNil {
			col.Init()
			col.AppendBoolean(value)
		}
	}
}

func setColValInAux(timeColVals *record.ColVal, idx int, ops []*comm.CallOption, rec *record.Record, minIndex, firstIndex, maxIndex, lastIndex int) {
	if rec.Schema.Len() > 2 && len(ops) == 1 {
		for i := range rec.Schema[:len(rec.Schema)-1] {
			field := &rec.Schema[i]
			col := rec.Column(i)
			timeColVals.Init()
			switch ops[0].Call.Name {
			case "min":
				setSchemaColVal(field, col, minIndex)
				_, minVTime := rec.ColMeta[idx].Min()
				timeColVals.AppendInteger(minVTime)
			case "max":
				setSchemaColVal(field, col, maxIndex)
				_, maxVTime := rec.ColMeta[idx].Max()
				timeColVals.AppendInteger(maxVTime)
			case "first":
				setSchemaColVal(field, col, firstIndex)
				_, firstVtime := rec.ColMeta[idx].First()
				timeColVals.AppendInteger(firstVtime)
			case "last":
				setSchemaColVal(field, col, lastIndex)
				_, lastVtime := rec.ColMeta[idx].Last()
				timeColVals.AppendInteger(lastVtime)
			}
		}
	}
}
