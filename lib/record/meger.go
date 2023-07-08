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

package record

import (
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type MergeColVal struct {
	typ int
	col *ColVal

	// data before offset has been merged
	offset int

	// number of valid values before offset
	valid int
}

func NewMergeColVal() *MergeColVal {
	return &MergeColVal{col: &ColVal{}}
}

func (mcv *MergeColVal) init(col *ColVal, typ int) {
	mcv.col = col
	mcv.typ = typ

	mcv.offset = 0
	mcv.valid = 0
}

func (mcv *MergeColVal) AppendSequence(src *MergeColVal, limit int) {
	if limit == 0 {
		return
	}

	mcv.appendSequence(src, mcv.typ, limit)
	src.offset += limit
}

func (mcv *MergeColVal) appendSequence(src *MergeColVal, typ, limit int) {
	if limit == 0 || src.col.Len == 0 || src.offset >= src.col.Len {
		return
	}

	if src.offset == 0 && limit == src.col.Len && mcv.col.Len == 0 {
		mcv.col.AppendAll(src.col)
		return
	}

	valid := src.col.ValidCount(src.offset, src.offset+limit)

	switch typ {
	case influx.Field_Type_String:
		mcv.col.appendStringCol(src.col, src.offset, limit)
	case influx.Field_Type_Int, influx.Field_Type_Float, influx.Field_Type_Boolean:
		mcv.col.appendBytes(src.col, typ, src.valid, src.valid+valid)
	default:
		panic("error type")
	}

	mcv.col.NilCount += limit - valid
	mcv.appendBitmap(src, src.offset, limit)
	mcv.col.Len += limit

	src.valid += valid
}

func (mcv *MergeColVal) appendBitmap(src *MergeColVal, offset, limit int) {
	mcv.col.appendBitmap(src.col.Bitmap, src.col.BitMapOffset, src.col.Len, offset, offset+limit)
}

func (mcv *MergeColVal) IsNil() bool {
	return mcv.col.IsNil(mcv.offset)
}

func (mcv *MergeColVal) Skip(limit int) {
	if mcv.typ == influx.Field_Type_String {
		mcv.offset += limit
		return
	}

	valid := mcv.col.ValidCount(mcv.offset, mcv.offset+limit)
	mcv.offset += limit
	mcv.valid += valid
}

type MergeHelper struct {
	unordered      []*ColVal
	unorderedTimes [][]int64
	performer      *ColMergePerformer
}

func NewMergeHelper() *MergeHelper {
	return &MergeHelper{
		performer: NewColMergePerformer(),
	}
}

func (h *MergeHelper) resetUnordered() {
	h.unordered = h.unordered[:0]
	h.unorderedTimes = h.unorderedTimes[:0]
}

func (h *MergeHelper) AddUnorderedCol(col *ColVal, times []int64) {
	h.unorderedTimes = append(h.unorderedTimes, times)
	h.unordered = append(h.unordered, col)
}

func (h *MergeHelper) Merge(col *ColVal, times []int64, typ int) (*ColVal, []int64, error) {
	if len(h.unordered) == 0 {
		return col, times, nil
	}

	p := h.performer

	for i := 0; i < len(h.unordered); i++ {
		p.InitTimes(times, h.unorderedTimes[i])
		p.InitColVal(col, h.unordered[i], typ)

		h.merge(p)
		col, times = p.MergedResult()
	}

	h.resetUnordered()
	return col, times, nil
}

// merge order and unordered data into new col
// merge all order data and unordered data whose maximum time is less than order data
func (h *MergeHelper) merge(p MergePerformer) {
	order, unordered := p.Times(true), p.Times(false)

	for {
		if order.isEnd() {
			unordered.limit = unordered.len() - unordered.offset
			p.MergeUnordered()
			break
		}

		if unordered.isEnd() {
			order.limit = order.len() - order.offset
			p.MergeOrder()
			break
		}

		// replace the old value with the new value at the same time.
		if order.current() == unordered.current() {
			unordered.incrLimit()
			order.incrLimit()
			p.MergeSameTime()
			continue
		}

		if order.current() < unordered.current() {
			for !order.isEnd() && order.current() < unordered.current() {
				order.incrLimit()
			}
			p.MergeOrder()
			continue
		}

		for !unordered.isEnd() && unordered.current() < order.current() {
			unordered.incrLimit()
		}
		p.MergeUnordered()
	}
}

type MergePerformer interface {
	MergeOrder()
	MergeUnordered()
	MergeSameTime()
	Times(bool) *Times
}

func NewColMergePerformer() *ColMergePerformer {
	return &ColMergePerformer{
		order:          NewMergeColVal(),
		orderTimes:     &Times{},
		unordered:      NewMergeColVal(),
		unorderedTimes: &Times{},
		merged:         NewMergeColVal(),
		swapCol:        &ColVal{},
	}
}

type ColMergePerformer struct {
	order     *MergeColVal
	unordered *MergeColVal

	orderTimes     *Times
	unorderedTimes *Times

	swapCol     *ColVal
	merged      *MergeColVal
	swapTimes   []int64
	mergedTimes []int64
}

func (p *ColMergePerformer) MergedResult() (*ColVal, []int64) {
	return p.merged.col, p.mergedTimes
}

func (p *ColMergePerformer) InitColVal(order, unordered *ColVal, typ int) {
	p.order.init(order, typ)
	p.unordered.init(unordered, typ)

	col := p.merged.col
	p.swapCol.Init()
	p.merged.init(p.swapCol, typ)
	p.swapCol = col
}

func (p *ColMergePerformer) InitTimes(order, unordered []int64) {
	p.orderTimes.init(order)
	p.unorderedTimes.init(unordered)
	p.mergedTimes, p.swapTimes = p.swapTimes, p.mergedTimes
	p.mergedTimes = p.mergedTimes[:0]
}

func (p *ColMergePerformer) Times(order bool) *Times {
	if order {
		return p.orderTimes
	}

	return p.unorderedTimes
}

func (p *ColMergePerformer) MergeSameTime() {
	order, unordered := p.order, p.unordered
	orderTimes, unorderedTimes := p.orderTimes, p.unorderedTimes

	if !unordered.IsNil() {
		p.appendSequence(unordered, unorderedTimes)
		order.Skip(1)
		orderTimes.incrOffset()
	} else {
		p.appendSequence(order, orderTimes)
		unordered.Skip(1)
		unorderedTimes.incrOffset()
	}
}

func (p *ColMergePerformer) MergeOrder() {
	p.merge(true)
}

func (p *ColMergePerformer) MergeUnordered() {
	p.merge(false)
}

func (p *ColMergePerformer) merge(order bool) {
	col := p.order
	times := p.orderTimes
	if !order {
		col = p.unordered
		times = p.unorderedTimes
	}

	p.appendSequence(col, times)
}

func (p *ColMergePerformer) appendSequence(col *MergeColVal, times *Times) {
	if times.limit == 0 {
		return
	}

	p.mergedTimes = append(p.mergedTimes, times.slice()...)
	p.merged.AppendSequence(col, times.limit)
	times.incrOffset()
}

type Times struct {
	values []int64
	offset int
	limit  int
}

func (t *Times) init(v []int64) {
	t.limit = 0
	t.offset = 0
	t.values = v
}

func (t *Times) len() int {
	return len(t.values)
}

func (t *Times) current() int64 {
	return t.values[t.offset+t.limit]
}

func (t *Times) slice() []int64 {
	return t.values[t.offset : t.offset+t.limit]
}

func (t *Times) incrLimit() {
	t.limit++
}

func (t *Times) incrOffset() {
	t.offset += t.limit
	t.limit = 0
}

func (t *Times) isEnd() bool {
	return t.offset+t.limit == len(t.values)
}
