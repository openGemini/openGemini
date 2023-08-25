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
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
)

const (
	fileCursorDurationSpan = "fileCursor_duration"
)

var (
	defaultChunkMetaSize uint64 = 4096
	RecordIteratorPool          = &sync.Pool{}
	ChunkMetaBufferPool         = bufferpool.NewByteBufferPool(defaultChunkMetaSize)
)

func getRecordIterator() *recordIter {
	v := RecordIteratorPool.Get()
	if v != nil {
		return v.(*recordIter)
	}
	itr := &recordIter{}
	return itr
}
func putRecordIterator(itr *recordIter) {
	itr.reset()
	RecordIteratorPool.Put(itr)
}

type fileCursor struct {
	ascending   bool
	isLastFile  bool
	isPreAgg    bool
	start       int
	index       int
	step        int
	buf         []byte
	minT, maxT  int64
	schema      record.Schemas
	file        immutable.TSSPFile
	tagSet      *tsi.TagSetInfo
	loc         *immutable.Location
	querySchema *executor.QuerySchema
	span        *tracing.Span
	ctx         *idKeyCursorContext
	seriesIter  *SeriesIter
	memIter     *recordIter
	recordPool  *record.CircularRecordPool
	memRecIters map[uint64][]*SeriesIter
}

type SeriesIter struct {
	iter  *recordIter
	index int
}

type DataBlockInfo struct {
	sid         uint64
	record      *record.Record
	sInfo       *seriesInfo
	index       int
	tagSetIndex int
}

const (
	fileCursorRecordNum = 2
)

var (
	FileCursorPool = record.NewRecordPool(record.FileCursorPool)
)

/*
	FileCursor read data from tssp by sid step by step.
	While use next to get data, this cursor will return all data about now sid and then step to the next sid.
*/

func newFileCursor(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, file immutable.TSSPFile, memRecIters map[uint64][]*SeriesIter) (*fileCursor, error) {
	if len(tagSet.IDs) == 0 {
		return nil, nil
	}
	loc := immutable.NewLocation(file, ctx.decs)
	var err error
	minT, maxT, err := file.MinMaxTime()
	if err != nil {
		return nil, err
	}
	c := &fileCursor{
		ctx:         ctx,
		file:        file,
		start:       start,
		index:       0,
		step:        step,
		span:        span,
		loc:         loc,
		minT:        minT,
		maxT:        maxT,
		querySchema: schema,
		tagSet:      tagSet,
		schema:      ctx.schema.Copy(),
		ascending:   schema.Options().IsAscending(),
		memRecIters: memRecIters,
		isPreAgg:    ctx.decs.MatchPreAgg(),
	}
	c.buf = ChunkMetaBufferPool.Get()
	c.seriesIter = &SeriesIter{iter: &recordIter{}}
	c.seriesIter.iter.reset()
	c.memIter = &recordIter{}
	c.memIter.reset()
	if len(ctx.decs.GetOps()) > 0 {
		c.recordPool = record.NewCircularRecordPool(FileCursorPool, fileCursorRecordNum, c.schema, true)
	} else {
		c.recordPool = record.NewCircularRecordPool(FileCursorPool, fileCursorRecordNum, c.schema, false)
	}
	return c, nil
}

func (f *fileCursor) next() (*DataBlockInfo, error) {
	var tm time.Time
	var duration time.Duration
	if f.span != nil {
		tm = time.Now()
	}
	var data *DataBlockInfo
	var err error
	if f.isPreAgg {
		data, err = f.readPreAggData()
	} else {
		data, err = f.readData()
	}

	if f.span != nil {
		duration = time.Since(tm)
		f.span.Count(fileCursorDurationSpan, int64(duration))
	}

	return data, err
}

func (f *fileCursor) SetLastFile() {
	f.isLastFile = true
}

func (f *fileCursor) nextIndex(current int) {
	next := current + f.step
	if next < len(f.tagSet.IDs) && f.tagSet.IDs[next] != f.tagSet.IDs[current] {
		f.start = next
		f.index = 0
	} else {
		f.index++
	}
}

/*
step is the number of concurrent goroutines, that is, the number of subTagSetIds,
start is the index of the first same ID in tagSetIds, index is the index in the same IDs.
For example:
step = 2, tagSetIds = [5 5 5 6 7 7 8 8 8 9], subTagSetIds: [5 5 7 8 8], start: [0 0 4 6 6], index: [0 1 0 0 1]
*/
func (f *fileCursor) readPreAggData() (*DataBlockInfo, error) {
	for {
		idx := f.index
		i := f.start + idx*f.step
		if i >= len(f.tagSet.IDs) {
			return nil, nil
		}
		f.nextIndex(i)
		sid := f.tagSet.IDs[i]
		ptTags := &(f.tagSet.TagsVec[i])
		sInfo := &seriesInfo{sid: sid, tags: *ptTags, key: f.tagSet.SeriesKeys[i]}
		f.loc.ResetMeta()
		contains, err := f.loc.Contains(sid, f.ctx.tr, &f.buf)
		if err != nil {
			return nil, err
		}
		// if sid not in files, return memdata
		if !contains {
			data := f.readInMemData(i, idx, sInfo)
			if data != nil {
				return data, err
			}
			continue
		}

		filter := f.tagSet.Filters[i]
		rowFilter := f.tagSet.GetRowFilter(i)
		filterOpts := immutable.NewFilterOpts(filter, &f.ctx.filterOption, ptTags, rowFilter)
		orderRec := f.recordPool.Get()
		rec, err := f.loc.ReadData(filterOpts, orderRec)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			f.recordPool.PutRecordInCircularPool()
			data := f.readInMemData(i, idx, sInfo)
			if data != nil {
				return data, err
			}
			continue
		}

		if _, ok := f.memRecIters[sid]; ok && f.isMemRecIterValid(sid, idx) {
			immutable.AggregateData(f.memRecIters[sid][idx].iter.record, rec, f.ctx.decs.GetOps())
			immutable.ResetAggregateData(f.memRecIters[sid][idx].iter.record, f.ctx.decs.GetOps())
			orderRec = f.memRecIters[sid][idx].iter.record.Copy()
			putRecordIterator(f.memRecIters[sid][idx].iter)
			if idx+1 == len(f.memRecIters[sid]) {
				delete(f.memRecIters, sid)
			}
		} else {
			orderRec = rec
		}
		return &DataBlockInfo{sInfo: sInfo, record: orderRec, sid: sid, index: idx, tagSetIndex: i}, nil
	}
}

func (f *fileCursor) readInMemData(tagSetIdx, idx int, sInfo *seriesInfo) *DataBlockInfo {
	var data *DataBlockInfo
	sid := sInfo.GetSid()
	if _, ok := f.memRecIters[sid]; ok {
		if f.isMemRecIterValid(sid, idx) {
			data = &DataBlockInfo{sInfo: sInfo, record: f.memRecIters[sid][idx].iter.record.Copy(),
				sid: sid, index: idx, tagSetIndex: tagSetIdx}
			putRecordIterator(f.memRecIters[sid][idx].iter)
		}
		if idx+1 == len(f.memRecIters[sid]) {
			delete(f.memRecIters, sid)
		}
	}
	return data
}

func (f *fileCursor) readData() (*DataBlockInfo, error) {
	for {
		idx := f.index
		i := f.start + idx*f.step
		if i >= len(f.tagSet.IDs) {
			return nil, nil
		}
		sid := f.tagSet.IDs[i]
		ptTags := &(f.tagSet.TagsVec[i])
		sInfo := &seriesInfo{sid: sid, tags: *ptTags, key: f.tagSet.SeriesKeys[i]}
		if f.seriesIter.iter.hasRemainData() {
			orderRec := mergeData(f.memIter, f.seriesIter.iter, f.querySchema.Options().ChunkSizeNum(), f.ascending)
			orderRec = orderRec.KickNilRow()
			return &DataBlockInfo{sInfo: sInfo, record: orderRec, sid: sid, index: idx, tagSetIndex: i}, nil
		}
		m := f.loc.GetChunkMeta()
		if m == nil || m.GetSid() != sid {
			contains, err := f.loc.Contains(sid, f.ctx.tr, &f.buf)
			if err != nil {
				return nil, err
			}
			if !contains {
				f.loc.ResetMeta()
				if f.isLastFile {
					if _, ok := f.memRecIters[sid]; ok && f.isMemRecIterValid(sid, idx) && f.memRecIters[sid][idx].iter.hasRemainData() {
						r := f.GetMemData(i, idx, sInfo)
						if r != nil && r.record != nil && r.record.RowNums() != 0 {
							f.nextIndex(i)
							return r, nil
						}
					}
				}
				f.nextIndex(i)
				continue
			}
		}

		if !f.memIter.hasRemainData() {
			if _, ok := f.memRecIters[sid]; ok && f.isMemRecIterValid(sid, idx) && f.memRecIters[sid][idx].iter.hasRemainData() {
				f.memIter.init(f.getSeriesNotInFile(sid, idx))
			}
		}
		filter := f.tagSet.Filters[i]
		rowFilter := f.tagSet.GetRowFilter(i)
		filterOpts := immutable.NewFilterOpts(filter, &f.ctx.filterOption, ptTags, rowFilter)
		orderRec := f.recordPool.Get()
		rec, err := f.loc.ReadData(filterOpts, orderRec)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			if f.memIter.hasRemainData() {
				orderRec = mergeData(f.memIter, f.seriesIter.iter, f.querySchema.Options().ChunkSizeNum(), f.ascending)
				orderRec = orderRec.KickNilRow()
				return &DataBlockInfo{sInfo: sInfo, record: orderRec, sid: sid, index: idx, tagSetIndex: i}, nil
			}
			f.loc.ResetMeta()
			f.memIter.reset()
			f.seriesIter.iter.reset()
			f.nextIndex(i)
			f.recordPool.PutRecordInCircularPool()
			continue
		}
		f.seriesIter.iter.init(rec)
		r := mergeData(f.memIter, f.seriesIter.iter, f.querySchema.Options().ChunkSizeNum(), f.ascending)
		r = r.KickNilRow()
		if r.RowNums() == 0 {
			f.recordPool.PutRecordInCircularPool()
			continue
		}
		return &DataBlockInfo{sInfo: sInfo, record: r, sid: sid, index: idx, tagSetIndex: i}, nil
	}
}

func (f *fileCursor) isMemRecIterValid(sid uint64, i int) bool {
	return i < len(f.memRecIters[sid]) && f.memRecIters[sid][i] != nil && f.memRecIters[sid][i].iter.record != nil
}

func (f *fileCursor) GetMemData(tagSetIdx, i int, sInfo *seriesInfo) *DataBlockInfo {
	sid := sInfo.GetSid()
	endIndex := f.getMemEndIndex(sid, i)
	r := f.memRecIters[sid][i].iter.cutRecord(endIndex - f.memRecIters[sid][i].iter.pos)
	r = r.KickNilRow()
	return &DataBlockInfo{sInfo: sInfo, record: r, sid: sInfo.sid, index: i, tagSetIndex: tagSetIdx}
}

func (f *fileCursor) getSeriesNotInFile(sid uint64, i int) *record.Record {
	endIndex := f.getMemEndIndex(sid, i)
	if endIndex == 0 && f.memRecIters[sid][i].iter.record == nil {
		return nil
	}
	if endIndex <= f.memRecIters[sid][i].iter.pos {
		return nil
	}
	return f.memRecIters[sid][i].iter.cutRecord(endIndex - f.memRecIters[sid][i].iter.pos)
}

func (f *fileCursor) getMemEndIndex(sid uint64, i int) int {
	if f.isLastFile {
		return f.memRecIters[sid][i].iter.rowCnt
	}
	var endIndex int
	t := f.memRecIters[sid][i].iter.record.Times()
	minT, maxT := f.loc.GetChunkMeta().MinMaxTime()
	if f.ascending {
		endIndex = record.GetTimeRangeEndIndex(t, f.memRecIters[sid][i].iter.pos, maxT) + 1
	} else {
		endIndex = record.GetTimeRangeEndIndexDescend(t, f.memRecIters[sid][i].iter.pos, minT) + 1
	}
	return endIndex
}

func (f *fileCursor) reset() {
	f.ctx = nil
	f.loc = nil
	f.span = nil
	f.file = nil
	f.tagSet = nil
	f.memRecIters = nil
	f.seriesIter.iter.reset()
	f.memIter.reset()
}

func (f *fileCursor) reInit(ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema,
	tagSet *tsi.TagSetInfo, start, step int, file immutable.TSSPFile, memRecIters map[uint64][]*SeriesIter) error {
	f.tagSet = tagSet
	f.file = file
	f.ctx = ctx
	if len(ctx.decs.GetOps()) > 0 {
		f.recordPool = record.NewCircularRecordPool(FileCursorPool, fileCursorRecordNum, f.schema, true)
	} else {
		f.recordPool = record.NewCircularRecordPool(FileCursorPool, fileCursorRecordNum, f.schema, false)
	}
	f.loc = immutable.NewLocation(file, f.ctx.decs)
	var err error
	f.minT, f.maxT, err = file.MinMaxTime()
	if err != nil {
		return err
	}
	f.memRecIters = memRecIters
	f.querySchema = schema
	f.start = start
	f.index = 0
	f.step = step
	f.span = span
	return nil
}

func (f *fileCursor) Close() error {
	if f.recordPool != nil {
		f.recordPool.Put()
	}
	ChunkMetaBufferPool.Put(f.buf)
	f.reset()
	return nil
}
