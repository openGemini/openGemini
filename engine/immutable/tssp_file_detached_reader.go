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

package immutable

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	ChunkMetaReadNum      = 16
	BatchReaderRecordNum  = 8
	chunkReadNum          = 16
	ReaderContentNumSpan  = "reader_content_num_span"
	ReaderContentSizeSpan = "reader_content_size_span"
	ReaderContentDuration = "reader_content_duration"
	ReaderFilterDuration  = "reader_filter_duration"
)

type TSSPFileDetachedReader struct {
	isSort          bool
	isInit          bool
	filterSeq       bool
	seqIndex        int
	metaIndexID     int
	currBlockID     int // currMetaRead
	currChunkMetaID int
	shardId         int64
	filterShardId   int64
	filterSeqTime   int64
	filterSeqId     int64
	filterSeqFunc   func(int64, int64) bool
	tr              util.TimeRange

	span            *tracing.Span
	metaDataQueue   MetaControl
	metaIndex       []*MetaIndex
	blocks          [][]int
	obsOpts         *obs.ObsOptions
	chunkMetaReader *DetachedChunkMetaReader
	dataReader      *DetachedMetaDataReader
	recordPool      *record.CircularRecordPool
	filterPool      *record.CircularRecordPool
	logFilterPool   *record.CircularRecordPool
	currChunkMeta   []*ChunkMeta
	unnest          *influxql.Unnest
	unnestOperator  UnnestOperator
	ctx             *FileReaderContext
	tm              time.Time
	options         hybridqp.Options
}

func NewTSSPFileDetachedReader(metaIndex []*MetaIndex, blocks [][]int, ctx *FileReaderContext, path *sparseindex.OBSFilterPath, unnest *influxql.Unnest,
	isSort bool, options hybridqp.Options) (*TSSPFileDetachedReader, error) {
	r := &TSSPFileDetachedReader{
		isSort:        isSort,
		metaIndex:     metaIndex,
		blocks:        blocks,
		obsOpts:       path.Option(),
		ctx:           ctx,
		metaDataQueue: NewMetaControl(ctx.readCtx.Ascending, ChunkMetaReadNum),
		unnest:        unnest,
		seqIndex:      -1,
		shardId:       obs.GetShardID(path.RemotePath()),
		tr:            util.TimeRange{Min: ctx.tr.Min, Max: ctx.tr.Max},
		options:       options,
	}
	if config.IsLogKeeper() && options.GetLogQueryCurrId() != "" && options.GetLimit() > 0 {
		err := r.parseSeqId(options)
		if err != nil {
			return nil, err
		}
	}
	r.recordPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, ctx.schemas, false)
	r.filterPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, ctx.schemas, false)
	var err error
	r.chunkMetaReader, err = NewDetachedChunkMetaReader(path.RemotePath(), path.Option())
	if err != nil {
		return nil, err
	}
	r.dataReader, err = NewDetachedMetaDataReader(path.RemotePath(), path.Option(), isSort)
	if r.unnest != nil {
		var err error
		r.unnestOperator, err = GetUnnestFuncOperator(unnest, r.ctx.schemas)
		if err != nil {
			return nil, err
		}
	}
	return r, err
}

func (t *TSSPFileDetachedReader) parseSeqId(options hybridqp.Options) error {
	arrFirst := strings.SplitN(options.GetLogQueryCurrId(), "^", 3)
	if len(arrFirst) != 3 {
		return fmt.Errorf("wrong scroll_id")
	}
	if arrFirst[0] == "" {
		return nil
	}
	arr := strings.Split(arrFirst[0], "|")
	if len(arr) == 3 {
		time, err := strconv.ParseInt(arr[0], 10, 64)
		if err != nil {
			return err
		}
		t.filterShardId, err = strconv.ParseInt(arr[1], 10, 64)
		if err != nil {
			return err
		}
		seqId, err := strconv.ParseInt(arr[2], 10, 64)
		if err != nil {
			return err
		}
		t.filterSeqTime = time
		t.filterSeqId = seqId
	}
	isExist := false
	for _, v := range t.GetSchema() {
		if v.Name == record.SeqIDField {
			isExist = true
			break
		}
	}
	if !isExist {
		return nil
	}
	t.filterSeq = true
	t.logFilterPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, t.ctx.schemas, false)
	if options.IsAscending() {
		t.filterSeqFunc = func(cmp, origin int64) bool {
			if t.filterShardId == t.shardId {
				return cmp < origin
			}
			return t.filterShardId < t.shardId
		}
	} else {
		t.filterSeqFunc = func(cmp, origin int64) bool {
			if t.filterShardId == t.shardId {
				return cmp > origin
			}
			return t.filterShardId > t.shardId
		}
	}
	return nil
}

func (t *TSSPFileDetachedReader) filterForLog(rec *record.Record) *record.Record {
	if !t.filterSeq {
		return rec
	}
	if rec == nil || rec.RowNums() == 0 {
		return rec
	}
	if t.seqIndex == -1 {
		t.seqIndex = rec.Schema.FieldIndex(record.SeqIDField)
	}
	if t.ctx.readCtx.Ascending && (t.filterSeqTime < rec.Time(0) || t.filterSeqTime > rec.Time(rec.RowNums()-1)) {
		return rec
	} else if !t.ctx.readCtx.Ascending && (t.filterSeqTime > rec.Time(0) || t.filterSeqTime < rec.Time(rec.RowNums()-1)) {
		return rec
	}
	rows := make([]int, 0, rec.RowNums())
	isExist := false
	for k, v := range rec.Times() {
		if v == t.filterSeqTime {
			currSeq, isNil := rec.ColVals[t.seqIndex].IntegerValue(k)
			if !isNil && !t.filterSeqFunc(t.filterSeqId, currSeq) {
				isExist = true
			} else {
				rows = append(rows, k)
			}
		} else {
			rows = append(rows, k)
		}
	}

	if !isExist {
		return rec
	}
	return GenRecByReserveIds(rec, t.logFilterPool.Get(), rows, make(map[int]struct{}))
}

func (t *TSSPFileDetachedReader) Name() string {
	return "TSSPFileDetachedReader"
}

func (t *TSSPFileDetachedReader) StartSpan(span *tracing.Span) {
	t.span = span
	if t.span != nil {
		t.tm = time.Now()
	}
}

func (t *TSSPFileDetachedReader) EndSpan() {
}

func (t *TSSPFileDetachedReader) SinkPlan(plan hybridqp.QueryNode) {
}

func (t *TSSPFileDetachedReader) GetSchema() record.Schemas {
	return t.ctx.schemas
}

func (t *TSSPFileDetachedReader) UpdateTime(time int64) {
	if t.ctx.readCtx.Ascending {
		t.tr.Max = time
	} else {
		t.tr.Min = time
	}
}

func (t *TSSPFileDetachedReader) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	for {
		if !t.isInit {
			if exist, err := t.initChunkMeta(); !exist {
				if t.span != nil {
					t.span.Count(ReaderContentDuration, int64(time.Since(t.tm)))
				}
				return nil, nil, err
			}
		}
		data, err := t.readBatch()
		if err != nil || data != nil {
			return data, nil, err
		}
		t.isInit = false
	}
}

func (t *TSSPFileDetachedReader) readBatch() (*record.Record, error) {
	for {
		r := t.recordPool.Get()
		colAux := record.ColAux{}
		result, err := t.dataReader.ReadBatch(r, t.ctx.readCtx)
		if err != nil {
			return nil, err
		}
		if result == nil {
			t.recordPool.PutRecordInCircularPool()
			return nil, nil
		}
		if t.span != nil {
			t.span.Count(ReaderContentSizeSpan, int64(result.Size()))
		}
		result.KickNilRow(nil, &colAux)
		if result.RowNums() == 0 {
			t.recordPool.PutRecordInCircularPool()
			continue
		}
		if t.unnest != nil {
			t.unnestOperator.Compute(result)
		}
		result = t.filterData(result)
		if result.RowNums() == 0 {
			t.recordPool.PutRecordInCircularPool()
			t.filterPool.PutRecordInCircularPool()
			continue
		}
		return result, nil
	}
}

func (t *TSSPFileDetachedReader) filterData(rec *record.Record) *record.Record {
	var tm time.Time
	if t.span != nil {
		tm = time.Now()
	}
	if rec != nil && (t.ctx.isOrder || t.isSort) {
		if t.ctx.readCtx.Ascending {
			rec = FilterByTime(rec, t.tr)
		} else {
			rec = FilterByTimeDescend(rec, t.tr)
		}
	}

	// filter by field
	if rec != nil && t.ctx.filterOpts.cond != nil {
		rec = FilterByField(rec, t.filterPool.Get(), t.ctx.filterOpts.options, t.ctx.filterOpts.cond, t.ctx.filterOpts.rowFilters, t.ctx.filterOpts.pointTags, t.ctx.filterBitmap, &t.ctx.filterOpts.colAux)
	}

	rec = t.filterForLog(rec)
	if t.span != nil {
		t.span.Count(ReaderFilterDuration, int64(time.Since(tm)))
	}
	return rec
}

func (t *TSSPFileDetachedReader) initChunkMeta() (bool, error) {
	var err error
	t.isInit = true
	if t.metaDataQueue.IsEmpty() {
		if t.metaIndexID >= len(t.metaIndex) {
			return false, nil
		}
		t.currChunkMetaID = 0
		t.currBlockID = 0
		var endMetaIndexID int
		if t.metaIndexID+ChunkMetaReadNum > len(t.metaIndex) {
			endMetaIndexID = len(t.metaIndex)
		} else {
			endMetaIndexID = t.metaIndexID + ChunkMetaReadNum
		}
		offset := make([]int64, 0, endMetaIndexID-t.metaIndexID)
		sizes := make([]int64, 0, endMetaIndexID-t.metaIndexID)
		start := t.metaIndexID
		for start < endMetaIndexID {
			offset = append(offset, t.metaIndex[start].offset)
			sizes = append(sizes, int64(t.metaIndex[start].size))
			start++
		}
		t.currChunkMeta, err = t.chunkMetaReader.ReadChunkMeta(offset, sizes)
		if err != nil {
			return false, nil
		}
		for _, chunkMeta := range t.currChunkMeta {
			for _, seg := range t.blocks[t.metaIndexID] {
				t.metaDataQueue.Push(NewSegmentMeta(seg, chunkMeta))
			}
			t.metaIndexID++
		}
	}

	chunkMetas := make([]*SegmentMeta, 0)
	for len(chunkMetas) < chunkReadNum && !t.metaDataQueue.IsEmpty() {
		s, _ := t.metaDataQueue.Pop()
		currTr := s.(*SegmentMeta).chunkMeta.GetTimeRangeBy(s.(*SegmentMeta).id)
		if currTr[0] <= t.tr.Max && currTr[1] >= t.tr.Min {
			chunkMetas = append(chunkMetas, s.(*SegmentMeta))
		}
	}
	t.dataReader.InitReadBatch(chunkMetas, t.ctx.schemas)
	if t.span != nil {
		t.span.Count(ReaderContentNumSpan, int64(len(chunkMetas)))
	}
	return true, nil
}

func (t *TSSPFileDetachedReader) ResetBy(metaIndex []*MetaIndex, blocks [][]int, ctx *FileReaderContext) {
	t.metaIndex = metaIndex
	t.blocks = blocks
	t.ctx = ctx
	t.metaIndexID = 0
	t.currBlockID = 0
	t.currChunkMetaID = 0
}

func (t *TSSPFileDetachedReader) Close() error {
	if t.recordPool != nil {
		t.recordPool.Put()
	}
	if t.filterPool != nil {
		t.filterPool.Put()
	}
	if t.dataReader != nil {
		t.dataReader.Close()
	}
	return nil
}

func (t *TSSPFileDetachedReader) SetOps(ops []*comm.CallOption) {

}

func (t *TSSPFileDetachedReader) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

type FileReaderContext struct {
	isOrder      bool
	tr           util.TimeRange
	schemas      record.Schemas
	readCtx      *ReadContext
	filterOpts   *FilterOptions
	filterBitmap *bitmap.FilterBitmap
}

func (f *FileReaderContext) GetSchemas() record.Schemas {
	return f.schemas
}

func NewFileReaderContext(tr util.TimeRange, schemas record.Schemas, decs *ReadContext, filterOpts *FilterOptions, filterBitmap *bitmap.FilterBitmap, isOrder bool) *FileReaderContext {
	return &FileReaderContext{
		isOrder:      isOrder,
		tr:           tr,
		schemas:      schemas,
		readCtx:      decs,
		filterOpts:   filterOpts,
		filterBitmap: filterBitmap,
	}
}
