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
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/logstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	chunkMetaReadNum     = 16
	BatchReaderRecordNum = 7
	chunkReadNum         = 16
)

type TSSPFileDetachedReader struct {
	isSort          bool
	isInit          bool
	metaIndexID     int
	currBlockID     int // currMetaRead
	currChunkMetaID int

	metaDataQueue   logstore.MetaControl
	metaIndex       []*MetaIndex
	blocks          [][]int
	obsOpts         *obs.ObsOptions
	chunkMetaReader *DetachedChunkMetaReader
	dataReader      *DetachedMetaDataReader
	recordPool      *record.CircularRecordPool
	filterPool      *record.CircularRecordPool
	currChunkMeta   []*ChunkMeta
	ctx             *FileReaderContext
}

func NewTSSPFileDetachedReader(metaIndex []*MetaIndex, blocks [][]int, ctx *FileReaderContext, path *sparseindex.OBSFilterPath, isSort bool) (*TSSPFileDetachedReader, error) {
	r := &TSSPFileDetachedReader{
		isSort:        isSort,
		metaIndex:     metaIndex,
		blocks:        blocks,
		obsOpts:       path.Option(),
		ctx:           ctx,
		metaDataQueue: logstore.NewMetaControl(true, chunkMetaReadNum),
	}
	r.recordPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, ctx.schemas, false)
	r.filterPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, ctx.schemas, false)
	var err error
	r.chunkMetaReader, err = NewDetachedChunkMetaReader(path.RemotePath(), path.Option())
	if err != nil {
		return nil, err
	}
	r.dataReader, err = NewDetachedMetaDataReader(path.RemotePath(), path.Option(), isSort)
	return r, err
}

func (t *TSSPFileDetachedReader) Name() string {
	return "TSSPFileDetachedReader"
}

func (t *TSSPFileDetachedReader) StartSpan(span *tracing.Span) {
}

func (t *TSSPFileDetachedReader) EndSpan() {
}

func (t *TSSPFileDetachedReader) SinkPlan(plan hybridqp.QueryNode) {
}

func (t *TSSPFileDetachedReader) GetSchema() record.Schemas {
	return t.ctx.schemas
}

func (t *TSSPFileDetachedReader) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	for {
		if !t.isInit {
			if exist, err := t.initChunkMeta(); !exist {
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
	r := t.recordPool.Get()
	for {
		result, err := t.dataReader.ReadBatch(r, t.ctx.readCtx)
		if err != nil {
			return nil, err
		}
		if result == nil {
			t.recordPool.PutRecordInCircularPool()
			return nil, nil
		}
		result.KickNilRow(nil)
		if result.RowNums() == 0 {
			t.recordPool.PutRecordInCircularPool()
			continue
		}
		return t.filterData(result), nil
	}
}

func (t *TSSPFileDetachedReader) filterData(rec *record.Record) *record.Record {
	if rec != nil && (t.ctx.isOrder || t.isSort) {
		rec = FilterByTime(rec, t.ctx.tr)
	}

	// filter by field
	if rec != nil && t.ctx.filterOpts.cond != nil {
		rec = FilterByField(rec, t.filterPool.Get(), t.ctx.filterOpts.options, t.ctx.filterOpts.cond, t.ctx.filterOpts.rowFilters, t.ctx.filterOpts.pointTags, t.ctx.filterBitmap, &t.ctx.filterOpts.colAux)
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
		if t.metaIndexID+chunkMetaReadNum > len(t.metaIndex) {
			endMetaIndexID = len(t.metaIndex)
		} else {
			endMetaIndexID = t.metaIndexID + chunkMetaReadNum
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
		chunkMetas = append(chunkMetas, s.(*SegmentMeta))
	}
	t.dataReader.InitReadBatch(chunkMetas, t.ctx.schemas)
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
