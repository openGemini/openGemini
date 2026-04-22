/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	MetaIndexAndBlockIdDuration = "meta_index_duration"
	PrimaryKeyDuration          = "primary_key_duration"
)

// detachedLazyLoadIndexReader is used to reduce the number of BF in the "select  order by time limit" scenario
type detachedLazyLoadIndexReader struct {
	dataPath   string
	ctx        *indexContext
	obsOptions *obs.ObsOptions
	readerCtx  *immutable.FileReaderContext
	span       *tracing.Span
}

func NewDetachedLazyLoadIndexReader(ctx *indexContext, obsOption *obs.ObsOptions, readerCtx *immutable.FileReaderContext) *detachedLazyLoadIndexReader {
	return &detachedLazyLoadIndexReader{
		obsOptions: obsOption,
		ctx:        ctx,
		readerCtx:  readerCtx,
	}
}

func (t *detachedLazyLoadIndexReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	t.span = span
}

func (r *detachedLazyLoadIndexReader) CreateCursors() ([]comm.KeyCursor, int, error) {
	cursors := make([]comm.KeyCursor, 0)
	mst := r.ctx.schema.Options().GetMeasurements()[0]
	r.dataPath = obs.GetBaseMstPath(r.ctx.shardPath, mst.Name)
	c := NewStreamDetachedReader(r.readerCtx, sparseindex.NewOBSFilterPath("", r.dataPath, r.obsOptions), r.ctx)
	sortLimitCursor := immutable.NewSortLimitCursor(r.ctx.schema.Options(), r.readerCtx.GetSchemas(), c, obs.GetShardID(r.dataPath))
	sortLimitCursor.StartSpan(r.span)
	cursors = append(cursors, sortLimitCursor)
	return cursors, 0, nil
}

// StreamDetachedReader implement comm.KeyCursor and comm.TimeCutKeyCursor, it can stream read detached data to reduce IO of BF.
type StreamDetachedReader struct {
	isInitDataReader bool
	isClose          bool
	isInit           bool
	idx              int
	blockId          uint64
	localPath        string
	dataPath         string
	span             *tracing.Span
	dataReader       comm.KeyCursor
	tr               util.TimeRange
	skFileReader     []sparseindex.SKFileReader
	info             *executor.DetachedIndexInfo
	options          hybridqp.Options
	path             *sparseindex.OBSFilterPath
	readerCtx        *immutable.FileReaderContext
	ctx              *indexContext
	tempFrs          []*fragment.FragmentRange
}

func NewStreamDetachedReader(readerCtx *immutable.FileReaderContext, path *sparseindex.OBSFilterPath, ctx *indexContext) *StreamDetachedReader {
	r := &StreamDetachedReader{
		options:   ctx.schema.Options(),
		ctx:       ctx,
		path:      path,
		readerCtx: readerCtx,
		tr:        util.TimeRange{Min: ctx.tr.Min, Max: ctx.tr.Max},
		tempFrs:   make([]*fragment.FragmentRange, 1),
	}
	r.tempFrs[0] = fragment.NewFragmentRange(uint32(0), uint32(0))

	return r
}

func (r *StreamDetachedReader) Init() (err error) {
	mst := r.options.GetMeasurements()[0]
	r.dataPath = obs.GetBaseMstPath(r.ctx.shardPath, mst.Name)
	if immutable.GetDetachedFlushEnabled() {
		r.localPath = obs.GetLocalMstPath(obs.GetPrefixDataPath(), r.dataPath)
	}
	chunkCount, err := immutable.GetMetaIndexChunkCount(r.path.Option(), r.dataPath)
	if err != nil {
		return
	}
	if chunkCount == 0 {
		return
	}
	var tm time.Time
	if r.span != nil {
		tm = time.Now()
	}
	miChunkIds, miFiltered, err := immutable.GetMetaIndexAndBlockId(r.dataPath, r.path.Option(), chunkCount, r.ctx.tr)
	if r.span != nil {
		r.span.Count(MetaIndexAndBlockIdDuration, int64(time.Since(tm)))
	}
	if err != nil {
		return
	}

	if len(miFiltered) == 0 {
		return nil
	}
	if r.span != nil {
		tm = time.Now()
	}
	pkMetaInfo, pkItems, err := immutable.GetPKItems(r.dataPath, r.path.Option(), miChunkIds)
	if err != nil {
		return err
	}
	if r.span != nil {
		r.span.Count(PrimaryKeyDuration, int64(time.Since(tm)))
	}
	r.info = executor.NewDetachedIndexInfo(miFiltered, pkItems)

	mstInfo := r.ctx.schema.Options().GetMeasurements()[0]
	r.skFileReader, err = r.ctx.skIndexReader.CreateSKFileReaders(r.ctx.schema.Options(), mstInfo, true)
	if err != nil {
		return err
	}
	if r.ctx.keyCondition != nil {
		return
	}

	for j := range r.skFileReader {
		if err = r.skFileReader[j].ReInit(sparseindex.NewOBSFilterPath(r.localPath, r.dataPath, r.path.Option())); err != nil {
			return err
		}
	}
	return initKeyCondition(r.info.Infos()[0].Data.Schema, r.ctx, pkMetaInfo.TCLocation)
}

func (t *StreamDetachedReader) Name() string {
	return "StreamDetachedReader"
}

func (t *StreamDetachedReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	t.span = span
	if t.skFileReader != nil {
		for _, sk := range t.skFileReader {
			sk.StartSpan(span)
		}
	}
	t.span.CreateCounter(immutable.ReaderContentNumSpan, "")
	t.span.CreateCounter(immutable.ReaderContentSizeSpan, "")
	t.span.CreateCounter(immutable.ReaderContentDuration, "ns")
	t.span.CreateCounter(immutable.ReaderFilterDuration, "ns")
	t.span.CreateCounter(MetaIndexAndBlockIdDuration, "ns")
	t.span.CreateCounter(PrimaryKeyDuration, "ns")
}

func (t *StreamDetachedReader) EndSpan() {
}

func (t *StreamDetachedReader) SinkPlan(plan hybridqp.QueryNode) {
}

func (t *StreamDetachedReader) GetSchema() record.Schemas {
	return t.readerCtx.GetSchemas()
}

func (t *StreamDetachedReader) UpdateTime(time int64) {
	if t.options.IsAscending() {
		t.tr.Max = time
	} else {
		t.tr.Min = time
	}
}

func (t *StreamDetachedReader) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if !t.isInit {
		err := t.Init()
		if err != nil {
			return nil, nil, err
		}
		t.isInit = true
	}
	for {
		if t.isClose {
			return nil, nil, nil
		}
		if t.info == nil {
			return nil, nil, nil
		}
		if t.idx >= len(t.info.Files()) {
			return nil, nil, nil
		}
		if !t.isInitDataReader {
			isExist, err := t.initDataReader()
			if err != nil {
				return nil, nil, err
			}
			if !isExist {
				continue
			}
		}
		re, se, err := t.dataReader.Next()
		if err != nil {
			return nil, nil, err
		}
		if re == nil {
			t.isInitDataReader = false
			continue
		}
		return re, se, err
	}
}

func (t *StreamDetachedReader) initDataReader() (bool, error) {
	currIdx := t.idx
	if !t.options.IsAscending() {
		currIdx = len(t.info.Files()) - 1 - t.idx
	}
	currInfo := t.info.Infos()[currIdx]
	if t.blockId >= currInfo.EndBlockId-currInfo.StartBlockId {
		t.resetIndex()
		return false, nil
	}
	if !t.info.Files()[currIdx].IsExist(t.tr) {
		t.resetIndex()
		return false, nil
	}
	currBlocks := make([]int, 0, immutable.BatchReaderRecordNum)
	for {
		if t.isClose {
			return false, nil
		}
		if t.blockId >= currInfo.EndBlockId-currInfo.StartBlockId {
			break
		}
		if len(currBlocks) >= immutable.BatchReaderRecordNum {
			break
		}
		currBlockId := t.blockId
		if !t.options.IsAscending() {
			currBlockId = currInfo.EndBlockId - currInfo.StartBlockId - 1 - t.blockId
		}
		t.blockId += 1
		if !t.tr.Overlaps(currInfo.Data.Time(int(currBlockId)), currInfo.Data.Time(int(currBlockId)+1)) {
			continue
		}
		t.tempFrs[0] = fragment.NewFragmentRange(uint32(currBlockId), uint32(currBlockId+1))
		isExist, err := t.filterBySk(currInfo)
		if err != nil {
			return false, err
		}
		if isExist {
			currBlocks = append(currBlocks, int(t.tempFrs[0].Start))
		}
	}
	if len(currBlocks) == 0 {
		return false, nil
	}

	var unnest *influxql.Unnest
	if t.ctx.schema.HasUnnests() {
		unnest = t.ctx.schema.GetUnnests()[0]
	}

	blocks := make([][]int, 1)
	blocks[0] = currBlocks
	var err error
	t.dataReader, err = immutable.NewTSSPFileDetachedReader(t.info.Files()[currIdx:currIdx+1], blocks, t.readerCtx,
		sparseindex.NewOBSFilterPath("", t.dataPath, t.path.Option()), unnest, true, t.ctx.schema.Options())
	if err != nil {
		return false, err
	}
	t.dataReader.StartSpan(t.span)
	t.isInitDataReader = true
	return true, nil
}

// filter PKInfo By SkIndexRead
func (t *StreamDetachedReader) filterBySk(currInfo *colstore.DetachedPKInfo) (bool, error) {
	isExist := true
	for j := range t.skFileReader {
		t.tempFrs[0].Start += uint32(currInfo.StartBlockId)
		t.tempFrs[0].End += uint32(currInfo.StartBlockId)
		frs, err := t.ctx.skIndexReader.Scan(t.skFileReader[j], t.tempFrs)
		if err != nil {
			return false, err
		}
		if frs.Empty() {
			isExist = false
			break
		}
		t.tempFrs[0].Start -= uint32(currInfo.StartBlockId)
		t.tempFrs[0].End -= uint32(currInfo.StartBlockId)
	}
	return isExist, nil
}

func (t *StreamDetachedReader) resetIndex() {
	t.blockId = 0
	t.isInitDataReader = false
	t.idx += 1
}

func (t *StreamDetachedReader) Close() error {
	t.isClose = true
	if !immutable.IsInterfaceNil(t.dataReader) {
		return t.dataReader.Close()
	}
	return nil
}

func (t *StreamDetachedReader) SetOps(ops []*comm.CallOption) {

}

func (t *StreamDetachedReader) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}
