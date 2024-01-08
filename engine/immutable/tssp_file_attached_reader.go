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
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

type TSSPFileAttachedReader struct {
	files      []TSSPFile
	fragRanges []fragment.FragmentRanges
	schema     hybridqp.Options

	recordPool *record.CircularRecordPool
	ctx        *FileReaderContext

	reader *LocationCursor
}

func NewTSSPFileAttachedReader(files []TSSPFile, fragRanges []fragment.FragmentRanges, ctx *FileReaderContext, schema hybridqp.Options) (*TSSPFileAttachedReader, error) {
	r := &TSSPFileAttachedReader{files: files, fragRanges: fragRanges, ctx: ctx, schema: schema}
	if err := r.initReader(files, fragRanges); err != nil {
		return nil, err
	}

	// init the filter record pool
	filterPool := record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, r.ctx.schemas, false)
	if r.ctx.filterOpts.options.CondFunctions.HaveFilter() {
		r.reader.AddFilterRecPool(filterPool)
	}

	// init the data record pool
	r.recordPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, r.ctx.schemas, false)
	return r, nil
}

func (t *TSSPFileAttachedReader) initReader(files []TSSPFile, fragRanges []fragment.FragmentRanges) error {
	tr := t.ctx.tr
	if !t.schema.IsTimeSorted() {
		tr = util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime}
	}
	var err error
	var locs []*Location
	buf := pool.GetChunkMetaBuffer()
	defer pool.PutChunkMetaBuffer(buf)

	for i, file := range files {
		loc := NewLocation(file, t.ctx.readCtx)
		chunkMeta, ok := GetChunkMeta(file.Path())
		if ok {
			loc.SetChunkMeta(chunkMeta)
		} else {
			ok, err = loc.Contains(0, tr, buf)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			PutChunkMeta(file.Path(), loc.GetChunkMeta())
		}
		loc.SetFragmentRanges(fragRanges[i])
		locs = append(locs, loc)
	}

	// init the attached file reader
	t.reader = NewLocationCursor(len(locs))
	for i := range locs {
		t.reader.AddLocation(locs[i])
	}
	return nil
}

func (t *TSSPFileAttachedReader) Name() string {
	return "TSSPFileAttachedReader"
}

func (t *TSSPFileAttachedReader) StartSpan(span *tracing.Span) {
}

func (t *TSSPFileAttachedReader) EndSpan() {
}

func (t *TSSPFileAttachedReader) SinkPlan(plan hybridqp.QueryNode) {
}

func (t *TSSPFileAttachedReader) GetSchema() record.Schemas {
	return t.ctx.schemas
}

func (t *TSSPFileAttachedReader) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	var err error
	rec := t.recordPool.Get()
	rec, err = t.reader.ReadData(t.ctx.filterOpts, rec, t.ctx.filterBitmap)
	return rec, nil, err
}

func (t *TSSPFileAttachedReader) ResetBy(files []TSSPFile, fragRanges []fragment.FragmentRanges) error {
	return t.initReader(files, fragRanges)
}

func (t *TSSPFileAttachedReader) Close() error {
	t.reader.Close()
	if t.recordPool != nil {
		t.recordPool.Put()
	}
	return nil
}

func (t *TSSPFileAttachedReader) SetOps(ops []*comm.CallOption) {

}

func (t *TSSPFileAttachedReader) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}
