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

package mutable

import (
	"fmt"
	"math"
	"reflect"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"go.uber.org/zap"
)

type WriteAttached struct {
	db            string
	rp            string
	mst           string
	primaryKey    record.Schemas
	sortKey       record.Schemas
	indexRelation *influxql.IndexRelation
}

func NewWriteAttached(mst string, primaryKey, sortKey record.Schemas, indexRelation *influxql.IndexRelation) *WriteAttached {
	return &WriteAttached{
		mst:           mst,
		primaryKey:    primaryKey,
		sortKey:       sortKey,
		indexRelation: indexRelation,
	}
}

func (w *WriteAttached) SetDBInfo(db string, rp string) {
	w.db = db
	w.rp = rp
}

func (w *WriteAttached) updateAccumulateMetaIndex(accumulateMetaIndex *immutable.AccumulateMetaIndex) {

}

func (w *WriteAttached) getAccumulateMetaIndex() *immutable.AccumulateMetaIndex {
	return nil
}

func (w *WriteAttached) flushChunk(primaryKey record.Schemas, msName string, indexRelation *influxql.IndexRelation, tbStore immutable.TablesStore,
	chunk *WriteChunkForColumnStore, writeMs *immutable.MsBuilder, tcLocation int8) {

	writeRec := chunk.WriteRec.GetRecord()

	writeMs = w.WriteRecordForFlush(writeRec, writeMs, tbStore, 0, true, math.MinInt64, primaryKey, indexRelation)
	atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(writeRec.RowNums()))

	if writeMs != nil {
		if err := immutable.WriteIntoFile(writeMs, true, writeMs.GetPKInfoNum() != 0, indexRelation); err != nil {
			writeMs = nil
			logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
			return
		}

		immutable.NewCSParquetManager().Convert(writeMs.Files, w.db, w.rp, msName)

		tbStore.AddTSSPFiles(writeMs.Name(), false, writeMs.Files...)
		if writeMs.GetPKInfoNum() != 0 {
			for i, file := range writeMs.Files {
				file.SetPkInfo(colstore.NewPKInfo(writeMs.GetPKRecord(i), writeMs.GetPKMark(i), tcLocation))
			}
		}
	}

	err := writeMs.CloseIndexWriters()
	if err != nil {
		logger.GetLogger().Error("close indexWriters failed", zap.String("mstName", msName), zap.Error(err))
		return
	}
}

func (w *WriteAttached) WriteRecordForFlush(rec *record.Record, msb *immutable.MsBuilder, tbStore immutable.TablesStore, id uint64, order bool,
	lastFlushTime int64, schema record.Schemas, skipIndexRelation *influxql.IndexRelation) *immutable.MsBuilder {
	var err error

	if !order && lastFlushTime == math.MaxInt64 {
		msb.StoreTimes()
	}

	msb, err = msb.WriteRecordByCol(id, rec, schema, skipIndexRelation, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
		return tbStore.NextSequence(), 0, 0, 0
	})
	if err != nil {
		panic(err)
	}

	return msb
}

func (w *WriteAttached) FlushRecord(tbStore immutable.TablesStore, rec *record.Record) {
	err := w.flushRecord(tbStore, rec)
	if err != nil {
		logger.GetLogger().Error("failed to flush memory data to disk", zap.Error(err))
		panic(err)
	}
}

func (w *WriteAttached) flushRecord(tbStore immutable.TablesStore, rec *record.Record) error {
	mmsTb, ok := tbStore.(*immutable.MmsTables)
	if !ok {
		return fmt.Errorf("unsupported store type. expect: *immutable.MmsTables, got: %v", reflect.TypeOf(tbStore))
	}

	sw := mmsTb.NewStreamWriteFile(w.mst)
	err := sw.InitFlushFile(tbStore.NextSequence(), true)
	if err != nil {
		return err
	}

	cw := immutable.NewColumnStoreTSSPWriter(sw)
	ret, err := cw.WriteRecord(rec, w.primaryKey, w.sortKey, w.indexRelation)

	if err != nil {
		return err
	}

	err = immutable.RenameTmpFilesWithPKIndex([]immutable.TSSPFile{ret.File}, w.indexRelation)
	if err != nil {
		return err
	}

	uts := util.Bytes2Uint64Slice(util.Int64Slice2byte(ret.Fragments))
	pkMark := fragment.NewIndexFragmentVariable(uts)
	ret.File.SetPkInfo(colstore.NewPKInfo(ret.PKRecord, pkMark, -1))
	tbStore.AddTSSPFiles(w.mst, true, ret.File)
	return nil
}
