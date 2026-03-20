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

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"go.uber.org/zap"
)

type WriteAttached struct {
	db  string
	rp  string
	mst *colstore.Measurement
}

func NewWriteAttached(mst *colstore.Measurement) *WriteAttached {
	return &WriteAttached{
		mst: mst,
	}
}

func (w *WriteAttached) SetDBInfo(db string, rp string) {
	w.db = db
	w.rp = rp
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

	sw := mmsTb.NewStreamWriteFile(w.mst.MeasurementInfo().Name)
	err := sw.InitFlushFile(tbStore.NextSequence(), true)
	if err != nil {
		return err
	}

	cw := immutable.NewColumnStoreTSSPWriter(sw, w.mst)
	ret, err := cw.WriteRecord(rec)

	if err != nil {
		return err
	}

	tcLoc := colstore.DefaultTCLocation
	if w.mst.TCDuration() > 0 {
		tcLoc = colstore.TCLocationUsed
	}

	uts := util.Bytes2Uint64Slice(util.Int64Slice2byte(ret.Fragments))
	pkMark := fragment.NewIndexFragmentVariable(uts)
	ret.File.SetPkInfo(colstore.NewPKInfo(ret.PKRecord, pkMark, w.mst.ColStoreInfo().GetPKType(), tcLoc))

	err = ret.File.Rename(immutable.RemoveTmpSuffix(ret.File.Path()))
	if err != nil {
		return err
	}
	tbStore.AddTSSPFiles(w.mst.MeasurementInfo().Name, true, ret.File)
	return nil
}
