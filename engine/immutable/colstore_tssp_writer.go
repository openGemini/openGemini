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

package immutable

import (
	"sort"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type ColumnStoreWriteResult struct {
	File          TSSPFile
	Fragments     []int64
	PKRecord      *record.Record
	IndexFilePath string
}

type ColumnStoreTSSPWriter struct {
	sw  *StreamWriteFile
	mst *colstore.Measurement
	pkf colstore.PrimaryKeyFetcher

	timeCol record.ColVal
	oks     colstore.OffsetKeySorter

	iw *ColStoreIndexWriter
}

func NewColumnStoreTSSPWriter(sw *StreamWriteFile, mst *colstore.Measurement) *ColumnStoreTSSPWriter {
	return &ColumnStoreTSSPWriter{
		sw:  sw,
		mst: mst,
	}
}

func (b *ColumnStoreTSSPWriter) WriteRecord(rec *record.Record) (*ColumnStoreWriteResult, error) {
	record.CheckRecord(rec)

	ir := b.mst.IndexRelation()
	pkMap := b.mst.BuildPKMap()

	b.iw = NewColStoreIndexWriter(b.sw.dir, b.sw.name, b.sw.fileName, b.sw.lock, ir, rec.Schema, pkMap)
	defer b.iw.MustClose()

	var ret = &ColumnStoreWriteResult{}
	var oks *colstore.OffsetKeySorter

	if b.mst.IsClusterPKIndex() {
		// 1. group by primary key
		// 2. sort groups by primary key
		// 3. sort within groups by sort key or time
		ret.PKRecord, ret.Fragments, oks = b.sortRecordCluster(rec)
	} else {
		ret.PKRecord, ret.Fragments, oks = b.sortRecordSparse(rec)
	}

	err := b.writeRecord(rec, oks)
	if err != nil {
		return nil, err
	}

	err = b.iw.Flush()
	if err != nil {
		return nil, err
	}

	file, err := b.sw.NewTSSPFile(true)
	if err != nil {
		return nil, err
	}

	file.SetSkipIndexInfo(b.iw.BuildSkipIndexInfo())
	ret.File = file

	err = b.writePrimaryIndex(ret)
	if err != nil {
		return nil, err
	}

	return ret, err
}

func (b *ColumnStoreTSSPWriter) sortRecordCluster(rec *record.Record) (*record.Record, []int64, *colstore.OffsetKeySorter) {
	pk := b.mst.PrimaryKey()
	sk := b.mst.SortKey()

	pkRec, offsetsMap, pkSlice := b.pkf.Fetch(rec, pk, b.mst.TCDuration())

	timeCol := &b.timeCol
	oks := &b.oks
	oksSwap := &colstore.OffsetKeySorter{}

	var fragments []int64
	var segCount int64 = 0
	var rowCount int64 = 0
	var buf []byte

	skCols := record.FetchColVals(rec, sk)

	// Sort the data within each group by the sort key
	for i := range pkSlice {
		timeCol.Init()
		oksSwap.Reset()

		oksSwap.Offsets = offsetsMap.AppendToIfExists(util.Bytes2str(pkSlice[i]), oksSwap.Offsets[:0])
		pickColVal(rec.TimeColumn(), timeCol, oksSwap.Offsets, influx.Field_Type_Int)
		oksSwap.Times = append(oksSwap.Times[:0], timeCol.IntegerValues()...)

		// If the sort key contains only a time column, sort by time
		if len(sk) != 1 || sk[0].Name != record.TimeField {
			for _, n := range oksSwap.Offsets {
				buf = colstore.FetchKeyAtRow(buf, skCols, sk, int(n), 0)
				oksSwap.Add(buf)
				buf = buf[len(buf):]
			}
		}

		sort.Stable(oksSwap)
		oks.Append(oksSwap)
		segCount += int64(util.DivisionCeil(oksSwap.Len(), GetColStoreConfig().GetMaxRowsPerSegment()))
		rowCount += int64(oksSwap.Len())

		// A fragment contains one or more segments, record the offset of the last segment.
		fragments = append(fragments, util.MergeToInt64(uint32(segCount), uint32(rowCount)))
	}

	return pkRec, fragments, oks
}

func (b *ColumnStoreTSSPWriter) sortRecordSparse(rec *record.Record) (*record.Record, []int64, *colstore.OffsetKeySorter) {
	oks := &b.oks
	var buf []byte

	sk := b.mst.SortKey()
	skCols := record.FetchColVals(rec, sk)

	oks.Times = append(oks.Times[:0], rec.Times()...)

	for i := range rec.RowNums() {
		buf = colstore.FetchKeyAtRow(buf, skCols, sk, i, 0)
		oks.Add(buf)
		oks.Offsets = append(oks.Offsets, int64(i))
		buf = buf[len(buf):]
	}

	sort.Stable(oks)

	maxRowsPreSeg := GetColStoreConfig().GetMaxRowsPerSegment()
	pkRec := &record.Record{}
	pkRec.ResetWithSchema(b.mst.PrimaryKey())
	dec := &codec.BinaryDecoder{}

	var fragments []int64
	var segCount int64 = 0

	total := oks.Len()
	for i := 0; i < oks.Len(); i += maxRowsPreSeg {
		oks.AddFragmentOffset(min(total, i+maxRowsPreSeg))
		colstore.AppendKeyToRecord(dec, pkRec, oks.Keys[i])
		segCount++
		fragments = append(fragments, segCount)
	}
	colstore.AppendKeyToRecord(dec, pkRec, oks.Keys[oks.Len()-1])
	fragments = append(fragments, segCount)

	return pkRec, fragments, oks
}

func (b *ColumnStoreTSSPWriter) writeRecord(rec *record.Record, oks *colstore.OffsetKeySorter) error {
	sw := b.sw
	var err error

	if len(b.mst.SortKey()) > 0 {
		sw.MarkTimeDisorder()
	}

	pkMap := b.mst.BuildPKMap()
	picker := colstore.NewColValPicker(oks, b.mst.IsUniqueEnabled())
	maxRowsPreSeg := GetColStoreConfig().GetMaxRowsPerSegment()

	sw.Init(colstore.SeriesID, rec.Schema)

	picker.IteratorSegment(maxRowsPreSeg, func(start, end int) bool {
		for i := range rec.Schema.Len() {
			ref := rec.Schema[i]
			if b.mst.IsClusterPKIndex() {
				// In cluster mode, the primary key column does not need to be written to the file
				if _, ok := pkMap[ref.Name]; ok {
					continue
				}
			}

			column := rec.Column(i)
			if err = sw.ChangeColumn(ref); err != nil {
				return false
			}

			dataCol, timeCol := picker.Pick(column, ref.Type, start, end)
			err = sw.WriteData(colstore.SeriesID, ref, *dataCol, timeCol)
			if err != nil {
				return false
			}

			err = b.iw.Write(ref, dataCol)
			if err != nil {
				return false
			}
		}
		err = b.iw.FlushSegment()
		return err == nil
	})

	if err != nil {
		return err
	}

	return b.sw.WriteCurrentMeta()
}

func (b *ColumnStoreTSSPWriter) writePrimaryIndex(ret *ColumnStoreWriteResult) error {
	if ret.PKRecord == nil {
		// no primary key
		return nil
	}

	indexFilePath := BuildPKFilePathFromTSSP(ret.File.Path())
	indexBuilder := colstore.NewIndexBuilder(b.sw.lock, indexFilePath+GetTmpFileSuffix())
	ret.IndexFilePath = indexFilePath

	defer indexBuilder.Reset()
	AppendFragmentsToPKRecord(ret.PKRecord, ret.Fragments)
	return indexBuilder.WriteData(ret.PKRecord, b.mst.TCLocation())
}

type ColStoreIndexWriter struct {
	ir           *influxql.IndexRelation
	indexBuilder *index.IndexWriterBuilder
	indexWriters map[string][]index.IndexWriter
}

func NewColStoreIndexWriter(dir, mst string, fileName TSSPFileName, lock *string,
	ir *influxql.IndexRelation, schema record.Schemas, pkMap map[string]struct{}) *ColStoreIndexWriter {

	iw := &ColStoreIndexWriter{
		ir: ir,
	}
	iw.indexBuilder = index.NewIndexWriterBuilder()
	iw.indexWriters = iw.indexBuilder.NewIndexWriters(dir, mst, fileName.String(), *lock, schema, pkMap, *ir)
	iw.Open()
	return iw
}

func (iw *ColStoreIndexWriter) MustClose() {
	for _, writer := range iw.indexBuilder.GetSkipIndexWriters() {
		util.MustClose(writer)
	}
}

func (iw *ColStoreIndexWriter) Flush() error {
	for _, writer := range iw.indexBuilder.GetSkipIndexWriters() {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (iw *ColStoreIndexWriter) Open() {
	for _, writer := range iw.indexBuilder.GetSkipIndexWriters() {
		writer.Open()
	}
}

func (iw *ColStoreIndexWriter) FlushSegment() error {
	for _, writers := range iw.indexWriters {
		for _, writer := range writers {
			err := writer.FlushSegment()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (iw *ColStoreIndexWriter) Write(ref record.Field, col *record.ColVal) error {
	if len(iw.indexWriters) == 0 {
		return nil
	}

	skipWriters, ok := iw.indexWriters[ref.Name]
	if !ok {
		return nil
	}

	var err error

	rec := &record.Record{}
	rec.Schema = append(rec.Schema, ref)
	rec.ColVals = append(rec.ColVals, *col)
	for i := range skipWriters {
		err = skipWriters[i].CreateAttachIndex(rec, []int{0}, []int{col.Len})
		if err != nil {
			return err
		}
	}
	return nil
}

func (iw *ColStoreIndexWriter) BuildSkipIndexInfo() []*colstore.SkipIndexInfo {
	return iw.indexBuilder.BuildSkipIndexInfo()
}

func pickColVal(src *record.ColVal, dst *record.ColVal, ofs []int64, typ int) {
	for _, i := range ofs {
		dst.AppendColVal(src, typ, int(i), int(i+1))
	}
}

func AppendFragmentsToPKRecord(dst *record.Record, fragments []int64) {
	dst.ReserveSchema(1)
	dst.ReserveColVal(1)

	dst.Schema[dst.Len()-1].Name = record.FragmentField
	dst.Schema[dst.Len()-1].Type = influx.Field_Type_Int
	dst.ColVals[dst.Len()-1].AppendIntegers(fragments...)
}
