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
	pkf colstore.PrimaryKeyFetcher

	timeCol record.ColVal
	dataCol record.ColVal
	oks     colstore.OffsetKeySorter
}

func NewColumnStoreTSSPWriter(sw *StreamWriteFile) *ColumnStoreTSSPWriter {
	return &ColumnStoreTSSPWriter{
		sw: sw,
	}
}

func (b *ColumnStoreTSSPWriter) WriteRecord(rec *record.Record, pk, sk record.Schemas, ir *influxql.IndexRelation) (*ColumnStoreWriteResult, error) {
	record.CheckRecord(rec)

	var ret = &ColumnStoreWriteResult{}
	var oks *colstore.OffsetKeySorter

	// 1. group by primary key
	// 2. sort groups by primary key
	// 3. sort within groups by sort key or time
	ret.PKRecord, ret.Fragments, oks = b.sortRecord(rec, pk, sk)

	err := b.writeRecord(rec, oks)
	if err != nil {
		return nil, err
	}

	file, err := b.sw.NewTSSPFile(true)
	if err != nil {
		return nil, err
	}

	ret.File = file

	err = b.writePrimaryIndex(ret)
	if err != nil {
		return nil, err
	}

	err = b.writeSkipIndex(rec, oks, file, ir)
	return ret, err
}

func (b *ColumnStoreTSSPWriter) sortRecord(rec *record.Record, pk, sk record.Schemas) (*record.Record, []int64, *colstore.OffsetKeySorter) {
	pkRec, offsetsMap, pkSlice := b.pkf.Fetch(rec, pk)

	timeCol := &b.timeCol
	oks := &b.oks
	oksSwap := &colstore.OffsetKeySorter{}

	var fragments []int64
	var segCount int32 = 0
	var buf []byte

	skCols := record.FetchColVals(rec, sk)

	// Sort the data within each group by the sort key
	for i := range pkSlice {
		timeCol.Init()
		oksSwap.Reset()

		oksSwap.Offsets = offsetsMap.AppendToIfExists(util.Bytes2str(pkSlice[i]), oksSwap.Offsets[:0])
		pickColVal(rec.TimeColumn(), timeCol, oksSwap.Offsets, influx.Field_Type_Int)
		oksSwap.Times = timeCol.IntegerValues()

		// If the sort key contains only a time column, sort by time
		if sk.Len() != 1 || sk[0].Name != record.TimeField {
			buf = buf[:0]
			for _, n := range oksSwap.Offsets {
				ofs := len(buf)
				buf = colstore.FetchKeyAtRow(buf, skCols, sk, int(n))
				oksSwap.Add(buf[ofs:])
			}
		}

		sort.Sort(oksSwap)
		n := oks.Append(oksSwap, GetColStoreConfig().GetMaxRowsPerSegment()) & 0xFFFF

		// A fragment corresponds to one or more segments
		// The high 32 bits record the segment offset, and the low 32 bits record the number of segments
		fragments = append(fragments, int64(segCount)<<32|int64(n))
		segCount += int32(n)
	}

	return pkRec, fragments, oks
}

func (b *ColumnStoreTSSPWriter) writeRecord(rec *record.Record, oks *colstore.OffsetKeySorter) error {
	timeCol := &b.timeCol
	dataCol := &b.dataCol
	sw := b.sw
	sw.ChangeSid(colstore.SeriesID)
	var err error

	for i := range rec.Schema.Len() - 1 { // skip time column
		column := rec.Column(i)
		ref := rec.Schema[i]
		if err = sw.AppendColumn(&ref); err != nil {
			return err
		}

		oks.IteratorSegment(func(times []int64, offsets []int64) bool {
			timeCol.Init()
			dataCol.Init()

			timeCol.AppendTimes(times)
			pickColVal(column, dataCol, offsets, ref.Type)

			err = sw.WriteData(colstore.SeriesID, ref, *dataCol, timeCol)
			return err == nil
		})

		if err != nil {
			return err
		}
	}

	ref := rec.LastSchema()
	if err = sw.AppendColumn(&ref); err != nil {
		return err
	}
	oks.IteratorSegment(func(times []int64, _ []int64) bool {
		timeCol.Init()
		timeCol.AppendTimes(times)

		err = sw.WriteData(colstore.SeriesID, ref, *timeCol, timeCol)

		return err == nil
	})

	if err != nil {
		return err
	}

	return b.sw.WriteCurrentMeta()
}

func (b *ColumnStoreTSSPWriter) writePrimaryIndex(ret *ColumnStoreWriteResult) error {
	indexFilePath := BuildPKFilePathFromTSSP(ret.File.Path())
	indexBuilder := colstore.NewIndexBuilder(b.sw.lock, indexFilePath+GetTmpFileSuffix())
	ret.IndexFilePath = indexFilePath

	defer indexBuilder.Reset()
	AppendFragmentsToPKRecord(ret.PKRecord, ret.Fragments)
	return indexBuilder.WriteData(ret.PKRecord, -1)
}

func (b *ColumnStoreTSSPWriter) writeSkipIndex(rec *record.Record, oks *colstore.OffsetKeySorter, file TSSPFile, indexRelation *influxql.IndexRelation) error {
	idxRec, segmentsOffset := b.fetchSkipIndexRecord(rec, oks, indexRelation)

	indexWriterBuilder := index.NewIndexWriterBuilder()
	tsspFileName := file.FileName()
	indexWriterBuilder.NewIndexWriters(b.sw.dir, b.sw.name, tsspFileName.String(), *b.sw.lock, idxRec.Schema, *indexRelation)

	var err error
	skipWriters := indexWriterBuilder.GetSkipIndexWriters()
	schemaIdxes := indexWriterBuilder.GetSchemaIdxes()
	for i := range skipWriters {
		err = skipWriters[i].CreateAttachIndex(idxRec, schemaIdxes[i], segmentsOffset)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *ColumnStoreTSSPWriter) fetchSkipIndexRecord(rec *record.Record, oks *colstore.OffsetKeySorter, indexRelation *influxql.IndexRelation) (*record.Record, []int) {
	mp := make(map[string]struct{})
	for _, list := range indexRelation.IndexList {
		for i := range list.IList {
			mp[list.IList[i]] = struct{}{}
		}
	}
	idxRec := &record.Record{}
	var segmentsOffset []int
	idxRec.ReserveSchema(len(mp))
	idxRec.ReserveColVal(len(mp))

	j := 0
	totalRows := 0
	for i := range rec.Len() {
		_, ok := mp[rec.Schema[i].Name]
		if !ok {
			continue
		}

		typ := rec.Schema[i].Type
		src := &rec.ColVals[i]
		dst := &idxRec.ColVals[j]
		idxRec.Schema[j] = rec.Schema[i]

		oks.IteratorSegment(func(times []int64, offsets []int64) bool {
			pickColVal(src, dst, offsets, typ)
			if j == 0 {
				totalRows += len(offsets)
				segmentsOffset = append(segmentsOffset, totalRows)
			}
			return true
		})

		j++
	}
	return idxRec, segmentsOffset
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
