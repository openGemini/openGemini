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
	"container/heap"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"sync"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

var fragmentIteratorsPool = NewFragmentIteratorsPool(cpu.GetCpuNum() / 2)

const timeType = "time"

type FragmentIteratorsPool struct {
	cache chan *FragmentIterators
	pool  sync.Pool
}

func NewFragmentIteratorsPool(n int) *FragmentIteratorsPool {
	if n < 2 {
		n = 2
	}
	if n > cpu.GetCpuNum() {
		n = cpu.GetCpuNum()
	}
	return &FragmentIteratorsPool{
		cache: make(chan *FragmentIterators, n),
	}
}

type FragmentIterators struct {
	closed          chan struct{}
	dropping        *int64
	chunkSegments   int
	estimateSize    int
	recordResultNum int
	pkRecPosition   int
	dir             string
	name            string // measurement name with version
	indexFilePath   string
	lock            *string
	inited          bool
	oldIndexFiles   []string
	SortKeyFileds   []record.Field
	fields          record.Schemas
	itrs            []*SortKeyIterator
	curItrs         *SortKeyIterator

	TableData
	mIndex       MetaIndex
	Conf         *Config
	ctx          *ReadContext
	colBuilder   *ColumnBuilder
	schemaMap    dictpool.Dict
	breakPoint   *breakPoint
	files        []TSSPFile
	builder      *MsBuilder
	PkRec        []*record.Record
	RecordResult *record.Record
	log          *Log.Logger
}

type breakPoint struct {
	sortItem []record.SortItem
	position int
}

func (f *FragmentIterators) WithLog(log *Log.Logger) {
	f.log = log
	for i := range f.itrs {
		f.itrs[i].WithLog(log)
	}
}

func (f *FragmentIterators) Len() int      { return len(f.itrs) }
func (f *FragmentIterators) Swap(i, j int) { f.itrs[i], f.itrs[j] = f.itrs[j], f.itrs[i] }
func (f *FragmentIterators) Less(i, j int) bool {
	im := f.itrs[i].sortData
	jm := f.itrs[j].sortData

	for idx := 0; idx < len(im); idx++ {
		v, err := im[idx].CompareSingleValue(jm[idx], f.itrs[i].position, f.itrs[j].position)
		if err != nil {
			panic("CompareSingleValue error")
		}
		if v == 0 {
			continue
		}
		return v > 0
	}

	return false
}

func (f *FragmentIterators) Push(v interface{}) {
	f.itrs = append(f.itrs, v.(*SortKeyIterator))
}

func (f *FragmentIterators) Pop() interface{} {
	l := len(f.itrs)
	v := f.itrs[l-1]
	f.itrs = f.itrs[:l-1]
	return v
}

func (f *FragmentIterators) Close() {
	for _, itr := range f.itrs {
		itr.Close()
	}
	f.itrs = f.itrs[:0]
	f.chunkSegments = 0
	f.ctx.preAggBuilders.reset()
	f.files = f.files[:0]
	f.schemaMap.Reset()
	f.colBuilder.resetPreAgg()
	putFragmentIterators(f)
}

func getFragmentIterators() *FragmentIterators {
	select {
	case itr := <-fragmentIteratorsPool.cache:
		return itr
	default:
		v := fragmentIteratorsPool.pool.Get()
		if v == nil {
			return &FragmentIterators{
				ctx:           NewReadContext(true),
				colBuilder:    NewColumnBuilder(),
				itrs:          make([]*SortKeyIterator, 0, 8),
				SortKeyFileds: make([]record.Field, 0, 16),
				RecordResult:  &record.Record{},
			}
		}
		return v.(*FragmentIterators)
	}
}

func putFragmentIterators(itr *FragmentIterators) {
	itr.reset()
	select {
	case fragmentIteratorsPool.cache <- itr:
	default:
		fragmentIteratorsPool.pool.Put(itr)
	}
}

func (f *FragmentIterators) compact(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error) {
	var err error
	var ok bool
	for {
		if len(f.itrs) == 0 {
			break
		}
		f.curItrs, ok = heap.Pop(f).(*SortKeyIterator)
		if !ok {
			return nil, errors.New("SortKeyIterator type switch error")
		}
		if len(f.itrs) == 0 { //last itrs
			f.RecordResult, err = f.curItrs.NextSingleFragment(tbStore, f, pkSchema)
			if err != nil {
				return nil, err
			}
		} else {
			f.GetBreakPoint()
			start := f.curItrs.position
			f.NextWithBreakPoint()
			if f.curItrs.position < f.curItrs.curRec.RowNums() && start == f.curItrs.position { // no data smaller than break point
				f.curItrs.position++
				f.recordResultNum++
			}

			f.RecordResult.AppendRec(f.curItrs.curRec, start, f.curItrs.position)
			if f.RecordResult.RowNums() == f.builder.Conf.maxRowsPerSegment*f.builder.Conf.FragmentsNumPerFlush {
				err = f.Flush(tbStore, pkSchema, false)
				if err != nil {
					return nil, err
				}
				f.RecordResult = resetResultRec(f.RecordResult)
				f.recordResultNum = 0
			}

			// curItrs is empty
			if f.curItrs.position >= f.curItrs.curRec.RowNums() {
				if f.curItrs.segmentPostion >= len(f.curItrs.curtChunkMeta.TimeMeta().entries) {
					continue
				}
				f.curItrs.position = 0
				err = f.curItrs.GetNewRecord()
				if err != nil {
					return nil, err
				}
				if f.curItrs.curRec.RowNums() != 0 {
					heap.Push(f, f.curItrs)
				}
			} else {
				heap.Push(f, f.curItrs)
			}
		}
	}

	return f.flushIntoFiles(pkSchema, tbStore)
}

func (f *FragmentIterators) flushIntoFiles(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error) {
	if err := f.Flush(tbStore, pkSchema, true); err != nil {
		return nil, err
	}
	f.RecordResult = resetResultRec(f.RecordResult)
	f.recordResultNum = 0

	newFiles := make([]TSSPFile, 0, len(f.builder.Files))
	newFiles = append(newFiles, f.builder.Files...)
	f.builder = nil
	return newFiles, nil
}

func resetResultRec(rec *record.Record) *record.Record {
	for i := range rec.ColVals {
		rec.ColVals[i].Init()
	}
	return rec
}

func (f *FragmentIterators) Flush(tbStore *MmsTables, pkSchema record.Schemas, final bool) error {
	var err error
	f.builder, err = f.writeRecord(func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
		ext = fn.extent
		ext++
		return fn.seq, fn.level, 0, ext
	}, final, pkSchema)
	if err != nil {
		return err
	}

	if f.builder != nil && final {
		if f.RecordResult.TimeColumn().Len != 0 {
			if err = f.writePkRec(0, pkSchema, true); err != nil {
				return err
			}
		}
		// write index if needed
		if f.PkRec[f.pkRecPosition].TimeColumn().Len > 0 {
			if err = f.writeIndex(); err != nil {
				return err
			}
		}

		if err = WriteIntoFile(f.builder, true, f.builder.GetPKInfoNum() != 0); err != nil {
			f.builder.log.Error("rename init file failed", zap.String("mstName", f.name), zap.Error(err))
			f.builder = nil
			return err
		}

		//update data files
		tbStore.AddTSSPFiles(f.builder.msName, true, f.builder.Files...)
		if f.builder.GetPKInfoNum() != 0 {
			for i, file := range f.builder.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendIndexSuffix(RemoveTsspSuffix(dataFilePath))
				if err = tbStore.ReplacePKFile(f.builder.msName, indexFilePath, f.builder.GetPKRecord(i), f.builder.GetPKMark(i), f.oldIndexFiles); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (f *FragmentIterators) writeIndex() error {
	pkMark := fragment.NewIndexFragmentFixedSize(uint32(f.PkRec[f.pkRecPosition].RowNums()-1), uint64(f.Conf.maxRowsPerSegment))
	indexBuilder := colstore.NewIndexBuilder(f.builder.lock, f.indexFilePath)
	err := indexBuilder.WriteData(f.PkRec[f.pkRecPosition], colstore.DefaultTCLocation)
	defer indexBuilder.Reset()
	if err != nil {
		return err
	}
	f.builder.pkRec = append(f.builder.pkRec, f.PkRec[f.pkRecPosition])
	f.builder.pkMark = append(f.builder.pkMark, pkMark)
	return nil
}

func (f *FragmentIterators) writeRecord(nextFile func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16), final bool, pkSchema record.Schemas) (*MsBuilder, error) {
	rowsLimit := f.builder.Conf.maxRowsPerSegment * f.builder.Conf.maxSegmentLimit
	var err error
	if err = f.writeData(f.RecordResult, final); err != nil {
		f.log.Error("write data record fail", zap.String("file", f.builder.fd.Name()), zap.Error(err))
		return f.builder, err
	}

	//write pkRec
	if err = f.appendPkRec(pkSchema, final); err != nil {
		f.log.Error("write index record fail", zap.String("file", f.builder.fd.Name()), zap.Error(err))
		return f.builder, err
	}
	fSize := f.builder.Size()
	if fSize < f.builder.Conf.fileSizeLimit || nextFile == nil || final {
		return f.builder, nil
	}

	// switch file
	f.inited = false
	// write index, switch file add the last row to pkRec
	if err = f.writePkRec(0, pkSchema, true); err != nil {
		return f.builder, err
	}

	if err = f.writeIndex(); err != nil {
		return f.builder, err
	}

	minT, maxT := f.builder.cm.MinMaxTime()
	if err = f.writeMeta(f.builder.cm, minT, maxT); err != nil {
		return f.builder, err
	}

	f.builder, err = switchTsspFile(f.builder, f.RecordResult, f.RecordResult, rowsLimit, fSize, nextFile)
	f.builder.NewPKIndexWriter()
	dataFilePath := f.builder.FileName.String()
	f.indexFilePath = path.Join(f.builder.Path, f.builder.msName, colstore.AppendIndexSuffix(dataFilePath)+tmpFileSuffix)
	f.PkRec = append(f.PkRec, record.NewRecordBuilder(pkSchema))
	f.pkRecPosition++
	return f.builder, err
}

func (f *FragmentIterators) appendPkRec(pkSchema record.Schemas, final bool) error {
	rowNums := f.RecordResult.RowNums()
	count := rowNums / f.Conf.maxRowsPerSegment
	var err error
	var idx int
	err = f.writePkRec(0, pkSchema, false)
	if err != nil {
		return err
	}

	if final {
		if count == 0 {
			err = f.writePkRec(idx, pkSchema, true)
			if err != nil {
				return err
			}
			f.RecordResult = resetResultRec(f.RecordResult)
			return nil
		}
		for i := 1; i <= count; i++ {
			idx = f.Conf.maxRowsPerSegment * i
			err = f.writePkRec(idx, pkSchema, false)
			if err != nil {
				return err
			}
		}

		return nil
	}

	for i := 1; i < count; i++ {
		idx = f.Conf.maxRowsPerSegment * i
		err = f.writePkRec(idx, pkSchema, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *FragmentIterators) writePkRec(index int, pkSchema record.Schemas, lastRow bool) error {
	for i := 0; i < pkSchema.Len(); i++ {
		if idx := f.RecordResult.Schema.FieldIndex(pkSchema.Field(i).Name); idx >= 0 {
			switch pkSchema.Field(i).Type {
			case influx.Field_Type_String, influx.Field_Type_Tag:
				f.generatePrimaryIndexData(pkSchema.Field(i).Type, idx, i, index, lastRow)
			case influx.Field_Type_Int:
				f.generatePrimaryIndexData(influx.Field_Type_Int, idx, i, index, lastRow)
			case influx.Field_Type_Float:
				f.generatePrimaryIndexData(influx.Field_Type_Float, idx, i, index, lastRow)
			case influx.Field_Type_Boolean:
				f.generatePrimaryIndexData(influx.Field_Type_Boolean, idx, i, index, lastRow)
			default:
				return errors.New("unsupported data type")
			}
		} else {
			return fmt.Errorf("the table does not have a primary key field, %s", pkSchema.Field(i).Name)
		}
	}
	return nil
}

func (f *FragmentIterators) generatePrimaryIndexData(dataType, srcColIdx, dstColIdx, start int, lastRow bool) {
	if lastRow {
		rowNum := f.RecordResult.RowNums()
		f.PkRec[f.pkRecPosition].ColVals[dstColIdx].AppendColVal(&f.RecordResult.ColVals[srcColIdx], dataType, rowNum-1, rowNum)
		return
	}
	f.PkRec[f.pkRecPosition].ColVals[dstColIdx].AppendColVal(&f.RecordResult.ColVals[srcColIdx], dataType, start, start+1)
}

func (f *FragmentIterators) initBuilder(columnCount int) {
	if !f.inited {
		f.builder.pair.Reset(f.name)
		f.inited = true
		f.builder.cmOffset = f.builder.cmOffset[:0]
		f.builder.preCmOff = 0
		f.builder.encodeChunk = append(f.builder.encodeChunk, tableMagic...)
		f.builder.encodeChunk = numberenc.MarshalUint64Append(f.builder.encodeChunk, version)
		f.builder.dataOffset = int64(len(f.builder.encodeChunk))
		if f.builder.cm == nil {
			f.builder.cm = &ChunkMeta{}
		}
		f.builder.chunkBuilder.setChunkMeta(f.builder.cm)
		f.builder.chunkBuilder.chunkMeta.offset = f.builder.dataOffset
		f.builder.chunkBuilder.chunkMeta.columnCount = uint32(columnCount)
		f.builder.mIndex.size = 0
		f.builder.mIndex.offset = 0
		f.builder.mIndex.minTime = math.MaxInt64
		f.builder.mIndex.maxTime = math.MinInt64
	}
}

func (f *FragmentIterators) encodeData(data *record.Record) error {
	var err error
	// encode data
	f.builder.encodeChunk, err = f.builder.chunkBuilder.EncodeChunkForColumnStore(f.builder.dataOffset, data, f.builder.encodeChunk)
	if err != nil {
		f.log.Error("encode chunk fail", zap.Error(err))
		return err
	}
	f.builder.dataOffset += int64(f.builder.cm.size - f.builder.chunkBuilder.preChunkMetaSize)
	// write data to disk
	wn, err := f.builder.diskFileWriter.WriteData(f.builder.encodeChunk)
	if err != nil {
		err = errWriteFail(f.builder.diskFileWriter.Name(), err)
		f.builder.log.Error("write chunk data fail", zap.Error(err))
		return err
	}
	if wn != len(f.builder.encodeChunk) {
		f.builder.log.Error("write chunk data fail", zap.String("file", f.builder.fd.Name()),
			zap.Ints("size", []int{len(f.builder.encodeChunk), wn}))
		return io.ErrShortWrite
	}
	return nil
}

func (f *FragmentIterators) writeData(data *record.Record, final bool) error {
	record.CheckRecord(data)
	f.builder.encodeChunk = f.builder.encodeChunk[:0]
	f.initBuilder(data.ColNums())
	if err := f.encodeData(data); err != nil {
		return err
	}
	cm := f.builder.cm
	minT, maxT := cm.MinMaxTime()
	f.builder.pair.Add(0, maxT)
	f.builder.pair.AddRowCounts(int64(data.RowNums()))
	f.builder.keys[0] = struct{}{}

	//final, encode meta and write meta to disk
	if final {
		return f.writeMeta(cm, minT, maxT)
	}
	return nil
}

func (f *FragmentIterators) writeMeta(cm *ChunkMeta, minT, maxT int64) error {
	f.builder.encChunkMeta = cm.marshal(f.builder.encChunkMeta[:0])
	cmOff := f.builder.diskFileWriter.ChunkMetaSize()
	f.builder.cmOffset = append(f.builder.cmOffset, uint32(cmOff))
	wn, err := f.builder.diskFileWriter.WriteChunkMeta(f.builder.encChunkMeta)
	if err != nil {
		err = errWriteFail(f.builder.diskFileWriter.Name(), err)
		f.builder.log.Error("write chunk meta fail", zap.Error(err))
		return err
	}
	if wn != len(f.builder.encChunkMeta) {
		f.builder.log.Error("write chunk meta fail", zap.String("file", f.builder.fd.Name()), zap.Ints("size", []int{len(f.builder.encChunkMeta), wn}))
		return io.ErrShortWrite
	}

	if err = f.writeMetaIndex(wn, minT, maxT); err != nil {
		return err
	}
	return nil
}

func (f *FragmentIterators) writeMetaIndex(wn int, minT, maxT int64) error {
	f.builder.mIndex.size += uint32(wn) + 4
	f.builder.mIndex.count++
	if f.builder.mIndex.minTime > minT {
		f.builder.mIndex.minTime = minT
	}
	if f.builder.mIndex.maxTime < maxT {
		f.builder.mIndex.maxTime = maxT
	}

	if f.builder.mIndex.size >= uint32(f.builder.Conf.maxChunkMetaItemSize) || f.builder.mIndex.count >= uint32(f.builder.Conf.maxChunkMetaItemCount) {
		offBytes := numberenc.MarshalUint32SliceAppend(nil, f.builder.cmOffset)
		_, err := f.builder.diskFileWriter.WriteChunkMeta(offBytes)
		if err != nil {
			err = errWriteFail(f.builder.diskFileWriter.Name(), err)
			f.builder.log.Error("write chunk meta fail", zap.Error(err))
			return err
		}
		f.builder.metaIndexItems = append(f.builder.metaIndexItems, f.builder.mIndex)
		f.builder.mIndex.reset()
		f.builder.cmOffset = f.builder.cmOffset[:0]
		f.builder.preCmOff = f.builder.diskFileWriter.ChunkMetaSize()
		f.builder.diskFileWriter.SwitchMetaBuffer()
	}

	minTime, maxTime := f.builder.cm.MinMaxTime()
	if f.builder.trailer.idCount == 0 {
		f.builder.trailer.minTime = minTime
		f.builder.trailer.maxTime = maxTime
	}

	f.builder.trailer.idCount++
	if f.builder.trailer.minTime > minTime {
		f.builder.trailer.minTime = minTime
	}
	if f.builder.trailer.maxTime < maxTime {
		f.builder.trailer.maxTime = maxTime
	}
	return nil
}

func (f *FragmentIterators) genFragmentSchema(group FilesInfo, sortKey []string) {
	f.fields = f.fields[:0]
	f.schemaMap.Reset()
	for _, iter := range group.compIts {
		f.mergeSchema(iter.curtChunkMeta)
		f.chunkSegments += len(iter.curtChunkMeta.timeRange)
	}
	sort.Sort(f.fields)
	f.fields = append(f.fields, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	f.RecordResult.Reset()
	f.RecordResult.SetSchema(f.fields)
	f.RecordResult.ReserveColVal(f.fields.Len())
	f.genSortKeyFields(sortKey)
}

func genFieldMap(schemas record.Schemas) map[string]int {
	fieldMap := make(map[string]int)
	for i, schema := range schemas {
		fieldMap[schema.Name] = i
	}
	return fieldMap
}

func (f *FragmentIterators) genSortKeyFields(sortKey []string) {
	sortKeyFiled := record.Field{}
	fieldMap := genFieldMap(f.fields)
	for i := range sortKey {
		sortKeyFiled.Name = sortKey[i]
		sortKeyFiled.Type = f.fields[fieldMap[sortKey[i]]].Type
		f.SortKeyFileds = append(f.SortKeyFileds, sortKeyFiled)
	}
}

func (f *FragmentIterators) mergeSchema(m *ChunkMeta) {
	for i := 0; i < len(m.colMeta)-1; i++ {
		cm := m.colMeta[i]
		ref := record.Field{Name: cm.name, Type: int(cm.ty)}
		if !f.schemaMap.Has(ref.Name) {
			f.schemaMap.Set(ref.Name, ref)
			f.fields = append(f.fields, ref)
		}
	}
}

func (f *FragmentIterators) GetBreakPoint() {
	tmp := f.itrs[0]
	f.breakPoint.sortItem = tmp.sortData
	f.breakPoint.position = tmp.position
}

func (f *FragmentIterators) CompareWithBreakPoint(curPos, breakPos int) bool {
	im := f.curItrs.sortData
	jm := f.breakPoint.sortItem

	for i := 0; i < len(im); i++ {
		v, err := im[i].CompareSingleValue(jm[i], curPos, breakPos)
		if err != nil {
			panic("CompareSingleValue error")
		}
		if v == 0 {
			if i == len(im)-1 {
				return true
			}
			continue
		}
		return v > 0
	}

	return false // if the data points are the same, do not deduplicate
}

func (f *FragmentIterators) NextWithBreakPoint() {
	limit := f.Conf.maxRowsPerSegment * f.Conf.FragmentsNumPerFlush
	//fast pash
	if f.curItrs.position < f.curItrs.curRec.RowNums() && f.CompareWithBreakPoint(f.curItrs.curRec.RowNums()-1, f.breakPoint.position) {
		addNum := f.curItrs.curRec.RowNums() - f.curItrs.position
		if f.recordResultNum+addNum <= limit {
			f.curItrs.position += addNum
			f.recordResultNum += addNum
			return
		}
		f.curItrs.position += limit - f.recordResultNum
		f.recordResultNum += limit - f.recordResultNum
		return
	}

	for f.curItrs.position < f.curItrs.curRec.RowNums() {
		if f.CompareWithBreakPoint(f.curItrs.position, f.breakPoint.position) {
			f.curItrs.position++
			f.recordResultNum++
			if f.recordResultNum == limit {
				break
			}
			continue
		}
		break
	}
}

func (f *FragmentIterators) reset() {
	f.inited = false
	f.pkRecPosition = 0
	f.PkRec = f.PkRec[:0]
	f.colBuilder.resetPreAgg()
	f.mIndex.reset()
	f.TableData.reset()
}

type SortKeyIterator struct {
	*FileIterator
	position       int
	segmentPostion int
	sortKeyColums  []*record.ColVal
	sortKeyFields  []record.Field
	sortData       []record.SortItem
	curRec         *record.Record
	ctx            *ReadContext
}

func NewSortKeyIterator(fi *FileIterator, sortKeyFields []record.Field, ctx *ReadContext, schema record.Schemas) (*SortKeyIterator, error) {
	sortKeyColums, data, err := GetSortKeyColVal(fi, sortKeyFields, ctx, 0)
	if err != nil {
		return nil, err
	}
	itr := &SortKeyIterator{
		FileIterator:   fi,
		sortKeyColums:  sortKeyColums,
		sortKeyFields:  sortKeyFields,
		segmentPostion: 0,
		sortData:       data,
		ctx:            ctx,
		curRec:         &record.Record{},
	}

	// read first fragment
	if err = itr.read(schema); err != nil {
		return nil, err
	}
	return itr, nil
}

func GetSortKeyColVal(fi *FileIterator, sortKey []record.Field, ctx *ReadContext, segPosition int) ([]*record.ColVal, []record.SortItem, error) {
	var sortKeyColums []*record.ColVal
	var data []record.SortItem
	var idx int
	curtChunkMeta := fi.curtChunkMeta

	for _, key := range sortKey {
		idx = curtChunkMeta.columnIndex(&key)
		if idx < 0 {
			return nil, nil, errors.New("failed to get sort key colVal")
		}
		cm := curtChunkMeta.colMeta[idx]
		colSeg := cm.entries[segPosition] // get first segment
		segData, err := fi.readData(colSeg.offset, colSeg.size)
		if err != nil {
			return nil, nil, err
		}
		tmpCol := &record.ColVal{}
		if key.Name == timeType {
			err = appendTimeColumnData(segData, tmpCol, ctx, false)
		} else {
			err = decodeColumnData(&key, segData, tmpCol, ctx, false)
		}
		if err != nil {
			return nil, nil, err
		}

		switch key.Type {
		case influx.Field_Type_Int:
			is := record.IntegerSlice{}
			is.PadIntSlice(tmpCol)
			data = append(data, &is)
		case influx.Field_Type_Float:
			fs := record.FloatSlice{}
			fs.PadFloatSlice(tmpCol)
			data = append(data, &fs)
		case influx.Field_Type_String:
			ss := record.StringSlice{}
			ss.PadStringSlice(tmpCol)
			data = append(data, &ss)
		case influx.Field_Type_Boolean:
			bs := record.BooleanSlice{}
			bs.PadBoolSlice(tmpCol)
			data = append(data, &bs)
		}
		sortKeyColums = append(sortKeyColums, tmpCol)
	}
	return sortKeyColums, data, nil
}

func (s *SortKeyIterator) NextSingleFragment(tbStore *MmsTables, f *FragmentIterators, pkSchema record.Schemas) (*record.Record, error) {
	var err error
	cMeta := s.curtChunkMeta
	limit := f.Conf.FragmentsNumPerFlush * f.Conf.maxRowsPerSegment
	if s.position < s.curRec.RowNums() {
		err = s.appendSingleFragment(tbStore, f, pkSchema, cMeta, limit)
		if err != nil {
			return nil, err
		}
	}

	s.position = 0
	for s.segmentPostion < len(cMeta.TimeMeta().entries) {
		for i := range s.curRec.ColVals {
			s.curRec.ColVals[i].Init()
		}
		s.curRec, err = s.r.ReadAt(cMeta, s.segmentPostion, s.curRec, s.ctx, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			s.log.Error("read fragment error", zap.String("file", s.r.Path()), zap.Error(err))
			return f.RecordResult, err
		}
		if s.curRec.RowNums() == 0 {
			return f.RecordResult, nil
		}
		err = s.appendSingleFragment(tbStore, f, pkSchema, cMeta, limit)
		if err != nil {
			return nil, err
		}
		s.segmentPostion++
	}

	return f.RecordResult, nil
}

func (s *SortKeyIterator) appendSingleFragment(tbStore *MmsTables, f *FragmentIterators, pkSchema record.Schemas, cMeta *ChunkMeta, limit int) error {
	var err error
	if f.recordResultNum+s.curRec.RowNums()-s.position < limit {
		f.RecordResult.AppendRec(s.curRec, s.position, s.curRec.RowNums())
		f.recordResultNum += s.curRec.RowNums() - s.position
	} else {
		cutNum := limit - f.recordResultNum
		f.RecordResult.AppendRec(s.curRec, s.position, s.position+cutNum)
		if err = f.Flush(tbStore, pkSchema, false); err != nil {
			return err
		}
		f.recordResultNum = 0
		// itr has no fragment left and curRec has no data left, so add the last rows to pkRec
		if s.segmentPostion == len(cMeta.TimeMeta().entries) && s.position+cutNum == s.curRec.RowNums() {
			if err = f.writePkRec(0, pkSchema, true); err != nil {
				return err
			}
		}
		f.RecordResult = resetResultRec(f.RecordResult)
		// if start == end,func will return
		f.RecordResult.AppendRec(s.curRec, s.position+cutNum, s.curRec.RowNums())
		f.recordResultNum += s.curRec.RowNums() - (s.position + cutNum)
	}
	return nil
}

func (s *SortKeyIterator) read(schema record.Schemas) error {
	var err error
	cMeta := s.curtChunkMeta

	s.curRec.Reset()
	s.curRec.SetSchema(schema)
	s.curRec.ReserveColVal(len(schema))
	record.CheckRecord(s.curRec)

	s.curRec, err = s.r.ReadAt(cMeta, s.segmentPostion, s.curRec, s.ctx, fileops.IO_PRIORITY_LOW_READ)
	if err != nil {
		s.log.Error("read fragment error", zap.String("file", s.r.Path()), zap.Error(err))
		return err
	}
	s.segmentPostion++
	return nil
}

func (s *SortKeyIterator) GetNewRecord() error {
	cMeta := s.curtChunkMeta
	for i := range s.curRec.ColVals {
		s.curRec.ColVals[i].Init()
	}
	var err error
	s.curRec, err = s.r.ReadAt(cMeta, s.segmentPostion, s.curRec, s.ctx, fileops.IO_PRIORITY_LOW_READ)
	if err != nil {
		s.log.Error("read fragment error", zap.String("file", s.r.Path()), zap.Error(err))
		return err
	}

	if s.curRec.RowNums() == 0 {
		return nil
	}

	sortKeyColums, data, err := GetSortKeyColVal(s.FileIterator, s.sortKeyFields, s.ctx, s.segmentPostion)
	if err != nil {
		return err
	}
	s.sortKeyColums = sortKeyColums
	s.sortData = data

	s.segmentPostion++
	return nil
}
