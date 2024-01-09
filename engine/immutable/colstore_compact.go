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
	"path"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

var fragmentIteratorsPool = NewFragmentIteratorsPool(cpu.GetCpuNum() / 2)
var bfBuffPool = bufferpool.NewByteBufferPool(uint64(logstore.GetConstant(logstore.CurrentLogTokenizerVersion).MaxBlockStoreNBytes), 0, 8)

const primaryKeyHeaderSize uint32 = 8 // primaryKeyMagic + primaryKeyVersion size
const blockCompareLength = 1          //The number of data rows to sort by in a batch of data

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

var bloomFilterPool sync.Pool

type bloomFilter []byte

func GetBloomFilterBuf() *bloomFilter {
	key := bloomFilterPool.Get()
	if key == nil {
		bloomFilterBuf := make(bloomFilter, 0, 1024*1024)
		return &bloomFilterBuf
	}
	return key.(*bloomFilter)
}

func PutBloomFilterBuf(key *bloomFilter) {
	*key = (*key)[:0]
	bloomFilterPool.Put(key)
}

func IsFlushToFinalFile(totalSegmentCnt, flushToFinalFileLimit uint64) bool {
	return totalSegmentCnt >= flushToFinalFileLimit
}

type FragmentIterator interface {
	compact(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error)
}

type IteratorByRow struct {
	f    *FragmentIterators
	conf *Config
}

func NewIteratorByRow(f *FragmentIterators, conf *Config) *IteratorByRow {
	return &IteratorByRow{
		f:    f,
		conf: conf,
	}
}

func (ir *IteratorByRow) needFlush(curRowNums int) bool {
	return curRowNums == ir.conf.maxRowsPerSegment*ir.conf.FragmentsNumPerFlush
}

func (ir *IteratorByRow) compact(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error) {
	f := ir.f
	var err error
	var ok bool
	for {
		if f.stopCompact() {
			f.log.Error("row compact stopped")
			return nil, ErrCompStopped
		}

		if f.IsEmpty() {
			break
		}
		f.curItrs, ok = heap.Pop(f).(*SortKeyIterator)
		if !ok {
			return nil, errors.New("SortKeyIterator type switch error")
		}

		if f.IsEmpty() {
			//last itrs
			f.RecordResult, err = f.curItrs.NextSingleFragment(tbStore, ir, pkSchema)
			if err != nil {
				return nil, err
			}
		} else {
			ir.GetBreakPoint()
			start := f.curItrs.position
			ir.NextWithBreakPoint()
			if f.curItrs.position < f.curItrs.curRec.RowNums() && start == f.curItrs.position { // no data smaller than break point
				f.curItrs.position++
				f.recordResultNum++
			}

			f.RecordResult.AppendRec(f.curItrs.curRec, start, f.curItrs.position)
			if f.tcDuration > 0 {
				f.TimeClusterResult.ColVals[0].AppendColVal(f.curItrs.sortKeyColums[0], influx.Field_Type_Int, start, f.curItrs.position)
			}
			if ir.needFlush(f.RecordResult.RowNums()) {
				err = ir.Flush(tbStore, pkSchema, false)

				if err != nil {
					return nil, err
				}
				f.RecordResult = resetResultRec(f.RecordResult)
				f.TimeClusterResult = resetResultRec(f.TimeClusterResult)
				f.recordResultNum = 0
			}

			// curItrs is empty
			if f.curItrs.position >= f.curItrs.curRec.RowNums() {
				if f.curItrs.segmentPostion >= len(f.curItrs.curtChunkMeta.TimeMeta().entries) {
					continue
				}
				f.curItrs.position = 0
				err = f.curItrs.GetNewRecord(f.tcDuration, false)
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

	return ir.flushIntoFiles(pkSchema, tbStore)
}

func (ir *IteratorByRow) Flush(tbStore *MmsTables, pkSchema record.Schemas, final bool) error {
	f := ir.f
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
			if err = f.writePrimaryIndex(); err != nil {
				return err
			}
		}

		if err = WriteIntoFile(f.builder, true, f.builder.GetPKInfoNum() != 0, nil); err != nil {
			f.builder.log.Error("rename init file failed", zap.String("mstName", f.name), zap.Error(err))
			f.builder = nil
			return err
		}

		//update data files
		tbStore.AddTSSPFiles(f.builder.msName, true, f.builder.Files...)
		if f.builder.GetPKInfoNum() != 0 {
			for i, file := range f.builder.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				if err = tbStore.ReplacePKFile(f.builder.msName, indexFilePath, f.builder.GetPKRecord(i), f.builder.GetPKMark(i), f.oldIndexFiles); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ir *IteratorByRow) NextWithBreakPoint() {
	f := ir.f
	limit := ir.conf.maxRowsPerSegment * ir.conf.FragmentsNumPerFlush
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

func (ir *IteratorByRow) GetBreakPoint() {
	f := ir.f
	tmp := f.itrs[0]
	f.breakPoint.sortItem = tmp.sortData
	f.breakPoint.position = tmp.position
}

func (ir *IteratorByRow) flushIntoFiles(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error) {
	f := ir.f
	if err := ir.Flush(tbStore, pkSchema, true); err != nil {
		return nil, err
	}
	f.RecordResult = resetResultRec(f.RecordResult)
	f.TimeClusterResult = resetResultRec(f.TimeClusterResult)
	f.recordResultNum = 0

	newFiles := make([]TSSPFile, 0, len(f.builder.Files))
	newFiles = append(newFiles, f.builder.Files...)
	f.builder = nil
	return newFiles, nil
}

type BloomFilterIterator struct {
	f *FragmentIterators

	blockRecord         []int    // record each block get from which file eg: [1,3,6,3,7,4,1]
	bloomFilterPaths    []string // new skip index files name to local
	bloomFilterBuf      []byte
	bloomFilterCols     []string                    // bloomfilter column list
	bloomFilterBlockPos [][]int64                   //each bf file's reader current position
	bloomFilterReaders  [][]fileops.BasicFileReader // each bf file's reader  (data files number * skipIndex columns number)

	constant *logstore.Constant
}

func NewBloomFilterIterator(f *FragmentIterators, oldFiles []TSSPFile, bfCols []string) (*BloomFilterIterator, error) {
	bfi := &BloomFilterIterator{
		f:               f,
		blockRecord:     make([]int, 0),
		bloomFilterBuf:  make([]byte, logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize),
		bloomFilterCols: bfCols,
		constant:        logstore.GetConstant(logstore.CurrentLogTokenizerVersion),
	}
	bfi.updateBloomFilterPaths()
	err := bfi.newBloomFilterReaders(oldFiles)
	if err != nil {
		return nil, err
	}
	return bfi, nil
}

func (bfi *BloomFilterIterator) reset() {
	bfi.blockRecord = bfi.blockRecord[:0]
}

func (bfi *BloomFilterIterator) updateBloomFilterPaths() {
	bfi.bloomFilterPaths = make([]string, len(bfi.bloomFilterCols))
	for i := range bfi.bloomFilterCols {
		bfi.bloomFilterPaths[i] = path.Join(bfi.f.builder.Path, bfi.f.name,
			colstore.AppendSKIndexSuffix(bfi.f.dataFilePath, bfi.bloomFilterCols[i], colstore.BloomFilterIndex)+tmpFileSuffix)
	}
}

func (bfi *BloomFilterIterator) newBloomFilterReader(name string) (fileops.BasicFileReader, error) {
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	lock := fileops.FileLockOption("")
	fd, err := fileops.Open(name, lock, pri)
	if err != nil {
		err = errno.NewError(errno.OpenFileFailed, err)
		log.Error("open bloom filter file failed", zap.String("file", name), zap.Error(err))
		return nil, err
	}
	lockPath := ""
	dr := fileops.NewFileReader(fd, &lockPath)
	return dr, nil
}

func (bfi *BloomFilterIterator) newBloomFilterReaders(oldFiles []TSSPFile) error {
	var fileName string
	var err error
	var dr fileops.BasicFileReader
	bfi.bloomFilterReaders = make([][]fileops.BasicFileReader, len(oldFiles))
	bfi.bloomFilterBlockPos = make([][]int64, len(oldFiles))
	for i := 0; i < len(oldFiles); i++ {
		fn := oldFiles[i].FileName()
		bfi.bloomFilterReaders[i] = make([]fileops.BasicFileReader, len(bfi.bloomFilterCols))
		bfi.bloomFilterBlockPos[i] = make([]int64, len(bfi.bloomFilterCols))
		for j := 0; j < len(bfi.bloomFilterCols); j++ {
			fileName = path.Join(bfi.f.builder.Path, bfi.f.name,
				colstore.AppendSKIndexSuffix(fn.String(), bfi.bloomFilterCols[j], colstore.BloomFilterIndex))
			dr, err = bfi.newBloomFilterReader(fileName)
			if err != nil {
				return err
			}
			bfi.bloomFilterReaders[i][j] = dr
		}
	}
	return nil
}

func (bfi *BloomFilterIterator) AppendFileIdx(fileIdx int) {
	bfi.blockRecord = append(bfi.blockRecord, fileIdx)
}

func (bfi *BloomFilterIterator) Write(toLocal bool) error {
	bf := GetBloomFilterBuf()
	defer PutBloomFilterBuf(bf)
	filterVerBuffer := bfBuffPool.Get()
	defer bfBuffPool.Put(filterVerBuffer)

	filterDataDiskSize := logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize
	var fileName string
	var fileIndex, count int
	var err error
	var dr fileops.BasicFileReader
	for idx := range bfi.bloomFilterCols {
		count = 0
		*bf = (*bf)[:0]
		for i := range bfi.blockRecord {
			fileIndex = bfi.blockRecord[i]
			dr = bfi.bloomFilterReaders[fileIndex][idx]
			//read target bf block
			_, err = dr.ReadAt(bfi.bloomFilterBlockPos[fileIndex][idx]*filterDataDiskSize, uint32(filterDataDiskSize),
				&bfi.bloomFilterBuf, fileops.IO_PRIORITY_ULTRA_HIGH)
			if err != nil {
				_ = dr.Close()
				log.Error("read file bloomFilter failed", zap.String("file", fileName), zap.Error(err))
				return err
			}
			*bf = append(*bf, bfi.bloomFilterBuf...)
			bfi.bloomFilterBlockPos[fileIndex][idx]++
			count++
			if !toLocal && count == int(bfi.constant.FilterCntPerVerticalGorup) {
				// flush vertical bloom filter
				filterVerBuffer = logstore.FlushVerticalFilter(filterVerBuffer[:0], *bf)

				_, err = bfi.f.builder.metaFileWriter.WriteBloomFilter(idx, filterVerBuffer)
				if err != nil {
					bfi.f.builder.log.Error("write bloom filter fail", zap.String("name", fileName), zap.Error(err))
					return err
				}
				*bf = (*bf)[:0]
				count = 0
			}
			filterVerBuffer = filterVerBuffer[:0]
		}
		// write to Local
		if len(*bf) > 0 {
			lockpath := ""
			indexBuilder := colstore.NewSkipIndexBuilder(&lockpath, bfi.bloomFilterPaths[idx])
			err = indexBuilder.WriteData(*bf)
			if err != nil {
				bfi.f.builder.log.Error("write bloom filter to local fail", zap.String("name", fileName), zap.Error(err))
				return err
			}
			indexBuilder.Reset()
		}
		*bf = (*bf)[:0]
	}
	return nil
}

// IteratorByBlock for single mst
type IteratorByBlock struct {
	name string // mst name
	f    *FragmentIterators
	conf *Config

	flushToFinalFile       bool // satisfy writing to the final file(eg. files on remote obs or hdfs storage)
	firstFlush             bool
	cacheDataTotalCnt      int
	cacheDataTotalCntLimit int
	cacheDataCnt           int

	constant            *logstore.Constant
	accumulateMetaIndex *AccumulateMetaIndex
}

func NewIteratorByBlock(f *FragmentIterators, conf *Config, group FilesInfo, accumulateMetaIndex *AccumulateMetaIndex) *IteratorByBlock {
	ib := &IteratorByBlock{
		name:                group.name,
		f:                   f,
		conf:                conf,
		firstFlush:          accumulateMetaIndex.blockId == 0,
		constant:            logstore.GetConstant(logstore.CurrentLogTokenizerVersion),
		accumulateMetaIndex: accumulateMetaIndex,
	}

	if IsFlushToFinalFile(group.totalSegmentCount, uint64(ib.constant.FilterCntPerVerticalGorup)) {
		ib.cacheDataTotalCntLimit = int(group.totalSegmentCount - (group.totalSegmentCount % uint64(ib.constant.FilterCntPerVerticalGorup)))
		ib.flushToFinalFile = true
	} else {
		ib.cacheDataTotalCntLimit = int(ib.constant.FilterCntPerVerticalGorup)
		ib.flushToFinalFile = false
	}

	return ib
}

func (ib *IteratorByBlock) needFlush() bool {
	if ib.cacheDataTotalCnt == ib.cacheDataTotalCntLimit || ib.cacheDataCnt == ib.conf.FragmentsNumPerFlush {
		return true
	}
	return false
}

func (ib *IteratorByBlock) compact(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error) {
	f := ib.f
	var err error
	var ok bool
	var accumulateRowCount int
	for {
		if f.stopCompact() {
			f.log.Error("block compact stopped")
			return nil, ErrCompStopped
		}

		if f.IsEmpty() {
			break
		}
		f.curItrs, ok = heap.Pop(f).(*SortKeyIterator)
		if !ok {
			return nil, errors.New("SortKeyIterator type switch error")
		}
		//merge data
		f.RecordResult.AppendRec(f.curItrs.curRec, 0, f.curItrs.curRec.RowNums())

		//get split idx according to flush size
		accumulateRowCount += f.curItrs.curRec.RowNums()
		f.accumulateRowsIndex = append(f.accumulateRowsIndex, accumulateRowCount)
		f.appendFileIdxForBF(f.curItrs.fileIdx)

		ib.cacheDataCnt++
		ib.cacheDataTotalCnt++
		if ib.needFlush() {
			err = ib.Flush(pkSchema, false, tbStore)
			if err != nil {
				return nil, err
			}
			f.RecordResult = resetResultRec(f.RecordResult)
			//accumulateRowsIndex just for this batch of data
			f.accumulateRowsIndex = f.accumulateRowsIndex[:0]
			accumulateRowCount = 0
		}

		//curItrs is empty
		if f.curItrs.segmentPostion >= len(f.curItrs.curtChunkMeta.TimeMeta().entries) {
			continue
		}

		f.curItrs.position = 0
		err = f.curItrs.GetNewRecord(f.tcDuration, true)
		if err != nil {
			return nil, err
		}
		if f.curItrs.curRec.RowNums() != 0 {
			heap.Push(f, f.curItrs)
		}
	}

	//The last batch of iteration data does not meet the specified size，then flush to sfs
	return ib.flushIntoFiles(pkSchema, tbStore)
}

func (ib *IteratorByBlock) Flush(pkSchema record.Schemas, readFinal bool, tbStore *MmsTables) error {
	f := ib.f
	var err error
	//flush to final file(eg. files on remote obs or hdfs storage)
	if ib.flushToFinalFile {
		//final must be false, so that chunkMeta hasn't flush to obs
		f.builder.EncodeChunkDataImp.SetDetachedInfo(true)
		f.builder, err = ib.writeRecord(f, readFinal, pkSchema)
		if err != nil {
			return err
		}
		ib.cacheDataCnt = 0

		//flush meta/bf/pkIndex/... to obs
		if ib.cacheDataTotalCnt == ib.cacheDataTotalCntLimit {
			err = ib.WriteDetachedMeta(pkSchema)
			if err != nil {
				return err
			}
			//update accumulateMetaIndex
			ib.accumulateMetaIndex.blockId += uint64(ib.cacheDataTotalCntLimit)
			ib.accumulateMetaIndex.dataOffset += f.builder.dataOffset
			ib.accumulateMetaIndex.offset += int64(f.builder.mIndex.size)

			ib.cacheDataTotalCnt = 0
			ib.flushToFinalFile = false
			ib.firstFlush = false

			f.resetFileIdxForBF()
			//renew builder, write remain data to sfs
			f.inited = false
			err = f.builder.closeFdWrite()
			if err != nil {
				return err
			}
			f.PkRec[f.pkRecPosition] = resetResultRec(f.PkRec[f.pkRecPosition])
			f.builder = ib.switchMsBuilder(tbStore)
			f.builder.EncodeChunkDataImp.SetDetachedInfo(false)
			tbStore.ImmTable.UpdateAccumulateMetaIndexInfo(ib.name, ib.accumulateMetaIndex)
		}
		return nil
	}

	//flush data to sfs
	f.builder, err = ib.writeRecord(f, readFinal, pkSchema)
	if err != nil {
		return err
	}

	if f.builder != nil && readFinal {
		minT, maxT := f.builder.chunkBuilder.getMinMaxTime(f.builder.timeSorted)
		if err = f.writeMeta(f.builder.chunkBuilder.chunkMeta, minT, maxT); err != nil {
			f.log.Error("write meta fail", zap.String("file", f.builder.fd.Name()), zap.Error(err))
			return err
		}

		if f.RecordResult.TimeColumn().Len != 0 {
			if err = f.writePkRec(0, pkSchema, true); err != nil {
				return err
			}
		}
		// write index if needed
		if f.PkRec[f.pkRecPosition].TimeColumn().Len > 0 {
			if err = f.writePrimaryIndex(); err != nil { // write index to obs
				return err
			}
		}

		err = f.writeBloomFilters(true)
		if err != nil {
			return err
		}

		if err = WriteIntoFile(f.builder, true, f.builder.GetPKInfoNum() != 0, nil); err != nil {
			f.builder.log.Error("rename init file failed", zap.String("mstName", f.name), zap.Error(err))
			f.builder = nil
			return err
		}
		//update data files
		tbStore.AddTSSPFiles(f.builder.msName, true, f.builder.Files...)
		if f.builder.GetPKInfoNum() != 0 {
			for i, file := range f.builder.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				if err = tbStore.ReplacePKFile(f.builder.msName, indexFilePath, f.builder.GetPKRecord(i), f.builder.GetPKMark(i), f.oldIndexFiles); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ib *IteratorByBlock) WriteDetachedMeta(pkSchema record.Schemas) error {
	f := ib.f
	//add last row to pkRec
	err := f.writePkRec(0, pkSchema, true)
	if err != nil {
		return err
	}

	err = f.writeBloomFilters(false)
	if err != nil {
		return err
	}

	err = f.builder.writeDetachedChunkMeta(f.RecordResult, ib.firstFlush, ib.accumulateMetaIndex.blockId)
	if err != nil {
		return err
	}

	tcLocation := colstore.DefaultTCLocation
	if f.tcDuration > 0 {
		tcLocation = 0
	}
	err = f.builder.writeDetachedPrimaryIndex(ib.firstFlush, f.PkRec[f.pkRecPosition], tcLocation, uint64(ib.cacheDataTotalCntLimit), ib.accumulateMetaIndex)
	if err != nil {
		return err
	}

	return f.builder.writeDetachedMetaIndex(ib.firstFlush, ib.accumulateMetaIndex)
}

func (ib *IteratorByBlock) switchMsBuilder(tbStore *MmsTables) *MsBuilder {
	msb := ib.f.builder
	builder := NewMsBuilder(tbStore.path, ib.f.name, tbStore.lock, tbStore.Conf, 1, msb.FileName, *tbStore.tier, nil, 1, config.COLUMNSTORE)
	builder.tcLocation = msb.tcLocation
	builder.timeSorted = msb.timeSorted
	builder.WithLog(msb.log)
	return builder
}

func (ib *IteratorByBlock) writeRecord(f *FragmentIterators, final bool, pkSchema record.Schemas) (*MsBuilder, error) {
	var err error
	if err = f.writeData(f.RecordResult, ib.firstFlush, ib.flushToFinalFile, ib.accumulateMetaIndex.dataOffset); err != nil {
		f.log.Error("write data record fail", zap.String("file", f.builder.fd.Name()), zap.Error(err))
		return f.builder, err
	}

	//write pkRec
	if err = f.appendPkRecBySize(pkSchema, final); err != nil {
		f.log.Error("write index record fail", zap.String("file", f.builder.fd.Name()), zap.Error(err))
		return f.builder, err
	}
	return f.builder, err
}

func (ib *IteratorByBlock) flushIntoFiles(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error) {
	if err := ib.Flush(pkSchema, true, tbStore); err != nil {
		return nil, err
	}
	ib.f.RecordResult = resetResultRec(ib.f.RecordResult)
	ib.f.recordResultNum = 0

	newFiles := make([]TSSPFile, 0, len(ib.f.builder.Files))
	newFiles = append(newFiles, ib.f.builder.Files...)
	ib.f.builder = nil
	return newFiles, nil
}

type FragmentIterators struct {
	closed              chan struct{}
	dropping            *int64
	chunkSegments       int
	estimateSize        int
	recordResultNum     int
	pkRecPosition       int
	dir                 string
	name                string // measurement name with version
	indexFilePath       string
	inited              bool
	oldIndexFiles       []string
	SortKeyFileds       []record.Field
	tcDuration          time.Duration // duration for time cluster
	fields              record.Schemas
	itrs                []*SortKeyIterator
	curItrs             *SortKeyIterator
	dataFilePath        string
	accumulateRowsIndex []int //eg, [700,1400,2100] there are three segment,each length is 700,700,700

	TableData
	fi                FragmentIterator
	bfIterator        *BloomFilterIterator
	mIndex            MetaIndex
	Conf              *Config
	ctx               *ReadContext
	colBuilder        *ColumnBuilder
	schemaMap         map[string]struct{}
	breakPoint        *breakPoint
	files             []TSSPFile
	builder           *MsBuilder
	PkRec             []*record.Record
	RecordResult      *record.Record
	TimeClusterResult *record.Record
	log               *Log.Logger
}

type breakPoint struct {
	sortItem []record.SortItem
	position int
}

func (f *FragmentIterators) stopCompact() bool {
	if atomic.LoadInt64(f.dropping) > 0 {
		return true
	}

	select {
	case <-f.closed:
		return true
	default:
		return false
	}
}

func (f *FragmentIterators) newIteratorByBlock(group FilesInfo, bfCols []string, accumulateMetaIndex *AccumulateMetaIndex) error {
	ib := NewIteratorByBlock(f, f.Conf, group, accumulateMetaIndex)
	if ib == nil {
		return fmt.Errorf("create IteratorByBlock failed")
	}
	f.fi = ib

	if len(bfCols) == 0 {
		return nil
	}
	bfIterator, err := NewBloomFilterIterator(f, group.oldFiles, bfCols)
	if err != nil {
		log.Error("new bloomFileterIterator failed", zap.String("file", group.name), zap.Error(err))
		return err
	}
	f.bfIterator = bfIterator

	return nil
}

func (f *FragmentIterators) newIteratorByRow() error {
	ir := NewIteratorByRow(f, f.Conf)
	if ir == nil {
		return fmt.Errorf("create IteratorByRow failed")
	}
	f.fi = ir
	return nil
}

func (f *FragmentIterators) updateIterators(m *MmsTables, group FilesInfo, sortKey []string, primaryKey record.Schemas,
	mstInfo *meta.MeasurementInfo, bfCols []string) error {
	f.closed = m.closed
	f.dropping = group.dropping
	f.name = group.name
	f.dir = m.path
	f.Conf = m.Conf
	f.itrs = f.itrs[:0]
	f.SortKeyFileds = f.SortKeyFileds[:0]
	f.genFragmentSchema(group, sortKey)
	f.estimateSize = group.estimateSize
	f.breakPoint = &breakPoint{}

	_, seq := group.oldFiles[0].LevelAndSequence()
	ext := group.oldFiles[0].FileNameExtend()
	fileName := NewTSSPFileName(seq, group.toLevel, 0, 0, true, m.lock)
	var err error
	//gen msBuilder
	if mstInfo.IsBlockCompact() {
		fileName.level--
		fileName.extent = ext + 1
		// totalSegmentCount less than bloomFilterFlushBlockCount, write data to sfs
		if !IsFlushToFinalFile(group.totalSegmentCount, uint64(logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterCntPerVerticalGorup)) {
			f.builder = NewMsBuilder(m.path, f.name, m.lock, m.Conf, 1, fileName, *m.tier, nil, 1, config.COLUMNSTORE)
		} else {
			f.builder, err = NewDetachedMsBuilder(m.path, f.name, m.lock, m.Conf, 1, fileName, *m.tier, nil, 1, config.COLUMNSTORE, mstInfo.ObsOptions, bfCols)
			if err != nil {
				return err
			}
		}
	} else {
		f.builder = NewMsBuilder(m.path, f.name, m.lock, m.Conf, 1, fileName, *m.tier, nil, 1, config.COLUMNSTORE)
	}

	f.builder.NewPKIndexWriter()
	f.dataFilePath = fileName.String()
	f.indexFilePath = path.Join(f.builder.Path, f.name, colstore.AppendPKIndexSuffix(f.dataFilePath)+tmpFileSuffix)
	f.PkRec = append(f.PkRec, record.NewRecordBuilder(primaryKey))
	f.pkRecPosition = 0
	f.recordResultNum = 0
	f.oldIndexFiles = group.oldIndexFiles
	f.accumulateRowsIndex = make([]int, 0, f.builder.Conf.FragmentsNumPerFlush)

	for idx, fi := range group.compIts {
		iterator, err := NewSortKeyIterator(fi, f.SortKeyFileds, f.ctx, f.fields, f.tcDuration, mstInfo.IsBlockCompact(), idx)
		if err != nil {
			panicInfo := fmt.Sprintf("[Column store compact Panic:err:%s, name:%s, oldFids:%v,] %s",
				err, group.name, group.oldFids, debug.Stack())
			log.Error(panicInfo)
			return err
		}
		f.itrs = append(f.itrs, iterator)
	}
	return nil
}

func (f *FragmentIterators) writeBloomFilters(toLocal bool) error {
	if f.bfIterator == nil {
		return nil
	}
	return f.bfIterator.Write(toLocal)
}

func (f *FragmentIterators) appendFileIdxForBF(fileIdx int) {
	if f.bfIterator == nil {
		return
	}
	f.bfIterator.AppendFileIdx(fileIdx)
}

func (f *FragmentIterators) resetFileIdxForBF() {
	if f.bfIterator == nil {
		return
	}
	f.bfIterator.reset()
}

func (f *FragmentIterators) WithLog(log *Log.Logger) {
	f.log = log
	for i := range f.itrs {
		f.itrs[i].WithLog(log)
	}
}

func (f *FragmentIterators) compact(pkSchema record.Schemas, tbStore *MmsTables) ([]TSSPFile, error) {
	return f.fi.compact(pkSchema, tbStore)
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

func (f *FragmentIterators) IsEmpty() bool {
	return len(f.itrs) == 0
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
	f.resetSchemaMap()
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
				ctx:               NewReadContext(true),
				colBuilder:        NewColumnBuilder(),
				itrs:              make([]*SortKeyIterator, 0, 8),
				SortKeyFileds:     make([]record.Field, 0, 16),
				RecordResult:      &record.Record{},
				TimeClusterResult: &record.Record{},
				schemaMap:         make(map[string]struct{}),
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

func resetResultRec(rec *record.Record) *record.Record {
	for i := range rec.ColVals {
		rec.ColVals[i].Init()
	}
	return rec
}

func (f *FragmentIterators) writePrimaryIndex() error {
	pkMark := fragment.NewIndexFragmentFixedSize(uint32(f.PkRec[f.pkRecPosition].RowNums()-1), uint64(f.Conf.maxRowsPerSegment))
	indexBuilder := colstore.NewIndexBuilder(f.builder.lock, f.indexFilePath)
	tcLocation := colstore.DefaultTCLocation
	if f.tcDuration > 0 {
		tcLocation = 0
	}
	err := indexBuilder.WriteData(f.PkRec[f.pkRecPosition], tcLocation)
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
	if err = f.writeData(f.RecordResult, false, false, 0); err != nil {
		f.log.Error("write data record fail", zap.String("file", f.builder.fd.Name()), zap.Error(err))
		return f.builder, err
	}

	//final, encode meta and write meta to disk
	if final {
		minT, maxT := f.builder.chunkBuilder.getMinMaxTime(f.builder.timeSorted)
		if err = f.writeMeta(f.builder.chunkBuilder.chunkMeta, minT, maxT); err != nil {
			f.log.Error("write meta fail", zap.String("file", f.builder.fd.Name()), zap.Error(err))
			return f.builder, err
		}
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

	if err = f.writePrimaryIndex(); err != nil {
		return f.builder, err
	}

	minT, maxT := f.builder.chunkBuilder.getMinMaxTime(f.builder.timeSorted)
	if err = f.writeMeta(f.builder.chunkBuilder.chunkMeta, minT, maxT); err != nil {
		return f.builder, err
	}

	f.builder, err = switchTsspFile(f.builder, f.RecordResult, f.RecordResult, rowsLimit, fSize, nextFile, config.COLUMNSTORE)
	f.builder.NewPKIndexWriter()
	dataFilePath := f.builder.FileName.String()
	f.indexFilePath = path.Join(f.builder.Path, f.builder.msName, colstore.AppendPKIndexSuffix(dataFilePath)+tmpFileSuffix)
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
			f.TimeClusterResult = resetResultRec(f.TimeClusterResult)
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

func (f *FragmentIterators) appendPkRecBySize(pkSchema record.Schemas, final bool) error {
	count := len(f.accumulateRowsIndex)
	if count == 0 {
		return nil
	}

	// the last item we need rec.RowsNums()-1 to create pk idx
	f.accumulateRowsIndex[count-1] = f.accumulateRowsIndex[count-1] - 1
	var err error
	err = f.writePkRec(0, pkSchema, false)
	if err != nil {
		return err
	}

	if final {
		if count == 0 {
			err = f.writePkRec(0, pkSchema, true)
			if err != nil {
				return err
			}
			f.RecordResult = resetResultRec(f.RecordResult)
			return nil
		}
		for i := 0; i < count-1; i++ {
			err = f.writePkRec(f.accumulateRowsIndex[i], pkSchema, false)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := 0; i < count-1; i++ {
		err = f.writePkRec(f.accumulateRowsIndex[i], pkSchema, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *FragmentIterators) writePkRec(index int, pkSchema record.Schemas, lastRow bool) error {
	var i int
	if f.tcDuration > 0 {
		i++
		f.generateTimeClusterIndexData(index, lastRow)
	}
	for ; i < pkSchema.Len(); i++ {
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

func (f *FragmentIterators) generateTimeClusterIndexData(start int, lastRow bool) {
	if lastRow {
		rowNum := f.TimeClusterResult.RowNums()
		f.PkRec[f.pkRecPosition].ColVals[0].AppendColVal(&f.TimeClusterResult.ColVals[0], influx.Field_Type_Int, rowNum-1, rowNum)
		return
	}
	f.PkRec[f.pkRecPosition].ColVals[0].AppendColVal(&f.TimeClusterResult.ColVals[0], influx.Field_Type_Int, start, start+1)
}

func (f *FragmentIterators) encodeData(data *record.Record) error {
	var err error
	//encode data
	f.builder.encodeChunk, err = f.builder.EncodeChunkDataImp.EncodeChunkForCompaction(f.builder.chunkBuilder,
		f.builder.dataOffset, data, f.builder.encodeChunk, f.accumulateRowsIndex, f.builder.timeSorted)
	if err != nil {
		f.log.Error("encode chunk fail", zap.Error(err))
		return err
	}
	return f.builder.writeDataToDisk()
}

func (f *FragmentIterators) writeData(data *record.Record, firstFlush, toRemote bool, dataOffset int64) error {
	record.CheckRecord(data)
	f.builder.encodeChunk = f.builder.encodeChunk[:0]
	f.builder.initDetachedBuilder(data.ColNums(), firstFlush, toRemote, dataOffset)
	if err := f.encodeData(data); err != nil {
		return err
	}
	cm := f.builder.chunkBuilder.chunkMeta
	_, maxT := cm.MinMaxTime()
	f.builder.pair.Add(0, maxT)
	f.builder.pair.AddRowCounts(int64(data.RowNums()))
	f.builder.keys[0] = struct{}{}

	return nil
}

func (f *FragmentIterators) writeMeta(cm *ChunkMeta, minT, maxT int64) error {
	wn, err := f.builder.WriteChunkMeta(cm)
	if err != nil {
		return err
	}

	if err = f.writeMetaIndex(wn, minT, maxT); err != nil {
		return err
	}
	return nil
}

func (f *FragmentIterators) writeMetaIndex(wn int, minT, maxT int64) error {
	f.builder.mIndex.size += uint32(wn)
	f.builder.mIndex.count++
	if f.builder.mIndex.minTime > minT {
		f.builder.mIndex.minTime = minT
	}
	if f.builder.mIndex.maxTime < maxT {
		f.builder.mIndex.maxTime = maxT
	}

	if f.builder.mIndex.size >= uint32(f.builder.Conf.maxChunkMetaItemSize) || f.builder.mIndex.count >= uint32(f.builder.Conf.maxChunkMetaItemCount) {
		err := f.builder.SwitchChunkMeta()
		if err != nil {
			return err
		}
	}

	if f.builder.trailer.idCount == 0 {
		f.builder.trailer.minTime = minT
		f.builder.trailer.maxTime = maxT
	}

	f.builder.trailer.idCount++
	if f.builder.trailer.minTime > minT {
		f.builder.trailer.minTime = minT
	}
	if f.builder.trailer.maxTime < maxT {
		f.builder.trailer.maxTime = maxT
	}
	return nil
}

func (f *FragmentIterators) resetSchemaMap() {
	for k := range f.schemaMap {
		delete(f.schemaMap, k)
	}
}

func (f *FragmentIterators) genFragmentSchema(group FilesInfo, sortKey []string) {
	f.fields = f.fields[:0]
	f.resetSchemaMap()
	for _, iter := range group.compIts {
		f.mergeSchema(iter.curtChunkMeta)
		f.chunkSegments += len(iter.curtChunkMeta.timeRange)
	}
	sort.Sort(f.fields)
	f.fields = append(f.fields, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	f.RecordResult.Reset()
	f.RecordResult.SetSchema(f.fields)
	f.RecordResult.ReserveColVal(f.fields.Len())
	f.TimeClusterResult.Reset()
	f.TimeClusterResult.SetSchema([]record.Field{{Name: record.TimeClusterCol, Type: influx.Field_Type_Int}})
	f.TimeClusterResult.ReserveColVal(1)
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
	if f.tcDuration > 0 { // set time clustered time as the first sort key
		f.SortKeyFileds = append(f.SortKeyFileds, record.Field{Type: influx.Field_Type_Int, Name: record.TimeClusterCol})
	}
	for i := range sortKey {
		sortKeyFiled.Name = sortKey[i]
		sortKeyFiled.Type = f.fields[fieldMap[sortKey[i]]].Type
		f.SortKeyFileds = append(f.SortKeyFileds, sortKeyFiled)
	}
}

func (f *FragmentIterators) mergeSchema(m *ChunkMeta) {
	for i := 0; i < len(m.colMeta)-1; i++ {
		cm := &m.colMeta[i]
		if _, ok := f.schemaMap[cm.Name()]; !ok {
			size := f.fields.Len()
			if cap(f.fields) <= size {
				f.fields = append(f.fields, record.Field{})
			} else {
				f.fields = f.fields[:size+1]
			}

			f.fields[size].Name = cm.Name()
			f.fields[size].Type = int(cm.ty)
			f.schemaMap[cm.Name()] = struct{}{}
		}
	}
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
	position       int // if compactWithBlock this position is always 0
	segmentPostion int
	fileIdx        int // file id,record which block comes from which file
	sortKeyColums  []*record.ColVal
	sortKeyFields  []record.Field
	sortData       []record.SortItem
	curRec         *record.Record
	ctx            *ReadContext
}

func NewSortKeyIterator(fi *FileIterator, sortKeyFields []record.Field, ctx *ReadContext, schema record.Schemas, tcDuration time.Duration, compactWithBlock bool, fileIdx int) (*SortKeyIterator, error) {
	sortKeyColums, data, err := GetSortKeyColVal(fi, sortKeyFields, ctx, tcDuration, 0, compactWithBlock)
	if err != nil {
		return nil, err
	}
	itr := &SortKeyIterator{
		FileIterator:   fi,
		sortKeyColums:  sortKeyColums,
		sortKeyFields:  sortKeyFields,
		segmentPostion: 0,
		fileIdx:        fileIdx,
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

func GetSortKeyColVal(fi *FileIterator, sortKey []record.Field, ctx *ReadContext, tcDuration time.Duration, segPosition int, compactWithBlock bool) ([]*record.ColVal, []record.SortItem, error) {
	var sortKeyColums []*record.ColVal
	var data []record.SortItem
	var idx int
	var start int
	curtChunkMeta := fi.curtChunkMeta

	var segDataTime []byte
	if tcDuration > 0 {
		start++
		cm := curtChunkMeta.TimeMeta()
		colSeg := cm.entries[segPosition] // get first segment
		segData, err := fi.readData(colSeg.offset, colSeg.size)
		if err != nil {
			return nil, nil, err
		}
		segDataTime = append(segDataTime, segData...)
		tmpCol := &record.ColVal{}
		err = appendTimeColumnData(segData, tmpCol, ctx, false)
		if err != nil {
			return nil, nil, err
		}
		vals := tmpCol.IntegerValues()
		for i, t := range vals {
			vals[i] = int64(time.Duration(t).Truncate(tcDuration))
		}
		is := record.IntegerSlice{V: vals}
		data = append(data, &is)
		tmpCol.Val = util.Int64Slice2byte(vals)
		sortKeyColums = append(sortKeyColums, tmpCol)
	}

	var segData []byte
	var err error
	for _, key := range sortKey[start:] {
		idx = curtChunkMeta.columnIndex(&key)
		if idx < 0 {
			return nil, nil, errors.New("failed to get sort key colVal")
		}
		cm := curtChunkMeta.colMeta[idx]
		colSeg := cm.entries[segPosition] // get first segment
		if key.Name == record.TimeField && len(segDataTime) != 0 {
			segData = segDataTime
		} else {
			segData, err = fi.readData(colSeg.offset, colSeg.size)
			if err != nil {
				return nil, nil, err
			}
		}

		tmpCol := &record.ColVal{}
		if key.Name == record.TimeField {
			err = appendTimeColumnData(segData, tmpCol, ctx, false)
		} else {
			err = decodeColumnData(&key, segData, tmpCol, ctx, false)
		}
		if err != nil {
			return nil, nil, err
		}
		// the first row
		switch key.Type {
		case influx.Field_Type_Int:
			is := record.IntegerSlice{}
			if compactWithBlock {
				is.PadIntSliceWithLimit(tmpCol, blockCompareLength)
			} else {
				is.PadIntSlice(tmpCol)
			}
			data = append(data, &is)
		case influx.Field_Type_Float:
			fs := record.FloatSlice{}
			if compactWithBlock {
				fs.PadFloatSliceWithLimit(tmpCol, blockCompareLength)
			} else {
				fs.PadFloatSlice(tmpCol)
			}
			data = append(data, &fs)
		case influx.Field_Type_String:
			ss := record.StringSlice{}
			if compactWithBlock {
				ss.PadStringSliceWithLimit(tmpCol, blockCompareLength)
			} else {
				ss.PadStringSlice(tmpCol)
			}
			data = append(data, &ss)
		case influx.Field_Type_Boolean:
			bs := record.BooleanSlice{}
			if compactWithBlock {
				bs.PadBoolSliceWithLimit(tmpCol, blockCompareLength)
			} else {
				bs.PadBoolSlice(tmpCol)
			}
			data = append(data, &bs)
		}
		sortKeyColums = append(sortKeyColums, tmpCol)
	}
	return sortKeyColums, data, nil
}

func (s *SortKeyIterator) NextSingleFragment(tbStore *MmsTables, impl *IteratorByRow, pkSchema record.Schemas) (*record.Record, error) {
	var err error
	f := impl.f
	cMeta := s.curtChunkMeta
	limit := f.Conf.FragmentsNumPerFlush * f.Conf.maxRowsPerSegment
	if s.position < s.curRec.RowNums() {
		err = s.appendSingleFragment(tbStore, impl, pkSchema, cMeta, limit, s.sortKeyColums[0])
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
		tcVals := &record.ColVal{}
		length := s.curRec.RowNums()
		vals := make([]int64, length)
		copy(vals, s.curRec.TimeColumn().IntegerValues())
		for i, t := range vals {
			vals[i] = int64(time.Duration(t).Truncate(f.tcDuration))
		}
		tcVals.Val = util.Int64Slice2byte(vals)
		tcVals.Len = s.curRec.RowNums()
		tcVals.BitMapOffset = 0
		tcVals.FillBitmap(255)
		tcVals.RepairBitmap()
		if s.curRec.RowNums() == 0 {
			return f.RecordResult, nil
		}
		err = s.appendSingleFragment(tbStore, impl, pkSchema, cMeta, limit, tcVals)
		if err != nil {
			return nil, err
		}
		s.segmentPostion++
	}

	return f.RecordResult, nil
}

func (s *SortKeyIterator) appendSingleFragment(tbStore *MmsTables, impl *IteratorByRow, pkSchema record.Schemas, cMeta *ChunkMeta, limit int, tcVal *record.ColVal) error {
	var err error
	f := impl.f
	if f.recordResultNum+s.curRec.RowNums()-s.position < limit {
		f.RecordResult.AppendRec(s.curRec, s.position, s.curRec.RowNums())
		if f.tcDuration > 0 {
			f.TimeClusterResult.ColVals[0].AppendColVal(tcVal, influx.Field_Type_Int, s.position, s.curRec.RowNums())
		}
		f.recordResultNum += s.curRec.RowNums() - s.position
	} else {
		cutNum := limit - f.recordResultNum
		f.RecordResult.AppendRec(s.curRec, s.position, s.position+cutNum)
		if f.tcDuration > 0 {
			f.TimeClusterResult.ColVals[0].AppendColVal(tcVal, influx.Field_Type_Int, s.position, s.position+cutNum)
		}
		if err = impl.Flush(tbStore, pkSchema, false); err != nil {
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
		f.TimeClusterResult = resetResultRec(f.TimeClusterResult)
		// if start == end,func will return
		f.RecordResult.AppendRec(s.curRec, s.position+cutNum, s.curRec.RowNums())
		if f.tcDuration > 0 {
			f.TimeClusterResult.ColVals[0].AppendColVal(tcVal, influx.Field_Type_Int, s.position+cutNum, s.curRec.RowNums())
		}
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

func (s *SortKeyIterator) GetNewRecord(tcDuration time.Duration, compactWithBlock bool) error {
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

	sortKeyColums, data, err := GetSortKeyColVal(s.FileIterator, s.sortKeyFields, s.ctx, tcDuration, s.segmentPostion, compactWithBlock)
	if err != nil {
		return err
	}
	s.sortKeyColums = sortKeyColums
	s.sortData = data

	s.segmentPostion++
	return nil
}
