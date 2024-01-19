/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type MsBuilder struct {
	Path string
	TableData
	Conf            *Config
	chunkBuilder    *ChunkDataBuilder
	mIndex          MetaIndex
	trailer         *Trailer
	keys            map[uint64]struct{}
	bf              *bloom.Filter
	MaxIds          int
	currentCMOffset int
	blockSizeIndex  int
	dataOffset      int64
	localBFCount    int64 //the block count of a local bloomFilter file
	trailerOffset   int64
	fileSize        int64

	fd             fileops.File
	diskFileWriter fileops.FileWriter // encode data fileWriter
	metaFileWriter fileops.MetaWriter
	cmOffset       []uint32

	encodeChunk       []byte
	encChunkMeta      []byte
	encChunkIndexMeta []byte
	encIdTime         []byte
	chunkMetaBlocks   [][]byte
	timeSorted        bool //if timeField isn't the first sortKey, then set timeSorted false
	fullTextIdx       bool //whether write full text bloom filter index
	inited            bool

	pair      IdTimePairs
	sequencer *Sequencer
	msName    string // measurement name with version.
	lock      *string
	tier      uint64

	Files              []TSSPFile
	FileName           TSSPFileName
	log                *logger.Logger
	pkIndexWriter      sparseindex.PKIndexWriter
	pkRec              []*record.Record
	pkMark             []fragment.IndexFragment
	tcLocation         int8 // time cluster
	skipIndexWriter    sparseindex.SkipIndexWriter
	EncodeChunkDataImp EncodeChunkData
}

func NewMsBuilder(dir, name string, lockPath *string, conf *Config, idCount int, fileName TSSPFileName,
	tier uint64, sequencer *Sequencer, estimateSize int, engineType config.EngineType) *MsBuilder {
	msBuilder := genMsBuilder(dir, name, lockPath, conf, idCount, tier, sequencer, estimateSize, engineType)
	msBuilder.FileName = fileName

	lock := fileops.FileLockOption(*lockPath)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	dir = filepath.Join(dir, name)
	_ = fileops.MkdirAll(dir, 0750, lock)
	filePath := fileName.Path(dir, true)
	_, err := fileops.Stat(filePath)
	if err == nil {
		panic(fmt.Sprintf("file(%v) exist", filePath))
	}
	msBuilder.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		panic(err)
	}

	limit := fileName.level > 0
	if msBuilder.cacheMetaInMemory() {
		msBuilder.diskFileWriter = newTsspFileWriter(msBuilder.fd, true, limit, lockPath)
		n := idCount/DefaultMaxChunkMetaItemCount + 1
		msBuilder.inMemBlock.ReserveMetaBlock(n)
	} else {
		msBuilder.diskFileWriter = newTsspFileWriter(msBuilder.fd, false, limit, lockPath)
		if msBuilder.chunkBuilder.chunkMeta == nil {
			msBuilder.chunkBuilder.chunkMeta = &ChunkMeta{}
		}
	}

	return msBuilder
}

func NewDetachedMsBuilder(dir, name string, lockPath *string, conf *Config, idCount int, fileName TSSPFileName,
	tier uint64, sequencer *Sequencer, estimateSize int, engineType config.EngineType, obsOpt *obs.ObsOptions, bfCols []string, fullTextIdx bool) (*MsBuilder, error) {
	msBuilder := genMsBuilder(dir, name, lockPath, conf, idCount, tier, sequencer, estimateSize, engineType)

	msBuilder.FileName = fileName
	filePath := filepath.Join(dir, name)
	if obsOpt != nil {
		filePath = filePath[len(GetPrefixDataPath()):]
	}
	fd, err := OpenObsFile(filePath, DataFile, obsOpt)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		return nil, err
	}
	msBuilder.fd = fd
	msBuilder.diskFileWriter, err = newObsFileWriter(fd, filePath, obsOpt)
	if err != nil {
		log.Error("create obsFileWriter failed", zap.Error(err))
		return nil, err
	}

	msBuilder.metaFileWriter, err = newObsIndexFileWriter(filePath, obsOpt, bfCols, fullTextIdx)
	if err != nil {
		log.Error("create newObsIndexFileWriter failed", zap.Error(err))
		return nil, err
	}

	return msBuilder, nil
}

func genMsBuilder(dir, name string, lockPath *string, conf *Config, idCount int,
	tier uint64, sequencer *Sequencer, estimateSize int, engineType config.EngineType) *MsBuilder {
	msBuilder := &MsBuilder{}
	msBuilder.chunkBuilder = NewChunkDataBuilder(conf.maxRowsPerSegment, conf.maxSegmentLimit)
	msBuilder.SetEncodeChunkDataImp(engineType)
	if msBuilder.trailer == nil {
		msBuilder.trailer = &Trailer{}
	}

	msBuilder.log = logger.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	msBuilder.WithLog(msBuilder.log)
	msBuilder.tier = tier
	msBuilder.Conf = conf
	msBuilder.lock = lockPath
	msBuilder.timeSorted = true
	msBuilder.trailer.name = append(msBuilder.trailer.name[:0], influx.GetOriginMstName(name)...)
	msBuilder.Path = dir
	msBuilder.inMemBlock = emptyMemReader
	if msBuilder.cacheDataInMemory() || msBuilder.cacheMetaInMemory() {
		idx := calcBlockIndex(estimateSize)
		msBuilder.inMemBlock = NewMemoryReader(blockSize[idx])
	}

	msBuilder.keys = make(map[uint64]struct{}, 256)
	msBuilder.sequencer = sequencer
	msBuilder.msName = name
	msBuilder.Files = msBuilder.Files[:0]
	msBuilder.MaxIds = idCount
	msBuilder.bf = nil
	msBuilder.tcLocation = colstore.DefaultTCLocation
	return msBuilder
}

func (b *MsBuilder) SetFullTextIdx(fullTextIdx bool) {
	b.fullTextIdx = fullTextIdx
}

func (b *MsBuilder) GetFullTextIdx() bool {
	return b.fullTextIdx
}

func (b *MsBuilder) StoreTimes() {
	b.trailer.SetData(IndexOfTimeStoreFlag, TimeStoreFlag)
}

func (b *MsBuilder) MaxRowsPerSegment() int {
	return b.Conf.maxRowsPerSegment
}

func (b *MsBuilder) GetChunkBuilder() *ChunkDataBuilder {
	return b.chunkBuilder
}

func (b *MsBuilder) SetEncodeChunkDataImp(engineType config.EngineType) {
	if engineType == config.TSSTORE {
		b.EncodeChunkDataImp = &TsChunkDataImp{}
	} else if engineType == config.COLUMNSTORE {
		b.EncodeChunkDataImp = &CsChunkDataImp{}
	}
}

func (b *MsBuilder) WithLog(log *logger.Logger) {
	b.log = log
	if b.chunkBuilder != nil {
		b.chunkBuilder.log = log
		if b.chunkBuilder.colBuilder != nil {
			b.chunkBuilder.colBuilder.log = log
		}
	}
}

func (b *MsBuilder) SetLocalBfCount(count int64) {
	b.localBFCount = count
}

func (b *MsBuilder) GetLocalBfCount() int64 {
	return b.localBFCount
}

func (b *MsBuilder) Reset() {
	b.reset()

	b.dataOffset = 0
	b.mIndex.reset()
	b.trailer.reset()
	b.Path = ""
	b.releaseEncoders()
	b.encodeChunk = b.encodeChunk[:0]
	b.inited = false
	if b.diskFileWriter != nil {
		_ = b.diskFileWriter.Close()
		b.diskFileWriter = nil
	}
	if b.fd != nil {
		_ = b.fd.Close()
		b.fd = nil
	}
	b.fileSize = 0
	b.Files = b.Files[:0]
}

func (b *MsBuilder) writeToDisk(rowCounts int64) error {
	wn, err := b.diskFileWriter.WriteData(b.encodeChunk)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk data fail", zap.Error(err))
		return err
	}
	if wn != len(b.encodeChunk) {
		b.log.Error("write chunk data fail", zap.String("file", b.fd.Name()),
			zap.Ints("size", []int{len(b.encodeChunk), wn}))
		return io.ErrShortWrite
	}

	cm := b.chunkBuilder.chunkMeta
	minT, maxT := b.chunkBuilder.getMinMaxTime(b.timeSorted)
	if b.mIndex.count == 0 {
		b.mIndex.size = 0
		b.mIndex.id = cm.sid
		b.mIndex.minTime = minT
		b.mIndex.maxTime = maxT
		b.mIndex.offset = b.diskFileWriter.ChunkMetaSize()
		b.currentCMOffset = 0
	}

	b.pair.Add(cm.sid, maxT)
	b.pair.AddRowCounts(rowCounts)

	_, err = b.WriteChunkMeta(cm)
	if err != nil {
		return err
	}

	b.mIndex.count++
	if b.mIndex.minTime > minT {
		b.mIndex.minTime = minT
	}
	if b.mIndex.maxTime < maxT {
		b.mIndex.maxTime = maxT
	}

	if needSwitchChunkMeta(b.Conf, int(b.diskFileWriter.ChunkMetaBlockSize()), int(b.mIndex.count)) {
		if err := b.SwitchChunkMeta(); err != nil {
			return err
		}
	}

	return nil
}

func needSwitchChunkMeta(conf *Config, size int, count int) bool {
	maxCount := conf.maxChunkMetaItemCount
	if GetChunkMetaCompressMode() != ChunkMetaCompressNone {
		maxCount = CompressModMaxChunkMetaItemCount
	}

	return size >= conf.maxChunkMetaItemSize || count >= maxCount
}

func switchTsspFile(msb *MsBuilder, rec, totalRec *record.Record, rowsLimit int, fSize int64,
	nextFile func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16), engineType config.EngineType) (*MsBuilder, error) {
	f, err := msb.NewTSSPFile(true)
	if err != nil {
		msb.log.Error("new file fail", zap.String("file", msb.fd.Name()), zap.Error(err))
		return msb, err
	}

	msb.log.Info("switch tssp file",
		zap.String("file", f.Path()),
		zap.Int("rowsLimit", rowsLimit),
		zap.Int("rows", rec.RowNums()),
		zap.Int("totalRows", totalRec.RowNums()),
		zap.Int64("fileSize", fSize),
		zap.Int64("sizeLimit", msb.Conf.fileSizeLimit))

	msb.Files = append(msb.Files, f)
	seq, lv, merge, ext := nextFile(msb.FileName)
	msb.FileName.SetSeq(seq)
	msb.FileName.SetMerge(merge)
	msb.FileName.SetExtend(ext)
	msb.FileName.SetLevel(lv)

	builder := NewMsBuilder(msb.Path, msb.Name(), msb.lock, msb.Conf, msb.MaxIds, msb.FileName, msb.tier, msb.sequencer, rec.Len(), engineType)
	builder.Files = append(builder.Files, msb.Files...)
	builder.pkRec = append(builder.pkRec, msb.pkRec...)
	builder.pkMark = append(builder.pkMark, msb.pkMark...)
	builder.tcLocation = msb.tcLocation
	builder.timeSorted = msb.timeSorted
	builder.WithLog(msb.log)
	return builder, nil
}

func (b *MsBuilder) NewPKIndexWriter() {
	b.pkIndexWriter = sparseindex.NewPKIndexWriter()
}

func (b *MsBuilder) SetTCLocation(tcLocation int8) {
	b.tcLocation = tcLocation
}

func (b *MsBuilder) SetTimeSorted(timeSorted bool) {
	b.timeSorted = timeSorted
}

func (b *MsBuilder) writePrimaryIndex(writeRec *record.Record, pkSchema record.Schemas, filepath, lockpath string, tcLocation int8, rowsPerSegment []int, fixRowsPerSegment int) error {
	// Generate the primary key record from the sorted chunk based on the primary key.
	pkRec, pkMark, err := b.pkIndexWriter.Build(writeRec, pkSchema, rowsPerSegment, tcLocation, fixRowsPerSegment)
	if err != nil {
		return err
	}
	indexBuilder := colstore.NewIndexBuilder(&lockpath, filepath)
	err = indexBuilder.WriteData(pkRec, tcLocation)
	defer indexBuilder.Reset()
	if err != nil {
		return err
	}
	b.pkRec = append(b.pkRec, pkRec)
	b.pkMark = append(b.pkMark, pkMark)

	return nil
}

func (b *MsBuilder) genDetachedPrimaryIndex(writeRec *record.Record, pkSchema record.Schemas, firstFlush bool,
	tcLocation int8, rowsPerSegment []int, fixRowsPerSegment int, accumulateMetaIndex *AccumulateMetaIndex) error {
	pkRec, _, err := b.pkIndexWriter.Build(writeRec, pkSchema, rowsPerSegment, tcLocation, fixRowsPerSegment)
	if err != nil {
		return err
	}

	return b.writeDetachedPrimaryIndex(firstFlush, pkRec, tcLocation, uint64(len(rowsPerSegment)), accumulateMetaIndex)
}

func (b *MsBuilder) writeDetachedPrimaryIndex(firstFlush bool, pkRec *record.Record, tcLocation int8, memSegmentCount uint64,
	accumulateMetaIndex *AccumulateMetaIndex) error {
	lockPath := ""
	indexBuilder := colstore.NewIndexBuilderByFd(&lockPath, b.metaFileWriter.GetPrimaryKeyHandler(), firstFlush)
	err := indexBuilder.WriteDetachedData(pkRec, tcLocation)
	if err != nil {
		return err
	}

	size := indexBuilder.GetEncodeChunkSize()
	if firstFlush {
		//first time pkData offset start after by primaryKeyHeader, first time pkData size not include primaryKeyHeader
		accumulateMetaIndex.pkDataOffset += primaryKeyHeaderSize
		size -= primaryKeyHeaderSize
	}

	err = indexBuilder.WriteDetachedMeta(accumulateMetaIndex.blockId, accumulateMetaIndex.blockId+memSegmentCount,
		accumulateMetaIndex.pkDataOffset, size, b.metaFileWriter.GetPrimaryKeyMetaHandler())
	accumulateMetaIndex.pkDataOffset += size
	indexBuilder.Reset()
	return err
}

func (b *MsBuilder) NewSkipIndexWriter() {
	b.skipIndexWriter = sparseindex.NewSkipIndexWriter(colstore.BloomFilterIndex)
}

func (b *MsBuilder) writeSkipIndex(writeRec *record.Record, schemaIndex []int, dataFilePath, lockpath string, rowsPerSegment []int, detached bool) error {
	var skipIndexFilePath string
	var data []byte
	var err error
	if b.fullTextIdx {
		memBfData := logstore.GetBloomFilterBuf(sparseindex.FullTextIdxColumnCnt)
		defer logstore.PutBloomFilterBuf(memBfData)
		skipIndexFilePath = b.getFullTextIdxFilePath()[0]
		skipIndexFilePath += tmpFileSuffix
		*memBfData, err = b.getMemoryFullTextIdxData(*memBfData, writeRec, schemaIndex, rowsPerSegment)
		if err != nil {
			return err
		}

		err = writeSkipIndexToDisk((*memBfData)[0], lockpath, skipIndexFilePath)
		if err != nil {
			return err
		}
	} else {
		for _, i := range schemaIndex {
			skipIndexFilePath = b.getSkipIndexFilePath(dataFilePath, writeRec.Schema[i].Name, detached)
			data, err = b.skipIndexWriter.CreateSkipIndex(&writeRec.ColVals[i], rowsPerSegment, writeRec.Schema[i].Type)
			if err != nil {
				return err
			}

			err = writeSkipIndexToDisk(data, lockpath, skipIndexFilePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func writeSkipIndexToDisk(data []byte, lockPath, skipIndexFilePath string) error {
	indexBuilder := colstore.NewSkipIndexBuilder(&lockPath, skipIndexFilePath)
	err := indexBuilder.WriteData(data)
	defer indexBuilder.Reset()
	return err
}

func (b *MsBuilder) getSkipIndexFilePath(dataFilePath, fieldName string, detached bool) string {
	if detached {
		return sparseindex.GetBloomFilterFilePath(b.Path, b.msName, fieldName)
	}
	return path.Join(b.Path, b.msName, colstore.AppendSKIndexSuffix(dataFilePath, fieldName, colstore.BloomFilterIndex)+tmpFileSuffix)
}

func (b *MsBuilder) genAccumulateRowsIndex(data *record.Record, skipIndexRelation *influxql.IndexRelation) ([]int, []int) {
	schemaIdx := logstore.GenSchemaIdxs(data.Schema, skipIndexRelation, b.fullTextIdx)
	if len(schemaIdx) == 0 {
		return nil, nil
	}
	accumulateRowsIndex := b.splitRecord(data, schemaIdx)
	if len(accumulateRowsIndex) == 0 {
		accumulateRowsIndex = append(accumulateRowsIndex, data.RowNums()-1)
	}
	return accumulateRowsIndex, schemaIdx
}

func (b *MsBuilder) splitRecord(data *record.Record, skipIndexSchema []int) []int {
	res := make([]int, 0)
	var idx, minIdx int
	start, targetCount := 0, uint32(1)
	for start < data.RowNums()-1 { // final start = data.RowNums()-1, then should break
		minIdx = math.MaxInt
		for _, i := range skipIndexSchema {
			idx = LeftBound(data.ColVals[i].Offset, b.Conf.expectedSegmentSize*targetCount, start)
			//one single row data size > b.Conf.expectedSegmentSize
			if len(res) > 0 && idx == res[len(res)-1] {
				continue
			}
			if idx < minIdx {
				minIdx = idx
			}
		}
		res = append(res, minIdx)
		start = minIdx
		targetCount++
	}
	return res
}

func LeftBound(nums []uint32, target uint32, left int) int {
	preIdx := left
	right := len(nums)
	for left < right {
		mid := left + (right-left)/2
		if nums[mid] >= target {
			right = mid
		} else {
			left = mid + 1
		}
	}

	if left >= len(nums) {
		left = len(nums) - 1
	}

	if left == preIdx && left != len(nums)-1 {
		left++
	}

	return left
}

func removeClusteredTimeCol(data *record.Record) {
	data.ColVals = data.ColVals[1:]
	data.Schema = data.Schema[1:]
}

func (b *MsBuilder) WriteRecordByCol(id uint64, data *record.Record, schema record.Schemas, skipIndexRelation *influxql.IndexRelation,
	nextFile func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16)) (*MsBuilder, error) {
	rowsLimit := b.Conf.maxRowsPerSegment * b.Conf.maxSegmentLimit
	var accumulateRowsIndex, schemaIdx []int
	fixRowsPerSegment := b.Conf.maxRowsPerSegment
	b.EncodeChunkDataImp.SetDetachedInfo(false)
	// fast path, most data does not reach the threshold for splitting files.
	if data.RowNums() <= rowsLimit {
		accumulateRowsIndex, schemaIdx, fixRowsPerSegment = b.getAccumulateRowsIndex(data, skipIndexRelation, fixRowsPerSegment)
		err := b.writeIndex(data, schema, skipIndexRelation, accumulateRowsIndex, schemaIdx, fixRowsPerSegment)
		if err != nil {
			return b, err
		}

		if err = b.WriteData(id, data); err != nil {
			b.log.Error("write data record fail", zap.String("file", b.fd.Name()), zap.Error(err))
			return b, err
		}

		fSize := b.Size()
		if fSize < b.Conf.fileSizeLimit || nextFile == nil {
			return b, nil
		}

		return switchTsspFile(b, data, data, rowsLimit, fSize, nextFile, config.COLUMNSTORE)
	}

	// slow path
	recs := data.Split(nil, rowsLimit)

	for i := range recs {
		accumulateRowsIndex, schemaIdx, fixRowsPerSegment = b.getAccumulateRowsIndex(&recs[i], skipIndexRelation, fixRowsPerSegment)
		err := b.writeIndex(&recs[i], schema, skipIndexRelation, accumulateRowsIndex, schemaIdx, fixRowsPerSegment)
		if err != nil {
			return b, err
		}
		err = b.WriteData(id, &recs[i])
		if err != nil {
			b.log.Error("write data record fail", zap.String("file", b.fd.Name()), zap.Error(err))
			return b, err
		}

		fSize := b.Size()
		if (i < len(recs)-1 || fSize >= b.Conf.fileSizeLimit) && nextFile != nil {
			b, err = switchTsspFile(b, &recs[i], data, rowsLimit, fSize, nextFile, config.COLUMNSTORE)
			if err != nil {
				return b, err
			}
			if len(schema) != 0 || b.tcLocation > colstore.DefaultTCLocation { // need to init indexwriter after switch tssp file, i.e. new b
				b.NewPKIndexWriter()
			}
			if skipIndexRelation != nil && len(skipIndexRelation.Oids) != 0 {
				b.NewSkipIndexWriter()
			}
		}
	}
	return b, nil
}

func (b *MsBuilder) WriteDetached(id uint64, data *record.Record, pkSchema record.Schemas, skipIndexRelation *influxql.IndexRelation,
	firstFlush bool, accumulateMetaIndex *AccumulateMetaIndex) error {
	record.CheckRecord(data)
	var accumulateRowsIndex, schemaIdx []int
	fixRowsPerSegment := b.Conf.maxRowsPerSegment
	b.EncodeChunkDataImp.SetDetachedInfo(true)
	accumulateRowsIndex, schemaIdx, fixRowsPerSegment = b.getAccumulateRowsIndex(data, skipIndexRelation, fixRowsPerSegment)
	//write detached data
	err := b.writeDetachedData(id, data, firstFlush, accumulateMetaIndex.dataOffset)
	if err != nil {
		return err
	}
	//write detached meta
	return b.WriteDetachedMetaAndIndex(data, pkSchema, firstFlush, accumulateMetaIndex, accumulateRowsIndex, schemaIdx, fixRowsPerSegment)
}

func (b *MsBuilder) writeDetachedData(id uint64, data *record.Record, firstFlush bool, dataOffset int64) error {
	b.encodeChunk = b.encodeChunk[:0]
	b.initDetachedBuilder(data.ColNums(), firstFlush, true, dataOffset)
	var err error
	//encode data
	b.encodeChunk, err = b.EncodeChunkDataImp.EncodeChunk(b.chunkBuilder, id, b.dataOffset, data, b.encodeChunk, b.timeSorted)
	if err != nil {
		b.log.Error("encode chunk fail", zap.Error(err))
		return err
	}
	//write data
	return b.writeDataToDisk()
}

func (b *MsBuilder) writeDataToDisk() error {
	b.dataOffset += int64(b.chunkBuilder.chunkMeta.size - b.chunkBuilder.preChunkMetaSize)
	// write data to disk
	wn, err := b.diskFileWriter.WriteData(b.encodeChunk)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk data fail", zap.Error(err))
		return err
	}
	if wn != len(b.encodeChunk) {
		b.log.Error("write chunk data fail", zap.String("file", b.fd.Name()),
			zap.Ints("size", []int{len(b.encodeChunk), wn}))
		return io.ErrShortWrite
	}
	return nil
}

func (b *MsBuilder) initDetachedBuilder(columnCount int, firstFlush, toRemote bool, dataOffset int64) {
	if !b.inited {
		b.pair.Reset(b.msName)
		b.inited = true
		b.cmOffset = b.cmOffset[:0]
		// block compaction accumulateRowsIndex length must greater than 0
		if !toRemote || firstFlush {
			// row compaction  or first block compaction
			b.encodeChunk = append(b.encodeChunk, tableMagic...)
			b.encodeChunk = numberenc.MarshalUint64Append(b.encodeChunk, version)
			b.dataOffset = int64(len(b.encodeChunk))
		} else {
			// remain block compaction task
			b.dataOffset = dataOffset
		}

		if b.chunkBuilder.chunkMeta == nil {
			b.chunkBuilder.chunkMeta = &ChunkMeta{}
		}

		b.chunkBuilder.chunkMeta.offset = b.dataOffset
		b.chunkBuilder.chunkMeta.columnCount = uint32(columnCount)
		b.mIndex.size = 0
		b.mIndex.offset = 0
		b.mIndex.minTime = math.MaxInt64
		b.mIndex.maxTime = math.MinInt64
	}
}

func (b *MsBuilder) WriteDetachedMetaAndIndex(writeRec *record.Record, pkSchema record.Schemas, firstFlush bool,
	accumulateMetaIndex *AccumulateMetaIndex, rowsPerSegment, schemaIdx []int, fixRowsPerSegment int) error {
	var err error
	err = b.writeDetachedChunkMeta(writeRec, firstFlush, accumulateMetaIndex.blockId)
	if err != nil {
		return err
	}
	err = b.writeDetachedBloomFilter(writeRec, schemaIdx, rowsPerSegment)
	if err != nil {
		return err
	}

	err = b.genDetachedPrimaryIndex(writeRec, pkSchema, firstFlush, b.tcLocation, rowsPerSegment, fixRowsPerSegment, accumulateMetaIndex)
	if err != nil {
		return err
	}
	err = b.writeDetachedMetaIndex(firstFlush, accumulateMetaIndex)
	if err != nil {
		return err
	}
	err = b.closeFdWrite()
	if err != nil {
		return err
	}
	b.updateAccumulateMetaIndex(uint64(len(rowsPerSegment)), accumulateMetaIndex)
	return nil
}

func (b *MsBuilder) updateAccumulateMetaIndex(blockCount uint64, accumulateMetaIndex *AccumulateMetaIndex) {
	accumulateMetaIndex.blockId += blockCount
	accumulateMetaIndex.dataOffset = b.dataOffset
	accumulateMetaIndex.offset += int64(b.mIndex.size)
}

func (b *MsBuilder) closeFdWrite() error {
	var err error
	if b.diskFileWriter != nil {
		err = b.diskFileWriter.Close()
		b.diskFileWriter = nil
		if err != nil {
			return err
		}
	}

	if b.skipIndexWriter != nil {
		err = b.skipIndexWriter.Close()
		b.skipIndexWriter = nil
		if err != nil {
			return err
		}
	}

	if b.metaFileWriter != nil {
		err = b.metaFileWriter.Close()
		b.metaFileWriter = nil
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *MsBuilder) writeDetachedBloomFilter(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	if len(schemaIdx) == 0 {
		return nil
	}

	var cols int
	if b.fullTextIdx {
		cols = sparseindex.FullTextIdxColumnCnt
	} else {
		cols = len(schemaIdx)
	}
	memBfData := logstore.GetBloomFilterBuf(cols)
	defer logstore.PutBloomFilterBuf(memBfData)

	var skipIndexFilePaths []string
	var err error
	var filterDetachedWriteTimes int
	if b.fullTextIdx {
		*memBfData, err = b.getMemoryFullTextIdxData(*memBfData, writeRec, schemaIdx, rowsPerSegment)
		if err != nil {
			return err
		}
		skipIndexFilePaths = b.getFullTextIdxFilePath()
	} else {
		*memBfData, err = b.getMemoryBloomFilterData(writeRec, schemaIdx, rowsPerSegment)
		if err != nil {
			return err
		}
		skipIndexFilePaths = b.getSkipIndexFilePaths(writeRec.Schema, schemaIdx, true, "")
	}
	filterDetachedWriteTimes = (len(rowsPerSegment) + int(b.localBFCount)) / int(logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterCntPerVerticalGorup)
	if b.BloomFilterNeedDetached(filterDetachedWriteTimes) {
		// local bf files and memory blocks satisfied the dump quantity,then write to remote
		*memBfData, err = b.detachBloomFilter(*memBfData, skipIndexFilePaths, filterDetachedWriteTimes)
		if err != nil {
			return err
		}
	}

	// bloomFilter blocks dissatisfied the dump quantity,then write to local
	err = b.writeMemoryBloomFilterData(*memBfData, skipIndexFilePaths, *b.lock)
	if err != nil {
		logger.GetLogger().Error("write skip index file failed", zap.String("mstName", b.msName), zap.Error(err))
	}
	return err
}

func (b *MsBuilder) detachBloomFilter(memBfData [][]byte, skipIndexFilePaths []string, filterDetachedWriteTimes int) ([][]byte, error) {
	//read local bf by col
	localBfData, err := b.getLocalBloomFilterData(skipIndexFilePaths)
	if err != nil {
		return nil, err
	}
	// row to col
	memBfData, err = b.writeVerticalFilter(localBfData, memBfData, filterDetachedWriteTimes)
	if err != nil {
		return nil, err
	}
	//clear local file
	return memBfData, b.clearLocalFile(skipIndexFilePaths)
}

func (b *MsBuilder) clearLocalFile(skipIndexFilePaths []string) error {
	for i := range skipIndexFilePaths {
		err := func() error {
			lock := fileops.FileLockOption(*b.lock)
			pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
			fd, err := fileops.OpenFile(skipIndexFilePaths[i], os.O_CREATE|os.O_RDWR, 0640, lock, pri)
			defer func() {
				_ = fd.Close()
			}()
			if err != nil {
				log.Error("open clear file fail", zap.String("name", skipIndexFilePaths[i]), zap.Error(err))
				panic(err)
			}
			err = fd.Truncate(0)
			log.Warn("truncate local bloom filter file", zap.Int("truncate start position is", 0))
			return err
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *MsBuilder) getSkipIndexFilePaths(schema record.Schemas, schemaIndex []int, detached bool, dataFilePath string) []string {
	skipIndexFilePaths := make([]string, len(schemaIndex))
	for k, v := range schemaIndex {
		skipIndexFilePaths[k] = b.getSkipIndexFilePath(dataFilePath, schema[v].Name, detached)
	}
	return skipIndexFilePaths
}

func (b *MsBuilder) getFullTextIdxFilePath() []string {
	var skipIndexFilePaths []string
	skipIndexFilePaths = append(skipIndexFilePaths, sparseindex.GetFullTextIdxFilePath(b.Path, b.msName))
	return skipIndexFilePaths
}

func (b *MsBuilder) writeMemoryBloomFilterData(memBfData [][]byte, skipIndexFilePaths []string, lockPath string) error {
	var err error
	for i := range skipIndexFilePaths {
		if len(memBfData[i]) == 0 {
			continue
		}
		err = writeSkipIndexToDisk(memBfData[i], lockPath, skipIndexFilePaths[i])
		if err != nil {
			return err
		}
	}

	// len(memBfData) must be greater than 0
	if len(memBfData[0]) == 0 {
		b.localBFCount = 0
	} else {
		b.localBFCount = int64(len(memBfData[0])) / logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize
	}
	return nil
}

func (b *MsBuilder) writeVerticalFilter(localBfData, memBfData [][]byte, filterDetachedWriteTimes int) ([][]byte, error) {
	filterVerBuffer := bfBuffPool.Get()
	defer bfBuffPool.Put(filterVerBuffer)
	constant := logstore.GetConstant(logstore.CurrentLogTokenizerVersion)
	var err error
	for k := 0; k < filterDetachedWriteTimes; k++ {
		//get specified size of memBfData to flush vertical filter
		concatSize := (constant.FilterCntPerVerticalGorup - b.localBFCount) * constant.FilterDataDiskSize
		for i := range memBfData {
			concatBuf := memBfData[i][:concatSize]
			//fast path
			if len(localBfData) == 0 || len(localBfData[i]) == 0 {
				filterVerBuffer = logstore.FlushVerticalFilter(filterVerBuffer[:0], concatBuf)
			} else {
				localBfData[i] = append(localBfData[i], concatBuf...)
				filterVerBuffer = logstore.FlushVerticalFilter(filterVerBuffer[:0], localBfData[i])
				localBfData[i] = localBfData[i][:0]
			}
			_, err = b.metaFileWriter.WriteBloomFilter(i, filterVerBuffer)
			if err != nil {
				b.log.Error("write vertical bloom filter fail", zap.Error(err))
				return nil, err
			}
			filterVerBuffer = filterVerBuffer[:0]

			//memBfData still remain data
			if len(memBfData[i]) == int(concatSize) {
				memBfData[i] = memBfData[i][:0]
			} else {
				memBfData[i] = memBfData[i][concatSize:]
			}
		}
		b.localBFCount = 0
	}
	return memBfData, nil
}

func (b *MsBuilder) getLocalBloomFilterData(skipIndexFilePaths []string) ([][]byte, error) {
	if b.localBFCount == 0 {
		return nil, nil
	}
	localBfData := make([][]byte, len(skipIndexFilePaths))
	var bfReader fileops.BasicFileReader
	bfi := BloomFilterIterator{
		constant: logstore.GetConstant(logstore.CurrentLogTokenizerVersion),
	}
	bfBuf := make([]byte, 0, b.localBFCount*bfi.constant.FilterDataDiskSize)
	var err error
	for i := range skipIndexFilePaths {
		bfBuf = bfBuf[:0]
		bfReader, err = bfi.newBloomFilterReader(skipIndexFilePaths[i])
		if err != nil {
			return nil, err
		}
		_, err = bfReader.ReadAt(0, uint32(b.localBFCount*bfi.constant.FilterDataDiskSize), &bfBuf, fileops.IO_PRIORITY_ULTRA_HIGH)
		if err != nil {
			_ = bfReader.Close()
			log.Error("read local bloomFilter file  failed", zap.String("file", skipIndexFilePaths[i]), zap.Error(err))
			return nil, err
		}
		localBfData[i] = append(localBfData[i], bfBuf...)
	}
	return localBfData, nil
}

func (b *MsBuilder) getMemoryBloomFilterData(writeRec *record.Record, schemaIdx, rowsPerSegment []int) ([][]byte, error) {
	bfCols := make([][]byte, len(schemaIdx))
	for k, v := range schemaIdx {
		data, err := b.skipIndexWriter.CreateSkipIndex(&writeRec.ColVals[v], rowsPerSegment, writeRec.Schema[v].Type)
		if err != nil {
			return nil, err
		}
		bfCols[k] = append(bfCols[k], data...)
	}
	return bfCols, nil
}

func (b *MsBuilder) getMemoryFullTextIdxData(memBfData [][]byte, writeRec *record.Record, schemaIdx, rowsPerSegment []int) ([][]byte, error) {
	data := b.skipIndexWriter.CreateFullTextIndex(writeRec, schemaIdx, rowsPerSegment)
	memBfData[0] = append(memBfData[0], data...)
	return memBfData, nil
}

func (b *MsBuilder) BloomFilterNeedDetached(filterDetachedWriteTimes int) bool {
	return filterDetachedWriteTimes >= 1
}

func (b *MsBuilder) writeDetachedChunkMeta(data *record.Record, firstFlush bool, blockId uint64) error {
	b.encChunkMeta = b.encChunkMeta[:0]
	if firstFlush {
		b.encChunkMeta = append(b.encChunkMeta, tableMagic...)
		b.encChunkMeta = numberenc.MarshalUint64Append(b.encChunkMeta, version)
	}

	cm := b.chunkBuilder.chunkMeta
	_, maxT := b.chunkBuilder.getMinMaxTime(b.timeSorted)
	b.pair.Add(0, maxT)
	b.pair.AddRowCounts(int64(data.RowNums()))
	b.keys[0] = struct{}{}
	cm.sid = blockId
	pos := len(b.encChunkMeta)
	// reserve crc32
	b.encChunkMeta = numberenc.MarshalUint32Append(b.encChunkMeta, 0)
	b.encChunkMeta = cm.marshal(b.encChunkMeta)
	crc := crc32.ChecksumIEEE(b.encChunkMeta[pos+crcSize:])
	numberenc.MarshalUint32Copy(b.encChunkMeta[pos:pos+crcSize], crc)

	wn, err := b.diskFileWriter.WriteChunkMeta(b.encChunkMeta)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk meta fail", zap.Error(err))
		return err
	}
	if wn != len(b.encChunkMeta) {
		b.log.Error("write chunk meta fail", zap.String("file", b.fd.Name()), zap.Ints("size", []int{len(b.encChunkMeta), wn}))
		return io.ErrShortWrite
	}
	return nil
}

func (b *MsBuilder) writeDetachedMetaIndex(firstFlush bool, accumulateMetaIndex *AccumulateMetaIndex) error {
	b.encChunkIndexMeta = b.encChunkIndexMeta[:0]
	b.mIndex.size += uint32(len(b.encChunkMeta))
	if firstFlush {
		b.encChunkIndexMeta = append(b.encChunkIndexMeta, tableMagic...)
		b.encChunkIndexMeta = numberenc.MarshalUint64Append(b.encChunkIndexMeta, version)
		accumulateMetaIndex.offset = int64(fileHeaderSize) //metaIndex file include tableMagic and version
		b.mIndex.size -= uint32(fileHeaderSize)            //the first time flush encChunkMeta include tableMagic and version
	}
	b.mIndex.offset = accumulateMetaIndex.offset
	b.mIndex.id = accumulateMetaIndex.blockId

	minT, maxT := b.chunkBuilder.getMinMaxTime(b.timeSorted)
	b.mIndex.count++
	if b.mIndex.minTime > minT {
		b.mIndex.minTime = minT
	}
	if b.mIndex.maxTime < maxT {
		b.mIndex.maxTime = maxT
	}

	pos := len(b.encChunkIndexMeta)
	// reserve crc32
	b.encChunkIndexMeta = numberenc.MarshalUint32Append(b.encChunkIndexMeta, 0)
	b.encChunkIndexMeta = b.mIndex.marshalDetached(b.encChunkIndexMeta)
	crc := crc32.ChecksumIEEE(b.encChunkIndexMeta[pos+crcSize:])
	numberenc.MarshalUint32Copy(b.encChunkIndexMeta[pos:pos+crcSize], crc)
	_, err := b.metaFileWriter.WriteMetaIndex(b.encChunkIndexMeta)
	if err != nil {
		b.log.Error("write meta index fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}
	return nil
}

func (b *MsBuilder) writeIndex(writeRecord *record.Record, schema record.Schemas, skipIndexRelation *influxql.IndexRelation,
	rowsPerSegment, schemaIdx []int, fixRowsPerSegment int) error {
	if len(schema) != 0 || b.tcLocation > colstore.DefaultTCLocation { // write index, works for colstore
		dataFilePath := b.FileName.String()
		indexFilePath := path.Join(b.Path, b.msName, colstore.AppendPKIndexSuffix(dataFilePath)+tmpFileSuffix)
		if err := b.writePrimaryIndex(writeRecord, schema, indexFilePath, *b.lock, b.tcLocation, rowsPerSegment, fixRowsPerSegment); err != nil {
			logger.GetLogger().Error("write primary key file failed", zap.String("mstName", b.msName), zap.Error(err))
			return err
		}
	}

	//write skip index, works for colStore
	if b.fullTextIdx || (skipIndexRelation != nil && len(skipIndexRelation.IndexNames) != 0) {
		dataFilePath := b.FileName.String()
		if err := b.writeSkipIndex(writeRecord, schemaIdx, dataFilePath, *b.lock, rowsPerSegment, false); err != nil {
			logger.GetLogger().Error("write skip index file failed", zap.String("mstName", b.msName), zap.Error(err))
			return err
		}
	}

	if b.tcLocation > colstore.DefaultTCLocation {
		removeClusteredTimeCol(writeRecord)
	}
	return nil
}

func (b *MsBuilder) getAccumulateRowsIndex(data *record.Record, skipIndexRelation *influxql.IndexRelation, fixRowsPerSegment int) ([]int, []int, int) {
	var accumulateRowsIndex, schemaIdx []int
	if b.fullTextIdx || (skipIndexRelation != nil && len(skipIndexRelation.IndexNames) != 0) {
		accumulateRowsIndex, schemaIdx = b.genAccumulateRowsIndex(data, skipIndexRelation)
		//There is no bf columns
		if len(schemaIdx) == 0 {
			accumulateRowsIndex = GenFixRowsPerSegment(data, b.Conf.maxRowsPerSegment)
		} else {
			fixRowsPerSegment = 0
		}
	} else {
		accumulateRowsIndex = GenFixRowsPerSegment(data, b.Conf.maxRowsPerSegment)
	}
	b.EncodeChunkDataImp.SetAccumulateRowsIndex(accumulateRowsIndex)
	return accumulateRowsIndex, schemaIdx, fixRowsPerSegment
}

func GenFixRowsPerSegment(data *record.Record, rowNumPerSegment int) []int {
	rowNum := data.RowNums()
	numFragment, remainFragment := rowNum/rowNumPerSegment, rowNum%rowNumPerSegment
	if remainFragment > 0 {
		numFragment += 1
	}
	res := make([]int, numFragment)
	for i := 0; i < numFragment-1; i++ {
		res[i] = rowNumPerSegment * (i + 1)
	}
	res[numFragment-1] = rowNum - 1
	return res
}

func (b *MsBuilder) WriteRecord(id uint64, data *record.Record, nextFile func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16)) (*MsBuilder, error) {
	rowsLimit := b.Conf.maxRowsPerSegment * b.Conf.maxSegmentLimit
	// fast path, most data does not reach the threshold for splitting files.
	if data.RowNums() <= rowsLimit {
		if err := b.WriteData(id, data); err != nil {
			b.log.Error("write data record fail", zap.String("file", b.fd.Name()), zap.Error(err))
			return b, err
		}

		fSize := b.Size()
		if fSize < b.Conf.fileSizeLimit || nextFile == nil {
			return b, nil
		}

		return switchTsspFile(b, data, data, rowsLimit, fSize, nextFile, config.TSSTORE)
	}

	// slow path
	recs := data.Split(nil, rowsLimit)
	for i := range recs {
		err := b.WriteData(id, &recs[i])
		if err != nil {
			b.log.Error("write data record fail", zap.String("file", b.fd.Name()), zap.Error(err))
			return b, err
		}

		fSize := b.Size()
		if (i < len(recs)-1 || fSize >= b.Conf.fileSizeLimit) && nextFile != nil {
			b, err = switchTsspFile(b, &recs[i], data, rowsLimit, fSize, nextFile, config.TSSTORE)
			if err != nil {
				return b, err
			}
		}
	}
	return b, nil
}

func (b *MsBuilder) NewTSSPFile(tmp bool) (TSSPFile, error) {
	if err := b.Flush(); err != nil {
		if err == errEmptyFile {
			b.removeEmptyFile()
			return nil, nil
		}
		b.log.Error("flush error", zap.String("name", b.fd.Name()), zap.Error(err))
		return nil, err
	}

	dr, err := CreateTSSPFileReader(b.fileSize, b.fd, b.trailer, &b.TableData, b.FileVersion(), tmp, b.lock)
	if err != nil {
		b.log.Error("create tssp file reader fail", zap.String("name", dr.FileName()), zap.Error(err))
		return nil, err
	}

	dr.avgChunkRows = 0
	dr.maxChunkRows = 0
	for _, rows := range b.pair.Rows {
		n := int(rows)
		if dr.maxChunkRows < n {
			dr.maxChunkRows = n
		}
		dr.avgChunkRows += n
	}
	dr.avgChunkRows /= len(b.pair.Rows)

	size := dr.InMemSize()
	if b.FileName.order {
		addMemSize(levelName(b.FileName.level), size, size, 0)
	} else {
		addMemSize(levelName(b.FileName.level), size, 0, size)
	}

	///todo for test check, delete after the version is stable
	validateFileName(b.FileName, dr.FileName(), b.lock)
	return &tsspFile{
		name:   b.FileName,
		reader: dr,
		ref:    1,
		lock:   b.lock,
	}, nil
}

func validateFileName(msbFileName TSSPFileName, filePath string, lockPath *string) {
	var fName TSSPFileName
	if err := fName.ParseFileName(filePath); err != nil {
		panic(err)
	}
	fName.lock = lockPath
	order := strings.Contains(filePath, "out-of-order")
	fName.SetOrder(!order)
	if fName != msbFileName {
		panic(fmt.Sprintf("fName:%v, bFName:%v", fName, msbFileName))
	}
}

func (b *MsBuilder) WriteData(id uint64, data *record.Record) error {
	record.CheckRecord(data)

	var err error
	if b.trailer.maxId != 0 {
		if id <= b.trailer.maxId {
			err = fmt.Errorf("file(%v) series id(%d) must be greater than %d", b.fd.Name(), id, b.trailer.maxId)
			b.log.Error("Invalid series id", zap.Error(err))
			return err
		}
	}

	b.encodeChunk = b.encodeChunk[:0]
	if !b.inited {
		b.pair.Reset(b.msName)
		b.inited = true
		b.cmOffset = b.cmOffset[:0]
		if b.cacheDataInMemory() {
			size := EstimateBufferSize(data.Size(), b.MaxIds)
			b.blockSizeIndex = calcBlockIndex(size)
		}
		b.encodeChunk = append(b.encodeChunk, tableMagic...)
		b.encodeChunk = numberenc.MarshalUint64Append(b.encodeChunk, version)
		b.dataOffset = int64(len(b.encodeChunk))
		if b.chunkBuilder.chunkMeta == nil {
			b.chunkBuilder.chunkMeta = &ChunkMeta{}
		}
	}

	b.encodeChunk, err = b.EncodeChunkDataImp.EncodeChunk(b.chunkBuilder, id, b.dataOffset, data, b.encodeChunk, b.timeSorted)
	if err != nil {
		b.log.Error("encode chunk fail", zap.Error(err))
		return err
	}
	b.dataOffset += int64(b.chunkBuilder.chunkMeta.size)

	if err = b.writeToDisk(int64(data.RowNums())); err != nil {
		return err
	}

	minTime, maxTime := b.chunkBuilder.getMinMaxTime(b.timeSorted)
	if b.trailer.idCount == 0 {
		b.trailer.minTime = minTime
		b.trailer.maxTime = maxTime
		b.trailer.minId = id
	}

	b.trailer.idCount++
	b.trailer.maxId = id
	if b.trailer.minTime > minTime {
		b.trailer.minTime = minTime
	}
	if b.trailer.maxTime < maxTime {
		b.trailer.maxTime = maxTime
	}

	b.keys[id] = struct{}{}

	if !b.cacheDataInMemory() {
		return nil
	}

	b.inMemBlock.AppendDataBlock(b.encodeChunk)

	return nil
}

func (b *MsBuilder) releaseEncoders() {
	if b.chunkBuilder != nil {
		if b.chunkBuilder.colBuilder != nil {
			colBuilder := b.chunkBuilder.colBuilder
			if colBuilder != nil {
				if colBuilder.intPreAggBuilder != nil {
					colBuilder.intPreAggBuilder.release()
					colBuilder.intPreAggBuilder = nil
				}

				if colBuilder.floatPreAggBuilder != nil {
					colBuilder.floatPreAggBuilder.release()
					colBuilder.floatPreAggBuilder = nil
				}

				if colBuilder.stringPreAggBuilder != nil {
					colBuilder.stringPreAggBuilder.release()
					colBuilder.stringPreAggBuilder = nil
				}

				if colBuilder.boolPreAggBuilder != nil {
					colBuilder.boolPreAggBuilder.release()
					colBuilder.boolPreAggBuilder = nil
				}

				colBuilder.coder.Release()
			}
		}
	}
}

func (b *MsBuilder) genBloomFilter() {
	bm, bk := bloom.Estimate(uint64(len(b.keys)), falsePositive)
	bmBytes := pow2((bm + 7) / 8)
	if uint64(cap(b.bloomFilter)) < bmBytes {
		b.bloomFilter = make([]byte, bmBytes)
	} else {
		b.bloomFilter = b.bloomFilter[:bmBytes]
		util.MemorySet(b.bloomFilter)
	}
	b.trailer.bloomM = bm
	b.trailer.bloomK = bk
	b.bf, _ = bloom.NewFilterBuffer(b.bloomFilter, bk)
	bytes := make([]byte, 8)
	for id := range b.keys {
		binary.BigEndian.PutUint64(bytes, id)
		b.bf.Insert(bytes)
	}
}

func (b *MsBuilder) Flush() error {
	if b.diskFileWriter.DataSize() <= 16 {
		return errEmptyFile
	}

	if b.mIndex.count > 0 {
		if err := b.SwitchChunkMeta(); err != nil {
			return err
		}
	}

	b.genBloomFilter()
	b.trailer.dataOffset = int64(len(tableMagic) + int(unsafe.Sizeof(version)))
	b.trailer.dataSize = b.diskFileWriter.DataSize() - b.trailer.dataOffset
	metaOff := b.diskFileWriter.DataSize()
	b.trailer.indexSize = b.diskFileWriter.ChunkMetaSize()

	if err := b.diskFileWriter.AppendChunkMetaToData(); err != nil {
		b.log.Error("copy chunk meta fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}
	if b.cacheMetaInMemory() {
		b.chunkMetaBlocks = b.diskFileWriter.MetaDataBlocks(b.chunkMetaBlocks[:0])
		b.inMemBlock.SetMetaBlocks(b.chunkMetaBlocks)
	}

	miOff := b.diskFileWriter.DataSize()
	for i := range b.metaIndexItems {
		m := &b.metaIndexItems[i]
		m.offset += metaOff
		b.encChunkIndexMeta = m.marshal(b.encChunkIndexMeta[:0])
		_, err := b.diskFileWriter.WriteData(b.encChunkIndexMeta)
		if err != nil {
			b.log.Error("write meta index fail", zap.String("name", b.fd.Name()), zap.Error(err))
			return err
		}
	}

	b.trailer.metaIndexSize = b.diskFileWriter.DataSize() - miOff
	b.trailer.metaIndexItemNum = int64(len(b.metaIndexItems))
	b.trailer.bloomSize = int64(len(b.bloomFilter))

	if _, err := b.diskFileWriter.WriteData(b.bloomFilter); err != nil {
		b.log.Error("write bloom filter fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}

	if b.sequencer != nil {
		b.sequencer.BatchUpdateCheckTime(&b.pair, false)
	}

	b.encIdTime = b.pair.Marshal(b.FileName.order || b.trailer.EqualData(IndexOfTimeStoreFlag, TimeStoreFlag), b.encIdTime[:0], b.chunkBuilder.colBuilder.coder)
	b.trailer.idTimeSize = int64(len(b.encIdTime))
	if _, err := b.diskFileWriter.WriteData(b.encIdTime); err != nil {
		b.log.Error("write id time data fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}
	b.trailer.SetChunkMetaCompressFlag()
	b.trailerData = b.trailer.marshal(b.trailerData[:0])

	b.trailerOffset = b.diskFileWriter.DataSize()
	if _, err := b.diskFileWriter.WriteData(b.trailerData); err != nil {
		b.log.Error("write trailer fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}

	var footer [8]byte
	fb := numberenc.MarshalInt64Append(footer[:0], b.trailerOffset)
	if _, err := b.diskFileWriter.WriteData(fb); err != nil {
		b.log.Error("write footer fail", zap.String("name", b.fd.Name()), zap.Error(err))
		return err
	}

	b.fileSize = b.diskFileWriter.DataSize()
	if err := b.diskFileWriter.Close(); err != nil {
		b.log.Error("close file fail", zap.String("name", b.fd.Name()), zap.Error(err))
	}
	b.diskFileWriter = nil

	return nil
}

func (b *MsBuilder) Name() string {
	return b.msName
}

func (b *MsBuilder) Size() int64 {
	if b.diskFileWriter.DataSize() <= 16 {
		return 0
	}
	n := b.diskFileWriter.DataSize()
	n += b.diskFileWriter.ChunkMetaSize()
	n += int64(len(b.metaIndexItems) * MetaIndexLen)
	bm, _ := bloom.Estimate(uint64(len(b.keys)), falsePositive)
	bmBytes := pow2((bm + 7) / 8)
	n += int64(bmBytes) + int64(trailerSize+len(b.Name()))
	n += int64(b.pair.Len()*3*8) / 2 // assuming the compression ratio is 50%
	return n
}

func (b *MsBuilder) removeEmptyFile() {
	if b.diskFileWriter != nil && b.fd != nil {
		_ = b.diskFileWriter.Close()
		name := b.fd.Name()
		_ = b.fd.Close()
		lock := fileops.FileLockOption(*b.lock)
		_ = fileops.Remove(name, lock)
		b.fd = nil
		b.diskFileWriter = nil
	}
}

func (b *MsBuilder) FileVersion() uint64 {
	return version ///todo ???
}

func (b *MsBuilder) cacheDataInMemory() bool {
	if b.tier == util.Hot {
		return b.Conf.cacheDataBlock
	} else if b.tier == util.Warm {
		return false
	}

	return b.Conf.cacheDataBlock
}

func (b *MsBuilder) cacheMetaInMemory() bool {
	if b.tier == util.Hot {
		return b.Conf.cacheMetaData
	} else if b.tier == util.Warm {
		return false
	}

	return b.Conf.cacheMetaData
}

func (b *MsBuilder) GetPKInfoNum() int {
	return len(b.pkRec)
}

func (b *MsBuilder) GetPKRecord(i int) *record.Record {
	return b.pkRec[i]
}

func (b *MsBuilder) GetPKMark(i int) fragment.IndexFragment {
	return b.pkMark[i]
}

func (b *MsBuilder) SwitchChunkMeta() error {
	offBytes := numberenc.MarshalUint32SliceAppend(nil, b.cmOffset)
	_, err := b.diskFileWriter.WriteChunkMeta(offBytes)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk meta fail", zap.Error(err))
		return err
	}

	size, err := b.diskFileWriter.SwitchMetaBuffer()
	if err != nil {
		return err
	}

	b.mIndex.size = uint32(size)
	b.metaIndexItems = append(b.metaIndexItems, b.mIndex)
	b.mIndex.reset()
	b.cmOffset = b.cmOffset[:0]
	return nil
}

func (b *MsBuilder) WriteChunkMeta(cm *ChunkMeta) (int, error) {
	b.encChunkMeta = cm.marshal(b.encChunkMeta[:0])
	b.cmOffset = append(b.cmOffset, uint32(b.currentCMOffset))
	b.currentCMOffset += len(b.encChunkMeta)

	wn, err := b.diskFileWriter.WriteChunkMeta(b.encChunkMeta)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk meta fail", zap.Error(err))
		return 0, err
	}
	if wn != len(b.encChunkMeta) {
		err = errno.NewError(errno.ShortWrite, wn, len(b.encChunkMeta))
		b.log.Error("write chunk meta fail", zap.String("file", b.fd.Name()), zap.Error(err))
	}
	return wn, err
}

func ReleaseMsBuilder(msb *MsBuilder) {
	for _, nf := range msb.Files {
		util.MustClose(nf)
		if err := nf.Remove(); err != nil {
			log.Error("failed to remove file", zap.String("file", nf.Path()))
		}
	}
	nm := msb.fd.Name()
	util.MustClose(msb.fd)
	lock := fileops.FileLockOption(*msb.lock)
	if err := fileops.Remove(nm, lock); err != nil {
		log.Error("failed to remove file", zap.String("file", nm))
	}
}
