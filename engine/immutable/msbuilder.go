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
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

type MsBuilder struct {
	Path string
	TableData
	Conf         *Config
	chunkBuilder *ChunkDataBuilder
	mIndex       MetaIndex
	trailer      *Trailer
	keys         map[uint64]struct{}
	bf           *bloom.Filter
	dataOffset   int64
	MaxIds       int

	trailerOffset     int64
	fd                fileops.File
	fileSize          int64
	diskFileWriter    fileops.FileWriter
	cmOffset          []uint32
	preCmOff          int64
	encodeChunk       []byte
	encChunkMeta      []byte
	encChunkIndexMeta []byte
	chunkMetaBlocks   [][]byte
	encIdTime         []byte
	inited            bool
	blockSizeIndex    int
	pair              IdTimePairs
	sequencer         *Sequencer
	msName            string // measurement name with version.
	lock              *string
	tier              uint64
	cm                *ChunkMeta

	Files         []TSSPFile
	FileName      TSSPFileName
	log           *logger.Logger
	pkIndexWriter sparseindex.IndexWriter
	pkRec         []*record.Record
	pkMark        []fragment.IndexFragment
}

func NewMsBuilder(dir, name string, lockPath *string, conf *Config, idCount int, fileName TSSPFileName,
	tier uint64, sequencer *Sequencer, estimateSize int) *MsBuilder {
	msBuilder := &MsBuilder{}

	if msBuilder.chunkBuilder == nil {
		msBuilder.chunkBuilder = NewChunkDataBuilder(conf.maxRowsPerSegment, conf.maxSegmentLimit)
	} else {
		msBuilder.chunkBuilder.maxRowsLimit = conf.maxRowsPerSegment
		msBuilder.chunkBuilder.colBuilder = NewColumnBuilder()
	}

	if msBuilder.trailer == nil {
		msBuilder.trailer = &Trailer{}
	}

	msBuilder.log = logger.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	msBuilder.WithLog(msBuilder.log)
	msBuilder.tier = tier
	msBuilder.Conf = conf
	msBuilder.lock = lockPath
	msBuilder.trailer.name = append(msBuilder.trailer.name[:0], influx.GetOriginMstName(name)...)
	msBuilder.Path = dir
	msBuilder.inMemBlock = emptyMemReader
	if msBuilder.cacheDataInMemory() || msBuilder.cacheMetaInMemory() {
		idx := calcBlockIndex(estimateSize)
		msBuilder.inMemBlock = NewMemoryReader(blockSize[idx])
	}

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
		if msBuilder.cm == nil {
			msBuilder.cm = &ChunkMeta{}
		}
	}
	msBuilder.keys = make(map[uint64]struct{}, 256)
	msBuilder.sequencer = sequencer
	msBuilder.msName = name
	msBuilder.Files = msBuilder.Files[:0]
	msBuilder.FileName = fileName
	msBuilder.MaxIds = idCount
	msBuilder.bf = nil

	return msBuilder
}

func (b *MsBuilder) StoreTimes() {
	b.trailer.SetData(IndexOfTimeStoreFlag, TimeStoreFlag)
}

func (b *MsBuilder) MaxRowsPerSegment() int {
	return b.Conf.maxRowsPerSegment
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

	cm := b.cm
	minT, maxT := cm.MinMaxTime()
	if b.mIndex.count == 0 {
		b.mIndex.size = 0
		b.mIndex.id = cm.sid
		b.mIndex.minTime = minT
		b.mIndex.maxTime = maxT
		b.mIndex.offset = b.diskFileWriter.ChunkMetaSize()
	}

	b.pair.Add(cm.sid, maxT)
	b.pair.AddRowCounts(rowCounts)

	b.encChunkMeta = cm.marshal(b.encChunkMeta[:0])
	cmOff := b.diskFileWriter.ChunkMetaSize()
	b.cmOffset = append(b.cmOffset, uint32(cmOff-b.preCmOff))
	wn, err = b.diskFileWriter.WriteChunkMeta(b.encChunkMeta)
	if err != nil {
		err = errWriteFail(b.diskFileWriter.Name(), err)
		b.log.Error("write chunk meta fail", zap.Error(err))
		return err
	}
	if wn != len(b.encChunkMeta) {
		b.log.Error("write chunk meta fail", zap.String("file", b.fd.Name()), zap.Ints("size", []int{len(b.encChunkMeta), wn}))
		return io.ErrShortWrite
	}

	b.mIndex.size += uint32(wn) + 4
	b.mIndex.count++
	if b.mIndex.minTime > minT {
		b.mIndex.minTime = minT
	}
	if b.mIndex.maxTime < maxT {
		b.mIndex.maxTime = maxT
	}

	if b.mIndex.size >= uint32(b.Conf.maxChunkMetaItemSize) || b.mIndex.count >= uint32(b.Conf.maxChunkMetaItemCount) {
		offBytes := numberenc.MarshalUint32SliceAppend(nil, b.cmOffset)
		_, err = b.diskFileWriter.WriteChunkMeta(offBytes)
		if err != nil {
			err = errWriteFail(b.diskFileWriter.Name(), err)
			b.log.Error("write chunk meta fail", zap.Error(err))
			return err
		}
		b.metaIndexItems = append(b.metaIndexItems, b.mIndex)
		b.mIndex.reset()
		b.cmOffset = b.cmOffset[:0]
		b.preCmOff = b.diskFileWriter.ChunkMetaSize()
		b.diskFileWriter.SwitchMetaBuffer()
	}

	return nil
}

func switchTsspFile(msb *MsBuilder, rec, totalRec *record.Record, rowsLimit int, fSize int64,
	nextFile func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16)) (*MsBuilder, error) {
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

	builder := NewMsBuilder(msb.Path, msb.Name(), msb.lock, msb.Conf, msb.MaxIds, msb.FileName, msb.tier, msb.sequencer, rec.Len())
	builder.Files = append(builder.Files, msb.Files...)
	builder.pkRec = append(builder.pkRec, msb.pkRec...)
	builder.pkMark = append(builder.pkMark, msb.pkMark...)
	builder.WithLog(msb.log)
	return builder, nil
}

func (b *MsBuilder) NewPKIndexWriter() {
	b.pkIndexWriter = sparseindex.NewIndexWriter()
}

func (b *MsBuilder) writeIndex(writeRec *record.Record, pkSchema record.Schemas, filepath, lockpath string) error {
	// Generate the primary key record from the sorted chunk based on the primary key.
	pkRec, pkMark, err := b.pkIndexWriter.CreatePrimaryIndex(writeRec, pkSchema, colstore.RowsNumPerFragment)
	if err != nil {
		return err
	}
	indexBuilder := colstore.NewIndexBuilder(&lockpath, filepath)
	err = indexBuilder.WriteData(pkRec)
	defer indexBuilder.Reset()
	if err != nil {
		return err
	}
	b.pkRec = append(b.pkRec, pkRec)
	b.pkMark = append(b.pkMark, pkMark)

	return nil
}

func (b *MsBuilder) WriteRecord(id uint64, data *record.Record, schema record.Schemas, nextFile func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16)) (*MsBuilder, error) {
	rowsLimit := b.Conf.maxRowsPerSegment * b.Conf.maxSegmentLimit

	// fast path, most data does not reach the threshold for splitting files.
	if data.RowNums() <= rowsLimit {
		if err := b.WriteData(id, data); err != nil {
			b.log.Error("write data record fail", zap.String("file", b.fd.Name()), zap.Error(err))
			return b, err
		}

		if len(schema) != 0 { // write index, works for colstore
			dataFilePath := b.FileName.String()
			indexFilePath := path.Join(b.Path, b.msName, colstore.AppendIndexSuffix(dataFilePath)+tmpFileSuffix)
			if err := b.writeIndex(data, schema, indexFilePath, *b.lock); err != nil {
				logger.GetLogger().Error("write primary key file failed", zap.String("mstName", b.msName), zap.Error(err))
				return b, err
			}
		}

		fSize := b.Size()
		if fSize < b.Conf.fileSizeLimit || nextFile == nil {
			return b, nil
		}

		return switchTsspFile(b, data, data, rowsLimit, fSize, nextFile)
	}

	// slow path
	recs := data.Split(nil, rowsLimit)
	for i := range recs {
		err := b.WriteData(id, &recs[i])
		if err != nil {
			b.log.Error("write data record fail", zap.String("file", b.fd.Name()), zap.Error(err))
			return b, err
		}
		if len(schema) != 0 { // write index, works for colstore
			dataFilePath := b.FileName.String()
			indexFilePath := path.Join(b.Path, b.msName, colstore.AppendIndexSuffix(dataFilePath)+tmpFileSuffix)
			if err := b.writeIndex(&recs[i], schema, indexFilePath, *b.lock); err != nil {
				logger.GetLogger().Error("write primary key file failed", zap.String("mstName", b.msName), zap.Error(err))
				return b, err
			}
		}

		fSize := b.Size()
		if (i < len(recs)-1 || fSize >= b.Conf.fileSizeLimit) && nextFile != nil {
			b, err = switchTsspFile(b, &recs[i], data, rowsLimit, fSize, nextFile)
			if err != nil {
				return b, err
			}
			if len(schema) != 0 { // need to init indexwriter after switch tssp file, i.e. new b
				b.NewPKIndexWriter()
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
		b.preCmOff = 0
		if b.cacheDataInMemory() {
			size := EstimateBufferSize(data.Size(), b.MaxIds)
			b.blockSizeIndex = calcBlockIndex(size)
		}
		b.encodeChunk = append(b.encodeChunk, tableMagic...)
		b.encodeChunk = numberenc.MarshalUint64Append(b.encodeChunk, version)
		b.dataOffset = int64(len(b.encodeChunk))
		if b.cm == nil {
			b.cm = &ChunkMeta{}
		}
	}

	b.chunkBuilder.setChunkMeta(b.cm)
	b.encodeChunk, err = b.chunkBuilder.EncodeChunk(id, b.dataOffset, data, b.encodeChunk)
	if err != nil {
		b.log.Error("encode chunk fail", zap.Error(err))
		return err
	}
	b.dataOffset += int64(b.cm.size)

	if err = b.writeToDisk(int64(data.RowNums())); err != nil {
		return err
	}

	minTime, maxTime := b.cm.MinMaxTime()
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
		offBytes := numberenc.MarshalUint32SliceAppend(nil, b.cmOffset)
		_, err := b.diskFileWriter.WriteChunkMeta(offBytes)
		if err != nil {
			b.log.Error("write chunk meta fail", zap.String("name", b.fd.Name()), zap.Error(err))
			return err
		}
		b.metaIndexItems = append(b.metaIndexItems, b.mIndex)
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
