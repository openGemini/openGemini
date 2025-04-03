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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const (
	BLOOMFILTER_SIZE         = 8
	SERIESKEY_STATISTIC_SIZE = 24
	COMPRESSION_RATIO        = 2
)

var zeroCRC32 = [4]byte{}

type StreamWriteFile struct {
	closed       chan struct{}
	version      uint64
	chunkRows    int64
	maxChunkRows int64
	maxN         int
	fileSize     int64
	//preCmOff     int64
	dir  string
	name string // measurement name with version
	TableData
	mIndex MetaIndex
	keys   map[uint64]struct{}
	bf     *bloom.Filter

	Conf       *Config
	ctx        *ReadContext
	colBuilder *ColumnBuilder
	trailer    Trailer
	fd         fileops.File
	writer     fileops.FileWriter
	pair       IdTimePairs
	dstMeta    ChunkMeta
	fileName   TSSPFileName

	encChunkMeta      []byte
	encChunkIndexMeta []byte
	encIdTime         []byte
	cmOffset          []uint32
	currentCMOffset   int

	file     TSSPFile
	schema   record.Schemas
	colSegs  []record.ColVal
	timeCols []record.ColVal
	newFile  TSSPFile
	log      *Log.Logger
	lock     *string

	// count the number of rows in each column
	// used only for data verification
	rowCount       map[string]int
	enableValidate bool
	tier           uint64

	chunkMetaCodecCtx *ChunkMetaCodecCtx
}

func NewWriteScanFile(mst string, m *MmsTables, file TSSPFile, schema record.Schemas) (*StreamWriteFile, error) {
	trailer := file.FileStat()

	compItr := m.NewStreamWriteFile(mst)
	compItr.maxN = int(trailer.idCount)
	compItr.schema = schema
	compItr.file = file
	compItr.tier = *(m.tier)
	return compItr, nil
}

func getStreamWriteFile() *StreamWriteFile {
	return &StreamWriteFile{
		ctx:        NewReadContext(true),
		colBuilder: NewColumnBuilder(),
		Conf:       GetTsStoreConfig(),
		rowCount:   make(map[string]int),
	}
}

func (c *StreamWriteFile) Init(id uint64, chunkDataOffset int64, schema record.Schemas) {
	c.dstMeta.resize(schema.Len(), 0)
	for i, ref := range schema {
		c.dstMeta.colMeta[i].name = ref.Name
		c.dstMeta.colMeta[i].ty = byte(ref.Type)
	}
	c.dstMeta.sid = id
	c.dstMeta.offset = chunkDataOffset
	c.dstMeta.size = 0
	c.dstMeta.columnCount = 0
	c.dstMeta.segCount = 0
	c.colBuilder.resetPreAgg()
}

func (c *StreamWriteFile) NewFile(addFileExt bool) error {
	if addFileExt {
		c.fileName.extent++
	}
	c.reset()
	c.trailer.name = append(c.trailer.name[:0], influx.GetOriginMstName(c.name)...)

	lock := fileops.FileLockOption(*c.lock)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	dir := filepath.Join(c.dir, c.name)
	_ = fileops.MkdirAll(dir, 0750, lock)
	filePath := c.fileName.Path(dir, true)
	_, err := fileops.Stat(filePath)
	if err == nil {
		c.log.Error("file exist", zap.String("file", filePath))
		return fmt.Errorf("file(%s) exist", filePath)
	}
	if c.tier == util.Cold {
		c.fd, err = fileops.CreateV2(filePath, lock, pri)
	} else {
		c.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0600, lock, pri)
	}

	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}

	c.writer = newTsspFileWriter(c.fd, false, false, c.lock)
	if c.file != nil && c.file.InMemSize() > 0 {
		c.writer = NewHotFileWriter(c.writer)
	}

	var buf [16]byte
	b := append(buf[:0], tableMagic...)
	b = numberenc.MarshalUint64Append(b, version)
	_, err = c.writer.WriteData(b)
	if err != nil {
		log.Error("write file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}

	c.version = version
	c.keys = make(map[uint64]struct{}, c.maxN)

	return nil
}

func (c *StreamWriteFile) NewTSSPFile(tmp bool) (TSSPFile, error) {
	if err := c.Flush(); err != nil {
		if errors.Is(err, errEmptyFile) {
			return nil, nil
		}
		c.log.Error("flush error", zap.String("name", c.fd.Name()), zap.Error(err))
		return nil, err
	}

	dr, err := CreateTSSPFileReader(c.fileSize, c.fd, &c.trailer, &c.TableData, c.version, tmp, c.lock)
	if err != nil {
		c.log.Error("create tssp file reader fail", zap.String("name", dr.FileName()), zap.Error(err))
		return nil, err
	}

	dr.maxChunkRows = int(c.maxChunkRows)
	dr.avgChunkRows = int(c.chunkRows / dr.trailer.idCount)
	if dr.avgChunkRows < 1 {
		dr.avgChunkRows = 1
	}

	f := &tsspFile{
		name:   c.fileName,
		reader: dr,
		ref:    1,
		lock:   c.lock,
	}
	hotWriter, hot := c.writer.(*HotFileWriter)
	if hot {
		dr.ApplyHotReader(hotWriter.BuildHotFileReader(dr.GetBasicFileReader()))
		hotWriter.Release()
	}
	c.writer = nil
	return f, nil
}

func (c *StreamWriteFile) InitFile(seq uint64) error {
	level, _ := c.file.LevelAndSequence()
	merge := c.file.FileNameMerge()
	extend := c.file.FileNameExtend()
	c.fileName = NewTSSPFileName(seq, level, merge, extend, true, c.lock)
	if err := c.NewFile(true); err != nil {
		dir := filepath.Join(c.dir, c.name)
		c.log.Error("create tssp file fail", zap.String("name", c.fileName.Path(dir, true)), zap.Error(err))
		return err
	}

	return nil
}

func (c *StreamWriteFile) InitMergedFile(f TSSPFile) error {
	c.file = f
	c.fileName = f.FileName()
	c.fileName.lock = c.lock
	c.fileName.merge++

	if err := c.NewFile(false); err != nil {
		return err
	}

	return nil
}

func (c *StreamWriteFile) ChangeSid(sid uint64) {
	for k := range c.rowCount {
		delete(c.rowCount, k)
	}
	dOff := c.writer.DataSize()
	c.Init(sid, dOff, c.schema)
}

func (c *StreamWriteFile) AppendColumn(ref *record.Field) error {
	c.colBuilder.BuildPreAgg()

	if err := c.colBuilder.initEncoder(*ref); err != nil {
		return err
	}

	c.colBuilder.colMeta = c.dstMeta.AllocColMeta(ref)
	c.colBuilder.cm = &c.dstMeta

	if err := c.writeCrc(zeroCRC32[:]); err != nil {
		return err
	}

	return nil
}

func (c *StreamWriteFile) ChangeColumn(ref record.Field) error {
	_ = c.colBuilder.initEncoder(ref)
	idx := c.dstMeta.columnIndex(&ref)
	c.colBuilder.cm = &c.dstMeta
	c.colBuilder.colMeta = &c.dstMeta.colMeta[idx]
	var crc [4]byte
	//pad crc
	if err := c.writeCrc(crc[:]); err != nil {
		return err
	}
	return nil
}

func (c *StreamWriteFile) SortColumns() {
	cols := c.dstMeta.colMeta
	sort.Slice(cols[:len(cols)-1], func(i, j int) bool {
		return cols[i].name < cols[j].name
	})
}

func (c *StreamWriteFile) WriteCurrentMeta() error {
	if c.dstMeta.sid == 0 {
		return nil
	}
	defer func() {
		c.dstMeta.sid = 0
	}()

	failpoint.Inject("write-current-meta-error", func() {
		failpoint.Return(fmt.Errorf("failed to wirte current meta"))
	})

	return c.WriteMeta(&c.dstMeta)
}

func (c *StreamWriteFile) SetValidate(en bool) {
	c.enableValidate = en
}

func (c *StreamWriteFile) validation() {
	if !c.enableValidate || len(c.rowCount) == 0 {
		return
	}

	exp, ok := c.rowCount[record.TimeField]
	if !ok {
		panic("missing time column")
	}

	for name, got := range c.rowCount {
		if got != exp {
			panic(fmt.Sprintf("number of rows is different. exp=%d, got=%d, ref=%s",
				exp, got, name))
		}
	}
}

func (c *StreamWriteFile) WriteMeta(cm *ChunkMeta) error {
	c.validation()

	cm.DelEmptyColMeta()
	cm.size = uint32(c.writer.DataSize() - cm.offset)
	cm.columnCount = uint32(len(cm.colMeta))
	cm.segCount = uint32(len(cm.timeRange))
	minT, maxT := cm.MinMaxTime()
	if c.mIndex.count == 0 {
		c.mIndex.size = 0
		c.mIndex.id = cm.sid
		c.mIndex.minTime = minT
		c.mIndex.maxTime = maxT
		c.mIndex.offset = c.writer.ChunkMetaSize()
		c.currentCMOffset = 0
	}

	c.updateChunkStat(cm.sid, maxT, int64(cm.Rows(c.colBuilder.timePreAggBuilder)))
	c.keys[cm.sid] = struct{}{}

	if c.enableValidate {
		cm.Validation()
	}

	_, err := c.WriteChunkMeta(cm)
	if err != nil {
		return err
	}

	if c.trailer.idCount == 0 {
		c.trailer.minTime = minT
		c.trailer.maxTime = maxT
		c.trailer.minId = cm.sid
	}

	c.trailer.idCount++
	c.trailer.maxId = cm.sid
	if c.trailer.minTime > minT {
		c.trailer.minTime = minT
	}
	if c.trailer.maxTime < maxT {
		c.trailer.maxTime = maxT
	}

	c.mIndex.count++
	if c.mIndex.minTime > minT {
		c.mIndex.minTime = minT
	}
	if c.mIndex.maxTime < maxT {
		c.mIndex.maxTime = maxT
	}

	if c.mIndex.size >= uint32(c.Conf.maxChunkMetaItemSize) || c.mIndex.count >= uint32(c.Conf.maxChunkMetaItemCount) {
		if err := c.SwitchChunkMeta(); err != nil {
			return err
		}
	}

	return nil
}

func (c *StreamWriteFile) SwitchChunkMeta() error {
	offBytes := numberenc.MarshalUint32SliceAppend(nil, c.cmOffset)
	_, err := c.writer.WriteChunkMeta(offBytes)
	if err != nil {
		err = errWriteFail(c.writer.Name(), err)
		c.log.Error("write chunk meta fail", zap.Error(err))
		return err
	}

	size, err := c.writer.SwitchMetaBuffer()
	if err != nil {
		return err
	}

	c.mIndex.size = uint32(size)
	c.metaIndexItems = append(c.metaIndexItems, c.mIndex)
	c.mIndex.reset()
	c.cmOffset = c.cmOffset[:0]
	return nil
}

func (c *StreamWriteFile) WriteChunkMeta(cm *ChunkMeta) (int, error) {
	buf, err := MarshalChunkMeta(c.chunkMetaCodecCtx, cm, c.encChunkMeta[:0])
	if err != nil {
		return 0, err
	}

	c.encChunkMeta = buf
	c.cmOffset = append(c.cmOffset, uint32(c.currentCMOffset))
	c.currentCMOffset += len(c.encChunkMeta)

	wn, err := c.writer.WriteChunkMeta(c.encChunkMeta)
	if err != nil {
		err = errWriteFail(c.writer.Name(), err)
		c.log.Error("write chunk meta fail", zap.Error(err))
		return 0, err
	}
	if wn != len(c.encChunkMeta) {
		err = errno.NewError(errno.ShortWrite, wn, len(c.encChunkMeta))
		c.log.Error("write chunk meta fail", zap.String("file", c.fd.Name()), zap.Error(err))
	}
	return wn, err
}

func (c *StreamWriteFile) WriteData(id uint64, ref record.Field, col record.ColVal, timeCol *record.ColVal) error {
	record.CheckCol(&col, ref.Type)
	if col.Len > c.Conf.GetMaxRowsPerSegment() {
		err := fmt.Sprintf("col.Len=%d is greater than MaxRowsPerSegment=%d", col.Len, c.Conf.GetMaxRowsPerSegment())
		panic(err)
	}

	if err := c.validate(id, ref); err != nil {
		return err
	}

	if c.enableValidate {
		c.rowCount[ref.Name] += col.Len
	}

	var err error
	c.colSegs[0] = col
	off := c.writer.DataSize()
	c.colBuilder.data = c.colBuilder.data[:0]
	if ref.Name == record.TimeField {
		err = c.colBuilder.encodeTimeColumn(c.colSegs, off)
		c.colBuilder.BuildPreAgg()
	} else {
		c.timeCols = c.timeCols[:0]
		if timeCol != nil {
			c.timeCols = append(c.timeCols, *timeCol)
		}
		err = c.colBuilder.encodeColumn(c.colSegs, c.timeCols, off, ref)
	}
	if err != nil {
		c.log.Error("encode column fail", zap.String("field", ref.String()), zap.String("file", c.fd.Name()), zap.Error(err))
		return err
	}
	wn, err := c.writer.WriteData(c.colBuilder.data)
	if err != nil || wn != len(c.colBuilder.data) {
		c.log.Error("write data segment fail", zap.String("file", c.fd.Name()), zap.Error(err))
		return err
	}
	return err
}

func (c *StreamWriteFile) updateChunkStat(id uint64, maxT int64, rows int64) {
	c.pair.Add(id, maxT)
	c.pair.AddRowCounts(rows)
	c.chunkRows += rows
	if rows > c.maxChunkRows {
		c.maxChunkRows = rows
	}
}

func (c *StreamWriteFile) WriteFile() error {
	fSize := c.Size()
	f, err := c.NewTSSPFile(true)
	if err != nil {
		c.log.Error("new tssp file fail", zap.Error(err))
		return err
	}

	c.log.Info("switch tssp file",
		zap.String("file", f.Path()),
		zap.Int("rowsLimit", c.Conf.maxSegmentLimit*c.Conf.maxRowsPerSegment),
		zap.Int64("fileSize", fSize),
		zap.Int64("sizeLimit", c.Conf.fileSizeLimit))

	c.newFile = f
	return nil
}

func (c *StreamWriteFile) GetTSSPFile() TSSPFile {
	return c.newFile
}

func (c *StreamWriteFile) Flush() error {
	if c.writer.DataSize() <= 16 {
		return errEmptyFile
	}

	if c.mIndex.count > 0 {
		if err := c.SwitchChunkMeta(); err != nil {
			return err
		}
	}

	c.genBloomFilter()
	c.trailer.dataOffset = int64(len(tableMagic) + int(unsafe.Sizeof(version)))
	c.trailer.dataSize = c.writer.DataSize() - c.trailer.dataOffset
	metaOff := c.writer.DataSize()
	c.trailer.indexSize = c.writer.ChunkMetaSize()

	if err := c.writer.AppendChunkMetaToData(); err != nil {
		c.log.Error("copy chunk meta fail", zap.String("name", c.fd.Name()), zap.Error(err))
		return err
	}

	miOff := c.writer.DataSize()
	for i := range c.metaIndexItems {
		m := &c.metaIndexItems[i]
		m.offset += metaOff
		c.encChunkIndexMeta = m.marshal(c.encChunkIndexMeta[:0])
		_, err := c.writer.WriteData(c.encChunkIndexMeta)
		if err != nil {
			c.log.Error("write meta index fail", zap.String("name", c.fd.Name()), zap.Error(err))
			return err
		}
	}

	c.trailer.metaIndexSize = c.writer.DataSize() - miOff
	c.trailer.metaIndexItemNum = int64(len(c.metaIndexItems))
	c.trailer.bloomSize = int64(len(c.bloomFilter))

	if _, err := c.writer.WriteData(c.bloomFilter); err != nil {
		c.log.Error("write bloom filter fail", zap.String("name", c.fd.Name()), zap.Error(err))
		return err
	}

	c.encIdTime = c.pair.Marshal(true, c.encIdTime[:0], c.colBuilder.coder)
	c.trailer.idTimeSize = int64(len(c.encIdTime))
	if _, err := c.writer.WriteData(c.encIdTime); err != nil {
		c.log.Error("write id time data fail", zap.String("name", c.fd.Name()), zap.Error(err))
		return err
	}
	c.trailer.EnableTimeStore()
	c.trailer.SetChunkMetaHeader(c.chunkMetaCodecCtx.GetHeader())
	c.trailer.SetChunkMetaCompressFlag()
	c.trailerData = c.trailer.Marshal(c.trailerData[:0])

	trailerOffset := c.writer.DataSize()
	if _, err := c.writer.WriteData(c.trailerData); err != nil {
		c.log.Error("write trailer fail", zap.String("name", c.fd.Name()), zap.Error(err))
		return err
	}

	var footer [8]byte
	fb := numberenc.MarshalInt64Append(footer[:0], trailerOffset)
	if _, err := c.writer.WriteData(fb); err != nil {
		c.log.Error("write footer fail", zap.String("name", c.fd.Name()), zap.Error(err))
		return err
	}

	c.fileSize = c.writer.DataSize()
	if err := c.writer.Close(); err != nil {
		c.log.Error("close file fail", zap.String("name", c.fd.Name()), zap.Error(err))
	}

	return nil
}

func (c *StreamWriteFile) genBloomFilter() {
	bm, bk := bloom.Estimate(uint64(len(c.keys)), falsePositive)
	bmBytes := pow2((bm + BLOOMFILTER_SIZE - 1) / BLOOMFILTER_SIZE)
	if uint64(cap(c.bloomFilter)) < bmBytes {
		c.bloomFilter = make([]byte, bmBytes)
	} else {
		c.bloomFilter = c.bloomFilter[:bmBytes]
		util.MemorySet(c.bloomFilter, 0)
	}
	c.trailer.bloomM = bm
	c.trailer.bloomK = bk
	c.bf, _ = bloom.NewFilterBuffer(c.bloomFilter, bk)
	bytes := make([]byte, 8)
	for id := range c.keys {
		binary.BigEndian.PutUint64(bytes, id)
		c.bf.Insert(bytes)
	}
}

func (c *StreamWriteFile) Size() int64 {
	if c.writer != nil && c.writer.DataSize() <= 16 {
		return 0
	}
	n := c.writer.DataSize()
	n += c.writer.ChunkMetaSize()
	n += int64(len(c.metaIndexItems) * MetaIndexLen)
	bm, _ := bloom.Estimate(uint64(len(c.keys)), falsePositive)
	bmBytes := pow2((bm + BLOOMFILTER_SIZE - 1) / BLOOMFILTER_SIZE)
	n += int64(bmBytes) + int64(trailerSize+len(c.name))
	n += int64(c.pair.Len()*SERIESKEY_STATISTIC_SIZE) / COMPRESSION_RATIO // assuming the compression ratio is 50%
	return n
}

func (c *StreamWriteFile) validate(id uint64, ref record.Field) error {
	if id == 0 {
		return fmt.Errorf("series id is 0")
	}

	if c.trailer.maxId != 0 {
		if id <= c.trailer.maxId {
			err := fmt.Errorf("file(%v) series id(%d) must be greater than %d", c.fd.Name(), id, c.trailer.maxId)
			c.log.Error("Invalid series id", zap.Error(err))
			return err
		}
	}

	if !c.colBuilder.colMeta.Equal(ref.Name, ref.Type) {
		err := fmt.Errorf("invalid column,exp:%v, but:%v::%v", ref.String(), c.colBuilder.colMeta.Name(),
			influx.FieldTypeName[int(c.colBuilder.colMeta.ty)])
		c.log.Error(err.Error())
		return err
	}

	return nil
}

func (c *StreamWriteFile) writeCrc(crc []byte) error {
	if _, err := c.writer.WriteData(crc[:]); err != nil {
		return err
	}

	return nil
}

func (c *StreamWriteFile) Close(isError bool) {
	if c.writer != nil && c.fd != nil {
		_ = c.writer.Close()
		if isError {
			name := c.fd.Name()
			_ = c.fd.Close()
			lock := fileops.FileLockOption(*c.lock)
			_ = fileops.Remove(name, lock)
		}
		c.fd = nil
		c.writer = nil
	}
}
