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
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/failpoint"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	BLOOMFILTER_SIZE         = 8
	SERIESKEY_STATISTIC_SIZE = 24
	COMPRESSION_RATIO        = 2
)

var zeroCRC32 = [4]byte{}

type StreamWriteFile struct {
	TableData

	closed       chan struct{}
	version      uint64
	chunkRows    int64
	maxChunkRows int64
	maxN         int
	fileSize     int64

	dir  string
	name string // measurement name with version
	bf   *bloom.Filter

	Conf       *Config
	ctx        *ReadContext
	colBuilder *ColumnBuilder
	trailer    Trailer
	fd         fileops.File
	writer     fileops.FileWriter
	pair       *IdTimePairs
	dstMeta    ChunkMeta
	fileName   TSSPFileName
	encIdTime  []byte

	file     TSSPFile
	schema   record.Schemas
	colSegs  []record.ColVal
	timeCols []record.ColVal
	newFile  TSSPFile
	log      *logger.Logger
	lock     *string

	// count the number of rows in each column
	// used only for data verification
	rowCount       map[string]int
	enableValidate bool
	timeDisorder   bool //For column store, data may not be sorted by time
	tier           uint64

	cmw *ChunkMetaWriter
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

func NewStreamWriteFile(mst string, Conf *Config, dir string, signal chan struct{}, lock *string) *StreamWriteFile {
	sw := &StreamWriteFile{
		name:       mst,
		dir:        dir,
		closed:     signal,
		ctx:        NewReadContext(true),
		colBuilder: NewColumnBuilder(),
		Conf:       Conf,
		rowCount:   make(map[string]int),
		cmw:        NewChunkMetaWriter(Conf.CompressChunkMeta),
		lock:       lock,
	}

	sw.pair = GetIDTimePairs(mst)
	sw.log = logger.NewLogger(errno.ModuleUnknown)
	sw.colSegs = make([]record.ColVal, 1)
	sw.colBuilder.timePreAggBuilder = acquireTimePreAggBuilder()
	return sw
}

func (c *StreamWriteFile) MarkTimeDisorder() {
	c.timeDisorder = true
	c.colBuilder.MarkTimeDisorder()
}

func (c *StreamWriteFile) GetDir() string {
	return c.dir
}

func (c *StreamWriteFile) GetFileName() TSSPFileName {
	return c.fileName
}

func (c *StreamWriteFile) Init(id uint64, schema record.Schemas) {
	c.dstMeta.resize(schema.Len(), 0)
	for i, ref := range schema {
		c.dstMeta.colMeta[i].name = ref.Name
		c.dstMeta.colMeta[i].ty = byte(ref.Type)
	}
	c.dstMeta.sid = id
	c.dstMeta.offset = c.writer.DataSize()
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

	c.Release()
	return f, nil
}

func (c *StreamWriteFile) Release() {
	c.writer = nil
	PutIDTimePairs(c.pair)
	c.pair = nil
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
	return c.initFromTSSPFile(f, true, false)
}

func (c *StreamWriteFile) InitCompactFile(f TSSPFile) error {
	return c.initFromTSSPFile(f, false, true)
}

func (c *StreamWriteFile) initFromTSSPFile(f TSSPFile, incMerged, incLevel bool) error {
	c.file = f
	c.fileName = f.FileName()
	c.fileName.lock = c.lock

	if incMerged {
		c.fileName.merge++
	}
	if incLevel {
		c.fileName.level++
	}

	return c.NewFile(false)
}

func (c *StreamWriteFile) InitFlushFile(seq uint64, order bool) error {
	c.fileName = NewTSSPFileName(seq, 0, 0, 0, order, c.lock)
	if err := c.NewFile(false); err != nil {
		dir := filepath.Join(c.dir, c.name)
		c.log.Error("create tssp file fail", zap.String("name", c.fileName.Path(dir, true)), zap.Error(err))
		return err
	}

	return nil
}

func (c *StreamWriteFile) Clean() {
	if c.file != nil {
		util.MustRun(c.file.Remove)
	}
}

func (c *StreamWriteFile) ChangeSid(sid uint64) {
	for k := range c.rowCount {
		delete(c.rowCount, k)
	}

	c.Init(sid, c.schema)
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

	failpoint.ApplyMethod(failpoint.WriteCurrentMetaError, c, "WriteMeta", func(*ChunkMeta) error {
		return errors.New("failed to wirte current meta")
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
	if c.colBuilder.timeDisorder {
		for i := range cm.timeRange {
			minT = min(minT, cm.timeRange[i].minTime())
			maxT = max(maxT, cm.timeRange[i].maxTime())
		}
	}

	c.cmw.UpdateMetaIndex(minT, maxT, cm.sid)

	c.updateChunkStat(cm.sid, maxT, int64(cm.Rows(c.colBuilder.timePreAggBuilder)))

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

	if config.ChunkMetaLimited(c.cmw.MemorySize(), c.cmw.MetaIndexCount()) {
		if err := c.SwitchChunkMeta(); err != nil {
			return err
		}
	}

	return nil
}

func (c *StreamWriteFile) SwitchChunkMeta() error {
	_, err := c.cmw.FlushChunkMeta(c.writer)
	return err
}

func (c *StreamWriteFile) WriteChunkMeta(cm *ChunkMeta) (int, error) {
	return c.cmw.Write(cm)
}

func (c *StreamWriteFile) WriteRecord(sid uint64, rec *record.Record) error {
	c.ChangeSid(sid)
	maxSegRow := c.Conf.GetMaxRowsPerSegment()
	timeCol := rec.TimeColumn()
	if rec.RowNums() < maxSegRow {
		for i := range rec.Len() {
			err := c.AppendColumn(&rec.Schema[i])
			if err != nil {
				return err
			}

			err = c.WriteData(sid, rec.Schema[i], rec.ColVals[i], timeCol)
			if err != nil {
				return err
			}
		}
		return c.WriteCurrentMeta()
	}

	var cols []record.ColVal
	timeCols := timeCol.Split(nil, maxSegRow, rec.LastSchema().Type)
	for i := range rec.Len() {
		err := c.AppendColumn(&rec.Schema[i])
		if err != nil {
			return err
		}

		for j := range cols {
			cols[j].Init()
		}

		cols = rec.ColVals[i].Split(cols[:0], maxSegRow, rec.Schema[i].Type)
		for j := range cols {
			err = c.WriteData(sid, rec.Schema[i], cols[j], &timeCols[j])
			if err != nil {
				return err
			}
		}
	}

	return c.WriteCurrentMeta()
}

func (c *StreamWriteFile) WriteData(id uint64, ref record.Field, col record.ColVal, timeCol *record.ColVal) error {
	record.CheckCol(&col, ref.Type)
	if timeCol != nil && col.Len != timeCol.Len {
		return fmt.Errorf("column length mismatch: %d vs %d", col.Len, timeCol.Len)
	}

	if col.Len > c.Conf.GetMaxRowsPerSegment() {
		return fmt.Errorf("col.Len=%d is greater than MaxRowsPerSegment=%d", col.Len, c.Conf.GetMaxRowsPerSegment())
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

func (c *StreamWriteFile) IdTimePairs() *IdTimePairs {
	return c.pair
}

func (c *StreamWriteFile) Flush() error {
	if c.writer.DataSize() <= 16 {
		return errEmptyFile
	}

	if c.cmw.MemorySize() > 0 {
		if err := c.SwitchChunkMeta(); err != nil {
			return err
		}
	}

	c.genBloomFilter()
	c.trailer.dataOffset = int64(len(tableMagic) + int(unsafe.Sizeof(version)))
	c.trailer.dataSize = c.writer.DataSize() - c.trailer.dataOffset - c.cmw.Size()
	c.trailer.indexSize = c.cmw.Size()

	wn, err := c.cmw.FlushMetaIndex(c.writer)
	if err != nil {
		return err
	}

	c.trailer.metaIndexSize = int64(wn)
	c.trailer.metaIndexItemNum = int64(c.cmw.MetaIndexCount())
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
	if c.cmw.CompressEnabled() {
		c.trailer.SetChunkMetaHeader(c.cmw.GetChunkMetaHeader())
		c.trailer.SetChunkMetaCompressFlag()
	}
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
	bm, bk := bloom.Estimate(uint64(c.pair.Len()), falsePositive)
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
	for _, id := range c.pair.Ids {
		binary.BigEndian.PutUint64(bytes, id)
		c.bf.Insert(bytes)
	}
}

func (c *StreamWriteFile) Size() int64 {
	if c.writer != nil && c.writer.DataSize() <= 16 {
		return 0
	}
	n := c.writer.DataSize()
	n += int64(c.cmw.MemorySize())
	n += int64(c.cmw.MetaIndexCount() * MetaIndexLen)
	bm, _ := bloom.Estimate(uint64(c.pair.Len()), falsePositive)
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

type ChunkMetaWriter struct {
	size           int64 // total size of chunk meta
	codecCtx       *ChunkMetaCodecCtx
	metaIndex      MetaIndex
	metaIndexItems []MetaIndex

	offsets []uint32
	buf     []byte
	swap    []byte
}

func NewChunkMetaWriter(compress bool) *ChunkMetaWriter {
	cmw := &ChunkMetaWriter{}
	if compress {
		cmw.codecCtx = NewChunkMetaCodecCtx()
		cmw.codecCtx.EnableCompress()
	}
	return cmw
}

func (cmw *ChunkMetaWriter) Reset() {
	cmw.size = 0
	cmw.metaIndexItems = cmw.metaIndexItems[:0]
}

func (cmw *ChunkMetaWriter) FlushMetaIndex(w fileops.FileWriter) (int, error) {
	wn := 0
	metaIndexItems := cmw.metaIndexItems
	for i := range metaIndexItems {
		m := &metaIndexItems[i]
		cmw.swap = m.marshal(cmw.swap[:0])
		n, err := w.WriteData(cmw.swap)
		if err != nil {
			logger.GetLogger().Error("write meta index fail", zap.String("name", w.Name()), zap.Error(err))
			return wn, err
		}
		wn += n
	}
	return wn, nil
}

func (cmw *ChunkMetaWriter) FlushChunkMeta(w fileops.FileWriter) (int, error) {
	cmw.buf = numberenc.MarshalUint32SliceAppend(cmw.buf, cmw.offsets)

	cmw.metaIndex.size = uint32(len(cmw.buf))
	cmw.metaIndex.offset = w.DataSize()
	cmw.metaIndexItems = append(cmw.metaIndexItems, cmw.metaIndex)
	cmw.metaIndex.reset()
	cmw.offsets = cmw.offsets[:0]

	n, err := w.WriteData(cmw.buf)
	cmw.buf = cmw.buf[:0]
	cmw.size += int64(n)

	return n, err
}

func (cmw *ChunkMetaWriter) UpdateMetaIndex(minT, maxT int64, sid uint64) {
	if cmw.metaIndex.count == 0 {
		cmw.metaIndex.id = sid
	}

	if cmw.metaIndex.minTime > minT {
		cmw.metaIndex.minTime = minT
	}
	if cmw.metaIndex.maxTime < maxT {
		cmw.metaIndex.maxTime = maxT
	}
}

func (cmw *ChunkMetaWriter) Write(cm *ChunkMeta) (int, error) {
	cmw.metaIndex.count++
	buf, err := MarshalChunkMeta(cmw.codecCtx, cm, cmw.swap[:0])
	if err != nil {
		return 0, err
	}

	cmw.swap = buf
	cmw.offsets = append(cmw.offsets, uint32(len(cmw.buf)))

	cmw.buf = append(cmw.buf, buf...)

	return len(buf), err
}

func (cmw *ChunkMetaWriter) MemorySize() int {
	return len(cmw.buf)
}

func (cmw *ChunkMetaWriter) MemoryCount() int {
	return int(cmw.metaIndex.count)
}

func (cmw *ChunkMetaWriter) Size() int64 {
	return cmw.size
}

func (cmw *ChunkMetaWriter) MetaIndexCount() int {
	return len(cmw.metaIndexItems)
}

func (cmw *ChunkMetaWriter) CompressEnabled() bool {
	return cmw.codecCtx != nil && cmw.codecCtx.CompressEnabled()
}

func (cmw *ChunkMetaWriter) GetChunkMetaHeader() *ChunkMetaHeader {
	if cmw.codecCtx == nil {
		return nil
	}
	return cmw.codecCtx.GetHeader()
}
