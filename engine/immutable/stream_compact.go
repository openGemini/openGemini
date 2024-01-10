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
	"container/heap"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	readBufferSize                = 1 * 1024 * 1024
	streamCompactMemThreshold     = 128 * 1024 * 1024
	streamCompactSegmentThreshold = 500
)

var (
	CLog             = Log.NewLogger(errno.ModuleCompact)
	fileIteratorPool = sync.Pool{}
	errEmptyFile     = fmt.Errorf("empty tssp file")
)

func NonStreamingCompaction(fi FilesInfo) bool {
	flag := GetMergeFlag4TsStore()
	if flag == NonStreamingCompact {
		return true
	} else if flag == StreamingCompact {
		return false
	} else {
		n := fi.avgChunkRows * fi.maxColumns * 8 * len(fi.compIts)
		if n >= streamCompactMemThreshold {
			return false
		}

		if fi.maxChunkRows > GetMaxRowsPerSegment4TsStore()*streamCompactSegmentThreshold {
			return false
		}

		return true
	}
}

type StreamIterator struct {
	*FileIterator

	col     *record.ColVal
	timeCol *record.ColVal
}

func NewStreamStreamIterator(fi *FileIterator) *StreamIterator {
	itr := &StreamIterator{
		FileIterator: fi,
		col:          &record.ColVal{},
		timeCol:      &record.ColVal{},
	}

	return itr
}

var streamIteratorsPool = NewStreamIteratorsPool(cpu.GetCpuNum() / 2)

type StreamIteratorsPool struct {
	cache chan *StreamIterators
	pool  sync.Pool
}

func NewStreamIteratorsPool(n int) *StreamIteratorsPool {
	if n < 2 {
		n = 2
	}
	if n > cpu.GetCpuNum() {
		n = cpu.GetCpuNum()
	}
	return &StreamIteratorsPool{
		cache: make(chan *StreamIterators, n),
	}
}

func getStreamIterators() *StreamIterators {
	select {
	case itr := <-streamIteratorsPool.cache:
		return itr
	default:
		v := streamIteratorsPool.pool.Get()
		if v == nil {
			return &StreamIterators{
				ctx:        NewReadContext(true),
				colBuilder: NewColumnBuilder(),
				col:        &record.ColVal{},
				tmpCol:     &record.ColVal{},
				timeCol:    &record.ColVal{},
				tmpTimeCol: &record.ColVal{},
				Conf:       GetTsStoreConfig(),
				schemaMap:  make(map[string]struct{}),
			}
		}
		return v.(*StreamIterators)
	}
}

func putStreamIterators(itr *StreamIterators) {
	select {
	case streamIteratorsPool.cache <- itr:
	default:
		streamIteratorsPool.pool.Put(itr)
	}
}

type StreamIterators struct {
	closed        chan struct{}
	dropping      *int64
	dir           string
	name          string // measurement name with version
	lock          *string
	itrs          []*StreamIterator
	chunkItrs     []*StreamIterator
	segmentIndex  int
	iteratorStart int
	estimateSize  int
	maxN          int
	fields        record.Schemas

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
	tier       uint64
	dstMeta    ChunkMeta
	schemaMap  map[string]struct{}
	fileName   TSSPFileName
	fileSize   int64

	encChunkMeta      []byte
	chunkMetaBlocks   [][]byte
	encChunkIndexMeta []byte
	encIdTime         []byte
	crc               uint32
	version           uint64
	chunkRows         int64
	maxChunkRows      int64

	chunkSegments   int
	cmOffset        []uint32
	currentCMOffset int

	lastSeg    record.Record
	col        *record.ColVal
	tmpCol     *record.ColVal
	timeCol    *record.ColVal
	tmpTimeCol *record.ColVal
	colSegs    []record.ColVal
	timeSegs   []record.ColVal
	files      []TSSPFile
	log        *Log.Logger
}

func (c *StreamIterators) WithLog(log *Log.Logger) {
	c.log = log
	for i := range c.itrs {
		c.itrs[i].WithLog(log)
	}
}

func (c *StreamIterators) Len() int      { return len(c.itrs) }
func (c *StreamIterators) Swap(i, j int) { c.itrs[i], c.itrs[j] = c.itrs[j], c.itrs[i] }
func (c *StreamIterators) Less(i, j int) bool {
	im := c.itrs[i].curtChunkMeta
	jm := c.itrs[j].curtChunkMeta
	iID := im.sid
	jID := jm.sid
	if iID != jID {
		return iID < jID
	}

	iTm := im.minTime()
	jTm := jm.minTime()

	return iTm < jTm
}

func (c *StreamIterators) Push(v interface{}) {
	c.itrs = append(c.itrs, v.(*StreamIterator))
}

func (c *StreamIterators) Pop() interface{} {
	l := len(c.itrs)
	v := c.itrs[l-1]
	c.itrs = c.itrs[:l-1]
	return v
}

func (c *StreamIterators) Close() {
	for _, itr := range c.itrs {
		itr.Close()
	}
	c.col.Init()
	c.tmpCol.Init()
	c.timeCol.Init()
	c.tmpTimeCol.Init()
	c.itrs = c.itrs[:0]
	c.chunkItrs = c.chunkItrs[:0]
	c.chunkSegments = 0
	c.ctx.preAggBuilders.reset()
	c.files = c.files[:0]
	c.resetSchemaMap()
	c.fileSize = 0
	c.colBuilder.resetPreAgg()
	putStreamIterators(c)
}

func (c *StreamIterators) swapLastTimeSegment(col *record.ColVal) {
	if col == nil {
		return
	}
	c.tmpTimeCol.Init()
	times := col.IntegerValues()
	for _, t := range times {
		c.tmpTimeCol.AppendInteger(t)
	}
	c.timeCol, c.tmpTimeCol = c.tmpTimeCol, c.timeCol
	c.tmpTimeCol.Init()
}

func (c *StreamIterators) swapLastSegment(ref *record.Field, col *record.ColVal) {
	c.tmpCol.Init()
	switch ref.Type {
	case influx.Field_Type_Int:
		for i := 0; i < col.Len; i++ {
			v, isNil := col.IntegerValue(i)
			if isNil {
				c.tmpCol.AppendIntegerNull()
			} else {
				c.tmpCol.AppendInteger(v)
			}
		}
	case influx.Field_Type_String:
		for i := 0; i < col.Len; i++ {
			v, isNil := col.StringValueUnsafe(i)
			if isNil {
				c.tmpCol.AppendStringNull()
			} else {
				c.tmpCol.AppendString(v)
			}
		}
	case influx.Field_Type_Boolean:
		for i := 0; i < col.Len; i++ {
			v, isNil := col.BooleanValue(i)
			if isNil {
				c.tmpCol.AppendBooleanNull()
			} else {
				c.tmpCol.AppendBoolean(v)
			}
		}
	case influx.Field_Type_Float:
		for i := 0; i < col.Len; i++ {
			v, isNil := col.FloatValue(i)
			if isNil {
				c.tmpCol.AppendFloatNull()
			} else {
				c.tmpCol.AppendFloat(v)
			}
		}
	}

	c.col, c.tmpCol = c.tmpCol, c.col
	c.tmpCol.Init()
}

func (c *StreamIterators) mergeSchema(m *ChunkMeta) {
	for i := 0; i < len(m.colMeta)-1; i++ {
		cm := &m.colMeta[i]
		if _, ok := c.schemaMap[cm.Name()]; !ok {
			size := c.fields.Len()
			if cap(c.fields) <= size {
				c.fields = append(c.fields, record.Field{})
			} else {
				c.fields = c.fields[:size+1]
			}

			c.fields[size].Name = cm.Name()
			c.fields[size].Type = int(cm.ty)
			c.schemaMap[cm.Name()] = struct{}{}
		}
	}
}

func (c *StreamIterators) genChunkSchema() {
	c.fields = c.fields[:0]
	itr := heap.Pop(c).(*StreamIterator)
	c.resetSchemaMap()
	c.mergeSchema(itr.curtChunkMeta)
	id := itr.curtChunkMeta.sid
	itr.col.Init()
	c.chunkSegments = len(itr.curtChunkMeta.timeRange)
	c.chunkItrs = append(c.chunkItrs[:0], itr)

	for c.Len() > 0 {
		itr = heap.Pop(c).(*StreamIterator)
		if id == itr.curtChunkMeta.sid {
			c.mergeSchema(itr.curtChunkMeta)
			itr.col.Init()
			c.chunkSegments += len(itr.curtChunkMeta.timeRange)
			c.chunkItrs = append(c.chunkItrs, itr)
		} else {
			heap.Push(c, itr)
			break
		}
	}

	sort.Sort(c.fields)
	c.fields = append(c.fields, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	c.lastSeg.Reset()
	c.lastSeg.SetSchema(c.fields)
	c.lastSeg.ReserveColVal(c.fields.Len())
}

func (c *StreamIterators) resetSchemaMap() {
	for k := range c.schemaMap {
		delete(c.schemaMap, k)
	}
}

func (c *StreamIterators) Init(id uint64, chunkDataOffset int64, schema record.Schemas) {
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
	c.crc = 0
	c.colBuilder.resetPreAgg()
}

func (m *MmsTables) NewStreamIterators(group FilesInfo) (*StreamIterators, error) {
	compItrs := getStreamIterators()
	compItrs.closed = m.closed
	compItrs.dropping = group.dropping
	compItrs.name = group.name
	compItrs.dir = m.path
	compItrs.lock = m.lock
	compItrs.pair.Reset(group.name)
	compItrs.Conf = m.Conf
	compItrs.itrs = compItrs.itrs[:0]
	for _, fi := range group.compIts {
		itr := NewStreamStreamIterator(fi)
		compItrs.itrs = append(compItrs.itrs, itr)
	}
	compItrs.maxN = group.maxChunkN
	compItrs.estimateSize = group.estimateSize
	compItrs.chunkRows = 0
	compItrs.maxChunkRows = 0

	heap.Init(compItrs)

	return compItrs, nil
}

func (c *StreamIterators) cacheMetaInMemory() bool {
	if c.tier == util.Hot {
		return c.Conf.cacheMetaData
	} else if c.tier == util.Warm {
		return false
	}

	return c.Conf.cacheMetaData
}

func (c *StreamIterators) cacheDataInMemory() bool {
	if c.tier == util.Hot {
		return c.Conf.cacheDataBlock
	} else if c.tier == util.Warm {
		return false
	}

	return c.Conf.cacheDataBlock
}

func (c *StreamIterators) reset() {
	c.colBuilder.resetPreAgg()
	c.trailer.reset()
	c.mIndex.reset()
	c.TableData.reset()
	c.pair.Reset(c.name)
	c.cmOffset = c.cmOffset[:0]
	c.currentCMOffset = 0
}

func (c *StreamIterators) SetWriter(w fileops.FileWriter) {
	c.writer = w
}

func (c *StreamIterators) NewFile(addFileExt bool) error {
	if addFileExt {
		c.fileName.extent++
	}
	c.reset()
	c.trailer.name = append(c.trailer.name[:0], influx.GetOriginMstName(c.name)...)
	c.inMemBlock = emptyMemReader
	if c.cacheDataInMemory() || c.cacheMetaInMemory() {
		idx := calcBlockIndex(c.estimateSize)
		c.inMemBlock = NewMemoryReader(blockSize[idx])
	}

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
	c.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}

	limit := c.fileName.level > 0
	if c.cacheMetaInMemory() {
		c.writer = newTsspFileWriter(c.fd, true, limit, c.lock)
	} else {
		c.writer = newTsspFileWriter(c.fd, false, limit, c.lock)
	}

	var buf [16]byte
	b := append(buf[:0], tableMagic...)
	b = numberenc.MarshalUint64Append(b, version)
	_, err = c.writer.WriteData(b)
	if err != nil {
		log.Error("write file fail", zap.String("name", filePath), zap.Error(err))
		return err
	}

	if c.cacheDataInMemory() {
		c.inMemBlock.AppendDataBlock(b)
	}

	c.version = version
	c.keys = make(map[uint64]struct{}, c.maxN)
	if !addFileExt {
		c.files = c.files[:0]
	}

	return nil
}

func (c *StreamIterators) Size() int64 {
	if c.writer != nil && c.writer.DataSize() <= 16 {
		return 0
	}
	n := c.writer.DataSize()
	n += c.writer.ChunkMetaSize()
	n += int64(len(c.metaIndexItems) * MetaIndexLen)
	bm, _ := bloom.Estimate(uint64(len(c.keys)), falsePositive)
	bmBytes := pow2((bm + 7) / 8)
	n += int64(bmBytes) + int64(trailerSize+len(c.name))
	n += int64(c.pair.Len()*3*8) / 2 // assuming the compression ratio is 50%
	return n
}

func (c *StreamIterators) updateChunkStat(id uint64, maxT int64) {
	rows := c.colBuilder.timePreAggBuilder.count()
	c.pair.Add(id, maxT)
	c.pair.AddRowCounts(rows)
	c.chunkRows += rows
	if rows > c.maxChunkRows {
		c.maxChunkRows = rows
	}
}

func (c *StreamIterators) writeMetaToDisk() error {
	cm := &c.dstMeta
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

	c.updateChunkStat(cm.sid, maxT)
	c.keys[cm.sid] = struct{}{}

	cm.validation()

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

func (c *StreamIterators) SwitchChunkMeta() error {
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

func (c *StreamIterators) WriteChunkMeta(cm *ChunkMeta) (int, error) {
	c.encChunkMeta = cm.marshal(c.encChunkMeta[:0])
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
		c.log.Error("write chunk meta fail", zap.String("file", c.writer.Name()), zap.Error(err))
	}
	return wn, err
}

func (c *StreamIterators) FileVersion() uint64 {
	return c.version
}

func (c *StreamIterators) removeEmptyFile() {
	if c.writer != nil && c.fd != nil {
		_ = c.writer.Close()
		name := c.fd.Name()
		_ = c.fd.Close()
		lock := fileops.FileLockOption(*c.lock)
		_ = fileops.Remove(name, lock)
		c.fd = nil
		c.writer = nil
	}
}

func (c *StreamIterators) NewTSSPFile(tmp bool) (TSSPFile, error) {
	if err := c.Flush(); err != nil {
		if err == errEmptyFile {
			return nil, nil
		}
		c.log.Error("flush error", zap.String("name", c.fd.Name()), zap.Error(err))
		return nil, err
	}

	dr, err := CreateTSSPFileReader(c.fileSize, c.fd, &c.trailer, &c.TableData, c.FileVersion(), tmp, c.lock)
	if err != nil {
		c.log.Error("create tssp file reader fail", zap.String("name", dr.FileName()), zap.Error(err))
		return nil, err
	}

	dr.maxChunkRows = int(c.maxChunkRows)
	dr.avgChunkRows = int(c.chunkRows / dr.trailer.idCount)
	if dr.avgChunkRows < 1 {
		dr.avgChunkRows = 1
	}

	size := dr.InMemSize()
	if c.fileName.order {
		addMemSize(levelName(c.fileName.level), size, size, 0)
	} else {
		addMemSize(levelName(c.fileName.level), size, 0, size)
	}

	return &tsspFile{
		name:   c.fileName,
		reader: dr,
		ref:    1,
		lock:   c.lock,
	}, nil
}

func (c *StreamIterators) genBloomFilter() {
	bm, bk := bloom.Estimate(uint64(len(c.keys)), falsePositive)
	bmBytes := pow2((bm + 7) / 8)
	if uint64(cap(c.bloomFilter)) < bmBytes {
		c.bloomFilter = make([]byte, bmBytes)
	} else {
		c.bloomFilter = c.bloomFilter[:bmBytes]
		util.MemorySet(c.bloomFilter)
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

func (c *StreamIterators) Flush() error {
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
	if c.cacheMetaInMemory() {
		c.chunkMetaBlocks = c.writer.MetaDataBlocks(c.chunkMetaBlocks[:0])
		c.inMemBlock.SetMetaBlocks(c.chunkMetaBlocks)
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

	c.encIdTime = c.pair.Marshal(c.fileName.order, c.encIdTime[:0], c.colBuilder.coder)
	c.trailer.idTimeSize = int64(len(c.encIdTime))
	if _, err := c.writer.WriteData(c.encIdTime); err != nil {
		c.log.Error("write id time data fail", zap.String("name", c.fd.Name()), zap.Error(err))
		return err
	}
	c.trailer.SetChunkMetaCompressFlag()
	c.trailerData = c.trailer.marshal(c.trailerData[:0])

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
	c.writer = nil

	return nil
}

func (c *StreamIterators) isClosed() bool {
	if atomic.LoadInt64(c.dropping) > 0 {
		return true
	}
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *StreamIterators) compactColumn(dstIdx int, ref record.Field, needCalPreAgg bool, fieldIndex []int) (iteratorStart, segmentIndex int, splitFile bool, err error) {
	// merge column ref
	_ = c.colBuilder.initEncoder(ref)
	splitFile = false
	id := c.chunkItrs[c.iteratorStart].curtChunkMeta.sid
	segmentN := 0
	lastSegRows := c.lastSeg.RowNums()
	if lastSegRows > 0 {
		idx := c.lastSeg.FieldIndexs(ref.Name)
		col := c.lastSeg.Column(idx)
		c.col.AppendColVal(col, ref.Type, 0, col.Len)
		if needCalPreAgg {
			tmCol := c.lastSeg.TimeColumn()
			c.timeCol.AppendColVal(tmCol, influx.Field_Type_Int, 0, tmCol.Len)
		}
		col.Init()
	}

	var rowCount = 0
	var maxRows = GetMaxRowsPerSegment4TsStore()

	for itrIndex := c.iteratorStart; itrIndex < len(c.chunkItrs); itrIndex++ {
		itr := c.chunkItrs[itrIndex]
		if c.isClosed() {
			err = ErrCompStopped
			return
		}

		srcMeta := itr.curtChunkMeta
		tm := srcMeta.TimeMeta()
		var srcColMeta *ColumnMeta

		c.colBuilder.cm = &c.dstMeta
		c.colBuilder.colMeta = &c.dstMeta.colMeta[dstIdx]

		idx := fieldIndex[itrIndex]
		if idx >= 0 {
			srcColMeta = &srcMeta.colMeta[idx]
		} else {
			rowCount = srcMeta.Rows(c.ctx.preAggBuilders.timeBuilder)
		}

		// merge full segments(full segment: rows in segment EQ 1000)
		for segIndex := c.segmentIndex; segIndex < len(tm.entries); segIndex++ {
			if c.isClosed() {
				err = ErrCompStopped
				return
			}

			if idx >= 0 {
				colSeg := &srcColMeta.entries[segIndex]
				if !needCalPreAgg {
					segData, er := itr.readData(colSeg.offset, colSeg.size)
					if er != nil {
						err = er
						return
					}
					if err = c.decodeSegment(segData, nil, ref); err != nil {
						return
					}
				} else {
					colData, er := itr.readData(colSeg.offset, colSeg.size)
					if er != nil {
						err = er
						return
					}

					tmSeg := &tm.entries[segIndex]
					tmData, er := itr.readTimeData(tmSeg.offset, tmSeg.size)
					if er != nil {
						err = er
						return
					}

					if err = c.decodeSegment(colData, tmData, ref); err != nil {
						return
					}
				}
			} else {
				if rowCount > maxRows {
					c.col.AppendColVal(newNilCol(maxRows, &ref), ref.Type, 0, maxRows)
					rowCount -= maxRows
				} else {
					c.col.AppendColVal(newNilCol(rowCount, &ref), ref.Type, 0, rowCount)
				}
			}

			if c.continueMerge(segIndex, itrIndex, tm) {
				break
			}

			if err = c.writeSegment(srcMeta.sid, ref, needCalPreAgg); err != nil {
				return
			}
			segmentN++
			if segmentN >= c.Conf.maxSegmentLimit {
				iteratorStart, segmentIndex, splitFile = c.nextSegmentPosition(itrIndex, segIndex, tm)
				if splitFile {
					c.saveSegment(ref)
					return
				}
			}

			// merge last segment
			if c.lastSegment(itrIndex, segIndex, segmentN, tm) {
				if err = c.writeSegment(itr.curtChunkMeta.sid, ref, needCalPreAgg); err != nil {
					return
				}
				segmentN++
			}
		}
	}

	err = c.writeLastSegment(segmentN, ref, id)

	return
}

func (c *StreamIterators) writeLastSegment(segmentN int, ref record.Field, id uint64) error {
	if c.col.Len > 0 {
		if segmentN < c.Conf.maxSegmentLimit {
			if err := c.writeSegment(id, ref, false); err != nil {
				return err
			}
		} else {
			c.saveSegment(ref)
		}
		c.timeCol.Init()
	}
	return nil
}

func (c *StreamIterators) writeCrc(crc []byte) error {
	if _, err := c.writer.WriteData(crc[:]); err != nil {
		return err
	}

	if c.cacheDataInMemory() {
		c.inMemBlock.AppendDataBlock(crc)
	}

	return nil
}

func (c *StreamIterators) compact(files []TSSPFile, level uint16, isOrder bool) ([]TSSPFile, error) {
	var crc [4]byte
	_, seq := files[0].LevelAndSequence()
	c.fileName = NewTSSPFileName(seq, level, 0, 0, isOrder, c.lock)
	if err := c.NewFile(false); err != nil {
		panic(err)
	}

	for c.Len() > 0 {
		// merge one chunk
		var err error
		splitFile := true
		iteratorStart, segmentIndex := 0, 0
		c.genChunkSchema()
		fieldIndex := make([]int, len(c.chunkItrs))
		itrFieldIndex := make([]int, len(c.chunkItrs))
		for splitFile {
			c.iteratorStart, c.segmentIndex = iteratorStart, segmentIndex
			dOff := c.writer.DataSize()
			id := c.chunkItrs[0].curtChunkMeta.sid
			c.Init(id, dOff, c.fields)
			resetItrFieldIndex(itrFieldIndex)
			for dstIdx, ref := range c.fields {
				if c.isClosed() {
					return nil, ErrCompStopped
				}

				//pad crc
				if err := c.writeCrc(crc[:]); err != nil {
					return nil, err
				}

				// calculate fieldIndex first
				c.columnIdxForIters(fieldIndex, itrFieldIndex, &ref)

				iteratorStart, segmentIndex, splitFile, err = c.compactColumn(dstIdx, ref, c.chunkSegments > c.Conf.maxSegmentLimit, fieldIndex)
				if err != nil {
					return nil, err
				}

				if err = c.genColumnPreAgg(c.colBuilder.colMeta, fieldIndex); err != nil {
					return nil, err
				}
			}

			record.CheckRecord(&c.lastSeg)
			if err = c.writeMetaToDisk(); err != nil {
				return nil, err
			}
			// check whether the file size exceeds the limit.
			fSize := c.Size()
			if fSize >= c.Conf.fileSizeLimit || splitFile {
				f, err := c.NewTSSPFile(true)
				if err != nil {
					c.log.Error("new tssp file fail", zap.Error(err))
					return nil, err
				}

				c.log.Info("switch tssp file",
					zap.String("file", f.Path()),
					zap.Int("rowsLimit", c.Conf.maxSegmentLimit*c.Conf.maxRowsPerSegment),
					zap.Int64("fileSize", fSize),
					zap.Int64("sizeLimit", c.Conf.fileSizeLimit))

				c.files = append(c.files, f)
				c.estimateSize -= int(f.FileSize())
				if err = c.NewFile(true); err != nil {
					panic(err)
				}
			}
		}

		c.nextChunk()
	}

	if c.Size() > 0 {
		f, err := c.NewTSSPFile(true)
		if err != nil {
			c.log.Error("new tssp file fail", zap.Error(err))
			return nil, err
		}
		if f != nil {
			c.files = append(c.files, f)
		}
	} else {
		c.removeEmptyFile()
	}

	newFiles := make([]TSSPFile, 0, len(c.itrs))
	newFiles = append(newFiles, c.files...)
	return newFiles, nil
}

func (c *StreamIterators) columnIdxForIters(fieldIndex, itrFieldIndex []int, ref *record.Field) {
	for i := range c.chunkItrs {
		fieldIndex[i] = c.chunkItrs[i].curtChunkMeta.columnIndexFastPath(ref, itrFieldIndex[i])
		if fieldIndex[i] != -1 {
			itrFieldIndex[i] = fieldIndex[i] + 1
		}
	}
}

func resetItrFieldIndex(itrFieldIndex []int) {
	for i := range itrFieldIndex {
		itrFieldIndex[i] = 0
	}
}

func (c *StreamIterators) nextChunk() {
	for _, itr := range c.chunkItrs {
		itr.curtChunkMeta = nil
		itr.chunkUsed++
		if itr.NextChunkMeta() {
			heap.Push(c, itr)
		} else {
			itr.Close()
		}
	}
}

func (c *StreamIterators) splitColumn(ref record.Field, needCalPreAgg bool) (splitCol bool, lastTmSeg *record.ColVal) {
	maxRow := c.Conf.maxRowsPerSegment
	splitCol = c.col.Len > maxRow
	if splitCol {
		c.colSegs = c.col.Split(c.colSegs[:0], maxRow, ref.Type)
		if len(c.colSegs) != 2 {
			panic(fmt.Sprintf("len(c.colSegs) != 2, got: %d", len(c.colSegs)))
		}
		if needCalPreAgg {
			if ref.Name != record.TimeField {
				c.timeSegs = c.timeCol.Split(c.timeSegs[:0], maxRow, influx.Field_Type_Int)
			} else {
				c.timeSegs = append(c.timeSegs[:0], c.colSegs...)
			}
			lastTmSeg = &c.timeSegs[1]
		}
	} else {
		c.colSegs = append(c.colSegs[:0], *c.col)
		if needCalPreAgg {
			if ref.Name != record.TimeField {
				c.timeSegs = append(c.timeSegs[:0], *c.timeCol)
			}
		}
	}
	return
}

func (c *StreamIterators) validate(id uint64, ref record.Field) error {
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

func (c *StreamIterators) writeSegment(id uint64, ref record.Field, needCalPreAgg bool) error {
	if err := c.validate(id, ref); err != nil {
		return err
	}

	splitCol, lastTmSeg := c.splitColumn(ref, needCalPreAgg)
	var err error

	cols := c.colSegs[:1]
	off := c.writer.DataSize()
	c.colBuilder.data = c.colBuilder.data[:0]
	if ref.Name == record.TimeField {
		err = c.colBuilder.encodeTimeColumn(cols, off)
	} else {
		if needCalPreAgg {
			tmCols := c.timeSegs[:1]
			err = c.colBuilder.encodeColumn(cols, tmCols, off, ref)
		} else {
			err = c.colBuilder.encodeColumn(cols, nil, off, ref)
		}
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

	if c.cacheDataInMemory() {
		c.inMemBlock.AppendDataBlock(c.colBuilder.data)
	}

	if splitCol {
		if needCalPreAgg && ref.Name != record.TimeField {
			c.swapLastTimeSegment(lastTmSeg)
		}
		c.swapLastSegment(&ref, &c.colSegs[1])
	} else {
		c.col.Init()
		c.timeCol.Init()
	}

	return nil
}

func (b *ColumnBuilder) encodeTimeColumn(cols []record.ColVal, offset int64) error {
	var err error
	for _, col := range cols {
		times := col.IntegerValues()
		b.timePreAggBuilder.addValues(nil, times)
		b.colMeta.growEntry()
		m := &b.colMeta.entries[len(b.colMeta.entries)-1]
		b.cm.growTimeRangeEntry()
		b.cm.timeRange[len(b.cm.timeRange)-1].setMinTime(times[0])
		b.cm.timeRange[len(b.cm.timeRange)-1].setMaxTime(times[len(times)-1])
		m.setOffset(offset)

		pos := len(b.data)
		b.data = EncodeColumnHeader(&col, b.data, encoding.BlockInteger)
		b.data, err = encoding.EncodeTimestampBlock(col.Val, b.data, b.coder)
		if err != nil {
			b.log.Error("encode integer value fail", zap.Error(err))
			return err
		}

		m.setOffset(offset)
		size := uint32(len(b.data) - pos)
		m.setSize(size)
		offset += int64(size)
	}

	return nil
}

func (b *ColumnBuilder) encodeColumn(segCols []record.ColVal, tmCols []record.ColVal, offset int64, ref record.Field) error {
	var err error
	for i := range segCols {
		segCol := &segCols[i]

		b.colMeta.growEntry()
		m := &b.colMeta.entries[len(b.colMeta.entries)-1]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = EncodeColumnHeader(segCol, b.data, uint8(ref.Type))

		switch ref.Type {
		case influx.Field_Type_String:
			b.data, err = encoding.EncodeStringBlock(segCol.Val, segCol.Offset, b.data, b.coder)
			if len(tmCols) != 0 {
				b.stringPreAggBuilder.addValues(segCol, tmCols[i].IntegerValues())
			}
		case influx.Field_Type_Boolean:
			b.data, err = encoding.EncodeBooleanBlock(segCol.Val, b.data, b.coder)
			if len(tmCols) != 0 {
				b.boolPreAggBuilder.addValues(segCol, tmCols[i].IntegerValues())
			}
		case influx.Field_Type_Float:
			b.data, err = encoding.EncodeFloatBlock(segCol.Val, b.data, b.coder)
			if len(tmCols) != 0 {
				b.floatPreAggBuilder.addValues(segCol, tmCols[i].IntegerValues())
			}
		case influx.Field_Type_Int:
			b.data, err = encoding.EncodeIntegerBlock(segCol.Val, b.data, b.coder)
			if len(tmCols) != 0 {
				b.intPreAggBuilder.addValues(segCol, tmCols[i].IntegerValues())
			}
		default:
			panic(ref)
		}
		if err != nil {
			b.log.Error("encode integer value fail", zap.Error(err))
			return err
		}
		size := uint32(len(b.data) - pos)
		m.setSize(size)
		offset += int64(size)
	}

	return err
}

func (c *StreamIterators) mergeTimePreAgg(cm *ColumnMeta) error {
	ab := c.colBuilder.timePreAggBuilder
	cm.preAgg = ab.marshal(cm.preAgg[:0])

	return nil
}

func (c *StreamIterators) mergeIntegerPreAgg(cm *ColumnMeta, ref *record.Field, fieldIndex []int) error {
	ab := c.colBuilder.intPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.intBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := fieldIndex[i]
		if idx >= 0 {
			srcMeta := &itr.curtChunkMeta.colMeta[idx]
			ab.reset()
			if i == 0 {
				if _, err := aggBuilder.unmarshal(srcMeta.preAgg); err != nil {
					c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
					return err
				}
				continue
			} else {
				if _, err := ab.unmarshal(srcMeta.preAgg); err != nil {
					c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
					return err
				}
			}

			v, t := ab.min()
			min := v.(int64)
			aggBuilder.addMin(float64(min), t)
			v, t = ab.max()
			max := v.(int64)
			aggBuilder.addMax(float64(max), t)
			aggBuilder.addCount(ab.count())
			vv := ab.sum().(int64)
			aggBuilder.addSum(float64(vv))
		}
	}

	cm.preAgg = aggBuilder.marshal(cm.preAgg[:0])
	return nil
}

func (c *StreamIterators) mergeFloatPreAgg(cm *ColumnMeta, ref *record.Field, fieldIndex []int) error {
	ab := c.colBuilder.floatPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.floatBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := fieldIndex[i]
		if idx >= 0 {
			srcMeta := &itr.curtChunkMeta.colMeta[idx]
			ab.reset()
			if i == 0 {
				if _, err := aggBuilder.unmarshal(srcMeta.preAgg); err != nil {
					c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
					return err
				}
				continue
			}

			if _, err := ab.unmarshal(srcMeta.preAgg); err != nil {
				c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
				return err
			}
			v, t := ab.min()
			min := v.(float64)
			aggBuilder.addMin(min, t)
			v, t = ab.max()
			max := v.(float64)
			aggBuilder.addMax(max, t)
			aggBuilder.addCount(ab.count())
			vv := ab.sum().(float64)
			aggBuilder.addSum(vv)
		}
	}
	cm.preAgg = aggBuilder.marshal(cm.preAgg[:0])
	return nil
}

func (c *StreamIterators) mergeStringPreAgg(cm *ColumnMeta, ref *record.Field, fieldIndex []int) error {
	ab := c.colBuilder.stringPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.stringBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := fieldIndex[i]
		if idx >= 0 {
			srcMeta := &itr.curtChunkMeta.colMeta[idx]
			ab.reset()
			if i == 0 {
				if _, err := aggBuilder.unmarshal(srcMeta.preAgg); err != nil {
					c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
					return err
				}
				continue
			}
			if _, err := ab.unmarshal(srcMeta.preAgg); err != nil {
				c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
				return err
			}

			aggBuilder.addCount(ab.count())
		}
	}
	cm.preAgg = aggBuilder.marshal(cm.preAgg[:0])
	return nil
}

func (c *StreamIterators) mergeBooleanPreAgg(cm *ColumnMeta, ref *record.Field, fieldIndex []int) error {
	ab := c.colBuilder.boolPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.boolBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := fieldIndex[i]
		if idx >= 0 {
			srcMeta := &itr.curtChunkMeta.colMeta[idx]
			ab.reset()
			if i == 0 {
				if _, err := aggBuilder.unmarshal(srcMeta.preAgg); err != nil {
					c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
					return err
				}
				continue
			}
			if _, err := ab.unmarshal(srcMeta.preAgg); err != nil {
				c.log.Error("unmarshal preagg fail", zap.String("column", ref.String()))
				return err
			}

			bv := float64(0)
			v, t := ab.min()
			min := v.(bool)
			if min {
				bv = 1
			}
			aggBuilder.addMin(bv, t)
			v, t = ab.max()
			max := v.(bool)
			bv = 0
			if max {
				bv = 1
			}
			aggBuilder.addMax(bv, t)
			aggBuilder.addCount(ab.count())
		}
	}
	cm.preAgg = aggBuilder.marshal(cm.preAgg[:0])
	return nil
}

func (c *StreamIterators) genColumnPreAgg(cm *ColumnMeta, fieldIndex []int) error {
	var err error
	ref := &record.Field{Name: cm.Name(), Type: int(cm.ty)}
	switch int(cm.ty) {
	case influx.Field_Type_Int:
		if cm.IsTime() {
			err = c.mergeTimePreAgg(cm)
		} else {
			err = c.mergeIntegerPreAgg(cm, ref, fieldIndex)
		}
	case influx.Field_Type_Float:
		err = c.mergeFloatPreAgg(cm, ref, fieldIndex)
	case influx.Field_Type_Boolean:
		err = c.mergeBooleanPreAgg(cm, ref, fieldIndex)
	case influx.Field_Type_String:
		err = c.mergeStringPreAgg(cm, ref, fieldIndex)
	default:
		err = fmt.Errorf("unknown column data type, %v::%v", cm.Name(), cm.ty)
	}
	return err
}

func (c *StreamIterators) decodeSegment(colData []byte, tmData []byte, ref record.Field) error {
	var err error
	if len(tmData) > 0 && ref.Name != record.TimeField {
		if err = appendTimeColumnData(tmData, c.tmpTimeCol, c.ctx, false); err != nil {
			c.log.Error("decode time column fail", zap.Error(err))
			return err
		}
	}

	if ref.Name == record.TimeField {
		err = appendTimeColumnData(colData, c.tmpCol, c.ctx, false)
	} else {
		err = decodeColumnData(&ref, colData, c.tmpCol, c.ctx, false)
	}
	if err != nil {
		c.log.Error("decode time fail", zap.String("field", ref.String()), zap.Error(err))
		return err
	}

	if c.col.Len == 0 {
		c.col, c.tmpCol = c.tmpCol, c.col
		c.timeCol, c.tmpTimeCol = c.tmpTimeCol, c.timeCol
	} else {
		c.col.AppendColVal(c.tmpCol, ref.Type, 0, c.tmpCol.Len)
		if c.tmpTimeCol.Len > 0 {
			c.timeCol.AppendColVal(c.tmpTimeCol, influx.Field_Type_Int, 0, c.tmpTimeCol.Len)
		}
	}
	c.tmpCol.Init()
	c.tmpTimeCol.Init()

	return nil
}

func (c *StreamIterators) continueMerge(segIndex, itrIndex int, tm *ColumnMeta) bool {
	return segIndex == len(tm.entries)-1 && itrIndex < len(c.chunkItrs)-1 && c.col.Len < c.Conf.maxRowsPerSegment
}

func (c *StreamIterators) lastSegment(itrIndex, segIndex int, segmentN int, tm *ColumnMeta) bool {
	return itrIndex == len(c.chunkItrs)-1 && segIndex == len(tm.entries)-1 && c.col.Len > 0 && segmentN < c.Conf.maxSegmentLimit
}

func (c *StreamIterators) nextSegmentPosition(itrIndex, segIndex int, tm *ColumnMeta) (iteratorStart, segmentIndex int, splitFile bool) {
	if itrIndex < len(c.chunkItrs)-1 || segIndex < len(tm.entries)-1 || c.col.Len > 0 {
		iteratorStart = itrIndex
		segmentIndex = segIndex + 1
		if segmentIndex == len(tm.entries) {
			segmentIndex = 0
			iteratorStart++
		}
		splitFile = true
		return
	}
	return
}

func (c *StreamIterators) saveSegment(ref record.Field) {
	if c.col.Len > 0 {
		idx := c.lastSeg.FieldIndexs(ref.Name)
		col := c.lastSeg.Column(idx)
		col.Init()
		col.AppendColVal(c.col, ref.Type, 0, c.col.Len)
		c.col.Init()
		c.timeCol.Init()
	}
}
