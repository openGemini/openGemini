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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
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

type FileIterator struct {
	r          TSSPFile
	tombstones []TombstoneFile
	err        error
	chunkN     int
	chunkUsed  int

	mIndexN   int
	mIndexPos int

	metaIndex     *MetaIndex
	cmBuffers     [2][]ChunkMeta
	chunkMetas    []ChunkMeta
	cmIdx         int
	curtChunkMeta *ChunkMeta
	curtChunkPos  int
	segPos        int

	log *Log.Logger

	dataOffset int64
	dataSize   int64

	tBuf   []byte
	buf    []byte
	buffer []byte
	offset int64
	size   int
}

func NewFileIterator(r TSSPFile, log *Log.Logger) *FileIterator {
	var fi *FileIterator
	trailer := r.FileStat()
	v := fileIteratorPool.Get()
	if v == nil {
		fi = &FileIterator{}
	} else {
		fi = v.(*FileIterator)
	}

	fi.r = r
	fi.chunkN = int(trailer.idCount)
	fi.mIndexN = int(trailer.metaIndexItemNum)
	fi.log = log

	fi.dataOffset = trailer.dataOffset
	fi.dataSize = trailer.dataSize

	return fi
}

func (itr *FileIterator) reset() {
	itr.r = nil
	itr.tombstones = itr.tombstones[:0]
	itr.err = nil
	itr.chunkN = 0
	itr.chunkUsed = 0
	itr.mIndexN = 0
	itr.mIndexPos = 0
	itr.metaIndex = nil
	itr.chunkMetas = itr.chunkMetas[:0]
	itr.cmIdx = 0
	itr.curtChunkMeta = nil
	itr.curtChunkPos = 0
	itr.segPos = 0
	itr.log = nil
	itr.buffer = itr.buffer[:0]
	itr.offset = 0
	itr.size = 0
}

func (itr *FileIterator) fromBuffer(segOffset int64, size uint32) []byte {
	off := segOffset - itr.offset
	endAt := off + int64(size)
	return itr.buffer[off:endAt]
}

func (itr *FileIterator) readData(segOffset int64, size uint32) ([]byte, error) {
	if itr.size == 0 {
		return itr.r.ReadData(segOffset, size, &itr.buf)
	}

	return itr.fromBuffer(segOffset, size), nil
}

func (itr *FileIterator) readTimeData(segOffset int64, size uint32) ([]byte, error) {
	if itr.size == 0 {
		return itr.r.ReadData(segOffset, size, &itr.tBuf)
	}

	return itr.fromBuffer(segOffset, size), nil
}

func (itr *FileIterator) readChunkData(chunkOffset int64, chunkSize uint32) error {
	if itr.offset == chunkOffset && itr.size == int(chunkSize) {
		return nil
	}

	if chunkSize > readBufferSize {
		itr.offset = 0
		itr.size = 0
		return nil
	}

	itr.buffer = bufferpool.Resize(itr.buffer, int(chunkSize))
	itr.buffer, itr.err = itr.r.ReadData(chunkOffset, chunkSize, &itr.buffer)
	if itr.err != nil {
		itr.log.Error("read data fail", zap.String("file", itr.r.Path()), zap.Error(itr.err))
		return itr.err
	}

	itr.offset = chunkOffset
	itr.size = int(chunkSize)
	return nil
}

func (itr *FileIterator) Close() {
	itr.r.Unref()
	itr.reset()
	fileIteratorPool.Put(itr)
}

func (itr *FileIterator) WithLog(log *Log.Logger) {
	itr.log = log
}

func (itr *FileIterator) readMetaBlocks() bool {
	if itr.metaIndex == nil || len(itr.chunkMetas) == 0 {
		return true
	}

	if len(itr.chunkMetas) > 0 && itr.curtChunkPos >= len(itr.chunkMetas) {
		if itr.curtChunkMeta == nil {
			return itr.mIndexPos < itr.mIndexN
		}

		if itr.curtChunkMeta != nil && itr.segPos >= len(itr.curtChunkMeta.timeRange) {
			return true
		}
	}

	return false
}

func (itr *FileIterator) NextChunkMeta() bool {
	if itr.chunkUsed >= itr.chunkN {
		return false
	}

	if itr.readMetaBlocks() {
		itr.metaIndex, itr.err = itr.r.MetaIndexAt(itr.mIndexPos)
		if itr.err != nil {
			itr.log.Error("read chunk meta fail", zap.String("file", itr.r.Path()), zap.Int("index", itr.mIndexPos), zap.Error(itr.err))
			return false
		}
		metaIndexAt := itr.mIndexPos
		itr.mIndexPos++

		if itr.cmIdx >= 2 {
			itr.cmIdx = 0
		}

		itr.cmBuffers[itr.cmIdx], itr.err = itr.r.ReadChunkMetaData(metaIndexAt, itr.metaIndex, itr.cmBuffers[itr.cmIdx][:0])
		if itr.err != nil {
			itr.log.Error("read chunk metas fail", zap.String("file", itr.r.Path()), zap.Any("index", itr.metaIndex), zap.Error(itr.err))
			return false
		}

		itr.chunkMetas = itr.cmBuffers[itr.cmIdx]
		itr.curtChunkMeta = &itr.chunkMetas[0]
		itr.curtChunkPos = 1
		itr.segPos = 0
		itr.cmIdx++
	}

	if itr.curtChunkMeta == nil || itr.segPos >= len(itr.curtChunkMeta.timeRange) {
		itr.curtChunkMeta = &itr.chunkMetas[itr.curtChunkPos]
		itr.curtChunkPos++
		itr.segPos = 0
	}

	return true
}

type FileIterators []*FileIterator

func (m *MmsTables) NewFileIterators(group *CompactGroup) (FilesInfo, error) {
	var fi FilesInfo
	fi.compIts = make(FileIterators, 0, len(group.group))
	fi.oldFiles = make([]TSSPFile, 0, len(group.group))
	for _, fn := range group.group {
		if m.isClosed() {
			fi.compIts.Close()
			return fi, ErrCompStopped
		}
		if atomic.LoadInt64(group.dropping) > 0 {
			fi.compIts.Close()
			return fi, ErrDroppingMst
		}
		f := m.File(group.name, fn, true)
		if f == nil {
			fi.compIts.Close()
			return fi, fmt.Errorf("table %v, %v, %v not find", group.name, fn, true)
		}
		fi.oldFiles = append(fi.oldFiles, f)
		itr := NewFileIterator(f, CLog)
		if itr.NextChunkMeta() {
			fi.compIts = append(fi.compIts, itr)
		} else {
			f.Unref()
			continue
		}

		maxRows, avgRows := f.MaxChunkRows(), f.AverageChunkRows()
		if fi.maxChunkRows < maxRows {
			fi.maxChunkRows = maxRows
		}

		fi.avgChunkRows += avgRows
		if fi.maxChunkN < itr.chunkN {
			fi.maxChunkN = itr.chunkN
		}

		if fi.maxColumns < int(itr.curtChunkMeta.columnCount) {
			fi.maxColumns = int(itr.curtChunkMeta.columnCount)
		}

		fi.estimateSize += int(itr.r.FileSize())
	}
	fi.avgChunkRows /= len(fi.compIts)
	fi.dropping = group.dropping
	fi.name = group.name
	fi.shId = group.shardId
	fi.toLevel = group.toLevel
	fi.oldFids = group.group

	return fi, nil
}

func (i FileIterators) Close() {
	for _, itr := range i {
		itr.Close()
	}
}

func (i FileIterators) MaxChunkRows() int {
	max := 0
	for _, itr := range i {
		mr := itr.r.MaxChunkRows()
		if max < mr {
			max = mr
		}
	}
	return max
}

func (i FileIterators) AverageRows() int {
	avg := 0
	for _, itr := range i {
		avg += itr.r.AverageChunkRows()
	}
	return avg / len(i)
}

func (i FileIterators) MaxColumns() int {
	max := -1
	for _, itr := range i {
		n := len(itr.curtChunkMeta.colMeta)
		if max < n {
			max = n
		}
	}
	return max
}

func NonStreamingCompaction(fi FilesInfo) bool {
	flag := MergeFlag()
	if flag == NonStreamingCompact {
		return true
	} else if flag == StreamingCompact {
		return false
	} else {
		n := fi.avgChunkRows * fi.maxColumns * 8 * len(fi.compIts)
		if n >= streamCompactMemThreshold {
			return false
		}

		if fi.maxChunkRows > int(maxRowsPerSegment)*streamCompactSegmentThreshold {
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
				Conf:       NewConfig(),
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
	name          string
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
	writer     FileWriter
	pair       IdTimePairs
	sequencer  *Sequencer
	tier       uint64
	dstMeta    ChunkMeta
	schemaMap  dictpool.Dict
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

	chunkSegments int
	cmOffset      []uint32
	preCmOff      int64

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
	c.itrs = c.itrs[:0]
	c.chunkItrs = c.chunkItrs[:0]
	c.chunkSegments = 0
	c.ctx.preAggBuilders.reset()
	c.files = c.files[:0]
	c.schemaMap.Reset()
	c.fileSize = 0
	c.colBuilder.resetPreAgg()
	putStreamIterators(c)
}

func (c *StreamIterators) stopCompact() bool {
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
		cm := m.colMeta[i]
		ref := record.Field{Name: cm.name, Type: int(cm.ty)}
		if !c.schemaMap.Has(ref.Name) {
			c.schemaMap.Set(ref.Name, ref)
			c.fields = append(c.fields, ref)
		}
	}
}

func (c *StreamIterators) genChunkSchema() {
	c.fields = c.fields[:0]
	itr := heap.Pop(c).(*StreamIterator)
	c.schemaMap.Reset()
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

func (c *StreamIterators) padRows(rows int, ref *record.Field) {
	switch ref.Type {
	case influx.Field_Type_String:
		c.col.AppendStringNulls(rows)
	case influx.Field_Type_Boolean:
		c.col.AppendBooleanNulls(rows)
	case influx.Field_Type_Float:
		c.col.AppendFloatNulls(rows)
	case influx.Field_Type_Int:
		c.col.AppendIntegerNulls(rows)
	}
}

func (m *MmsTables) NewStreamIterators(group FilesInfo) (*StreamIterators, error) {
	compItrs := getStreamIterators()
	compItrs.closed = m.closed
	compItrs.dropping = group.dropping
	compItrs.name = group.name
	compItrs.dir = m.path
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

func (m *MmsTables) streamCompactToLevel(group FilesInfo, full bool) error {
	compactStatItem := statistics.NewCompactStatItem(group.name, group.shId)
	compactStatItem.Full = full
	compactStatItem.Level = group.toLevel - 1
	compactStat.AddActive(1)
	defer func() {
		compactStat.AddActive(-1)
		compactStat.PushCompaction(compactStatItem)
	}()

	cLog, logEnd := logger.NewOperation(log, "StreamCompaction", group.name)
	defer logEnd()
	lcLog := Log.NewLogger(errno.ModuleCompact).SetZapLogger(cLog)
	start := time.Now()
	lcLog.Debug("start compact file", zap.Uint64("shid", group.shId), zap.Any("seqs", group.oldFids), zap.Time("start", start))

	lcLog.Debug(fmt.Sprintf("compactionGroup: name=%v, groups=%v", group.name, group.oldFids))

	compItrs, err := m.NewStreamIterators(group)
	if err != nil {
		lcLog.Error("new chunk readers fail", zap.Error(err))
		return err
	}

	compItrs.WithLog(lcLog)
	oldFilesSize := compItrs.estimateSize
	newFiles, err := compItrs.compact(group.oldFiles, group.toLevel, true)
	if err != nil {
		lcLog.Error("compact fail", zap.Error(err))
		compItrs.Close()
		return err
	}

	compItrs.Close()
	if err = m.ReplaceFiles(group.name, group.oldFiles, newFiles, true, lcLog); err != nil {
		lcLog.Error("replace compacted file error", zap.Error(err))
		return err
	}

	end := time.Now()
	lcLog.Debug("compact file done", zap.Any("files", group.oldFids), zap.Time("end", end), zap.Duration("time used", end.Sub(start)))

	if oldFilesSize != 0 {
		compactStatItem.OriginalFileCount = int64(len(group.oldFiles))
		compactStatItem.CompactedFileCount = int64(len(newFiles))
		compactStatItem.OriginalFileSize = int64(oldFilesSize)
		compactStatItem.CompactedFileSize = sumFilesSize(newFiles)
	}
	return nil
}

func (c *StreamIterators) cacheMetaInMemory() bool {
	if c.tier == meta.Hot {
		return c.Conf.cacheMetaData
	} else if c.tier == meta.Warm {
		return false
	}

	return c.Conf.cacheMetaData
}

func (c *StreamIterators) cacheDataInMemory() bool {
	if c.tier == meta.Hot {
		return c.Conf.cacheDataBlock
	} else if c.tier == meta.Warm {
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
	c.preCmOff = 0
}

func (c *StreamIterators) NewFile(addFileExt bool) error {
	if addFileExt {
		c.fileName.extent++
	}
	c.reset()
	c.trailer.name = append(c.trailer.name[:0], c.name...)
	c.inMemBlock = emptyMemReader
	if c.cacheDataInMemory() || c.cacheMetaInMemory() {
		idx := calcBlockIndex(c.estimateSize)
		c.inMemBlock = NewMemoryReader(blockSize[idx])
	}

	lock := fileops.FileLockOption("")
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
		c.writer = newFileWriter(c.fd, true, limit)
	} else {
		c.writer = newFileWriter(c.fd, false, limit)
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
	cm.segCount = uint16(len(cm.timeRange))
	minT, maxT := cm.MinMaxTime()
	if c.mIndex.count == 0 {
		c.mIndex.size = 0
		c.mIndex.id = cm.sid
		c.mIndex.minTime = minT
		c.mIndex.maxTime = maxT
		c.mIndex.offset = c.writer.ChunkMetaSize()
	}

	c.updateChunkStat(cm.sid, maxT)
	c.keys[cm.sid] = struct{}{}

	c.encChunkMeta = cm.marshal(c.encChunkMeta[:0])
	cmOff := c.writer.ChunkMetaSize()
	c.cmOffset = append(c.cmOffset, uint32(cmOff-c.preCmOff))
	wn, err := c.writer.WriteChunkMeta(c.encChunkMeta)
	if err != nil {
		err = errWriteFail(c.writer.Name(), err)
		c.log.Error("write chunk meta fail", zap.Error(err))
		return err
	}
	if wn != len(c.encChunkMeta) {
		c.log.Error("write chunk meta fail", zap.String("file", c.fd.Name()), zap.Ints("size", []int{len(c.encChunkMeta), wn}))
		return io.ErrShortWrite
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

	c.mIndex.size += uint32(wn) + 4
	c.mIndex.count++
	if c.mIndex.minTime > minT {
		c.mIndex.minTime = minT
	}
	if c.mIndex.maxTime < maxT {
		c.mIndex.maxTime = maxT
	}

	if c.mIndex.size >= uint32(c.Conf.maxChunkMetaItemSize) || c.mIndex.count >= uint32(c.Conf.maxChunkMetaItemCount) {
		offBytes := record.Uint32Slice2byte(c.cmOffset)
		_, err = c.writer.WriteChunkMeta(offBytes)
		if err != nil {
			err = errWriteFail(c.writer.Name(), err)
			c.log.Error("write chunk meta fail", zap.Error(err))
			return err
		}
		c.metaIndexItems = append(c.metaIndexItems, c.mIndex)
		c.mIndex.reset()
		c.cmOffset = c.cmOffset[:0]
		c.preCmOff = c.writer.ChunkMetaSize()
		c.writer.SwitchMetaBuffer()
	}

	return nil
}

func (c *StreamIterators) FileVersion() uint64 {
	return c.version
}

func (c *StreamIterators) removeEmptyFile() {
	if c.writer != nil && c.fd != nil {
		_ = c.writer.Close()
		name := c.fd.Name()
		_ = c.fd.Close()
		_ = fileops.Remove(name)
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

	dr, err := CreateTSSPFileReader(c.fileSize, c.fd, &c.trailer, &c.TableData, c.FileVersion(), tmp)
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
	}, nil
}

func (c *StreamIterators) genBloomFilter() {
	bm, bk := bloom.Estimate(uint64(len(c.keys)), falsePositive)
	bmBytes := pow2((bm + 7) / 8)
	if uint64(cap(c.bloomFilter)) < bmBytes {
		c.bloomFilter = make([]byte, bmBytes)
	} else {
		c.bloomFilter = c.bloomFilter[:bmBytes]
		record.MemorySet(c.bloomFilter)
	}
	c.trailer.bloomM = bm
	c.trailer.bloomK = bk
	c.bf, _ = bloom.NewFilterBuffer(c.bloomFilter, bk)
	for id := range c.keys {
		c.bf.Insert(record.Uint64ToBytes(id))
	}
}

func (c *StreamIterators) Flush() error {
	if c.writer.DataSize() <= 16 {
		return errEmptyFile
	}

	if c.mIndex.count > 0 {
		offBytes := record.Uint32Slice2byte(c.cmOffset)
		_, err := c.writer.WriteChunkMeta(offBytes)
		if err != nil {
			c.log.Error("write chunk meta fail", zap.String("name", c.fd.Name()), zap.Error(err))
			return err
		}
		c.metaIndexItems = append(c.metaIndexItems, c.mIndex)
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

	if c.sequencer != nil {
		c.sequencer.BatchUpdate(&c.pair)
	}

	c.encIdTime = c.pair.Marshal(c.fileName.order, c.encIdTime[:0], c.colBuilder.coder)
	c.trailer.idTimeSize = int64(len(c.encIdTime))
	if _, err := c.writer.WriteData(c.encIdTime); err != nil {
		c.log.Error("write id time data fail", zap.String("name", c.fd.Name()), zap.Error(err))
		return err
	}
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

func (c *StreamIterators) compactColumn(ref record.Field) (iteratorStart, segmentIndex int, splitFile bool, err error) {
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
		col.Init()
	}

	for itrIndex := c.iteratorStart; itrIndex < len(c.chunkItrs); itrIndex++ {
		itr := c.chunkItrs[itrIndex]
		if c.isClosed() {
			err = ErrCompStopped
			return
		}

		srcMeta := itr.curtChunkMeta
		if err = itr.readChunkData(srcMeta.offset, srcMeta.size); err != nil {
			return
		}

		tm := srcMeta.TimeMeta()
		var srcColMeta *ColumnMeta

		idx := c.dstMeta.columnIndex(&ref)
		c.colBuilder.cm = &c.dstMeta
		c.colBuilder.colMeta = &c.dstMeta.colMeta[idx]
		idx = srcMeta.columnIndex(&ref)
		if idx >= 0 {
			srcColMeta = &srcMeta.colMeta[idx]
		} else {
			c.padRows(srcMeta.Rows(c.ctx.preAggBuilders.timeBuilder), &ref)
		}

		// merge full segments(full segment: rows in segment EQ 1000)
		for segIndex := c.segmentIndex; segIndex < len(tm.entries); segIndex++ {
			if c.isClosed() {
				err = ErrCompStopped
				return
			}

			if idx >= 0 {
				colSeg := &srcColMeta.entries[segIndex]
				segData, er := itr.readData(colSeg.offset, colSeg.size)
				if er != nil {
					err = er
					return
				}
				if err = c.decodeSegment(segData, nil, ref); err != nil {
					return
				}
			}

			if c.continueMerge(segIndex, itrIndex, tm) {
				break
			}

			if err = c.writeSegment(srcMeta.sid, ref); err != nil {
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
				if err = c.writeSegment(itr.curtChunkMeta.sid, ref); err != nil {
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
			if err := c.writeSegment(id, ref); err != nil {
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
	c.fileName = NewTSSPFileName(seq, level, 0, 0, isOrder)
	if err := c.NewFile(false); err != nil {
		panic(err)
	}

	for c.Len() > 0 {
		// merge one chunk
		var err error
		splitFile := true
		iteratorStart, segmentIndex := 0, 0
		c.genChunkSchema()
		for splitFile {
			c.iteratorStart, c.segmentIndex = iteratorStart, segmentIndex
			dOff := c.writer.DataSize()
			id := c.chunkItrs[0].curtChunkMeta.sid
			c.Init(id, dOff, c.fields)
			for _, ref := range c.fields {
				if c.stopCompact() {
					return nil, ErrCompStopped
				}

				//pad crc
				if err := c.writeCrc(crc[:]); err != nil {
					return nil, err
				}

				if c.chunkSegments > c.Conf.maxSegmentLimit {
					iteratorStart, segmentIndex, splitFile, err = c.mergeColumnCalcPreAgg(ref)
				} else {
					iteratorStart, segmentIndex, splitFile, err = c.compactColumn(ref)
				}
				if err != nil {
					return nil, err
				}

				if err = c.genColumnPreAgg(c.colBuilder.colMeta); err != nil {
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

func (c *StreamIterators) splitColumn(ref record.Field) (splitCol bool, lastTmSeg *record.ColVal) {
	maxRow := c.Conf.maxRowsPerSegment
	splitCol = c.col.Len > maxRow
	if splitCol {
		c.colSegs = c.col.Split(c.colSegs[:0], maxRow, ref.Type)
		if len(c.colSegs) != 2 {
			panic("len(c.colSegs) != 2")
		}
		if ref.Name != record.TimeField {
			c.timeSegs = c.timeCol.Split(c.timeSegs[:0], maxRow, influx.Field_Type_Int)
		} else {
			c.timeSegs = append(c.timeSegs[:0], c.colSegs...)
		}
		lastTmSeg = &c.timeSegs[1]
	} else {
		c.colSegs = append(c.colSegs[:0], *c.col)
		if ref.Name != record.TimeField {
			c.timeSegs = append(c.timeSegs[:0], *c.timeCol)
		}
	}
	return
}

func (c *StreamIterators) writeSegmentCalcPreAgg(id uint64, ref record.Field) error {
	if err := c.validate(id, ref); err != nil {
		return err
	}

	if c.chunkSegments > c.Conf.maxSegmentLimit {
		if c.col.Len != c.timeCol.Len && ref.Name != record.TimeField {
			err := fmt.Errorf("column(%v) rows(%v) neq time rows(%v)", ref.String(), c.col.Len, c.timeCol.Len)
			c.log.Error(err.Error())
			return err
		}
	}

	splitCol, lastTmSeg := c.splitColumn(ref)
	var err error

	cols := c.colSegs[:1]
	off := c.writer.DataSize()
	c.colBuilder.data = c.colBuilder.data[:0]
	if ref.Name == record.TimeField {
		err = c.colBuilder.encodeTimeColumn(cols, off)
	} else {
		tmCols := c.timeSegs[:1]
		err = c.colBuilder.encodeColumnCalcPreAgg(cols, tmCols, off, ref)
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
		if ref.Name != record.TimeField {
			c.swapLastTimeSegment(lastTmSeg)
		}
		c.swapLastSegment(&ref, &c.colSegs[1])
	} else {
		c.col.Init()
		c.timeCol.Init()
	}

	return nil
}

func (c *StreamIterators) validate(id uint64, ref record.Field) error {
	if c.trailer.maxId != 0 {
		if id <= c.trailer.maxId {
			err := fmt.Errorf("file(%v) series id(%d) must be greater than %d", c.fd.Name(), id, c.trailer.maxId)
			c.log.Error("Invalid series id", zap.Error(err))
			return err
		}
	}

	if ref.Name != c.colBuilder.colMeta.name || ref.Type != int(c.colBuilder.colMeta.ty) {
		err := fmt.Errorf("invalid column,exp:%v, but:%v::%v", ref.String(),
			c.colBuilder.colMeta.name,
			influx.FieldTypeName[int(c.colBuilder.colMeta.ty)])
		c.log.Error(err.Error())
		return err
	}

	return nil
}

func (c *StreamIterators) writeSegment(id uint64, ref record.Field) error {
	if err := c.validate(id, ref); err != nil {
		return err
	}

	maxRow := c.Conf.maxRowsPerSegment
	splitCol := c.col.Len > maxRow
	if splitCol {
		c.colSegs = c.col.Split(c.colSegs[:0], maxRow, ref.Type)
		if len(c.colSegs) != 2 {
			panic("len(c.colSegs) != 2")
		}
	} else {
		c.colSegs = append(c.colSegs[:0], *c.col)
	}

	var err error

	cols := c.colSegs[:1]
	off := c.writer.DataSize()
	c.colBuilder.data = c.colBuilder.data[:0]
	if ref.Name == record.TimeField {
		err = c.colBuilder.encodeTimeColumn(cols, off)
	} else {
		err = c.colBuilder.encodeColumn(cols, off, ref)
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
		c.swapLastSegment(&ref, &c.colSegs[1])
	} else {
		c.col.Init()
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
		b.data = append(b.data, BlockInteger)
		nilBitMap, bitmapOffset := col.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(col.NilCount))
		b.data, err = EncodeTimestampBlock(col.Val, b.data, b.coder)
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

func (b *ColumnBuilder) encodeColumn(segCols []record.ColVal, offset int64, ref record.Field) error {
	var err error
	for i := range segCols {
		segCol := &segCols[i]

		b.colMeta.growEntry()
		m := &b.colMeta.entries[len(b.colMeta.entries)-1]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, byte(ref.Type))
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		switch ref.Type {
		case influx.Field_Type_String:
			b.data, err = EncodeStringBlock(segCol.Val, segCol.Offset, b.data, b.coder)
		case influx.Field_Type_Boolean:
			b.data, err = EncodeBooleanBlock(segCol.Val, b.data, b.coder)
		case influx.Field_Type_Float:
			b.data, err = EncodeFloatBlock(segCol.Val, b.data, b.coder)
		case influx.Field_Type_Int:
			b.data, err = EncodeIntegerBlock(segCol.Val, b.data, b.coder)
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

func (b *ColumnBuilder) encodeColumnCalcPreAgg(segCols []record.ColVal, tmCols []record.ColVal, offset int64, ref record.Field) error {
	var err error
	if len(segCols) != len(tmCols) {
		panic("len(segCols) != len(tmCols)")
	}
	for i := range segCols {
		tmCol := &tmCols[i]
		segCol := &segCols[i]
		times := tmCol.IntegerValues()

		b.colMeta.growEntry()
		m := &b.colMeta.entries[len(b.colMeta.entries)-1]
		m.setOffset(offset)

		pos := len(b.data)
		b.data = append(b.data, byte(ref.Type))
		nilBitMap, bitmapOffset := segCol.SubBitmapBytes()
		b.data = numberenc.MarshalUint32Append(b.data, uint32(len(nilBitMap)))
		b.data = append(b.data, nilBitMap...)
		b.data = numberenc.MarshalUint32Append(b.data, uint32(bitmapOffset))
		b.data = numberenc.MarshalUint32Append(b.data, uint32(segCol.NullN()))
		switch ref.Type {
		case influx.Field_Type_String:
			b.data, err = EncodeStringBlock(segCol.Val, segCol.Offset, b.data, b.coder)
			b.stringPreAggBuilder.addValues(segCol, times)
		case influx.Field_Type_Boolean:
			b.data, err = EncodeBooleanBlock(segCol.Val, b.data, b.coder)
			b.boolPreAggBuilder.addValues(segCol, times)
		case influx.Field_Type_Float:
			b.data, err = EncodeFloatBlock(segCol.Val, b.data, b.coder)
			b.floatPreAggBuilder.addValues(segCol, times)
		case influx.Field_Type_Int:
			b.data, err = EncodeIntegerBlock(segCol.Val, b.data, b.coder)
			b.intPreAggBuilder.addValues(segCol, times)
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

func (c *StreamIterators) mergeIntegerPreAgg(cm *ColumnMeta, ref *record.Field) error {
	ab := c.colBuilder.intPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.intBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := itr.curtChunkMeta.columnIndex(ref)
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

func (c *StreamIterators) mergeFloatPreAgg(cm *ColumnMeta, ref *record.Field) error {
	ab := c.colBuilder.floatPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.floatBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := itr.curtChunkMeta.columnIndex(ref)
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

func (c *StreamIterators) mergeStringPreAgg(cm *ColumnMeta, ref *record.Field) error {
	ab := c.colBuilder.stringPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.stringBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := itr.curtChunkMeta.columnIndex(ref)
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

func (c *StreamIterators) mergeBooleanPreAgg(cm *ColumnMeta, ref *record.Field) error {
	ab := c.colBuilder.boolPreAggBuilder
	if c.chunkSegments > c.Conf.maxSegmentLimit {
		cm.preAgg = ab.marshal(cm.preAgg[:0])
		return nil
	}

	aggBuilder := c.ctx.preAggBuilders.boolBuilder
	aggBuilder.reset()
	for i := 0; i < len(c.chunkItrs); i++ {
		itr := c.chunkItrs[i]
		idx := itr.curtChunkMeta.columnIndex(ref)
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

func (c *StreamIterators) genColumnPreAgg(cm *ColumnMeta) error {
	var err error
	ref := &record.Field{Name: cm.name, Type: int(cm.ty)}
	switch int(cm.ty) {
	case influx.Field_Type_Int:
		if cm.name == record.TimeField {
			err = c.mergeTimePreAgg(cm)
		} else {
			err = c.mergeIntegerPreAgg(cm, ref)
		}
	case influx.Field_Type_Float:
		err = c.mergeFloatPreAgg(cm, ref)
	case influx.Field_Type_Boolean:
		err = c.mergeBooleanPreAgg(cm, ref)
	case influx.Field_Type_String:
		err = c.mergeStringPreAgg(cm, ref)
	default:
		err = fmt.Errorf("unknown column data type, %v::%v", cm.name, cm.ty)
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

func (c *StreamIterators) mergeColumnCalcPreAgg(ref record.Field) (iteratorStart, segmentIndex int, splitFile bool, err error) {
	// merge column ref
	_ = c.colBuilder.initEncoder(ref)
	id := c.chunkItrs[c.iteratorStart].curtChunkMeta.sid
	splitFile = false
	segmentN := 0
	lastSegRows := c.lastSeg.RowNums()
	if lastSegRows > 0 {
		idx := c.lastSeg.FieldIndexs(ref.Name)
		col := c.lastSeg.Column(idx)
		tmCol := c.lastSeg.TimeColumn()
		c.col.AppendColVal(col, ref.Type, 0, col.Len)
		c.timeCol.AppendColVal(tmCol, influx.Field_Type_Int, 0, tmCol.Len)
		col.Init()
	}

	for itrIndex := c.iteratorStart; itrIndex < len(c.chunkItrs); itrIndex++ {
		itr := c.chunkItrs[itrIndex]
		if c.isClosed() {
			err = ErrCompStopped
			return
		}

		srcMeta := itr.curtChunkMeta
		if err = itr.readChunkData(srcMeta.offset, srcMeta.size); err != nil {
			return
		}

		tm := srcMeta.TimeMeta()
		var srcColMeta *ColumnMeta

		idx := c.dstMeta.columnIndex(&ref)
		c.colBuilder.cm = &c.dstMeta
		c.colBuilder.colMeta = &c.dstMeta.colMeta[idx]
		idx = srcMeta.columnIndex(&ref)
		if idx >= 0 {
			srcColMeta = &srcMeta.colMeta[idx]
		} else {
			c.padRows(srcMeta.Rows(c.ctx.preAggBuilders.timeBuilder), &ref)
		}

		// merge full segments(full segment: rows in segment EQ 1000)
		for segIndex := c.segmentIndex; segIndex < len(tm.entries); segIndex++ {
			if c.isClosed() {
				err = ErrCompStopped
				return
			}

			if idx >= 0 {
				colSeg := &srcColMeta.entries[segIndex]
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

			if c.continueMerge(segIndex, itrIndex, tm) {
				break
			}

			if err = c.writeSegmentCalcPreAgg(srcMeta.sid, ref); err != nil {
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
				if err = c.writeSegmentCalcPreAgg(id, ref); err != nil {
					return
				}
				segmentN++
			}
		}
	}

	err = c.writeLastSegment(segmentN, ref, id)

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
