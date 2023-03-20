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
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/openGemini/openGemini/engine/immutable/readcache"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const (
	defaultIoSize = 64 * 1024
)

var (
	errLoadFail   = func(v ...interface{}) error { return errno.NewError(errno.LoadFilesFailed, v...) }
	errCreateFail = func(v ...interface{}) error { return errno.NewError(errno.CreateFileFailed, v...) }
	errWriteFail  = func(v ...interface{}) error { return errno.NewError(errno.WriteFileFailed, v...) }
	errRenameFail = func(v ...interface{}) error { return errno.NewError(errno.RenameFileFailed, v...) }
	errRemoveFail = func(v ...interface{}) error { return errno.NewError(errno.RemoveFileFailed, v...) }
	errReadFail   = func(v ...interface{}) error { return errno.NewError(errno.ReadFileFailed, v...) }
	errOpenFail   = func(v ...interface{}) error { return errno.NewError(errno.OpenFileFailed, v...) }
	errMapFail    = func(v ...interface{}) error { return errno.NewError(errno.MapFileFailed, v...) }
	errCloseFail  = func(v ...interface{}) error { return errno.NewError(errno.MapFileFailed, v...) }
)

type TSSPFileReader struct {
	ref            int64
	r              DiskFileReader
	inited         int32
	metaIndexItems []MetaIndex
	trailer        Trailer
	bloom          *bloom.Filter
	version        uint64
	trailerOffset  int64
	fileSize       int64
	avgChunkRows   int
	maxChunkRows   int
	mu             sync.RWMutex
	openMu         sync.RWMutex

	// in memory data and meta block
	inMemBlock MemoryReader
}

func CreateTSSPFileReader(size int64, fd fileops.File, trailer *Trailer, tb *TableData, ver uint64, tmp bool, lockPath *string) (*TSSPFileReader, error) {
	if size <= minTableSize() {
		log.Error("zero file size", zap.Int64("size", size))
		panic("zero file size")
	}

	var bloomFilter *bloom.Filter
	var err error
	// copy bloom filter content for reader, buf does not share
	bloomBuf := make([]byte, len(tb.bloomFilter))
	copy(bloomBuf, tb.bloomFilter)

	if bloomFilter, err = bloom.NewFilterBuffer(bloomBuf, trailer.bloomK); err != nil {
		return nil, err
	}

	if !tmp {
		tmpName := fd.Name()
		if err = fd.Close(); err != nil {
			err = errCreateFail(tmpName, err)
			log.Error("close file fail", zap.String("name", tmpName), zap.Error(err))
			return nil, err
		}

		lock := fileops.FileLockOption(*lockPath)
		pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
		name := tmpName[:len(tmpName)-len(tmpTsspFileSuffix)]
		if err = fileops.RenameFile(tmpName, name, lock); err != nil {
			err = errCreateFail(tmpName, err)
			log.Error("stat file fail", zap.String("name", tmpName), zap.Error(err))
			return nil, err
		}
		lock = ""
		fd, err = fileops.Open(name, lock, pri)
		if err != nil {
			log.Error("open file fail", zap.String("name", tmpName), zap.Error(err))
			return nil, err
		}
		fi, err := fd.Stat()
		if err != nil {
			_ = fd.Close()
			return nil, err
		}
		if fi.Size() != size {
			err = fmt.Errorf("invalid file(%v) size, %v != %v", name, fi.Size(), size)
			err = errCreateFail(err)
			log.Error("invalid file size", zap.String("name", tmpName), zap.Error(err))
			_ = fd.Close()
			return nil, err
		}
		log.Info("rename file", zap.String("old", tmpName), zap.String("new", name), zap.Int64("size", size))
	}

	r := getTSSPFileReader()
	r.r = NewDiskFileReader(fd, lockPath)
	r.trailer = Trailer{}
	r.trailerOffset = size - int64(len(tb.trailerData))
	r.fileSize = size
	r.version = ver
	r.ref = 0
	atomic.StoreInt32(&r.inited, 0)

	r.bloom = bloomFilter
	trailer.copyTo(&r.trailer)
	r.copyMetaIndex(tb.metaIndexItems)
	r.inMemBlock = emptyMemReader
	if tb.inMemBlock.MetaInMemory() || tb.inMemBlock.DataInMemory() {
		r.inMemBlock = NewMemoryReader(len(tb.inMemBlock.DataBlocks()[0]))
	}
	r.inMemBlock.CopyBlocks(tb.inMemBlock)

	return r, nil
}

func NewTSSPFileReader(name string, lockPath *string) (*TSSPFileReader, error) {
	var header [fileHeaderSize]byte
	var footer [8]byte
	fi, err := fileops.Stat(name)
	if err != nil {
		log.Error("stat file failed", zap.String("file", name), zap.Error(err))
		err = errOpenFail(name, err)
		return nil, err
	}
	lock := fileops.FileLockOption(*lockPath)

	if fi.Size() < minTableSize() {
		err = fmt.Errorf("invalid file(%v) size:%v", name, fi.Size())
		log.Error(err.Error())
		err = errOpenFail(err)
		_ = fileops.Remove(name, lock)
		return nil, err
	}

	size := fi.Size()
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	lock = ""
	fd, err := fileops.Open(name, lock, pri)
	if err != nil {
		err = errCreateFail(name, err)
		log.Error("open file failed", zap.String("file", name), zap.Error(err))
		return nil, err
	}

	dr := NewDiskFileReader(fd, lockPath)

	hd := header[:]
	hb, err := dr.ReadAt(0, uint32(len(header[:])), &hd)
	if err != nil {
		_ = dr.Close()
		log.Error("read file header failed", zap.String("file", name), zap.Error(err))
		return nil, err
	}

	if record.Bytes2str(hb[:len(tableMagic)]) != tableMagic {
		_ = dr.Close()
		err = fmt.Errorf("invalid file(%v) magic: %v", name, record.Bytes2str(hb[:len(tableMagic)]))
		log.Error(err.Error())
		err = errOpenFail(err)
		return nil, err
	}
	version := numberenc.UnmarshalUint64(hb[len(tableMagic):])

	ft := footer[:]
	fb, err := dr.ReadAt(size-8, 8, &ft)
	if err != nil {
		_ = dr.Close()
		log.Error("read file footer failed", zap.String("file", name), zap.Error(err))
		return nil, err
	}

	trailOff := numberenc.UnmarshalInt64(fb)
	if trailOff < 0 || trailOff > size-8 {
		_ = dr.Close()
		err = fmt.Errorf("invalid file footer offset, file(%v), offset(%v), file size(%v)", name, trailOff, size)
		log.Error(err.Error())
		err = errOpenFail(err)
		return nil, err
	}

	trSize := size - 8 - trailOff
	trailer := make([]byte, trSize)
	tb, err := dr.ReadAt(trailOff, uint32(trSize), &trailer)
	if err != nil {
		_ = dr.Close()
		log.Error("read file trailer failed", zap.String("file", name), zap.Error(err))
		return nil, err
	}

	r := getTSSPFileReader()
	tr := &r.trailer
	_, err = tr.unmarshal(tb)
	if err != nil {
		err = errCreateFail(dr.Name(), err)
		_ = dr.Close()
		log.Error("unmarshal file trailer fail", zap.Error(err))
		return nil, err
	}

	r.trailerOffset = trailOff
	r.fileSize = size
	r.version = version
	r.r = dr
	r.ref = 0
	atomic.StoreInt32(&r.inited, 0)

	return r, nil
}

func (r *TSSPFileReader) copyMetaIndex(items []MetaIndex) {
	if cap(r.metaIndexItems) < len(items) {
		r.metaIndexItems = make([]MetaIndex, len(items))
	} else {
		r.metaIndexItems = r.metaIndexItems[:len(items)]
	}
	copy(r.metaIndexItems, items)
}

func (r *TSSPFileReader) initialized() bool {
	return atomic.LoadInt32(&r.inited) == 1
}

func (r *TSSPFileReader) Open() error {
	return nil
}

func (r *TSSPFileReader) validate(offset, size int64) error {
	if offset < r.trailer.dataOffset {
		return fmt.Errorf("invlaid read offset, %v < %v", offset, r.trailer.dataOffset)
	}

	if offset+size > r.trailer.dataOffset+r.trailer.dataSize {
		return fmt.Errorf("read offset size out of range, [%d, %d] [%d %d]", r.trailer.dataOffset, r.trailer.dataSize,
			offset, size)
	}

	return nil
}

func (r *TSSPFileReader) FreeFileHandle() error {
	if !r.r.IsOpen() {
		return nil
	}
	defer util.TimeCost("close file")()
	if err := r.r.FreeFileHandle(); err != nil {
		return err
	}
	atomic.StoreInt32(&r.inited, 0)
	return nil
}

func (r *TSSPFileReader) ReadData(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext) (*record.Record, error) {
	err := r.validate(cm.offset, int64(cm.size))
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	if len(decs.ops) > 0 {
		return r.readSegmentMetaRecord(cm, dst, decs, false)
	}

	dst, err = r.readSegmentRecord(cm, segment, dst, decs)
	if err != nil {
		return nil, err
	}

	return dst, nil
}

// readSegmentMetaRecord read column meta to speedup
func (r *TSSPFileReader) readSegmentMetaRecord(cm *ChunkMeta, dst *record.Record, decs *ReadContext, copied bool) (*record.Record, error) {
	var err error
	ops := decs.ops

	schema := dst.Schema
	if dst.RecMeta == nil {
		dst.RecMeta = &record.RecMeta{}
	}
	if cap(dst.ColMeta) < len(schema)-1 {
		dst.ColMeta = make([]record.ColMeta, len(schema)-1)
	}
	dst.ColMeta = dst.ColMeta[:len(schema)-1]

	for _, call := range ops {
		idx := dst.Schema.FieldIndex(call.Ref.Val)
		if idx < 0 {
			return nil, nil
		}
		ref := dst.Schema.Field(idx)

		switch call.Call.Name {
		case "min", "max":
			isMin := call.Call.Name == "min"
			// one call support pre agg, other columns are aux fields
			err = readMinMax(cm, ref, dst, decs, r, copied, isMin)
			if err != nil {
				return nil, err
			}
		case "first", "last":
			isFirst := call.Call.Name == "first"
			// one call support pre agg, other columns are aux fields
			err = readFirstOrLast(cm, ref, dst, decs, r, copied, isFirst)
			if err != nil {
				return nil, err
			}
		case "count", "sum":
			isSum := call.Call.Name == "sum"
			err = readSumCount(cm, ref, dst, decs, r, copied, isSum)
			if err != nil {
				return nil, err
			}
		default:
			panic(call)
		}
	}

	if !dst.IsEmpty() {
		return dst, nil
	}

	return nil, nil
}

func columnData(chunk []byte, baseOffset int64, segOff int64, segSize uint32) []byte {
	off := segOff - baseOffset
	return chunk[off : off+int64(segSize)]
}

func (r *TSSPFileReader) readSegmentRecord(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext) (*record.Record, error) {
	var err error
	var chunkData []byte
	if cm.size < defaultIoSize {
		chunkData, err = r.ReadDataBlock(cm.offset, cm.size, &decs.readBuf)
		if err != nil {
			log.Error("read chunk data fail", zap.String("file", r.r.Name()), zap.Error(err))
			return nil, err
		}
	}

	schema := dst.Schema
	fieldMatched := false
	for i := range schema[:len(schema)-1] {
		ref := &schema[i]
		idx := cm.columnIndex(ref)
		if idx < 0 {
			continue
		}
		fieldMatched = true

		if ref.Name == record.TimeField {
			// For: select count(time) as time from ...

			if err := r.decodeTimeColumn(cm, segment, chunkData, dst.Column(i), decs); err != nil {
				return nil, err
			}
			continue
		}

		colBuilder := dst.Column(i)
		cMeta := cm.colMeta[idx]
		seg := cMeta.entries[segment]

		var data []byte
		segOff, segSize := seg.offsetSize()
		if len(chunkData) > 0 {
			data = columnData(chunkData, cm.offset, segOff, segSize)
		} else {
			data, err = r.ReadDataBlock(segOff, segSize, &decs.readBuf)
			if err != nil {
				log.Error("read column data fail", zap.String("file", r.FileName()), zap.String("col", cMeta.name), zap.Error(err))
				return nil, err
			}
		}

		err = decodeColumnData(ref, data, colBuilder, decs, false)
		failpoint.Inject("mock-decodeColumnData-panic", nil)
		if err != nil {
			err = errReadFail(r.FileName(), ref.Name, err)
			log.Error("decode column fail", zap.Error(err))
			return nil, err
		}
	}

	if !fieldMatched {
		return nil, nil
	}

	if err := r.decodeTimeColumn(cm, segment, chunkData, dst.TimeColumn(), decs); err != nil {
		return nil, err
	}
	dst.TryPadColumn()
	return dst, nil
}

func (r *TSSPFileReader) decodeTimeColumn(cm *ChunkMeta, segment int, chunkData []byte,
	timeCol *record.ColVal, decs *ReadContext) error {

	var tmData []byte
	var err error

	timeSeg := cm.timeMeta().entries[segment]
	segOff, segSize := timeSeg.offsetSize()
	if len(chunkData) > 0 {
		tmData = columnData(chunkData, cm.offset, segOff, segSize)
	} else {
		tmData, err = r.ReadDataBlock(segOff, segSize, &decs.readBuf)
		if err != nil {
			log.Error("read time column fail", zap.String("file", r.FileName()), zap.Error(err))
			return err
		}
	}

	err = appendTimeColumnData(tmData, timeCol, decs, false)
	if err != nil {
		err = errReadFail(r.FileName(), "time", err)
		log.Error("decode time column fail", zap.Error(err))
	}
	return err
}

func (r *TSSPFileReader) ReadMetaBlock(metaIdx int, id uint64, offset int64, size uint32, count uint32,
	dst *[]byte) (rb []byte, err error) {
	if r.inMemBlock.MetaInMemory() {
		rb = r.inMemBlock.ReadChunkMetaBlock(metaIdx, id, count)
		if len(rb) == 0 {
			panic(id)
		}
		return rb, nil
	}

	end := offset + int64(size)
	mOff, mSize := r.trailer.metaOffsetSize()
	if offset < mOff || end > mOff+mSize {
		err = fmt.Errorf("invalid read meta offset(%d) size(%d), [%d, %d]", offset, size, mOff, mSize)
		log.Error("read chunk meta fail", zap.String("file", r.FileName()), zap.Error(err))
		return nil, err
	}

	if readCacheEn {
		rb, err = r.GetTSSPFileBytes(offset, size, dst)
	} else {
		rb, err = r.Read(offset, size, dst)
	}
	if err != nil {
		log.Error("read file failed", zap.String("file", r.r.Name()), zap.Error(err))
		return nil, err
	}

	return rb, nil
}

func (r *TSSPFileReader) ReadDataBlock(offset int64, size uint32, dst *[]byte) (rb []byte, err error) {
	if r.inMemBlock.DataInMemory() {
		return r.inMemBlock.ReadDataBlock(offset, size, dst)
	}

	rb, err = r.Read(offset, size, dst)

	if err != nil {
		log.Error("read file failed", zap.String("file", r.FileName()), zap.Error(err))
		return nil, err
	}

	return rb, nil
}

func (r *TSSPFileReader) GetTSSPFileBytes(offset int64, size uint32, buf *[]byte) ([]byte, error) {
	var err error
	cacheIns := readcache.GetReadCacheIns()
	cacheKey := cacheIns.CreatCacheKey(r.FileName(), offset)
	var b []byte
	var page *readcache.CachePage
	if value, isGet := cacheIns.Get(cacheKey); isGet {
		page = value.(*readcache.CachePage)
		if page.Size >= int64(size) {
			b = page.Value[:size]
			return b, nil
		}
	}

	b, err = r.Read(offset, size, buf)
	if err != nil {
		log.Error("read TSSPFile failed", zap.Error(err))
		return nil, err
	}
	cacheIns.AddPage(cacheKey, b, int64(size))
	return b, nil
}

func (r *TSSPFileReader) Read(offset int64, size uint32, dst *[]byte) ([]byte, error) {
	if err := r.lazyInit(); err != nil {
		errInfo := errno.NewError(errno.LoadFilesFailed)
		log.Error("Read", zap.Error(errInfo))
		return nil, err
	}

	b, err := r.r.ReadAt(offset, size, dst)
	if err == nil && len(b) != int(size) {
		err = fmt.Errorf("short read, exp size: %d , got: %d", size, len(b))
	}

	if err != nil {
		log.Error("read file failed", zap.String("file", r.FileName()), zap.Error(err))
		return nil, err
	}

	return b, nil
}

func chunkMetaDataAndOffsets(src []byte, itemCount uint32) ([]byte, []uint32, error) {
	if len(src) < (ChunkMetaMinLen+record.Uint32SizeBytes)*int(itemCount) {
		err := fmt.Errorf("too smaller data for chunk meta block count:%v, datalen:%v", itemCount, len(src))
		log.Error(err.Error())
		return nil, nil, err
	}

	off := record.Uint32SizeBytes * int(itemCount)
	n := len(src) - off
	offs := record.Bytes2Uint32SliceBigEndian(src[n:])

	return src[:n], offs, nil
}

func (r *TSSPFileReader) unmarshalChunkMetas(src []byte, itemCount uint32, dst []ChunkMeta) ([]ChunkMeta, error) {
	cmData, ofs, err := chunkMetaDataAndOffsets(src, itemCount)
	if err != nil {
		return nil, err
	}

	idx := len(dst)
	for i := 0; i < int(itemCount); i++ {
		if cap(dst[idx:]) >= 1 {
			dst = dst[:idx+1]
		} else {
			dst = append(dst, ChunkMeta{})
		}

		cm := &dst[idx]
		size := ofs[i]
		if i < len(ofs)-1 {
			size = ofs[i+1] - size
		} else {
			size = uint32(len(cmData))
		}
		_, err = cm.unmarshal(cmData[:size])
		if err != nil {
			return nil, err
		}
		cmData = cmData[size:]
		idx++
	}

	return dst, nil
}

func (r *TSSPFileReader) ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta) ([]ChunkMeta, error) {
	var buf []byte
	if !r.r.IsMmapRead() {
		buf = bufferpool.Get()
		buf = bufferpool.Resize(buf, int(m.size))
		defer bufferpool.Put(buf)
	}

	rb, err := r.ReadMetaBlock(metaIdx, m.id, m.offset, m.size, m.count, &buf)
	if err != nil {
		log.Error("read chunk meta fail", zap.String("file", r.FileName()), zap.Error(err))
		return nil, err
	}

	if cap(dst) < int(m.count) {
		dst = dst[:cap(dst)]
		delta := int(m.count) - cap(dst)
		dst = append(dst, make([]ChunkMeta, delta)...)
	}
	dst = dst[:0]
	return r.unmarshalChunkMetas(rb, m.count, dst)
}

func (r *TSSPFileReader) BlockHeader(meta *ChunkMeta, dst []record.Field) ([]record.Field, error) {
	fields := unmarshalBlockHeader(meta, dst)
	return fields, nil
}

func (r *TSSPFileReader) MetaIndexAt(idx int) (*MetaIndex, error) {
	if err := r.lazyInit(); err != nil {
		errInfo := errno.NewError(errno.LoadFilesFailed)
		log.Error("MetaIndexAt", zap.Error(errInfo))
		return nil, err
	}
	if idx >= len(r.metaIndexItems) {
		return nil, fmt.Errorf("index %d larger than %d", idx, len(r.metaIndexItems))
	}

	return &r.metaIndexItems[idx], nil
}

func (r *TSSPFileReader) MetaIndex(id uint64, tr record.TimeRange) (int, *MetaIndex, error) {
	if err := r.lazyInit(); err != nil {
		errInfo := errno.NewError(errno.LoadFilesFailed)
		log.Error("MetaIndex", zap.Error(errInfo))
		return -1, nil, err
	}

	if id < r.trailer.minId || id > r.trailer.maxId {
		return 0, nil, nil
	}

	idx := searchMetaIndexItem(r.metaIndexItems, id)
	if idx < 0 {
		return -1, nil, nil
	}

	metaIndex := &r.metaIndexItems[idx]

	if !tr.Overlaps(metaIndex.minTime, metaIndex.maxTime) {
		return 0, nil, nil
	}

	return idx, metaIndex, nil
}

func searchChunkMeta(data []byte, offsets []uint32, sid uint64, dst *ChunkMeta) (*ChunkMeta, error) {
	var cmData []byte

	left, right := 0, len(offsets)
	for left < right {
		mid := (left + right) / 2
		off := offsets[mid]
		curtId := numberenc.UnmarshalUint64(data[off : off+8])
		if sid == curtId {
			if mid == len(offsets)-1 {
				cmData = data[off:]
			} else {
				off1 := offsets[mid+1]
				cmData = data[off:off1]
			}
			break
		} else if sid < curtId {
			right = mid
		} else {
			left = mid + 1
		}
	}

	if len(cmData) != 0 {
		if dst == nil {
			dst = &ChunkMeta{}
		}
		_, err := dst.unmarshal(cmData)
		if err != nil {
			log.Error("unmarshal chunkmeta fail", zap.Error(err))
			return nil, err
		}
		return dst, nil
	}

	return nil, nil
}

func (r *TSSPFileReader) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *ChunkMeta, buffer *[]byte) (*ChunkMeta, error) {
	needCopy := !r.r.IsMmapRead()
	if needCopy {
		if buffer == nil {
			buf := bufferpool.Get()
			buf = bufferpool.Resize(buf, int(size))
			buffer = &buf
			defer bufferpool.Put(buf)
		} else {
			n := int(size)
			bufferSize := cap(*buffer)
			nn := n - bufferSize
			if nn > 0 {
				*buffer = append((*buffer)[:bufferSize], make([]byte, nn)...)
			}
			*buffer = (*buffer)[:n]
		}
	}

	rb, err := r.ReadMetaBlock(metaIdx, id, offset, size, itemCount, buffer)
	if err != nil {
		log.Error("read chunk mata data fail", zap.Error(err))
	}

	cmData, cmOffset, err := chunkMetaDataAndOffsets(rb, itemCount)
	if err != nil {
		return nil, err
	}

	return searchChunkMeta(cmData, cmOffset, id, dst)
}

func (r *TSSPFileReader) ChunkMetaAt(index int) (*ChunkMeta, error) {
	panic("impl me")
}

func (r *TSSPFileReader) Stat() *Trailer {
	return &r.trailer
}

func (r *TSSPFileReader) MinMaxSeriesID() (min, max uint64, err error) {
	return r.trailer.minId, r.trailer.maxId, nil
}

func (r *TSSPFileReader) MinMaxTime() (min, max int64, err error) {
	return r.trailer.minTime, r.trailer.maxTime, nil
}

func (r *TSSPFileReader) Contains(id uint64, tm record.TimeRange) bool {
	if !r.trailer.ContainsId(id) || !r.trailer.ContainsTime(tm) {
		return false
	}

	if err := r.lazyInit(); err != nil {
		errInfo := errno.NewError(errno.LoadFilesFailed)
		log.Error("Contains", zap.Error(errInfo))
		return false
	}

	if !r.bloom.Contains(record.Uint64ToBytes(id)) {
		return false
	}

	return true
}

func (r *TSSPFileReader) ContainsTime(tm record.TimeRange) bool {
	return tm.Overlaps(r.trailer.minTime, r.trailer.maxTime)
}

func (r *TSSPFileReader) ContainsId(id uint64) bool {
	tr := record.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime}
	return r.Contains(id, tr)
}

func (r *TSSPFileReader) CreateTime() int64 {
	name := r.r.Name()
	tm, err := fileops.CreateTime(name)
	if err != nil {
		log.Error("get crate file time failed", zap.String("file", name), zap.Error(err))
		panic(err)
	}
	return tm.UnixNano()
}

func (r *TSSPFileReader) Name() string {
	return string(r.trailer.name)
}

func (r *TSSPFileReader) FileName() string {
	return r.r.Name()
}

func (r *TSSPFileReader) FileSize() int64 {
	return r.fileSize
}

func (r *TSSPFileReader) InMemSize() int64 {
	n := r.inMemBlock.Size()
	return n
}

func (r *TSSPFileReader) Close() error {
	err := r.r.Close()

	r.inMemBlock.FreeMemory()

	putTSSPFileReader(r)

	return err
}

func (r *TSSPFileReader) Rename(newName string) error {
	return r.r.Rename(newName)
}

func (r *TSSPFileReader) Version() uint64 {
	return r.version
}

func (r *TSSPFileReader) loadDiskFileReader() error {
	return r.r.ReOpen()
}

func (r *TSSPFileReader) loadBloomFilter() error {
	if r.bloom != nil {
		return nil
	}
	tr := &r.trailer
	// load bloom filter
	bloomBuf := make([]byte, tr.bloomSize)
	metaIndexOff, _ := r.trailer.metaIndexOffsetSize()
	bloomOff := metaIndexOff + tr.metaIndexSize
	bb, err := r.r.ReadAt(bloomOff, uint32(tr.bloomSize), &bloomBuf)
	if err != nil {
		log.Error("read bloom data failed, ", zap.String("file:", r.r.Name()), zap.Error(err))
		return err
	}

	r.bloom, err = bloom.NewFilterBuffer(bb, tr.bloomK)
	if err != nil {
		log.Error("new bloom filter fail", zap.Uint64("m", tr.bloomK), zap.Int64("buf", tr.bloomSize), zap.Error(err))
		return err
	}

	return nil
}

func (r *TSSPFileReader) loadMetaIndex() error {
	if len(r.metaIndexItems) != 0 {
		return nil
	}
	tr := &r.trailer
	metaIndexOff, metaIndexSize := r.trailer.metaIndexOffsetSize()

	// load meta index
	var buf []byte
	var err error
	if !r.r.IsMmapRead() {
		buf = bufferpool.Get()
		buf = bufferpool.Resize(buf, int(metaIndexSize))
		defer bufferpool.Put(buf)
	}

	buf, err = r.r.ReadAt(metaIndexOff, uint32(metaIndexSize), &buf)
	if err != nil {
		log.Error("read file metaindex fail", zap.Error(err))
		return err
	}

	if cap(r.metaIndexItems) < int(tr.metaIndexItemNum) {
		r.metaIndexItems = r.metaIndexItems[:cap(r.metaIndexItems)]
		delta := int(tr.metaIndexItemNum) - len(r.metaIndexItems)
		r.metaIndexItems = append(r.metaIndexItems, make([]MetaIndex, delta)...)
	}

	r.metaIndexItems = r.metaIndexItems[:tr.metaIndexItemNum]

	for i := range r.metaIndexItems {
		m := &r.metaIndexItems[i]
		buf, err = m.unmarshal(buf)
		if err != nil {
			log.Error("unmarshal metaindex fail", zap.Int("number", i), zap.Int("total", len(r.metaIndexItems)), zap.Error(err))
			return err
		}
	}

	return err
}

func (r *TSSPFileReader) LoadComponents() error {
	if r.initialized() {
		return nil
	}

	r.openMu.Lock()
	defer r.openMu.Unlock()

	if !r.r.IsOpen() {
		if err := r.loadDiskFileReader(); err != nil {
			err = errLoadFail(r.FileName(), err)
			log.Error("load diskFileReader fail", zap.Error(err))
			return err
		}
	}

	if err := r.loadBloomFilter(); err != nil {
		err = errLoadFail(r.FileName(), err)
		log.Error("load bloom filter fail", zap.Error(err))
		return err
	}

	if err := r.loadMetaIndex(); err != nil {
		err = errLoadFail(r.FileName(), err)
		log.Error("load metaindex fail", zap.Error(err))
		return err
	}

	atomic.StoreInt32(&r.inited, 1)
	return nil
}

func (r *TSSPFileReader) loadIdTimes(isOrder bool, p *IdTimePairs) error {
	var buf []byte
	var err error

	off, size := r.trailer.idTimeOffsetSize()
	if !r.r.IsMmapRead() {
		buf = bufferpool.Get()
		buf = bufferpool.Resize(buf, int(size))
		defer bufferpool.Put(buf)
	}

	if err = r.lazyInit(); err != nil {
		errInfo := errno.NewError(errno.LoadFilesFailed)
		log.Error("loadIdTimes", zap.Error(errInfo))
		return err
	}

	buf, err = r.r.ReadAt(off, uint32(size), &buf)
	if err != nil {
		log.Error("read id time data fail", zap.String("file", r.r.Name()), zap.Int64s("offset/size", []int64{off, size}), zap.Error(err))
		return err
	}
	if int64(len(buf)) != size {
		err = fmt.Errorf("read(%v) id time data fail, need:%v, read:%v", r.r.Name(), size, len(buf))
		log.Error(err.Error())
		return err
	}

	_, err = p.Unmarshal(isOrder, buf)
	if err != nil {
		err = errLoadFail(r.r.Name(), err)
		log.Error("id time pairs unmarshal fail", zap.Error(err))
		return err
	}

	r.initChunkStat(p)

	return nil
}

func (r *TSSPFileReader) initChunkStat(p *IdTimePairs) {
	var max, n int64
	max = math.MinInt64
	for _, rows := range p.Rows {
		n += rows
		if max < rows {
			max = rows
		}
	}
	r.maxChunkRows = int(max)
	if len(p.Rows) > 0 {
		r.avgChunkRows = int(n / int64(len(p.Rows)))
	}
	if r.avgChunkRows < 1 {
		r.avgChunkRows = 1
	}
}

func (r *TSSPFileReader) FreeMemory() int64 {
	return r.inMemBlock.FreeMemory()
}

func (r *TSSPFileReader) LoadIntoMemory() error {
	if err := r.LoadComponents(); err != nil {
		log.Error("load index fail", zap.String("file", r.r.Name()), zap.Error(err))
		return err
	}

	return r.inMemBlock.LoadIntoMemory(r.r, &r.trailer, r.metaIndexItems)
}

func (r *TSSPFileReader) reset() {
	r.trailer.reset()
	r.bloom = nil
	r.version = version
	r.metaIndexItems = r.metaIndexItems[:0]
	r.trailerOffset = 0
	r.fileSize = 0
	r.r = nil
	r.avgChunkRows = 0
	r.maxChunkRows = 0
	atomic.StoreInt32(&r.inited, 0)

	r.inMemBlock.Reset()
}

func (r *TSSPFileReader) AverageChunkRows() int {
	return r.avgChunkRows
}

func (r *TSSPFileReader) MaxChunkRows() int {
	return r.maxChunkRows
}

var (
	fileReaderPool = sync.Pool{}
)

func getTSSPFileReader() *TSSPFileReader {
	v := fileReaderPool.Get()
	if v == nil {
		return &TSSPFileReader{}
	}

	r, ok := v.(*TSSPFileReader)
	if !ok {
		return &TSSPFileReader{}
	}

	return r
}

func putTSSPFileReader(r *TSSPFileReader) {
	r.reset()
	fileReaderPool.Put(r)
}

var (
	_ TableReader = (*TSSPFileReader)(nil)
)

func (r *TSSPFileReader) lazyInit() error {
	failpoint.Inject("lazyInit-error", func() {
		failpoint.Return(fmt.Errorf("lazyInit error"))
	})

	if err := r.LoadComponents(); err != nil {
		return err
	}
	return nil
}

func (r *TSSPFileReader) Ref() {
	r.mu.RLock()
	atomic.AddInt64(&r.ref, 1)
	r.mu.RUnlock()

}

func (r *TSSPFileReader) Unref() {
	if atomic.AddInt64(&r.ref, -1) == 0 {
		r.mu.Lock()
		if r.ref == 0 {
			if err := r.FreeFileHandle(); err != nil {
				r.mu.Unlock()
				panic("freeFile failed")
			}
		}
		r.mu.Unlock()
	}
	if r.ref < 0 {
		panic("file closed: " + r.FileName())
	}
}
