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
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type FileIterator struct {
	r         TSSPFile
	err       error
	chunkN    int
	chunkUsed int

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

	timeReader *BufferReader
	dataReader *BufferReader
}

func NewFileIterator(r TSSPFile, log *Log.Logger) *FileIterator {
	var fi *FileIterator
	trailer := r.FileStat()
	v := fileIteratorPool.Get()
	if v == nil {
		fi = &FileIterator{}
		fi.timeReader = NewBufferReader(fileops.DefaultBufferSize)
		fi.dataReader = NewBufferReader(fileops.DefaultBufferSize)
	} else {
		fi = v.(*FileIterator)
	}

	fi.r = r
	fi.chunkN = int(trailer.idCount)
	fi.mIndexN = int(trailer.metaIndexItemNum)
	fi.log = log

	fi.dataOffset = trailer.dataOffset
	fi.dataSize = trailer.dataSize

	fi.timeReader.Reset(r)
	fi.dataReader.Reset(r)

	return fi
}

func (itr *FileIterator) reset() {
	itr.r = nil
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
}

func (itr *FileIterator) readData(offset int64, size uint32) ([]byte, error) {
	return itr.dataReader.Read(offset, size)
}

func (itr *FileIterator) readTimeData(offset int64, size uint32) ([]byte, error) {
	return itr.timeReader.Read(offset, size)
}

func (itr *FileIterator) Close() {
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

func (itr *FileIterator) GetCurtChunkMeta() *ChunkMeta {
	return itr.curtChunkMeta
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

		itr.cmBuffers[itr.cmIdx], itr.err = itr.r.ReadChunkMetaData(metaIndexAt, itr.metaIndex, itr.cmBuffers[itr.cmIdx][:0], fileops.IO_PRIORITY_LOW_READ)
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
		if m.isClosed() || m.isCompMergeStopped() {
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

type BufferReader struct {
	r       TSSPFile
	buf     []byte
	swap    []byte
	offset  int64
	size    uint32
	maxSize uint32

	fileSize int64
}

func NewBufferReader(maxSize uint32) *BufferReader {
	return &BufferReader{maxSize: maxSize}
}

func (br *BufferReader) Reset(r TSSPFile) {
	br.r = r
	br.buf = br.buf[:0]
	br.offset = 0
	br.size = 0
	br.fileSize = r.FileSize()
}

func (br *BufferReader) Read(offset int64, size uint32) ([]byte, error) {
	if size > br.maxSize {
		return br.r.ReadData(offset, size, &br.swap, fileops.IO_PRIORITY_LOW_READ)
	}

	if err := br.preRead(offset, size); err != nil {
		return nil, err
	}

	start := offset - br.offset
	end := start + int64(size)
	if len(br.buf) < int(end) {
		return nil, errno.NewError(errno.ShortRead, len(br.buf), end)
	}

	return br.buf[start:end], nil
}

func (br *BufferReader) preRead(offset int64, size uint32) error {
	if br.offset <= offset && (br.offset+int64(br.size)) >= (offset+int64(size)) {
		return nil
	}

	br.reset(offset)
	readSize := br.maxSize
	readOffset := br.offset + int64(br.size)
	if readOffset+int64(readSize) > br.fileSize {
		readSize = uint32(br.fileSize - readOffset)
	}

	br.buf = bufferpool.Resize(br.buf, int(br.size+readSize))
	dst := br.buf[br.size : br.size+readSize]
	buf, err := br.r.ReadData(readOffset, readSize, &dst, fileops.IO_PRIORITY_LOW_READ)

	br.size += uint32(len(buf))
	br.buf = br.buf[:br.size]

	return err
}

func (br *BufferReader) reset(offset int64) {
	n := uint32(offset - br.offset)

	// 1. Expected offset is less than cached offset
	// cache:      ----------------
	// expect:  --------
	// 2. no intersection: (br.offset+br.size) < offset
	// cache:  ----------------
	// expect:                        ------------
	if br.offset > offset || n > br.size {
		br.offset = offset
		br.size = 0
		return
	}

	// offset are the same: br.offset == offset
	if n == 0 {
		return
	}

	// Delete the first n bytes
	br.buf = append(br.buf[:0], br.buf[n:]...)
	br.offset = offset
	br.size -= n
}
