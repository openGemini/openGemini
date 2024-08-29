// Copyright Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package immutable

import (
	"fmt"
	"io"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	"go.uber.org/zap"
)

func NewMemoryReader(blkSize int) MemoryReader {
	r := &MemBlock{blockSize: int64(blkSize)}
	return r
}

type MemBlock struct {
	blockSize  int64
	data       [][]byte
	chunkMetas [][]byte
}

func (mb *MemBlock) CopyBlocks(src MemoryReader) {
	dataBlocks := src.DataBlocks()
	if len(dataBlocks) > 0 {
		mb.ReserveDataBlock(len(dataBlocks))
		mb.data = append(mb.data, dataBlocks...)
	}

	metaBlocks := src.MetaBlocks()
	if len(metaBlocks) > 0 {
		mb.ReserveMetaBlock(len(metaBlocks))
		mb.chunkMetas = append(mb.chunkMetas, metaBlocks...)
	}

	if len(mb.data) > 0 {
		mb.blockSize = int64(cap(mb.data[0]))
	}

	src.Reset()
}

func (mb *MemBlock) ReadDataBlock(offset int64, size uint32, dstPtr *[]byte) ([]byte, error) {
	if err := mb.valid(offset); err != nil {
		return nil, err
	}

	blkIndex := int(offset / mb.blockSize)
	src := mb.data[blkIndex]
	start := offset % mb.blockSize
	end := start + int64(size)
	if end <= int64(len(src)) {
		return src[start:end], nil
	}

	buf := bufferpool.Resize(*dstPtr, int(size))
	*dstPtr = buf

	pos := copy(buf, src[start:])
	for i := blkIndex + 1; i < len(mb.data); i++ {
		pos += copy(buf[pos:], mb.data[i])
		if pos == int(size) {
			break
		}
	}

	return buf, nil
}

func (mb *MemBlock) loadDataBlock(dr fileops.BasicFileReader, tr *Trailer) error {
	var pos int64
	var err error

	defer func() {
		if err != nil {
			for i := range mb.data {
				putDataBlockBuffer(mb.data[i])
			}
			mb.data = mb.data[:0]
		}
	}()

	idx := calcBlockIndex(int(tr.dataOffset + tr.dataSize))
	mb.blockSize = int64(blockSize[idx])
	dataSize := tr.dataOffset + tr.dataSize
	for pos < dataSize {
		buf := getDataBlockBuffer(int(mb.blockSize))
		buf = buf[:mb.blockSize]
		s := dataSize - pos
		if s < int64(len(buf)) {
			buf = buf[:s]
		}
		buf, err = dr.ReadAt(pos, uint32(len(buf)), &buf, fileops.IO_PRIORITY_ULTRA_HIGH)
		if err != nil {
			if err != io.EOF {
				log.Error("read data fail", zap.String("file", dr.Name()),
					zap.Int64s("offset/size", []int64{pos, mb.blockSize}), zap.Error(err))
				return err
			}
		}

		mb.data = append(mb.data, buf)
		pos += int64(len(buf))
	}

	return err
}

func (mb *MemBlock) loadMetaBlock(dr fileops.BasicFileReader, metaIndexItems []MetaIndex) error {
	var err error
	var buf []byte

	defer func() {
		if err != nil {
			freeMetaBlocks(mb.chunkMetas)
			mb.chunkMetas = mb.chunkMetas[:0]
		}
	}()

	if cap(mb.chunkMetas) < len(metaIndexItems) {
		mb.chunkMetas = mb.chunkMetas[:cap(mb.chunkMetas)]
		n := len(metaIndexItems) - len(mb.chunkMetas)
		mb.chunkMetas = append(mb.chunkMetas, make([][]byte, n)...)
	}
	mb.chunkMetas = mb.chunkMetas[:0]

	for i := range metaIndexItems {
		mi := &metaIndexItems[i]
		buf = getMetaBlockBuffer(int(mi.size))
		buf = bufferpool.Resize(buf, int(mi.size))
		buf, err = dr.ReadAt(mi.offset, mi.size, &buf, fileops.IO_PRIORITY_ULTRA_HIGH)
		if err != nil {
			freeMetaBlockBuffer(buf)
			log.Error("read chunkmeta fail", zap.String("file", dr.Name()),
				zap.Int64s("offset/size", []int64{mi.offset, int64(mi.size)}), zap.Error(err))
			return err
		}
		mb.chunkMetas = append(mb.chunkMetas, buf)
	}

	return nil
}

func (mb *MemBlock) LoadIntoMemory(dr fileops.BasicFileReader, tr *Trailer, metaIndexItems []MetaIndex) error {
	if CacheMetaInMemory() {
		if err := mb.loadMetaBlock(dr, metaIndexItems); err != nil {
			log.Error("load meta fail", zap.String("file", dr.Name()), zap.Error(err))
			return err
		}
	}

	if CacheDataInMemory() {
		if err := mb.loadDataBlock(dr, tr); err != nil {
			log.Error("load data fail", zap.String("file", dr.Name()), zap.Error(err))
			return err
		}
	}

	return nil
}

func (mb *MemBlock) ReadChunkMetaBlock(metaIdx int, sid uint64, count uint32) []byte {
	if len(mb.chunkMetas) == 1 {
		return mb.chunkMetas[0]
	}

	if metaIdx >= 0 && metaIdx < len(mb.chunkMetas) {
		return mb.chunkMetas[metaIdx]
	}

	left, right := 0, len(mb.chunkMetas)
	for left < right {
		mid := (left + right) / 2
		data := mb.chunkMetas[mid]
		cmData, ofs, err := chunkMetaDataAndOffsets(data, count)
		if err != nil {
			panic(err)
		}

		minId := numberenc.UnmarshalUint64(cmData[ofs[0]:][:8])
		maxId := numberenc.UnmarshalUint64(cmData[ofs[len(ofs)-1]:][:8])
		if sid >= minId && sid <= maxId {
			return data
		} else if sid < minId {
			right = mid
		} else {
			left = mid + 1
		}
	}

	return nil
}

func (mb *MemBlock) FreeMemory() int64 {
	n := freeDataBlocks(mb.data)
	n += freeMetaBlocks(mb.chunkMetas)
	mb.data = mb.data[:0]
	mb.chunkMetas = mb.chunkMetas[:0]
	return int64(n)
}

func (mb *MemBlock) Reset() {
	mb.data = mb.data[:0]
	mb.chunkMetas = mb.chunkMetas[:0]
}

func (mb *MemBlock) DataInMemory() bool {
	return len(mb.data) > 0
}

func (mb *MemBlock) MetaInMemory() bool {
	return len(mb.chunkMetas) > 0
}

func (mb *MemBlock) valid(offset int64) error {
	if len(mb.data) > 0 {
		idx := offset / mb.blockSize
		if idx >= int64(len(mb.data)) {
			return fmt.Errorf("data block index out of range: idx:%d, blocks:%v, offset:%v",
				idx, len(mb.data), offset)
		}
	}

	return nil
}

func (mb *MemBlock) Size() int64 {
	n := 0
	for i := range mb.data {
		n += cap(mb.data[i])
	}

	for i := range mb.chunkMetas {
		n += cap(mb.chunkMetas[i])
	}

	return int64(n)
}

func (mb *MemBlock) AppendDataBlock(srcData []byte) {
	var chunk []byte
	if len(mb.data) == 0 {
		chunk = getDataBlockBuffer(int(mb.blockSize))
		mb.data = append(mb.data, chunk)
	} else {
		chunk = mb.data[len(mb.data)-1]
	}

	delta := cap(chunk) - len(chunk)
	if delta == 0 {
		chunk = getDataBlockBuffer(int(mb.blockSize))
		mb.data = append(mb.data, chunk)
	}

	src := srcData
	for len(src) > 0 {
		delta = cap(chunk) - len(chunk)
		if delta >= len(src) {
			chunk = append(chunk, src...)
			mb.data[len(mb.data)-1] = chunk
			break
		}

		chunk = append(chunk, src[:delta]...)
		src = src[delta:]
		mb.data[len(mb.data)-1] = chunk
		chunk = getDataBlockBuffer(int(mb.blockSize))
		mb.data = append(mb.data, chunk)
	}
}

func (mb *MemBlock) ReserveMetaBlock(n int) {
	if cap(mb.chunkMetas) < n {
		delta := n - cap(mb.chunkMetas)
		mb.chunkMetas = mb.chunkMetas[:cap(mb.chunkMetas)]
		mb.chunkMetas = append(mb.chunkMetas, make([][]byte, delta)...)
	}
	mb.chunkMetas = mb.chunkMetas[:0]
}

func (mb *MemBlock) ReserveDataBlock(n int) {
	if cap(mb.data) < n {
		delta := n - cap(mb.data)
		mb.data = mb.data[:cap(mb.data)]
		mb.data = append(mb.data, make([][]byte, delta)...)
	}
	mb.data = mb.data[:0]
}

func (mb *MemBlock) DataBlocks() [][]byte {
	return mb.data
}

func (mb *MemBlock) MetaBlocks() [][]byte {
	return mb.chunkMetas
}

func (mb *MemBlock) SetMetaBlocks(blocks [][]byte) {
	mb.ReserveMetaBlock(len(blocks))
	mb.chunkMetas = append(mb.chunkMetas, blocks...)
}
