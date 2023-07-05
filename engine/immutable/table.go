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
	"sync"
	"unsafe"

	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

const (
	tableMagic = "53ac2021"
	version    = uint64(2)

	trailerSize           = int(unsafe.Sizeof(Trailer{})) - 24*2 + (2 + 0) + (2 + 1)
	fileHeaderSize        = len(tableMagic) + int(unsafe.Sizeof(version))
	kb                    = 1024
	maxImmTablePercentage = 85
)

var (
	falsePositive         = 0.08
	nodeImmTableSizeLimit = int64(20 * 1024 * 1024 * 1024)
	nodeImmTableSizeUsed  = int64(0)
	loadSizeLimit         = nodeImmTableSizeLimit
)

func SetImmTableMaxMemoryPercentage(sysTotalMem, percentage int) {
	if percentage > maxImmTablePercentage {
		percentage = maxImmTablePercentage
	}
	nodeImmTableSizeLimit = int64(sysTotalMem * percentage / 100)
	logger.GetLogger().Info("Set imm table max memory percentage", zap.Int64("nodeImmTableSizeLimit", nodeImmTableSizeLimit),
		zap.Int64("sysTotalMem", int64(sysTotalMem)), zap.Int64("percentage", int64(percentage)))
	loadSizeLimit = nodeImmTableSizeLimit
}

type TableData struct {
	// id bloom filter data
	bloomFilter []byte
	// include version | each section offset | measurement name | key and time range etc...
	trailerData    []byte
	metaIndexItems []MetaIndex

	inMemBlock MemoryReader
}

func (t *TableData) reset() {
	t.trailerData = t.trailerData[:0]
	t.bloomFilter = t.bloomFilter[:0]
	t.metaIndexItems = t.metaIndexItems[:0]
	if t.inMemBlock != nil {
		t.inMemBlock.Reset()
	}
}

func minTableSize() int64 {
	return int64(len(tableMagic)+trailerSize) + 8 + 8
}

func pow2(v uint64) uint64 {
	for i := uint64(8); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

var (
	blockSize     = []int{2 * kb, 4 * kb, 8 * kb, 16 * kb, 32 * kb, 64 * kb, 128 * kb, 256 * kb, 512 * kb, 1024 * kb, 2048 * kb, 4096 * kb}
	dataBlockPool = make([]sync.Pool, len(blockSize))
)

func calcBlockIndex(size int) int {
	for i := range blockSize {
		if size <= blockSize[i] {
			return i
		}
	}

	return len(blockSize) - 1
}

func getDataBlockBuffer(size int) []byte {
	idx := calcBlockIndex(size)
	size = blockSize[idx]
	v := dataBlockPool[idx].Get()
	if v == nil {
		return make([]byte, 0, size)
	}
	return v.([]byte)
}

// nolint
func putDataBlockBuffer(b []byte) {
	if cap(b) == 0 {
		return
	}

	b = b[:0]
	idx := calcBlockIndex(cap(b))
	dataBlockPool[idx].Put(b)
}

func freeDataBlocks(buffers [][]byte) int {
	n := 0
	for i := range buffers {
		n += cap(buffers[i])
		putDataBlockBuffer(buffers[i])
		buffers[i] = nil
	}
	return n
}

func EstimateBufferSize(recSize int, rows int) int {
	return rows * recSize
}
