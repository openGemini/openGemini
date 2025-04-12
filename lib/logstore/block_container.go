/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package logstore

import (
	"os"
	"sync/atomic"

	"github.com/influxdata/influxdb/uuid"
	"github.com/openGemini/openGemini/lib/fileops"
)

type BlockFileContainer struct {
	basePath  string
	bankNum   int32
	blockNum  int32
	blockSize int32
	banks     [][]byte
	locks     []BlockLocks
	counters  []int32
}

func NewBlockFileContainer(basePath string, bankNum int32, blockNum int32, blockSize int32) (*BlockFileContainer, error) {
	c := &BlockFileContainer{
		basePath:  basePath,
		bankNum:   bankNum,
		blockNum:  blockNum,
		blockSize: blockSize,
	}
	err := fileops.RemoveAll(basePath)
	if err != nil {
		return nil, err
	}
	err = fileops.MkdirAll(c.basePath, 0750)
	if err != nil {
		return nil, err
	}
	c.banks = make([][]byte, c.bankNum)
	c.locks = make([]BlockLocks, c.bankNum)
	c.counters = make([]int32, c.bankNum)
	for i := int32(0); i < c.bankNum; i++ {
		c.banks[i], err = c.initBank(int64(c.blockNum) * int64(c.blockSize))
		if err != nil {
			return nil, err
		}
		c.locks[i] = NewBlockLocks(c.blockNum)
	}
	return c, nil
}

func (c *BlockFileContainer) initBank(bankSize int64) ([]byte, error) {
	fileName := c.basePath + uuid.TimeUUID().String()
	file, err := fileops.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = file.Truncate(bankSize)
	if err != nil {
		return nil, err
	}
	bytes, err := fileops.MmapRW(int(file.Fd()), 0, int(bankSize))
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (c *BlockFileContainer) Fetch(bankId int32, blockId int32) ([]byte, bool) {
	if bankId < 0 || bankId >= c.bankNum || blockId >= c.blockNum {
		return nil, false
	}
	if c.locks[bankId].Empty(blockId) {
		return nil, false
	}
	start := int64(blockId) * int64(c.blockSize)
	dst := make([]byte, c.blockSize)
	copy(dst, c.banks[bankId][start:start+int64(c.blockSize)])
	return dst, true
}

func (c *BlockFileContainer) FetchWith(bankId int32, blockId int32, offset int64, length int64) ([]byte, bool) {
	if bankId < 0 || bankId >= c.bankNum || blockId >= c.blockNum {
		return nil, false
	}
	if length <= 0 {
		return nil, false
	}
	if offset+length > int64(c.blockSize) {
		return nil, false
	}
	if c.locks[bankId].Empty(blockId) {
		return nil, false
	}
	start := int64(blockId)*int64(c.blockSize) + offset
	dst := make([]byte, length)
	copy(dst, c.banks[bankId][start:start+length])
	return dst, true
}

func (c *BlockFileContainer) Store(data []byte) (int32, int32, bool) {
	bankId, blockId, exist := c.findEmptyBlock()
	if !exist {
		return bankId, blockId, exist
	}
	start := int64(blockId) * int64(c.blockSize)
	length := min(c.blockSize, int32(len(data)))
	// only store one block
	copy(c.banks[bankId][start:start+int64(c.blockSize)], data[:length])
	return bankId, blockId, true
}

func (c *BlockFileContainer) MultiStore(data []byte) ([]int32, []int32, bool) {
	totalLength := int32(len(data))
	num := (totalLength + c.blockSize - 1) / c.blockSize
	banks := make([]int32, num)
	blocks := make([]int32, num)
	offset := int32(0)
	for i := int32(0); i < num; i++ {
		length := min(totalLength, c.blockSize)
		bankId, blockId, success := c.Store(data[offset : offset+length])
		if !success {
			return nil, nil, false
		}
		banks[i] = bankId
		blocks[i] = blockId
		totalLength -= length
		offset += length
	}
	return banks, blocks, true
}

func (c *BlockFileContainer) MultiFetch(banks []int32, blocks []int32, totalLength int32) ([]byte, bool) {
	result := make([]byte, totalLength)
	offset := int32(0)
	for i, bankId := range banks {
		blockId := blocks[i]
		if bankId < 0 || bankId >= c.bankNum || blockId >= c.blockNum {
			return nil, false
		}
		if c.locks[bankId].Empty(blockId) {
			return nil, false
		}
		start := int64(blockId) * int64(c.blockSize)
		length := min(totalLength, c.blockSize)
		copy(result[offset:], c.banks[bankId][start:start+int64(length)])
		totalLength -= length
		offset += length
	}
	return result, true
}

func (c *BlockFileContainer) findEmptyBlock() (int32, int32, bool) {
	for i := int32(0); i < c.bankNum; i++ {
		// bank is full of blocks
		if atomic.LoadInt32(&c.counters[i]) == c.blockNum {
			continue
		}
		lock := c.locks[i]
		blockId := lock.Next()
		for {
			if blockId == -1 || blockId >= c.blockNum {
				break
			}
			if !lock.Set(blockId) { // set failed
				blockId = lock.Next()
			} else {
				atomic.AddInt32(&c.counters[i], 1)
				return i, blockId, true
			}
		}
	}
	return -1, -1, false
}

func (c *BlockFileContainer) Remove(bankId int32, blockId int32) {
	if bankId < 0 {
		return
	}
	c.locks[bankId].Clear(blockId)
	atomic.AddInt32(&c.counters[bankId], -1)
}

func (c *BlockFileContainer) MultiRemove(banks []int32, blocks []int32) {
	for i, bankId := range banks {
		c.Remove(bankId, blocks[i])
	}
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
