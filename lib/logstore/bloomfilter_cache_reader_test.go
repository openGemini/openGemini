/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/stretchr/testify/assert"
)

func TestNewBloomfilterReader(t *testing.T) {
	filePath := "/tmp/test_block_cache_reader"
	fileName := "field.bf"
	reader := NewVlmCacheReader(nil, filePath, fileName, 0)
	assert.NotNil(t, reader)
}

func TestVlmCacheReader_ReadAllHit(t *testing.T) {
	blockLruCache := NewBlockLruCache("/tmp/test_block_cache_store_reader/", PieceCacheType, 16, 2, time.Hour, nil)
	r := &VlmCacheReader{
		basicReader:   &MockStoreReader{},
		pieceLruCache: blockLruCache,
	}
	r.pieceLruCache.Store(BlockCacheKey{0, 0}, 0, []byte{1, 1})
	r.pieceLruCache.Store(BlockCacheKey{0, 2}, 0, []byte{2, 2})
	r.pieceLruCache.Store(BlockCacheKey{0, 4}, 0, []byte{3, 3})
	r.pieceLruCache.Store(BlockCacheKey{0, 6}, 0, []byte{4, 4})

	offs := []int64{0, 2, 4, 6}
	sizes := []int64{2, 2, 2, 2}
	result := make(map[int64][]byte)
	err := r.ReadBatch(offs, sizes, 0, true, result)
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 1}, result[0][0:2])
	assert.Equal(t, []byte{2, 2}, result[2][0:2])
	assert.Equal(t, []byte{3, 3}, result[4][0:2])
	assert.Equal(t, []byte{4, 4}, result[6][0:2])
}

func TestVlmCacheReader_GroupAllHit(t *testing.T) {
	blockLruCache := NewBlockLruCache("/tmp/test_block_cache_store_reader/", GroupCacheType, 16, 2, time.Hour, nil)
	r := &VlmCacheReader{
		basicReader:   &MockStoreReader{},
		groupLruCache: blockLruCache,
	}
	r.groupLruCache.Store(BlockCacheKey{0, 0}, 0, []byte{1, 1})

	blockSize := uint64(blockLruCache.blockFileContainers[0].blockSize)
	r.groupLruCache.Store(BlockCacheKey{0, 1 * blockSize}, 0, []byte{2, 2})
	r.groupLruCache.Store(BlockCacheKey{0, 2 * blockSize}, 0, []byte{3, 3})
	r.groupLruCache.Store(BlockCacheKey{0, 3 * blockSize}, 0, []byte{4, 4})
	bytes, exit := r.groupLruCache.FetchWith(BlockCacheKey{0, 3 * blockSize}, 0, 2)
	assert.True(t, exit)
	assert.Equal(t, []byte{4, 4}, bytes[0:2])

	iblockSize := int64(blockSize)
	offs := []int64{0, 1 * iblockSize, 2 * iblockSize, 3 * iblockSize}
	sizes := []int64{2, 2, 2, 2}
	result := make(map[int64][]byte)
	err := r.ReadBatch(offs, sizes, 0, true, result)
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 1}, result[0][0:2])
	assert.Equal(t, []byte{2, 2}, result[1*iblockSize][0:2])
	assert.Equal(t, []byte{3, 3}, result[2*iblockSize][0:2])
	assert.Equal(t, []byte{4, 4}, result[3*iblockSize][0:2])
}

func TestVlmCacheReader_ReadAllMiss(t *testing.T) {
	mockReader := &MockStoreReader{}
	blockLruCache := NewBlockLruCache("/tmp/test_block_cache_store_reader/", PieceCacheType, 16, 2, time.Hour, nil)
	r := &VlmCacheReader{
		basicReader:    mockReader,
		pieceLruCache:  blockLruCache,
		pathId:         0,
		version:        0,
		enableHotCache: true,
	}
	offs := []int64{0, 2, 4, 6}
	sizes := []int64{2, 2, 2, 2}
	result := make(map[int64][]byte)
	mockReader.readErr = true
	err := r.ReadBatch(offs, sizes, 0, false, result)
	assert.NotNil(t, err)

	mockReader.readErr = false
	mockReader.contents = [][]byte{{1, 1}, {2, 2}, {3, 3}, {4, 4}}
	err = r.ReadBatch(offs, sizes, 0, false, result)
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 1}, result[0])
	assert.Equal(t, []byte{2, 2}, result[2])
	assert.Equal(t, []byte{3, 3}, result[4])
	assert.Equal(t, []byte{4, 4}, result[6])

	vlmCacheTimeOut = 0
	bytes, exist := r.pieceLruCache.Fetch(BlockCacheKey{0, 0})
	assert.True(t, exist)
	assert.Equal(t, []byte{1, 1}, bytes[0:2])
	bytes, exist = r.pieceLruCache.Fetch(BlockCacheKey{0, 2})
	assert.True(t, exist)
	assert.Equal(t, []byte{2, 2}, bytes[0:2])
	bytes, exist = r.pieceLruCache.Fetch(BlockCacheKey{0, 4})
	assert.True(t, exist)
	assert.Equal(t, []byte{3, 3}, bytes[0:2])
	bytes, exist = r.pieceLruCache.Fetch(BlockCacheKey{0, 6})
	assert.True(t, exist)
	assert.Equal(t, []byte{4, 4}, bytes[0:2])
}

func TestVlmCacheReader_ReadPartialHit(t *testing.T) {
	_, span := tracing.NewTrace("vlmcache")
	span.AppendNameValue("read", "bloomfiter cache")
	mockReader := &MockStoreReader{}
	blockLruCache := NewBlockLruCache("/tmp/test_block_cache_store_reader/", PieceCacheType, 16, 2, time.Hour, nil)
	r := &VlmCacheReader{
		basicReader:    mockReader,
		pieceLruCache:  blockLruCache,
		pathId:         0,
		version:        0,
		enableHotCache: true,
	}
	r.StartSpan(span)
	r.pieceLruCache.Store(BlockCacheKey{0, 0}, 0, []byte{1, 1})
	r.pieceLruCache.Store(BlockCacheKey{0, 2}, 0, []byte{2, 2})
	mockReader.contents = [][]byte{{3, 3}, {4, 4}}

	offs := []int64{0, 2, 4, 6}
	sizes := []int64{2, 2, 2, 2}
	result := make(map[int64][]byte)
	err := r.ReadBatch(offs, sizes, 0, false, result)
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 1}, result[0][0:2])
	assert.Equal(t, []byte{2, 2}, result[2][0:2])
	assert.Equal(t, []byte{3, 3}, result[4][0:2])
	assert.Equal(t, []byte{4, 4}, result[6][0:2])

	vlmCacheTimeOut = 0
	bytes, exist := r.pieceLruCache.Fetch(BlockCacheKey{0, 4})
	assert.True(t, exist)
	assert.Equal(t, []byte{3, 3}, bytes[0:2])
	bytes, exist = r.pieceLruCache.Fetch(BlockCacheKey{0, 6})
	assert.True(t, exist)
	assert.Equal(t, []byte{4, 4}, bytes[0:2])
}

type MockStoreReader struct {
	readErr  bool
	contents [][]byte
}

func (m *MockStoreReader) Size() (int64, error) {
	return 0, nil
}

func (m *MockStoreReader) ReadBatch(offs, sizes []int64, limit int, isStat bool, results map[int64][]byte) error {
	if m.readErr {
		return fmt.Errorf("mock read error")
	}
	for i, offset := range offs {
		results[offset] = m.contents[i]
	}
	return nil
}

func (m *MockStoreReader) Close() error {
	return nil
}

func (m *MockStoreReader) StartSpan(span *tracing.Span) {
}

func (m *MockStoreReader) EndSpan() {
}
