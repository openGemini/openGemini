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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockFileContainer_Initialize(t *testing.T) {
	c, err := NewBlockFileContainer("/tmp/.", 2, 2, 4)
	assert.Nil(t, c)
	assert.NotNil(t, err)

	c, err = NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 4)
	assert.NotNil(t, c)
	assert.Nil(t, err)
}

func TestBlockFileContainer_Store(t *testing.T) {
	c, _ := NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 2)
	block1 := []byte{1, 1}
	block2 := []byte{2, 2}
	block3 := []byte{3, 3}
	block4 := []byte{4, 4}
	bankId, blockId, success := c.Store(block1)
	assert.Equal(t, int32(0), bankId)
	assert.Equal(t, int32(0), blockId)
	assert.True(t, success)
	bankId, blockId, success = c.Store(block2)
	assert.Equal(t, int32(0), bankId)
	assert.Equal(t, int32(1), blockId)
	assert.True(t, success)
	bankId, blockId, success = c.Store(block3)
	assert.Equal(t, int32(1), bankId)
	assert.Equal(t, int32(0), blockId)
	assert.True(t, success)
	bankId, blockId, success = c.Store(block4)
	assert.Equal(t, int32(1), bankId)
	assert.Equal(t, int32(1), blockId)
	assert.True(t, success)
	bankId, blockId, success = c.Store(block4)
	assert.Equal(t, int32(-1), bankId)
	assert.Equal(t, int32(-1), blockId)
	assert.False(t, success)
}

func TestBlockFileContainer_Fetch(t *testing.T) {
	c, _ := NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 2)
	res, success := c.Fetch(0, 0)
	assert.False(t, success)
	assert.Nil(t, res)
	block1 := []byte{1, 1}
	block2 := []byte{2, 2}
	block3 := []byte{3, 3}
	block4 := []byte{4, 4}
	c.Store(block1)
	c.Store(block2)
	c.Store(block3)
	c.Store(block4)
	res, success = c.Fetch(0, 0)
	assert.True(t, success)
	assert.Equal(t, block1, res)
	res, success = c.Fetch(0, 1)
	assert.True(t, success)
	assert.Equal(t, block2, res)
	res, success = c.Fetch(1, 0)
	assert.True(t, success)
	assert.Equal(t, block3, res)
	res, success = c.Fetch(1, 1)
	assert.True(t, success)
	assert.Equal(t, block4, res)
	res, success = c.Fetch(1, 2)
	assert.False(t, success)
	assert.Nil(t, res)
	res, success = c.Fetch(2, 1)
	assert.False(t, success)
	assert.Nil(t, res)
	res, success = c.Fetch(-1, 1)
	assert.False(t, success)
	assert.Nil(t, res)
}

func TestBlockFileContainer_FetchWith(t *testing.T) {
	c, _ := NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 2)
	res, success := c.FetchWith(0, 0, 0, 1)
	assert.False(t, success)
	assert.Nil(t, res)
	block1 := []byte{1, 1}
	block2 := []byte{2, 2}
	block3 := []byte{3, 3}
	block4 := []byte{4, 4}
	c.Store(block1)
	c.Store(block2)
	c.Store(block3)
	c.Store(block4)
	res, success = c.FetchWith(0, 0, 0, 1)
	assert.True(t, success)
	assert.Equal(t, []byte{1}, res)
	res, success = c.FetchWith(0, 0, 1, 1)
	assert.True(t, success)
	assert.Equal(t, []byte{1}, res)
	res, success = c.FetchWith(0, 0, 0, 2)
	assert.True(t, success)
	assert.Equal(t, []byte{1, 1}, res)
	res, success = c.FetchWith(0, 1, 0, 1)
	assert.True(t, success)
	assert.Equal(t, []byte{2}, res)
	res, success = c.FetchWith(0, 1, 1, 1)
	assert.True(t, success)
	assert.Equal(t, []byte{2}, res)
	res, success = c.FetchWith(0, 1, 0, 2)
	assert.True(t, success)
	assert.Equal(t, []byte{2, 2}, res)
	_, success = c.FetchWith(-1, 1, 1, 1)
	assert.False(t, success)
	_, success = c.FetchWith(2, 1, 1, 1)
	assert.False(t, success)
	_, success = c.FetchWith(0, 2, 1, 1)
	assert.False(t, success)
	_, success = c.FetchWith(0, 1, 1, 2)
	assert.False(t, success)
	_, success = c.FetchWith(0, 1, 1, 0)
	assert.False(t, success)
}

func TestBlockFileContainer_Remove(t *testing.T) {
	c, _ := NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 2)
	c.Store([]byte{1, 1})
	_, success := c.Fetch(0, 0)
	assert.True(t, success)
	c.Remove(0, 0)
	c.Remove(0, 0)
	_, success = c.Fetch(0, 0)
	assert.False(t, success)
	c.Remove(-1, 0)
}

func TestBlockFileContainer_MultiStore(t *testing.T) {
	c, _ := NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 2)
	content := []byte{1, 1, 2, 2, 3, 3, 4, 4}
	banks, blocks, success := c.MultiStore(content)
	assert.Equal(t, []int32{0, 0, 1, 1}, banks)
	assert.Equal(t, []int32{0, 1, 0, 1}, blocks)
	assert.True(t, success)

	banks, blocks, success = c.MultiStore(content)
	assert.Equal(t, []int32(nil), banks)
	assert.Equal(t, []int32(nil), blocks)
	assert.False(t, success)
}

func TestBlockFileContainer_MultiFetch(t *testing.T) {
	c, _ := NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 2)
	res, success := c.MultiFetch([]int32{0}, []int32{0}, 2)
	assert.False(t, success)
	assert.Nil(t, res)

	content := []byte{1, 1, 2, 2, 3, 3, 4, 4}
	c.MultiStore(content)
	res, success = c.MultiFetch([]int32{0, 0, 1, 1}, []int32{0, 1, 0, 1}, 8)
	assert.True(t, success)
	assert.Equal(t, content, res)
	res, success = c.MultiFetch([]int32{0, 0, 1, 1}, []int32{0, 1, 0, 1}, 7)
	assert.True(t, success)
	assert.Equal(t, []byte{1, 1, 2, 2, 3, 3, 4}, res)
	res, success = c.MultiFetch([]int32{1}, []int32{2}, 2)
	assert.False(t, success)
	assert.Nil(t, res)
	res, success = c.MultiFetch([]int32{2}, []int32{1}, 2)
	assert.False(t, success)
	assert.Nil(t, res)
	res, success = c.MultiFetch([]int32{-1}, []int32{1}, 2)
	assert.False(t, success)
	assert.Nil(t, res)
}

func TestBlockFileContainer_MultiRemove(t *testing.T) {
	c, _ := NewBlockFileContainer("/tmp/test_block_file_container/", 2, 2, 2)
	c.Store([]byte{1, 1, 2, 2, 3, 3, 4, 4})
	c.MultiRemove([]int32{0, 1}, []int32{1, 0})
	_, success := c.Fetch(0, 1)
	assert.False(t, success)
	_, success = c.Fetch(1, 0)
	assert.False(t, success)
}
