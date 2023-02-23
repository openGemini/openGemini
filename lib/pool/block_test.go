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

package pool_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/pool"
)

func Test_DataBlockPool(t *testing.T) {
	var blocks []*pool.DataBlock
	num := 257
	for i := 0; i < num; i++ {
		blocks = append(blocks, pool.StreamDataBlockGet())
	}
	for b := range blocks {
		pool.StreamDataBlockPut(blocks[b])
	}
	blocks = blocks[:0]
	for i := 0; i < num; i++ {
		blocks = append(blocks, pool.StreamDataBlockGet())
	}
}

func Test_DataBlock_Exchange(t *testing.T) {
	block := pool.StreamDataBlockGet()
	block.ReqBuf = []byte("block")
	block2 := pool.StreamDataBlockGet()
	block2.ReqBuf = []byte("block2")
	block.Exchange(&block2.ReqBuf, &block2.Rows, &block2.TagPool, &block2.FieldPool, &block2.IndexKeyPool,
		&block2.IndexOptionPool, &block2.LastResetTime)
	if string(block.ReqBuf) != "block2" {
		t.Error(fmt.Sprintf("expect %v ,got %v", "block2", string(block.ReqBuf)))
	}
}
