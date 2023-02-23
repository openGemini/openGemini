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

package executor

// CircularChunkNum Do not Modify.
// CircularChunks consists of one input chunk, two cached chunks(agg/fill and cached channel) and one chunk(cycle)
const CircularChunkNum = 4

type CircularChunkPool struct {
	index    int
	chunkNum int
	chunks   []Chunk
}

func NewCircularChunkPool(chunkNum int, chunkBuilder *ChunkBuilder) *CircularChunkPool {
	rcp := &CircularChunkPool{
		index:    0,
		chunkNum: chunkNum,
		chunks:   make([]Chunk, 0, chunkNum),
	}
	for i := 0; i < chunkNum; i++ {
		rcp.chunks = append(rcp.chunks, chunkBuilder.NewChunk(""))
	}
	return rcp
}

func (cp *CircularChunkPool) GetChunk() Chunk {
	c := cp.chunks[cp.index]
	c.Reset()
	cp.index = (cp.index + 1) % cp.chunkNum
	return c
}

func (cp *CircularChunkPool) Release() {
	for i := 0; i < cp.chunkNum; i++ {
		cp.chunks[i].Release()
	}
}

// BlockChunkPool fixed-capacity memory pool that blocks when the pool is empty or full.
type BlockChunkPool struct {
	pool chan Chunk
}

func NewBlockChunkPool(chunkNum int, chunkBuilder *ChunkBuilder) *BlockChunkPool {
	cp := &BlockChunkPool{
		pool: make(chan Chunk, chunkNum),
	}
	for i := 0; i < chunkNum; i++ {
		cp.pool <- chunkBuilder.NewChunk("")
	}
	return cp
}

func (cp *BlockChunkPool) Get() Chunk {
	for {
		select {
		case c := <-cp.pool:
			return c
		}
	}
}

func (cp *BlockChunkPool) Put(c Chunk) {
	c.Reset()
	for {
		select {
		case cp.pool <- c:
			return
		}
	}
}

func (cp *BlockChunkPool) Release() {
	close(cp.pool)
}
