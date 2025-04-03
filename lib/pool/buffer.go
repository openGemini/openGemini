// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package pool

import (
	"github.com/openGemini/openGemini/lib/cpu"
)

const (
	maxChunkMetaLocalCacheSize = 48 * 1024 * 1024 // 48 MB
)

type Buffer struct {
	B    []byte
	Swap []byte
}

func (b *Buffer) MemSize() int {
	return cap(b.B) + cap(b.Swap)
}

var chunkMetaPool *UnionPool[Buffer]

func init() {
	chunkMetaPool = NewUnionPool[Buffer](cpu.GetCpuNum()*2, maxChunkMetaLocalCacheSize, DefaultMaxEleMemSize, func() *Buffer {
		return &Buffer{}
	})
	chunkMetaPool.EnableHitRatioStat("ChunkMetaBuffer")
}

func GetChunkMetaBuffer() (*Buffer, func()) {
	buf := chunkMetaPool.Get()
	return buf, func() {
		chunkMetaPool.Put(buf)
	}
}
