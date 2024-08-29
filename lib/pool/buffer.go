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
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
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

func (b *Buffer) Instance() Object {
	return &Buffer{}
}

var chunkMetaPool *ObjectPool

func init() {
	s := stat.NewHitRatioStatistics()
	hook := NewHitRatioHook(s.AddChunkMetaGetTotal, s.AddChunkMetaHitTotal)
	chunkMetaPool = NewObjectPool(cpu.GetCpuNum()*2, &Buffer{}, maxChunkMetaLocalCacheSize)
	chunkMetaPool.SetHitRatioHook(hook)
}

func GetChunkMetaBuffer() *Buffer {
	b, ok := chunkMetaPool.Get().(*Buffer)
	if !ok || b == nil {
		b = &Buffer{}
	}
	return b
}

func PutChunkMetaBuffer(b *Buffer) {
	chunkMetaPool.Put(b)
}
