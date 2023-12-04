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

package immutable

import (
	"fmt"
	"time"

	"github.com/openGemini/openGemini/lib/cache"
	"go.uber.org/zap"
)

const (
	ChunkMetaSize int64 = 128 * 1024 * 1024
	ChunkMetaTTL        = 60 * time.Minute
)

var ChunkMetaCache = cache.NewCache(ChunkMetaSize, ChunkMetaTTL)

type ChunkMetaEntry struct {
	filePath  string
	chunkMeta *ChunkMeta
	ts        time.Time
}

func NewChunkMetaEntry(filePath string) *ChunkMetaEntry {
	return &ChunkMetaEntry{filePath: filePath}
}

func (e *ChunkMetaEntry) SetTime(time time.Time) {
	e.ts = time
}

func (e *ChunkMetaEntry) GetTime() time.Time {
	return e.ts
}

func (e *ChunkMetaEntry) SetValue(value interface{}) {
	meta, ok := value.(*ChunkMeta)
	if !ok {
		log.Error("ChunkMetaEntry", zap.Error(fmt.Errorf("invalid element type")))
	}
	e.chunkMeta = meta
}

func (e *ChunkMetaEntry) GetValue() interface{} {
	return e.chunkMeta
}

func (e *ChunkMetaEntry) GetKey() string {
	return e.filePath
}

func (e *ChunkMetaEntry) Size() int64 {
	var size int64
	size += int64(len(e.filePath))           // filePath
	size += 24                               // time
	size += 60                               // common of chunk meta
	size += int64(e.chunkMeta.segCount * 16) // time range of chunk meta
	for i := range e.chunkMeta.colMeta {
		size += int64(len(e.chunkMeta.colMeta[i].name)+17+len(e.chunkMeta.colMeta[i].preAgg)) + int64(e.chunkMeta.segCount*12) // column meta of chunk meta
	}
	return size
}

func UpdateChunkMetaFunc(_, _ cache.Entry) bool {
	return true
}

func PutChunkMeta(filePath string, chunkMeta *ChunkMeta) {
	entry := NewChunkMetaEntry(filePath)
	entry.SetValue(chunkMeta)
	ChunkMetaCache.Put(filePath, entry, UpdateChunkMetaFunc)
}

func GetChunkMeta(filePath string) (*ChunkMeta, bool) {
	entry, ok := ChunkMetaCache.Get(filePath)
	if !ok {
		return nil, false
	}
	chunkMeta, ok := entry.GetValue().(*ChunkMeta)
	return chunkMeta, ok
}
