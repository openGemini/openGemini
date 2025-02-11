// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package resultcache

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

var log = logger.NewLogger(errno.ModuleQueryEngine)

type ByteArrayEntry struct {
	key   string
	value []byte
	time  time.Time
}

func (b *ByteArrayEntry) SetTime(time time.Time) {
	b.time = time
}

func (b *ByteArrayEntry) GetTime() time.Time {
	return b.time
}

func (b *ByteArrayEntry) SetValue(value interface{}) {
	byteValue, ok := value.([]byte)
	if !ok {
		log.Error(fmt.Sprintf("value is not the type of byte[], value: %v, [SetValue]", value))
		return
	}
	b.value = byteValue
}

func (b *ByteArrayEntry) GetValue() interface{} {
	return b.value
}

func (b *ByteArrayEntry) GetKey() string {
	return b.key
}

func (b *ByteArrayEntry) Size() int64 {
	size := int(unsafe.Sizeof(*b))
	size += len(b.key)
	size += cap(b.value)
	size += int(unsafe.Sizeof(*b.time.Location()))
	return int64(size)
}

type MemCacheAdapter struct {
	memCache *cache.Cache
}

func (adapter *MemCacheAdapter) Get(key string) ([]byte, bool) {
	value, ok := adapter.memCache.Get(key)
	if !ok {
		return nil, false
	}

	result, ok := value.GetValue().([]byte)
	return result, ok
}

func (adapter *MemCacheAdapter) Put(key string, value []byte) {
	entry := ByteArrayEntry{key: key, value: value}
	adapter.memCache.Put(key, &entry, func(cache.Entry, cache.Entry) bool { return true })
	statistics.NewResultCache().InuseCacheSize.Store(adapter.memCache.Size())
}
