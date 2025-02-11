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
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/stretchr/testify/assert"
)

func TestPutValue(t *testing.T) {
	config := config.ResultCacheConfig{CacheType: config.MEM_CACHE, MemCacheSize: 400, MemCacheExpiration: toml.Duration(time.Hour)}
	instance := NewResultCache(config)
	instance.Put("a", []byte("hello"))
	result, _ := instance.Get("a")
	assert.Equal(t, string(result), "hello")
}

func TestGetNoExistKey(t *testing.T) {
	config := config.ResultCacheConfig{CacheType: config.MEM_CACHE, MemCacheSize: 400, MemCacheExpiration: toml.Duration(time.Hour)}
	instance := NewResultCache(config)
	result, ok := instance.Get("b")
	assert.False(t, ok)
	assert.Equal(t, cap(result), 0)
}

func TestGetSize(t *testing.T) {
	entry := ByteArrayEntry{key: "abc", value: []byte("hello"), time: time.Now()}
	assert.Equal(t, entry.Size(), int64(176))
}

func TestGetTime(t *testing.T) {
	entry := ByteArrayEntry{key: "abc", value: []byte("hello")}
	now := time.Now()
	entry.SetTime(now)
	assert.Equal(t, entry.GetTime(), now)
}

func TestSetValue(t *testing.T) {
	entry := ByteArrayEntry{key: "abc", value: []byte("hello")}
	entry.SetValue([]byte("hi"))
	result, _ := entry.GetValue().([]byte)
	assert.Equal(t, string(result), "hi")
}

func TestSetErrorValue(t *testing.T) {
	entry := ByteArrayEntry{key: "abc", value: []byte("hello")}
	entry.SetValue(123)
	result, _ := entry.GetValue().([]byte)
	assert.Equal(t, string(result), "hello")
}
