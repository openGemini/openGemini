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

	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/stretchr/testify/assert"
)

func TestCreateInstance(t *testing.T) {
	config := config.ResultCacheConfig{CacheType: config.MEM_CACHE, MemCacheSize: 100}
	instance := NewResultCache(config)
	_, ok := instance.(*MemCacheAdapter)
	assert.True(t, ok)
}

func TestCreateInstanceNil(t *testing.T) {
	config := config.ResultCacheConfig{CacheType: config.FILE_CACHE, MemCacheSize: 100}
	instance := NewResultCache(config)
	assert.Equal(t, instance, nil)
}
