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

package lastrowcache

import (
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/lastrowcache"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/smartystreets/goconvey/convey"
)

func TestLastRowCacheStat(t *testing.T) {
	convey.Convey("test last row status", t, func() {
		config.SetLastRowCacheConfig(config.NewLastRowCacheConfig())
		err := lastrowcache.InitCache()
		convey.So(err, convey.ShouldBeNil)

		value := &lastrowcache.CacheValue{}
		value.SetTimestamp(123)
		fields := &sync.Map{}
		fields.Store("fieldKey1", float64(66))
		value.SetFields(fields)
		lastrowcache.Set("test", "test", []byte("test"), value)
		time.Sleep(time.Millisecond * 10)

		ts, fieldVals, flag := lastrowcache.Get("test", "test", []byte("test"), []string{"fieldKey1"})
		convey.ShouldBeTrue(flag)
		convey.So(ts, convey.ShouldEqual, int64(123))
		convey.So(len(fieldVals), convey.ShouldEqual, 1)
		convey.So(fieldVals["fieldKey1"], convey.ShouldEqual, float64(66))

		stats := genLastRowCacheStatMap()
		convey.So(stats[StatLastRowCacheHits], convey.ShouldEqual, 1)

		InitLastRowCacheStatistics(map[string]string{"host": "127.0.0.1"})
		CollectOpsLastRowCacheStatistics()
		_, err = CollectLastRowCacheStatistics([]byte{})
		convey.So(err, convey.ShouldBeNil)
	})
}
