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

package statistics_test

import (
	"testing"
	"time"

	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/require"
)

func TestShelf(t *testing.T) {
	obj := stat.NewShelf()
	obj.WALConverting.Store(10)
	obj.WALFileCount.Incr()
	obj.WALFileCount.Incr()
	obj.WALFileCount.Decr()

	assertCollect(t, "shelf", "WALConverting=10", "WALFileCount=1")
}

func TestHotMode(t *testing.T) {
	obj := stat.NewHotMode()
	obj.ReadBytesTotal.Add(11)
	obj.TotalMemorySize.Add(2)

	assertCollect(t, "hotMode", "ReadBytesTotal=11")
}

func TestResultCache(t *testing.T) {
	obj := stat.NewResultCache()
	obj.TotalCacheSize.Store(1000)
	obj.InuseCacheSize.Store(800)
	obj.CacheTotal.Add(1)

	assertCollect(t, "resultCache", "TotalCacheSize=1000", "InuseCacheSize=800", "CacheTotal=1")
}

func TestStoreQuery(t *testing.T) {
	obj := stat.NewStoreQuery()
	obj.StoreQueryRequests.Store(10)
	obj.UnmarshalQueryTimeTotal.Store(100)

	assertCollect(t, obj.MeasurementName(), "storeQueryReq=10", "UnmarshalQueryTimeTotal=100")
	assertCollectOps(t, obj.MeasurementName(), map[string]interface{}{
		"storeQueryReq":           int64(10),
		"UnmarshalQueryTimeTotal": int64(100),
	})
}

func TestHandler(t *testing.T) {
	obj := stat.NewHandler()
	obj.QueryRequests.Store(10)
	obj.Query400ErrorStmtCount.Store(10)
	obj.Query400ErrorStmtCount.Add(5)
	obj.Query400ErrorStmtCount.Incr()
	obj.Query400ErrorStmtCount.Decr()
	obj.Query400ErrorStmtCount.Decr()

	assertCollect(t, obj.MeasurementName(), "queryStmtCount=14", "queryErrorStmtCount=10")
	assertCollectOps(t, obj.MeasurementName(), map[string]interface{}{
		"queryStmtCount":      int64(14),
		"queryErrorStmtCount": int64(10),
	})
}

func TestRuntime(t *testing.T) {
	now := time.Now()
	obj := stat.RuntimeIns()
	obj.SetVersion("1.0.0")
	obj.SetSpdyCertExpireAt(now)

	assertCollect(t, "runtime", `Version="1.0.0"`, `HttpsCertExpireAt=""`)

	for range 5 {
		stat.NewCollector().CollectOps()
	}
	assertCollectOps(t, "runtime", map[string]interface{}{
		"Version":          "1.0.0",
		"SpdyCertExpireAt": now.Format(time.DateTime),
	})
}

func assertCollect(t *testing.T, mst string, contents ...string) {
	col := stat.NewCollector()
	col.SetGlobalTags(map[string]string{
		"host": "127.0.0.1",
	})

	buf, err := col.Collect(nil)
	require.NoError(t, err)

	require.Contains(t, string(buf), mst+",")
	for _, content := range contents {
		require.Contains(t, string(buf), content)
	}
	require.Contains(t, string(buf), "host=127.0.0.1")
}

func assertCollectOps(t *testing.T, mst string, exp map[string]interface{}) {
	col := stat.NewCollector()
	col.SetGlobalTags(map[string]string{
		"host": "127.0.0.1",
	})

	mstExists := false
	items := col.CollectOps()
	for _, item := range items {
		if item.Name != mst {
			continue
		}

		for key, value := range exp {
			require.Equal(t, value, item.Values[key])
		}
		mstExists = true
	}
	require.True(t, mstExists)
}
