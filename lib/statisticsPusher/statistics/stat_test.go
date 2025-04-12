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

	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/require"
)

func TestShelf(t *testing.T) {
	obj := stat.NewShelf()
	obj.WALConverting.Store(10)
	obj.WALFileCount.Incr()
	obj.WALFileCount.Incr()
	obj.WALFileCount.Decr()

	runTest(t, "shelf", "WALConverting=10", "WALFileCount=1")
}

func TestHotMode(t *testing.T) {
	obj := stat.NewHotMode()
	obj.ReadBytesTotal.Add(11)
	obj.TotalMemorySize.Add(2)

	runTest(t, "hotMode", "ReadBytesTotal=11")
}

func TestResultCache(t *testing.T) {
	obj := stat.NewResultCache()
	obj.TotalCacheSize.Store(1000)
	obj.InuseCacheSize.Store(800)
	obj.CacheTotal.Add(1)

	runTest(t, "resultCache", "TotalCacheSize=1000", "InuseCacheSize=800", "CacheTotal=1")
}

func runTest(t *testing.T, mst string, contents ...string) {
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
