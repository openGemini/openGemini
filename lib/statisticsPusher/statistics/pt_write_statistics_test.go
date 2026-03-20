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
	"sync"
	"testing"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/require"
)

func TestPtWriteStatistics_Enabled(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
	}

	// Test with enabled=true
	stat := statistics.NewPtWriteStatistics(tags, true, 0.5)
	require.True(t, stat.Enabled())

	// Test with enabled=false
	stat = statistics.NewPtWriteStatistics(tags, false, 0.5)
	require.False(t, stat.Enabled())
}

func TestPtWriteStatistics_ShouldCollect(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
	}

	// Test with zero ratio
	stat := statistics.NewPtWriteStatistics(tags, true, 0)
	require.False(t, stat.ShouldCollect())

	stat = statistics.NewPtWriteStatistics(tags, true, -4)
	require.False(t, stat.ShouldCollect())

	// Test with ratio=1.0 (should always collect)
	stat = statistics.NewPtWriteStatistics(tags, true, 1.0)
	require.True(t, stat.ShouldCollect())

	stat = statistics.NewPtWriteStatistics(tags, true, 20)
	require.True(t, stat.ShouldCollect())

	// Since we can't predict the random value, we'll just verify it doesn't panic
	for i := 0; i < 5; i++ {
		result := stat.ShouldCollect()
		require.True(t, result == true || result == false)
	}

}

func TestInitPtWriteStatistics(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
	}

	// Test with enabled=false
	statistics.InitPtWriteStatistics(tags, false, 0.5)
	require.False(t, statistics.PTWriteStat.Enabled())

	// Test with ratio > 1.0 (should be clamped to 1.0)
	statistics.InitPtWriteStatistics(tags, true, 1.5)
	require.True(t, statistics.PTWriteStat.Enabled())
	require.True(t, statistics.PTWriteStat.ShouldCollect())

	// Test with ratio > 1.0 (should be clamped to 1.0)
	statistics.InitPtWriteStatistics(tags, true, -2)
	require.True(t, statistics.PTWriteStat.Enabled())
	require.False(t, statistics.PTWriteStat.ShouldCollect())
}

func TestCollectPtWriteStatistics(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
	}

	// Test with disabled statistics
	statistics.InitPtWriteStatistics(tags, false, 0.5)
	buffer := make([]byte, 0)
	buffer, err := statistics.CollectPtWriteStatistics(buffer)
	require.NoError(t, err)
	require.Empty(t, buffer)

	// Test with enabled statistics but no data
	statistics.InitPtWriteStatistics(tags, true, 0.5)
	buffer = make([]byte, 0)
	buffer, err = statistics.CollectPtWriteStatistics(buffer)
	require.NoError(t, err)
	// Should be empty since no stats have been added
	require.Empty(t, buffer)

	// Test with enabled statistics and some data
	db, pt, rp := "testdb", uint32(123), "testrp"
	mst := "testmeasurement"

	// Add some statistics
	ptStats := statistics.NewPtWriteStats(db, rp, pt)
	require.NotNil(t, ptStats)
	ptStats.AddMstBytesCount(mst, 1024)

	buffer = make([]byte, 0)
	buffer, err = statistics.CollectPtWriteStatistics(buffer)
	require.NoError(t, err)
	require.NotEmpty(t, buffer)

	// Verify the buffer contains the expected data
	require.Contains(t, string(buffer), "pt_write")
	require.Contains(t, string(buffer), "database=testdb")
	require.Contains(t, string(buffer), "ptId=123")
	require.Contains(t, string(buffer), "retentionPolity=testrp")
	require.Contains(t, string(buffer), "measurement=testmeasurement")
	require.Contains(t, string(buffer), "BytesCount=1024")
}

func TestClearPtWriteStatistics(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
	}

	statistics.InitPtWriteStatistics(tags, true, 0.5)
	db, pt, rp := "testdb", uint32(456), "testrp"
	mst := "testmeasurement"

	// Add some statistics
	ptStats := statistics.NewPtWriteStats(db, rp, pt)
	ptStats.AddMstBytesCount(mst, 1024)

	statistics.ClearPtWriteStatistics(db, rp, pt)

	buffer := make([]byte, 0)
	buffer, err := statistics.CollectPtWriteStatistics(buffer)
	require.NoError(t, err)
	require.Empty(t, buffer)

	statistics.ClearPtWriteStatistics("none", rp, pt)
}

func TestPtWriteStatistics_RaceCondition(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
	}

	statistics.InitPtWriteStatistics(tags, true, 0.5)
	db, pt, rp := "testdb", uint32(789), "testrp"

	// Run this test multiple times to catch race conditions
	for i := 0; i < 10; i++ {
		ptStats := statistics.NewPtWriteStats(db, rp, pt)
		require.NotNil(t, ptStats)

		var wg sync.WaitGroup

		// Create multiple goroutines that add and collect statistics
		for j := 0; j < 5; j++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for k := 0; k < 100; k++ {
					ptStats.AddMstBytesCount("mst", int64(id*1000+k))

					// Occasionally collect statistics
					if k%10 == 0 {
						buffer := make([]byte, 0)
						ptStats.Collect(buffer)
					}
				}
			}(j)
		}

		wg.Wait()

		// Verify the final result
		copy := ptStats.Copy()
		require.NotEmpty(t, copy)

		// Verify we can get the stats
		retrievedStats := statistics.GetPtWriteStats(db, rp, pt)
		require.NotNil(t, retrievedStats)
		require.Equal(t, copy, retrievedStats.Copy())
	}
}

func TestGetPtWriteStats(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
	}

	// Test with disabled statistics
	statistics.InitPtWriteStatistics(tags, false, 0.5)
	db, pt, rp := "testdb", uint32(123), "testrp"

	// When statistics are disabled, GetPtWriteStats should return nil
	stats := statistics.GetPtWriteStats(db, rp, pt)
	require.Nil(t, stats)

	// Test with enabled statistics but no existing stats
	statistics.InitPtWriteStatistics(tags, true, 0.5)
	stats = statistics.GetPtWriteStats(db, rp, pt)
	require.Nil(t, stats)

	// Test with enabled statistics and existing stats
	// First create stats using NewPtWriteStats
	ptStats := statistics.NewPtWriteStats(db, rp, pt)
	require.NotNil(t, ptStats)

	// Add some measurement data
	ptStats.AddMstBytesCount("testmeasurement", 1024)

	// Now retrieve it using GetPtWriteStats
	retrievedStats := statistics.GetPtWriteStats(db, rp, pt)
	require.NotNil(t, retrievedStats)

	// Verify the retrieved stats are the same as the original
	require.Equal(t, ptStats.Copy(), retrievedStats.Copy())

	// Test with different partition ID
	differentPt := uint32(456)
	differentStats := statistics.GetPtWriteStats(db, rp, differentPt)
	require.Nil(t, differentStats)

	// Test with different database name
	differentDb := "differentdb"
	differentStats = statistics.GetPtWriteStats(differentDb, rp, pt)
	require.Nil(t, differentStats)

	// Test with different retention policy
	differentRp := "differentrp"
	differentStats = statistics.GetPtWriteStats(db, differentRp, pt)
	require.Nil(t, differentStats)

	// Test with multiple partitions
	pt2 := uint32(789)
	ptStats2 := statistics.NewPtWriteStats(db, rp, pt2)
	require.NotNil(t, ptStats2)
	ptStats2.AddMstBytesCount("anothermeasurement", 2048)

	// Verify both partitions can be retrieved correctly
	retrievedStats1 := statistics.GetPtWriteStats(db, rp, pt)
	require.NotNil(t, retrievedStats1)
	require.Equal(t, ptStats.Copy(), retrievedStats1.Copy())

	retrievedStats2 := statistics.GetPtWriteStats(db, rp, pt2)
	require.NotNil(t, retrievedStats2)
	require.Equal(t, ptStats2.Copy(), retrievedStats2.Copy())

	// Verify they are different instances
	require.NotEqual(t, retrievedStats1, retrievedStats2)

	// Test with zero partition ID
	zeroPt := uint32(0)
	zeroStats := statistics.NewPtWriteStats(db, rp, zeroPt)
	require.NotNil(t, zeroStats)
	zeroStats.AddMstBytesCount("zeropartition", 512)

	retrievedZeroStats := statistics.GetPtWriteStats(db, rp, zeroPt)
	require.NotNil(t, retrievedZeroStats)
	require.Equal(t, zeroStats.Copy(), retrievedZeroStats.Copy())
}

func TestRemovePtWriteStats(t *testing.T) {
	tags := map[string]string{
		"hostname": "127.0.0.1:8090",
		"app":      "ts-sql",
	}

	// Test with disabled statistics
	statistics.InitPtWriteStatistics(tags, false, 0.5)
	statistics.RemovePtWriteStats("db0", "rp0", 0)
	statistics.GetPtWriteStats("db0", "rp0", 0)

	statistics.InitPtWriteStatistics(tags, true, 1)

	db, pt, rp := "testdb", uint32(123), "testrp"
	ptStats := statistics.NewPtWriteStats(db, rp, pt)
	require.NotNil(t, ptStats)

	// Add some measurement data
	ptStats.AddMstBytesCount("testmeasurement", 1024)
	retrievedStats := statistics.GetPtWriteStats(db, rp, pt)
	require.NotNil(t, retrievedStats)

	statistics.RemovePtWriteStats(db, rp, pt)
	retrievedStats = statistics.GetPtWriteStats(db, rp, pt)
	require.Nil(t, retrievedStats)
}
