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

package meta

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
)

func TestMasterRPPTBalance(t *testing.T) {
	t.Run("manager disabled", func(t *testing.T) {
		bm := NewRpPtBalanceManager(time.Hour, false)
		globalService = &Service{store: &Store{data: &meta.Data{}}}
		bm.Start()
	})

	t.Run("manager enabled", func(t *testing.T) {
		bm := NewRpPtBalanceManager(time.Second, true)
		globalService = &Service{store: &Store{
			data:      &meta.Data{},
			cacheData: &meta.Data{},
		}}
		bm.Start()
		time.Sleep(2 * time.Second)
		bm.Stop()
	})

}

func TestRpPtBalanceManager_balanceRPPTWithLoads(t *testing.T) {
	conf := config.NewMeta()
	service := NewService(conf, nil, nil)
	store := NewStore(conf, "", "", "")
	service.store = store
	t.Run("successful balance", func(t *testing.T) {
		// Setup
		bm := NewRpPtBalanceManager(time.Hour, true)
		// Call the method
		bm.balanceRPPTWithLoads()

		// Verify results
		bm.mu.RLock()
		defer bm.mu.RUnlock()

		assert.Equal(t, 0, len(bm.shardingPlans))
	})

	t.Run("balance with error", func(t *testing.T) {
		// Setup
		bm := NewRpPtBalanceManager(time.Hour, true)
		store.cacheData.Databases = map[string]*meta.DatabaseInfo{
			"testdb": {
				Name: "testdb",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"testrp": {
						Name:             "testrp",
						AdaptiveSharding: true,
						Measurements: map[string]*meta.MeasurementInfo{
							"measurement1": {
								Name:            "measurement1",
								ShardIdexes:     map[uint64][]int{1: {0, 1, 2, 3}},
								InitNumOfShards: 4,
							},
							"measurement2": {
								Name:            "measurement2",
								ShardIdexes:     map[uint64][]int{1: {0, 1, 2, 3}},
								InitNumOfShards: 4,
							},
						},
					},
				},
			},
		}
		// Call the method
		bm.balanceRPPTWithLoads()

		// Verify that shardingPlans is nil when there's an error
		bm.mu.RLock()
		defer bm.mu.RUnlock()
		assert.Equal(t, 0, len(bm.shardingPlans))
	})
}

func TestRpPtBalanceManager_GetShardingPlans(t *testing.T) {
	t.Run("enabled manager with existing plans", func(t *testing.T) {
		// Setup
		bm := NewRpPtBalanceManager(time.Hour, true)

		// Set up sharding plans
		bm.mu.Lock()
		bm.shardingPlans = DBShardingPlans{
			"testdb": RPShardingPlans{
				"testrp": ShardingPlans{
					"testmst": []int{0, 1, 2},
				},
			},
		}
		bm.mu.Unlock()

		// Call the method
		result, err := bm.GetShardingPlans("testdb", "testrp")

		// Verify results
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(result) == 0 {
			t.Error("Expected non-empty result")
		}
	})

	t.Run("enabled manager with non-existing db", func(t *testing.T) {
		// Setup
		bm := NewRpPtBalanceManager(time.Hour, true)

		// Set up sharding plans
		bm.mu.Lock()
		bm.shardingPlans = DBShardingPlans{
			"otherdb": RPShardingPlans{
				"testrp": ShardingPlans{
					"testmst": []int{0, 1, 2},
				},
			},
		}
		bm.mu.Unlock()

		// Call the method
		result, err := bm.GetShardingPlans("testdb", "testrp")

		// Verify results
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result != nil {
			t.Error("Expected nil result for non-existing database")
		}
	})

	t.Run("enabled manager with non-existing rp", func(t *testing.T) {
		// Setup
		bm := NewRpPtBalanceManager(time.Hour, true)

		// Set up sharding plans
		bm.mu.Lock()
		bm.shardingPlans = DBShardingPlans{
			"testdb": RPShardingPlans{
				"otherrp": ShardingPlans{
					"testmst": []int{0, 1, 2},
				},
			},
		}
		bm.mu.Unlock()

		// Call the method
		result, err := bm.GetShardingPlans("testdb", "testrp")

		// Verify results
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result != nil {
			t.Error("Expected nil result for non-existing retention policy")
		}
	})

	t.Run("disabled manager", func(t *testing.T) {
		// Setup disabled manager
		bm := NewRpPtBalanceManager(time.Hour, false)

		// Set up sharding plans
		bm.mu.Lock()
		bm.shardingPlans = DBShardingPlans{
			"testdb": RPShardingPlans{
				"testrp": ShardingPlans{
					"testmst": []int{0, 1, 2},
				},
			},
		}
		bm.mu.Unlock()

		// Call the method
		result, err := bm.GetShardingPlans("testdb", "testrp")

		// Verify results
		if err == nil {
			t.Error("Expected error for disabled manager")
		}

		expectedErrMsg := "RpPtBalanceManager disabled"
		if err.Error() != expectedErrMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
		}

		if result != nil {
			t.Error("Expected nil result for disabled manager")
		}
	})
}
