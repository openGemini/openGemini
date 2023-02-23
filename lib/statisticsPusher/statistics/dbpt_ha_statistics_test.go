/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package statistics

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_DBPTTaskInit_disable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(false)
	defer func() {
		StoreTaskInstance = nil
	}()
	DBPTTaskInit(1, "db0", 2)
	require.Equal(t, 0, len(StoreTaskInstance.dbptTasks))
}

func Test_DBPTTaskInit_enable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()
	// init task
	DBPTTaskInit(1, "db0", 2)
	require.Equal(t, 1, StoreTaskInstance.dbptTasks[1].tries)
	require.Equal(t, "db0", StoreTaskInstance.dbptTasks[1].db)
	require.Equal(t, 2, StoreTaskInstance.dbptTasks[1].pt)
	require.Equal(t, DBPTLoading, StoreTaskInstance.dbptTasks[1].status)

	// imitate retry the same task
	DBPTTaskInit(1, "db0", 2)
	require.Equal(t, 2, StoreTaskInstance.dbptTasks[1].tries)
	require.Equal(t, "db0", StoreTaskInstance.dbptTasks[1].db)
	require.Equal(t, 2, StoreTaskInstance.dbptTasks[1].pt)
	require.Equal(t, DBPTLoading, StoreTaskInstance.dbptTasks[1].status)
}

func Test_DBPTStepDuration_disable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(false)
	defer func() {
		StoreTaskInstance = nil
	}()
	DBPTStepDuration(1, "", 0, DBPTLoading, "")
	require.Equal(t, 0, len(StoreTaskInstance.dbptTasks))
}

func Test_DBPTStepDuration_enable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()
	DBPTStepDuration(999, "Step1", 1000, DBPTLoading, "")

	// task running
	DBPTTaskInit(1, "db0", 2)

	DBPTStepDuration(1, "Step1", 1000, DBPTLoading, "")
	require.Equal(t, 1, StoreTaskInstance.dbptTasks[1].tries)
	require.Equal(t, "Step1", StoreTaskInstance.dbptTasks[1].step)
	require.Equal(t, int64(1000), StoreTaskInstance.dbptTasks[1].cost)
	require.Equal(t, int64(1000), StoreTaskInstance.dbptTasks[1].totalCost)

	DBPTStepDuration(1, "Step2", 2000, DBPTLoading, "")
	DBPTStepDuration(1, "Step3", 3000, DBPTLoaded, "")
	require.Equal(t, 0, len(StoreTaskInstance.dbptTasks))

	point, _ := StoreTaskInstance.Collect(nil)

	require.Equal(t, 3, bytes.Count(point, []byte("dbpt_tasks")))
	require.Equal(t, 312, len(point))
}

func Test_DBPTStepDuration_enable_loadErr(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()

	// task running
	DBPTTaskInit(1, "db0", 2)
	DBPTStepDuration(1, "Step1", 1000, DBPTLoading, "")
	DBPTStepDuration(1, "Step2", 2000, DBPTLoadErr, "LoadError")
	require.Equal(t, 0, len(StoreTaskInstance.dbptTasks))

	StoreTaskInstance.pointMu.Lock()
	point := StoreTaskInstance.loadPoints
	StoreTaskInstance.pointMu.Unlock()

	require.Equal(t, 2, bytes.Count(point, []byte("dbpt_tasks")))
	require.Equal(t, 217, len(point))
	require.Containsf(t, string(point), "LoadError", "DBPTStepDuration error")
}

func Test_DBPTStepDuration_enable_retries(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()

	// task running
	DBPTTaskInit(1, "db0", 2)
	DBPTStepDuration(1, "Step1", 1000, DBPTLoading, "")
	DBPTTaskInit(1, "db0", 2)
	DBPTStepDuration(1, "Step2", 2000, DBPTLoading, "")
	DBPTStepDuration(1, "Step1", 2000, DBPTLoading, "")
	DBPTStepDuration(1, "Step3", 3000, DBPTLoaded, "")
	DBPTStepDuration(1, "Step2", 2000, DBPTLoading, "")
	DBPTStepDuration(1, "Step3", 2000, DBPTLoaded, "")
	require.Equal(t, 0, len(StoreTaskInstance.dbptTasks))

	StoreTaskInstance.pointMu.Lock()
	point := StoreTaskInstance.loadPoints
	StoreTaskInstance.pointMu.Unlock()

	require.Equal(t, 6, bytes.Count(point, []byte("dbpt_tasks")))
	require.Equal(t, 626, len(point))
}

func Test_IndexTaskInit_disable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(false)
	defer func() {
		StoreTaskInstance = nil
	}()
	IndexTaskInit(3, 1, "db0", 2, "rp0")
	require.Equal(t, 0, len(StoreTaskInstance.indexTasks))

	// non-HA case
	StoreTaskInstance = NewStoreTaskDuration(true)
	IndexTaskInit(3, 0, "db0", 2, "rp0")
	require.Equal(t, 0, len(StoreTaskInstance.indexTasks))
}

func Test_IndexTaskInit_enable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()
	// init task
	IndexTaskInit(3, 1, "db0", 2, "rp0")
	require.Equal(t, 1, StoreTaskInstance.indexTasks[3].tries)
	require.Equal(t, "db0", StoreTaskInstance.indexTasks[3].db)
	require.Equal(t, 2, StoreTaskInstance.indexTasks[3].pt)

	// imitate retry the same task
	IndexTaskInit(3, 1, "db0", 2, "rp0")
	require.Equal(t, 2, StoreTaskInstance.indexTasks[3].tries)
}

func Test_IndexStepDuration_disable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(false)
	defer func() {
		StoreTaskInstance = nil
	}()

	IndexTaskInit(3, 1, "db0", 2, "rp0")
	require.Equal(t, 0, len(StoreTaskInstance.indexTasks))
}

func Test_IndexStepDuration_enable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()
	IndexStepDuration(999, 999, "Step_404", 0, true)

	IndexTaskInit(3, 1, "db0", 2, "rp0")
	IndexStepDuration(3, 0, "Step0", 0, true)
	IndexStepDuration(3, 1, "Step1", 1000, true)
	require.Equal(t, 0, len(StoreTaskInstance.indexTasks))

	StoreTaskInstance.pointMu.Lock()
	point := StoreTaskInstance.loadPoints
	StoreTaskInstance.pointMu.Unlock()

	require.Equal(t, 1, bytes.Count(point, []byte("index_tasks")))
	require.Equal(t, 108, len(point))
}

func Test_IndexStepDuration_enable_retries(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()

	IndexTaskInit(3, 1, "db0", 2, "rp0")
	IndexTaskInit(3, 1, "db0", 2, "rp0")
	IndexStepDuration(3, 0, "Step0", 0, true) // ignore
	IndexStepDuration(3, 1, "Step1", 1000, true)
	IndexStepDuration(3, 1, "Step1", 2000, true)
	require.Equal(t, 0, len(StoreTaskInstance.indexTasks))

	StoreTaskInstance.pointMu.Lock()
	point := StoreTaskInstance.loadPoints
	StoreTaskInstance.pointMu.Unlock()

	require.Equal(t, 2, bytes.Count(point, []byte("index_tasks")))
	require.Equal(t, 216, len(point))
}

func Test_ShardTaskInit_disable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(false)
	defer func() {
		StoreTaskInstance = nil
	}()
	ShardTaskInit(1, "db0", 2, "rp0", 3)
	require.Equal(t, 0, len(StoreTaskInstance.shardTasks))

	// write rows create shard
	StoreTaskInstance = NewStoreTaskDuration(true)
	ShardTaskInit(0, "db0", 2, "rp0", 3)
	require.Equal(t, 0, len(StoreTaskInstance.shardTasks))
}

func Test_ShardTaskInit_enable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()
	// init task
	ShardTaskInit(1, "db0", 2, "rp0", 3)
	require.Equal(t, 1, StoreTaskInstance.shardTasks[3].tries)
	require.Equal(t, "db0", StoreTaskInstance.shardTasks[3].db)
	require.Equal(t, 2, StoreTaskInstance.shardTasks[3].pt)
	require.Equal(t, "rp0", StoreTaskInstance.shardTasks[3].rp)
}

func Test_ShardStepDuration_disable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(false)
	defer func() {
		StoreTaskInstance = nil
	}()
	// init task
	ShardStepDuration(3, 1, "Step1", 1000, false)
	require.Equal(t, 0, len(StoreTaskInstance.shardTasks))
}

func Test_ShardStepDuration_enable(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()
	ShardStepDuration(999, 999, "Step_404", 1000, false)

	// init task
	ShardTaskInit(1, "db0", 2, "rp0", 3)
	ShardStepDuration(3, 1, "Step1", 1000, false)
	ShardStepDuration(3, 1, "Step2", 2000, true)
	require.Equal(t, 0, len(StoreTaskInstance.shardTasks))

	StoreTaskInstance.pointMu.Lock()
	point := StoreTaskInstance.loadPoints
	StoreTaskInstance.pointMu.Unlock()

	require.Equal(t, 2, bytes.Count(point, []byte("shard_tasks")))
	require.Equal(t, 208, len(point))
}

func Test_ShardStepDuration_enable_retries(t *testing.T) {
	StoreTaskInstance = NewStoreTaskDuration(true)
	defer func() {
		StoreTaskInstance = nil
	}()
	// init task
	ShardTaskInit(1, "db0", 2, "rp0", 3)
	ShardStepDuration(3, 1, "Step1", 1000, false)
	ShardTaskInit(1, "db0", 2, "rp0", 3)
	ShardStepDuration(3, 1, "Step1", 1000, false)
	ShardStepDuration(3, 1, "Step2", 2000, true)
	ShardStepDuration(3, 1, "Step2", 2000, true)
	require.Equal(t, 0, len(StoreTaskInstance.shardTasks))

	StoreTaskInstance.pointMu.Lock()
	point := StoreTaskInstance.loadPoints
	StoreTaskInstance.pointMu.Unlock()

	require.Equal(t, 4, bytes.Count(point, []byte("shard_tasks")))
	require.Equal(t, 416, len(point))
}
