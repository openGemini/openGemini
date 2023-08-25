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

func Test_MetaDBPTTaskInit_disable(t *testing.T) {
	MetaTaskInstance = NewMetaTaskDuration(false)
	defer func() {
		MetaTaskInstance = nil
	}()
	MetaDBPTTaskInit(1, "db0", 2)
	require.Equal(t, 0, len(MetaTaskInstance.dbptTasks))
}

func Test_MetaDBPTTaskInit_enable(t *testing.T) {
	MetaTaskInstance = NewMetaTaskDuration(true)
	defer func() {
		MetaTaskInstance = nil
	}()
	// init task
	MetaDBPTTaskInit(1, "db0", 2)
	require.Equal(t, 1, MetaTaskInstance.dbptTasks[1].tries)
	require.Equal(t, "db0", MetaTaskInstance.dbptTasks[1].db)
	require.Equal(t, 2, MetaTaskInstance.dbptTasks[1].pt)
	require.Equal(t, DBPTLoading, MetaTaskInstance.dbptTasks[1].status)

	// imitate retry the same task
	MetaDBPTTaskInit(1, "db0", 2)
	require.Equal(t, 2, MetaTaskInstance.dbptTasks[1].tries)
	require.Equal(t, "db0", MetaTaskInstance.dbptTasks[1].db)
	require.Equal(t, 2, MetaTaskInstance.dbptTasks[1].pt)
	require.Equal(t, DBPTLoading, MetaTaskInstance.dbptTasks[1].status)
}

func Test_MetaDBPTStepDuration_disable(t *testing.T) {
	MetaTaskInstance = NewMetaTaskDuration(false)
	defer func() {
		MetaTaskInstance = nil
	}()
	MetaDBPTStepDuration("test", 1, "", 0, 1, 0, DBPTLoading, "")
	require.Equal(t, 0, len(MetaTaskInstance.dbptTasks))
}

func Test_MetaDBPTStepDuration_enable(t *testing.T) {
	MetaTaskInstance = NewMetaTaskDuration(true)
	defer func() {
		MetaTaskInstance = nil
	}()

	MetaDBPTStepDuration("test", 9999, "Step0", 0, 1, 1000, DBPTLoading, "")
	require.Nil(t, MetaTaskInstance.dbptTasks[9999])

	// task running
	MetaDBPTTaskInit(1, "db0", 2)

	MetaDBPTStepDuration("test", 1, "Step1", 0, 1, 1000, DBPTLoading, "")
	require.Equal(t, 1, MetaTaskInstance.dbptTasks[1].tries)
	require.Equal(t, "Step1", MetaTaskInstance.dbptTasks[1].step)
	require.Equal(t, int64(1000), MetaTaskInstance.dbptTasks[1].cost)
	require.Equal(t, int64(1000), MetaTaskInstance.dbptTasks[1].totalCost)

	MetaDBPTStepDuration("test", 1, "Step2", 0, 1, 2000, DBPTLoading, "")
	MetaDBPTStepDuration("test", 1, "Step3", 0, 1, 3000, DBPTLoaded, "")
	require.Equal(t, 0, len(MetaTaskInstance.dbptTasks))

	point, _ := MetaTaskInstance.Collect(nil)

	require.Equal(t, 3, bytes.Count(point, []byte("meta_dbpt_tasks")))
	require.Equal(t, 408, len(point))
}

func Test_MetaDBPTStepDuration_enable_loadErr(t *testing.T) {
	MetaTaskInstance = NewMetaTaskDuration(true)
	defer func() {
		MetaTaskInstance = nil
	}()

	// task running
	MetaDBPTTaskInit(1, "db0", 2)
	MetaDBPTStepDuration("test", 1, "Step1", 0, 1, 1000, DBPTLoading, "")
	MetaDBPTStepDuration("test", 1, "Step2", 0, 1, 2000, DBPTLoadErr, "LoadError")
	require.Equal(t, 0, len(MetaTaskInstance.dbptTasks))

	MetaTaskInstance.pointMu.Lock()
	point := MetaTaskInstance.loadPoints
	MetaTaskInstance.pointMu.Unlock()

	require.Equal(t, 2, bytes.Count(point, []byte("meta_dbpt_tasks")))
	require.Equal(t, 281, len(point))
	require.Containsf(t, string(point), "LoadError", "MetaDBPTStepDuration error")
}

func Test_MetaDBPTStepDuration_enable_retries(t *testing.T) {
	MetaTaskInstance = NewMetaTaskDuration(true)
	defer func() {
		MetaTaskInstance = nil
	}()

	// task running
	MetaDBPTTaskInit(1, "db0", 2)
	MetaDBPTStepDuration("test", 1, "Step1", 0, 1, 1000, DBPTLoading, "")
	MetaDBPTTaskInit(1, "db0", 2)
	MetaDBPTStepDuration("test", 1, "Step2", 0, 1, 2000, DBPTLoading, "")
	MetaDBPTStepDuration("test", 1, "Step1", 0, 1, 2000, DBPTLoading, "")
	MetaDBPTStepDuration("test", 1, "Step3", 0, 1, 3000, DBPTLoaded, "")
	MetaDBPTStepDuration("test", 1, "Step2", 0, 1, 2000, DBPTLoading, "")
	MetaDBPTStepDuration("test", 1, "Step3", 0, 1, 2000, DBPTLoaded, "")
	require.Equal(t, 0, len(MetaTaskInstance.dbptTasks))

	MetaTaskInstance.pointMu.Lock()
	point := MetaTaskInstance.loadPoints
	MetaTaskInstance.pointMu.Unlock()

	require.Equal(t, 6, bytes.Count(point, []byte("meta_dbpt_tasks")))
	require.Equal(t, 818, len(point))
}
