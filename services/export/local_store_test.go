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

package export_test

import (
	"testing"

	"github.com/openGemini/openGemini/services/export"
	"github.com/stretchr/testify/require"
)

func TestLocalStore(t *testing.T) {
	store := export.NewLocalStore()

	task1 := &export.OriginalExportTask{ID: 1, Name: "task1"}
	store.SaveOriginalExportTask(task1)
	retrievedTask1, ok := store.GetOriginalExportTask(1)
	require.True(t, ok)
	require.Equal(t, task1, retrievedTask1)

	store.DeleteOriginalExportTask(1)
	_, ok = store.GetOriginalExportTask(1)
	require.False(t, ok)

	subTask1 := &export.SubExportTask{ID: "sub1", ParentID: 1, ParentName: "task1", Db: "test_db", Rp: "test_rp", Mst: "test_mst"}
	store.SaveOriginalExportTask(task1)
	store.SaveSubExportTask(subTask1)
	subTasks := store.ListSubExportTasksByTaskID(1)
	require.Len(t, subTasks, 1)
	require.Equal(t, subTask1, subTasks[0])

	store.DeleteSubExportTask(subTask1)
	subTasks = store.ListSubExportTasksByTaskID(1)
	require.Len(t, subTasks, 0)

	subTask2 := &export.SubExportTask{ID: "sub2", ParentID: 1, ParentName: "task1", Db: "test_db", Rp: "test_rp", Mst: "test_mst"}
	store.SaveSubExportTask(subTask2)
	store.DeleteSubExportTasks(task1)
	subTasks = store.ListSubExportTasksByTaskID(1)
	require.Len(t, subTasks, 0)

	task2 := &export.OriginalExportTask{ID: 2, Name: "task2"}
	store.SaveOriginalExportTask(task2)
	tasks := store.ListOriginalExportTasks()
	require.Len(t, tasks, 2)
	require.Contains(t, tasks, task1)
	require.Contains(t, tasks, task2)
}
