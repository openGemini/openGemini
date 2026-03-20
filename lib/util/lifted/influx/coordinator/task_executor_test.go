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

package coordinator

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

// TestExecuteCreateTaskStmt tests the creation of a task
func TestExecuteCreateTaskStmt(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *influxql.CreateTaskStmt
		setup   func(*metaclient.Client) []*gomonkey.Patches
		wantErr bool
	}{
		{
			name: "create valid task",
			stmt: &influxql.CreateTaskStmt{
				Name: "test-task",
				Properties: map[string]string{
					"type":   "export",
					"db":     "db-test",
					"rp":     "rp-test",
					"format": "parquet",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				var patches []*gomonkey.Patches
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return nil, false
					}))
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetAllTaskCount",
					func(_ *metaclient.Client) int {
						return 0
					}))
				patches = append(patches, gomonkey.ApplyMethod(mc, "CreateTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType, _ map[string]string) error {
						return nil
					}))
				return patches
			},
			wantErr: false,
		},
		{
			name: "create task when task limit is exceeded",
			stmt: &influxql.CreateTaskStmt{
				Name: "test-task",
				Properties: map[string]string{
					"type":   "export",
					"db":     "db-test",
					"rp":     "rp-test",
					"format": "parquet",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				var patches []*gomonkey.Patches
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetAllTaskCount",
					func(_ *metaclient.Client) int {
						return 100
					}))
				return patches
			},
			wantErr: true,
		},
		{
			name: "create duplicate task",
			stmt: &influxql.CreateTaskStmt{
				Name: "test-task",
				Properties: map[string]string{
					"type":   "export",
					"format": "parquet",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				var patches []*gomonkey.Patches
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetAllTaskCount",
					func(_ *metaclient.Client) int {
						return 0
					}))
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return nil, true
					}))
				return patches
			},
			wantErr: true,
		},
		{
			name: "create task with missing required field",
			stmt: &influxql.CreateTaskStmt{
				Name: "invalid-task",
				Properties: map[string]string{
					"type": "export",
					"db":   "db01",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				var patches []*gomonkey.Patches
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetAllTaskCount",
					func(_ *metaclient.Client) int {
						return 0
					}))
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return nil, false
					}))
				return patches
			},
			wantErr: true,
		},
		{
			name: "create task with invalid time zone",
			stmt: &influxql.CreateTaskStmt{
				Name: "invalid-task",
				Properties: map[string]string{
					"db":        "db01",
					"format":    "parquet",
					"time_zone": "Invalid/Shanghai",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				var patches []*gomonkey.Patches
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetAllTaskCount",
					func(_ *metaclient.Client) int {
						return 0
					}))
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return nil, false
					}))
				return patches
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &metaclient.Client{}
			if tt.setup != nil {
				patches := tt.setup(client)
				for i := range patches {
					defer patches[i].Reset()
				}
			}
			executor := &StatementExecutor{MetaClient: client}

			// Execute create statement
			err := executor.ExecuteCreateTaskStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteCreateTaskStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestExecuteShowTaskStmt tests the retrieval of a specific task
func TestExecuteShowTaskStmt(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *influxql.ShowTaskStmt
		setup    func(*metaclient.Client) *gomonkey.Patches
		wantErr  bool
		wantRows models.Rows
	}{
		{
			name: "show existing task",
			stmt: &influxql.ShowTaskStmt{
				Name: "test-task",
				Properties: map[string]string{
					"type": "export",
				},
			},
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return map[string]string{
							"type":   "export",
							"format": "parquet",
						}, true
					})
			},
			wantErr: false,
			wantRows: models.Rows{
				&models.Row{
					Name:    "test-task",
					Columns: []string{"Property", "Value"},
					Values:  [][]interface{}{},
				},
			},
		},
		{
			name: "show non-existent task",
			stmt: &influxql.ShowTaskStmt{
				Name: "non-existent",
				Properties: map[string]string{
					"type": "export",
				},
			},
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return nil, false
					})
			},
			wantErr:  true,
			wantRows: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &metaclient.Client{}
			if tt.setup != nil {
				patch := tt.setup(client)
				defer patch.Reset()
			}
			executor := &StatementExecutor{MetaClient: client}

			rows, err := executor.ExecuteShowTaskStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteShowTaskStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(rows) != len(tt.wantRows) {
				t.Errorf("ExecuteShowTaskStmt() = %v, want %v", rows, tt.wantRows)
			}
		})
	}
}

// TestExecuteShowTasksStmt tests the retrieval of all tasks
func TestExecuteShowTasksStmt(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*metaclient.Client) *gomonkey.Patches
		wantErr  bool
		wantRows models.Rows
	}{
		{
			name: "show multiple tasks",
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetTasks",
					func(_ *metaclient.Client, _ influxql.TaskType) []string {
						return []string{"task1", "task2"}
					})
			},
			wantErr: false,
			wantRows: models.Rows{
				&models.Row{
					Name:    "Tasks",
					Columns: []string{"Name"},
					Values:  [][]interface{}{{"task1"}, {"task2"}},
				},
			},
		},
		{
			name: "show no tasks",
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetTasks",
					func(_ *metaclient.Client, _ influxql.TaskType) []string {
						return []string{}
					})
			},
			wantErr:  false,
			wantRows: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &metaclient.Client{}
			if tt.setup != nil {
				patch := tt.setup(mc)
				defer patch.Reset()
			}
			executor := &StatementExecutor{MetaClient: mc}
			rows, err := executor.ExecuteShowTasksStmt(&influxql.ShowTasksStmt{
				Properties: map[string]string{
					"type": "export",
				},
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteShowTasksStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(rows, tt.wantRows) {
				t.Errorf("ExecuteShowTasksStmt() = %v, want %v", rows, tt.wantRows)
			}
		})
	}
}

// TestExecuteAlterTaskStmt tests the alteration of a task
func TestExecuteAlterTaskStmt(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *influxql.AlterTaskStmt
		setup   func(*metaclient.Client) []*gomonkey.Patches
		wantErr bool
	}{
		{
			name: "alter export task error",
			stmt: &influxql.AlterTaskStmt{
				Name: "test-task",
				Properties: map[string]string{
					"type":   "export",
					"db":     "db01",
					"format": "parquet",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &metaclient.Client{}
			if tt.setup != nil {
				patches := tt.setup(mc)
				for i := range patches {
					defer patches[i].Reset()
				}
			}

			executor := &StatementExecutor{MetaClient: mc}
			err := executor.ExecuteAlterTaskStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteAlterTaskStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestExecuteDropTaskStmt tests the deletion of a task
func TestExecuteDropTaskStmt(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *influxql.DropTaskStmt
		setup   func(*metaclient.Client) []*gomonkey.Patches
		wantErr bool
	}{
		{
			name: "drop existing task",
			stmt: &influxql.DropTaskStmt{
				Name: "test-task",
				Properties: map[string]string{
					"type": "export",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				var patches []*gomonkey.Patches
				patches = append(patches, gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return map[string]string{
							"db":     "db01",
							"format": "parquet",
						}, true
					}))
				patches = append(patches, gomonkey.ApplyMethod(mc, "DropTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) error {
						return nil
					}))
				return patches
			},
			wantErr: false,
		},
		{
			name: "drop non-existent task",
			stmt: &influxql.DropTaskStmt{Name: "non-existent"},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches := gomonkey.ApplyMethod(mc, "GetTask",
					func(_ *metaclient.Client, _ string, _ influxql.TaskType) (map[string]string, bool) {
						return nil, false
					})
				return []*gomonkey.Patches{patches}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &metaclient.Client{}
			if tt.setup != nil {
				patches := tt.setup(mc)
				for i := range patches {
					defer patches[i].Reset()
				}
			}
			executor := &StatementExecutor{MetaClient: mc}
			err := executor.ExecuteDropTaskStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteDropTaskStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
