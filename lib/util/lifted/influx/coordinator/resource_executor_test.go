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
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

// TestExecuteCreateResourceStmt tests the creation of a resource
func TestExecuteCreateResourceStmt(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *influxql.CreateResourceStmt
		setup   func(*metaclient.Client) []*gomonkey.Patches
		wantErr bool
	}{
		{
			name: "create valid resource",
			stmt: &influxql.CreateResourceStmt{
				Name: "test-resource",
				Properties: map[string]string{
					"type":              "llm",
					"llm.provider_type": "openai",
					"llm.endpoint":      "https://api.openai.com",
					"llm.model_name":    "gpt-4",
					"llm.api_key":       "test-key",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches := gomonkey.ApplyMethod(mc, "CreateResource",
					func(_ *metaclient.Client, _ string, _ map[string]string) error {
						return nil
					})
				return []*gomonkey.Patches{patches}
			},
			wantErr: false,
		},
		{
			name: "create duplicate resource",
			stmt: &influxql.CreateResourceStmt{
				Name: "test-resource",
				Properties: map[string]string{
					"type":              "llm",
					"llm.provider_type": "openai",
					"llm.endpoint":      "https://api.openai.com",
					"llm.model_name":    "gpt-4",
					"llm.api_key":       "test-key",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches := gomonkey.ApplyMethod(mc, "CreateResource",
					func(_ *metaclient.Client, name string, _ map[string]string) error {
						return errno.NewError(errno.ResourceExists, name)
					})
				return []*gomonkey.Patches{patches}
			},
			wantErr: true,
		},
		{
			name: "create resource with missing required field",
			stmt: &influxql.CreateResourceStmt{
				Name: "invalid-resource",
				Properties: map[string]string{
					"llm.provider_type": "openai",
					"llm.endpoint":      "https://api.openai.com",
				},
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
			err := executor.ExecuteCreateResourceStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteCreateResourceStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestExecuteShowResourceStmt tests the retrieval of a specific resource
func TestExecuteShowResourceStmt(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *influxql.ShowResourceStmt
		setup    func(*metaclient.Client) *gomonkey.Patches
		wantErr  bool
		wantRows models.Rows
	}{
		{
			name: "show existing resource",
			stmt: &influxql.ShowResourceStmt{Name: "test-resource"},
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetResource",
					func(client *metaclient.Client, resourceName string) (map[string]string, bool) {
						return map[string]string{
							"llm.provider_type": "openai",
							"llm.endpoint":      "https://api.openai.com",
						}, true
					})
			},
			wantErr: false,
			wantRows: models.Rows{
				&models.Row{
					Name:    "test-resource",
					Columns: []string{"Property", "Value", "Required", "Sensitive", "Default", "Description"},
					Values:  [][]interface{}{},
				},
			},
		},
		{
			name: "show non-existent resource",
			stmt: &influxql.ShowResourceStmt{Name: "non-existent"},
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetResource",
					func(client *metaclient.Client, name string) (map[string]string, bool) {
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

			rows, err := executor.ExecuteShowResourceStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteShowResourceStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(rows) != len(tt.wantRows) {
				t.Errorf("ExecuteShowResourceStmt() = %v, want %v", rows, tt.wantRows)
			}
		})
	}
}

// TestExecuteShowResourcesStmt tests the retrieval of all resources
func TestExecuteShowResourcesStmt(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*metaclient.Client) *gomonkey.Patches
		wantErr  bool
		wantRows models.Rows
	}{
		{
			name: "show multiple resources",
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetResources",
					func(_ *metaclient.Client) []string {
						return []string{"resource1", "resource2"}
					})
			},
			wantErr: false,
			wantRows: models.Rows{
				&models.Row{
					Name:    "resources",
					Columns: []string{"name"},
					Values: [][]interface{}{
						{"resource1"},
						{"resource2"},
					},
				},
			},
		},
		{
			name: "show no resources",
			setup: func(mc *metaclient.Client) *gomonkey.Patches {
				return gomonkey.ApplyMethod(mc, "GetResources",
					func(_ *metaclient.Client) []string {
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
			rows, err := executor.ExecuteShowResourcesStmt(&influxql.ShowResourcesStmt{})
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteShowResourcesStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(rows, tt.wantRows) {
				t.Errorf("ExecuteShowResourcesStmt() = %v, want %v", rows, tt.wantRows)
			}
		})
	}
}

// TestExecuteAlterResourceStmt tests the alteration of a resource
func TestExecuteAlterResourceStmt(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *influxql.AlterResourceStmt
		setup   func(*metaclient.Client) []*gomonkey.Patches
		wantErr bool
	}{
		{
			name: "alter existing resource",
			stmt: &influxql.AlterResourceStmt{
				Name: "test-resource",
				Properties: map[string]string{
					"llm.endpoint": "https://new-endpoint.com",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches1 := gomonkey.ApplyMethod(mc, "GetResource",
					func(_ *metaclient.Client, resourceName string) (map[string]string, bool) {
						return map[string]string{
							"type":              "llm",
							"llm.provider_type": "openai",
							"llm.endpoint":      "https://api.openai.com",
							"llm.model_name":    "gpt-4",
							"llm.api_key":       "test-key"}, true
					})
				patches2 := gomonkey.ApplyMethod(mc, "AlterResource",
					func(_ *metaclient.Client, resourceName string, properties map[string]string) error {
						return nil
					})
				return []*gomonkey.Patches{patches1, patches2}
			},
			wantErr: false,
		},
		{
			name: "alter non-existent resource",
			stmt: &influxql.AlterResourceStmt{
				Name: "non-existent",
				Properties: map[string]string{
					"llm.endpoint": "https://new-endpoint.com",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches1 := gomonkey.ApplyMethod(mc, "GetResource",
					func(_ *metaclient.Client, resourceName string) (map[string]string, bool) {
						return nil, false
					})
				return []*gomonkey.Patches{patches1}
			},
			wantErr: true,
		},
		{
			name: "alter resource with invalid properties",
			stmt: &influxql.AlterResourceStmt{
				Name: "test-resource",
				Properties: map[string]string{
					"invalid.field": "test",
				},
			},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches := gomonkey.ApplyMethod(mc, "GetResource",
					func(_ *metaclient.Client, resourceName string) (map[string]string, bool) {
						return map[string]string{
							"type": "llm"}, true
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
			err := executor.ExecuteAlterResourceStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteAlterResourceStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestExecuteDropResourceStmt tests the deletion of a resource
func TestExecuteDropResourceStmt(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *influxql.DropResourceStmt
		setup   func(*metaclient.Client) []*gomonkey.Patches
		wantErr bool
	}{
		{
			name: "drop existing resource",
			stmt: &influxql.DropResourceStmt{Name: "test-resource"},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches1 := gomonkey.ApplyMethod(mc, "GetResource",
					func(_ *metaclient.Client, resourceName string) (map[string]string, bool) {
						return map[string]string{
							"type": "llm"}, true
					})
				patches2 := gomonkey.ApplyMethod(mc, "DropResource",
					func(_ *metaclient.Client, resourceName string) error {
						return nil
					})
				return []*gomonkey.Patches{patches1, patches2}
			},
			wantErr: false,
		},
		{
			name: "drop non-existent resource",
			stmt: &influxql.DropResourceStmt{Name: "non-existent"},
			setup: func(mc *metaclient.Client) []*gomonkey.Patches {
				patches1 := gomonkey.ApplyMethod(mc, "GetResource",
					func(_ *metaclient.Client, resourceName string) (map[string]string, bool) {
						return nil, false
					})
				return []*gomonkey.Patches{patches1}
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
			err := executor.ExecuteDropResourceStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteDropResourceStmt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
