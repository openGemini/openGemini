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

package influxql

import (
	"bytes"
	"testing"
)

func TestCreateResourceStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *CreateResourceStmt
		wantDepth int
	}{
		{
			name:      "normal case",
			stmt:      &CreateResourceStmt{Name: "test", Properties: map[string]string{"key1": "value1", "key2": "value2"}},
			wantDepth: 3,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &CreateResourceStmt{Name: "test", Properties: map[string]string{"key1": "value1", "key2": "value2"}}
	stmt.RenderBytes(buf, posmap)

	expected := "CREATE RESOURCE test PROPERTIES (key1=value1, key2=value2)"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestShowResourceStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *ShowResourceStmt
		wantDepth int
	}{
		{
			name:      "normal case",
			stmt:      &ShowResourceStmt{Name: "test"},
			wantDepth: 1,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &ShowResourceStmt{Name: "test"}
	stmt.RenderBytes(buf, posmap)

	expected := "SHOW RESOURCE test"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestShowResourcesStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *ShowResourcesStmt
		wantDepth int
	}{
		{
			name:      "normal case",
			stmt:      &ShowResourcesStmt{},
			wantDepth: 1,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &ShowResourcesStmt{}
	stmt.RenderBytes(buf, posmap)

	expected := "SHOW RESOURCES"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestAlterResourceStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *AlterResourceStmt
		wantDepth int
	}{
		{
			name:      "normal case",
			stmt:      &AlterResourceStmt{Name: "test", Properties: map[string]string{"key1": "value1", "key2": "value2"}},
			wantDepth: 3,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &AlterResourceStmt{Name: "test", Properties: map[string]string{"key1": "value1", "key2": "value2"}}
	stmt.RenderBytes(buf, posmap)

	expected := "ALTER RESOURCE test PROPERTIES (key1=value1, key2=value2)"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestDropResourceStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *DropResourceStmt
		wantDepth int
	}{
		{
			name:      "normal case",
			stmt:      &DropResourceStmt{Name: "test"},
			wantDepth: 1,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &DropResourceStmt{Name: "test"}
	stmt.RenderBytes(buf, posmap)

	expected := "DROP RESOURCE test"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestCreateTaskStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *CreateTaskStmt
		wantDepth int
	}{
		{
			name: "normal case",
			stmt: &CreateTaskStmt{
				Name:       "test",
				Properties: map[string]string{"key1": "value1", "key2": "value2"},
			},
			wantDepth: 3,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &CreateTaskStmt{
		Name:       "test",
		Properties: map[string]string{"key1": "value1", "key2": "value2"},
	}
	stmt.RenderBytes(buf, posmap)

	expected := "CREATE TASK test PROPERTIES (key1=value1, key2=value2)"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestAlterTaskStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *AlterTaskStmt
		wantDepth int
	}{
		{
			name: "normal case",
			stmt: &AlterTaskStmt{
				Name:       "test",
				Properties: map[string]string{"key1": "value1", "key2": "value2"},
			},
			wantDepth: 3,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &AlterTaskStmt{
		Name:       "test",
		Properties: map[string]string{"key1": "value1", "key2": "value2"},
	}
	stmt.RenderBytes(buf, posmap)

	expected := "ALTER TASK test PROPERTIES (key1=value1, key2=value2)"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestShowTaskStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *ShowTaskStmt
		wantDepth int
	}{
		{
			name: "normal case",
			stmt: &ShowTaskStmt{
				Name:       "test",
				Properties: map[string]string{"key1": "value1"},
			},
			wantDepth: 2,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &ShowTaskStmt{
		Name:       "test",
		Properties: map[string]string{"key1": "value1"},
	}
	stmt.RenderBytes(buf, posmap)

	expected := "SHOW TASK test PROPERTIES (key1=value1)"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestShowTasksStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *ShowTasksStmt
		wantDepth int
	}{
		{
			name: "normal case",
			stmt: &ShowTasksStmt{
				Properties: map[string]string{"key1": "value1"},
			},
			wantDepth: 1,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &ShowTasksStmt{
		Properties: map[string]string{"key1": "value1"},
	}
	stmt.RenderBytes(buf, posmap)

	expected := "SHOW TASKS PROPERTIES (key1=value1)"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}

func TestDropTaskStmt(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *DropTaskStmt
		wantDepth int
	}{
		{
			name: "normal case",
			stmt: &DropTaskStmt{
				Name:       "test",
				Properties: map[string]string{"key1": "value1"},
			},
			wantDepth: 2,
		},
		{
			name:      "empty case",
			stmt:      nil,
			wantDepth: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stmt.Depth(); got != tt.wantDepth {
				t.Errorf("Depth() = %v, want %v", got, tt.wantDepth)
			}
			if got := tt.stmt.UpdateDepthForTests(); got != tt.wantDepth {
				t.Errorf("UpdateDepthForTests() = %v, want %v", got, tt.wantDepth)
			}
		})
	}

	// Test String() and RenderBytes()
	buf := &bytes.Buffer{}
	posmap := make(BufPositionsMap)
	stmt := &DropTaskStmt{
		Name:       "test",
		Properties: map[string]string{"key1": "value1"},
	}
	stmt.RenderBytes(buf, posmap)

	expected := "DROP TASK test PROPERTIES (key1=value1)"
	if got := stmt.String(); got != expected {
		t.Errorf("String() = %v, want %v", got, expected)
	}

	if pos, ok := posmap[stmt]; !ok || pos.Begin != 0 || pos.End != len(expected) {
		t.Errorf("Position mapping incorrect")
	}
}
