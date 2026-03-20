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
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	PropertyField = "Property"
	ValueField    = "Value"
	NameField     = "Name"
	Tasks         = "Tasks"
	TaskTypeKey   = "type"
)

// ExecuteCreateTaskStmt runs the CREATE TASK logic
func (e *StatementExecutor) ExecuteCreateTaskStmt(stmt *influxql.CreateTaskStmt) error {
	taskType, err := parseTaskTypeFromProperties(stmt.Properties)
	if err != nil {
		return err
	}
	maxTotalTask := config.GetCoordinatorConfig().TaskNumLimit
	currentTaskNum := e.MetaClient.GetAllTaskCount()
	if currentTaskNum >= maxTotalTask {
		return errno.NewError(errno.TaskLimitExceeded, currentTaskNum, maxTotalTask)
	}
	_, ok := e.MetaClient.GetTask(stmt.Name, taskType)
	if ok {
		return errno.NewError(errno.TaskExists, stmt.Name, taskType)
	}
	switch taskType {
	case influxql.TaskExport:
		return e.handleCreateExportTask(stmt, taskType)
	default:
		return errno.NewError(errno.InvalidTaskType, taskType)
	}
}

// ExecuteShowTaskStmt runs the SHOW TASK logic
func (e *StatementExecutor) ExecuteShowTaskStmt(stmt *influxql.ShowTaskStmt) (models.Rows, error) {
	taskType, err := parseTaskTypeFromProperties(stmt.Properties)
	if err != nil {
		return nil, err
	}
	// 1. Fetch properties from metadata store
	props, ok := e.MetaClient.GetTask(stmt.Name, taskType)
	if !ok {
		return nil, errno.NewError(errno.TaskNotFound, stmt.Name, taskType)
	}

	// 2. Format results with properties that stored
	var res models.Rows
	fields := []string{PropertyField, ValueField}
	propLength := len(props)
	row := &models.Row{
		Name:    stmt.Name,
		Columns: fields,
		Values:  make([][]interface{}, propLength),
	}

	fieldSpecs := make([]string, 0, propLength)
	for k := range props {
		fieldSpecs = append(fieldSpecs, k)
	}
	sort.Strings(fieldSpecs)
	for i, k := range fieldSpecs {
		row.Values[i] = []interface{}{
			k,
			props[k],
		}
	}

	res = append(res, row)
	return res, nil
}

// ExecuteShowTasksStmt runs the SHOW TASKS logic
func (e *StatementExecutor) ExecuteShowTasksStmt(stmt *influxql.ShowTasksStmt) (models.Rows, error) {
	taskType, err := parseTaskTypeFromProperties(stmt.Properties)
	if err != nil {
		return nil, err
	}
	// 1. Fetch all tasks of the specified type from metadata store
	var res models.Rows
	names := e.MetaClient.GetTasks(taskType)
	if len(names) == 0 {
		return nil, nil
	}

	// 2. Format results
	fields := []string{NameField}
	row := &models.Row{
		Name:    Tasks,
		Columns: fields,
		Values:  make([][]interface{}, len(names)),
	}
	for i, k := range names {
		row.Values[i] = []interface{}{k}
	}
	res = append(res, row)
	return res, nil
}

// ExecuteAlterTaskStmt runs the ALTER TASK logic
func (e *StatementExecutor) ExecuteAlterTaskStmt(stmt *influxql.AlterTaskStmt) error {
	// Switch on the task type to handle different types of task modifications.
	// Currently, the 'export' task type is not supported.
	// This switch statement is designed to allow for future expansion to support other task types.
	taskType, err := parseTaskTypeFromProperties(stmt.Properties)
	if err != nil {
		return err
	}
	switch taskType {
	default:
		// Return an error if the task type is not supported.
		return errno.NewError(errno.InvalidTaskType, taskType)
	}
}

// ExecuteDropTaskStmt runs the DROP TASK logic
func (e *StatementExecutor) ExecuteDropTaskStmt(stmt *influxql.DropTaskStmt) error {
	taskType, err := parseTaskTypeFromProperties(stmt.Properties)
	if err != nil {
		return err
	}
	// 1. Check if task exists
	_, ok := e.MetaClient.GetTask(stmt.Name, taskType)
	if !ok {
		return errno.NewError(errno.TaskNotFound, stmt.Name, taskType)
	}
	// 2. Delete the task
	return e.MetaClient.DropTask(stmt.Name, taskType)
}

func (e *StatementExecutor) handleCreateExportTask(stmt *influxql.CreateTaskStmt, taskType influxql.TaskType) error {
	validatedProps, err := influxql.ValidateAndFillPropertiesByType(taskType, stmt.Properties)
	if err != nil {
		return errno.NewError(errno.InvalidTaskProperties, stmt.Name, taskType, err)
	}
	return e.MetaClient.CreateTask(stmt.Name, taskType, validatedProps)
}

func parseTaskTypeFromProperties(properties map[string]string) (influxql.TaskType, error) {
	taskTypeStr, ok := properties[TaskTypeKey]
	if !ok {
		return "", errno.NewError(errno.MissingTaskType)
	}
	taskType, err := influxql.ParseTaskType(taskTypeStr)
	if err != nil {
		return "", err
	}
	return taskType, nil
}
