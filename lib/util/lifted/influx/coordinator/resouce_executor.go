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
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

// ExecuteCreateResourceStmt runs the CREATE RESOURCE logic
func (e *StatementExecutor) ExecuteCreateResourceStmt(stmt *influxql.CreateResourceStmt) error {
	// 1. Validate input properties and fill defaults
	validatedProps, err := influxql.ValidateAndFillProperties(stmt.Properties)
	if err != nil {
		return errno.NewError(errno.InValidResource, stmt.Name, err)
	}

	// 2. Encrypt sensitive fields before storage
	encryptedProps, err := influxql.EncryptSensitiveFields(validatedProps)
	if err != nil {
		return err
	}

	// 3. Create resource with source name and properties
	return e.MetaClient.CreateResource(stmt.Name, encryptedProps)
}

// ExecuteShowResourceStmt runs the SHOW RESOURCE logic
func (e *StatementExecutor) ExecuteShowResourceStmt(stmt *influxql.ShowResourceStmt) (models.Rows, error) {
	// 1. Fetch raw (encrypted) properties from metadata store
	props, ok := e.MetaClient.GetResource(stmt.Name)
	if !ok {
		return nil, errno.NewError(errno.ResourceNotFound, stmt.Name)
	}

	// 2. Decrypt sensitive fields (or mask) based on permission
	displayProps, err := influxql.DecryptSensitiveFields(props, false)
	if err != nil {
		return nil, err
	}

	// 3. Format results with metadata from LLMFieldSpecs
	var res models.Rows
	fields := []string{"Property", "Value", "Required", "Sensitive", "Default", "Description"}
	row := &models.Row{
		Name:    stmt.Name,
		Columns: fields,
		Values:  make([][]interface{}, len(influxql.LLMFieldSpecs)),
	}

	fieldSpecs := make([]string, 0, len(influxql.LLMFieldSpecs))
	for k := range influxql.LLMFieldSpecs {
		fieldSpecs = append(fieldSpecs, k)
	}
	sort.Strings(fieldSpecs)
	for i, k := range fieldSpecs {
		spec := influxql.LLMFieldSpecs[k]
		row.Values[i] = []interface{}{
			k,
			displayProps[spec.Key],
			spec.Required,
			spec.Sensitive,
			spec.Default,
			spec.Description,
		}
	}
	res = append(res, row)
	return res, nil
}

// ExecuteShowResourcesStmt runs the SHOW RESOURCES logic
func (e *StatementExecutor) ExecuteShowResourcesStmt(_ *influxql.ShowResourcesStmt) (models.Rows, error) {
	var res models.Rows
	names := e.MetaClient.GetResources()
	if len(names) == 0 {
		return nil, nil
	}
	fields := []string{"name"}
	row := &models.Row{
		Name:    "resources",
		Columns: fields,
		Values:  make([][]interface{}, len(names)),
	}
	for i, k := range names {
		row.Values[i] = []interface{}{k}
	}
	res = append(res, row)
	return res, nil
}

// ExecuteAlterResourceStmt runs the ALTER RESOURCE logic
func (e *StatementExecutor) ExecuteAlterResourceStmt(stmt *influxql.AlterResourceStmt) error {
	// 1. Check if resource exists
	oldProps, ok := e.MetaClient.GetResource(stmt.Name)
	if !ok {
		return errno.NewError(errno.ResourceNotFound, stmt.Name)
	}

	// 2. Encrypt sensitive fields before merge
	oldProps, err := influxql.DecryptSensitiveFields(oldProps, true)
	if err != nil {
		return err
	}

	// 3. Merge new properties with existing ones (new values override old)
	mergedProps := make(map[string]string)
	for k, v := range oldProps {
		mergedProps[k] = v
	}
	for k, v := range stmt.Properties {
		mergedProps[k] = v // Override with new values
	}

	// 4. Validate merged properties (ensure required fields exist and all are valid)
	validatedProps, err := influxql.ValidateAndFillProperties(mergedProps)
	if err != nil {
		return errno.NewError(errno.InValidResource, stmt.Name, err)
	}

	// 5. Encrypt sensitive fields in the validated properties
	encryptedProps, err := influxql.EncryptSensitiveFields(validatedProps)
	if err != nil {
		return err
	}

	// 6. Execute update
	return e.MetaClient.AlterResource(stmt.Name, encryptedProps)
}

// ExecuteDropResourceStmt runs the DROP RESOURCE logic
func (e *StatementExecutor) ExecuteDropResourceStmt(stmt *influxql.DropResourceStmt) error {
	// 1. Check if resource exists
	_, ok := e.MetaClient.GetResource(stmt.Name)
	if !ok {
		return errno.NewError(errno.ResourceNotFound, stmt.Name)
	}

	// 2. Delete the resource
	return e.MetaClient.DropResource(stmt.Name)
}
