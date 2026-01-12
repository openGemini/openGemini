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

package export

import (
	"errors"
	"fmt"
	"time"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

const (
	FieldDB        = "db"
	FieldRP        = "rp"
	FieldFormat    = "format"
	FieldSplitUnit = "split_unit"
	FieldDelay     = "delay"
	FieldTimeZone  = "time_zone"
	FieldSource    = "source"
)

// Converter is a task conversion tool responsible for mapping data to the model and updating it.
type Converter struct {
	metaClient metaclient.MetaClient
	logger     *logger.Logger
}

// NewExportTaskConverter creates a new Converter instance with the provided meta client.
func NewExportTaskConverter(metaClient metaclient.MetaClient, logger *logger.Logger) *Converter {
	return &Converter{
		metaClient: metaClient,
		logger:     logger,
	}
}

// Convert creates a new OriginalExportTask from a map[string]string.
func (c *Converter) Convert(data map[string]string) (*OriginalExportTask, error) {
	task := &OriginalExportTask{}
	err := c.convert(task, data)
	if err != nil {
		c.logger.Error("convert creates a new OriginalExportTask from map[string]string failed",
			zap.Error(err))
		return nil, err
	}
	return task, nil
}

// Update updates an existing OriginalExportTask based on a map[string]string.
func (c *Converter) convert(task *OriginalExportTask, data map[string]string) error {
	task.Db = data[FieldDB]
	task.Rp = data[FieldRP]
	if err := c.convertFormat(&task.Format, data[FieldFormat]); err != nil {
		return err
	}
	if err := c.convertDurationField(&task.SplitUnit, data[FieldSplitUnit]); err != nil {
		return err
	}
	if err := c.convertDurationField(&task.Delay, data[FieldDelay]); err != nil {
		return err
	}
	if err := c.convertTimezoneField(&task.Timezone, data[FieldTimeZone]); err != nil {
		return err
	}
	task.Source = data[FieldSource]

	opts, mstOrigin, err := c.CompileQuery(task)
	if err != nil {
		return err
	}
	minTime := time.Unix(0, influxql.MaxTime)
	if opts.StartTime == influxql.MinTime {
		policy, err := c.metaClient.RetentionPolicy(task.Db, task.Rp)
		if err != nil {
			return err
		}
		for _, sg := range policy.ShardGroups {
			if sg.StartTime.Before(minTime) {
				minTime = sg.StartTime
			}
		}
		if !minTime.Equal(time.Unix(0, influxql.MaxTime)) {
			opts.StartTime = minTime.UnixNano()
		}
	}

	task.Mst = mstOrigin
	task.Opts = opts

	return nil
}

// convertFormat validates and updates the data format field.
func (c *Converter) convertFormat(target *DataFormat, value string) error {
	if value == "" {
		return nil
	}

	format := DataFormat(value)
	if _, ok := validFormats[format]; !ok {
		return fmt.Errorf("invalid format: %s", value)
	}
	*target = format
	return nil
}

// convertDurationField updates a duration field (based on actual duration).
func (c *Converter) convertDurationField(current *time.Duration, newStr string) error {
	if newStr == "" {
		return nil
	}
	newDur, err := time.ParseDuration(newStr)
	if err != nil {
		return fmt.Errorf("parse duration failed: %s", err.Error())
	}
	*current = newDur
	return nil
}

// convertTimezoneField updates the timezone field from a string.
func (c *Converter) convertTimezoneField(current **time.Location, newStr string) error {
	if newStr == "" {
		return nil
	}
	newLoc, err := time.LoadLocation(newStr)
	if err != nil {
		return fmt.Errorf("load timezone failed: %s", err.Error())
	}
	*current = newLoc
	return nil
}

// CompileQuery compiles the SQL query and returns the processor options and the measurement name.
func (c *Converter) CompileQuery(task *OriginalExportTask) (*query.ProcessorOptions, string, error) {
	q, err := influxql.ParseQuery(task.Source)
	if err != nil {
		return nil, "", err
	}

	mapper := &coordinator.ClusterShardMapper{
		MetaClient: c.metaClient,
		Logger:     logger.NewLogger(errno.ModuleMetaClient),
	}

	selectStmt, ok := q.Statements[0].(*influxql.SelectStatement)
	if !ok {
		return nil, "", errors.New("invalid select query")
	}
	selectStmt.Location = task.Timezone

	sources := selectStmt.Sources
	if len(sources) != 1 {
		return nil, "",
			fmt.Errorf("query contains invalid number of data sources: got %d, expected exactly 1", len(sources))
	}
	mst, ok := selectStmt.Sources[0].(*influxql.Measurement)
	if !ok {
		return nil, "", errors.New("the first source is not a valid measurement")
	}
	mst.Database = task.Db
	mst.RetentionPolicy = task.Rp

	stmt, err := query.Prepare(selectStmt, mapper, query.SelectOptions{})
	if err != nil {
		return nil, "", err
	}

	mstOrigin, err := c.metaClient.Measurement(mst.Database, mst.RetentionPolicy, mst.Name)
	if err != nil {
		return nil, "", fmt.Errorf("failed to query mst origin info for database=%s,"+
			" retention_policy=%s, name=%s: %w", mst.Database, mst.RetentionPolicy, mst.Name, err)
	}

	opts := stmt.ProcessorOptions()
	if opts == nil {
		return nil, "", fmt.Errorf("failed to compile query: %s", task.Source)
	}

	fields := stmt.Statement().Fields
	for _, field := range fields {
		var ref, ok = field.Expr.(*influxql.VarRef)
		if !ok {
			return nil, "", fmt.Errorf(
				"failed to compile query: field expression is not a valid VarRef (actual type: %T), query source: %s",
				field.Expr,
				task.Source,
			)
		}
		if ref.Type == influxql.Tag {
			opts.TagAux = append(opts.TagAux, *ref)
		} else {
			opts.FieldAux = append(opts.FieldAux, *ref)
		}
	}

	influxql.WalkFunc(stmt.Statement().Condition, func(node influxql.Node) {
		ref, ok := node.(*influxql.VarRef)
		if ok {
			opts.Aux = append(opts.Aux, *ref)
		}
	})

	return opts, mstOrigin.Name, nil
}
