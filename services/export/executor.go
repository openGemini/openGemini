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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/scheduler"
	"go.uber.org/zap"
)

// Executor is the subtask executor (implements specific business logic)
type Executor struct {
	store   *LocalStore
	logger  *logger.Logger
	engine  engine.Engine
	factory *ExporterFactory
}

// NewExecutor creates a new Executor instance.
func NewExecutor(store *LocalStore, engine engine.Engine, factory *ExporterFactory, logger *logger.Logger) *Executor {
	return &Executor{
		store:   store,
		logger:  logger,
		engine:  engine,
		factory: factory,
	}
}

// Execute executes a subtask
func (e *Executor) Execute(subTask *SubExportTask) error {
	// 1. Acquire execution rights (state transition: Pending→Running)
	if !subTask.Status.TryStart() {
		e.logger.Warn("subtask is not pending, skip execution",
			zap.String("subtask_id", subTask.ID),
		)
		return nil
	}
	e.store.SaveSubExportTask(subTask)

	// 2. Update the status after execution (whether successful or failed)
	defer func() {
		if !subTask.Status.IsSuccess() {
			subTask.Status.MarkFailed()
		}
		e.store.SaveSubExportTask(subTask)
		if r := recover(); r != nil {
			e.logger.Error("subtask panicked during execution",
				zap.String("subtask_id", subTask.ID),
				zap.Any("panic", r),
			)
			e.store.SaveSubExportTask(subTask)
		}
	}()
	// 3. Prepare the export context (get the original task configuration)
	originalTask, ok := e.store.GetOriginalExportTask(subTask.ParentID)
	if !ok {
		err := errors.New("original export task not found")
		return err
	}
	// 4. Select and call the corresponding Exporter (obtained through a factory, not dependent on specific format)
	exporter, err := e.factory.GetExporter(originalTask.Format)
	if err != nil {
		return fmt.Errorf("failed to get exporter: %w", err)
	}

	// 5. Execute the export logic (specific format is implemented by Exporter)
	exportCtx := &TaskExportContext{
		Store:  e.store,
		Engine: e.engine,
	}
	if err = exporter.Export(exportCtx, subTask); err != nil {
		return fmt.Errorf("export failed: %w", err)
	}

	// 6. Export successful, update the status and persist
	if !subTask.Status.MarkSuccess() {
		e.logger.Error("failed to mark subtask as success (status changed unexpectedly)",
			zap.String("subtask_id", subTask.ID),
		)
		return nil
	}
	return nil
}

// TaskAdapter adapts SubExportTask (model layer) to the scheduler's task interface,
// bridging model and execution via Executor.
type TaskAdapter struct {
	scheduler.BaseTask
	executor *Executor
	subTask  *SubExportTask
	ctx      context.Context
}

// Execute implements the scheduler task interface, delegating execution to Executor.
func (t *TaskAdapter) Execute() {
	Stat.RunningSubTasks.Incr()
	startTime := time.Now()

	err := t.executor.Execute(t.subTask)
	Stat.SubTaskExecutionTime.AddSinceMilli(startTime)
	if err != nil {
		t.executor.logger.Error("export task failed",
			zap.String("subtask_id", t.subTask.ID),
			zap.Error(err),
		)
	}
}

// NewTaskAdapter creates a TaskAdapter linking SubExportTask with Executor.
func NewTaskAdapter(executor *Executor, subTask *SubExportTask, ctx context.Context, key string) *TaskAdapter {
	task := &TaskAdapter{
		executor: executor,
		subTask:  subTask,
		ctx:      ctx,
	}
	task.Init(key)
	return task
}
