package query

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/execution_context.go
*/

import (
	"context"
	"sync"

	"github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// ExecutionContext contains state that the query is currently executing with.
type ExecutionContext struct {
	context.Context

	// The statement ID of the executing query.
	statementID int

	// The query ID of the executing query.
	QueryID uint64

	// The query task information available to the StatementExecutor.
	task *Task

	// Output channel where results and errors should be sent.
	Results chan *query.Result

	// Options used to start this query.
	ExecutionOptions

	// point writer which belong to the query, it is used for INTO statement
	PointsWriter interface {
		RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error
	}

	mu   sync.RWMutex
	done chan struct{}
	err  error
}

func (ctx *ExecutionContext) watch() {
	ctx.done = make(chan struct{})
	if ctx.err != nil {
		close(ctx.done)
		return
	}

	go func() {
		defer close(ctx.done)

		var taskCtx <-chan struct{}
		if ctx.task != nil {
			taskCtx = ctx.task.closing
		}

		select {
		case <-taskCtx:
			ctx.err = ctx.task.Error()
			if ctx.err == nil {
				ctx.err = errno.NewError(errno.ErrQueryKilled)
			}
		case <-ctx.AbortCh:
			ctx.err = ErrQueryAborted
		case <-ctx.Context.Done():
			ctx.err = ctx.Context.Err()
		}
	}()
}

func (ctx *ExecutionContext) Done() <-chan struct{} {
	ctx.mu.RLock()
	if ctx.done != nil {
		defer ctx.mu.RUnlock()
		return ctx.done
	}
	ctx.mu.RUnlock()

	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.done == nil {
		ctx.watch()
	}
	return ctx.done
}

func (ctx *ExecutionContext) Err() error {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.err
}

func (ctx *ExecutionContext) Value(key interface{}) interface{} {
	switch key {
	case monitorContextKey{}:
		return ctx.task
	}
	return ctx.Context.Value(key)
}

// send sends a Result to the Results channel and will exit if the query has
// been aborted.
func (ctx *ExecutionContext) send(result *query.Result, seq int) error {
	result.StatementID = seq
	select {
	case <-ctx.AbortCh:
		return ErrQueryAborted
	case ctx.Results <- result:
	}
	return nil
}

// Send sends a Result to the Results channel and will exit if the query has
// been interrupted or aborted.
func (ctx *ExecutionContext) Send(result *query.Result, seq int) error {
	result.StatementID = seq
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ctx.Results <- result:
	}
	return nil
}
