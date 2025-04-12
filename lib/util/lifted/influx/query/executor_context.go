package query

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/execution_context.go
*/

import (
	"context"
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/lib/errno"
	index2 "github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// ExecutionContext contains state that the query is currently executing with.
type ExecutionContext struct {
	context.Context

	// The statement ID of the executing query.
	statementID int

	// The query IDs of the executing query. Each statement assigns one ID.
	QueryID []uint64

	// The query task information available to the StatementExecutor. Each statement is one sub task.
	task []*Task

	// Output channel where results and errors should be sent.
	Results chan *Result

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

		for _, task := range ctx.task {
			var taskCtx <-chan struct{}
			if task != nil {
				taskCtx = task.closing
			}

			select {
			case <-taskCtx:
				ctx.err = task.Error()
				if ctx.err == nil {
					ctx.err = errno.NewError(errno.ErrQueryKilled)
				}
			case <-ctx.AbortCh:
				ctx.err = ErrQueryAborted
			case <-ctx.Context.Done():
				ctx.err = ctx.Context.Err()
			}
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
func (ctx *ExecutionContext) send(result *Result, seq int) error {
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
func (ctx *ExecutionContext) Send(result *Result, seq int, ctxWithWriter context.Context) error {
	if ctxWithWriter != nil {
		queryIndexState := ctxWithWriter.Value(index2.QueryIndexState)
		if ctx.ExecutionOptions.IsQuerySeriesLimit && queryIndexState != nil {
			queryIndexStateV := queryIndexState.(*int32)
			if *queryIndexStateV > 0 {
				result.Err = fmt.Errorf("num of index over the series limit")
			}
		}
	}

	result.StatementID = seq
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ctx.Results <- result:
	}
	return nil
}
