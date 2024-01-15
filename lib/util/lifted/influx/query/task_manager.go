package query

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/task_manager.go
*/

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const (
	// DefaultQueryTimeout is the default timeout for executing a query.
	// A value of zero will have no query timeout.
	DefaultQueryTimeout = time.Duration(0)
	//The query id span of the current SQL node.
	queryIdSpan = 100000000 // 100 million
)

type TaskStatus int

const (
	// RunningTask is set when the task is running.
	RunningTask TaskStatus = iota + 1

	// KilledTask is set when the task is killed, but resources are still
	// being used.
	KilledTask
)

func (t TaskStatus) String() string {
	switch t {
	case RunningTask:
		return "running"
	case KilledTask:
		return "killed"
	default:
		return "unknown"
	}
}

func (t TaskStatus) MarshalJSON() ([]byte, error) {
	s := t.String()
	return json.Marshal(s)
}

func (t *TaskStatus) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("running")) {
		*t = RunningTask
	} else if bytes.Equal(data, []byte("killed")) {
		*t = KilledTask
	} else if bytes.Equal(data, []byte("unknown")) {
		*t = TaskStatus(0)
	} else {
		return fmt.Errorf("unknown task status: %s", string(data))
	}
	return nil
}

type QueryIDRegister interface {
	RetryRegisterQueryIDOffset(host string) (uint64, error)
}

// TaskManager takes care of all aspects related to managing running queries.
type TaskManager struct {
	// Query execution timeout.
	QueryTimeout time.Duration

	// Log queries if they are slower than this time.
	// If zero, slow queries will never be logged.
	LogQueriesAfter time.Duration

	// Maximum number of concurrent queries.
	MaxConcurrentQueries int

	// Logger to use for all logging.
	// Defaults to discarding all log output.
	Logger *logger.Logger

	// Used for managing and tracking running queries.
	nextID            uint64
	queryIDOffset     uint64
	queryIDUpperLimit uint64
	queries           map[uint64]*Task

	registerOnce uint32 // ensure the registration of sql node will only be successfully executed once

	Register QueryIDRegister
	Host     string

	mu       sync.RWMutex
	shutdown bool
}

// NewTaskManager creates a new TaskManager.
func NewTaskManager() *TaskManager {
	return &TaskManager{
		QueryTimeout: DefaultQueryTimeout,
		Logger:       logger.NewLogger(errno.ModuleHTTP),
		queries:      make(map[uint64]*Task),
	}
}

// ExecuteStatement executes a statement containing one of the task management queries.
func (t *TaskManager) ExecuteStatement(stmt influxql.Statement, ctx *ExecutionContext, seq int) error {
	switch stmt := stmt.(type) {
	case *influxql.ShowQueriesStatement:
		rows, err := t.executeShowQueriesStatement(stmt)
		if err != nil {
			return err
		}

		ctx.Send(&query.Result{
			Series: rows,
		}, seq)
	case *influxql.KillQueryStatement:
		var messages []*query.Message
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}

		if err := t.executeKillQueryStatement(stmt); err != nil {
			return err
		}
		ctx.Send(&query.Result{
			Messages: messages,
		}, seq)
	default:
		return ErrInvalidQuery
	}
	return nil
}

func (t *TaskManager) executeKillQueryStatement(stmt *influxql.KillQueryStatement) error {
	return t.KillQuery(stmt.QueryID)
}

func (t *TaskManager) executeShowQueriesStatement(q *influxql.ShowQueriesStatement) (models.Rows, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := time.Now()

	values := make([][]interface{}, 0, len(t.queries))
	for id, qi := range t.queries {
		d := now.Sub(qi.startTime)

		switch {
		case d >= time.Second:
			d = d - (d % time.Second)
		case d >= time.Millisecond:
			d = d - (d % time.Millisecond)
		case d >= time.Microsecond:
			d = d - (d % time.Microsecond)
		}

		values = append(values, []interface{}{id, qi.query, qi.database, d.String(), qi.status.String()})
	}

	return []*models.Row{{
		Columns: []string{"qid", "query", "database", "duration", "status"},
		Values:  values,
	}}, nil
}

func (t *TaskManager) queryError(qid uint64, err error) {
	t.mu.RLock()
	query := t.queries[qid]
	t.mu.RUnlock()
	if query != nil {
		query.setError(err)
	}
}

// AttachQuery attaches a running query to be managed by the TaskManager.
// Returns the query id of the newly attached query or an error if it was
// unable to assign a query id or attach the query to the TaskManager.
// This function also returns a channel that will be closed when this
// query finishes running.
//
// After a query finishes running, the system is free to reuse a query id.
func (t *TaskManager) AttachQuery(q *influxql.Query, opt ExecutionOptions, interrupt <-chan struct{}, qStat *statistics.SQLSlowQueryStatistics) (*ExecutionContext, func(), error) {
	t.mu.RLock()
	isShutDown := t.shutdown
	curQueriesCount := len(t.queries)
	t.mu.RUnlock()

	if isShutDown {
		return nil, nil, ErrQueryEngineShutdown
	}
	if t.MaxConcurrentQueries > 0 && curQueriesCount >= t.MaxConcurrentQueries {
		return nil, nil, ErrMaxConcurrentQueriesLimitExceeded(len(t.queries), t.MaxConcurrentQueries)
	}

	// only the first query can try to register query id offset
	if err := t.tryRegisterQueryIDOffset(); err != nil {
		return nil, nil, err
	}

	qid := t.AssignQueryID()

	query := &Task{
		database:  opt.Database,
		status:    RunningTask,
		startTime: time.Now(),
		closing:   make(chan struct{}),
		monitorCh: make(chan error),
	}
	if qStat != nil {
		query.query = qStat.Query
	} else {
		query.query = q.String()
	}

	t.mu.Lock()
	t.queries[qid] = query
	t.mu.Unlock()

	go t.waitForQuery(qid, query.closing, interrupt, query.monitorCh)
	if t.LogQueriesAfter != 0 {
		go query.monitor(func(closing <-chan struct{}) error {
			timer := time.NewTimer(t.LogQueriesAfter)
			defer timer.Stop()

			select {
			case <-timer.C:
				t.Logger.Warn("Detected slow query", zap.String("query", query.query),
					zap.Uint64("qid", qid), zap.String("db", query.database),
					zap.Duration("threshold", t.LogQueriesAfter))
			case <-closing:
			}
			return nil
		})
	}

	qCtx := context.Background()
	qCtx = context.WithValue(qCtx, QueryIDKey, qid)
	qCtx = context.WithValue(qCtx, QueryDurationKey, qStat)
	ctx := &ExecutionContext{
		Context:          qCtx,
		QueryID:          qid,
		task:             query,
		ExecutionOptions: opt,
	}
	ctx.watch()
	return ctx, func() { t.DetachQuery(qid) }, nil
}

// KillQuery enters a query into the killed state and closes the channel
// from the TaskManager. This method can be used to forcefully terminate a
// running query.
func (t *TaskManager) KillQuery(qid uint64) error {
	t.mu.RLock()
	query := t.queries[qid]
	t.mu.RUnlock()

	if query == nil {
		return fmt.Errorf("no such query id: %d", qid)
	}
	return query.kill()
}

// DetachQuery removes a query from the query table. If the query is not in the
// killed state, this will also close the related channel.
func (t *TaskManager) DetachQuery(qid uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	query := t.queries[qid]
	if query == nil {
		return fmt.Errorf("no such query id: %d", qid)
	}

	query.close()
	delete(t.queries, qid)
	return nil
}

func (t *TaskManager) tryRegisterQueryIDOffset() error {
	// Ensure that only one goroutine can register.
	// And if registration is failed, return the error directly.
	if atomic.LoadUint32(&t.registerOnce) == 0 {
		t.mu.Lock()
		defer t.mu.Unlock()
		if atomic.LoadUint32(&t.registerOnce) == 0 {
			offset, err := t.Register.RetryRegisterQueryIDOffset(t.Host)
			if err != nil {
				return err
			}
			// update registerOnce state to allow assign query id
			t.InitQueryIDByOffset(offset)
			atomic.StoreUint32(&t.registerOnce, 1)
		}
	}
	return nil
}

// QueryInfo represents the information for a query.
type QueryInfo struct {
	ID       uint64        `json:"id"`
	Query    string        `json:"query"`
	Database string        `json:"database"`
	Duration time.Duration `json:"duration"`
	Status   TaskStatus    `json:"status"`
}

// Queries returns a list of all running queries with information about them.
func (t *TaskManager) Queries() []QueryInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := time.Now()
	queries := make([]QueryInfo, 0, len(t.queries))
	for id, qi := range t.queries {
		queries = append(queries, QueryInfo{
			ID:       id,
			Query:    qi.query,
			Database: qi.database,
			Duration: now.Sub(qi.startTime),
			Status:   qi.status,
		})
	}
	return queries
}

func (t *TaskManager) waitForQuery(qid uint64, interrupt <-chan struct{}, closing <-chan struct{}, monitorCh <-chan error) {
	var timerCh <-chan time.Time
	if t.QueryTimeout != 0 {
		timer := time.NewTimer(t.QueryTimeout)
		timerCh = timer.C
		defer timer.Stop()
	}

	select {
	case <-closing:
		t.queryError(qid, errno.NewError(errno.ErrQueryKilled, qid))
	case err := <-monitorCh:
		if err == nil {
			break
		}

		t.queryError(qid, err)
	case <-timerCh:
		t.queryError(qid, ErrQueryTimeoutLimitExceeded)
	case <-interrupt:
		// Query was manually closed so exit the select.
		return
	}
	t.KillQuery(qid)
}

// Close kills all running queries and prevents new queries from being attached.
func (t *TaskManager) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.shutdown = true
	for _, query := range t.queries {
		query.setError(ErrQueryEngineShutdown)
		query.close()
	}
	t.queries = nil
	return nil
}

func (t *TaskManager) Statistics(buffer []byte) ([]byte, error) {
	return nil, nil
}

func (t *TaskManager) InitQueryIDByOffset(offset uint64) {
	t.nextID = offset
	t.queryIDOffset = offset
	t.queryIDUpperLimit = offset + queryIdSpan
}

// AssignQueryID assign a query id for a sql
func (t *TaskManager) AssignQueryID() uint64 {
	// set-query-id: The qid can be controlled for easy testing.
	failpoint.Inject("set-query-id", func(val failpoint.Value) {
		if n, ok := val.(int); ok {
			n = n % queryIdSpan
			n += int(t.queryIDOffset)
			atomic.StoreUint64(&t.nextID, uint64(n))
			failpoint.Return(uint64(n))
		}
	})
	atomic.CompareAndSwapUint64(&t.nextID, t.queryIDUpperLimit, t.queryIDOffset)
	return atomic.AddUint64(&t.nextID, 1)
}
