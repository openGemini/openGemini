package query

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/executor.go

2022.01.23 It has been modified to compatible files in influx/influxql and influx/query.
Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/limiter"
	query2 "github.com/influxdata/influxdb/query"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")

	// ErrQueryAborted is an error returned when the query is aborted.
	ErrQueryAborted = errors.New("query aborted")

	// ErrQueryEngineShutdown is an error sent when the query cannot be
	// created because the query engine was shutdown.
	ErrQueryEngineShutdown = errors.New("query engine shutdown")

	// ErrQueryTimeoutLimitExceeded is an error when a query hits the max time allowed to run.
	ErrQueryTimeoutLimitExceeded = errors.New("query-timeout limit exceeded")

	// ErrAlreadyKilled is returned when attempting to kill a query that has already been killed.
	ErrAlreadyKilled = errors.New("already killed")
)

// Statistics for the Executor
const (
	// PanicCrashEnv is the environment variable that, when set, will prevent
	// the handler from recovering any panics.
	PanicCrashEnv = "INFLUXDB_PANIC_CRASH"
)

type qCtxKey uint8

const (
	QueryDurationKey qCtxKey = iota

	QueryIDKey
)

var batchQueryConcurrenceLimiter limiter.Fixed
var batchQueryLimiterMu sync.RWMutex

func SetBatchqueryLimit(concurrence int) {
	batchQueryLimiterMu.Lock()
	batchQueryConcurrenceLimiter = limiter.NewFixed(concurrence)
	batchQueryLimiterMu.Unlock()
}

// ErrDatabaseNotFound returns a database not found error for the given database name.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrMaxSelectPointsLimitExceeded is an error when a query hits the maximum number of points.
func ErrMaxSelectPointsLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-select-point limit exceeed: (%d/%d)", n, limit)
}

// ErrMaxConcurrentQueriesLimitExceeded is an error when a query cannot be run
// because the maximum number of queries has been reached.
func ErrMaxConcurrentQueriesLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-concurrent-queries limit exceeded(%d, %d)", n, limit)
}

// CoarseAuthorizer determines if certain operations are authorized at the database level.
//
// It is supported both in OSS and Enterprise.
type CoarseAuthorizer interface {
	// AuthorizeDatabase indicates whether the given Privilege is authorized on the database with the given name.
	AuthorizeDatabase(p originql.Privilege, name string) bool
}

type openCoarseAuthorizer struct{}

func (a openCoarseAuthorizer) AuthorizeDatabase(originql.Privilege, string) bool { return true }

// OpenCoarseAuthorizer is a fully permissive implementation of CoarseAuthorizer.
var OpenCoarseAuthorizer openCoarseAuthorizer

// FineAuthorizer determines if certain operations are authorized at the series level.
//
// It is only supported in InfluxDB Enterprise. In OSS it always returns true.
type FineAuthorizer interface {
	// AuthorizeDatabase indicates whether the given Privilege is authorized on the database with the given name.
	AuthorizeDatabase(p originql.Privilege, name string) bool

	// AuthorizeSeriesRead determines if a series is authorized for reading
	AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool

	// AuthorizeSeriesWrite determines if a series is authorized for writing
	AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool

	// IsOpen guarantees that the other methods of a FineAuthorizer always return true.
	//IsOpen() bool
}

// OpenAuthorizer is the Authorizer used when authorization is disabled.
// It allows all operations.
type openAuthorizer struct{}

// OpenAuthorizer can be shared by all goroutines.
var OpenAuthorizer = openAuthorizer{}

// AuthorizeDatabase returns true to allow any operation on a database.
func (a openAuthorizer) AuthorizeDatabase(originql.Privilege, string) bool { return true }

// AuthorizeSeriesRead allows access to any series.
func (a openAuthorizer) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite allows access to any series.
func (a openAuthorizer) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

func (a openAuthorizer) IsOpen() bool { return true }

// AuthorizeSeriesRead allows any query to execute.
func (a openAuthorizer) AuthorizeQuery(_ string, _ *influxql.Query) error { return nil }

// AuthorizerIsOpen returns true if the provided Authorizer is guaranteed to
// authorize anything. A nil Authorizer returns true for this function, and this
// function should be preferred over directly checking if an Authorizer is nil
// or not.
//func AuthorizerIsOpen(a FineAuthorizer) bool {
//	return a == nil || a.IsOpen()
//}

type RowsChan struct {
	Rows    models.Rows // models.Rows of data
	Partial bool        // is partial of rows
}

// ExecutionOptions contains the options for executing a query.
type ExecutionOptions struct {
	// The database the query is running against.
	Database string

	// The retention policy the query is running against.
	RetentionPolicy string

	// Authorizer handles series-level authorization
	Authorizer FineAuthorizer

	// CoarseAuthorizer handles database-level authorization
	CoarseAuthorizer CoarseAuthorizer

	// Node to execute on.
	NodeID uint64

	// The requested maximum number of points to return in each result.
	ChunkSize int

	// If this query return chunk once by once
	Chunked bool

	// If this query is being executed in a read-only context.
	ReadOnly bool

	QueryLimitEn bool

	// Quiet suppresses non-essential output from the query executor.
	Quiet bool

	// AbortCh is a channel that signals when results are no longer desired by the caller.
	AbortCh <-chan struct{}

	// The ChunkImpl maximum number of points to contain. Developers are advised to change only.
	InnerChunkSize int

	// The results of the query executor
	RowsChan chan RowsChan

	ParallelQuery bool
}

type (
	iteratorsContextKey struct{}
	monitorContextKey   struct{}
)

// NewContextWithIterators returns a new context.Context with the *Iterators slice added.
// The query planner will add instances of AuxIterator to the Iterators slice.

//func NewContextWithIterators(ctx context.Context, itr *Processors) context.Context {
//return context.WithValue(ctx, iteratorsContextKey{}, itr)
//}

// StatementExecutor executes a statement within the Executor.
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	ExecuteStatement(stmt influxql.Statement, ctx *ExecutionContext, seq int) error
	Statistics(buffer []byte) ([]byte, error)
}

// StatementNormalizer normalizes a statement before it is executed.
type StatementNormalizer interface {
	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	NormalizeStatement(stmt influxql.Statement, database, retentionPolicy string) error
}

// Executor executes every statement in an Query.
type Executor struct {
	// Used for executing a statement in the query.
	StatementExecutor StatementExecutor

	// Used for tracking running queries.
	TaskManager *TaskManager

	// writer is used for INTO statement
	PointsWriter interface {
		RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error
	}

	// Logger to use for all logging.
	// Defaults to discarding all log output.
	Logger *logger.Logger
}

// NewExecutor returns a new instance of Executor.
func NewExecutor(concurrence int) *Executor {
	batchQueryConcurrenceLimiter = limiter.NewFixed(concurrence)

	return &Executor{
		TaskManager: NewTaskManager(),
		Logger:      logger.NewLogger(errno.ModuleHTTP).With(zap.String("service", "executor")),
	}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *Executor) Close() error {
	return e.TaskManager.Close()
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (e *Executor) WithLogger(log *logger.Logger) {
	e.Logger = log.With(zap.String("service", "query"))
}

// ExecuteQuery executes each statement within a query.
func (e *Executor) ExecuteQuery(query *influxql.Query, opt ExecutionOptions, closing chan struct{}, qDuration *statistics.SQLSlowQueryStatistics) <-chan *query2.Result {
	results := make(chan *query2.Result)
	if opt.ParallelQuery {
		go e.executeParallelQuery(query, opt, closing, qDuration, results)
	} else {
		go e.executeQuery(query, opt, closing, qDuration, results)
	}
	return results
}

func (e *Executor) executeQuery(query *influxql.Query, opt ExecutionOptions, closing <-chan struct{}, qStat *statistics.SQLSlowQueryStatistics, results chan *query2.Result) {
	defer close(results)
	defer e.recover(query, results)
	if qStat != nil && len(query.Statements) > 0 {
		qStat.SetQueryBatch(len(query.Statements))
		qStat.SetQuery(query.String())
	}
	ctx, detach, err := e.TaskManager.AttachQuery(query, opt, closing, qStat)
	if err != nil {
		select {
		case results <- &query2.Result{Err: err}:
		case <-opt.AbortCh:
		}
		return
	}
	defer detach()

	// init point writer of context which may be used to write data with into statement
	ctx.PointsWriter = e.PointsWriter
	// Setup the execution context that will be used when executing statements.
	ctx.Results = results
	atomic.AddInt64(&statistics.HandlerStat.QueryStmtCount, int64(len(query.Statements)))

	var i int
LOOP:
	for ; i < len(query.Statements); i++ {
		ctx.statementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := opt.Database
		if defaultDB == "" {
			if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		if ok, err := isSystemMeasurements(stmt); ok {
			results <- &query2.Result{
				Err: err,
			}
			break LOOP
		}

		// Rewrite statements, if necessary.
		// This can occur on meta read statements which convert to SELECT statements.
		newStmt, err := RewriteStatement(stmt)
		if err != nil {
			results <- &query2.Result{Err: err}
			break
		}
		stmt = newStmt

		// Normalize each statement if possible.
		if normalizer, ok := e.StatementExecutor.(StatementNormalizer); ok {
			if err := normalizer.NormalizeStatement(stmt, defaultDB, opt.RetentionPolicy); err != nil {
				if err := ctx.send(&query2.Result{Err: err}, i); err == ErrQueryAborted {
					return
				}
				break
			}
		}

		// Log each normalized statement.
		if !ctx.Quiet {
			e.Logger.Info("Executing query", zap.String("query", stmt.String()))
		}

		// Send any other statements to the underlying statement executor.
		err = e.StatementExecutor.ExecuteStatement(stmt, ctx, i)
		if errno.Equal(err, errno.ErrQueryKilled) {
			// Query was interrupted so retrieve the real interrupt error from
			// the query task if there is one.
			if qerr := ctx.Err(); qerr != nil {
				err = qerr
			}
		}

		// Send an error for this result if it failed for some reason.
		if err != nil {
			if err := ctx.send(&query2.Result{
				StatementID: i,
				Err:         err,
			}, i); err == ErrQueryAborted {
				return
			}
			// Stop after the first error.
			break
		}

		// Check if the query was interrupted during an uninterruptible statement.
		interrupted := false
		select {
		case <-ctx.Done():
			interrupted = true
		default:
			// Query has not been interrupted.
		}

		if interrupted {
			break
		}
	}

	// Send error results for any statements which were not executed.
	for ; i < len(query.Statements)-1; i++ {
		if err := ctx.send(&query2.Result{
			StatementID: i,
			Err:         ErrNotExecuted,
		}, i); err == ErrQueryAborted {
			return
		}
	}
}

func (e *Executor) executeParallelQuery(query *influxql.Query, opt ExecutionOptions, closing <-chan struct{}, qStat *statistics.SQLSlowQueryStatistics, results chan *query2.Result) {
	defer close(results)
	defer e.recover(query, results)
	if qStat != nil && len(query.Statements) > 0 {
		qStat.SetQueryBatch(len(query.Statements))
		qStat.SetQuery(query.String())
	}

	ctx, detach, err := e.TaskManager.AttachQuery(query, opt, closing, qStat)
	if err != nil {
		select {
		case results <- &query2.Result{Err: err}:
		case <-opt.AbortCh:
		}
		return
	}
	defer detach()

	// init point writer of context which may be used to write data with into statement
	ctx.PointsWriter = e.PointsWriter
	// Setup the execution context that will be used when executing statements.
	ctx.Results = results

	atomic.AddInt64(&statistics.HandlerStat.QueryStmtCount, int64(len(query.Statements)))

	var wg sync.WaitGroup
	var i int
LOOP:
	for ; i < len(query.Statements); i++ {
		ctx.statementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := opt.Database
		if defaultDB == "" {
			if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		if ok, err := isSystemMeasurements(stmt); ok {
			results <- &query2.Result{
				Err: err,
			}
			break LOOP
		}

		batchQueryConcurrenceLimiter.Take()

		wg.Add(1)
		go func(stmt influxql.Statement, seq int) {
			defer func() {
				wg.Done()

				batchQueryConcurrenceLimiter.Release()
			}()

			// Rewrite statements, if necessary.
			// This can occur on meta read statements which convert to SELECT statements.
			newStmt, err := RewriteStatement(stmt)
			if err != nil {
				results <- &query2.Result{Err: err}
				e.Logger.Error("Rewrite Statement", zap.Stringer("query", stmt), zap.Error(err))
				return
			}
			stmt = newStmt

			// Normalize each statement if possible.
			if normalizer, ok := e.StatementExecutor.(StatementNormalizer); ok {
				if err := normalizer.NormalizeStatement(stmt, defaultDB, opt.RetentionPolicy); err != nil {
					if err := ctx.send(&query2.Result{Err: err}, seq); err == ErrQueryAborted {
						e.Logger.Error("Normalize Statement ErrQueryAborted", zap.Stringer("query", stmt))
						return
					}
					e.Logger.Error("Normalize Statement", zap.Stringer("query", stmt), zap.Error(err))
					return
				}
			}

			// Log each normalized statement.
			if !ctx.Quiet {
				e.Logger.Info("Executing query", zap.Stringer("query", stmt))
			} else {
				if seq == 0 {
					e.Logger.Info("Executing query", zap.Stringer("query", stmt), zap.Int("batch", len(query.Statements)))
				}
			}

			// Send any other statements to the underlying statement executor.
			err = e.StatementExecutor.ExecuteStatement(stmt, ctx, seq)
			if errno.Equal(err, errno.ErrQueryKilled) {
				// Query was interrupted so retrieve the real interrupt error from
				// the query task if there is one.
				if qerr := ctx.Err(); qerr != nil {
					err = qerr
				}
			}

			// Send an error for this result if it failed for some reason.
			if err != nil {
				if err := ctx.send(&query2.Result{
					StatementID: i,
					Err:         err,
				}, seq); err == ErrQueryAborted {
					e.Logger.Error("ctx send fail, ErrQueryAborted", zap.Stringer("query", stmt))
					return
				}

				e.Logger.Error("ctx send fail", zap.Stringer("query", stmt), zap.Error(err))
			}
		}(stmt, i)

		// Check if the query was interrupted during an uninterruptible statement.
		interrupted := false
		select {
		case <-ctx.Done():
			interrupted = true
		default:
			// Query has not been interrupted.
		}

		if interrupted {
			break
		}
	}

	wg.Wait()
}

// Do not let queries manually use the system measurements. If we find
// one, return an error. This prevents a person from using the
// measurement incorrectly and causing a panic.
func isSystemMeasurements(stmt influxql.Statement) (bool, error) {
	if stmt, ok := stmt.(*influxql.SelectStatement); ok {
		for _, s := range stmt.Sources {
			switch s := s.(type) {
			case *influxql.Measurement:
				if influxql.IsSystemName(s.Name) {
					command := "the appropriate meta command"
					switch s.Name {
					case "_fieldKeys":
						command = "SHOW FIELD KEYS"
					case "_measurements":
						command = "SHOW MEASUREMENTS"
					case "_series":
						command = "SHOW SERIES"
					case "_tagKeys":
						command = "SHOW TAG KEYS"
					case "_tags":
						command = "SHOW TAG VALUES"
					}
					return true, fmt.Errorf("unable to use system source '%s': use %s instead", s.Name, command)
				}
			}
		}
	}
	return false, nil
}

// Determines if the Executor will recover any panics or let them crash
// the server.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

func (e *Executor) recover(query *influxql.Query, results chan *query2.Result) {
	if err := recover(); err != nil {
		e.Logger.Error(fmt.Sprintf("%s [error:%s] %s", query.String(), err, debug.Stack()))

		internalErr, ok := err.(*errno.Error)
		if ok && errno.Equal(internalErr, errno.DtypeNotSupport) {
			results <- &query2.Result{
				StatementID: -1,
				Err:         fmt.Errorf("%s", err),
			}
		} else {
			results <- &query2.Result{
				StatementID: -1,
				Err:         fmt.Errorf("%s [error:%s]", query.String(), err),
			}
		}

		if willCrash {
			e.Logger.Error(fmt.Sprintf("\n\n=====\nAll goroutines now follow:"))
			buf := debug.Stack()
			e.Logger.Error(fmt.Sprintf("%s", buf))
			os.Exit(1)
		}
	}
}

// Task is the internal data structure for managing queries.
// For the public use data structure that gets returned, see Task.
type Task struct {
	query     string
	database  string
	status    TaskStatus
	startTime time.Time
	closing   chan struct{}
	monitorCh chan error
	err       error
	mu        sync.Mutex
}

// Monitor starts a new goroutine that will monitor a query. The function
// will be passed in a channel to signal when the query has been finished
// normally. If the function returns with an error and the query is still
// running, the query will be terminated.
func (q *Task) Monitor(fn MonitorFunc) {
	go q.monitor(fn)
}

// Error returns any asynchronous error that may have occurred while executing
// the query.
func (q *Task) Error() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

func (q *Task) setError(err error) {
	q.mu.Lock()
	q.err = err
	q.mu.Unlock()
}

func (q *Task) monitor(fn MonitorFunc) {
	if err := fn(q.closing); err != nil {
		select {
		case <-q.closing:
		case q.monitorCh <- err:
		}
	}
}

// close closes the query task closing channel if the query hasn't been previously killed.
func (q *Task) close() {
	q.mu.Lock()
	if q.status != KilledTask {
		// Set the status to killed to prevent closing the channel twice.
		q.status = KilledTask
		close(q.closing)
	}
	q.mu.Unlock()
}

func (q *Task) kill() error {
	q.mu.Lock()
	if q.status == KilledTask {
		q.mu.Unlock()
		return ErrAlreadyKilled
	}
	q.status = KilledTask
	close(q.closing)
	q.mu.Unlock()
	return nil
}
