package httpd

import (
	"bytes"
	"context"
	json2 "encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bmizerany/pat"
	"github.com/golang-jwt/jwt/v5"
	"github.com/golang/snappy"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/engine/hybridqp"
	config2 "github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/auth"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	query2 "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary
var handlerLogLimit int

const (
	// DefaultChunkSize specifies the maximum number of points that will
	// be read before sending results back to the engine.
	//
	// This has no relation to the number of bytes that are returned.
	DefaultChunkSize = 10000

	MaxChunkSize = DefaultChunkSize * 50

	DefaultInnerChunkSize = 1024

	MaxInnerChunkSize = 4096

	DefaultDebugRequestsInterval = 10 * time.Second

	MaxDebugRequestsInterval = 6 * time.Hour

	// fieldTagKey is the tag key that all field names use in the new storage processor
	fieldTagKey = "_field"

	// measurementTagKey is the tag key that all measurement names use in the new storage processor
	measurementTagKey = "_measurement"
)

var (
	// ErrBearerAuthDisabled is returned when client specifies bearer auth in
	// a request but bearer auth is disabled.
	ErrBearerAuthDisabled = errors.New("bearer auth disabld")
)

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	// Authenticate using basic authentication.
	UserAuthentication AuthenticationMethod = iota

	// Authenticate with jwt.
	BearerAuthentication
)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

// Route specifies how to handle a HTTP verb for a given endpoint.
type Route struct {
	Name           string
	Method         string
	Pattern        string
	Gzipped        bool
	LoggingEnabled bool
	HandlerFunc    interface{}
}

type SubscriberManager interface {
	Send(db, rp string, lineProtocol []byte)
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	mux       *pat.PatternServeMux
	Version   string
	BuildType string

	MetaClient interface {
		Database(name string) (*meta2.DatabaseInfo, error)
		Authenticate(username, password string) (ui meta2.User, err error)
		User(username string) (meta2.User, error)
		AdminUserExists() bool
		ShowShards() models.Rows
		TagArrayEnabled(db string) bool
		DataNode(id uint64) (*meta2.DataNode, error)
		DataNodes() ([]meta2.DataNode, error)

		CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta2.DatabaseInfo, error)
		Databases() map[string]*meta2.DatabaseInfo
		MarkDatabaseDelete(name string) error
		Measurements(database string, ms influxql.Measurements) ([]string, error)

		CreateRetentionPolicy(database string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error)
		RetentionPolicy(database, name string) (rpi *meta2.RetentionPolicyInfo, err error)
		MarkRetentionPolicyDelete(database, name string) error
		CreateMeasurement(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo, indexR *influxql.IndexRelation, engineType config2.EngineType,
			colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error)
		UpdateMeasurement(db, rp, mst string, options *meta2.Options) error
		GetShardGroupByTimeRange(repoName, streamName string, min, max time.Time) ([]*meta2.ShardGroupInfo, error)
	}

	QueryAuthorizer interface {
		AuthorizeQuery(u meta2.User, query *influxql.Query, database string) error
	}

	WriteAuthorizer interface {
		AuthorizeWrite(username, database string) error
	}

	ExtSysCtrl interface {
		SendSysCtrlOnNode(nodID uint64, req netstorage.SysCtrlRequest) (map[string]string, error)
	}

	QueryExecutor *query2.Executor

	Monitor interface {
	}

	PointsWriter interface {
		RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error
	}

	RecordWriter interface {
		RetryWriteLogRecord(database, retentionPolicy, measurement string, rec *record.Record) error
	}

	SubscriberManager

	Config           *config.Config
	Logger           *logger.Logger
	CLFLogger        *zap.Logger
	accessLog        *os.File
	accessLogFilters config.StatusFilters

	requestTracker   *httpd.RequestTracker
	writeThrottler   *Throttler
	queryThrottler   *Throttler
	slowQueries      chan *hybridqp.SelectDuration
	StatisticsPusher *statisticsPusher.StatisticsPusher
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(c config.Config) *Handler {
	h := &Handler{
		mux:            pat.New(),
		Config:         &c,
		Logger:         logger.NewLogger(errno.ModuleHTTP),
		CLFLogger:      logger.GetLogger(),
		requestTracker: httpd.NewRequestTracker(),
		slowQueries:    make(chan *hybridqp.SelectDuration, 256),
		QueryExecutor:  query2.NewExecutor(cpu.GetCpuNum()),
	}

	// Limit the number of concurrent & enqueued write requests.
	h.writeThrottler = NewThrottler(c.MaxConcurrentWriteLimit, c.MaxEnqueuedWriteLimit, c.WriteRequestRateLimit)
	h.writeThrottler.EnqueueTimeout = time.Duration(c.EnqueuedWriteTimeout)
	h.writeThrottler.Logger = logger.GetLogger()

	h.queryThrottler = NewThrottler(c.MaxConcurrentQueryLimit, c.MaxEnqueuedQueryLimit, c.QueryRequestRateLimit)
	h.queryThrottler.EnqueueTimeout = time.Duration(c.EnqueuedQueryTimeout)
	h.queryThrottler.Logger = logger.GetLogger()

	// Disable the write log if they have been suppressed.
	writeLogEnabled := c.LogEnabled
	if c.SuppressWriteLog {
		writeLogEnabled = false
	}

	h.AddRoutes([]Route{
		Route{
			"query-options", // Satisfy CORS checks.
			"OPTIONS", "/query", false, true, h.serveOptions,
		},
		Route{
			"query", // Query serving route.
			"GET", "/query", true, true, h.serveQuery,
		},
		Route{
			"query", // Query serving route.
			"POST", "/query", true, true, h.serveQuery,
		},
		Route{
			"write-options", // Satisfy CORS checks.
			"OPTIONS", "/write", false, true, h.serveOptions,
		},
		Route{
			"write", // Data-ingest route.
			"POST", "/write", true, writeLogEnabled, h.serveWrite,
		},
		Route{ // Ping
			"ping",
			"GET", "/ping", false, true, h.servePing,
		},
		Route{ // Ping
			"ping-head",
			"HEAD", "/ping", false, true, h.servePing,
		},
		Route{ // Ping w/ status
			"status",
			"GET", "/status", false, true, h.serveStatus,
		},
		Route{ // Ping w/ status
			"status-head",
			"HEAD", "/status", false, true, h.serveStatus,
		},
		Route{
			"prometheus-metrics",
			"GET", "/metrics", false, true, promhttp.Handler().ServeHTTP,
		},
		Route{
			"failpoint",
			"POST", "/failpoint", false, true, h.failPoint,
		},
		Route{
			"prometheus-write", // Prometheus remote write
			"POST", "/api/v1/prom/write", false, true, h.servePromWrite,
		},
		Route{
			"prometheus-read", // Prometheus remote read
			"POST", "/api/v1/prom/read", true, true, h.servePromRead,
		},
		Route{ // sysCtrl
			"sysCtrl",
			"POST", "/debug/ctrl", false, true, h.serveSysCtrl,
		},
		// repository related operations
		Route{
			"create-repository",
			"POST", "/api/v1/repository/:repository", false, true, h.serveCreateRepository,
		},
		Route{
			"delete-repository",
			"DELETE", "/api/v1/repository/:repository", false, true, h.serveDeleteRepository,
		},
		Route{
			"list-repository",
			"GET", "/api/v1/repository", false, true, h.serveListRepository,
		},
		Route{
			"show-repository",
			"GET", "/api/v1/repository/:repository", false, true, h.serveShowRepository,
		},
		Route{
			"update-repository",
			"PUT", "/api/v1/repository/:repository", false, true, h.serveUpdateRepository,
		},
		// logstream related operations
		Route{
			"create-logStream",
			"POST", "/api/v1/logstream/:repository/:logStream", false, true, h.serveCreateLogstream,
		},
		Route{
			"delete-logStream",
			"DELETE", "/api/v1/logstream/:repository/:logStream", false, true, h.serveDeleteLogstream,
		},
		Route{
			"list-logStream",
			"GET", "/api/v1/logstream/:repository", false, true, h.serveListLogstream,
		},
		Route{
			"show-logStream",
			"GET", "/api/v1/logstream/:repository/:logStream", false, true, h.serveShowLogstream,
		},
		Route{
			"update-logStream",
			"PUT", "/api/v1/logstream/:repository/:logStream", false, true, h.serveUpdateLogstream,
		},
		Route{
			"write-log", // Data-ingest route.
			"POST", "/repo/:repository/logstreams/:logStream/records", false, true, h.serveRecord,
		},
		Route{
			"upload", // Data-upload route.
			"POST", "/repo/:repository/logstreams/:logStream/upload", false, true, h.serveUpload,
		},
		Route{
			"log-list", // Query for Log.
			"GET", "/repo/:repository/logstreams/:logStream/logs", true, true, h.serveQueryLog,
		},
		Route{
			"log-by-cursor", // Query for Log by cursor.
			"GET", "/repo/:repository/logstreams/:logStream/logbycursor", true, true, h.serveQueryLogByCursor,
		},
		Route{
			"log-consume", // Query for Log.
			"GET", "/repo/:repository/logstreams/:logStream/consume/logs", true, true, h.serveConsumeLogs,
		},
		Route{
			"log-consume-cursor-time", // Query for Log.
			"GET", "/repo/:repository/logstreams/:logStream/consume/cursor-time", true, true, h.serveConsumeCursorTime,
		},
		Route{
			"log-consume-cursors", // Query for Log.
			"GET", "/repo/:repository/logstreams/:logStream/consume/cursors", true, true, h.serveGetConsumeCursors,
		},
		Route{
			"log-context", // Query for Log.
			"GET", "/repo/:repository/logstreams/:logStream/context", true, true, h.serveContextQueryLog,
		},
		Route{
			"log-agg", // Query for Log.
			"GET", "/repo/:repository/logstreams/:logStream/histogram", true, true, h.serveAggLogQuery,
		},
		Route{
			"log-agg", // Query for Log.
			"GET", "/repo/:repository/logstreams/:logStream/analytics", true, true, h.serveAnalytics,
		},
		Route{
			"log-cursor", // Get Cursor for Log.
			"GET", "/repo/:repository/logstreams/:logStream/cursor", true, true, h.serveGetCursor,
		},
		Route{
			"log-pull-cursor", // Pull data for Log.
			"GET", "/repo/:repository/logstreams/:logStream/cursor/:cursor", true, true, h.servePullLog,
		},
	}...)

	fluxRoute := Route{
		"flux-read",
		"POST", "/api/v2/query", true, true, nil,
	}

	if !c.FluxEnabled {
		fluxRoute.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Flux query service disabled. Verify flux-enabled=true in the [http] section of the InfluxDB config.", http.StatusForbidden)
		}
	} else {
		fluxRoute.HandlerFunc = h.serveFluxQuery
	}
	h.AddRoutes(fluxRoute)

	return h
}

func (h *Handler) Open() {
	if h.Config.LogEnabled {
		path := "stderr"

		if h.Config.AccessLogPath != "" {
			f, err := os.OpenFile(h.Config.AccessLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
			if err != nil {
				h.Logger.Error("unable to open access log, falling back to stderr", zap.Error(err), zap.String("path", h.Config.AccessLogPath))
				return
			}
			h.CLFLogger = zap.NewNop()
			h.accessLog = f
			path = h.Config.AccessLogPath
		}
		h.Logger.Info("opened HTTP access log", zap.String("path", path))
	}
	h.accessLogFilters = config.StatusFilters(h.Config.AccessLogStatusFilters)

	if h.Config.AuthEnabled && h.Config.SharedSecret == "" {
		h.Logger.Info("Auth is enabled but shared-secret is blank. BearerAuthentication is disabled.")
	}

	h.QueryAuthorizer = auth.NewQueryAuthorizer(h.MetaClient.(*meta.Client))
	h.WriteAuthorizer = auth.NewWriteAuthorizer(h.MetaClient.(*meta.Client))
}

func (h *Handler) Close() {
	if h.accessLog != nil {
		h.accessLog.Close()
		h.accessLog = nil
		h.accessLogFilters = nil
	}
}

// AddRoutes sets the provided routes on the handler.
func (h *Handler) AddRoutes(routes ...Route) {
	for _, r := range routes {
		var handler http.Handler

		// If it's a handler func that requires authorization, wrap it in authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, meta2.User)); ok {
			handler = authenticate(hf, h, h.Config.AuthEnabled)
		}

		// This is a normal handler signature and does not require authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}

		// Throttle route if this is a write endpoint.
		if r.Method == http.MethodPost {
			switch r.Pattern {
			case "/write", "/api/v1/prom/write":
				handler = h.writeThrottler.Handler(handler)
			case "/query", "/api/v1/prom/query":
				handler = h.queryThrottler.Handler(handler)
			default:
			}
		}

		if r.Method == http.MethodGet {
			switch r.Pattern {
			case "/query", "/api/v1/prom/query":
				handler = h.queryThrottler.Handler(handler)
			default:
			}
		}

		handler = h.responseWriter(handler)
		if r.Gzipped {
			handler = gzipFilter(handler)
		}
		handler = cors(handler)
		handler = requestID(handler)
		if h.Config.LogEnabled && r.LoggingEnabled {
			handler = h.logging(handler, r.Name)
		}
		//handler = h.recovery(handler, r.Name) // make sure recovery is always last

		h.mux.Add(r.Method, r.Pattern, handler)
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&statistics.HandlerStat.Requests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveRequests, 1)
	defer atomic.AddInt64(&statistics.HandlerStat.ActiveRequests, -1)
	start := time.Now()

	// changed 2023-06-30, use GeminiDB replace influxdb
	// Add version and build header to all Geminidb requests.
	w.Header().Add("X-Geminidb-Version", h.Version)
	w.Header().Add("X-Geminidb-Build", h.BuildType)

	if strings.HasPrefix(r.URL.Path, "/debug/pprof") && h.Config.PprofEnabled {
		h.handleProfiles(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/requests") {
		h.serveDebugRequests(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/vars") {
		h.serveExpvar(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/query") {
		h.serveDebugQuery(w, r)
	} else {
		h.mux.ServeHTTP(w, r)
	}

	atomic.AddInt64(&statistics.HandlerStat.RequestDuration, time.Since(start).Nanoseconds())
}

// writeHeader writes the provided status code in the response, and
// updates relevant http error statistics.
func (h *Handler) writeHeader(w http.ResponseWriter, code int) {
	switch code / 100 {
	case 4:
		atomic.AddInt64(&statistics.HandlerStat.ClientErrors, 1)
	case 5:
		atomic.AddInt64(&statistics.HandlerStat.ServerErrors, 1)
	}
	w.WriteHeader(code)
}

func isInternalDatabase(dbName string) bool {
	return dbName == "_internal"
}

func (h *Handler) serveSysCtrl(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.requestTracker.Add(r, user)

	// Check authorization.
	if h.Config.AuthEnabled {
		if user == nil {
			// no users in system
			h.httpError(w, "error authorizing query: create admin user first or disable authentication", http.StatusForbidden)
			h.Logger.Error("error authorizing query: create admin user first or disable authentication")
			return
		}
		if !user.AuthorizeUnrestricted() {
			h.httpError(w, "error authorizing, requires admin privilege only", http.StatusForbidden)
			h.Logger.Error("exec error! authorizing query", zap.Any("r", r), zap.String("userID", user.ID()))
			return
		}
		h.Logger.Info("execute sys ctrl by admin user", zap.String("userID", user.ID()))
	}

	h.serveDebug(w, r)
}

func (h *Handler) getQueryFromRequest(r *http.Request, param *QueryParam, user meta2.User) string {
	var qp string
	if param == nil {
		// Attempt to read the form value from the "q" form value.
		qp = r.FormValue("q")
	} else {
		qp = param.Query
	}

	qp = strings.TrimSpace(qp)
	if user != nil {
		h.Logger.Info(app.HideQueryPassword(qp), zap.String("userID", user.ID()))
	} else {
		h.Logger.Info(app.HideQueryPassword(qp))
	}

	return qp
}

func (h *Handler) newQueryReader(r *http.Request, param *QueryParam, user meta2.User) (io.Reader, multipart.File, error) {
	// Attempt to read the query value from the request.
	qp := h.getQueryFromRequest(r, param, user)
	if qp != "" {
		return strings.NewReader(qp), nil, nil
	}

	if r.MultipartForm != nil && r.MultipartForm.File != nil {
		// If we have a multipart/form-data, try to retrieve a file from 'q'.
		if fhs := r.MultipartForm.File["q"]; len(fhs) > 0 {
			f, err := fhs[0].Open()
			if err != nil {
				h.Logger.Error("query error! ", zap.Error(err), zap.Any("r", r))
				return nil, nil, err
			}
			return f, f, nil
		}
	}
	h.Logger.Error("query error! `missing required parameter: q", zap.Any("r", r))
	return nil, nil, fmt.Errorf(`missing required parameter "q"`)
}

func (h *Handler) parseQueryParams(r *http.Request) (map[string]interface{}, error) {
	rawParams := r.FormValue("params")
	if rawParams == "" {
		return nil, nil
	}

	var params map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(rawParams))
	decoder.UseNumber()
	if err := decoder.Decode(&params); err != nil {
		h.Logger.Error("query error! parsing query parameters", zap.Error(err), zap.String("db", r.FormValue("db")), zap.Any("r", r))
		return nil, fmt.Errorf("error parsing query parameters: " + err.Error())
	}

	// Convert json.Number into int64 and float64 values
	for k, v := range params {
		if v, ok := v.(json2.Number); ok {
			var err error
			if strings.Contains(string(v), ".") {
				params[k], err = v.Float64()
			} else {
				params[k], err = v.Int64()
			}

			if err != nil {
				h.Logger.Error("query error! parsing json value", zap.Error(err), zap.String("db", r.FormValue("db")), zap.Any("r", r))
				return nil, fmt.Errorf("error parsing json value: " + err.Error())
			}
		}
	}
	return params, nil
}

func (h *Handler) checkAuthorization(user meta2.User, query *influxql.Query, database string) error {
	// Check authorization.
	if !h.Config.AuthEnabled {
		return nil
	}
	var userID string
	if user != nil {
		// no users in system
		userID = user.ID()
	}

	if err := h.QueryAuthorizer.AuthorizeQuery(user, query, database); err != nil {
		if err, ok := err.(meta2.ErrAuthorize); ok {
			h.Logger.Info("Unauthorized request",
				zap.String("user", err.User),
				zap.Stringer("query", err.Query),
				zap.String("database", err.Database))
		}
		h.Logger.Error("query error! authorizing query", zap.Error(err), zap.String("db", database), zap.String("userID", userID))
		return err
	}
	h.Logger.Info("login success", zap.String("userID", userID))
	return nil
}

func (h *Handler) parseChunkSize(r *http.Request) (bool, int, int, error) {
	// Parse chunk size. Use default if not provided or unparsable.
	chunked := r.FormValue("chunked") == "true"
	chunkSize := DefaultChunkSize

	if chunked {
		if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
			chunkSize = int(n)
			if chunkSize > MaxChunkSize {
				msg := fmt.Sprintf("request chunk_size:%v larger than max chunk_size(%v)", n, MaxChunkSize)
				h.Logger.Error(msg, zap.String("db", r.FormValue("db")), zap.Any("r", r))
				return false, 0, 0, fmt.Errorf(msg)
			}
		}
	}

	innerChunkSize := DefaultInnerChunkSize
	if n, err := strconv.ParseInt(r.FormValue("inner_chunk_size"), 10, 64); err == nil && int(n) > 0 {
		if n <= MaxInnerChunkSize {
			innerChunkSize = int(n)
		}
	}
	return chunked, chunkSize, innerChunkSize, nil
}

func (h *Handler) getSqlQuery(r *http.Request, qr io.Reader) (*influxql.Query, error, int) {
	p := influxql.NewParser(qr)
	defer p.Release()

	// Sanitize the request query params so it doesn't show up in the response logger.
	// Do this before anything else so a parsing error doesn't leak passwords.
	sanitize(r)

	// Parse the parameters
	params, err := h.parseQueryParams(r)
	if err != nil {
		return nil, err, http.StatusBadRequest
	}
	if params != nil {
		p.SetParams(params)
	}

	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	q, err := YyParser.GetQuery()
	if err != nil {
		h.Logger.Error("query error! parsing query value:", zap.Error(err), zap.String("db", r.FormValue("db")), zap.Any("r", r))
		return nil, fmt.Errorf("error parsing query: " + err.Error()), http.StatusBadRequest
	}

	return q, nil, http.StatusOK
}

func (h *Handler) getAuthorizer(user meta2.User) query2.FineAuthorizer {
	if h.Config.AuthEnabled {
		if user != nil && user.AuthorizeUnrestricted() {
			return query2.OpenAuthorizer
		} else {
			// The current user determines the authorized actions.
			return user
		}
	} else {
		// Auth is disabled, so allow everything.
		return query2.OpenAuthorizer
	}
}

func (h *Handler) getResultRowsCnt(r *query.Result, rows int) int {
	// Limit the number of rows that can be returned in a non-chunked
	// response.  This is to prevent the server from going OOM when
	// returning a large response.  If you want to return more than the
	// default chunk size, then use chunking to process multiple blobs.
	// Iterate through the series in this result to count the rows and
	// truncate any rows we shouldn't return.
	if h.Config.MaxRowLimit <= 0 {
		return 0
	}
	for i, series := range r.Series {
		n := h.Config.MaxRowLimit - rows
		if n < len(series.Values) {
			// We have reached the maximum number of values. Truncate
			// the values within this row.
			series.Values = series.Values[:n]
			// Since this was truncated, it will always be a partial return.
			// Add this so the client knows we truncated the response.
			series.Partial = true
		}
		rows += len(series.Values)

		if rows >= h.Config.MaxRowLimit {
			// Drop any remaining series since we have already reached the row limit.
			if i < len(r.Series) {
				r.Series = r.Series[:i+1]
			}
			break
		}
	}
	return rows
}

func (h *Handler) updateStmtId2Result(r *query.Result, stmtID2Result map[int]*query.Result) bool {
	// It's not chunked so buffer results in memory.
	// Results for statements need to be combined together.
	// We need to check if this new result is for the same statement as
	// the last result, or for the next statement
	if result, ok := stmtID2Result[r.StatementID]; ok {
		if r.Err != nil {
			stmtID2Result[r.StatementID] = r
			return false
		}

		cr := result
		rowsMerged := 0
		if len(cr.Series) > 0 {
			lastSeries := cr.Series[len(cr.Series)-1]

			for _, row := range r.Series {
				if !lastSeries.SameSeries(row) {
					// Next row is for a different series than last.
					break
				}
				// Values are for the same series, so append them.
				lastSeries.Values = append(lastSeries.Values, row.Values...)
				lastSeries.Partial = row.Partial
				rowsMerged++
			}
		}

		// Append remaining rows as new rows.
		r.Series = r.Series[rowsMerged:]
		cr.Series = append(cr.Series, r.Series...)
		cr.Messages = append(cr.Messages, r.Messages...)
		cr.Partial = r.Partial
	} else {
		stmtID2Result[r.StatementID] = r
	}

	return true
}

func (h *Handler) getStmtResult(stmtID2Result map[int]*query.Result) Response {
	resp := Response{Results: make([]*query.Result, 0, len(stmtID2Result))}
	var keys []int
	for k := range stmtID2Result {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, k := range keys {
		resp.Results = append(resp.Results, stmtID2Result[k])
	}
	return resp
}

func transformRequestParams(r *http.Request) {
	q := r.URL.Query()
	q.Add(Repository, r.FormValue("db"))
	q.Add(LogStream, r.FormValue("measurement"))
	q.Add(Sql, "true")
	q.Add("query", r.FormValue("q"))
	r.URL.RawQuery = q.Encode()
	r.Form = nil
}

// rewritePipeStateForQuery is used to generate selectStmt based on the pipe state.
func (h *Handler) rewritePipeStateForQuery(q *influxql.Query, param *QueryParam, r *http.Request) {
	var selectStmt *influxql.SelectStatement
	if stmt, currOk := q.Statements[0].(*influxql.ExplainStatement); currOk {
		selectStmt = stmt.Statement
	} else {
		selectStmt, _ = q.Statements[0].(*influxql.SelectStatement)
	}
	selectStmt.RewriteUnnestSource()
	selectStmt.Sources = influxql.Sources{&influxql.Measurement{Name: r.URL.Query().Get(":logStream"), Database: r.URL.Query().Get(":repository"), RetentionPolicy: r.URL.Query().Get(":logStream")}}
	if param != nil {
		selectStmt.Limit = param.Limit
		timeCond := &influxql.BinaryExpr{
			LHS: &influxql.BinaryExpr{
				LHS: &influxql.VarRef{Val: "time", Type: influxql.Time},
				Op:  influxql.GTE,
				RHS: &influxql.IntegerLiteral{Val: param.TimeRange.start},
			},
			Op: influxql.AND,
			RHS: &influxql.BinaryExpr{
				LHS: &influxql.VarRef{Val: "time", Type: influxql.Time},
				Op:  influxql.LT,
				RHS: &influxql.IntegerLiteral{Val: param.TimeRange.end},
			},
		}
		if selectStmt.Condition == nil {
			selectStmt.Condition = timeCond
		} else {
			selectStmt.Condition = &influxql.BinaryExpr{
				LHS: selectStmt.Condition,
				Op:  influxql.AND,
				RHS: timeCond,
			}
		}
	}
}

func (h *Handler) buildLogQueryParam(r *http.Request) (*QueryParam, error) {
	transformRequestParams(r)
	queryLogRequest, err := getQueryLogRequest(r)
	if err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.String("request params", r.URL.RawQuery))
		return nil, err
	}

	para := NewQueryPara(queryLogRequest.Query, queryLogRequest.Reverse, queryLogRequest.Highlight,
		queryLogRequest.Timeout, queryLogRequest.Limit, queryLogRequest.From*1e6, queryLogRequest.To*1e6,
		queryLogRequest.Scroll, queryLogRequest.Scroll_id, false, queryLogRequest.Explain, queryLogRequest.IsTruncate)

	return para, nil
}

func (h *Handler) parsePipeAndSqlForQuery(r *http.Request, user meta2.User) (*influxql.Query, int, error) {
	para, err := h.buildLogQueryParam(r)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	qp := h.getQueryFromRequest(r, para, user)
	if qp == "" {
		return nil, http.StatusBadRequest, fmt.Errorf(`missing required parameter "q"`)
	}
	ppl, sql := getPplAndSqlFromQuery(qp)
	// sql parser
	var sqlQuery *influxql.Query
	var status int
	if sql != "" {
		sqlQuery, err, status = h.getSqlQuery(r, strings.NewReader(sql))
		if err != nil {
			return nil, status, err
		}
	} else {
		stmt := generateDefaultStatement()
		sqlQuery = &influxql.Query{Statements: influxql.Statements{stmt}}
	}
	// ppl parser
	if ppl != "" {
		_, err, status := h.getPplQuery(r, strings.NewReader(ppl), sqlQuery)
		if err != nil {
			return nil, status, err
		}
	}

	h.rewritePipeStateForQuery(sqlQuery, para, r)
	return sqlQuery, http.StatusOK, nil
}

// serveQuery parses an incoming query and, if valid, executes the query
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user meta2.User) {
	atomic.AddInt64(&statistics.HandlerStat.QueryRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, 1)
	start := time.Now()
	defer func() {
		atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.QueryRequestDuration, time.Since(start).Nanoseconds())
	}()
	h.requestTracker.Add(r, user)

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	if syscontrol.DisableReads {
		h.httpError(rw, `disable read!`, http.StatusForbidden)
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	var q *influxql.Query
	var err error
	var status int
	isPipe := r.FormValue("pipe") == "true"
	if isPipe {
		q, _, err = h.parsePipeAndSqlForQuery(r, user)
		if err != nil {
			h.httpError(rw, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		// new reader for sql statement
		qr, f, err := h.newQueryReader(r, nil, user)
		if err != nil {
			h.httpError(rw, err.Error(), http.StatusBadRequest)
			return
		}
		if f != nil {
			defer util.MustClose(f)
		}
		q, err, status = h.getSqlQuery(r, qr)
		if err != nil {
			h.httpError(rw, err.Error(), status)
			return
		}
	}

	epoch := strings.TrimSpace(r.FormValue("epoch"))

	db := r.FormValue("db")
	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(db) {
		qDuration = statistics.NewSqlSlowQueryStatistics(db)
		defer func() {
			d := time.Now().Sub(start)
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
				h.Logger.Info("slow query", zap.Duration("duration", d), zap.String("db", qDuration.DB),
					zap.String("query", qDuration.Query))
			}
		}()
	}

	// Check authorization.
	err = h.checkAuthorization(user, q, db)
	if err != nil {
		h.httpError(rw, "error authorizing query: "+err.Error(), http.StatusForbidden)
		return
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		h.httpError(rw, err.Error(), http.StatusBadRequest)
	}
	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query2.ExecutionOptions{
		Database:        db,
		RetentionPolicy: r.FormValue("rp"),
		ChunkSize:       chunkSize,
		Chunked:         chunked,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		InnerChunkSize:  innerChunkSize,
		ParallelQuery:   atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		Quiet:           true,
		Authorizer:      h.getAuthorizer(user),
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		done := make(chan struct{})

		opts.AbortCh = closing
		defer func() {
			close(done)
		}()
		go func() {
			select {
			case <-done:
			case <-r.Context().Done():
			}
			close(closing)
		}()
	}

	// Execute query
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	// If we are running in async mode, open a goroutine to drain the results
	// and return with a StatusNoContent.
	if async {
		go h.async(q, results)
		h.writeHeader(w, http.StatusNoContent)
		return
	}

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	stmtID2Result := make(map[int]*query.Result)

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	if !isPipe {
		h.writeHeader(rw, http.StatusOK)
		if w, ok := w.(http.Flusher); ok {
			w.Flush()
		}
	}

	// pull all results from the channel
	rows := 0
	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		if isPipe && r.Err != nil {
			h.httpError(rw, r.Err.Error(), http.StatusBadRequest)
			return
		}

		// if requested, convert result timestamps to epoch
		if epoch != "rfc3339" {
			convertToEpoch(r, epoch)
		}

		// Write out result immediately if chunked.
		if chunked {
			n, _ := rw.WriteResponse(Response{
				Results: []*query.Result{r},
			})
			atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
			w.(http.Flusher).Flush()
			continue
		}

		rows = h.getResultRowsCnt(r, rows)
		if !h.updateStmtId2Result(r, stmtID2Result) {
			continue
		}

		// Drop out of this loop and do not process further results when we hit the row limit.
		if h.Config.MaxRowLimit > 0 && rows >= h.Config.MaxRowLimit {
			// If the result is marked as partial, remove that partial marking
			// here. While the series is partial and we would normally have
			// tried to return the rest in the next chunk, we are not using
			// chunking and are truncating the series so we don't want to
			// signal to the client that we plan on sending another JSON blob
			// with another result.  The series, on the other hand, still
			// returns partial true if it was truncated or had more data to
			// send in a future chunk.
			r.Partial = false
			break
		}
	}

	resp := h.getStmtResult(stmtID2Result)
	// If it's not chunked we buffered everything in memory, so write it out
	if !chunked {
		n, _ := rw.WriteResponse(resp)
		atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
	}
}

// async drains the results from an async query and logs a message if it fails.
func (h *Handler) async(q *influxql.Query, results <-chan *query.Result) {
	for r := range results {
		// Drain the results and do nothing with them.
		// If it fails, log the failure so there is at least a record of it.
		if r.Err != nil {
			// Do not log when a statement was not executed since there would
			// have been an earlier error that was already logged.
			if r.Err == query2.ErrNotExecuted {
				continue
			}
			h.Logger.Info("Error while running async query",
				zap.Stringer("query", q),
				zap.Error(r.Err))
		}
	}
}

func (h *Handler) logRowsIfNecessary(rows []influx.Row, ReqBuf []byte) {
	syscontrol.MuLogRowsRule.RLock()
	defer syscontrol.MuLogRowsRule.RUnlock()
	var isLog bool
	if len(syscontrol.MyLogRowsRule.Tags) == 0 {
		for _, row := range rows {
			if row.Name == syscontrol.MyLogRowsRule.Mst {
				isLog = true
				h.Logger.Info("log rows", zap.Any("point", row))
			}
		}
	} else {
		for _, row := range rows {
			if row.Name == syscontrol.MyLogRowsRule.Mst {
				hit := 0
				for _, tag := range row.Tags {
					v := syscontrol.MyLogRowsRule.Tags[tag.Key]
					if v == tag.Value {
						hit++
					}
				}
				if hit == len(syscontrol.MyLogRowsRule.Tags) {
					isLog = true
					h.Logger.Info("log rows", zap.Any("point", row))
				}
			}
		}
	}
	if isLog {
		h.Logger.Info("log ReqBuf", zap.ByteString("req_buf", ReqBuf))
	}
}

// serveWrite receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user meta2.User) {
	atomic.AddInt64(&statistics.HandlerStat.WriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.WriteRequestBytesIn, r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.WriteRequestDuration, d)
	}(time.Now())
	h.requestTracker.Add(r, user)

	if syscontrol.DisableWrites {
		h.httpError(w, `disable write!`, http.StatusForbidden)
		h.Logger.Error("write is forbidden!", zap.Bool("DisableWrites", syscontrol.DisableWrites))
		return
	}

	if syscontrol.IsReadonly() {
		h.httpError(w, "readonly now and writing is not allowed", http.StatusBadRequest)
		return
	}

	urlValues := r.URL.Query()
	database := urlValues.Get("db")
	if database == "" {
		err := errno.NewError(errno.HttpDatabaseNotFound)
		h.Logger.Error("serveWrite", zap.Error(err))
		h.httpError(w, "database is required", http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	if _, err := h.MetaClient.Database(database); err != nil {
		err := errno.NewError(errno.HttpDatabaseNotFound)
		h.Logger.Error("serveWrite", zap.Error(err), zap.String("db", database))
		h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			err := errno.NewError(errno.HttpForbidden)
			h.Logger.Error("write error: user is required to write to database", zap.Error(err), zap.String("db", database))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			err := errno.NewError(errno.HttpForbidden)
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			h.Logger.Error("write error:user is not authorized to write to database", zap.Error(err), zap.String("db", database), zap.String("user", user.ID()))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := GetGzipReader(r.Body)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			error := errno.NewError(errno.HttpBadRequest)
			h.Logger.Error("write error:Handle gzip decoding of the body err", zap.Error(error), zap.String("db", database))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
		defer PutGzipReader(b)
		body = b
	}

	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			err := errno.NewError(errno.HttpRequestEntityTooLarge)
			h.Logger.Error("serveWrite", zap.Int64("ContentLength", r.ContentLength), zap.Error(err), zap.String("db", database))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	}

	precision := urlValues.Get("precision")

	tsMultiplier := int64(1)
	switch precision {
	case "ns":
		tsMultiplier = 1
	case "u", "us", "µ":
		tsMultiplier = 1e3
	case "ms":
		tsMultiplier = 1e6
	case "s":
		tsMultiplier = 1e9
	case "m":
		tsMultiplier = 1e9 * 60
	case "h":
		tsMultiplier = 1e9 * 3600
	}

	ctx := influx.GetStreamContext(body)
	defer influx.PutStreamContext(ctx)

	var numPtsParse, numPtsInsert int

	readBlockSize := int(h.Config.ReadBlockSize)
	rp := urlValues.Get("rp")
	for ctx.Read(readBlockSize) {
		numPtsParse++
		uw := influx.GetUnmarshalWork()
		uw.Callback = func(db string, rows []influx.Row, err error) {
			if err != nil {
				ctx.ErrLock.Lock()
				ctx.UnmarshalErr = err
				ctx.ErrLock.Unlock()
				ctx.Wg.Done()
				return
			}
			if atomic.LoadInt32(&syscontrol.LogRowsRuleSwitch) == 1 {
				h.logRowsIfNecessary(rows, uw.ReqBuf)
			}
			if err = h.PointsWriter.RetryWritePointRows(db, rp, rows); err != nil {
				ctx.ErrLock.Lock()
				if ctx.CallbackErr == nil {
					ctx.CallbackErr = err
				}
				ctx.ErrLock.Unlock()
			} else {
				if h.SubscriberManager != nil {
					// uw.ReqBuf is the line protocol
					h.SubscriberManager.Send(db, rp, uw.ReqBuf)
				}
				atomic.AddInt64(&statistics.HandlerStat.PointsWrittenOK, int64(len(rows)))
			}
			ctx.Wg.Done()
		}
		uw.TsMultiplier = tsMultiplier
		uw.Db = database
		uw.ReqBuf, ctx.ReqBuf = ctx.ReqBuf, uw.ReqBuf
		uw.EnableTagArray = h.MetaClient.TagArrayEnabled(database)
		atomic.AddInt64(&statistics.HandlerStat.WriteRequestBytesReceived, int64(len(uw.ReqBuf)))

		ctx.Wg.Add(1)
		start := time.Now()
		influx.ScheduleUnmarshalWork(uw)
		atomic.AddInt64(&statistics.HandlerStat.WriteScheduleUnMarshalDns, time.Since(start).Nanoseconds())
		numPtsInsert++
	}
	ctx.Wg.Wait()
	if err := ctx.Error(); err != nil {
		h.Logger.Error("write error:read body ", zap.Error(err), zap.String("db", database))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	if err := ctx.UnmarshalErr; err != nil {
		atomic.AddInt64(&statistics.HandlerStat.PointsWrittenFail, int64(numPtsInsert))
		h.Logger.Error("write client error, unmarshal points failed", zap.Error(err), zap.String("db", database))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	if err := ctx.CallbackErr; err != nil {
		if influxdb.IsClientError(err) {
			atomic.AddInt64(&statistics.HandlerStat.PointsWrittenFail, int64(numPtsInsert))
			h.Logger.Error("write client error:WritePointsWithContext", zap.Error(err), zap.String("db", database))
			h.httpError(w, err.Error(), http.StatusBadRequest)
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		} else if influxdb.IsAuthorizationError(err) {
			atomic.AddInt64(&statistics.HandlerStat.PointsWrittenFail, int64(numPtsParse))
			h.httpError(w, err.Error(), http.StatusForbidden)
			h.Logger.Error("write authorization error:WritePointsWithContext", zap.Error(err), zap.String("db", database))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		} else if werr, ok := err.(netstorage.PartialWriteError); ok {
			atomic.AddInt64(&statistics.HandlerStat.PointsWrittenOK, int64(numPtsInsert-werr.Dropped))
			atomic.AddInt64(&statistics.HandlerStat.PointsWrittenDropped, int64(werr.Dropped))
			h.httpError(w, werr.Error(), http.StatusBadRequest)
			h.Logger.Error("write Partial Write error:WritePointsWithContext", zap.Error(werr.Reason), zap.String("db", database))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		} else if errno.Equal(err, errno.MeasurementNameTooLong) {
			atomic.AddInt64(&statistics.HandlerStat.PointsWrittenFail, int64(numPtsParse))
			h.httpError(w, werr.Error(), http.StatusBadRequest)
			h.Logger.Error("write error:WritePointsWithContext", zap.Error(werr.Reason), zap.String("db", database))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		} else if err != nil {
			atomic.AddInt64(&statistics.HandlerStat.PointsWrittenFail, int64(numPtsInsert))
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			h.Logger.Error("write error:WritePointsWithContext", zap.Error(err), zap.String("db", database))
			atomic.AddInt64(&statistics.HandlerStat.Write500ErrRequests, 1)
			return
		}
	}

	h.writeHeader(w, http.StatusNoContent)
}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (h *Handler) serveOptions(w http.ResponseWriter, r *http.Request) {
	h.writeHeader(w, http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	verbose := r.URL.Query().Get("verbose")
	atomic.AddInt64(&statistics.HandlerStat.PingRequests, 1)

	if verbose != "" && verbose != "0" && verbose != "false" {
		h.writeHeader(w, http.StatusOK)
		b, _ := json.Marshal(map[string]string{"version": h.Version})
		w.Write(b)
	} else {
		h.writeHeader(w, http.StatusNoContent)
	}
}

// serveStatus has been deprecated.
func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	h.Logger.Info("WARNING: /status has been deprecated.  Use /ping instead.")
	atomic.AddInt64(&statistics.HandlerStat.StatusRequests, 1)
	h.writeHeader(w, http.StatusNoContent)
}

func (h *Handler) failPoint(w http.ResponseWriter, r *http.Request) {
	point := r.URL.Query().Get("point")
	flag := r.URL.Query().Get("flag")
	var err error
	if flag == "enable" {
		term := strings.TrimSpace(r.FormValue("term"))
		err = failpoint.Enable(point, term)
		if err != nil {
			h.Logger.Error("enable failpoint fail", zap.String("point", point), zap.String("term", term), zap.Error(err))
		} else {
			h.Logger.Info("enable failpoint success", zap.String("point", point), zap.String("term", term))
		}
		var req netstorage.SysCtrlRequest
		req.SetMod(syscontrol.Failpoint)
		req.SetParam(map[string]string{
			"point":    point,
			"switchon": "true",
			"term":     term,
		})

		var sb strings.Builder
		sb.WriteString("{\n\t")
		err = syscontrol.ProcessRequest(req, &sb)
		sb.WriteString("\n}\n")
		w.Write([]byte(sb.String()))
	} else if flag == "disable" {
		err = failpoint.Disable(point)
		if err != nil {
			h.Logger.Error("disable failpoint fail", zap.String("point", point), zap.Error(err))
		} else {
			h.Logger.Info("disable failpoint success", zap.String("point", point))
		}
		var req netstorage.SysCtrlRequest
		req.SetMod(syscontrol.Failpoint)
		req.SetParam(map[string]string{
			"point":    point,
			"switchon": "false",
		})
		var sb strings.Builder
		sb.WriteString("{\n\t")
		err = syscontrol.ProcessRequest(req, &sb)
		sb.WriteString("\n}\n")
		w.Write([]byte(sb.String()))
	} else {
		h.Logger.Error("invalid failpoint args", zap.String("flag", flag))
		err = fmt.Errorf("invalid failpoint args: flag: %s. Optional for enable or disable", flag)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
	}
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
func convertToEpoch(r *query.Result, epoch string) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	for _, s := range r.Series {
		for _, v := range s.Values {
			if ts, ok := v[0].(time.Time); ok {
				v[0] = ts.UnixNano() / divisor
			}
		}
	}
}

// servePromWrite receives data in the Prometheus remote write protocol and writes it
// to the database
func (h *Handler) servePromWrite(w http.ResponseWriter, r *http.Request, user meta2.User) {
	atomic.AddInt64(&statistics.HandlerStat.WriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.WriteRequestBytesIn, r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.WriteRequestDuration, d)
	}(time.Now())
	h.requestTracker.Add(r, user)

	if syscontrol.DisableWrites {
		h.httpError(w, `disable write!`, http.StatusForbidden)
		h.Logger.Error("write is forbidden!", zap.Bool("DisableWrites", syscontrol.DisableWrites))
		return
	}

	urlValues := r.URL.Query()
	database := urlValues.Get("db")
	if database == "" {
		h.httpError(w, "database is required", http.StatusBadRequest)
		return
	}

	if _, err := h.MetaClient.Database(database); err != nil {
		h.httpError(w, fmt.Sprintf(err.Error()), http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.Config.WriteTracing {
			h.Logger.Info("Prom write handler unable to read bytes from request body")
		}
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if h.Config.WriteTracing {
		h.Logger.Info("Prom write body received by handler", zap.ByteString("body", buf.Bytes()))
	}

	reqBuf, err := snappy.Decode(nil, buf.Bytes())
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Convert the Prometheus remote write request to Influx Points
	var req prompb.WriteRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	points, err := prometheus.WriteRequestToPoints(&req)
	if err != nil {
		if h.Config.WriteTracing {
			h.Logger.Info("Prom write handler", zap.Error(err))
		}

		// Check if the error was from something other than dropping invalid values.
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	rows, e := Points2Rows(points)
	if e != nil {
		h.Logger.Info("points transfer wrong", zap.Error(e))
	}

	// Determine required consistency level.
	level := urlValues.Get("consistency")
	if level != "" {
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.RetryWritePointRows(database, urlValues.Get("rp"), rows); influxdb.IsClientError(err) {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else if influxdb.IsAuthorizationError(err) {
		h.httpError(w, err.Error(), http.StatusForbidden)
		return
	} else if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeHeader(w, http.StatusNoContent)
}

// servePromRead will convert a Prometheus remote read request into a storage
// query and returns data in Prometheus remote read protobuf format.
func (h *Handler) servePromRead(w http.ResponseWriter, r *http.Request, user meta2.User) {
	if syscontrol.DisableReads {
		h.httpError(w, `disable read!`, http.StatusForbidden)
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return
	}
	//h.httpError(w, "not implementation", http.StatusBadRequest)
	startTime := time.Now()
	h.requestTracker.Add(r, user)
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Query the DB and create a ReadResponse for Prometheus
	db := r.FormValue("db")

	queries, err := ReadRequestToInfluxQuery(&req)
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
	}
	YyParser.Scanner = influxql.NewScanner(strings.NewReader(queries))
	YyParser.ParseTokens()
	q, err := YyParser.GetQuery()

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if h.Config.AuthEnabled {
		//&& h.Config.PromReadAuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to read from database %q", db), http.StatusForbidden)
			return
		}
		if h.QueryAuthorizer.AuthorizeQuery(user, q, db) != nil {
			h.httpError(w, fmt.Sprintf("user %q is not authorized to read from database %q", user.ID(), db), http.StatusForbidden)
			return
		}
	}

	//todo: change here
	//readRequest, err := prometheus.ReadRequestToInfluxStorageRequest(&req, db, rp)

	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(db) {
		qDuration = statistics.NewSqlSlowQueryStatistics(db)
		defer func() {
			d := time.Since(startTime)
			if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d.Nanoseconds())
				statistics.AppendSqlQueryDuration(qDuration)
				h.Logger.Info("slow query", zap.Duration("duration", d), zap.String("db", qDuration.DB),
					zap.String("query", qDuration.Query))
			}
		}()
	}

	respond := func(resp *prompb.ReadResponse) {
		data, err := resp.Marshal()
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	//ctx := context.Background()

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query2.ExecutionOptions{
		Database:        db,
		RetentionPolicy: r.FormValue("rp"),
		ChunkSize:       1,
		Chunked:         true,
		ReadOnly:        r.Method == "GET",
		InnerChunkSize:  1,
		//ParallelQuery:   atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		//QueryLimitEn:    atomic.LoadInt32(&syscontrol.QueryLimitEn) == 1,
		Quiet: true,
	}

	if h.Config.AuthEnabled {
		if user != nil && user.AuthorizeUnrestricted() {
			opts.Authorizer = query2.OpenAuthorizer
		} else {
			// The current user determines the authorized actions.
			opts.Authorizer = user
		}
	} else {
		// Auth is disabled, so allow everything.
		opts.Authorizer = query2.OpenAuthorizer
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		done := make(chan struct{})

		opts.AbortCh = closing
		defer func() {
			close(done)
		}()
		go func() {
			select {
			case <-done:
			case <-r.Context().Done():
			}
			close(closing)
		}()
	}

	// Execute query
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}

	if results == nil {
		respond(resp)
		return
	}

	var unsupportedCursor string

	var tags models.Tags

	sameTag := false

	for r := range results {
		for i := range r.Series {
			s := r.Series[i]
			var series *prompb.TimeSeries
			if sameTag {
				series = resp.Results[0].Timeseries[len(resp.Results[0].Timeseries)-1]
			} else {
				tags = TagsConverterRemoveInfluxSystemTag(s.Tags)
				// We have some data for this series.
				series = &prompb.TimeSeries{
					Labels:  prometheus.ModelTagsToLabelPairs(tags),
					Samples: make([]prompb.Sample, 0, len(r.Series)),
				}
			}
			start := len(series.Samples)
			series.Samples = append(series.Samples, make([]prompb.Sample, len(s.Values))...)

			for j := range s.Values {
				sample := &series.Samples[start+j]
				if t, ok := s.Values[j][0].(time.Time); !ok {
					h.httpError(w, "wrong time datatype, should be time.Time", http.StatusBadRequest)
					return
				} else {
					sample.Timestamp = t.UnixNano() / int64(time.Millisecond)
				}
				if value, ok := s.Values[j][len(s.Values[j])-1].(float64); !ok {
					h.httpError(w, "wrong value datatype, should be float64", http.StatusBadRequest)
					return
				} else {
					sample.Value = value
				}

			}
			// There was data for the series.
			if !sameTag {
				resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, series)
			}

			if len(unsupportedCursor) > 0 {
				h.Logger.Info("Prometheus can't read data",
					zap.String("cursor_type", unsupportedCursor),
					zap.Stringer("series", tags),
				)
			}
			sameTag = s.Partial
		}
	}
	h.Logger.Info("serve prometheus read", zap.String("SQL:", q.String()), zap.Duration("prometheus query duration:", time.Since(startTime)))
	respond(resp)
}

func (h *Handler) serveFluxQuery(w http.ResponseWriter, r *http.Request, user meta2.User) {
	if syscontrol.DisableReads {
		h.httpError(w, `disable read!`, http.StatusForbidden)
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return
	}
	h.httpError(w, "not implementation", http.StatusBadRequest)

}

// serveExpvar serves internal metrics in /debug/vars format over HTTP.
func (h *Handler) serveExpvar(w http.ResponseWriter, r *http.Request) {
	app.SetStatsResponse(h.StatisticsPusher, w, r)
}

// serveDebugRequests will track requests for a period of time.
func (h *Handler) serveDebugRequests(w http.ResponseWriter, r *http.Request) {
	var d time.Duration
	if s := r.URL.Query().Get("seconds"); s == "" {
		d = DefaultDebugRequestsInterval
	} else if seconds, err := strconv.ParseInt(s, 10, 64); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else {
		d = time.Duration(seconds) * time.Second
		if d > MaxDebugRequestsInterval {
			h.httpError(w, fmt.Sprintf("exceeded maximum interval time: %s > %s",
				influxql.FormatDuration(d),
				influxql.FormatDuration(MaxDebugRequestsInterval)),
				http.StatusBadRequest)
			return
		}
	}

	var closing <-chan bool
	if notifier, ok := w.(http.CloseNotifier); ok {
		closing = notifier.CloseNotify()
	}

	profile := h.requestTracker.TrackRequests()

	timer := time.NewTimer(d)
	select {
	case <-timer.C:
		profile.Stop()
	case <-closing:
		// Connection was closed early.
		profile.Stop()
		timer.Stop()
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Add("Connection", "close")

	fmt.Fprintln(w, "{")
	first := true
	for req, st := range profile.Requests {
		val, err := json.Marshal(st)
		if err != nil {
			continue
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q: ", req.String())
		w.Write(bytes.TrimSpace(val))
	}
	fmt.Fprintln(w, "\n}")
}

// httpError writes an error to the client in a standard format.
func (h *Handler) httpError(w http.ResponseWriter, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", h.Config.Realm))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	response := Response{Err: errors.New(errmsg)}
	if rw, ok := w.(ResponseWriter); ok {
		h.writeHeader(w, code)
		rw.WriteResponse(response)
		return
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	b, _ := json.Marshal(response)
	w.Write(b)
}

// Filters and filter helpers

type credentials struct {
	Method   AuthenticationMethod
	Username string
	Password string
	Token    string
}

func parseToken(token string) (user, pass string, ok bool) {
	s := strings.IndexByte(token, ':')
	if s < 0 {
		return
	}
	return token[:s], token[s+1:], true
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=password
// As basic auth: http://username:password@127.0.0.1
// As Bearer token in Authorization header: Bearer <JWT_TOKEN_BLOB>
// As Token in Authorization header: Token <username:password>
func ParseCredentials(r *http.Request) (*credentials, error) {
	q := r.URL.Query()

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return &credentials{
			Method:   UserAuthentication,
			Username: u,
			Password: p,
		}, nil
	}

	// Check for the HTTP Authorization header.
	if s := r.Header.Get("Authorization"); s != "" {
		// Check for Bearer token.
		strs := strings.Split(s, " ")
		if len(strs) == 2 {
			switch strs[0] {
			case "Bearer":
				return &credentials{
					Method: BearerAuthentication,
					Token:  strs[1],
				}, nil
			case "Token":
				if u, p, ok := parseToken(strs[1]); ok {
					return &credentials{
						Method:   UserAuthentication,
						Username: u,
						Password: p,
					}, nil
				}
			}
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return &credentials{
				Method:   UserAuthentication,
				Username: u,
				Password: p,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to parse authentication credentials")
}

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func authenticate(inner func(http.ResponseWriter, *http.Request, meta2.User), h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !requireAuthentication {
			inner(w, r, nil)
			return
		}
		var user meta2.User

		// TODO corylanou: never allow this in the future without users
		if requireAuthentication && h.MetaClient.AdminUserExists() {
			creds, err := ParseCredentials(r)
			if err != nil {
				atomic.AddInt64(&statistics.HandlerStat.AuthenticationFailures, 1)
				h.httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}

			switch creds.Method {
			case UserAuthentication:
				if creds.Username == "" {
					atomic.AddInt64(&statistics.HandlerStat.AuthenticationFailures, 1)
					errMsg := "username required"
					err := errno.NewError(errno.HttpUnauthorized)
					log := logger.NewLogger(errno.ModuleHTTP)
					log.Error(errMsg, zap.Error(err))
					h.httpError(w, errMsg, http.StatusUnauthorized)
					return
				}

				user, err = h.MetaClient.Authenticate(creds.Username, creds.Password)
				if err != nil {
					atomic.AddInt64(&statistics.HandlerStat.AuthenticationFailures, 1)
					errMsg := "authorization failed"
					if err == meta2.ErrUserLocked {
						errMsg = err.Error()
					}
					err := errno.NewError(errno.HttpUnauthorized)
					log := logger.NewLogger(errno.ModuleHTTP)
					log.Error(errMsg, zap.Error(err))
					h.httpError(w, errMsg, http.StatusUnauthorized)
					return
				}
			case BearerAuthentication:
				if h.Config.SharedSecret == "" {
					atomic.AddInt64(&statistics.HandlerStat.AuthenticationFailures, 1)
					h.httpError(w, ErrBearerAuthDisabled.Error(), http.StatusUnauthorized)
					return
				}
				keyLookupFn := func(token *jwt.Token) (interface{}, error) {
					// Check for expected signing method.
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
					}
					return []byte(h.Config.SharedSecret), nil
				}

				// Parse and validate the token.
				token, err := jwt.Parse(creds.Token, keyLookupFn)
				if err != nil {
					h.httpError(w, err.Error(), http.StatusUnauthorized)
					return
				} else if !token.Valid {
					h.httpError(w, "invalid token", http.StatusUnauthorized)
					return
				}

				claims, ok := token.Claims.(jwt.MapClaims)
				if !ok {
					h.httpError(w, "problem authenticating token", http.StatusInternalServerError)
					h.Logger.Info("Could not assert JWT token claims as jwt.MapClaims")
					return
				}

				// Make sure an expiration was set on the token.
				if exp, ok := claims["exp"].(float64); !ok || exp <= 0.0 {
					h.httpError(w, "token expiration required", http.StatusUnauthorized)
					return
				}

				// Get the username from the token.
				username, ok := claims["username"].(string)
				if !ok {
					h.httpError(w, "username in token must be a string", http.StatusUnauthorized)
					return
				} else if username == "" {
					h.httpError(w, "token must contain a username", http.StatusUnauthorized)
					return
				}

				// Lookup user in the metastore.
				if user, err = h.MetaClient.User(username); err != nil {
					h.httpError(w, err.Error(), http.StatusUnauthorized)
					return
				} else if user == nil {
					h.httpError(w, meta2.ErrUserNotFound.Error(), http.StatusUnauthorized)
					return
				}
			default:
				h.httpError(w, "unsupported authentication", http.StatusUnauthorized)
			}

		}
		inner(w, r, user)
	})
}

// cors responds to incoming requests and adds the appropriate cors headers
// TODO: corylanou: add the ability to configure this in our config
func cors(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
			}, ", "))

			w.Header().Set(`Access-Control-Expose-Headers`, strings.Join([]string{
				`Date`,
				`X-InfluxDB-Version`,
				`X-InfluxDB-Build`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

func requestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// X-Request-Id takes priority.
		rid := r.Header.Get("X-Request-Id")

		// If X-Request-Id is empty, then check Request-Id
		if rid == "" {
			rid = r.Header.Get("Request-Id")
		}

		// If Request-Id is empty then generate a v1 UUID.
		if rid == "" {
			rid = uuid.TimeUUID().String()
		}

		// We read Request-Id in other handler code so we'll use that naming
		// convention from this point in the request cycle.
		r.Header.Set("Request-Id", rid)

		// Set the request ID on the response headers.
		// X-Request-Id is the most common name for a request ID header.
		w.Header().Set("X-Request-Id", rid)

		// We will also set Request-Id for backwards compatibility with previous
		// versions of InfluxDB.
		w.Header().Set("Request-Id", rid)

		inner.ServeHTTP(w, r)
	})
}

func (h *Handler) logging(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)

		if h.accessLogFilters.Match(l.Status()) {
			if handlerLogLimit >= 10 {
				h.Logger.Info(buildLogLine(l, r, start))
				handlerLogLimit = 0
			} else {
				handlerLogLimit++
			}
		}

		// Log server errors.
		if l.Status()/100 == 5 {
			errStr := l.Header().Get("X-InfluxDB-Error")
			if errStr != "" {
				h.Logger.Error(fmt.Sprintf("[%d] - %q", l.Status(), errStr))
			}
		}
	})
}

func (h *Handler) responseWriter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = NewResponseWriter(w, r)
		inner.ServeHTTP(w, r)
	})
}

// if the env var is set, and the value is truthy, then we will *not*
// recover from a panic.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(query2.PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

func (h *Handler) recovery(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s] %s", logLine, err, debug.Stack())
				h.Logger.Info(logLine)
				http.Error(w, http.StatusText(http.StatusInternalServerError), 500)
				atomic.AddInt64(&statistics.HandlerStat.RecoveredPanics, 1) // Capture the panic in _internal stats.

				if willCrash {
					h.Logger.Info("\n\n=====\nAll goroutines now follow:")
					buf := debug.Stack()
					h.Logger.Info(string(buf))
					os.Exit(1) // If we panic then the Go server will recover.
				}
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

// Response represents a list of statement results.
type Response struct {
	Results []*query.Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct.
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, rr := range r.Results {
		if rr.Err != nil {
			return rr.Err
		}
	}
	return nil
}

// Throttler represents an HTTP throttler that limits the number of concurrent
// requests being processed as well as the number of enqueued requests.
type Throttler struct {
	current  chan struct{}
	enqueued chan struct{}

	// Maximum amount of time requests can wait in queue.
	// Must be set before adding middleware.
	EnqueueTimeout time.Duration

	Logger *zap.Logger

	limiter *rate.Limiter

	ctx context.Context
}

// NewThrottler returns a new instance of Throttler that limits to concurrentN.
// requests processed at a time and maxEnqueueN requests waiting to be processed.
func NewThrottler(concurrentN, maxEnqueueN int, rateValue int) *Throttler {
	var limiter *rate.Limiter
	if rateValue > 0 {
		limiter = rate.NewLimiter(rate.Limit(rateValue), rateValue)
	}

	return &Throttler{
		current:  make(chan struct{}, concurrentN),
		enqueued: make(chan struct{}, concurrentN+maxEnqueueN),
		Logger:   zap.NewNop(),
		limiter:  limiter,
		ctx:      context.Background(),
	}
}

// Handler wraps h in a middleware handler that throttles requests.
func (t *Throttler) Handler(h http.Handler) http.Handler {
	timeout := t.EnqueueTimeout

	// Return original handler if concurrent requests is zero.
	if cap(t.current) == 0 {
		return h
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Start a timer to limit enqueued request times.
		var timerCh <-chan time.Time
		if timeout > 0 {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			timerCh = timer.C
		}

		// Wait for a spot in the queue.
		if cap(t.enqueued) > cap(t.current) {
			select {
			case t.enqueued <- struct{}{}:
				defer func() { <-t.enqueued }()
			default:
				t.Logger.Warn("request throttled, queue full", zap.Duration("d", timeout), zap.Int("enqueued length", len(t.enqueued)))
				http.Error(w, "request throttled, queue full", http.StatusServiceUnavailable)
				return
			}
		}

		// First check if we can immediately send in to current because there is
		// available capacity. This helps reduce racyness in tests.
		select {
		case t.current <- struct{}{}:
		default:
			// Wait for a spot in the list of concurrent requests, but allow checking the timeout.
			select {
			case t.current <- struct{}{}:
			case <-timerCh:
				t.Logger.Warn("request throttled, exceeds timeout", zap.Duration("d", timeout), zap.Int("current length", len(t.current)))
				http.Error(w, "request throttled, exceeds timeout", http.StatusServiceUnavailable)
				return
			}
		}
		defer func() { <-t.current }()

		// Execute request.
		h.ServeHTTP(w, r)
	})
}

func buildCommand(q *prompb.Query) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	// If we don't find a metric name matcher, query all metrics
	// (InfluxDB measurements) by default.
	from := "FROM /.+/"
	for _, m := range q.Matchers {
		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				from = fmt.Sprintf("FROM %q", m.Value)
			case prompb.LabelMatcher_RE:
				from = fmt.Sprintf("FROM /%s/", escapeSlashes(m.Value))
			default:
				// TODO: Figure out how to support these efficiently.
				return "", errors.New("non-equal or regex-non-equal matchers are not supported on the metric name yet")
			}
			continue
		}

		switch m.Type {
		case prompb.LabelMatcher_EQ:
			matchers = append(matchers, fmt.Sprintf("%q = '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_NEQ:
			matchers = append(matchers, fmt.Sprintf("%q != '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_RE:
			matchers = append(matchers, fmt.Sprintf("%q =~ /%s/", m.Name, escapeSlashes(m.Value)))
		case prompb.LabelMatcher_NRE:
			matchers = append(matchers, fmt.Sprintf("%q !~ /%s/", m.Name, escapeSlashes(m.Value)))
		default:
			return "", errors.New("unknown match type")
		}
	}
	matchers = append(matchers, fmt.Sprintf("time >= %vms", q.StartTimestampMs))
	matchers = append(matchers, fmt.Sprintf("time <= %vms", q.EndTimestampMs))

	return fmt.Sprintf("SELECT value %s WHERE %v GROUP BY *", from, strings.Join(matchers, " AND ")), nil
}

func escapeSlashes(str string) string {
	return strings.Replace(str, `/`, `\/`, -1)
}

func escapeSingleQuotes(str string) string {
	return strings.Replace(str, `'`, `\'`, -1)
}

func ReadRequestToInfluxQuery(req *prompb.ReadRequest) (string, error) {
	var readRequest string
	for _, q := range req.Queries {
		if s, err := buildCommand(q); err != nil {
			return "", err
		} else {
			readRequest += ";" + s
		}
	}
	if len(readRequest) != 0 {
		return readRequest[1:], nil
	}
	return "", nil
}

func TagsConverterRemoveInfluxSystemTag(tags map[string]string) models.Tags {
	var t models.Tags
	for k, v := range tags {
		if k == measurementTagKey || v == fieldTagKey {
			continue
		}
		tt := models.Tag{
			Key:   []byte(k),
			Value: []byte(v),
		}
		t = append(t, tt)
	}
	sort.Sort(t)
	return t
}

func Points2Rows(points []models.Point) ([]influx.Row, error) {
	rows := make([]influx.Row, 0, len(points))
	for _, p := range points {
		r := influx.Row{
			Timestamp: p.UnixNano(),
			Name:      string(p.Name()),
		}
		for _, t := range p.Tags() {
			r.Tags = append(r.Tags, influx.Tag{
				Key:   string(t.Key),
				Value: string(t.Value),
			})
		}
		f, err := p.Fields()
		if err != nil {
			return nil, err
		}
		if len(f) > 1 {
			return nil, errors.New("point should have only one value")
		}
		for k, v := range f {
			if value, ok := v.(float64); !ok {
				return nil, errors.New("value should be float64")
			} else {
				r.Fields = []influx.Field{
					influx.Field{
						Type:     influx.Field_Type_Float,
						Key:      k,
						NumValue: value,
					},
				}
			}

		}

		rows = append(rows, r)
	}
	return rows, nil
}

func (h *Handler) httpErrorRsp(w http.ResponseWriter, b []byte, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", h.Config.Realm))
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	w.Write(b)
}

type LogResponse struct {
	ErrorCode string `json:"error_code"`
	ErrorMsg  string `json:"error_msg"`
}

func ErrorResponse(msg string, errCode string) []byte {
	res := LogResponse{
		ErrorCode: errCode,
		ErrorMsg:  msg,
	}

	by, _ := json.Marshal(res)
	return by
}
