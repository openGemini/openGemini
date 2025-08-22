package httpd

import (
	"bufio"
	"context"
	"encoding/json"
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

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/openGemini/openGemini/engine/hybridqp"
	compression "github.com/openGemini/openGemini/lib/compress"
	config2 "github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/memory"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/auth"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/validation"
	"github.com/pingcap/failpoint"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var json2 = jsoniter.ConfigCompatibleWithStandardLibrary

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

	periodOfInspection = 100 * time.Millisecond
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
	Name              string
	Method            string
	Pattern           string
	CompressSupported bool
	LoggingEnabled    bool
	HandlerFunc       interface{}
}

type SubscriberManager interface {
	Send(db, rp string, lineProtocol []byte)
}

type PointsWriter interface {
	RetryWritePointRows(db, rp string, points []influx.Row) error
}

type RecordWriter interface {
	RetryWriteLogRecord(rec *record.BulkRecords) error
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	mux       *mux.Router
	Version   string
	BuildType string

	MetaClient interface {
		Database(name string) (*meta2.DatabaseInfo, error)
		Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
		Authenticate(username, password string) (ui meta2.User, err error)
		User(username string) (meta2.User, error)
		AdminUserExists() bool
		ShowShards(db string, rp string, mst string) models.Rows
		TagArrayEnabled(db string) bool
		DataNode(id uint64) (*meta2.DataNode, error)
		DataNodes() ([]meta2.DataNode, error)
		SqlNodes() ([]meta2.DataNode, error)

		CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta2.DatabaseInfo, error)
		Databases() map[string]*meta2.DatabaseInfo
		MarkDatabaseDelete(name string) error
		Measurements(database string, ms influxql.Measurements) ([]string, error)

		CreateStreamPolicy(info *meta2.StreamInfo) error
		CreateStreamMeasurement(info *meta2.StreamInfo, src, dest *influxql.Measurement, stmt *influxql.SelectStatement) error
		DropStream(name string) error
		CreateRetentionPolicy(database string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error)
		RetentionPolicy(database, name string) (rpi *meta2.RetentionPolicyInfo, err error)
		DBPtView(database string) (meta2.DBPtInfos, error)
		MarkRetentionPolicyDelete(database, name string) error
		CreateMeasurement(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation, engineType config2.EngineType,
			colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error)
		UpdateMeasurement(db, rp, mst string, options *meta2.Options) error
		GetShardGroupByTimeRange(repoName, streamName string, min, max time.Time) ([]*meta2.ShardGroupInfo, error)
		RevertRetentionPolicyDelete(database, name string) error
	}

	QueryAuthorizer interface {
		AuthorizeQuery(u meta2.User, query *influxql.Query, database string) error
	}

	WriteAuthorizer interface {
		AuthorizeWrite(username, database string) error
	}

	QueryExecutor *query.Executor

	Monitor interface {
	}

	PointsWriter PointsWriter
	RecordWriter RecordWriter

	SubscriberManager

	Config           *config.Config
	Logger           *logger.Logger
	CLFLogger        *zap.Logger
	accessLog        *os.File
	accessLogFilters config.StatusFilters

	writeThrottler   *Throttler
	queryThrottler   *Throttler
	slowQueries      chan *hybridqp.SelectDuration
	StatisticsPusher *statisticsPusher.StatisticsPusher
	SQLConfig        *config2.TSSql
	ResultCache      *ResultsCache
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(c config.Config) *Handler {
	h := &Handler{
		mux:           mux.NewRouter(),
		Config:        &c,
		Logger:        logger.NewLogger(errno.ModuleHTTP),
		CLFLogger:     logger.GetLogger(),
		slowQueries:   make(chan *hybridqp.SelectDuration, 256),
		QueryExecutor: query.NewExecutor(cpu.GetCpuNum()),
	}

	// Limit the number of concurrent & enqueued write requests.
	h.writeThrottler = NewThrottler(c.MaxConcurrentWriteLimit, c.MaxEnqueuedWriteLimit, c.WriteRequestRateLimit, false)
	h.writeThrottler.EnqueueTimeout = time.Duration(c.EnqueuedWriteTimeout)
	h.writeThrottler.Logger = logger.GetLogger()

	h.queryThrottler = NewThrottler(c.MaxConcurrentQueryLimit, c.MaxEnqueuedQueryLimit, c.QueryRequestRateLimit, true)
	h.queryThrottler.EnqueueTimeout = time.Duration(c.EnqueuedQueryTimeout)
	h.queryThrottler.Logger = logger.GetLogger()

	// Disable the write log if they have been suppressed.
	writeLogEnabled := c.LogEnabled

	if c.SuppressWriteLog {
		writeLogEnabled = false
	}
	h.AddInfluxDBAPIRoutes(writeLogEnabled)
	h.AddPrometheusAPIRoutes()
	h.AddSysAPIRoutes()
	h.AddFluxAPIRoute(c.FluxEnabled)

	if config2.IsLogKeeper() {
		h.AddLogstreamAPIRoutes()
	}

	if c.ResultCache.Enabled {
		h.ResultCache = NewResultCache(h.Logger, c.ResultCache)
	}

	return h
}

func (h *Handler) AddFluxAPIRoute(FluxEnabled bool) {
	fluxRoute := Route{
		"flux-read",
		"POST", "/api/v2/query", true, true, nil,
	}
	if !FluxEnabled {
		fluxRoute.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Flux query service disabled. Verify flux-enabled=true in the [http] section of the InfluxDB config.", http.StatusForbidden)
		}
	} else {
		fluxRoute.HandlerFunc = h.serveFluxQuery
	}
	h.AddRoutes(fluxRoute)
}

func (h *Handler) AddInfluxDBAPIRoutes(writeLogEnabled bool) {
	h.AddRoutes([]Route{
		{
			"query-options", // Satisfy CORS checks.
			"OPTIONS", "/query", false, true, h.serveOptions,
		},
		{
			"query", // Query serving route.
			"GET", "/query", true, true, h.serveQuery,
		},
		{
			"query", // Query serving route.
			"POST", "/query", true, true, h.serveQuery,
		},
		{
			"write-options", // Satisfy CORS checks.
			"OPTIONS", "/write", false, true, h.serveOptions,
		},
		{
			"write", // Data-ingest route.
			"POST", "/write", true, writeLogEnabled, h.serveWriteV1,
		},
		{
			"write", // Data-ingest route.
			"POST", "/api/v2/write", true, writeLogEnabled, h.serveWriteV2,
		},
		{ // Ping
			"ping",
			"GET", "/ping", false, true, h.servePing,
		},
		{ // batch-fence-match
			"batch-fence-match",
			"GET", "/fence/match_batch", false, true, h.batchFenceMatch,
		},
		{ // fence-delete
			"fence-delete",
			"POST", "/fence/delete_fence", false, true, h.fenceDelete,
		},
		{ // Ping
			"ping-head",
			"HEAD", "/ping", false, true, h.servePing,
		},
		{ // Ping w/ status
			"status",
			"GET", "/status", false, true, h.serveStatus,
		},
		{ // Ping w/ status
			"status-head",
			"HEAD", "/status", false, true, h.serveStatus,
		},
		{
			"failpoint",
			"POST", "/failpoint", false, true, h.failPoint,
		},
		{
			"otlp-traces-write", // open-telemetry OTLP traces remote write
			"POST", "/api/v1/otlp/traces", false, true, h.serveOtlpTracesWrite,
		},
		{
			"otlp-metrics-write", // open-telemetry OTLP metrics remote write
			"POST", "/api/v1/otlp/metrics", false, true, h.serveOtlpMetricsWrite,
		},
		{
			"otlp-logs-write", // open-telemetry OTLP logs remote write
			"POST", "/api/v1/otlp/logs", false, true, h.serveOtlpLogsWrite,
		},
	}...)
}

func (h *Handler) AddPrometheusAPIRoutes() {
	h.AddRoutes([]Route{
		{
			"prometheus-metrics",
			"GET", "/metrics", false, true, h.serveMetrics,
		},
		{
			"prometheus-write", // Prometheus remote write
			"POST", "/api/v1/write", false, true, h.servePromWrite,
		},
		{
			"prometheus-read", // Prometheus remote read
			"POST", "/api/v1/read", true, true, h.servePromRead,
		},
		{
			"prometheus-read", // Prometheus remote read
			"GET", "/api/v1/read", true, true, h.servePromRead,
		},
		{
			"prometheus-instant-query", // Prometheus instant query
			"GET", "/api/v1/query", true, true, h.servePromQuery,
		},
		{
			"prometheus-instant-query", // Prometheus instant query
			"POST", "/api/v1/query", true, true, h.servePromQuery,
		},
		{
			"prometheus-range-query", // Prometheus range query
			"GET", "/api/v1/query_range", true, true, h.servePromQueryRange,
		},
		{
			"prometheus-range-query", // Prometheus range query
			"POST", "/api/v1/query_range", true, true, h.servePromQueryRange,
		},
		{
			"prometheus-labels-query", // Prometheus labels query
			"GET", "/api/v1/labels", true, true, h.servePromQueryLabels,
		},
		{
			"prometheus-labels-query", // Prometheus labels query
			"POST", "/api/v1/labels", true, true, h.servePromQueryLabels,
		},
		{
			"prometheus-label-values-query", // Prometheus label-values query
			"GET", "/api/v1/label/{name}/values", true, true, h.servePromQueryLabelValues,
		},
		{
			"prometheus-label-values-query", // Prometheus label-values query
			"POST", "/api/v1/label/{name}/values", true, true, h.servePromQueryLabelValues,
		},
		{
			"prometheus-series-query", // Prometheus series query
			"GET", "/api/v1/series", true, true, h.servePromQuerySeries,
		},
		{
			"prometheus-series-query", // Prometheus series query
			"POST", "/api/v1/series", true, true, h.servePromQuerySeries,
		},
		{
			"prometheus-metadata-query", // Prometheus metadata query
			"GET", "/api/v1/metadata", true, true, h.servePromQueryMetaData,
		},
		{
			"prometheus-metadata-query", // Prometheus metadata query
			"POST", "/api/v1/metadata", true, true, h.servePromQueryMetaData,
		},
		{
			"prometheus-create-tsdb", // Prometheus create tsdb
			"POST", "/api/v1/tsdb/{tsdb}", false, true, h.servePromCreateTSDB,
		},
		{
			"prometheus-write-metric-store", // Prometheus remote write
			"POST", "/prometheus/{metric_store}/api/v1/write", false, true, h.servePromWriteWithMetricStore,
		},
		{
			"prometheus-read-metric-store", // Prometheus remote read
			"POST", "/prometheus/{metric_store}/api/v1/read", true, true, h.servePromReadWithMetricStore,
		},
		{
			"prometheus-read-metric-store", // Prometheus remote read
			"GET", "/prometheus/{metric_store}/api/v1/read", true, true, h.servePromReadWithMetricStore,
		},
		{
			"prometheus-instant-query-metric-store", // Prometheus instant query
			"GET", "/prometheus/{metric_store}/api/v1/query", true, true, h.servePromQueryWithMetricStore,
		},
		{
			"prometheus-instant-query-metric-store", // Prometheus instant query
			"POST", "/prometheus/{metric_store}/api/v1/query", true, true, h.servePromQueryWithMetricStore,
		},
		{
			"prometheus-range-query-metric-store", // Prometheus range query
			"GET", "/prometheus/{metric_store}/api/v1/query_range", true, true, h.servePromQueryRangeWithMetricStore,
		},
		{
			"prometheus-range-query-metric-store", // Prometheus range query
			"POST", "/prometheus/{metric_store}/api/v1/query_range", true, true, h.servePromQueryRangeWithMetricStore,
		},
		{
			"prometheus-labels-query-metric-store", // Prometheus labels query
			"GET", "/prometheus/{metric_store}/api/v1/labels", true, true, h.servePromQueryLabelsWithMetricStore,
		},
		{
			"prometheus-labels-query-metric-store", // Prometheus labels query
			"POST", "/prometheus/{metric_store}/api/v1/labels", true, true, h.servePromQueryLabelsWithMetricStore,
		},
		{
			"prometheus-label-values-query-metric-store", // Prometheus label-values query
			"GET", "/prometheus/{metric_store}/api/v1/label/{name}/values", true, true, h.servePromQueryLabelValuesWithMetricStore,
		},
		{
			"prometheus-label-values-query-metric-store", // Prometheus label-values query
			"POST", "/prometheus/{metric_store}/api/v1/label/{name}/values", true, true, h.servePromQueryLabelValuesWithMetricStore,
		},
		{
			"prometheus-series-query-metric-store", // Prometheus series query
			"GET", "/prometheus/{metric_store}/api/v1/series", true, true, h.servePromQuerySeriesWithMetricStore,
		},
		{
			"prometheus-series-query-metric-store", // Prometheus series query
			"POST", "/prometheus/{metric_store}/api/v1/series", true, true, h.servePromQuerySeriesWithMetricStore,
		},
		{
			"prometheus-metadata-query-metric-store", // Prometheus metadata query
			"GET", "/prometheus/{metric_store}/api/v1/metadata", true, true, h.servePromQueryMetaDataWithMetricStore,
		},
		{
			"prometheus-metadata-query-metric-store", // Prometheus metadata query
			"POST", "/prometheus/{metric_store}/api/v1/metadata", true, true, h.servePromQueryMetaDataWithMetricStore,
		},
	}...)
}

func (h *Handler) AddSysAPIRoutes() {
	h.AddRoutes([]Route{
		{ // sysCtrl
			"sysCtrl",
			"POST", "/debug/ctrl", false, true, h.serveSysCtrl,
		},
		{ // backup
			"backup",
			"POST", "/backup/run", false, true, h.serveBackupRun,
		},
		{ // backup
			"backup-abort",
			"POST", "/backup/abort", false, true, h.serveBackupAbort,
		},
		{ // backup
			"backup-status",
			"POST", "/backup/status", false, true, h.serveBackupStatus,
		},
	}...)
}
func (h *Handler) AddLogstreamAPIRoutes() {
	h.AddRoutes([]Route{
		// repository related operations
		{
			"create-repository",
			"POST", "/api/v1/repository/{repository}", false, true, h.serveCreateRepository,
		},
		{
			"delete-repository",
			"DELETE", "/api/v1/repository/{repository}", false, true, h.serveDeleteRepository,
		},
		{
			"list-repository",
			"GET", "/api/v1/repository", false, true, h.serveListRepository,
		},
		{
			"show-repository",
			"GET", "/api/v1/repository/{repository}", false, true, h.serveShowRepository,
		},
		{
			"update-repository",
			"PUT", "/api/v1/repository/{repository}", false, true, h.serveUpdateRepository,
		},
		// logstream related operations
		{
			"create-logStream",
			"POST", "/api/v1/logstream/{repository}/{logStream}", false, true, h.serveCreateLogstream,
		},
		{
			"delete-logStream",
			"DELETE", "/api/v1/logstream/{repository}/{logStream}", false, true, h.serveDeleteLogstream,
		},
		{
			"list-logStream",
			"GET", "/api/v1/logstream/{repository}", false, true, h.serveListLogstream,
		},
		{
			"show-logStream",
			"GET", "/api/v1/logstream/{repository}/{logStream}", false, true, h.serveShowLogstream,
		},
		{
			"update-logStream",
			"PUT", "/api/v1/logstream/{repository}/{logStream}", false, true, h.serveUpdateLogstream,
		},
		{
			"write-log", // Data-ingest route.
			"POST", "/repo/{repository}/logstreams/{logStream}/records", false, true, h.serveRecord,
		},
		{
			"upload", // Data-upload route.
			"POST", "/repo/{repository}/logstreams/{logStream}/upload", false, true, h.serveUpload,
		},
		{
			"log-list", // Query for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/logs", true, true, h.serveQueryLog,
		},
		{
			"log-by-cursor", // Query for Log by cursor.
			"GET", "/repo/{repository}/logstreams/{logStream}/logbycursor", true, true, h.serveQueryLogByCursor,
		},
		{
			"log-consume", // Query for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/consume/logs", true, true, h.serveConsumeLogs,
		},
		{
			"log-consume-cursor-time", // Query for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/consume/cursor-time", true, true, h.serveConsumeCursorTime,
		},
		{
			"log-consume-cursors", // Query for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/consume/cursors", true, true, h.serveGetConsumeCursors,
		},
		{
			"log-context", // Query for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/context", true, true, h.serveContextQueryLog,
		},
		{
			"log-agg", // Query for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/histogram", true, true, h.serveAggLogQuery,
		},
		{
			"log-agg", // Query for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/analytics", true, true, h.serveAnalytics,
		},
		{
			"log-cursor", // Get Cursor for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/cursor", true, true, h.serveGetCursor,
		},
		{
			"log-pull-cursor", // Pull data for Log.
			"GET", "/repo/{repository}/logstreams/{logStream}/cursor/{cursor}", true, true, h.servePullLog,
		},
		{
			"recall-data",
			"POST", "/repo/{repository}/logstreams/{logStream}/recalldata", false, true, h.serveRecallData,
		},
		{
			"create-stream-task",
			"POST", "/repo/{repository}/logstreams/{logStream}/stream-task", false, true, h.serveCreateStreamTask,
		},
		{
			"delete-stream-task",
			"DELETE", "/repo/{repository}/logstreams/{logStream}/stream-task/{taskId}", false, true, h.serveDeleteStreamTask,
		},
	}...)
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
			case "/write", "/api/v1/prom/write", "/repo/{repository}/logstreams/{logStream}/records",
				"/api/streams/{repository}/{logStream}/upload", "/api/v1/otlp/traces", "/api/v1/otlp/metrics", "/api/v1/otlp/logs":
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
			case "/repo/{repository}/logstreams/{logStream}/logs", "/repo/{repository}/logstreams/{logStream}/consume/logs",
				"/repo/{repository}/logstreams/{logStream}/context", "/repo/{repository}/logstreams/{logStream}/histogram",
				"/repo/{repository}/logstreams/{logStream}/analytics", "/repo/{repository}/logstreams/{logStream}/logbycursor":
				handler = h.queryThrottler.Handler(handler)
			default:
			}
		}

		handler = h.responseWriter(handler)
		if r.CompressSupported {
			handler = compressFilter(handler)
		}
		handler = cors(handler)
		handler = requestID(handler)
		if h.Config.LogEnabled && r.LoggingEnabled {
			handler = h.logging(handler, r.Name)
		}
		handler = h.recovery(handler, r.Name) // make sure recovery is always last

		h.mux.HandleFunc(r.Pattern, handler.ServeHTTP).Methods(r.Method)
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handlerStat.Requests.Incr()
	handlerStat.ActiveRequests.Incr()
	defer handlerStat.ActiveRequests.Decr()
	start := time.Now()

	// changed 2023-06-30, use GeminiDB replace influxdb
	// Add version and build header to all Geminidb requests.
	w.Header().Add("X-Geminidb-Version", h.Version)
	w.Header().Add("X-Geminidb-Build", h.BuildType)

	if strings.HasPrefix(r.URL.Path, "/debug/pprof") && h.Config.PprofEnabled {
		h.handleProfiles(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/vars") {
		h.serveExpvar(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/query") {
		h.serveDebugQuery(w, r)
	} else {
		h.mux.ServeHTTP(w, r)
	}

	handlerStat.RequestDuration.AddSinceNano(start)
}

// writeHeader writes the provided status code in the response, and
// updates relevant http error statistics.
func (h *Handler) writeHeader(w http.ResponseWriter, code int) {
	switch code / 100 {
	case 4:
		handlerStat.ClientErrors.Incr()
	case 5:
		handlerStat.ServerErrors.Incr()
	}
	w.WriteHeader(code)
}

func isInternalDatabase(dbName string) bool {
	return dbName == "_internal"
}

func (h *Handler) serveSysCtrl(w http.ResponseWriter, r *http.Request, user meta2.User) {
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

func (h *Handler) checkAuth(w http.ResponseWriter, r *http.Request, user meta2.User) bool {
	if h.Config.AuthEnabled {
		if user == nil {
			// no users in system
			h.httpError(w, "error authorizing query: create admin user first or disable authentication", http.StatusForbidden)
			h.Logger.Error("error authorizing query: create admin user first or disable authentication")
			return false
		}
		if !user.AuthorizeUnrestricted() {
			h.httpError(w, "error authorizing, requires admin privilege only", http.StatusForbidden)
			h.Logger.Error("exec error! authorizing query", zap.Any("r", r), zap.String("userID", user.ID()))
			return false
		}
		h.Logger.Info("execute backup by admin user", zap.String("userID", user.ID()))
	}
	return true
}

func (h *Handler) serveBackupRun(w http.ResponseWriter, r *http.Request, user meta2.User) {
	// Check authorization.
	if ok := h.checkAuth(w, r, user); !ok {
		return
	}

	h.serveBackup(w, r, syscontrol.Backup)
}

func (h *Handler) serveBackupAbort(w http.ResponseWriter, r *http.Request, user meta2.User) {
	// Check authorization.
	if ok := h.checkAuth(w, r, user); !ok {
		return
	}

	h.serveBackup(w, r, syscontrol.AbortBackup)
}

func (h *Handler) serveBackupStatus(w http.ResponseWriter, r *http.Request, user meta2.User) {
	// Check authorization.
	if ok := h.checkAuth(w, r, user); !ok {
		return
	}

	h.serveBackup(w, r, syscontrol.BackupStatus)
}

func (h *Handler) serveBackup(w http.ResponseWriter, r *http.Request, mod string) {
	q := r.URL.Query()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	var req msgservice.SysCtrlRequest

	mp := make(map[string]string, len(q))
	for k, v := range q {
		if len(v) < 1 {
			continue
		}
		mp[k] = v[0]
	}
	req.SetParam(mp)
	req.SetMod(mod)

	nw := bufio.NewWriter(w)
	nw.WriteString("{\n\t")

	var err error
	if mod == syscontrol.Backup {
		err = syscontrol.ProcessBackup(req, nw, config.CombineDomain(h.SQLConfig.HTTP.Domain, h.Config.BindAddress))
	} else {
		err = syscontrol.ProcessRequest(req, nw)
	}

	if err != nil {
		h.httpError(w, "backup execute error: "+err.Error(), http.StatusBadRequest)
		return
	}
	nw.WriteString("\n}\n")
	util.MustRun(nw.Flush)
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
		h.Logger.Info(HideQueryPassword(qp), zap.String("userID", user.ID()), zap.String("remote_addr", r.RemoteAddr))
	} else {
		h.Logger.Info(HideQueryPassword(qp), zap.String("remote_addr", r.RemoteAddr))
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
	decoder := json2.NewDecoder(strings.NewReader(rawParams))
	decoder.UseNumber()
	if err := decoder.Decode(&params); err != nil {
		h.Logger.Error("query error! parsing query parameters", zap.Error(err), zap.String("db", r.FormValue("db")), zap.Any("r", r))
		return nil, fmt.Errorf("error parsing query parameters: %s", err.Error())
	}

	// Convert json.Number into int64 and float64 values
	for k, v := range params {
		if v, ok := v.(json.Number); ok {
			var err error
			if strings.Contains(string(v), ".") {
				params[k], err = v.Float64()
			} else {
				params[k], err = v.Int64()
			}

			if err != nil {
				h.Logger.Error("query error! parsing json value", zap.Error(err), zap.String("db", r.FormValue("db")), zap.Any("r", r))
				return nil, fmt.Errorf("error parsing json value: %s", err.Error())
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
				return false, 0, 0, fmt.Errorf("%s", msg)
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
		return nil, fmt.Errorf("error parsing query: %s", err.Error()), http.StatusBadRequest
	}

	return q, nil, http.StatusOK
}

func (h *Handler) getAuthorizer(user meta2.User) query.FineAuthorizer {
	if h.Config.AuthEnabled {
		if user != nil && user.AuthorizeUnrestricted() {
			return query.OpenAuthorizer
		} else {
			// The current user determines the authorized actions.
			return user
		}
	} else {
		// Auth is disabled, so allow everything.
		return query.OpenAuthorizer
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
	q.Add(Sql, "true")
	q.Add("query", r.FormValue("q"))
	r.URL.RawQuery = q.Encode()
	r.Form = nil
}

// rewritePipeStateForQuery is used to generate selectStmt based on the pipe state.
func (h *Handler) rewritePipeStateForQuery(q *influxql.Query, param *QueryParam, info *measurementInfo) {
	var selectStmt *influxql.SelectStatement
	if stmt, currOk := q.Statements[0].(*influxql.ExplainStatement); currOk {
		selectStmt = stmt.Statement
	} else {
		selectStmt, _ = q.Statements[0].(*influxql.SelectStatement)
	}
	selectStmt.RewriteUnnestSource()
	selectStmt.Sources = influxql.Sources{&influxql.Measurement{Name: info.name, Database: info.database, RetentionPolicy: info.retentionPolicy}}
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

	para := NewQueryPara(queryLogRequest)

	return para, nil
}

func (h *Handler) parsePipeAndSqlForQuery(r *http.Request, user meta2.User, info *measurementInfo) (*influxql.Query, int, error) {
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
		_, err, status := h.getPplQuery(info, strings.NewReader(ppl), sqlQuery)
		if err != nil {
			return nil, status, err
		}
	}

	h.rewritePipeStateForQuery(sqlQuery, para, info)
	return sqlQuery, http.StatusOK, nil
}

// serveQuery parses an incoming query and, if valid, executes the query
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user meta2.User) {
	handlerStat.QueryRequests.Incr()
	handlerStat.ActiveQueryRequests.Incr()
	start := time.Now()
	defer func() {
		handlerStat.ActiveQueryRequests.Decr()
		handlerStat.QueryRequestDuration.AddSinceNano(start)
	}()

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

	// Wrap r.Body in maxBytesReader before first call to r.FormValue to avoid parsing too much of the input.
	if h.Config.MaxBodySize > 0 {
		r.Body = truncateReader(r.Body, int64(h.Config.MaxBodySize))
	}
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			err := errno.NewError(errno.HttpRequestEntityTooLarge)
			h.Logger.Error("serveQuery", zap.Int64("ContentLength", r.ContentLength), zap.Error(err))
			handlerStat.Write400ErrRequests.Incr()
			return
		}
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	var q *influxql.Query
	var err error
	var status int
	isPipe := r.FormValue("pipe") == "true"
	if isPipe {
		repository := r.URL.Query().Get("db")
		logStream := r.URL.Query().Get("measurement")
		info := &measurementInfo{
			name:            logStream,
			database:        repository,
			retentionPolicy: logStream,
		}
		q, _, err = h.parsePipeAndSqlForQuery(r, user, info)
		if err != nil {
			h.httpError(rw, err.Error(), http.StatusBadRequest)
			h.Logger.Error("serveQuery: parsePipeAndSqlForQuery error!", zap.Error(err))
			return
		}
	} else {
		// new reader for sql statement
		qr, f, err := h.newQueryReader(r, nil, user)
		if err != nil {
			h.httpError(rw, err.Error(), http.StatusBadRequest)
			h.Logger.Error("serveQuery: newQueryReader error!", zap.Error(err))
			return
		}
		if f != nil {
			defer util.MustClose(f)
		}
		q, err, status = h.getSqlQuery(r, qr)
		if err != nil {
			h.httpError(rw, err.Error(), status)
			h.Logger.Error("serveQuery: getSqlQuery error!", zap.Error(err))
			return
		}
	}

	epoch := strings.TrimSpace(r.FormValue("epoch"))
	if epoch == "" {
		epoch = "rfc3339"
	}

	db := r.FormValue("db")
	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(db) {
		qDuration = statistics.NewSqlSlowQueryStatistics(db)
		defer func() {
			d := time.Now().Sub(start)
			statQueryInfo(q, d, db)
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
		h.Logger.Error("serveQuery error:user is not authorized to query to database", zap.Error(err))
		return
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		h.httpError(rw, err.Error(), http.StatusBadRequest)
		h.Logger.Error("serveQuery: parseChunkSize error!", zap.Error(err))
	}
	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	isQuerySeriesLimit := r.FormValue("is_query_series_limit") == "true"
	opts := query.ExecutionOptions{
		IsQuerySeriesLimit: isQuerySeriesLimit,
		Database:           db,
		RetentionPolicy:    r.FormValue("rp"),
		ChunkSize:          chunkSize,
		Chunked:            chunked,
		ReadOnly:           r.Method == "GET",
		NodeID:             nodeID,
		InnerChunkSize:     innerChunkSize,
		ParallelQuery:      atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		Quiet:              true,
		Authorizer:         h.getAuthorizer(user),
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
			h.Logger.Error("serveQuery: executeQuery results error!", zap.Error(err))
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
			handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
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
		handlerStat.QueryRequestBytesTransmitted.Add(int64(n))
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
			if r.Err == query.ErrNotExecuted {
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

// bucket2drbp extracts a bucket and retention policy from a properly formatted
// string.
//
// The 2.x compatible endpoints encode the databse and retention policy names
// in the database URL query value.  It is encoded using a forward slash like
// "database/retentionpolicy" and we should be able to simply split that string
// on the forward slash.
func bucket2dbrp(bucket string) (string, string, error) {
	// test for a slash in our bucket name.
	switch idx := strings.IndexByte(bucket, '/'); idx {
	case -1:
		// if there is no slash, we're mapping bucket to the databse.
		switch db := bucket; db {
		case "":
			// if our "database" is an empty string, this is an error.
			return "", "", fmt.Errorf(`bucket name %q is missing a slash; not in "database/retention-policy" format`, bucket)
		default:
			return db, "", nil
		}
	default:
		// there is a slash
		switch db, rp := bucket[:idx], bucket[idx+1:]; {
		case db == "":
			// empty database is unrecoverable
			return "", "", fmt.Errorf(`bucket name %q is in db/rp form but has an empty database`, bucket)
		default:
			return db, rp, nil
		}
	}
}

// serveWriteV2 maps v2 write parameters to a v1 style handler.  the concepts
// of an "org" and "bucket" are mapped to v1 "database" and "retention
// policies".
func (h *Handler) serveWriteV2(w http.ResponseWriter, r *http.Request, user meta2.User) {
	db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))
	if err != nil {
		h.httpError(w, err.Error(), http.StatusNotFound)
		return
	}
	h.serveWrite(db, rp, w, r, user)
}

// serveWriteV1 handles v1 style writes.
func (h *Handler) serveWriteV1(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.serveWrite(r.URL.Query().Get("db"), r.URL.Query().Get("rp"), w, r, user)
}

// serveWrite receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveWrite(database string, rp string, w http.ResponseWriter, r *http.Request, user meta2.User) {
	handlerStat.WriteRequests.Incr()
	handlerStat.ActiveWriteRequests.Incr()
	handlerStat.WriteRequestBytesIn.Add(r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		handlerStat.ActiveWriteRequests.Decr()
		handlerStat.WriteRequestDuration.Add(d)
	}(time.Now())

	if syscontrol.DisableWrites {
		h.httpError(w, `disable write!`, http.StatusForbidden)
		h.Logger.Error("write is forbidden!", zap.Bool("DisableWrites", syscontrol.DisableWrites))
		return
	}

	if syscontrol.IsReadonly() {
		h.httpError(w, "readonly now and writing is not allowed", http.StatusBadRequest)
		h.Logger.Error("serveWrite: readonly now and writing is not allowed", zap.Bool("IsReadonly", syscontrol.IsReadonly()))
		return
	}

	urlValues := r.URL.Query()
	if database == "" {
		err := errno.NewError(errno.HttpDatabaseNotFound)
		h.Logger.Error("serveWrite", zap.Error(err))
		h.httpError(w, "database is required", http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}

	if _, err := h.MetaClient.Database(database); err != nil {
		err := errno.NewError(errno.HttpDatabaseNotFound)
		h.Logger.Error("serveWrite", zap.Error(err), zap.String("db", database))
		h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		handlerStat.Write400ErrRequests.Incr()
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			err := errno.NewError(errno.HttpForbidden)
			h.Logger.Error("write error: user is required to write to database", zap.Error(err), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			err := errno.NewError(errno.HttpForbidden)
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			h.Logger.Error("write error:user is not authorized to write to database", zap.Error(err), zap.String("db", database), zap.String("user", user.ID()))
			handlerStat.Write400ErrRequests.Incr()
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := compression.GetGzipReader(r.Body)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			error := errno.NewError(errno.HttpBadRequest)
			h.Logger.Error("write error:Handle gzip decoding of the body err", zap.Error(error), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
			return
		}
		defer compression.PutGzipReader(b)
		body = b
	}

	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			err := errno.NewError(errno.HttpRequestEntityTooLarge)
			h.Logger.Error("serveWrite", zap.Int64("ContentLength", r.ContentLength), zap.Error(err), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
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

	ctx := influx.GetStreamContext(body, h.Config.MaxLineSize)
	defer influx.PutStreamContext(ctx)

	var numPtsParse, numPtsInsert int

	readBlockSize := int(h.Config.ReadBlockSize)
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
				handlerStat.PointsWrittenOK.Add(int64(len(rows)))
			}
			ctx.Wg.Done()
		}
		uw.TsMultiplier = tsMultiplier
		uw.Db = database
		uw.ReqBuf, ctx.ReqBuf = ctx.ReqBuf, uw.ReqBuf
		uw.EnableTagArray = h.MetaClient.TagArrayEnabled(database)
		handlerStat.WriteRequestBytesReceived.Add(int64(len(uw.ReqBuf)))

		ctx.Wg.Add(1)
		start := time.Now()
		influx.ScheduleUnmarshalWork(uw)
		handlerStat.WriteScheduleUnMarshalDns.AddSinceNano(start)
		numPtsInsert++
	}
	ctx.Wg.Wait()
	if err := ctx.Error(); err != nil {
		h.Logger.Error("write error:read body ", zap.Error(err), zap.String("db", database))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	if err := ctx.UnmarshalErr; err != nil {
		handlerStat.PointsWrittenFail.Add(int64(numPtsInsert))
		h.Logger.Error("write client error, unmarshal points failed", zap.Error(err), zap.String("db", database))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		handlerStat.Write400ErrRequests.Incr()
		return
	}
	if err := ctx.CallbackErr; err != nil {
		if influxdb.IsClientError(err) {
			handlerStat.PointsWrittenFail.Add(int64(numPtsInsert))
			h.Logger.Error("write client error:WritePointsWithContext", zap.Error(err), zap.String("db", database))
			h.httpError(w, err.Error(), http.StatusBadRequest)
			handlerStat.Write400ErrRequests.Incr()
			return
		} else if influxdb.IsAuthorizationError(err) {
			handlerStat.PointsWrittenFail.Add(int64(numPtsParse))
			h.httpError(w, err.Error(), http.StatusForbidden)
			h.Logger.Error("write authorization error:WritePointsWithContext", zap.Error(err), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
			return
		} else if werr, ok := err.(msgservice.PartialWriteError); ok {
			handlerStat.PointsWrittenOK.Add(int64(numPtsInsert - werr.Dropped))
			handlerStat.PointsWrittenDropped.Add(int64(werr.Dropped))
			h.httpError(w, werr.Error(), http.StatusBadRequest)
			h.Logger.Error("write Partial Write error:WritePointsWithContext", zap.Error(werr.Reason), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
			return
		} else if errno.Equal(err, errno.MeasurementNameTooLong) {
			handlerStat.PointsWrittenFail.Add(int64(numPtsParse))
			h.httpError(w, werr.Error(), http.StatusBadRequest)
			h.Logger.Error("write error:WritePointsWithContext", zap.Error(werr.Reason), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
			return
		} else if err != nil {
			handlerStat.PointsWrittenFail.Add(int64(numPtsInsert))
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			h.Logger.Error("write error:WritePointsWithContext", zap.Error(err), zap.String("db", database))
			handlerStat.Write500ErrRequests.Incr()
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
	handlerStat.PingRequests.Incr()

	if verbose != "" && verbose != "0" && verbose != "false" {
		h.writeHeader(w, http.StatusOK)
		b, _ := json2.Marshal(map[string]string{"version": h.Version})
		w.Write(b)
	} else {
		h.writeHeader(w, http.StatusNoContent)
	}
}

// serveStatus has been deprecated.
func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	h.Logger.Info("WARNING: /status has been deprecated.  Use /ping instead.")
	handlerStat.StatusRequests.Incr()
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
		var req msgservice.SysCtrlRequest
		req.SetMod(syscontrol.Failpoint)
		req.SetParam(map[string]string{
			"point":    point,
			"switchon": "true",
			"term":     term,
		})

		nw := bufio.NewWriter(w)
		nw.WriteString("{\n\t")
		err = syscontrol.ProcessRequest(req, nw)
		nw.WriteString("\n}\n")
		nw.Flush()
	} else if flag == "disable" {
		err = failpoint.Disable(point)
		if err != nil {
			h.Logger.Error("disable failpoint fail", zap.String("point", point), zap.Error(err))
		} else {
			h.Logger.Info("disable failpoint success", zap.String("point", point))
		}
		var req msgservice.SysCtrlRequest
		req.SetMod(syscontrol.Failpoint)
		req.SetParam(map[string]string{
			"point":    point,
			"switchon": "false",
		})
		nw := bufio.NewWriter(w)
		nw.WriteString("{\n\t")
		err = syscontrol.ProcessRequest(req, nw)
		nw.WriteString("\n}\n")
		nw.Flush()
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
	SetStatsResponse(h.StatisticsPusher, w, r)
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
	b, _ := json2.Marshal(response)
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
				handlerStat.AuthenticationFailures.Incr()
				h.httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}

			switch creds.Method {
			case UserAuthentication:
				if creds.Username == "" {
					handlerStat.AuthenticationFailures.Incr()
					errMsg := "username required"
					err := errno.NewError(errno.HttpUnauthorized)
					log := logger.NewLogger(errno.ModuleHTTP)
					log.Error(errMsg, zap.Error(err))
					h.httpError(w, errMsg, http.StatusUnauthorized)
					return
				}

				user, err = h.MetaClient.Authenticate(creds.Username, creds.Password)
				if err != nil {
					handlerStat.AuthenticationFailures.Incr()
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
					handlerStat.AuthenticationFailures.Incr()
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
		if r.Header.Get(FORWARD) == "true" {
			inner.ServeHTTP(w, r)
			return
		}
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

		if config2.IsLogKeeper() {
			logger.GetLogger().Info(buildLogLine(l, r, start))
		} else if h.accessLogFilters.Match(l.Status()) {
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
	if willCrash, err = strconv.ParseBool(os.Getenv(query.PanicCrashEnv)); err != nil {
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
				h.Logger.Error(logLine)
				http.Error(w, http.StatusText(http.StatusInternalServerError), 500)
				handlerStat.RecoveredPanics.Incr()

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

	return json2.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct.
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	err := json2.Unmarshal(b, &o)
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

	// query is used to distinguish between a write and query throttler.
	query bool
}

// NewThrottler returns a new instance of Throttler that limits to concurrentN.
// requests processed at a time and maxEnqueueN requests waiting to be processed.
func NewThrottler(concurrentN, maxEnqueueN int, rateValue int, query bool) *Throttler {
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
		query:    query,
	}
}

// Handler wraps h in a middleware handler that throttles requests.
func (t *Throttler) Handler(h http.Handler) http.Handler {
	timeout := t.EnqueueTimeout

	// Return original handler if concurrent requests is zero.
	if cap(t.current) == 0 {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If there is no limit for concurrent queries and queues, the memory usage
			// exceeds the threshold the new query is canceled and an error is reported
			if t.query && sysconfig.GetInterruptQuery() {
				memUsed := memory.GetMemMonitor().MemUsedPct()
				memThre := float64(sysconfig.GetUpperMemPct())
				if memUsed > memThre {
					// Even if the memory usage exceeds the threshold
					// the `show queries` and `kill query` statements will not be blocked.
					uri := strings.ToLower(r.RequestURI)
					if strings.Contains(uri, "show+queries") || strings.Contains(uri, "kill+query") {
						h.ServeHTTP(w, r)
						return
					}
					resMsg := "request throttled, query memory exceeds the threshold, query is canceled"
					t.Logger.Warn(resMsg, zap.Float64("mem used", memUsed), zap.Float64("mem threshold", memThre))
					http.Error(w, resMsg, http.StatusServiceUnavailable)
					return
				}
			}

			// Execute request.
			h.ServeHTTP(w, r)
		})
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
				// If there is limit for concurrent queries and queues, the memory usage exceeds the threshold
				// the new query is blocked and wait until the memory is less than the threshold the query
				// queue is full, or the query times out
				if t.query && sysconfig.GetInterruptQuery() {
					memUsed := memory.GetMemMonitor().MemUsedPct()
					memThre := float64(sysconfig.GetUpperMemPct())
					if memUsed < memThre {
						break
					}
					// Even if the memory usage exceeds the threshold
					// the `show queries` and `kill query` statements will not be blocked.
					uri := strings.ToLower(r.RequestURI)
					if strings.Contains(uri, "show+queries") || strings.Contains(uri, "kill+query") {
						break
					}
					resMsg := "request throttled, query memory exceeds the threshold, query is blocked"
					t.Logger.Warn(resMsg, zap.Float64("mem used", memUsed), zap.Float64("mem threshold", memThre))
					ticker := time.NewTicker(periodOfInspection)
					defer ticker.Stop()
				Loop:
					for {
						select {
						case <-ticker.C:
							if memory.GetMemMonitor().MemUsedPct() < float64(sysconfig.GetUpperMemPct()) {
								break Loop
							}
						case <-timerCh:
							t.Logger.Warn("request throttled, exceeds timeout", zap.Duration("d", timeout), zap.Int("current length", len(t.current)))
							http.Error(w, "request throttled, exceeds timeout", http.StatusServiceUnavailable)
							return
						}
					}
				}
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

func buildCommand(q *prompb.Query, mst string) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	// If we don't find a metric name matcher, query all metrics
	// (InfluxDB measurements) by default.
	from := "FROM /.+/"
	haveMetricStore := len(mst) > 0
	if haveMetricStore {
		from = fmt.Sprintf("FROM %s", mst)
	}
	for _, m := range q.Matchers {
		if !haveMetricStore && m.Name == model.MetricNameLabel {
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
	matchers = append(matchers, fmt.Sprintf("time >= %v", q.StartTimestampMs*1e6))
	matchers = append(matchers, fmt.Sprintf("time <= %v", q.EndTimestampMs*1e6))

	return fmt.Sprintf("SELECT value %s WHERE %v GROUP BY *", from, strings.Join(matchers, " AND ")), nil
}

func escapeSlashes(str string) string {
	return strings.Replace(str, `/`, `\/`, -1)
}

func escapeSingleQuotes(str string) string {
	return strings.Replace(str, `'`, `\'`, -1)
}

func ReadRequestToInfluxQuery(req *prompb.ReadRequest, mst string) (string, error) {
	var readRequest string
	for _, q := range req.Queries {
		if err := validation.ValidateQueryTimeRange(mst, time.UnixMilli(q.StartTimestampMs), time.UnixMilli(q.EndTimestampMs)); err != nil {
			return "", err
		}

		if s, err := buildCommand(q, mst); err != nil {
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

	by, _ := json2.Marshal(res)
	return by
}

func statQueryInfo(q *influxql.Query, d time.Duration, db string) {
	queryStat := statistics.NewQueryInfoStatistics()
	for _, statement := range q.Statements {
		s, ok := statement.(*influxql.SelectStatement)
		if !ok {
			continue
		}
		if b, ok := s.Condition.(*influxql.BinaryExpr); ok {
			ConditionFuzz(b)
		}
	}
	queryStat.AddQueryInfo(q.String(), d.Nanoseconds(), db)
}

func ConditionFuzz(b *influxql.BinaryExpr) {
	switch lhs := b.LHS.(type) {
	case *influxql.BinaryExpr:
		ConditionFuzz(lhs)
		if rhs, ok := b.RHS.(*influxql.BinaryExpr); ok {
			ConditionFuzz(rhs)
		}
	case *influxql.VarRef:
		b.RHS = &influxql.StringLiteral{Val: "?"}
	default:
		return
	}
}
