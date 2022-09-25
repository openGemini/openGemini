// This package is a set of convenience helpers and structs to make integration testing easier
package tests

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/tests/server_helpers.go
*/

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	_ "path/filepath"
	"regexp"
	_ "runtime"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/toml"
	jsoniter "github.com/json-iterator/go"
	tssql "github.com/openGemini/openGemini/app/ts-sql/sql"
	"github.com/openGemini/openGemini/lib/config"
	_ "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

var verboseServerLogs bool
var indexType string
var cleanupData bool
var seed int64
var json = jsoniter.ConfigCompatibleWithStandardLibrary
var SuccessCount int
var FailCount int

// Server represents a test wrapper for run.Server.
type Server interface {
	URL() string
	Open() error
	SetLogOutput(w io.Writer)
	Close()
	Closed() bool

	CreateDatabase(db string) (*meta.DatabaseInfo, error)
	CreateDatabaseShardKey(db string, shardKey string) error
	CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error
	DropDatabase(db string) error
	Reset() error
	ContainDatabase(name string) bool

	Query(query string) (results string, err error)
	QueryWithParams(query string, values url.Values) (results string, err error)

	Write(db, rp, body string, params url.Values) (results string, err error)
	MustWrite(db, rp, body string, params url.Values) string
	CheckDropDatabases(dbs []string) error
}

// RemoteServer is a Server that is accessed remotely via the HTTP API
type RemoteServer struct {
	*client
	url string
}

func (s *RemoteServer) URL() string {
	return s.url
}

func (s *RemoteServer) Open() error {
	resp, err := http.Get(s.URL() + "/ping")
	if err != nil {
		return err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
	return nil
}

func (s *RemoteServer) Close() {
	// ignore, we can't shutdown a remote server
}

func (s *RemoteServer) SetLogOutput(w io.Writer) {
	// ignore, we can't change the logging of a remote server
}

func (s *RemoteServer) Closed() bool {
	return true
}

func (s *RemoteServer) CreateDatabase(db string) (*meta.DatabaseInfo, error) {
	stmt := fmt.Sprintf("CREATE+DATABASE+%s", db)

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return nil, err
	}
	return &meta.DatabaseInfo{}, nil
}

func (s *RemoteServer) CreateDatabaseShardKey(db string, shardKey string) error {
	stmt := fmt.Sprintf("CREATE+DATABASE+%s+WITH+SHARDKEY+%s", db, shardKey)

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	return err
}

func (s *RemoteServer) CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error {
	if _, err := s.CreateDatabase(db); err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE+RETENTION+POLICY+%s+ON+\"%s\"+DURATION+%s+REPLICATION+%v+SHARD+DURATION+%s",
		rp.Name, db, rp.Duration, *rp.ReplicaN, rp.ShardGroupDuration)
	if makeDefault {
		stmt += "+DEFAULT"
	}

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	return err
}

func (s *RemoteServer) DropDatabase(db string) error {
	stmt := fmt.Sprintf("DROP+DATABASE+%s", db)

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	return err
}

// Reset attempts to remove all database state by dropping everything
func (s *RemoteServer) Reset() error {
	stmt := fmt.Sprintf("SHOW+DATABASES")
	results, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return err
	}

	resp := &Response{}
	if err := resp.UnmarshalJSON([]byte(results)); err != nil {
		return err
	}

	dbs := make([]string, 0)
	for _, db := range resp.Results[0].Series[0].Values {
		dbs = append(dbs, fmt.Sprintf("%s", db[0]))
		if err := s.DropDatabase(fmt.Sprintf("%s", db[0])); err != nil {
			return err
		}
	}

	if err := s.CheckDropDatabases(dbs); err != nil {
		return err
	}
	return nil

}

func (s *RemoteServer) CheckDropDatabases(dbs []string) error {
	retryCount := 0
	for {
		ret := true
		for _, db := range dbs {
			if s.ContainDatabase(db) {
				ret = false
				fmt.Println("check drop databases")
			}
		}

		if ret {
			return nil
		}

		if retryCount == 1200 {
			fmt.Println("retryCount 1200")
			return fmt.Errorf("drop database timeout")
		}

		time.Sleep(100 * time.Millisecond)
		retryCount++
	}
}

func (s *RemoteServer) ContainDatabase(name string) bool {
	stmt := fmt.Sprintf("SHOW+DATABASES")
	results, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return false
	}

	resp := &Response{}
	if err := resp.UnmarshalJSON([]byte(results)); err != nil {
		return false
	}

	for _, db := range resp.Results[0].Series[0].Values {
		dbname := fmt.Sprintf("%s", db[0])
		if dbname == name {
			return true
		}
	}
	return false
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

// NewServer returns a new instance of Server.
func NewServer(c *Config) Server {
	// If URL exists, create a server that will run against a remote endpoint
	if url := os.Getenv("URL"); url != "" {
		s := &RemoteServer{
			url: url,
			client: &client{
				URLFn: func() string {
					return url
				},
			},
		}
		if err := s.Reset(); err != nil {
			panic(err.Error())
		}
		return s
	}
	return nil
}

// NewServer returns a new instance of Server.
func NewTSDBServer(c *Config) Server {
	// If URL exists, create a server that will run against a remote endpoint
	if url := os.Getenv("TSDBURL"); url != "" {
		s := &RemoteServer{
			url: url,
			client: &client{
				URLFn: func() string {
					return url
				},
			},
		}
		if err := s.Reset(); err != nil {
			panic(err.Error())
		}
		return s
	}
	return nil
}

// OpenServer opens a test server.
func OpenServer(c *Config) Server {
	s := NewServer(c)
	configureLogging(s)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenServer opens a test server.
func OpenTSDBServer(c *Config) Server {
	s := NewTSDBServer(c)
	configureLogging(s)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenServerWithVersion opens a test server with a specific version.
func OpenServerWithVersion(c *Config, version string) Server {
	// We can't change the version of a remote server.  The test needs to
	// be skipped if using this func.
	if RemoteEnabled() {
		panic("OpenServerWithVersion not support with remote server")
	}
	return nil
}

// OpenDefaultServer opens a test server with a default database & retention policy.
func OpenDefaultServer(c *Config) Server {
	s := OpenServer(c)
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		panic(err)
	}
	return s
}

// LocalServer is a Server that is running in-process and can be accessed directly
type LocalServer struct {
	mu sync.RWMutex
	*tssql.Server

	*client
	Config *Config
}

// Open opens the server. If running this test on a 32-bit platform it reduces
// the size of series files so that they can all be addressable in the process.
func (s *LocalServer) Open() error {
	return s.Server.Open()
}

// Close shuts down the server and removes all temporary paths.
func (s *LocalServer) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.Server.Close(); err != nil {
		panic(err.Error())
	}

	if cleanupData {
		if err := os.RemoveAll(s.Config.rootPath); err != nil {
			panic(err.Error())
		}
	}

	// Nil the server so our deadlock detector goroutine can determine if we completed writes
	// without timing out
	s.Server = nil
}

func (s *LocalServer) Closed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Server == nil
}

func (s *LocalServer) CreateDatabase(db string) (*meta.DatabaseInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return nil, nil
}

func (s *LocalServer) CreateDatabaseShardKey(db string, shardKey string) error {
	return nil
}

// CreateDatabaseAndRetentionPolicy will create the database and retention policy.
func (s *LocalServer) CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return nil
}

func (s *LocalServer) DropDatabase(db string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return nil
}

func (s *LocalServer) Reset() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return nil
}

func (s *LocalServer) ContainDatabase(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return false
}

func (s *LocalServer) SetLogOutput(w io.Writer) {
	// ignore, we can't change the logging of a remote server
}

func (s *LocalServer) CheckDropDatabases(dbs []string) error { return nil }

// client abstract querying and writing to a Server using HTTP
type client struct {
	URLFn func() string
}

func (c *client) URL() string {
	return c.URLFn()
}

// Query executes a query against the server and returns the results.
func (s *client) Query(query string) (results string, err error) {
	return s.QueryWithParams(query, nil)
}

// MustQuery executes a query against the server and returns the results.
func (s *client) MustQuery(query string) string {
	results, err := s.Query(query)
	if err != nil {
		panic(err)
	}
	return results
}

// Query executes a query against the server and returns the results.
func (s *client) QueryWithParams(query string, values url.Values) (results string, err error) {
	var v url.Values
	if values == nil {
		v = url.Values{}
	} else {
		v, _ = url.ParseQuery(values.Encode())
	}
	if v.Get("chunked") == "true" {
		v.Set("chunked", "true")
	} else {
		v.Set("chunked", "false")
	}
	v.Set("q", query)
	return s.HTTPPost(s.URL()+"/query?"+v.Encode(), nil)
}

// MustQueryWithParams executes a query against the server and returns the results.
func (s *client) MustQueryWithParams(query string, values url.Values) string {
	results, err := s.QueryWithParams(query, values)
	if err != nil {
		panic(err)
	}
	return results
}

// HTTPGet makes an HTTP GET request to the server and returns the response.
func (s *client) HTTPGet(url string) (results string, err error) {
	resp, err := http.DefaultClient.Get(url)
	if err != nil {
		return "", err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		if !expectPattern(".*error parsing query*.", body) {
			return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
		}
		return body, nil
	case http.StatusOK:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}

// HTTPPost makes an HTTP POST request to the server and returns the response.
func (s *client) HTTPPost(url string, content []byte) (results string, err error) {
	buf := bytes.NewBuffer(content)
	resp, err := http.DefaultClient.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		if !expectPattern(".*error parsing query*.", body) {
			return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
		}
		return body, nil
	case http.StatusOK, http.StatusNoContent:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}

type WriteError struct {
	body       string
	statusCode int
}

func (wr WriteError) StatusCode() int {
	return wr.statusCode
}

func (wr WriteError) Body() string {
	return wr.body
}

func (wr WriteError) Error() string {
	return fmt.Sprintf("invalid status code: code=%d, body=%s", wr.statusCode, wr.body)
}

// Write executes a write against the server and returns the results.
func (s *client) Write(db, rp, body string, params url.Values) (results string, err error) {
	if params == nil {
		params = url.Values{}
	}
	if params.Get("db") == "" {
		params.Set("db", db)
	}
	if params.Get("rp") == "" {
		params.Set("rp", rp)
	}
	resp, err := http.Post(s.URL()+"/write?"+params.Encode(), "", strings.NewReader(body))
	if err != nil {
		return "", err
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return "", WriteError{statusCode: resp.StatusCode, body: string(MustReadAll(resp.Body))}
	}
	return string(MustReadAll(resp.Body)), nil
}

// MustWrite executes a write to the server. Panic on error.
func (s *client) MustWrite(db, rp, body string, params url.Values) string {
	results, err := s.Write(db, rp, body, params)
	if err != nil {
		panic(err)
	}
	return results
}

// Config is a test wrapper around a run.Config. It also contains a root temp
// directory, making cleanup easier.
type Config struct {
	rootPath string
	//commonConfig *run.CommonConfig
	//metaConfig *tsmeta.Config
	ingesterConfig *config.TSSql
	//storeConfig *storage.Config
}

func ParseConfig(path string) *Config {
	testCfg := NewParseConfig(path)
	if path != "" {
		if err := testCfg.FromTomlFile(path); err != nil {
			return nil
		}
	}

	return testCfg
}

// NewConfig returns the default config with temporary paths.
func NewParseConfig(path string) *Config {
	c := &Config{}
	return c
}

// NewConfig returns the default config with temporary paths.
func NewConfig() *Config {
	ingestConfig := config.NewTSSql()
	_ = config.Parse(ingestConfig, "config/sql-1.conf")

	c := &Config{ingesterConfig: ingestConfig}
	return c
}

// FromTomlFile loads the config from a TOML file.
func (c *Config) FromTomlFile(fpath string) error {
	return nil
}

// form a correct retention policy given name, replication factor and duration
func NewRetentionPolicySpec(name string, rf int, duration time.Duration) *meta.RetentionPolicySpec {
	return &meta.RetentionPolicySpec{Name: name, ReplicaN: &rf, Duration: &duration}
}

func maxInt64() string {
	maxInt64, _ := json.Marshal(^int64(0))
	return string(maxInt64)
}

func now() time.Time {
	return time.Now().UTC()
}

func yesterday() time.Time {
	return now().Add(-1 * time.Hour * 24)
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

func mustParseLocation(tzname string) *time.Location {
	loc, err := time.LoadLocation(tzname)
	if err != nil {
		panic(err)
	}
	return loc
}

var LosAngeles = mustParseLocation("America/Los_Angeles")

// MustReadAll reads r. Panic on error.
func MustReadAll(r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return b
}

func RemoteEnabled() bool {
	return os.Getenv("URL") != ""
}

func expectPattern(exp, act string) bool {
	return regexp.MustCompile(exp).MatchString(act)
}

type Query struct {
	name     string
	command  string
	params   url.Values
	exp, act string
	pattern  bool
	skip     bool
	repeat   int
	once     bool
	exps     []string
}

// Execute runs the command and returns an err if it fails
func (q *Query) Execute(s Server) (err error) {
	if q.params == nil {
		q.act, err = s.Query(q.command)
		if err != nil {
			FailCount++
		}
		return
	}
	q.act, err = s.QueryWithParams(q.command, q.params)
	if err != nil {
		FailCount++
	}
	return
}

func (q *Query) success() bool {
	if q.pattern {
		if expectPattern(q.exp, q.act) {
			SuccessCount++
		} else {
			FailCount++
		}
		return expectPattern(q.exp, q.act)
	}
	if q.exp == q.act {
		SuccessCount++
	} else {
		FailCount++
	}
	return q.exp == q.act
}

func (q *Query) isSuccess() bool {
	if q.pattern {
		if expectPattern(q.exp, q.act) {
			SuccessCount++
		} else {
			FailCount++
		}
		return expectPattern(q.exp, q.act)
	}
	for _, exp := range q.exps {
		if exp == q.act {
			SuccessCount++
			return true
		}
	}
	FailCount++
	return false
}

func (q *Query) Error(err error) string {
	return fmt.Sprintf("%s: %v", q.name, err)
}

func (q *Query) failureMessage() string {
	return fmt.Sprintf("%s: unexpected results\nquery:  %s\nparams:  %v\nexp:    %s\nactual: %s\n", q.name, q.command, q.params, q.exp, q.act)
}

func (q *Query) failureMessage2() string {
	if len(q.exps) > 0 {
		return fmt.Sprintf("%s: unexpected results\nquery:  %s\nparams:  %v\nexp:    %s...\nactual: %s\n", q.name, q.command, q.params, q.exps[0], q.act)
	} else {
		return fmt.Sprintf("%s: unexpected results\nquery:  %s\nparams:  %v\nexp:    %s\nactual: %s\n", q.name, q.command, q.params, q.exp, q.act)
	}
}

func (q *Query) ExecuteResult(s Server) (result string, err error) {
	if q.params == nil {
		result, err = s.Query(q.command)
		if err != nil {
			FailCount++
		}
		return
	}
	result, err = s.QueryWithParams(q.command, q.params)
	if err != nil {
		FailCount++
	}
	return
}

type Write struct {
	db   string
	rp   string
	data string
}

func (w *Write) duplicate() *Write {
	return &Write{
		db:   w.db,
		rp:   w.rp,
		data: w.data,
	}
}

type Writes []*Write

func (a Writes) duplicate() Writes {
	writes := make(Writes, 0, len(a))
	for _, w := range a {
		writes = append(writes, w.duplicate())
	}
	return writes
}

type Tests map[string]Test

type Test struct {
	initialized bool
	writes      Writes
	params      url.Values
	db          string
	rp          string
	exp         string
	queries     []*Query
}

func NewTest(db, rp string) Test {
	return Test{
		db: db,
		rp: rp,
	}
}

func (t Test) duplicate() Test {
	test := Test{
		initialized: t.initialized,
		writes:      t.writes.duplicate(),
		db:          t.db,
		rp:          t.rp,
		exp:         t.exp,
		queries:     make([]*Query, len(t.queries)),
	}

	if t.params != nil {
		t.params = url.Values{}
		for k, a := range t.params {
			vals := make([]string, len(a))
			copy(vals, a)
			test.params[k] = vals
		}
	}
	copy(test.queries, t.queries)
	return test
}

func (t *Test) addQueries(q ...*Query) {
	t.queries = append(t.queries, q...)
}

func (t *Test) database() string {
	if t.db != "" {
		return t.db
	}
	return "db0"
}

func (t *Test) retentionPolicy() string {
	if t.rp != "" {
		return t.rp
	}
	return "default"
}

func (t *Test) init(s Server) error {
	if len(t.writes) == 0 || t.initialized {
		return nil
	}
	if t.db == "" {
		t.db = "db0"
	}
	if t.rp == "" {
		t.rp = "rp0"
	}

	if err := writeTestData(s, t); err != nil {
		return err
	}

	t.initialized = true

	return nil
}

func writeTestData(s Server, t *Test) error {
	for i, w := range t.writes {
		if w.db == "" {
			w.db = t.database()
		}
		if w.rp == "" {
			w.rp = t.retentionPolicy()
		}

		if !s.ContainDatabase(t.database()) {
			if err := s.CreateDatabaseAndRetentionPolicy(w.db, NewRetentionPolicySpec(w.rp, 1, 0), true); err != nil {
				return err
			}
		}

		if res, err := s.Write(w.db, w.rp, w.data, t.params); err != nil {
			return fmt.Errorf("write #%d: %s", i, err)
		} else if t.exp != res {
			return fmt.Errorf("unexpected results\nexp: %s\ngot: %s\n", t.exp, res)
		}
	}

	_, err := http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)
	return err
}

func configureLogging(s Server) {
	// Set the logger to discard unless verbose is on
	if !verboseServerLogs {
		s.SetLogOutput(ioutil.Discard)
	}
}
