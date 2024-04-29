package config

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/cpu"
)

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8086"

	// DefaultFlightAddress is the default address to bind to.
	DefaultFlightAddress = ":8087"

	// DefaultRealm is the default realm sent back when issuing a basic auth challenge.
	DefaultRealm = "InfluxDB"

	// DefaultBindSocket is the default unix socket to bind to.
	DefaultBindSocket = "/var/run/tssql.sock"

	// DefaultMaxBodySize is the default maximum size of a client request body, in bytes. Specify 0 for no limit.
	DefaultMaxBodySize = 25e6

	// DefaultEnqueuedWriteTimeout is the maximum time a write request can wait to be processed.
	DefaultEnqueuedWriteTimeout = 30 * time.Second
	DefaultEnqueuedQueryTimeout = 5 * time.Minute
	// DefaultMaxRowNum is the maximum row number of a query result.
	DefaultMaxRowNum = 1000000

	DefaultBlockSize = 64 * 1024
)

// Config represents a configuration for a HTTP service.
type Config struct {
	BindAddress             string         `toml:"bind-address"`
	FlightAddress           string         `toml:"flight-address"`
	FlightEnabled           bool           `toml:"flight-enabled"`
	FlightAuthEnabled       bool           `toml:"flight-auth-enabled"`
	FlightChFactor          int            `toml:"flight-ch-factor"`
	Domain                  string         `toml:"domain"`
	AuthEnabled             bool           `toml:"auth-enabled"`
	WeakPwdPath             string         `toml:"weakpwd-path"`
	LogEnabled              bool           `toml:"log-enabled"`
	SuppressWriteLog        bool           `toml:"suppress-write-log"`
	WriteTracing            bool           `toml:"write-tracing"`
	FluxEnabled             bool           `toml:"flux-enabled"`
	FluxLogEnabled          bool           `toml:"flux-log-enabled"`
	PprofEnabled            bool           `toml:"pprof-enabled"`
	DebugPprofEnabled       bool           `toml:"debug-pprof-enabled"`
	HTTPSEnabled            bool           `toml:"https-enabled"`
	HTTPSCertificate        string         `toml:"https-certificate"`
	HTTPSPrivateKey         string         `toml:"https-private-key"`
	MaxRowLimit             int            `toml:"max-row-limit"`
	MaxConnectionLimit      int            `toml:"max-connection-limit"`
	SharedSecret            string         `toml:"shared-secret"`
	Realm                   string         `toml:"realm"`
	UnixSocketEnabled       bool           `toml:"unix-socket-enabled"`
	UnixSocketGroup         *toml.Group    `toml:"unix-socket-group"`
	UnixSocketPermissions   toml.FileMode  `toml:"unix-socket-permissions"`
	BindSocket              string         `toml:"bind-socket"`
	MaxBodySize             int            `toml:"max-body-size"`
	AccessLogPath           string         `toml:"access-log-path"`
	AccessLogStatusFilters  []StatusFilter `toml:"access-log-status-filters"`
	MaxConcurrentWriteLimit int            `toml:"max-concurrent-write-limit"`
	MaxEnqueuedWriteLimit   int            `toml:"max-enqueued-write-limit"`
	EnqueuedWriteTimeout    toml.Duration  `toml:"enqueued-write-timeout"`
	MaxConcurrentQueryLimit int            `toml:"max-concurrent-query-limit"`
	MaxEnqueuedQueryLimit   int            `toml:"max-enqueued-query-limit"`
	QueryRequestRateLimit   int            `toml:"query-request-ratelimit"`
	WriteRequestRateLimit   int            `toml:"write-request-ratelimit"`
	EnqueuedQueryTimeout    toml.Duration  `toml:"enqueued-query-timeout"`
	TLS                     *tls.Config    `toml:"-"`
	WhiteList               string         `toml:"white_list"`
	SlowQueryTime           toml.Duration  `toml:"slow-query-time"`
	ParallelQueryInBatch    bool           `toml:"parallel-query-in-batch-enabled"`
	QueryMemoryLimitEnabled bool           `toml:"query-memory-limit-enabled"`
	ChunkReaderParallel     int            `toml:"chunk-reader-parallel"`
	ReadBlockSize           toml.Size      `toml:"read-block-size"`
	TimeFilterProtection    bool           `toml:"time-filter-protection"`
	CPUThreshold            int            `toml:"cpu-threshold"`
}

func CombineDomain(domain, addr string) string {
	if domain == "" {
		return addr
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	return fmt.Sprintf("%s:%s", domain, port)
}

func (c Config) BindAddr() string {
	return CombineDomain(c.Domain, c.BindAddress)
}

// NewHttpConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		FluxEnabled:             false,
		FluxLogEnabled:          false,
		BindAddress:             DefaultBindAddress,
		FlightAddress:           DefaultFlightAddress,
		FlightEnabled:           false,
		FlightAuthEnabled:       false,
		FlightChFactor:          2,
		LogEnabled:              true,
		PprofEnabled:            true,
		DebugPprofEnabled:       false,
		HTTPSEnabled:            false,
		HTTPSCertificate:        "/etc/ssl/influxdb.pem",
		MaxRowLimit:             DefaultMaxRowNum,
		Realm:                   DefaultRealm,
		UnixSocketEnabled:       false,
		UnixSocketPermissions:   0777,
		BindSocket:              DefaultBindSocket,
		MaxBodySize:             DefaultMaxBodySize,
		EnqueuedWriteTimeout:    toml.Duration(DefaultEnqueuedWriteTimeout),
		EnqueuedQueryTimeout:    toml.Duration(DefaultEnqueuedQueryTimeout),
		SlowQueryTime:           toml.Duration(time.Second * 10),
		ParallelQueryInBatch:    true,
		QueryMemoryLimitEnabled: true,
		ChunkReaderParallel:     cpu.GetCpuNum(),
		ReadBlockSize:           toml.Size(DefaultBlockSize),
		TimeFilterProtection:    false,
	}
}

// Validate validates that the configuration is acceptable.
func (c Config) Validate() error {
	if c.BindAddress == "" {
		return errors.New("http bind-address must be specified")
	}
	if c.FlightAddress == "" {
		return errors.New("http arrowflight-address must be specified")
	}
	if c.MaxConnectionLimit < 0 {
		return errors.New("http max-connection-limit can not be negative")
	}
	if c.MaxConcurrentWriteLimit < 0 {
		return errors.New("http max-concurrent-write-limit can not be negative")
	}
	if c.MaxConcurrentQueryLimit < 0 {
		return errors.New("http max-concurrent-query-limit can not be negative")
	}
	if c.EnqueuedWriteTimeout < 0 {
		return errors.New("http enqueued-write-timeout can not be negative")
	}
	if c.EnqueuedQueryTimeout < 0 {
		return errors.New("http enqueued-query-timeout can not be negative")
	}
	if c.ChunkReaderParallel < 0 {
		return errors.New("http chunk-reader-parallel can not be negative")
	}
	if c.MaxBodySize < 0 {
		return errors.New("http max-body-size can not be negative")
	}
	return nil
}

func (c *Config) ShowConfigs() map[string]interface{} {
	return map[string]interface{}{
		"http.bind-address":                    c.BindAddress,
		"http.flight-address":                  c.FlightAddress,
		"http.flight-enabled":                  c.FlightEnabled,
		"http.flight-auth-enabled":             c.FlightAuthEnabled,
		"http.flight-ch-factor":                c.FlightChFactor,
		"http.domain":                          c.Domain,
		"http.auth-enabled":                    c.AuthEnabled,
		"http.weakpwd-path":                    c.WeakPwdPath,
		"http.log-enabled":                     c.LogEnabled,
		"http.suppress-write-log":              c.SuppressWriteLog,
		"http.write-tracing":                   c.WriteTracing,
		"http.flux-enabled":                    c.FluxEnabled,
		"http.flux-log-enabled":                c.FluxLogEnabled,
		"http.pprof-enabled":                   c.PprofEnabled,
		"http.debug-pprof-enabled":             c.DebugPprofEnabled,
		"http.https-enabled":                   c.HTTPSEnabled,
		"http.https-certificate":               c.HTTPSCertificate,
		"http.https-private-key":               c.HTTPSPrivateKey,
		"http.max-row-limit":                   c.MaxRowLimit,
		"http.max-connection-limit":            c.MaxConnectionLimit,
		"http.shared-secret":                   c.SharedSecret,
		"http.realm":                           c.Realm,
		"http.unix-socket-enabled":             c.UnixSocketEnabled,
		"http.unix-socket-group":               c.UnixSocketGroup,
		"http.unix-socket-permissions":         c.UnixSocketPermissions,
		"http.bind-socket":                     c.BindSocket,
		"http.max-body-size":                   c.MaxBodySize,
		"http.access-log-path":                 c.AccessLogPath,
		"http.access-log-status-filters":       c.AccessLogStatusFilters,
		"http.max-concurrent-write-limit":      c.MaxConcurrentWriteLimit,
		"http.max-enqueued-write-limit":        c.MaxEnqueuedWriteLimit,
		"http.enqueued-write-timeout":          c.EnqueuedWriteTimeout,
		"http.max-concurrent-query-limit":      c.MaxConcurrentQueryLimit,
		"http.max-enqueued-query-limit":        c.MaxEnqueuedQueryLimit,
		"http.query-request-rate-limit":        c.QueryRequestRateLimit,
		"http.write-request-rate-limit":        c.WriteRequestRateLimit,
		"http.enqueued-query-timeout":          c.EnqueuedQueryTimeout,
		"http.white_list":                      c.WhiteList,
		"http.slow-query-time":                 c.SlowQueryTime,
		"http.parallel-query-in-batch-enabled": c.ParallelQueryInBatch,
		"http.query-memory-limit-enabled":      c.QueryMemoryLimitEnabled,
		"http.chunk-reader-parallel":           c.ChunkReaderParallel,
		"http.read-block-size":                 c.ReadBlockSize,
		"http.time-filter-protection":          c.TimeFilterProtection,
		"http.cpu-threshold":                   c.CPUThreshold,
	}
}

// StatusFilter will check if an http status code matches a certain pattern.
type StatusFilter struct {
	base    int
	divisor int
}

// reStatusFilter ensures that the format is digits optionally followed by X values.
var reStatusFilter = regexp.MustCompile(`^([1-5]\d*)([xX]*)$`)

// ParseStatusFilter will create a new status filter from the string.
func ParseStatusFilter(s string) (StatusFilter, error) {
	m := reStatusFilter.FindStringSubmatch(s)
	if m == nil {
		return StatusFilter{}, fmt.Errorf("status filter must be a digit that starts with 1-5 optionally followed by X characters")
	} else if len(s) != 3 {
		return StatusFilter{}, fmt.Errorf("status filter must be exactly 3 characters long")
	}

	// Compute the divisor and the expected value. If we have one X, we divide by 10 so we are only comparing
	// the first two numbers. If we have two Xs, we divide by 100 so we only compare the first number. We
	// then check if the result is equal to the remaining number.
	base, err := strconv.Atoi(m[1])
	if err != nil {
		return StatusFilter{}, err
	}

	divisor := 1
	switch len(m[2]) {
	case 1:
		divisor = 10
	case 2:
		divisor = 100
	}
	return StatusFilter{
		base:    base,
		divisor: divisor,
	}, nil
}

// Match will check if the status code matches this filter.
func (sf StatusFilter) Match(statusCode int) bool {
	if sf.divisor == 0 {
		return false
	}
	return statusCode/sf.divisor == sf.base
}

// UnmarshalText parses a TOML value into a duration value.
func (sf *StatusFilter) UnmarshalText(text []byte) error {
	f, err := ParseStatusFilter(string(text))
	if err != nil {
		return err
	}
	*sf = f
	return nil
}

// MarshalText converts a duration to a string for decoding toml
func (sf StatusFilter) MarshalText() (text []byte, err error) {
	var buf bytes.Buffer
	if sf.base != 0 {
		buf.WriteString(strconv.Itoa(sf.base))
	}

	switch sf.divisor {
	case 1:
	case 10:
		buf.WriteString("X")
	case 100:
		buf.WriteString("XX")
	default:
		return nil, errors.New("invalid status filter")
	}
	return buf.Bytes(), nil
}

type StatusFilters []StatusFilter

func (filters StatusFilters) Match(statusCode int) bool {
	if len(filters) == 0 {
		return true
	}

	for _, sf := range filters {
		if sf.Match(statusCode) {
			return true
		}
	}
	return false
}
