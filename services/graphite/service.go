// Copyright 2026 openGemini Authors.
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

/*
The listeners, the connection tracking and the creation of the database are adapted from:
https://github.com/influxdata/influxdb/blob/1.8/services/graphite/service.go
Copyright (c) 2013-2018 InfluxData Inc.
*/

// Package graphite provides a service to write data with the Graphite plaintext protocol.
package graphite

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/graphite"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const udpBufferSize = 65536

// maxLineSize is the longest line accepted on a TCP connection.
const maxLineSize = 64 * 1024

// PointsWriter writes the rows parsed from the Graphite lines.
type PointsWriter interface {
	RetryWritePointRows(database, retentionPolicy string, rows []influx.Row) error
}

// MetaClient creates the database and the retention policy the rows are written to.
type MetaClient interface {
	Database(name string) (*meta2.DatabaseInfo, error)
	CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta2.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicy(name string, spec *meta2.RetentionPolicySpec, shardKey *meta2.ShardKeyInfo,
		enableTagArray bool, replicaN uint32) (*meta2.DatabaseInfo, error)
	CreateRetentionPolicy(database string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error)
}

// Service receives metrics in the Graphite plaintext protocol over TCP or UDP,
// and writes them in batches to the configured database and retention policy.
type Service struct {
	bindAddress     string
	database        string
	retentionPolicy string
	protocol        string
	batchSize       int
	batchTimeout    time.Duration
	udpReadBuffer   int

	parser *graphite.Parser
	logger *logger.Logger

	// ready is true once the database and the retention policy exist.
	// It is only accessed by the goroutine writing the batches.
	ready bool

	// rows buffers the parsed rows until they are written in batches.
	rows chan influx.Row

	mu      sync.Mutex
	opened  bool
	closing bool
	conns   map[net.Conn]struct{}
	ln      net.Listener
	udpConn *net.UDPConn
	addr    net.Addr

	// connWg tracks the goroutines producing rows, writeWg the goroutine writing them.
	connWg  sync.WaitGroup
	writeWg sync.WaitGroup

	PointsWriter PointsWriter
	MetaClient   MetaClient
}

// NewService returns a Graphite service for the given config.
func NewService(c graphite.Config) (*Service, error) {
	d := c.WithDefaults()
	if err := d.Validate(); err != nil {
		return nil, fmt.Errorf("invalid graphite config: %v", err)
	}

	parser, err := graphite.NewParserWithOptions(graphite.Options{
		Templates:   d.Templates,
		DefaultTags: d.DefaultTags(),
		Separator:   d.Separator,
	})
	if err != nil {
		return nil, err
	}

	s := &Service{
		bindAddress:     d.BindAddress,
		database:        d.Database,
		retentionPolicy: d.RetentionPolicy,
		protocol:        strings.ToLower(d.Protocol),
		batchSize:       d.BatchSize,
		batchTimeout:    time.Duration(d.BatchTimeout),
		udpReadBuffer:   d.UDPReadBuffer,
		parser:          parser,
		rows:            make(chan influx.Row, d.BatchSize*d.BatchPending),
		conns:           make(map[net.Conn]struct{}),
	}
	s.WithLogger(logger.NewLogger(errno.ModuleWrite))
	return s, nil
}

// WithLogger sets the logger of the service.
func (s *Service) WithLogger(log *logger.Logger) {
	s.logger = log.With(
		zap.String("service", "graphite"),
		zap.String("protocol", s.protocol),
		zap.String("addr", s.bindAddress),
	)
}

// Open starts listening and writing the received metrics.
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.opened {
		return errors.New("graphite service can only be opened once")
	}

	var err error
	switch s.protocol {
	case "tcp":
		err = s.openTCPServer()
	case "udp":
		err = s.openUDPServer()
	default:
		err = fmt.Errorf("unrecognized graphite input protocol %s", s.protocol)
	}
	if err != nil {
		return err
	}
	s.opened = true

	s.writeWg.Add(1)
	go s.processRows()

	s.logger.Info("graphite service started", zap.Stringer("listen", s.addr),
		zap.String("db", s.database), zap.String("rp", s.retentionPolicy),
		zap.Int("batch-size", s.batchSize), zap.Duration("batch-timeout", s.batchTimeout))
	return nil
}

// Close stops listening, closes the connections and writes the rows still buffered.
func (s *Service) Close() error {
	s.mu.Lock()
	if !s.opened || s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	if s.ln != nil {
		_ = s.ln.Close()
	}
	if s.udpConn != nil {
		_ = s.udpConn.Close()
	}
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()

	// No more rows are produced once the listeners and connections are done.
	s.connWg.Wait()
	close(s.rows)
	s.writeWg.Wait()
	return nil
}

// Addr returns the address the service listens on.
func (s *Service) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func (s *Service) openTCPServer() error {
	ln, err := net.Listen("tcp", s.bindAddress)
	if err != nil {
		return err
	}
	s.ln = ln
	s.addr = ln.Addr()

	s.connWg.Add(1)
	go func() {
		defer s.connWg.Done()
		for {
			conn, err := ln.Accept()
			if errors.Is(err, net.ErrClosed) {
				s.logger.Info("graphite TCP listener closed")
				return
			}
			if err != nil {
				s.logger.Warn("graphite failed to accept TCP connection", zap.Error(err))
				continue
			}
			if !s.trackConnection(conn) {
				_ = conn.Close()
				return
			}

			s.connWg.Add(1)
			go s.handleTCPConnection(conn)
		}
	}()
	return nil
}

func (s *Service) trackConnection(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Service) untrackConnection(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
}

func (s *Service) handleTCPConnection(conn net.Conn) {
	defer s.connWg.Done()
	defer s.untrackConnection(conn)
	defer func() {
		_ = conn.Close()
	}()

	reader := bufio.NewReaderSize(conn, maxLineSize)
	for {
		// Read up to the next newline, never buffering more than maxLineSize bytes.
		buf, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			s.logger.Warn("graphite line too long, dropping it", zap.Int("max", maxLineSize))
			if err = discardLine(reader); err != nil {
				return
			}
			continue
		}
		if len(buf) > 0 {
			s.handleLine(strings.TrimSpace(string(buf)))
		}
		if err != nil {
			return
		}
	}
}

// discardLine skips the rest of the current line.
func discardLine(reader *bufio.Reader) error {
	for {
		_, err := reader.ReadSlice('\n')
		if !errors.Is(err, bufio.ErrBufferFull) {
			return err
		}
	}
}

func (s *Service) openUDPServer() error {
	addr, err := net.ResolveUDPAddr("udp", s.bindAddress)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	if s.udpReadBuffer != 0 {
		if err = conn.SetReadBuffer(s.udpReadBuffer); err != nil {
			_ = conn.Close()
			return fmt.Errorf("unable to set UDP read buffer to %d: %s", s.udpReadBuffer, err)
		}
	}
	s.udpConn = conn
	s.addr = conn.LocalAddr()

	s.connWg.Add(1)
	go func() {
		defer s.connWg.Done()
		buf := make([]byte, udpBufferSize)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					s.logger.Error("graphite UDP listener stopped", zap.Error(err))
				}
				_ = conn.Close()
				return
			}

			for _, line := range strings.Split(string(buf[:n]), "\n") {
				s.handleLine(strings.TrimSpace(line))
			}
		}
	}()
	return nil
}

func (s *Service) handleLine(line string) {
	if line == "" {
		return
	}

	point, err := s.parser.Parse(line)
	if err != nil {
		var unsupported *graphite.UnsupportedValueError
		// Graphite ignores NaN values with no error.
		if errors.As(err, &unsupported) && math.IsNaN(unsupported.Value) {
			return
		}
		s.logger.Warn("graphite failed to parse line", zap.String("line", line), zap.Error(err))
		return
	}

	if err = checkPointSize(point); err != nil {
		s.logger.Warn("graphite dropped point", zap.String("line", line), zap.Error(err))
		return
	}

	s.rows <- pointToRow(point)
}

// checkPointSize ensures the point fits the limits of the shard-write serialization.
func checkPointSize(p graphite.Point) error {
	if len(p.Measurement) > util.MaxMeasurementLength {
		return errno.NewError(errno.MeasurementNameTooLong, p.Measurement, util.MaxMeasurementLength, len(p.Measurement))
	}
	if len(p.Field) > util.MaxFieldNameLength {
		return errno.NewError(errno.FieldNameTooLong, p.Field, util.MaxFieldNameLength, len(p.Field))
	}
	for k, v := range p.Tags {
		if len(k) > util.MaxTagNameLength {
			return errno.NewError(errno.TagNameTooLong, k, util.MaxTagNameLength, len(k))
		}
		if len(v) > util.MaxTagValueLength {
			return errno.NewError(errno.TagValueTooLong, k, util.MaxTagValueLength, len(v))
		}
	}
	return nil
}

// pointToRow converts a Graphite point to a row with sorted tags, dropping tags with an empty value.
func pointToRow(p graphite.Point) influx.Row {
	tags := make(influx.PointTags, 0, len(p.Tags))
	for k, v := range p.Tags {
		if v == "" {
			continue
		}
		tags = append(tags, influx.Tag{Key: k, Value: v})
	}
	sort.Sort(&tags)

	return influx.Row{
		Name:      p.Measurement,
		Tags:      tags,
		Fields:    influx.Fields{{Key: p.Field, NumValue: p.Value, Type: influx.Field_Type_Float}},
		Timestamp: p.Timestamp,
	}
}

// processRows writes the rows in batches, once a batch is full or batch-timeout after its first row.
func (s *Service) processRows() {
	defer s.writeWg.Done()

	timer := time.NewTimer(s.batchTimeout)
	timer.Stop()
	batch := make([]influx.Row, 0, s.batchSize)
	flush := func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		if len(batch) == 0 {
			return
		}
		s.writeBatch(batch)
		batch = make([]influx.Row, 0, s.batchSize)
	}

	for {
		select {
		case row, ok := <-s.rows:
			if !ok {
				flush()
				return
			}
			if len(batch) == 0 {
				timer.Reset(s.batchTimeout)
			}
			batch = append(batch, row)
			if len(batch) >= s.batchSize {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

func (s *Service) writeBatch(batch []influx.Row) {
	// Will attempt to create the database if not yet created.
	if err := s.createInternalStorage(); err != nil {
		s.logger.Error("graphite failed to create the database or retention policy", zap.String("db", s.database),
			zap.String("rp", s.retentionPolicy), zap.Int("rows", len(batch)), zap.Error(err))
		return
	}

	if err := s.PointsWriter.RetryWritePointRows(s.database, s.retentionPolicy, batch); err != nil {
		s.logger.Error("graphite failed to write rows", zap.String("db", s.database),
			zap.String("rp", s.retentionPolicy), zap.Int("rows", len(batch)), zap.Error(err))
	}
}

// createInternalStorage ensures that the database and the retention policy exist.
func (s *Service) createInternalStorage() error {
	if s.ready {
		return nil
	}

	db, err := s.MetaClient.Database(s.database)
	if err != nil && !errno.Equal(err, errno.DatabaseNotFound) {
		return err
	}

	switch {
	case db == nil && s.retentionPolicy == "":
		_, err = s.MetaClient.CreateDatabase(s.database, false, 0, nil)
	case db == nil:
		spec := &meta2.RetentionPolicySpec{Name: s.retentionPolicy}
		_, err = s.MetaClient.CreateDatabaseWithRetentionPolicy(s.database, spec, &meta2.ShardKeyInfo{}, false, 0)
	case s.retentionPolicy != "" && db.RetentionPolicy(s.retentionPolicy) == nil:
		replicaN := db.ReplicaN
		spec := &meta2.RetentionPolicySpec{Name: s.retentionPolicy, ReplicaN: &replicaN}
		_, err = s.MetaClient.CreateRetentionPolicy(s.database, spec, false)
	}
	if err != nil {
		return err
	}

	s.ready = true
	return nil
}
