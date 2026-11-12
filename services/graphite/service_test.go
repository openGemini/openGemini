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

package graphite

import (
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	itoml "github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/graphite"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

type batch struct {
	db, rp string
	rows   []influx.Row
}

type mockPointsWriter struct {
	mu      sync.Mutex
	batches []batch
	err     error
}

func (w *mockPointsWriter) RetryWritePointRows(database, retentionPolicy string, rows []influx.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batches = append(w.batches, batch{db: database, rp: retentionPolicy, rows: rows})
	return w.err
}

func (w *mockPointsWriter) getBatches() []batch {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]batch(nil), w.batches...)
}

type mockMetaClient struct {
	db    *meta2.DatabaseInfo
	dbErr error

	createdDB  string
	createdRP  *meta2.RetentionPolicySpec
	defaultRP  bool
	createErr  error
	numCreates int
}

func (c *mockMetaClient) Database(name string) (*meta2.DatabaseInfo, error) {
	if c.dbErr != nil {
		return nil, c.dbErr
	}
	if c.db == nil {
		return nil, errno.NewError(errno.DatabaseNotFound, name)
	}
	return c.db, nil
}

func (c *mockMetaClient) CreateDatabase(name string, _ bool, _ uint32, _ *obs.ObsOptions) (*meta2.DatabaseInfo, error) {
	c.numCreates++
	c.createdDB = name
	return nil, c.createErr
}

func (c *mockMetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta2.RetentionPolicySpec, _ *meta2.ShardKeyInfo,
	_ bool, _ uint32) (*meta2.DatabaseInfo, error) {
	c.numCreates++
	c.createdDB = name
	c.createdRP = spec
	return nil, c.createErr
}

func (c *mockMetaClient) CreateRetentionPolicy(_ string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error) {
	c.numCreates++
	c.createdRP = spec
	c.defaultRP = makeDefault
	return nil, c.createErr
}

func newTestService(t *testing.T, c graphite.Config) (*Service, *mockPointsWriter, *mockMetaClient) {
	if c.BindAddress == "" {
		c.BindAddress = "127.0.0.1:0"
	}
	s, err := NewService(c)
	require.NoError(t, err)

	w := &mockPointsWriter{}
	m := &mockMetaClient{}
	s.PointsWriter = w
	s.MetaClient = m
	return s, w, m
}

func waitBatches(t *testing.T, w *mockPointsWriter, n int) []batch {
	require.Eventually(t, func() bool {
		return len(w.getBatches()) >= n
	}, 10*time.Second, 10*time.Millisecond)
	return w.getBatches()
}

func TestService_TCP(t *testing.T) {
	s, w, m := newTestService(t, graphite.Config{
		BatchSize:    2,
		BatchTimeout: itoml.Duration(time.Hour),
		Templates:    []string{"servers.* .host.measurement.field region=us-west"},
		Tags:         []string{"dc=dc1", "host=should not be set"},
	})
	require.NoError(t, s.Open())
	defer func() {
		require.NoError(t, s.Close())
	}()

	conn, err := net.Dial("tcp", s.Addr().String())
	require.NoError(t, err)
	_, err = conn.Write([]byte("servers.server01.cpu.load 0.5 1435077219\n\nservers.server02.cpu.idle 99 1435077220"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	batches := waitBatches(t, w, 1)
	require.Len(t, batches, 1)
	require.Equal(t, graphite.DefaultDatabase, batches[0].db)
	require.Equal(t, "", batches[0].rp)
	require.Equal(t, []influx.Row{
		{
			Name: "cpu",
			Tags: influx.PointTags{
				{Key: "dc", Value: "dc1"},
				{Key: "host", Value: "server01"},
				{Key: "region", Value: "us-west"},
			},
			Fields:    influx.Fields{{Key: "load", NumValue: 0.5, Type: influx.Field_Type_Float}},
			Timestamp: time.Unix(1435077219, 0).UnixNano(),
		},
		{
			Name: "cpu",
			Tags: influx.PointTags{
				{Key: "dc", Value: "dc1"},
				{Key: "host", Value: "server02"},
				{Key: "region", Value: "us-west"},
			},
			Fields:    influx.Fields{{Key: "idle", NumValue: 99, Type: influx.Field_Type_Float}},
			Timestamp: time.Unix(1435077220, 0).UnixNano(),
		},
	}, batches[0].rows)
	require.Equal(t, graphite.DefaultDatabase, m.createdDB)
	require.Equal(t, 1, m.numCreates)
}

func TestService_TCPLongLine(t *testing.T) {
	s, w, _ := newTestService(t, graphite.Config{BatchSize: 1, BatchTimeout: itoml.Duration(time.Hour)})
	require.NoError(t, s.Open())
	defer func() {
		require.NoError(t, s.Close())
	}()

	conn, err := net.Dial("tcp", s.Addr().String())
	require.NoError(t, err)
	long := strings.Repeat("a", 3*maxLineSize)
	_, err = conn.Write([]byte(long + " 1 1435077219\ncpu 2 1435077220\n"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	batches := waitBatches(t, w, 1)
	require.Len(t, batches, 1)
	require.Equal(t, "cpu", batches[0].rows[0].Name)
}

func TestService_DropOversizedPoint(t *testing.T) {
	s, w, _ := newTestService(t, graphite.Config{BatchSize: 1, BatchTimeout: itoml.Duration(time.Hour)})
	require.NoError(t, s.Open())
	defer func() {
		require.NoError(t, s.Close())
	}()

	conn, err := net.Dial("tcp", s.Addr().String())
	require.NoError(t, err)
	long := strings.Repeat("a", util.MaxMeasurementLength+1)
	_, err = conn.Write([]byte(long + " 1 1435077219\ncpu 2 1435077220\n"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	batches := waitBatches(t, w, 1)
	require.Len(t, batches, 1)
	require.Equal(t, "cpu", batches[0].rows[0].Name)
}

func TestService_UDP(t *testing.T) {
	s, w, _ := newTestService(t, graphite.Config{
		Protocol:        "UDP",
		Database:        "db0",
		RetentionPolicy: "rp0",
		BatchSize:       100,
		BatchTimeout:    itoml.Duration(10 * time.Millisecond),
		UDPReadBuffer:   udpBufferSize,
	})
	require.NoError(t, s.Open())
	defer func() {
		require.NoError(t, s.Close())
	}()

	conn, err := net.Dial("udp", s.Addr().String())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()
	_, err = conn.Write([]byte("cpu.load 1 1435077219\ncpu.load NaN 1435077220\nbad line\ncpu.load 2 1435077221\n"))
	require.NoError(t, err)

	// the batch is written after batch-timeout since it is never full
	batches := waitBatches(t, w, 1)
	require.Len(t, batches, 1)
	require.Equal(t, "db0", batches[0].db)
	require.Equal(t, "rp0", batches[0].rp)
	require.Len(t, batches[0].rows, 2)
	for i, row := range batches[0].rows {
		require.Equal(t, "cpu.load", row.Name)
		require.Empty(t, row.Tags)
		require.Equal(t, influx.Fields{{Key: graphite.DefaultField, NumValue: float64(i + 1), Type: influx.Field_Type_Float}}, row.Fields)
	}
}

func TestService_CloseWritesBufferedRows(t *testing.T) {
	s, w, _ := newTestService(t, graphite.Config{
		BatchSize:    100,
		BatchTimeout: itoml.Duration(time.Hour),
	})
	require.NoError(t, s.Open())

	s.handleLine("cpu 1 1435077219")
	s.handleLine("cpu 2 1435077220")
	require.NoError(t, s.Close())

	batches := w.getBatches()
	require.Len(t, batches, 1)
	require.Len(t, batches[0].rows, 2)

	// closing again is a no-op
	require.NoError(t, s.Close())
}

func TestService_OpenClose(t *testing.T) {
	s, _, _ := newTestService(t, graphite.Config{})
	// closing a service that is not opened is fine
	require.NoError(t, s.Close())

	require.NoError(t, s.Open())
	require.EqualError(t, s.Open(), "graphite service can only be opened once")

	// the address is in use
	other, _, _ := newTestService(t, graphite.Config{BindAddress: s.Addr().String()})
	require.Error(t, other.Open())
	require.NoError(t, other.Close())

	require.NoError(t, s.Close())
}

func TestNewService_InvalidConfig(t *testing.T) {
	_, err := NewService(graphite.Config{Protocol: "http"})
	require.EqualError(t, err, `invalid graphite config: unrecognized protocol "http", must be tcp or udp`)

	_, err = NewService(graphite.Config{Templates: []string{"host.region"}})
	require.EqualError(t, err, "invalid graphite config: no measurement in template `host.region`")
}

func TestPointToRow(t *testing.T) {
	row := pointToRow(graphite.Point{
		Measurement: "cpu",
		Tags:        map[string]string{"zone": "a", "host": "server01", "empty": "", "dc": "dc1"},
		Field:       "value",
		Value:       1.5,
		Timestamp:   100,
	})
	require.Equal(t, influx.Row{
		Name: "cpu",
		Tags: influx.PointTags{
			{Key: "dc", Value: "dc1"},
			{Key: "host", Value: "server01"},
			{Key: "zone", Value: "a"},
		},
		Fields:    influx.Fields{{Key: "value", NumValue: 1.5, Type: influx.Field_Type_Float}},
		Timestamp: 100,
	}, row)
}

func TestService_CreateInternalStorage(t *testing.T) {
	newDB := func(rps ...string) *meta2.DatabaseInfo {
		db := &meta2.DatabaseInfo{Name: "db0", ReplicaN: 3, RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{}}
		for _, rp := range rps {
			db.RetentionPolicies[rp] = &meta2.RetentionPolicyInfo{Name: rp}
		}
		if len(rps) > 0 {
			db.DefaultRetentionPolicy = rps[0]
		}
		return db
	}

	t.Run("create database", func(t *testing.T) {
		s, _, m := newTestService(t, graphite.Config{Database: "db0"})
		require.NoError(t, s.createInternalStorage())
		require.Equal(t, "db0", m.createdDB)
		require.Nil(t, m.createdRP)

		// the database is only created once
		require.NoError(t, s.createInternalStorage())
		require.Equal(t, 1, m.numCreates)
	})

	t.Run("create database with retention policy", func(t *testing.T) {
		s, _, m := newTestService(t, graphite.Config{Database: "db0", RetentionPolicy: "rp0"})
		require.NoError(t, s.createInternalStorage())
		require.Equal(t, "db0", m.createdDB)
		require.Equal(t, &meta2.RetentionPolicySpec{Name: "rp0"}, m.createdRP)
	})

	t.Run("create retention policy", func(t *testing.T) {
		s, _, m := newTestService(t, graphite.Config{Database: "db0", RetentionPolicy: "rp0"})
		m.db = newDB("autogen")
		require.NoError(t, s.createInternalStorage())
		require.Equal(t, "", m.createdDB)
		replicaN := 3
		require.Equal(t, &meta2.RetentionPolicySpec{Name: "rp0", ReplicaN: &replicaN}, m.createdRP)
		require.False(t, m.defaultRP)
	})

	t.Run("database and retention policy exist", func(t *testing.T) {
		for _, rp := range []string{"", "rp0"} {
			s, _, m := newTestService(t, graphite.Config{Database: "db0", RetentionPolicy: rp})
			m.db = newDB("autogen", "rp0")
			require.NoError(t, s.createInternalStorage())
			require.Equal(t, 0, m.numCreates)
		}
	})

	t.Run("failed to get database", func(t *testing.T) {
		s, w, m := newTestService(t, graphite.Config{Database: "db0"})
		m.dbErr = errors.New("meta error")
		require.EqualError(t, s.createInternalStorage(), "meta error")

		// the batch is dropped
		s.writeBatch([]influx.Row{{Name: "cpu"}})
		require.Empty(t, w.getBatches())

		// creating the database is retried with the next batch
		m.dbErr = nil
		s.writeBatch([]influx.Row{{Name: "cpu"}})
		require.Len(t, w.getBatches(), 1)
		require.Equal(t, 1, m.numCreates)
	})

	t.Run("failed to create database", func(t *testing.T) {
		s, w, m := newTestService(t, graphite.Config{Database: "db0"})
		m.createErr = errors.New("create error")
		s.writeBatch([]influx.Row{{Name: "cpu"}})
		require.Empty(t, w.getBatches())
		require.False(t, s.ready)
	})
}

func TestService_WriteError(t *testing.T) {
	s, w, _ := newTestService(t, graphite.Config{})
	w.err = errors.New("write error")

	// the failed batch is dropped and the next batch is still written
	s.writeBatch([]influx.Row{{Name: "cpu"}})
	s.writeBatch([]influx.Row{{Name: "mem"}})
	require.Len(t, w.getBatches(), 2)
}
