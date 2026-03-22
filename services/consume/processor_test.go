// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package consume_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/consume"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
	"github.com/openGemini/openGemini/services/pubsub"
	"github.com/stretchr/testify/require"
)

func TestCreateProcessor(t *testing.T) {
	topic := &consume.Topic{
		Mode:  consume.HistoryMode,
		Uuid:  "",
		Query: `SELECT usage_idle,usage_name FROM db0."default".mst WHERE time>'2024-01-01 00:00:00' AND time<'2024-01-02 00:00:00'`,
	}

	mc := NewMockMetaClient()
	eng := &MockEngine{}
	eng.itr = &MockRecordIterator{total: 1}

	p := consume.NewProcessor(mc, eng)
	err := p.Init(topic)
	require.NoError(t, err)

	ok := false
	params := consume.FetchParams{
		MinRows:   1,
		MaxRows:   10,
		TimeoutMs: 200,
	}
	err = p.Process(params, func(msg protocol.Marshaler) bool {
		ok = true
		return ok
	})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCreateProcessor_Error(t *testing.T) {
	topic := &consume.Topic{
		Mode:  consume.HistoryMode,
		Uuid:  "",
		Query: `show databases`,
	}

	mc := NewMockMetaClient()
	eng := &MockEngine{}

	p := consume.NewProcessor(mc, eng)
	err := p.Init(topic)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid select query")

	topic.Query = "invalid query"
	err = p.Init(topic)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected SELECT")

	topic.Query = "SELECT * FROM (SELECT usage_idle,usage_name FROM db0.\"default\".mst)"
	err = p.Init(topic)
	require.Error(t, err)
	require.Contains(t, err.Error(), "the first source is not a valid measurement")

	// mock create iterator failed
	topic.Query = `SELECT usage_idle,usage_name FROM db0."default".mst WHERE hostname='localhost' AND value>10 AND` +
		` time>'2024-01-01 00:00:00' AND time<'2024-01-02 00:00:00'`
	err = p.Init(topic)
	require.Error(t, err)

	// mock iterate data failed
	eng.itr = &MockRecordIterator{total: 10, err: fmt.Errorf("some error")}
	topic.Query = `SELECT usage_idle,usage_name FROM db0."default".mst WHERE hostname='localhost' AND value>10 AND` +
		` time>'2024-01-01 00:00:00' AND time<'2024-01-02 00:00:00'`
	err = p.Init(topic)
	require.NoError(t, err)
	params := consume.FetchParams{
		MinRows:   1,
		TimeoutMs: 200,
	}
	require.Error(t, p.Process(params, func(msg protocol.Marshaler) bool {
		return true
	}))

	// mock iterate data empty
	eng.itr.err = io.EOF
	topic.Query = `SELECT usage_idle,usage_name FROM db0."default".mst WHERE hostname='localhost' AND value>10 AND` +
		` time>'2024-01-01 00:00:00' AND time<'2024-01-02 00:00:00'`
	err = p.Init(topic)
	require.NoError(t, err)
	require.NoError(t, p.Process(params, func(msg protocol.Marshaler) bool {
		return true
	}))

}

func TestInitProcessorRealTimeMode(t *testing.T) {
	release := initConfig(2)
	defer release()

	lock1 := "1"
	wal := shelf.NewWal(t.TempDir(), &lock1, nil)
	defer wal.MustClose()

	info := &shelf.ShardInfo{}
	mockEvent := &MockWalCreatedEvent{Wal: wal, Info: info, WorkId: 0}
	pubsub.Publish("db0:default", mockEvent)

	topic := &consume.Topic{
		Mode:  consume.RealTimeMode,
		Uuid:  "",
		Query: `SELECT usage_idle,usage_name FROM db0."default".mst WHERE time>'2024-01-01 00:00:00' AND time<'2024-01-02 00:00:00'`,
	}

	mc := NewMockMetaClient()
	eng := &MockEngine{}

	p := consume.NewProcessor(mc, eng)
	defer p.Release()
	require.NoError(t, p.Init(topic))
}

type MockWalCreatedEvent struct {
	Wal    *shelf.Wal
	Info   *shelf.ShardInfo
	WorkId int
}

func (msg *MockWalCreatedEvent) CreateIterator(mst string) record.RecIterator {
	walItr := &MockRecIterator{}
	return walItr
}

func (msg *MockWalCreatedEvent) Ref() {

}

func (msg *MockWalCreatedEvent) UnRef() {

}

func (msg *MockWalCreatedEvent) UniqueId() uint64 {
	return 0
}

type MockRecIterator struct {
}

func (m MockRecIterator) Next() (*record.Record, error) {
	return &record.Record{}, nil
}

func (m MockRecIterator) Release() {
}

type MockMetaClient struct {
	msi *meta.MeasurementInfo
	dbi *meta.DatabaseInfo

	metaclient.MetaClient
}

func NewMockMetaClient() *MockMetaClient {
	mc := &MockMetaClient{}
	mc.msi = createMsi()
	mc.dbi = &meta.DatabaseInfo{
		ShardKey: meta.ShardKeyInfo{},
		Name:     "db0",
		RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
			"rp0": {
				Name:        "rp0",
				IndexGroups: nil,
				Measurements: map[string]*meta.MeasurementInfo{
					"foo": mc.msi,
				},
			},
		},
	}
	return mc
}

func (mc *MockMetaClient) Database(name string) (*meta.DatabaseInfo, error) {
	if name == "not_exists" {
		return nil, nil
	}
	if name == "some_error" {
		return nil, fmt.Errorf("some error")
	}

	return mc.dbi, nil
}

func (mc *MockMetaClient) GetMeasurements(m *influxql.Measurement) ([]*meta.MeasurementInfo, error) {
	return []*meta.MeasurementInfo{mc.msi}, nil
}

func (mc *MockMetaClient) Measurement(database string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	return &meta.MeasurementInfo{Name: mc.msi.Name}, nil
}

func (mc *MockMetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) ([]meta.ShardGroupInfo, error) {
	group := meta.ShardGroupInfo{
		ID:        10,
		StartTime: time.Unix(1704038400, 0), //2024-01-01
		EndTime:   time.Unix(1704038400+86400, 0),
		Shards:    nil,
	}

	for i := range 8 {
		group.Shards = append(group.Shards, meta.ShardInfo{
			ID:      uint64(i + 1),
			Owners:  []uint32{uint32(i + 100)},
			Min:     "",
			Max:     "",
			IndexID: uint64(i + 1),
		})
	}

	return []meta.ShardGroupInfo{group}, nil
}

func (mc *MockMetaClient) GetAliveShards(database string, sgi *meta.ShardGroupInfo, isRead bool) []int {
	return []int{0, 1, 2, 3, 4, 5, 6}
}

func (mc *MockMetaClient) Schema(database string, retentionPolicy string, mst string) (map[string]int32, map[string]struct{}, error) {
	fields := make(map[string]int32)
	dimensions := make(map[string]struct{})
	msti := mc.msi
	callback := func(k string, v int32) {
		if v == influx.Field_Type_Tag {
			dimensions[k] = struct{}{}
		} else {
			fields[k] = v
		}
	}
	msti.Schema.RangeTypCall(callback)
	return fields, dimensions, nil
}

func createMsi() *meta.MeasurementInfo {
	return &meta.MeasurementInfo{
		Name: "foo",
		Schema: &meta.CleanSchema{
			"hostname": meta.SchemaVal{
				Typ:     influx.Field_Type_Tag,
				EndTime: 0,
			},
			"value": meta.SchemaVal{
				Typ:     influx.Field_Type_Float,
				EndTime: 0,
			},
			"region": meta.SchemaVal{
				Typ:     influx.Field_Type_Tag,
				EndTime: 0,
			},
			"time": meta.SchemaVal{
				Typ:     influx.Field_Type_Int,
				EndTime: 0,
			},
			"usage_guest": meta.SchemaVal{
				Typ:     influx.Field_Type_Float,
				EndTime: 0,
			},
			"usage_idle": meta.SchemaVal{
				Typ:     influx.Field_Type_Int,
				EndTime: 0,
			},
			"usage_name": meta.SchemaVal{
				Typ:     influx.Field_Type_String,
				EndTime: 0,
			},
		},
	}
}

type MockEngine struct {
	itr *MockRecordIterator
	engine.Engine
}

func (eng *MockEngine) CreateConsumeIterator(ident util.MeasurementIdent, pts []uint32, opt *query.ProcessorOptions) ([]record.Iterator, func()) {
	if eng.itr == nil {
		return nil, func() {}
	}
	return []record.Iterator{eng.itr}, func() {}
}

func (eng *MockEngine) GetDBPtIds(db string) []uint32 {
	return []uint32{1}
}

type MockRecordIterator struct {
	err   error
	total int
}

func (itr *MockRecordIterator) Next() (*record.ConsumeRecord, error) {
	if itr.err != nil {
		return nil, itr.err
	}

	if itr.total == 0 {
		return nil, io.EOF
	}
	itr.total--

	rec := record.NewRecord(record.Schemas{
		record.Field{
			Type: influx.Field_Type_Int,
			Name: "foo",
		},
		record.Field{
			Type: influx.Field_Type_Int,
			Name: "time",
		},
	}, false)
	rec.ColVals[0].AppendInteger(1)
	rec.ColVals[1].AppendInteger(1)
	res := &record.ConsumeRecord{Rec: rec}
	return res, nil
}

func (itr *MockRecordIterator) Release() {

}

func (itr *MockRecordIterator) SidCnt() int {
	return 0
}

type MockWriter struct {
	err error
	w   *bytes.Buffer
}

func NewMockWriter() *MockWriter {
	return &MockWriter{
		w: bytes.NewBuffer(nil),
	}
}

func (w *MockWriter) Write(p []byte) (int, error) {
	if errors.Is(w.err, io.ErrShortWrite) {
		return 1, nil
	}

	if w.err != nil {
		return 0, w.err
	}

	return w.w.Write(p)
}

func (w *MockWriter) Size() int {
	return w.w.Len()
}

func initConfig(n int) func() {
	conf := config.GetShelfMode()
	conf.Enabled = true
	conf.SeriesHashFactor = 2
	conf.Concurrent = n
	conf.TSSPConvertConcurrent = max(1, n/4)
	conf.ReliabilityLevel = config.ReliabilityLevelHigh

	shelf.Open()
	return func() {
		conf.ReliabilityLevel = config.ReliabilityLevelMedium
		conf.Enabled = false
	}
}
