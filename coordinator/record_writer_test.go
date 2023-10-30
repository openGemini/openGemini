/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package coordinator

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/stretchr/testify/assert"
)

var writeRec = &WriteRes{}

type WriteRes struct {
	db      []string
	rp      []string
	mst     []string
	ptId    []uint32
	shardID []uint64
	recs    []*record.Record
}

type MockStorageEngine struct{}

func NewMockStorageEngine() *MockStorageEngine {
	return &MockStorageEngine{}
}

func (w *MockStorageEngine) WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, _ []byte) error {
	writeRec.recs = append(writeRec.recs, rec.Clone())
	writeRec.db = append(writeRec.db, db)
	writeRec.rp = append(writeRec.rp, rp)
	writeRec.mst = append(writeRec.mst, mst)
	writeRec.ptId = append(writeRec.ptId, ptId)
	writeRec.shardID = append(writeRec.shardID, shardID)
	return nil
}

var StrValuePad = "aaaaabbbbbcccccdddddeeeeefffffggggghhhhhiiiijjjjj"

func MockArrowRecords(numRec, numRowPerRec int) []array.Record {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int", Type: arrow.PrimitiveTypes.Int64},
			{Name: "float", Type: arrow.PrimitiveTypes.Float64},
			{Name: "boolean", Type: &arrow.BooleanType{}},
			{Name: "string", Type: &arrow.StringType{}},
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	b.Retain()

	recs := make([]array.Record, 0, numRec)
	now := time.Now().UnixNano()
	for i := 0; i < numRec; i++ {
		for j := 0; j < numRowPerRec; j++ {
			b.Field(0).(*array.Int64Builder).Append(int64(i*numRec + j))
			b.Field(1).(*array.Float64Builder).Append(float64(i*numRec + j))
			if (i*numRec+j)%2 == 0 {
				b.Field(2).(*array.BooleanBuilder).Append(true)
			} else {
				b.Field(2).(*array.BooleanBuilder).Append(false)
			}
			b.Field(3).(*array.StringBuilder).Append(fmt.Sprintf("%s-%d", StrValuePad, i*numRec+j))
			b.Field(4).(*array.Int64Builder).Append(now + int64(i*numRec+j))
		}
		rec := b.NewRecord()
		rec.Retain()
		recs = append(recs, rec)
	}
	return recs
}

type RPCServer struct {
	metaData *meta.Data
}

func (c *RPCServer) Abort() {}

func (c *RPCServer) Handle(w spdy.Responser, data interface{}) error {
	metaMsg := data.(*message.MetaMessage)
	switch msg := metaMsg.Data().(type) {
	case *message.CreateNodeRequest:
		return c.HandleCreateNode(w, msg)
	}
	return nil
}

func (c *RPCServer) HandleCreateNode(w spdy.Responser, msg *message.CreateNodeRequest) error {
	fmt.Printf("server HandleCreateNode: %+v \n", msg)
	nodeStartInfo := meta.NodeStartInfo{}
	nodeStartInfo.NodeId = 1
	buf, _ := nodeStartInfo.MarshalBinary()
	rsp := &message.CreateNodeResponse{
		Data: buf,
		Err:  "",
	}
	return w.Response(message.NewMetaMessage(message.CreateNodeResponseMessage, rsp), true)
}

var server = &RPCServer{}

func startServer(address string) (*spdy.RRCServer, error) {
	rrcServer := spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", address)
	rrcServer.RegisterEHF(transport.NewEventHandlerFactory(spdy.MetaRequest, server, &message.MetaMessage{}))
	if err := rrcServer.Start(); err != nil {
		return nil, err
	}
	return rrcServer, nil
}

func startClient(t *testing.T) {
	address := "127.0.0.10:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)
	metaPath := filepath.Join(t.TempDir(), "meta")
	metaclient.NewClient(metaPath, false, 20)
}

func TestRetryWriteRecord(t *testing.T) {
	var err error
	db := "db0"
	rp := "rp0"
	mst := "rtt"
	numRec, numRowPerRec := 100, 8192

	// get the cur nodeId
	startClient(t)

	// build the record writer
	rw := NewRecordWriter(10*time.Second, 1, 2)
	defer func() {
		if err = rw.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	streamDistribution = diffDis
	engineType = config.COLUMNSTORE
	rw.MetaClient = NewMockMetaClient()
	rw.StorageEngine = NewMockStorageEngine()
	if err = rw.Open(); err != nil {
		t.Fatal(err)
	}
	idx := 0
	recs := MockArrowRecords(numRec, numRowPerRec)

	// write the record
	for idx < len(recs)-1 {
		if err = rw.RetryWriteRecord(db, rp, mst, recs[idx]); err != nil {
			t.Fatal(err)
		}
		idx++
	}

	// build an scenario to cover the err
	if err = rw.RetryWriteRecord(db, rp, "rtt111", recs[len(recs)-1]); err != nil {
		t.Fatal(err)
	}

	// read the record
	now := time.Now()
	for {
		if len(writeRec.recs) >= numRec || time.Since(now) >= 3*time.Second {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var rpInfo = NewRetentionPolicy("rp0", time.Hour, engineType)

type MockRWMetaClient struct {
	DatabaseErr           error
	RetentionPolicyErr    error
	CreateShardGroupErr   error
	DBPtViewErr           error
	MeasurementErr        error
	CreateMeasurementErr  error
	GetShardInfoByTimeErr error
}

func (c *MockRWMetaClient) Database(_ string) (di *meta.DatabaseInfo, err error) {
	if c.DatabaseErr != nil {
		return nil, c.DatabaseErr
	}

	rpInfo.MarkDeleted = true
	database := &meta.DatabaseInfo{Name: "db0", RetentionPolicies: map[string]*meta.RetentionPolicyInfo{"rp0": rpInfo},
		ShardKey: meta.ShardKeyInfo{ShardKey: []string{"tk1", "tk2"}}}
	return database, nil
}

func (c *MockRWMetaClient) RetentionPolicy(_, _ string) (*meta.RetentionPolicyInfo, error) {
	return nil, c.RetentionPolicyErr
}

func (c *MockRWMetaClient) CreateShardGroup(_, _ string, _ time.Time, _ config.EngineType) (*meta.ShardGroupInfo, error) {
	return nil, c.CreateShardGroupErr
}

func (c *MockRWMetaClient) DBPtView(_ string) (meta.DBPtInfos, error) {
	return nil, c.DBPtViewErr
}

func (c *MockRWMetaClient) Measurement(_ string, _ string, _ string) (*meta.MeasurementInfo, error) {
	return nil, c.MeasurementErr
}

func (c *MockRWMetaClient) UpdateSchema(_ string, _ string, _ string, _ []*proto.FieldSchema) error {
	return nil
}

func (c *MockRWMetaClient) CreateMeasurement(_ string, _ string, _ string, _ *meta.ShardKeyInfo, _ *meta.IndexRelation, _ config.EngineType, _ *meta.ColStoreInfo, _ []*proto.FieldSchema) (*meta.MeasurementInfo, error) {
	return nil, c.CreateMeasurementErr
}

func (c *MockRWMetaClient) GetShardInfoByTime(_, _ string, _ time.Time, _ int, _ uint64, _ config.EngineType) (*meta.ShardInfo, error) {
	if c.GetShardInfoByTimeErr != nil {
		return nil, c.GetShardInfoByTimeErr
	}
	return &rpInfo.ShardGroups[0].Shards[0], nil
}

func TestRetryWriteRecordErr(t *testing.T) {
	ctx := getWriteRecCtx()
	defer putWriteRecCtx(ctx)

	db, rp, mst := "db0", "rp0", "mst0"
	err := ctx.checkDBRP(db, rp, &MockRWMetaClient{DatabaseErr: io.EOF})
	assert.Equal(t, err, io.EOF)

	err = ctx.checkDBRP(db, "", &MockRWMetaClient{})
	assert.Equal(t, err, meta.ErrRetentionPolicyNotFound(""))

	mstInfo := NewMeasurement(mst, config.COLUMNSTORE)
	sameSchema := true
	mstInfo1, err := createMeasurement(db, rp, mst, &MockRWMetaClient{}, &mstInfo, &sameSchema, config.COLUMNSTORE)
	assert.Equal(t, err, nil)
	assert.Equal(t, mstInfo, mstInfo1)

	sameSchema = false
	_, err = createMeasurement(db, rp, mst, &MockRWMetaClient{MeasurementErr: meta.ErrMeasurementNotFound, CreateMeasurementErr: io.EOF}, &mstInfo, &sameSchema, config.COLUMNSTORE)
	assert.Equal(t, err, io.EOF)

	preSg := &rpInfo.ShardGroups[0]
	sg, _, _ := createShardGroup(db, rp, &MockRWMetaClient{}, &preSg, time.Now(), config.COLUMNSTORE)
	assert.Equal(t, sg, &rpInfo.ShardGroups[0])

	wh := newRecordWriterHelper(&MockRWMetaClient{RetentionPolicyErr: io.EOF}, 0)
	_, err = wh.createShardGroupsByTimeRange(db, rp, time.Now(), time.Now().Add(time.Hour), config.ENGINETYPEEND)
	assert.Equal(t, err, io.EOF)

	wh = newRecordWriterHelper(&MockRWMetaClient{CreateShardGroupErr: io.EOF}, 0)
	_, _, err = wh.createShardGroup(db, rp, time.Now(), config.COLUMNSTORE)
	assert.Equal(t, err, io.EOF)

	wh = newRecordWriterHelper(&MockRWMetaClient{GetShardInfoByTimeErr: io.EOF}, 0)
	_, err = wh.GetShardByTime(nil, db, rp, time.Now(), 0, config.COLUMNSTORE)
	assert.Equal(t, err, io.EOF)

	recs := MockArrowRecords(1, 1)

	rec := record.NewRecord(record.ArrowSchemaToNativeSchema(recs[0].Schema()), false)
	err = record.ArrowRecordToNativeRecord(recs[0], rec)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, SearchLowerBoundOfRec(rec, &rpInfo.ShardGroups[0], 2), -1)
}

type MockStorageEngineErr struct {
	retry bool
}

func (w *MockStorageEngineErr) WriteRec(_, _, _ string, _ uint32, _ uint64, _ *record.Record, _ []byte) error {
	if w.retry {
		return errno.NewError(errno.NoConnectionAvailable)
	}
	return io.EOF
}

func TestRetryWriteRecordWriteErr(t *testing.T) {
	var err error
	db, rp, mst := "db0", "rp0", "rtt"
	// build the record writer
	rw := NewRecordWriter(10*time.Second, 1, 2)
	streamDistribution = diffDis
	engineType = config.COLUMNSTORE
	client := NewMockMetaClient()
	rw.MetaClient = client
	rw.StorageEngine = NewMockStorageEngine()

	recs := MockArrowRecords(1, 0)
	err = rw.writeRecord(db, rp, mst, recs[0], 0)
	assert.Equal(t, err, nil)

	recs = MockArrowRecords(1, 1)

	client1 := &MockRWMetaClient{DatabaseErr: io.EOF}
	rw.recWriterHelpers = append(rw.recWriterHelpers, newRecordWriterHelper(client1, 0))
	err = rw.writeRecord(db, rp, mst, recs[0], 0)
	assert.Equal(t, err, io.EOF)

	client1 = &MockRWMetaClient{CreateShardGroupErr: io.EOF}
	rw.recWriterHelpers[0] = newRecordWriterHelper(client1, 0)
	err = rw.writeRecord(db, "", mst, recs[0], 0)
	assert.Equal(t, err, meta.ErrRetentionPolicyNotFound(""))

	rec := record.NewRecord(record.ArrowSchemaToNativeSchema(recs[0].Schema()), false)
	err = record.ArrowRecordToNativeRecord(recs[0], rec)

	rw.StorageEngine = &MockStorageEngineErr{retry: true}
	_, err = rw.writeShard(&rpInfo.ShardGroups[0].Shards[0], db, rp, mst, rec, nil)
	assert.Equal(t, errno.Equal(err, errno.NoConnectionAvailable), true)

	rw.StorageEngine = &MockStorageEngineErr{retry: false}
	_, err = rw.writeShard(&rpInfo.ShardGroups[0].Shards[0], db, rp, mst, rec, nil)
	assert.Equal(t, err, io.EOF)

	client1 = &MockRWMetaClient{DatabaseErr: io.EOF}
	msg := &RecMsg{Database: "db0", RetentionPolicy: "rp0", Measurement: "mst0", Rec: recs[0]}
	rw.processRecord(msg, 0)
}

func TestSplitAndWriteByShardErr(t *testing.T) {
	var err error
	db, rp, mst := "db0", "rp0", "rtt"
	// build the record writer
	rw := NewRecordWriter(10*time.Second, 1, 2)
	recs := MockArrowRecords(1, 1)
	rec := record.NewRecord(record.ArrowSchemaToNativeSchema(recs[0].Schema()), false)
	err = record.ArrowRecordToNativeRecord(recs[0], rec)
	sgi := make([]*meta.ShardGroupInfo, 1)
	sgi[0] = &meta.ShardGroupInfo{}
	sgi[0].StartTime = time.Unix(0, 0)
	sgi[0].EndTime = time.Unix(0, 1)
	err = rw.splitAndWriteByShard(sgi, db, rp, mst, rec, 0, config.COLUMNSTORE)
	assert.Equal(t, errno.Equal(err, errno.ArrowFlightGetShardGroupErr), true)
}

func MockArrowRecord1() array.Record {
	schema := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBuilder(memory.DefaultAllocator, schema).NewRecord()
}

func MockArrowRecord2() array.Record {
	schema := arrow.NewSchema([]arrow.Field{{Name: "int1", Type: arrow.PrimitiveTypes.Int64}, {Name: "time", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBuilder(memory.DefaultAllocator, schema).NewRecord()
}

func MockArrowRecord3() array.Record {
	schema := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.PrimitiveTypes.Float64}, {Name: "int", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBuilder(memory.DefaultAllocator, schema).NewRecord()
}

func MockArrowRecord4() array.Record {
	schema := arrow.NewSchema([]arrow.Field{{Name: "int", Type: arrow.PrimitiveTypes.Float64}, {Name: "time", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBuilder(memory.DefaultAllocator, schema).NewRecord()
}

func TestCheckAndUpdateSchema(t *testing.T) {
	rw := newRecordWriterHelper(NewMockMetaClient(), 0)
	_, _, _, err := rw.checkAndUpdateSchema("db0", "rp0", "mst0", MockArrowRecord1())
	assert.Equal(t, errno.Equal(err, errno.ColumnStoreColNumErr), true)

	_, _, _, err = rw.checkAndUpdateSchema("db0", "rp0", "mst0", MockArrowRecord2())
	assert.Equal(t, errno.Equal(err, errno.ColumnStoreSchemaNullErr), true)

	rw.preMst = NewMeasurement("rtt", config.COLUMNSTORE)
	rw.preMst.ColStoreInfo = nil
	_, _, _, err = rw.checkAndUpdateSchema("db0", "rp0", "rtt", MockArrowRecord2())
	assert.Equal(t, errno.Equal(err, errno.ColumnStorePrimaryKeyNullErr), true)

	rw.preMst.ColStoreInfo = &meta.ColStoreInfo{PrimaryKey: []string{"int1"}}
	_, _, _, err = rw.checkAndUpdateSchema("db0", "rp0", "rtt", MockArrowRecord4())
	assert.Equal(t, errno.Equal(err, errno.ColumnStorePrimaryKeyLackErr), true)

	rw.preMst.ColStoreInfo = &meta.ColStoreInfo{PrimaryKey: []string{"time"}}
	_, _, _, err = rw.checkAndUpdateSchema("db0", "rp0", "rtt", MockArrowRecord3())
	assert.Equal(t, errno.Equal(err, errno.ArrowRecordTimeFieldErr), true)

	_, _, _, err = rw.checkAndUpdateSchema("db0", "rp0", "rtt", MockArrowRecord4())
	assert.Equal(t, errno.Equal(err, errno.ColumnStoreFieldTypeErr), true)
}
