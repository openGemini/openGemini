/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/client.go

2022.01.23 changed.
Add new test case etc.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/
package metaclient

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	RetryGetUserInfoTimeout = 1 * time.Second
	RetryExecTimeout = 1 * time.Second
	RetryReportTimeout = 1 * time.Second
}

type RPCServer struct {
	metaData *meta2.Data
}

func (c *RPCServer) Abort() {

}

func (c *RPCServer) Handle(w spdy.Responser, data interface{}) error {
	metaMsg := data.(*message.MetaMessage)

	switch msg := metaMsg.Data().(type) {
	case *message.SnapshotRequest:
		return c.HandleSnapshot(w, msg)
	}
	return nil
}

func (c *RPCServer) HandleSnapshot(w spdy.Responser, msg *message.SnapshotRequest) error {
	fmt.Printf("server Handle: %+v \n", msg)

	c.metaData = &meta2.Data{
		Index: msg.Index + 1,
	}

	buf, err := c.metaData.MarshalBinary()
	rsp := &message.SnapshotResponse{
		Data: buf,
		Err:  fmt.Sprintf("%v", err),
	}

	return w.Response(message.NewMetaMessage(message.SnapshotResponseMessage, rsp), true)
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

func TestSnapshot(t *testing.T) {
	address := "127.0.0.10:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{logger: logger.NewLogger(errno.ModuleUnknown)}
	data, err := mc.getSnapshot(SQL, nodeId, 0)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if data.Index != server.metaData.Index {
		t.Fatalf("error index; exp: %d; got: %d", server.metaData.Index, data.Index)
	}
}

func TestClient_compareHashAndPwdVerTwo(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	hashed, err := c.genPbkdf2PwdVal("AsdF_789")
	if err != nil {
		t.Error("gen pbkdf2 password failed", zap.Error(err))
	}
	err = c.compareHashAndPwdVerTwo(hashed, "AsdF_789")
	if err != nil {
		t.Error("compare hash and plaintext failed", zap.Error(err))
	}
}

func TestClient_saltedEncryptByVer(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	_, _, err := c.saltedEncryptByVer("AsdF_789", hashAlgoVerOne)
	if err != nil {
		t.Error("encrypt by version one failed", zap.Error(err))
	}
	_, _, err = c.saltedEncryptByVer("AsdF_789", hashAlgoVerTwo)
	if err != nil {
		t.Error("encrypt by version two failed", zap.Error(err))
	}
}

func TestClient_isValidPwd(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	err := c.isValidPwd("1337", "jdoe1")
	require.EqualError(t, err, "the password needs to be between 8 and 256 characters long")

	err = c.isValidPwd("jdoe1jdoe1", "jdoe1jdoe1")
	require.EqualError(t, err, "User passwords must not same with username or username's reverse.")

	err = c.isValidPwd("jdoe1jdoe1", "jdoe1")
	require.EqualError(t, err, "The user password must contain more than 8 characters "+
		"and uppercase letters, lowercase letters, digits, and at least one of "+
		"the special characters.")
}

func TestClient_FieldKeys(t *testing.T) {
	keys := []FieldKey{
		{
			Field:     "f1",
			FieldType: 1,
		},
		{
			Field:     "f3",
			FieldType: 3,
		},

		{
			Field:     "f2",
			FieldType: 2,
		},
	}
	sort.Sort(FieldKeys(keys))
	require.Equal(t, keys[0].Field, "f1")
	require.Equal(t, keys[1].Field, "f2")
	require.Equal(t, keys[2].Field, "f3")
}

func TestClient_ReportCtx(t *testing.T) {
	loadCtx := LoadCtx{}
	// new ReportCtx
	ctx := loadCtx.GetReportCtx()
	ctx.DBPTStat = ctx.GetDBPTStat()
	ctx.DBPTStat.DB = proto.String("db0")
	ctx.DBPTStat.PtID = proto.Uint32(0)
	rpShardStatus := ctx.GetRpStat()
	rpShardStatus = append(rpShardStatus, &proto2.RpShardStatus{
		RpName: proto.String("autogen"),
		ShardStats: &proto2.ShardStatus{
			ShardID:     proto.Uint64(1),
			ShardSize:   proto.Uint64(1),
			SeriesCount: proto.Int32(1),
			MaxTime:     proto.Int64(1),
		},
	})
	ctx.DBPTStat.RpStats = rpShardStatus
	// put ReportCtx to sync pool
	loadCtx.PutReportCtx(ctx)
	ctxUsed := loadCtx.GetReportCtx()

	require.Equal(t, &ctx, &ctxUsed)
	// get ReportCtx from sync pool and check it
	dbptStatus := ctxUsed.GetDBPTStat()
	require.Equal(t, &ctx.DBPTStat, &dbptStatus)
	usedRPShardStatus := ctxUsed.GetRpStat()
	require.Equal(t, ctxUsed.DBPTStat.DB, proto.String(""))
	require.Equal(t, ctxUsed.DBPTStat.PtID, proto.Uint32(0))
	require.Equal(t, len(usedRPShardStatus), 0)
	require.Equal(t, cap(usedRPShardStatus), 1)
	usedRPShardStatus = usedRPShardStatus[:1]
	require.Equal(t, &rpShardStatus, &usedRPShardStatus)
}

func TestClient_BasicFunctions(t *testing.T) {
	client := &Client{
		nodeID: 1,
		cacheData: &meta2.Data{
			PtNumPerNode: 1,
			ClusterID:    1,
			DataNodes: []meta2.DataNode{
				meta2.DataNode{
					NodeInfo: meta2.NodeInfo{
						ID:      1,
						Host:    "127.0.0.1:8090",
						TCPHost: "127.0.0.1:8091",
					},
					ConnID:      0,
					AliveConnID: 0,
				},
			},
			MetaNodes: []meta2.NodeInfo{
				{
					ID:      1,
					Host:    "127.0.0.1:8088",
					TCPHost: "127.0.0.1:8089",
				},
			},
		},
	}

	require.Equal(t, uint64(1), client.NodeID())
	require.NoError(t, client.SetTier("hot"))
	require.NoError(t, client.SetTier("warm"))
	require.NoError(t, client.SetTier("cold"))
	require.EqualError(t, client.SetTier("xxxx"), "invalid tier xxxx")

	require.Equal(t, client.ClusterID(), uint64(1))

	// DataNode
	dataNode, _ := client.DataNode(1)
	require.Equal(t, dataNode.ID, uint64(1))
	require.Equal(t, dataNode.Host, "127.0.0.1:8090")
	_, err := client.DataNode(2)
	require.EqualError(t, err, meta2.ErrNodeNotFound.Error())

	// DataNodes
	dataNodes, _ := client.DataNodes()
	require.Equal(t, dataNodes[0].ID, uint64(1))
	require.Equal(t, dataNodes[0].Host, "127.0.0.1:8090")

	// DataNodeByHTTPHost
	dataNode, _ = client.DataNodeByHTTPHost("127.0.0.1:8090")
	require.Equal(t, dataNode.ID, uint64(1))
	require.Equal(t, dataNode.Host, "127.0.0.1:8090")
	_, err = client.DataNodeByHTTPHost("127.0.0.x")
	require.EqualError(t, err, meta2.ErrNodeNotFound.Error())

	// DataNodeByTCPHost
	dataNode, _ = client.DataNodeByTCPHost("127.0.0.1:8091")
	require.Equal(t, dataNode.ID, uint64(1))
	require.Equal(t, dataNode.Host, "127.0.0.1:8090")
	require.Equal(t, dataNode.TCPHost, "127.0.0.1:8091")
	_, err = client.DataNodeByTCPHost("127.0.0.x")
	require.EqualError(t, err, meta2.ErrNodeNotFound.Error())

	// MetaNodes
	metaNodes, _ := client.MetaNodes()
	require.Equal(t, metaNodes[0].ID, uint64(1))
	require.Equal(t, metaNodes[0].Host, "127.0.0.1:8088")
	require.Equal(t, metaNodes[0].TCPHost, "127.0.0.1:8089")

	// MetaNodeByAddr
	metaNode := client.MetaNodeByAddr("127.0.0.1:8088")
	require.Equal(t, metaNode.ID, uint64(1))
	require.Equal(t, metaNode.Host, "127.0.0.1:8088")
	require.Equal(t, metaNode.TCPHost, "127.0.0.1:8089")
}

func TestClient_UserCmd(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Users: []meta2.UserInfo{
				{
					Name: "admin",
					// Suibian@123
					Hash:  "#Ver:002#6012B9879FAB5709DA59F31C5FA676D7DCFB4D05DAF76EBEEFDF64B10428EB8FD532110FA805B7E13CCBB4FCE8FC14D9CD9417236EF41381171C91D3CCC4C622",
					Admin: true,
				},
				{
					Name: "user1",
					// Suibian@123
					Hash:  "#Ver:002#6012B9879FAB5709DA59F31C5FA676D7DCFB4D05DAF76EBEEFDF64B10428EB8FD532110FA805B7E13CCBB4FCE8FC14D9CD9417236EF41381171C91D3CCC4C622",
					Admin: false,
				},
			},
		},
		logger: logger.NewLogger(errno.ModuleUnknown),
	}

	_, err := c.CreateUser("admin", "Suibian@123", true, false)
	require.NoError(t, err)
	_, err = c.CreateUser("admin", "Suibian@1234", true, false)
	require.EqualError(t, err, meta2.ErrUserExists.Error())
	_, err = c.CreateUser("admin_new", "Suibian@1234", true, false)
	require.EqualError(t, err, meta2.ErrUserForbidden.Error())

	require.EqualError(t, c.DropUser("none"), meta2.ErrUserNotFound.Error())
	require.EqualError(t, c.DropUser("admin"), meta2.ErrUserDropSelf.Error())

	require.EqualError(t, c.UpdateUser("user1", "Suibian@123"), meta2.ErrPwdUsed.Error())
}

func TestClient_CreateDatabaseWithRetentionPolicy(t *testing.T) {
	c := &Client{
		cacheData:   &meta2.Data{},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}
	spec := &meta2.RetentionPolicySpec{Name: "testRp"}
	ski := &meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}}
	_, err := c.CreateDatabaseWithRetentionPolicy("db0", spec, ski)
	require.EqualError(t, err, "execute command timeout")
}

func TestClient_CreateDatabaseWithRetentionPolicy2(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}}}},
		},
	}
	spec := &meta2.RetentionPolicySpec{Name: "testRp"}
	ski := &meta2.ShardKeyInfo{ShardKey: []string{"tag2", "tag3"}}
	_, err := c.CreateDatabaseWithRetentionPolicy("test", spec, ski)
	require.EqualError(t, err, "shard key conflict")
}

func TestClient_Stream(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
					},
				}},
				"db1": {
					Name:     "db1",
					ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp1": {
							Name:         "rp1",
							Duration:     72 * time.Hour,
							Measurements: map[string]*meta2.MeasurementInfo{"mst1": {Name: "mst1"}},
						},
					},
				},
			},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	info := &meta2.StreamInfo{
		Name: "test",
		ID:   0,
		SrcMst: &meta2.StreamMeasurementInfo{
			Name:            "mst0",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		DesMst: &meta2.StreamMeasurementInfo{
			Name:            "mst1",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		Dims: []string{"tag1"},
		Calls: []*meta2.StreamCall{
			{
				Call:  "first",
				Field: "age",
				Alias: "first_age",
			},
			{
				Call:  "first",
				Field: "score",
				Alias: "first_score",
			},
			{
				Call:  "first",
				Field: "name",
				Alias: "first_name",
			},
			{
				Call:  "first",
				Field: "alive",
				Alias: "first_alive",
			},
		},
		Delay: time.Second,
	}
	err := c.CreateStreamPolicy(info)
	require.EqualError(t, err, "execute command timeout")
	err = c.DropStream("test")

	c.cacheData.Streams = map[string]*meta2.StreamInfo{"test": info}
	_, _ = c.ShowStreams("db0", false)
	_, _ = c.ShowStreams("", true)

	stmt := &influxql.SelectStatement{
		Fields: influxql.Fields{
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "age", Type: influxql.Integer}},
				},
			},
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "name", Type: influxql.String}},
				},
			},
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "score", Type: influxql.Float}},
				},
			},
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "alive", Type: influxql.Boolean}},
				},
			},
		},
		Dimensions: influxql.Dimensions{
			{
				Expr: &influxql.VarRef{Val: "tag1", Type: influxql.Tag},
			},
		},
	}
	stmt1 := &influxql.SelectStatement{
		Fields: influxql.Fields{
			{
				Expr: &influxql.VarRef{Val: "tag1", Type: influxql.Tag},
			},
		},
	}
	stmt2 := &influxql.SelectStatement{
		Fields: influxql.Fields{
			{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "tag1", Type: influxql.TAG}},
				},
			},
		},
	}
	c.UpdateStreamMstSchema("db0", "rp0", "mst1", stmt)
	if c.UpdateStreamMstSchema("db0", "rp0", "mst1", stmt1).Error() != "unexpected call function" {
		t.Fatal()
	}
	if c.UpdateStreamMstSchema("db0", "rp0", "mst1", stmt2).Error() != "unexpected call type" {
		t.Fatal()
	}

	infos := c.GetStreamInfos()
	if len(infos) != 1 {
		t.Fatal("not find stream infos")
	}

	infos = c.GetStreamInfosStore()
	if len(infos) != 0 {
		t.Fatal("query from store should not return value in ut")
	}
}

func TestClient_MeasurementInfo(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	_, err := c.GetMeasurementInfoStore("test", "rp0", "test")
	if err == nil {
		t.Fatal("query from store should not return value in ut")
	}
}

func TestClient_Stream_GetStreamInfos(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	info := &meta2.StreamInfo{
		Name: "test",
		ID:   0,
		SrcMst: &meta2.StreamMeasurementInfo{
			Name:            "mst0",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		DesMst: &meta2.StreamMeasurementInfo{
			Name:            "mst1",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		Dims: []string{"tag1", "tag2"},
		Calls: []*meta2.StreamCall{
			{
				Call:  "first",
				Field: "age",
				Alias: "first_age",
			},
		},
		Delay: time.Second,
	}
	c.cacheData.Streams = map[string]*meta2.StreamInfo{"test": info}
	sis := c.GetStreamInfos()
	if _, ok := sis["test"]; !ok {
		t.Fatal("GetStreamInfos failed")
	}
}

func TestClient_Stream_GetDstStreamInfos(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	info := &meta2.StreamInfo{
		Name: "test",
		ID:   0,
		SrcMst: &meta2.StreamMeasurementInfo{
			Name:            "mst0",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		DesMst: &meta2.StreamMeasurementInfo{
			Name:            "mst1",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		Dims: []string{"tag1"},
		Calls: []*meta2.StreamCall{
			{
				Call:  "first",
				Field: "age",
				Alias: "first_age",
			},
		},
		Delay: time.Second,
	}
	c.cacheData.Streams = map[string]*meta2.StreamInfo{"test": info}
	dstSis := &[]*meta2.StreamInfo{}
	db := "db0"
	rp := "rp0"
	exist := c.GetDstStreamInfos(db, rp, dstSis)
	if !exist {
		t.Fatal("GetStreamInfos failed")
	}

	c.cacheData.Streams = map[string]*meta2.StreamInfo{}
	exist = c.GetDstStreamInfos(db, rp, dstSis)
	if exist {
		t.Fatal("GetStreamInfos failed")
	}
}

func TestClient_CreateDownSamplePolicy(t *testing.T) {
	info := &meta2.DownSamplePolicyInfo{
		Calls: []*meta2.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
		},
		DownSamplePolicies: []*meta2.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
		},
		Duration: 240 * time.Hour,
	}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	err := c.NewDownSamplePolicy("test", "rp0", info)
	require.EqualError(t, err, "execute command timeout")
	err = c.DropDownSamplePolicy("test", "rp0", true)
	require.EqualError(t, err, "execute command timeout")

	c.cacheData.Databases["test"].RetentionPolicies["rp0"].DownSamplePolicyInfo = info
	row, _ := c.ShowDownSamplePolicies("test")
	names := []string{"rpName", "field_operator", "duration", "sampleInterval", "timeInterval", "waterMark"}
	values := []interface{}{"rp0", "integer{min,max}", "240h0m0s", "1h0m0s", "25s", "1h0m0s"}
	for i := range row[0].Columns {
		if row[0].Columns[i] != names[i] {
			t.Fatalf("wrong column names")
		}
		if row[0].Values[0][i] != values[i] {
			t.Fatalf("wrong column values")
		}
	}
}

func TestGetDownSampleInfo(t *testing.T) {
	info := &meta2.DownSamplePolicyInfo{
		Calls: []*meta2.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
		},
		DownSamplePolicies: []*meta2.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
		},
		Duration: 240 * time.Hour,
	}
	address := "127.0.0.1:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8491"},
		logger:      logger.NewLogger(errno.ModuleUnknown),
	}

	mc.cacheData.Databases["test"].RetentionPolicies["rp0"].DownSamplePolicyInfo = info
	var data []byte
	_ = data
	data, err = mc.getDownSampleInfo(nodeId)
	_, err = mc.GetMstInfoWithInRp("test", "rp0", []int64{int64(influxql.Integer)})
	_, err = mc.GetDownSamplePolicies()
	ident := &meta2.ShardIdentifier{
		ShardID: 1,
		OwnerDb: "db0",
		Policy:  "rp0",
	}
	err = mc.UpdateShardDownSampleInfo(ident)
	rmfi := meta2.RpMeasurementsFieldsInfo{
		MeasurementInfos: []*meta2.MeasurementFieldsInfo{
			{
				MstName: "mst",
				TypeFields: []*meta2.MeasurementTypeFields{
					{Type: 1, Fields: []string{"field1"}},
				},
			},
		},
	}
	b1, e1 := rmfi.MarshalBinary()
	if e1 != nil {
		t.Fatal(e1)
	}
	_, err1 := mc.unmarshalMstInfoWithInRp(b1)
	if err1 != nil {
		t.Fatal(err1)
	}
	dspi := &meta2.DownSamplePoliciesInfoWithDbRp{
		Infos: []*meta2.DownSamplePolicyInfoWithDbRp{
			{Info: info, DbName: "test", RpName: "rp0"},
		},
	}
	b2, e2 := dspi.MarshalBinary()
	if e2 != nil {
		t.Fatal(e2)
	}
	_, err2 := mc.unmarshalDownSamplePolicies(b2)
	if err2 != nil {
		t.Fatal(err2)
	}
}

func TestUserInfo(t *testing.T) {
	address := "127.0.0.10:8492"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	RetryGetUserInfoTimeout = 1 * time.Second
	mc := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
			Index: 0,
		},
		metaServers: []string{"127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	mc.UpdateUserInfo()
	time.Sleep(2 * time.Second)
}

func TestVerifyDataNodeStatus(t *testing.T) {
	config.SetHaEnable(true)
	defer config.SetHaEnable(false)

	address := "127.0.0.10:8492"
	nodeId := 0
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)
	mc := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		nodeID:      1,
		metaServers: []string{"127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"},
		closing:     make(chan struct{}),
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}

	go mc.verifyDataNodeStatus()
	time.Sleep(2 * time.Second)
}

func TestCreateMeasurement(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	var err error
	invalidMst := []string{"", "/111", ".", "..", "bbb\\aaa", string([]byte{'m', 's', 't', 0, '_', '0', '0'})}

	for _, mst := range invalidMst {
		_, err = c.CreateMeasurement("db0", "rp0", mst, nil, nil)
		require.EqualError(t, err, errno.NewError(errno.InvalidMeasurement, mst).Error())
	}

	validMst := []string{"--", "__", ".-_", "mst....", "...", ".mst", "mst中文", "mst#11"}
	for _, mst := range validMst {
		require.True(t, meta2.ValidMeasurementName(mst))
	}
}

func TestDBPTCtx_String(t *testing.T) {
	ctx := &DBPTCtx{}
	ctx.DBPTStat = &proto2.DBPtStatus{
		DB:   proto.String("db0"),
		PtID: proto.Uint32(100),
		RpStats: []*proto2.RpShardStatus{{
			RpName: proto.String("default"),
			ShardStats: &proto2.ShardStatus{
				ShardID:     proto.Uint64(101),
				ShardSize:   proto.Uint64(102),
				SeriesCount: proto.Int32(103),
				MaxTime:     proto.Int64(104),
			},
		}, nil},
	}

	exp := `DB:"db0" PtID:100 RpStats:<RpName:"default" ShardStats:<ShardID:101 ShardSize:102 SeriesCount:103 MaxTime:104 > > RpStats:<nil> `
	require.Equal(t, exp, ctx.String())
	ctx.DBPTStat = nil
	require.Equal(t, "", ctx.String())
}

func TestClient_RegisterQueryIDOffset(t *testing.T) {
	type fields struct {
		metaServers []string
		cacheData   *meta2.Data
		logger      *logger.Logger
	}

	type args struct {
		host string
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    uint64
	}{
		{
			name: "TimeOut",
			fields: fields{
				metaServers: []string{"127.0.0.1:8092"},
				cacheData: &meta2.Data{
					QueryIDInit: map[string]uint64{},
				},
				logger: logger.NewLogger(errno.ModuleMetaClient),
			},
			args:    args{host: "127.0.0.1:8086"},
			wantErr: true,
		},
		{
			name: "DuplicateRegistration",
			fields: fields{
				metaServers: []string{"127.0.0.1:8092"},
				cacheData: &meta2.Data{
					QueryIDInit: map[string]uint64{"127.0.0.1:8086": 0},
				},
				logger: logger.NewLogger(errno.ModuleMetaClient),
			},
			args:    args{"127.0.0.1:8086"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				metaServers: tt.fields.metaServers,
				cacheData:   tt.fields.cacheData,
				logger:      tt.fields.logger,
			}
			if _, err := c.RegisterQueryIDOffset(tt.args.host); (err != nil) != tt.wantErr {
				t.Errorf("RegisterQueryIDOffset() error = %v, wantErr %v", err, tt.wantErr)
				require.EqualError(t, err, "register query id offset timeout")
			}
		})
	}
}
