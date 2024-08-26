/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package raftconn

import (
	"context"
	"fmt"
	"testing"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type mockRaftNodeTransfer struct {
	raft.Node
	leader uint64
}

func (m *mockRaftNodeTransfer) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	m.leader = transferee
}

func (m *mockRaftNodeTransfer) Status() raft.Status {
	return raft.Status{BasicStatus: raft.BasicStatus{SoftState: raft.SoftState{Lead: m.leader}}}
}

func (m *mockRaftNodeTransfer) Step(ctx context.Context, msg raftpb.Message) error {
	return nil
}

func TestStepRaftMessages(t *testing.T) {
	node := &RaftNode{
		nodeId: 1,
		id:     1,
		ctx:    context.TODO(),
		node:   &mockRaftNodeTransfer{},
	}
	m1 := raftpb.Message{Type: raftpb.MsgApp, To: 1, From: 0, Term: 1, LogTerm: 0, Index: 1}
	m2 := raftpb.Message{Type: raftpb.MsgApp, To: 2, From: 0, Term: 1, LogTerm: 0, Index: 2}
	node.StepRaftMessage([]raftpb.Message{m1, m2})
}

func TestSend_ToSelf(t *testing.T) {
	node := &RaftNode{
		nodeId: 1,
		id:     1,
		peers: map[uint32]uint64{
			0: 1,
		},
	}
	node.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	msg := raftpb.Message{To: 1}
	node.send(msg)
}

type mockNetStorage struct {
	sendRaftMessagesErr error
}

func (ns mockNetStorage) SendRaftMessages(nodeID uint64, database string, pt uint32, msgs raftpb.Message) error {
	return ns.sendRaftMessagesErr
}

func (ns mockNetStorage) Client() metaclient.MetaClient {
	return nil
}

func TestSend_ToOther(t *testing.T) {
	node := &RaftNode{
		nodeId: 1,
		id:     1,
		peers: map[uint32]uint64{
			0: 1,
			1: 2,
		},
		ISend: &mockNetStorage{},
	}
	node.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	msg := raftpb.Message{To: 2}
	node.send(msg)
}

func TestSend_SendRaftMsgError(t *testing.T) {
	node := &RaftNode{
		nodeId: 1,
		id:     1,
		peers: map[uint32]uint64{
			0: 1,
			1: 2,
		},
		ISend: &mockNetStorage{
			sendRaftMessagesErr: fmt.Errorf("mock error"),
		},
	}
	node.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	msg := raftpb.Message{To: 2}
	node.send(msg)
}

func TestTransferLeadership(t *testing.T) {
	n := &RaftNode{
		node:   &mockRaftNodeTransfer{leader: 1},
		ctx:    context.TODO(),
		logger: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop()),
	}

	newLeader := uint64(2)
	n.TransferLeadership(newLeader)
	if n.node.Status().Lead != newLeader {
		t.Errorf("expected leader %d, got %d", newLeader, n.node.Status().Lead)
	}
}

func TestSnapShot(t *testing.T) {
	tmpDir := t.TempDir()
	rds, err := raftlog.Init(tmpDir)
	defer rds.Close()
	entsInit := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	rds.SaveEntries(nil, entsInit, nil)
	mockTransPeers := make(map[uint32]uint64)
	client := &MockMetaClient{}
	mockPeers := []raft.Peer{
		{ID: 2}, {ID: 3},
	}
	node := StartNode(rds, 1, "db", 1, mockPeers, client, mockTransPeers)
	node.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	node.SetConfState(&raftpb.ConfState{})
	node.SnapShotter.CommittedIndex = 4
	err = node.snapShot()
	require.Nil(t, err)

	node.SnapShotter.CommittedIndex = 1
	err = node.snapShot()
	require.Nil(t, err)
}

func TestReplay(t *testing.T) {
	tmpDir := t.TempDir()
	rds, err := raftlog.Init(tmpDir)
	if err != nil {
		t.Errorf("Test replay failed, raft disk storage init failed, err is %v", err)
	}
	mockNode := &RaftNode{
		proposeC: make(chan []byte, 1),
		Store:    rds,
	}
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	hardState := &raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 5,
	}
	err = rds.Save(hardState, ents, nil)
	defer rds.Close()
	if err != nil {
		t.Errorf("raft disk storage save failed")
	}
	snapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 3,
		},
	}
	err = mockNode.replay(snapshot)
	require.Nil(t, err)

	snapshot2 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
		},
	}
	err = mockNode.replay(snapshot2)
	require.NotNilf(t, err, "err is not nil")

	snapshot3 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 0,
		},
	}
	err = mockNode.replay(snapshot3)
	require.NotNilf(t, err, "err is not nil")

	snapshot4 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 6,
		},
	}
	err = mockNode.replay(snapshot4)
	require.NoError(t, err)

}

func TestInitAndStartNode(t *testing.T) {
	InitAndStartNode(t)
}

func InitAndStartNode(t *testing.T) *RaftNode {
	tmpDir := t.TempDir()
	rds, err := raftlog.Init(tmpDir)
	if err != nil {
		t.Errorf("Test replay failed, raft disk storage init failed, err is %v", err)
	}
	mockTransPeers := make(map[uint32]uint64)
	mockPeers := []raft.Peer{
		{ID: 2}, {ID: 3},
	}
	client := &MockMetaClient{}
	node := StartNode(rds, 1, "db", 1, mockPeers, client, mockTransPeers)
	node.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	hardState := &raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 5,
	}
	err = rds.Save(hardState, ents, nil)
	defer rds.Close()
	if err != nil {
		t.Errorf("raft disk storage save failed")
	}

	snapshot := &raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     3,
			Term:      1,
			ConfState: raftpb.ConfState{},
		},
	}
	err = rds.Save(nil, nil, snapshot)

	err = node.InitAndStartNode()

	require.Nil(t, err)

	return node
}

func TestIsLeader(t *testing.T) {
	node := InitAndStartNode(t)
	leader := node.isLeader()
	require.False(t, leader)
}

func TestDeleteEntryLog(t *testing.T) {
	node := InitAndStartNode(t)
	_, err := node.prepareDeleteEntryLogProposeData(5)
	require.NoError(t, err)
	err = node.deleteEntryLog()
	require.NoError(t, err)
}

func TestSaveConfStateToMeta(t *testing.T) {
	node := InitAndStartNode(t)
	node.saveConfStateToMeta()
}

func TestComparePeerFileIdWithLeaderFileId(t *testing.T) {
	node := InitAndStartNode(t)
	err := node.comparePeerFileIdWithLeaderFileId(2, 1)
	require.Error(t, err, "member's index is advanced")
	err = node.comparePeerFileIdWithLeaderFileId(-1, -1)
	require.NoError(t, err)
}

func TestCheckAllRgMembers(t *testing.T) {
	node := &RaftNode{
		MetaClient: &MockMetaClient{},
		peers: map[uint32]uint64{
			0: 1,
			1: 2,
		},
	}
	node.CheckAllRgMembers()
}

func TestCommittedDataCErr(t *testing.T) {
	node := &RaftNode{
		DataCommittedC: make(map[uint64]chan error),
	}
	node.RetCommittedDataC(&raftlog.DataWrapper{}, nil)
	node.RemoveCommittedDataC(&raftlog.DataWrapper{})
}

type MockMetaClient struct {
	metaclient.MetaClient
}

func (client *MockMetaClient) ThermalShards(db string, start, end time.Duration) map[uint64]struct{} {
	panic("implement me")
}

func (client *MockMetaClient) UpdateStreamMstSchema(database string, retentionPolicy string, mst string, stmt *influxql.SelectStatement) error {
	return nil
}

func (client *MockMetaClient) CreateStreamPolicy(info *meta2.StreamInfo) error {
	return nil
}

func (client *MockMetaClient) ShowStreams(database string, showAll bool) (models.Rows, error) {
	return nil, nil
}

func (client *MockMetaClient) DropStream(name string) error {
	return nil
}

func (client *MockMetaClient) OpenAtStore() error {
	return nil
}

func (client *MockMetaClient) UpdateShardDownSampleInfo(Ident *meta2.ShardIdentifier) error {
	return nil
}

func (client *MockMetaClient) GetNodePtsMap(database string) (map[uint64][]uint32, error) {
	panic("implement me")
}

func (client *MockMetaClient) CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation,
	engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) AlterShardKey(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo) error {
	return nil
}
func (client *MockMetaClient) CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta2.RetentionPolicySpec, shardKey *meta2.ShardKeyInfo, enableTagArray bool, replicaN uint32) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateRetentionPolicy(database string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return nil
}
func (client *MockMetaClient) CreateUser(name, password string, admin, rwuser bool) (meta2.User, error) {
	return nil, nil
}
func (client *MockMetaClient) Databases() map[string]*meta2.DatabaseInfo {
	return nil
}
func (client *MockMetaClient) Database(name string) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) DataNode(id uint64) (*meta2.DataNode, error) {
	NodeInfo := meta2.NodeInfo{
		Status: serf.StatusAlive,
	}
	node := &meta2.DataNode{
		NodeInfo: NodeInfo,
	}
	return node, nil
}
func (client *MockMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return nil, nil
}
func (client *MockMetaClient) DeleteDataNode(id uint64) error {
	return nil
}
func (client *MockMetaClient) DeleteMetaNode(id uint64) error {
	return nil
}
func (client *MockMetaClient) DropShard(id uint64) error {
	return nil
}
func (client *MockMetaClient) DropDatabase(name string) error {
	return nil
}
func (client *MockMetaClient) DropRetentionPolicy(database, name string) error {
	return nil
}
func (client *MockMetaClient) DropSubscription(database, rp, name string) error {
	return nil
}
func (client *MockMetaClient) DropUser(name string) error {
	return nil
}
func (client *MockMetaClient) MetaNodes() ([]meta2.NodeInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) RetentionPolicy(database, name string) (rpi *meta2.RetentionPolicyInfo, err error) {
	return nil, nil
}
func (client *MockMetaClient) SetAdminPrivilege(username string, admin bool) error {
	return nil
}
func (client *MockMetaClient) SetPrivilege(username, database string, p originql.Privilege) error {
	return nil
}
func (client *MockMetaClient) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta2.ShardInfo, err error) {
	return nil, nil
}
func (client *MockMetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta2.ShardGroupInfo, err error) {
	return nil, nil
}

func (client *MockMetaClient) UpdateRetentionPolicy(database, name string, rpu *meta2.RetentionPolicyUpdate, makeDefault bool) error {
	return nil
}
func (client *MockMetaClient) UpdateUser(name, password string) error {
	return nil
}
func (client *MockMetaClient) UserPrivilege(username, database string) (*originql.Privilege, error) {
	return nil, nil
}
func (client *MockMetaClient) UserPrivileges(username string) (map[string]originql.Privilege, error) {
	return nil, nil
}
func (client *MockMetaClient) Users() []meta2.UserInfo {
	return nil
}
func (client *MockMetaClient) MarkDatabaseDelete(name string) error {
	return nil
}
func (client *MockMetaClient) MarkRetentionPolicyDelete(database, name string) error {
	return nil
}
func (client *MockMetaClient) MarkMeasurementDelete(database, policy, mst string) error {
	return nil
}
func (client *MockMetaClient) DBPtView(database string) (meta2.DBPtInfos, error) {
	return nil, nil
}
func (client *MockMetaClient) DBRepGroups(database string) []meta2.ReplicaGroup {
	return nil
}
func (client *MockMetaClient) GetReplicaN(database string) (int, error) {
	return 1, nil
}
func (client *MockMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta2.ShardGroupInfo) {
	var shards []meta2.ShardInfo
	shards = append(shards, meta2.ShardInfo{})
	return "", "", &meta2.ShardGroupInfo{Shards: shards}
}
func (client *MockMetaClient) Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) Schema(database string, retentionPolicy string, mst string) (fields map[string]int32, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

func (client *MockMetaClient) GetMeasurements(m *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) TagKeys(database string) map[string]set.Set {
	return nil
}
func (client *MockMetaClient) FieldKeys(database string, ms influxql.Measurements) (map[string]map[string]int32, error) {
	return nil, nil
}
func (client *MockMetaClient) QueryTagKeys(database string, ms influxql.Measurements, cond influxql.Expr) (map[string]map[string]struct{}, error) {
	return nil, nil
}
func (client *MockMetaClient) MatchMeasurements(database string, ms influxql.Measurements) (map[string]*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) Measurements(database string, ms influxql.Measurements) ([]string, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowShards(db string, rp string, mst string) models.Rows {
	return nil
}
func (client *MockMetaClient) ShowShardGroups() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowSubscriptions() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowRetentionPolicies(database string) (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowContinuousQueries() (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowCluster(nodeType string, ID uint64) (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowClusterWithCondition(nodeType string, ID uint64) (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) GetAliveShards(database string, sgi *meta2.ShardGroupInfo) []int {
	return nil
}

func (client *MockMetaClient) AdminUserExists() bool {
	return true
}

func (client *MockMetaClient) Authenticate(username, password string) (u meta2.User, e error) {
	return nil, meta2.ErrUserLocked
}

func (client *MockMetaClient) DropDownSamplePolicy(database, name string, dropAll bool) error {
	return nil
}

func (client *MockMetaClient) NewDownSamplePolicy(database, name string, info *meta2.DownSamplePolicyInfo) error {
	return nil
}

func (client *MockMetaClient) ShowDownSamplePolicies(database string) (models.Rows, error) {
	return nil, nil
}

func (client *MockMetaClient) UpdateUserInfo() {}

func (client *MockMetaClient) GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta2.RpMeasurementsFieldsInfo, error) {
	return nil, nil
}

func (mmc *MockMetaClient) GetStreamInfos() map[string]*meta2.StreamInfo {
	return nil
}

func (mmc *MockMetaClient) GetDstStreamInfos(db, rp string, dstSis *[]*meta2.StreamInfo) bool {
	return false
}

func (mmc *MockMetaClient) GetAllMst(dbName string) []string {
	var msts []string
	msts = append(msts, "cpu")
	msts = append(msts, "mem")
	return msts
}
