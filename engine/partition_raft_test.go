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
package engine

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type MockStorageService struct {
	netstorage.StorageService
}

func (s *MockStorageService) Write(db, rp, mst string, ptId uint32, shardID uint64, writeData func() error) error {
	return nil
}
func (s *MockStorageService) WriteDataFunc(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte, index *raftlog.SnapShotter) error {
	return nil
}
func (s *MockStorageService) GetNodeId() uint64 {
	return 1
}

func TestDealNormalData(t *testing.T) {
	bytes := mockMarshaledPoint()

	dataWrapper := &raftlog.DataWrapper{
		Data:     bytes,
		DataType: raftlog.Normal,
	}
	client := &MockMetaClient{}
	storage := &MockStorageService{}

	tmpDir := t.TempDir()
	entsInit := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	rds, err := raftlog.Init(tmpDir, 0)
	require.NoError(t, err)
	defer rds.Close()
	err = rds.Save(nil, entsInit, nil)
	require.NoError(t, err)

	mockPeers := []raft.Peer{
		{ID: 2}, {ID: 3},
	}
	mockTransPeers := make(map[uint32]uint64)

	node := raftconn.StartNode(rds, 1, "db", 1, mockPeers, client, mockTransPeers)

	err = dealNormalData(dataWrapper, "db", 1, client, storage, node)
	require.Equal(t, err.Error(), "db is not same")

	err = dealNormalData(dataWrapper, "", 1, client, storage, node)
	require.Equal(t, err.Error(), "sgi shards less than ptid")

	err = dealNormalData(dataWrapper, "", 0, client, storage, node)
	require.NoError(t, err)
}

func TestDealCommitData(t *testing.T) {
	bytes := mockMarshaledPoint()

	dataWrapper := &raftlog.DataWrapper{
		Data:     bytes,
		DataType: raftlog.Normal,
		Identity: "db_0",
	}
	client := &MockMetaClient{}
	storage := &MockStorageService{}

	tmpDir := t.TempDir()
	entsInit := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	rds, err := raftlog.Init(tmpDir, 0)
	require.NoError(t, err)
	defer rds.Close()
	err = rds.Save(nil, entsInit, nil)
	require.NoError(t, err)

	mockPeers := []raft.Peer{
		{ID: 2}, {ID: 3},
	}
	mockTransPeers := make(map[uint32]uint64)

	node := raftconn.StartNode(rds, 1, "db", 1, mockPeers, client, mockTransPeers)

	dealCommitData(node, client, storage, dataWrapper.Marshal(), "db", 1)

	var dst []byte
	dst = encoding.MarshalUint64(dst, 1)
	dataWrapper2 := &raftlog.DataWrapper{
		Data:     dst,
		DataType: raftlog.Snapshot,
	}
	dealCommitData(node, client, storage, dataWrapper2.Marshal(), "db", 1)

	var dst2 []byte
	dst2 = encoding.MarshalUint64(dst2, 1)
	dataWrapper3 := &raftlog.DataWrapper{
		Data:     dst2,
		DataType: raftlog.ClearEntryLog,
	}
	dealCommitData(node, client, storage, dataWrapper3.Marshal(), "db", 1)
}

func TestReadReplayForReplication(t *testing.T) {
	client := &MockMetaClient{}
	storage := &MockStorageService{}
	replayC := make(chan *raftconn.Commit, 1)
	bytes := mockMarshaledPoint()
	dataWrapper := &raftlog.DataWrapper{
		Data:     bytes,
		DataType: raftlog.Normal,
	}
	var data [][]byte
	data = append(data, dataWrapper.Marshal())
	commit := &raftconn.Commit{
		Database:   "db0",
		PtId:       1,
		MasterShId: 1,
		Data:       data,
	}
	replayC <- commit
	close(replayC)
	readReplayForReplication(replayC, client, storage)
	replayC = make(chan *raftconn.Commit, 1)
	close(replayC)
	readReplayForReplication(replayC, client, storage)

}

func mockMarshaledPoint() []byte {
	pBuf := make([]byte, 0)
	// ptid
	pt := uint32(0)
	pBuf = encoding.MarshalUint32(pBuf, pt)
	shard := uint64(0)
	pBuf = encoding.MarshalUint64(pBuf, shard)
	rows := mockRows()
	pBuf, err := influx.FastMarshalMultiRows(pBuf, rows)
	if err != nil {
		panic(err)
	}
	return pBuf
}

func mockRows() []influx.Row {
	keys := []string{
		"mst0,tk1=value1,tk2=value2,tk3=value3 f1=value1,f2=value2",
		"mst0,tk1=value11,tk2=value22,tk3=value33 f1=value1,f2=value2",
	}
	pts := make([]influx.Row, 0, len(keys))
	for _, key := range keys {
		pt := influx.Row{}
		splited := strings.Split(key, " ")
		strs := strings.Split(splited[0], ",")
		pt.Name = strs[0]
		pt.Tags = make(influx.PointTags, len(strs)-1)
		for i, str := range strs[1:] {
			kv := strings.Split(str, "=")
			pt.Tags[i].Key = kv[0]
			pt.Tags[i].Value = kv[1]
		}
		sort.Sort(&pt.Tags)
		fields := strings.Split(splited[1], ",")
		pt.Fields = make(influx.Fields, len(fields))
		for i, str := range fields {
			kv := strings.Split(str, "=")
			pt.Fields[i].Key = kv[0]
			pt.Fields[i].Type = influx.Field_Type_String
			pt.Fields[i].StrValue = kv[1]
		}
		sort.Sort(&pt.Fields)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}
	return pts
}
