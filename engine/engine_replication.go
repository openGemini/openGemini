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
	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/raftconn"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type raftNodeRequest interface {
	StepRaftMessage(msg []raftpb.Message)
	Stop()
}

//lint:ignore U1000 use for replication feature
func (e *Engine) startRaftNode(opId uint64, nodeId uint64, dbPt *DBPTInfo, client metaclient.MetaClient) error {
	database := dbPt.database
	ptId := dbPt.id

	ptViews, err := e.metaClient.DBPtView(database)
	if err != nil {
		return err
	}
	var rgId = -1
	ptGroup := make(map[uint32][]uint32)  // key is RGID, value is the list of ptId at the RGID
	transPeers := make(map[uint32]uint64) // get transPeers map, key is ptId, value is nodeId
	for _, view := range ptViews {
		if view.PtId == ptId {
			rgId = int(view.RGID)
		}
		ptGroup[view.RGID] = append(ptGroup[view.RGID], view.PtId)
		transPeers[view.PtId] = view.Owner.NodeID
	}
	if rgId == -1 || len(ptGroup[uint32(rgId)]) <= 0 {
		return errors.Newf("Got database: %s, ptId: %d peers error, opId:%d", database, ptId, opId)
	}
	var peers []raft.Peer
	ptPeers := ptGroup[uint32(rgId)]
	for _, ptid := range ptPeers {
		peers = append(peers, raft.Peer{ID: raftconn.GetRaftNodeId(ptid)})
	}
	n := raftconn.StartNode(e.dataPath, nodeId, database, raftconn.GetRaftNodeId(ptId), peers, client, transPeers)
	n.WithLogger(e.log.With(zap.String("service", "raft node")))

	var leaderPtID = -1
	raftGroups := e.metaClient.DBRepGroups(database)
	for _, rg := range raftGroups {
		if rg.ID == uint32(rgId) {
			leaderPtID = int(rg.MasterPtID)
			break
		}
	}

	dbPt.node = n
	go n.TransferLeadership(raftconn.GetRaftNodeId(uint32(leaderPtID)))
	return nil
}

func (e *Engine) SendRaftMessage(database string, ptId uint64, msgs raftpb.Message) error {
	dbPt, err := e.getPartition(database, uint32(ptId), false)
	if err != nil {
		if errno.Equal(err, errno.DBPTClosed) {
			return err
		}
		return errors.Wrapf(err, "get partition %s:%d error", database, ptId)
	}
	if dbPt.node == nil {
		return nil
	}
	dbPt.node.StepRaftMessage([]raftpb.Message{msgs})
	return nil
}
