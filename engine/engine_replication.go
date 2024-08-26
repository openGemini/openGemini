// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"path"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

//lint:ignore U1000 use for replication feature
func (e *Engine) startRaftNode(opId uint64, nodeId uint64, dbPt *DBPTInfo, client metaclient.MetaClient, storage netstorage.StorageService) error {
	database := dbPt.database
	ptId := dbPt.id

	err2 := e.checkRepGroupStatus(opId, database, ptId)
	if err2 != nil {
		return err2
	}

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
	walPath := path.Join(e.walPath, config.WalDirectory, database, strconv.Itoa(int(ptId)))
	store, err := raftlog.Init(walPath)
	if err != nil {
		return err
	}
	store.SetUint(raftlog.RaftId, raftconn.GetRaftNodeId(ptId))
	store.SetUint(raftlog.GroupId, 0) // All zeros have group zero.

	replayC := make(chan *raftconn.Commit, 1)
	node := raftconn.StartNode(store, nodeId, database, raftconn.GetRaftNodeId(ptId), peers, client, transPeers)
	node.WithLogger(e.log.With(zap.String("service", "raft node")))
	node.ReplayC = replayC
	if err = node.InitAndStartNode(); err != nil {
		return err
	}
	close(replayC)
	dbPt.node = node
	dbPt.proposeC = dbPt.node.GetProposeC()
	dbPt.ReplayC = replayC
	go readCommitFromRaft(node, client, storage)

	var leaderPtID = -1
	raftGroups := e.metaClient.DBRepGroups(database)
	for _, rg := range raftGroups {
		if rg.ID == uint32(rgId) {
			leaderPtID = int(rg.MasterPtID)
			break
		}
	}
	go node.TransferLeadership(raftconn.GetRaftNodeId(uint32(leaderPtID)))
	return nil
}

func (e *Engine) checkRepGroupStatus(opId uint64, database string, ptId uint32) error {
	var retryNum = 5
	for {
		retryNum--
		var needTry bool
		raftGroupId, err := e.getRaftGroupId(database, ptId)
		if err != nil {
			return err
		}
		groups := e.metaClient.DBRepGroups(database)
		for i := range groups {
			if groups[i].ID == raftGroupId {
				if groups[i].Status == meta.UnFull {
					needTry = true
				}
			}
		}
		if !needTry {
			break
		}
		if retryNum < 0 {
			return errors.Newf("Got database: %s, ptId: %d raftGroup is still UnFull, opId:%d", database, ptId, opId)
		}
		time.Sleep(200 * time.Millisecond)
	}
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

func (e *Engine) getRaftGroupId(dataBase string, ptId uint32) (uint32, error) {
	ptViews, err := e.metaClient.DBPtView(dataBase)
	if err != nil {
		return 0, err
	}
	var rgId uint32
	for _, view := range ptViews {
		if view.PtId == ptId {
			rgId = view.RGID
			break
		}
	}
	return rgId, nil
}
