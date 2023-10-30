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

package metaclient

import (
	"sync"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

func (c *Client) GetReplicaInfo(db string, pt uint32) *message.ReplicaInfo {
	return c.replicaInfoManager.Get(db, pt)
}

func (c *Client) GetReplicaInfoManager() *ReplicaInfoManager {
	return c.replicaInfoManager
}

type ReplicaInfoManager struct {
	mu   sync.RWMutex
	data map[string]map[uint32]*message.ReplicaInfo
}

func NewReplicaInfoManager() *ReplicaInfoManager {
	return &ReplicaInfoManager{data: make(map[string]map[uint32]*message.ReplicaInfo)}
}

func (m *ReplicaInfoManager) Get(db string, pt uint32) *message.ReplicaInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.data[db][pt]
}

func (m *ReplicaInfoManager) update(data map[string]map[uint32]*message.ReplicaInfo) {
	m.mu.Lock()
	m.data = data
	m.mu.Unlock()
}

func (m *ReplicaInfoManager) Update(data *meta.Data, nodeID uint64) {
	var replicaInfo = make(map[string]map[uint32]*message.ReplicaInfo)

	for db, ptView := range data.PtView {
		for i := range ptView {
			pt := &ptView[i]
			if pt.Owner.NodeID != nodeID {
				continue
			}

			info := m.buildReplicaInfo(data, db, pt)
			if info == nil {
				// no replica info
				continue
			}

			if _, ok := replicaInfo[db]; !ok {
				replicaInfo[db] = make(map[uint32]*message.ReplicaInfo)
			}
			replicaInfo[db][pt.PtId] = info
		}
	}

	m.update(replicaInfo)
}

func (m *ReplicaInfoManager) buildReplicaInfo(data *meta.Data, dbName string, pt *meta.PtInfo) *message.ReplicaInfo {
	rg := data.GetReplicaGroup(dbName, pt.RGID)
	if rg == nil {
		return nil
	}

	if rg.IsMasterPt(pt.PtId) {
		return m.buildMasterReplicaInfo(dbName, data, pt, rg)
	}

	return m.buildSlaveReplicaInfo(dbName, data, pt, rg)
}

func (m *ReplicaInfoManager) buildSlaveReplicaInfo(dbName string, data *meta.Data, pt *meta.PtInfo, rg *meta.ReplicaGroup) *message.ReplicaInfo {
	masterPt := data.GetPtInfo(dbName, rg.MasterPtID)
	if masterPt == nil {
		return nil
	}

	return &message.ReplicaInfo{
		Master:        message.NewPeerInfo(masterPt),
		ReplicaStatus: rg.Status,
		ReplicaRole:   rg.GetPtRole(pt.PtId),
		Term:          0,
	}
}

func (m *ReplicaInfoManager) buildMasterReplicaInfo(dbName string, data *meta.Data, pt *meta.PtInfo, rg *meta.ReplicaGroup) *message.ReplicaInfo {
	info := &message.ReplicaInfo{
		ReplicaRole:   meta.Master,
		Master:        message.NewPeerInfo(pt),
		Peers:         make([]*message.PeerInfo, 0, len(rg.Peers)),
		ReplicaStatus: rg.Status,
		Term:          0,
	}

	for i := range rg.Peers {
		if rg.Peers[i].PtRole != meta.Slave {
			continue
		}

		slavePt := data.GetPtInfo(dbName, rg.Peers[i].ID)
		if slavePt == nil {
			continue
		}

		peer := message.NewPeerInfo(slavePt)
		m.buildShardMapping(peer, dbName, data, pt.PtId, slavePt.PtId)
		info.AddPeer(peer)
	}

	return info
}

func (m *ReplicaInfoManager) buildShardMapping(peer *message.PeerInfo, dbName string, data *meta.Data, masterPt, slavePt uint32) {
	dbi := data.Database(dbName)
	if dbi == nil {
		return
	}

	dbi.WalkRetentionPolicy(func(rp *meta.RetentionPolicyInfo) {
		rp.WalkShardGroups(func(sg *meta.ShardGroupInfo) {
			if len(sg.Shards) <= int(masterPt) || len(sg.Shards) < int(slavePt) {
				return
			}

			peer.AddShardMapper(sg.Shards[masterPt].ID, sg.Shards[slavePt].ID)
		})
	})
}
