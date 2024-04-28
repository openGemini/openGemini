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

package meta

import (
	"github.com/openGemini/openGemini/lib/errno"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

type Role uint8

const (
	Master Role = iota
	Slave
	Catcher // salve restart and need catch up with master, pt in this role can not read or write
)

type RGStatus uint8

const (
	Health    RGStatus = iota // write request return success only if the write to all peers in replica group is successful
	SubHealth                 // write request return success if the write to master is successful
)

type Peer struct {
	ID     uint32 // pt id
	PtRole Role
}

type ReplicaGroup struct {
	ID         uint32
	MasterPtID uint32
	Peers      []Peer // the other member in this replica group
	Status     RGStatus
	Term       uint64 // term of master, if master changed term changed
}

func (rg *ReplicaGroup) init(id, masterPtId uint32, peers []Peer, status RGStatus, term uint64) {
	rg.ID = id
	rg.MasterPtID = masterPtId
	rg.Peers = peers
	rg.Status = status
	rg.Term = term
}

func (rg *ReplicaGroup) clone() {
	other := *rg
	if len(rg.Peers) > 0 {
		other.Peers = make([]Peer, len(rg.Peers))
		for i := range rg.Peers {
			other.Peers[i] = rg.Peers[i]
		}
	}
}

func (rg *ReplicaGroup) GetPtRole(ptID uint32) Role {
	for i := range rg.Peers {
		if rg.Peers[i].ID == ptID {
			return rg.Peers[i].PtRole
		}
	}
	return Catcher
}

func (rg *ReplicaGroup) IsMasterPt(ptID uint32) bool {
	return ptID == rg.MasterPtID
}

func (rg *ReplicaGroup) marshal() *proto2.ReplicaGroup {
	pb := &proto2.ReplicaGroup{
		ID:       proto.Uint32(rg.ID),
		MasterID: proto.Uint32(rg.MasterPtID),
		Status:   proto.Uint32(uint32(rg.Status)),
		Term:     proto.Uint64(rg.Term),
	}
	if len(rg.Peers) > 0 {
		pb.Peers = make([]*proto2.Peer, len(rg.Peers))
		for i := range rg.Peers {
			pb.Peers[i] = &proto2.Peer{
				ID:   proto.Uint32(rg.Peers[i].ID),
				Role: proto.Uint32(uint32(rg.Peers[i].PtRole)),
			}
		}
	}
	return pb
}

func (rg *ReplicaGroup) unmarshal(pb *proto2.ReplicaGroup) {
	rg.ID = pb.GetID()
	rg.MasterPtID = pb.GetMasterID()
	rg.Term = pb.GetTerm()
	rg.Status = RGStatus(pb.GetStatus())
	if len(pb.GetPeers()) > 0 {
		rg.Peers = make([]Peer, len(pb.GetPeers()))
		for i := range pb.Peers {
			rg.Peers[i] = Peer{ID: pb.Peers[i].GetID(), PtRole: Role(pb.Peers[i].GetRole())}
		}
	}
}

func (data *Data) CreateReplication(db string, replicaN uint32) error {
	if replicaN <= 1 {
		return nil
	}
	return data.createReplicationImp(db, replicaN)
}

func (data *Data) createReplicationImp(db string, replicaN uint32) error {
	ptView := data.DBPtView(db)
	ptNum := uint32(len(ptView))
	nodeNum := ptNum / data.PtNumPerNode
	if nodeNum%replicaN != 0 {
		return errno.NewError(errno.ReplicaNodeNumIncorrect, nodeNum, replicaN)
	}

	data.createReplicationGroup(db, replicaN, 0, ptNum/replicaN, 0)
	return nil
}

func (data *Data) createReplicationGroup(db string, replicaN, repStart, repEnd, ptStart uint32) {
	data.expandRepGroups(db, repEnd-repStart)
	repGroups := data.DBRepGroups(db)
	ptView := data.DBPtView(db)
	for i := repStart; i < repEnd; i++ {
		peers := make([]Peer, 0, replicaN-1)
		peers = append(peers, Peer{ID: i*replicaN + 1, PtRole: Slave}, Peer{ID: i*replicaN + 2, PtRole: Slave})
		repGroups[i].init(i, i*replicaN, peers, Health, 0)

		// update ptview RGID
		ptView[i*replicaN].RGID = i
		ptView[i*replicaN+1].RGID = i
		ptView[i*replicaN+2].RGID = i
	}

	DataLogger.Info("create replication", zap.String("db", db),
		zap.Any("new replication group", repGroups[repStart:]), zap.Any("new pt info", ptView[ptStart:]),
		zap.Uint32("replication start", repStart), zap.Uint32("replication end", repEnd),
		zap.Uint32("pt start", ptStart))
}

func (data *Data) createReplicationInner(db string, replicaN, repStart, repEnd, ptStart uint32) {
	var masterPtId, slavePtId uint32

	data.expandRepGroups(db, repEnd-repStart)
	ptNumPerNode := data.PtNumPerNode
	repGroups := data.DBRepGroups(db)
	ptView := data.DBPtView(db)
	for repGroupId, ptIdx := repStart, ptStart; repGroupId < repEnd; {
		for k := uint32(0); k < ptNumPerNode; k++ {
			peers := make([]Peer, replicaN-1, replicaN-1)
			for i := uint32(0); i < replicaN-1; i++ {
				slavePtId = ptIdx + ptNumPerNode*(i+1)
				peers[i].ID = slavePtId
				peers[i].PtRole = Slave

				// update slave pt RG id
				ptView[slavePtId].RGID = repGroupId
			}

			masterPtId = ptIdx
			repGroups[repGroupId].init(repGroupId, masterPtId, peers, Health, 0)

			// update master pt RG id
			ptView[masterPtId].RGID = repGroupId
			repGroupId++
		}
		ptIdx += ptNumPerNode * replicaN
	}
	DataLogger.Info("create replication", zap.String("db", db),
		zap.Any("new replication group", repGroups[repStart:]), zap.Any("new pt info", ptView[ptStart:]),
		zap.Uint32("replication start", repStart), zap.Uint32("replication end", repEnd),
		zap.Uint32("pt start", ptStart))
}
