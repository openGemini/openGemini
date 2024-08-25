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
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

type RepDisPolicy uint8

var repDisPolicy RepDisPolicy

func SetRepDisPolicy(flag uint8) {
	repDisPolicy = RepDisPolicy(flag)
}

const (
	NodeHard RepDisPolicy = iota
	AzHard
	End
)

type ChooseRGFn func(data *Data, db string, newNode *DataNode, replicasN int)
type CreateDBRGFn func(data *Data, db string, replicaN int) error

var ChooseRGFns []ChooseRGFn

var CreateDBRGFns []CreateDBRGFn

func init() {
	ChooseRGFns = make([]ChooseRGFn, End)
	ChooseRGFns[NodeHard] = NodeHardChooseRG
	ChooseRGFns[AzHard] = AzHardChooseRG
	CreateDBRGFns = make([]CreateDBRGFn, End)
	CreateDBRGFns[NodeHard] = NodeHardCreateDBRG
	CreateDBRGFns[AzHard] = AzHardCreateDBRG
}

func getFirstUnFullRGLoc(rgs []ReplicaGroup, replicasN int) int {
	i := 0
	for ; i < len(rgs); i++ {
		if rgs[i].Status == UnFull {
			return i
		}
	}
	return i
}

func getFirstUnFullRGLocWithDiffAz(data *Data, db string, currentAz string) int {
	rgs := data.ReplicaGroups[db]
	i := 0
	for ; i < len(rgs); i++ {
		if rgs[i].Status == UnFull {
			rgGroup := rgs[i]
			if inAzSet(data, db, rgGroup, currentAz) {
				continue
			}
			return i
		}
	}
	return i
}

func getAzSet(data *Data, db string, rgGroup ReplicaGroup) map[string]struct{} {
	azSet := make(map[string]struct{})
	ptId := rgGroup.MasterPtID
	node := getDataNodeByPtId(data, db, ptId)
	azSet[node.Az] = struct{}{}
	peers := rgGroup.Peers
	for _, peer := range peers {
		peerNode := getDataNodeByPtId(data, db, peer.ID)
		azSet[peerNode.Az] = struct{}{}
	}
	return azSet
}

func inAzSet(data *Data, db string, rgGroup ReplicaGroup, currentAz string) bool {
	azSet := getAzSet(data, db, rgGroup)
	_, ok := azSet[currentAz]
	return ok
}

func getDataNodeByPtId(data *Data, db string, ptId uint32) *DataNode {
	ptInfo := data.GetPtInfo(db, ptId)
	nodeId := ptInfo.Owner.NodeID
	node := data.DataNode(nodeId)
	return node
}

// choose rgId for each pt owned to newNode when create a new datanode, or called by createDBPtView
func NodeHardChooseRG(data *Data, db string, newNode *DataNode, replicasN int) {
	rgStart := getFirstUnFullRGLoc(data.ReplicaGroups[db], replicasN)
	joinRpGroup(data, db, newNode, replicasN, rgStart, "", joinForNodeHard)
}

// choose rgId for each newPt of a newDB when createDBPtView of createDataBase
func NodeHardCreateDBRG(data *Data, db string, replicaN int) error {
	DataLogger.Info("NodeHardCreateDBRG start", zap.String("db", db), zap.Int("replicaN", replicaN))
	data.expandDBRG(db)
	for i := range data.DataNodes {
		NodeHardChooseRG(data, db, &data.DataNodes[i], replicaN)
	}
	DataLogger.Info("NodeHardCreateDBRG end")
	return nil
}

// choose rgId for each newPt of a newDB when createDBPtView of createDataBase
func AzHardCreateDBRG(data *Data, db string, replicaN int) error {
	DataLogger.Info("AzHardCreateDBRG start", zap.String("db", db), zap.Int("replicaN", replicaN))
	data.expandDBRG(db)
	for i := range data.DataNodes {
		AzHardChooseRG(data, db, &data.DataNodes[i], replicaN)
	}
	DataLogger.Info("AzHardCreateDBRG end")
	return nil
}

// choose rgId for each pt owned to newNode when create a new datanode
func AzHardChooseRG(data *Data, db string, newNode *DataNode, replicasN int) {
	currentNodeAz := newNode.Az
	rgStart := getFirstUnFullRGLocWithDiffAz(data, db, currentNodeAz)
	joinRpGroup(data, db, newNode, replicasN, rgStart, currentNodeAz, joinForAzHard)
}

func joinRpGroup(data *Data, db string, newNode *DataNode, replicasN int, rgStart int, currentNodeAz string, joinFunc JoinForRgFn) {
	var prePtRGId uint32
	initFirstPtRg := false
	for ptLoc, pt := range data.PtView[db] {
		if pt.Owner.NodeID == newNode.ID {
			choosed := false
			for i := rgStart; i < len(data.ReplicaGroups[db]); i++ {
				rgGroup := data.ReplicaGroups[db][i]
				if joinFunc(data, db, rgGroup, currentNodeAz) {
					if !initFirstPtRg || prePtRGId != data.ReplicaGroups[db][i].ID {
						data.PtView[db][ptLoc].RGID = data.ReplicaGroups[db][i].ID
						data.ReplicaGroups[db][i].addPeer(pt.PtId, replicasN)
						data.ReplicaGroups[db][i].nextUnFull(replicasN, db)
						choosed = true
						prePtRGId = data.ReplicaGroups[db][i].ID
						initFirstPtRg = true
						DataLogger.Info("NodeHardChooseRG addPeer", zap.String("db", db), zap.Uint32("ptId", pt.PtId), zap.Uint32("RGId", data.ReplicaGroups[db][i].ID))
						break
					}
				}
			}
			if !choosed {
				// to create a new rg as owner Rg of this pt, first add pt is masterPt
				newRg := NewReplicaGroup(uint32(len(data.ReplicaGroups[db])), pt.PtId, nil, UnFull, 0)
				prePtRGId = uint32(len(data.ReplicaGroups[db]))
				initFirstPtRg = true
				data.PtView[db][ptLoc].RGID = prePtRGId
				newRg.nextUnFull(replicasN, db)
				data.ReplicaGroups[db] = append(data.ReplicaGroups[db], *newRg)
				DataLogger.Info("NodeHardChooseRG newRG", zap.String("db", db), zap.Uint32("newRGId", newRg.ID), zap.Uint32("masterPtId", pt.PtId))
			}
		}
	}
}

type JoinForRgFn func(data *Data, db string, rg ReplicaGroup, currentAz string) bool

func joinForAzHard(data *Data, db string, rg ReplicaGroup, currentAz string) bool {
	return rg.Status == UnFull && !inAzSet(data, db, rg, currentAz)
}

func joinForNodeHard(data *Data, db string, rg ReplicaGroup, currentAz string) bool {
	return rg.Status == UnFull
}

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
	UnFull
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

func NewReplicaGroup(id, masterPtId uint32, peers []Peer, status RGStatus, term uint64) *ReplicaGroup {
	return &ReplicaGroup{
		ID:         id,
		MasterPtID: masterPtId,
		Peers:      peers,
		Status:     status,
		Term:       term,
	}
}

func (rg *ReplicaGroup) hasPt(ptId uint32) bool {
	if rg.MasterPtID == ptId {
		return true
	}
	for i := range rg.Peers {
		if rg.Peers[i].ID == ptId {
			return true
		}
	}
	return false
}

func (rg *ReplicaGroup) addPeer(ptId uint32, replicasN int) {
	rg.Peers = append(rg.Peers, Peer{ID: ptId, PtRole: Slave})
}

// UnFull -> SubHealth
func (rg *ReplicaGroup) nextUnFull(replicasN int, db string) {
	if rg.Status != UnFull {
		return
	}
	if replicasN == len(rg.Peers)+1 {
		rg.Status = SubHealth
		DataLogger.Info("rg UnFull -> SubHealth", zap.String("db", db), zap.Int("replicasN", replicasN), zap.Uint32("masterPtId", rg.MasterPtID))
	}
}

// SubHealth -> Health
func (rg *ReplicaGroup) nextSubHealth(db string, ptView DBPtInfos) {
	if rg.Status != SubHealth {
		return
	}
	replicasN := len(rg.Peers) + 1
	onlinePtN := 0
	ptId := rg.MasterPtID
	for i := range ptView {
		if ptView[i].PtId == ptId && ptView[i].Status == Online {
			onlinePtN++
		}
	}
	for i := range rg.Peers {
		ptId = rg.Peers[i].ID
		for j := range ptView {
			if ptId == ptView[j].PtId && ptView[j].Status == Online {
				onlinePtN++
			}
		}
	}
	halfReplicasN := replicasN / 2
	if onlinePtN > halfReplicasN {
		rg.Status = Health
		DataLogger.Info("rg SubHealth -> Health", zap.String("db", db), zap.Int("replicasN", replicasN), zap.Int("halfReplicasN", halfReplicasN), zap.Int("onlinePtN", onlinePtN))
	}
}

// Health -> SubHealth
func (rg *ReplicaGroup) nextHealth(db string, ptView DBPtInfos, replicasN int) {
	if rg.Status != Health {
		return
	}
	onlinePtN := 0
	ptId := rg.MasterPtID
	for i := range ptView {
		if ptView[i].PtId == ptId && ptView[i].Status == Online {
			onlinePtN++
		}
	}
	for i := range rg.Peers {
		ptId = rg.Peers[i].ID
		for j := range ptView {
			if ptId == ptView[j].PtId && ptView[j].Status == Online {
				onlinePtN++
			}
		}
	}
	halfReplicasN := replicasN / 2
	if onlinePtN <= halfReplicasN {
		rg.Status = SubHealth
		DataLogger.Info("rg Health -> SubHealth", zap.String("db", db), zap.Int("replicasN", replicasN), zap.Int("halfReplicasN", halfReplicasN), zap.Int("onlinePtN", onlinePtN))
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
