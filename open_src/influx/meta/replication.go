package meta

import (
	"github.com/gogo/protobuf/proto"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
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
	rg.MasterPtID = pb.GetID()
	rg.Term = pb.GetTerm()
	rg.Status = RGStatus(pb.GetStatus())
	if len(pb.GetPeers()) > 0 {
		rg.Peers = make([]Peer, len(pb.GetPeers()))
		for i := range pb.Peers {
			rg.Peers[i] = Peer{ID: pb.Peers[i].GetID(), PtRole: Role(pb.Peers[i].GetRole())}
		}
	}
}
