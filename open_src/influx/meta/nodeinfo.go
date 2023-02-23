package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/data.go

2022.01.23 Add status as member of NodeInfo
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

type NodeStatus int64

const (
	StatusNone NodeStatus = iota
	StatusAlive
	StatusFailed
	StatusRestart

	StatusLeaving
	StatusLeft
)

// NodeInfo represents information about a single node in the cluster.
type NodeInfo struct {
	ID         uint64
	Host       string
	RPCAddr    string
	TCPHost    string
	Status     serf.MemberStatus
	LTime      uint64
	GossipAddr string
}

// clone returns a deep copy of ni.
func (ni NodeInfo) clone() NodeInfo { return ni }

// marshal serializes to a protobuf representation.
func (ni NodeInfo) marshal() *proto2.NodeInfo {
	pb := &proto2.NodeInfo{}
	pb.ID = proto.Uint64(ni.ID)
	pb.Host = proto.String(ni.Host)
	pb.RPCAddr = proto.String(ni.RPCAddr)
	pb.TCPHost = proto.String(ni.TCPHost)
	pb.Status = proto.Int64(int64(ni.Status))
	pb.LTime = proto.Uint64(ni.LTime)
	pb.GossipAddr = proto.String(ni.GossipAddr)
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (ni *NodeInfo) unmarshal(pb *proto2.NodeInfo) {
	ni.ID = pb.GetID()
	ni.Host = pb.GetHost()
	ni.RPCAddr = pb.GetRPCAddr()
	ni.TCPHost = pb.GetTCPHost()
	ni.Status = serf.MemberStatus(pb.GetStatus())
	ni.LTime = pb.GetLTime()
	ni.GossipAddr = pb.GetGossipAddr()
}

type DataNode struct {
	NodeInfo
	ConnID      uint64
	AliveConnID uint64
}

func (n *DataNode) MarshalBinary() ([]byte, error) {
	return proto.Marshal(n.marshal())
}

func (n *DataNode) UnmarshalBinary(buf []byte) error {
	var pb proto2.DataNode
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	n.unmarshal(&pb)
	return nil
}

func (n *DataNode) marshal() *proto2.DataNode {
	pb := &proto2.DataNode{}
	pb.Ni = n.NodeInfo.marshal()
	pb.ConnID = proto.Uint64(n.ConnID)
	pb.AliveConnID = proto.Uint64(n.AliveConnID)
	return pb
}
func (n *DataNode) unmarshal(pb *proto2.DataNode) {
	n.NodeInfo.unmarshal(pb.GetNi())
	n.ConnID = pb.GetConnID()
	n.AliveConnID = pb.GetAliveConnID()
}

// NodeInfos is a slice of NodeInfo used for sorting
type DataNodeInfos []DataNode

// Len implements sort.Interface.
func (n DataNodeInfos) Len() int { return len(n) }

// Swap implements sort.Interface.
func (n DataNodeInfos) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

// Less implements sort.Interface.
func (n DataNodeInfos) Less(i, j int) bool { return n[i].ID < n[j].ID }

// NodeInfos is a slice of NodeInfo used for sorting
type NodeInfos []NodeInfo

// Len implements sort.Interface.
func (n NodeInfos) Len() int { return len(n) }

// Swap implements sort.Interface.
func (n NodeInfos) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

// Less implements sort.Interface.
func (n NodeInfos) Less(i, j int) bool { return n[i].ID < n[j].ID }
