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
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	tickInterval = 400 * time.Millisecond
)

type RaftNode struct {
	confChangeC chan raftpb.ConfChange // proposed cluster config changes
	errorC      chan<- error           // errors from raft session

	nodeId   uint64 // real data node id
	database string
	id       uint64            // the raft node id, value is ptId + 1
	peers    map[uint32]uint64 // key is ptId, value is nodeId
	node     raft.Node
	tick     *time.Ticker

	ctx      context.Context
	cancelFn context.CancelFunc

	confState raftpb.ConfState

	raftStorage *raft.MemoryStorage

	ISend netstorage.SendRaftMessageToStorage

	logger *logger.Logger
}

func StartNode(dir string, nodeId uint64, database string, id uint64, peers []raft.Peer, client metaclient.MetaClient, transPeers map[uint32]uint64) *RaftNode {
	storage := raft.NewMemoryStorage()
	c := raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	confChangeC := make(chan raftpb.ConfChange)
	errorC := make(chan error)

	ctx := context.Background()
	cctx, cancelFn := context.WithCancel(ctx)
	n := &RaftNode{
		confChangeC: confChangeC,
		errorC:      errorC,
		nodeId:      nodeId,
		database:    database,
		id:          id,
		peers:       transPeers,
		tick:        time.NewTicker(tickInterval),
		ctx:         cctx,
		cancelFn:    cancelFn,
		raftStorage: storage,
		ISend:       netstorage.NewNetStorage(client),
	}

	n.node = raft.StartNode(&c, peers)

	go n.serveChannels()
	return n
}

// WithLogger sets logger to the RaftNode
func (n *RaftNode) WithLogger(log *logger.Logger) {
	n.logger = log.With(zap.String("database", n.database), zap.Uint32("partition", GetPtId(n.id)))
}

func (n *RaftNode) TransferLeadership(newLeader uint64) {
	// wait for electing a leader
	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}
		if n.node.Status().Lead == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	// try to transfer to a new leader
	oldLeader := n.node.Status().Lead
	if oldLeader != newLeader {
		n.node.TransferLeadership(n.ctx, oldLeader, newLeader)
	}
	n.logger.Info("raft try to elect a new leader", zap.Uint32("old leader partition", GetPtId(oldLeader)), zap.Uint32("new leader partition", GetPtId(newLeader)))

	var timer = time.NewTimer(10 * time.Second)
	for loop := true; loop && n.node.Status().Lead != newLeader; {
		select {
		case <-timer.C:
			loop = false
			n.logger.Error("try to transfer leadership timeout")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	n.logger.Info("try to transfer leadership finish")
}

// serveChannels handles the raft messages
func (n *RaftNode) serveChannels() {
	for {
		select {
		case <-n.ctx.Done():
			n.node.Stop()
			return
		case <-n.tick.C:
			n.node.Tick()

		case rd := <-n.node.Ready():
			n.SaveToStorage(&rd.HardState, rd.Entries, &rd.Snapshot)

			for i := range n.processMessages(rd.Messages) {
				n.send(rd.Messages[i])
			}
			n.node.Advance()
		}
	}
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (n *RaftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = n.confState
		}
	}
	return ms
}

// Stop stops this raft node
func (n *RaftNode) Stop() {
	if n.cancelFn != nil {
		n.cancelFn()
	}
	close(n.confChangeC)
	close(n.errorC)
}

// StepRaftMessage sends raft message to the raft state machine
func (n *RaftNode) StepRaftMessage(msg []raftpb.Message) {
	for _, m := range msg {
		err := n.node.Step(n.ctx, m)
		if err != nil || n.ctx.Err() != nil {
			n.logger.Error("step raft message error", zap.Error(err), zap.Any("ctx error", n.ctx.Err()))
		}
	}
}

// send sends the given RAFT message from this node.
func (n *RaftNode) send(msg raftpb.Message) {
	nodeId := n.peers[GetPtId(msg.To)]
	if nodeId == n.nodeId {
		n.logger.Error("sending message to itself")
		return
	}
	err := n.ISend.SendRaftMessages(nodeId, n.database, GetPtId(msg.To), msg)
	if err != nil {
		n.logger.Error("send raft message error", zap.Error(err))
	}
}

// SaveToStorage saves the hard state, entries, and snapshot to persistent storage, in that order.
func (n *RaftNode) SaveToStorage(h *raftpb.HardState, es []raftpb.Entry, s *raftpb.Snapshot) {
	for {
		if err := n.raftStorage.Append(es); err != nil {
			n.logger.Error("While trying to save Raft update. Retrying...", zap.Error(err))
		} else {
			return
		}
	}
}

// GetRaftNodeId returns raft node id. Greater 1 than the ptId.
func GetRaftNodeId(ptId uint32) uint64 {
	return uint64(ptId) + 1
}

// GetPtId returns pt view id. Less 1 than the raft node.
func GetPtId(raftNode uint64) uint32 {
	return uint32(raftNode) - 1
}
