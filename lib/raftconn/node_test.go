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

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
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
