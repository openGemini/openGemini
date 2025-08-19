// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package raftconn

import (
	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type SendRaftMessageToStorage interface {
	SendRaftMessages(nodeID uint64, database string, pt uint32, msgs raftpb.Message) error
}

type RaftConnStore struct {
	mc meta.MetaClient
}

func NewRaftConnStore(mc meta.MetaClient) SendRaftMessageToStorage {
	return &RaftConnStore{mc: mc}
}

func (s *RaftConnStore) SendRaftMessages(nodeID uint64, database string, pt uint32, msgs raftpb.Message) error {
	req := &msgservice.RaftMessagesRequest{}
	req.Database = database
	req.PtId = pt
	req.RaftMessage = msgs

	node, err := s.mc.DataNode(nodeID)
	if err != nil || node.Status != serf.StatusAlive {
		return nil
	}

	v, err := s.raftRequestWithNodeId(nodeID, msgservice.RaftMessagesRequestMessage, req)
	if err != nil {
		return err
	}
	resp, ok := v.(*msgservice.RaftMessagesResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.RaftMsgResponse", v)
	}
	if resp.GetErrMsg() != "" {
		return errors.New(resp.GetErrMsg())
	}
	return nil
}

func (s *RaftConnStore) raftRequestWithNodeId(nodeID uint64, typ uint8, data codec.BinaryCodec) (interface{}, error) {
	r := msgservice.NewRequester(typ, data, s.mc)
	r.SetToInsert()
	r.SetTimeout(config.RaftMsgTimeout)
	err := r.InitWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	return r.RaftMsg()
}
