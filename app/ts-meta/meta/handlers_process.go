/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
	"encoding/json"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

func (h *Ping) Process() (transport.Codec, error) {
	// if they're not asking to check all servers, just return who we think
	// the leader is
	rsp := &message.PingResponse{}
	if h.req.All != 0 {
		rsp.Leader = []byte(h.store.leader())
		return rsp, nil
	}
	rsp.Leader = []byte(h.store.leader())
	return rsp, nil
}

func (h *Peers) Process() (transport.Codec, error) {
	rsp := &message.PeersResponse{
		StatusCode: http.StatusOK,
	}
	rsp.Peers = h.store.peers()
	return rsp, nil
}

func (h *CreateNode) Process() (transport.Codec, error) {
	rsp := &message.CreateNodeResponse{}

	httpAddr := h.req.WriteHost
	tcpAddr := h.req.QueryHost
	b, err := h.store.createDataNode(httpAddr, tcpAddr)
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.Data = b
	return rsp, nil
}

func (h *Snapshot) Process() (transport.Codec, error) {
	rsp := &message.SnapshotResponse{}
	if h.isClosed() {
		rsp.Err = "server closed"
		return rsp, nil
	}

	index := h.req.Index
	checkRaft := time.After(2 * time.Second)
	tries := 0
	for {
		select {
		case <-h.store.afterIndex(index):
			rsp.Data = h.store.getSnapshot()
			h.logger.Info("serveSnapshot ok", zap.Uint64("index", index))
			return rsp, nil
		case <-h.closing:
			rsp.Err = "server closed"
			return rsp, nil
		case <-checkRaft:
			checkRaft = time.After(2 * time.Second)
			if h.store.isCandidate() {
				tries++
				if tries >= 3 {
					rsp.Err = "server closed"
					return rsp, nil
				}
				h.logger.Info("checkRaft failed", zap.Int("tries", tries))
				continue
			}
			return rsp, nil
		}
	}
}

func (h *Update) Process() (transport.Codec, error) {
	rsp := &message.UpdateResponse{}
	if h.isClosed() {
		rsp.Err = "server closed"
		return rsp, nil
	}
	body := h.req.Body

	n := &meta.NodeInfo{}
	if err := json.Unmarshal(body, n); err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}

	node, err := h.store.Join(n)
	if err == raft.ErrNotLeader {
		rsp.Err = "node is not the leader"
		return rsp, nil
	}
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.Data, err = json.Marshal(node)
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	return rsp, nil
}

func (h *Execute) Process() (transport.Codec, error) {
	rsp := &message.ExecuteResponse{}
	if h.isClosed() {
		rsp.Err = "server closed"
		return rsp, nil
	}

	body := h.req.Body
	var err error
	// Make sure it's a valid command.
	if _, err = validateCommand(body); err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}

	// Apply the command to the store.
	if err := h.store.apply(body); err != nil {
		// We aren't the leader
		if errno.Equal(err, errno.MetaIsNotLeader) || errno.Equal(err, errno.RaftIsNotOpen) {
			rsp.Err = err.Error()
		}
		// Error wasn't a leadership error so pass it back to client.
		rsp.ErrCommand = err.Error()
		return rsp, nil
	} else {
		// Apply was successful. Return the new store index to the client.
		rsp.Index = h.store.index()
	}
	return rsp, nil
}

func (h *Report) Process() (transport.Codec, error) {
	rsp := &message.ReportResponse{}
	if h.isClosed() {
		rsp.Err = "server closed"
		return rsp, nil
	}

	body := h.req.Body
	// Make sure it's a valid command.
	if _, err := validateCommand(body); err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}

	// Apply the command to the store.
	if err := h.store.UpdateLoad(body); err != nil {
		// We aren't the leader
		if err == raft.ErrNotLeader {
			rsp.Err = "node is not the leader"
			return rsp, nil
		}
		// Error wasn't a leadership error so pass it back to client.
		rsp.ErrCommand = err.Error()
		return rsp, nil
	} else {
		// Apply was successful. Return the new store index to the client.
		rsp.Index = h.store.index()
	}
	return rsp, nil
}

func (h *GetShardInfo) Process() (transport.Codec, error) {
	rsp := &message.GetShardInfoResponse{}

	body := h.req.Body
	// Make sure it's a valid command.
	if _, err := validateCommand(body); err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}

	b, err := h.store.getShardAuxInfo(body)

	if err == raft.ErrNotLeader {
		rsp.Err = "node is not the leader"
		return rsp, nil
	}

	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.Data = b
	return rsp, nil
}
