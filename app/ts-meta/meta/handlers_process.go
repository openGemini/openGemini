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
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
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
		h.logger.Error("createNode fail", zap.Error(err))
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
			rsp.Data = h.store.getSnapshot(metaclient.Role(h.req.Role))
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

func needRetry(err error) bool {
	return !errno.Equal(err, errno.ReplicaNodeNumIncorrect)
}

func (h *Execute) Process() (transport.Codec, error) {
	rsp := &message.ExecuteResponse{}
	if h.isClosed() {
		rsp.Err = "server closed"
		return rsp, nil
	}

	body := h.req.Body
	var err error
	var cmd *proto2.Command
	// Make sure it's a valid command.
	if cmd, err = validateCommand(body); err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}

	if cmd.GetType() == proto2.Command_CreateDatabaseCommand {
		err = createDatabase(cmd)
		if err != nil {
			if needRetry(err) {
				rsp.Err = err.Error()
			} else {
				rsp.ErrCommand = err.Error()
			}
			return rsp, nil
		}
	}

	// Apply the command to the store.
	if err := h.store.apply(body); err != nil {
		// We aren't the leader
		if isRetryError(err) {
			rsp.Err = err.Error()
			return rsp, nil
		}
		// Error wasn't a leadership error so pass it back to client.
		rsp.ErrCommand = err.Error()
		return rsp, nil
	}
	// Apply was successful. Return the new store index to the client.
	rsp.Index = h.store.index()
	return rsp, nil
}

func isRetryError(err error) bool {
	return errno.Equal(err, errno.MetaIsNotLeader) ||
		errno.Equal(err, errno.RaftIsNotOpen) ||
		errno.Equal(err, errno.ConflictWithEvent)
}

func createDatabase(cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDatabaseCommand_Command)
	v, ok := ext.(*proto2.CreateDatabaseCommand)
	if !ok {
		return fmt.Errorf("%s is not a CreateDatabaseCommand", ext)
	}

	// 1.create db pt view
	val := &proto2.CreateDbPtViewCommand{
		DbName:     v.Name,
		ReplicaNum: v.ReplicaNum,
	}
	t := proto2.Command_CreateDbPtViewCommand
	command := &proto2.Command{Type: &t}
	if err := proto.SetExtension(command, proto2.E_CreateDbPtViewCommand_Command, val); err != nil {
		panic(err)
	}
	if err := globalService.store.ApplyCmd(command); err != nil {
		return err
	}

	// 2.assign db pt
	dbPts, err := globalService.store.getDbPtsByDbname(v.GetName(), v.GetEnableTagArray())
	if err != nil {
		return err
	}

	once := sync.Once{}
	wg := sync.WaitGroup{}
	wg.Add(len(dbPts))
	for _, dbPt := range dbPts {
		go func(pt *meta.DbPtInfo) {
			nodeId := pt.Pti.Owner.NodeID
			aliveConnId, innerErr := globalService.store.getDataNodeAliveConnId(nodeId)
			if innerErr == nil {
				// Do not wait db pt assign successfully.
				// If wait, in write-available-first, create database will failed due to store is not alive.
				innerErr = globalService.balanceManager.assignDbPt(pt, nodeId, aliveConnId, true)
			}

			if innerErr != nil {
				once.Do(func() {
					err = innerErr
				})
			}
			wg.Done()
		}(dbPt)
	}
	wg.Wait()

	return err
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
		h.logger.Error("get shard aux info fail", zap.Error(err))
		switch stdErr := err.(type) {
		case *errno.Error:
			rsp.ErrCode = stdErr.Errno()
		default:
		}
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.Data = b
	return rsp, nil
}

func (h *GetDownSampleInfo) Process() (transport.Codec, error) {
	rsp := &message.GetDownSampleInfoResponse{}
	if h.isClosed() {
		rsp.Err = "server closed"
		return rsp, nil
	}
	b, err := h.store.GetDownSampleInfo()
	if err != nil {
		rsp.Err = err.Error()
	}
	rsp.Data = b
	return rsp, nil
}

func (h *GetRpMstInfos) Process() (transport.Codec, error) {
	rsp := &message.GetRpMstInfosResponse{}
	if h.isClosed() {
		rsp.Err = "server closed"
		return rsp, nil
	}
	b, err := h.store.GetRpMstInfos(h.req.DbName, h.req.RpName, h.req.DataTypes)
	if err != nil {
		rsp.Err = err.Error()
	}
	rsp.Data = b
	return rsp, nil
}

func (h *GetStreamInfo) Process() (transport.Codec, error) {
	rsp := &message.GetStreamInfoResponse{}

	b, err := h.store.getStreamInfo()

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

func (h *GetMeasurementInfo) Process() (transport.Codec, error) {
	rsp := &message.GetMeasurementInfoResponse{}
	b, err := h.store.getMeasurementInfo(h.req.DbName, h.req.RpName, h.req.MstName)
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.Data = b
	return rsp, nil
}

func (h *GetMeasurementsInfo) Process() (transport.Codec, error) {
	rsp := &message.GetMeasurementsInfoResponse{}
	b, err := h.store.getMeasurementsInfo(h.req.DbName, h.req.RpName)
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.Data = b
	return rsp, nil
}

func (h *RegisterQueryIDOffset) Process() (transport.Codec, error) {
	rsp := &message.RegisterQueryIDOffsetResponse{}
	offset, err := h.store.registerQueryIDOffset(meta.SQLHost(h.req.Host))
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.Offset = offset
	return rsp, nil
}

func (h *Sql2MetaHeartbeat) Process() (transport.Codec, error) {
	rsp := &message.Sql2MetaHeartbeatResponse{}
	err := h.store.handlerSql2MetaHeartbeat(h.req.Host)
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	return rsp, nil
}

func (h *GetContinuousQueryLease) Process() (transport.Codec, error) {
	rsp := &message.GetContinuousQueryLeaseResponse{}
	cqNames, err := h.store.getContinuousQueryLease(h.req.Host)
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	rsp.CQNames = cqNames
	return rsp, nil
}

func (h *VerifyDataNodeStatus) Process() (transport.Codec, error) {
	rsp := &message.VerifyDataNodeStatusResponse{}
	err := h.store.verifyDataNodeStatus(h.req.NodeID)
	if err != nil {
		rsp.Err = err.Error()
		return rsp, nil
	}
	return rsp, nil
}
