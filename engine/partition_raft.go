// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"errors"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/pointsdecoder"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/raftlog"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type raftNodeRequest interface {
	StepRaftMessage(msg []raftpb.Message)
	GetProposeC() chan []byte
	GetCommitC() <-chan *raftconn.Commit
	GenerateProposeId() uint64
	GetIdentity() string
	AddCommittedDataC(*raftlog.DataWrapper) (chan error, error)
	RemoveCommittedDataC(*raftlog.DataWrapper)
	RetCommittedDataC(*raftlog.DataWrapper, error)
	Stop()
}

func readReplayForReplication(ReplayC <-chan *raftconn.Commit, client metaclient.MetaClient, storage netstorage.StorageService) {
	if len(ReplayC) == 0 {
		return
	}
	for commit := range ReplayC {
		if commit == nil {
			continue
		}
		database := commit.Database
		ptId := commit.PtId
		for _, data := range commit.Data {
			dataWrapper, _ := raftlog.Unmarshal(data)
			if dataWrapper.DataType == raftlog.Normal {
				err := dealNormalData(dataWrapper, database, ptId, client, storage, nil)
				if err != nil {
					logger.GetLogger().Error("deal normal data failed", zap.Error(err))
				}
			}
		}
	}
}

func readCommitFromRaft(node *raftconn.RaftNode, client metaclient.MetaClient, storage netstorage.StorageService) {
	commitC := node.GetCommitC()
	for commit := range commitC {
		if commit == nil {
			// TODO: signaled to load snapshot
			continue
		}
		committedIndex := commit.CommittedIndex
		database := commit.Database
		ptId := commit.PtId
		for _, data := range commit.Data {
			dealCommitData(node, client, storage, data, database, ptId)
		}
		close(commit.ApplyDoneC)
		node.SnapShotter.TryToUpdateCommittedIndex(committedIndex)
	}
}

func retCommittedDataC(node *raftconn.RaftNode, dw *raftlog.DataWrapper, committedErr error) {
	if dw != nil && dw.Identity == node.GetIdentity() {
		node.RetCommittedDataC(dw, committedErr)
	}
}

func dealCommitData(node *raftconn.RaftNode, client metaclient.MetaClient, storage netstorage.StorageService, data []byte, database string, ptId uint32) {
	dataWrapper, err := raftlog.Unmarshal(data)
	defer retCommittedDataC(node, dataWrapper, err)
	if err != nil {
		logger.GetLogger().Error("Unmarshal commit data failed", zap.Error(err))
		return
	}
	if dataWrapper.DataType == raftlog.Normal {
		err = dealNormalData(dataWrapper, database, ptId, client, storage, node)
		if err != nil {
			logger.GetLogger().Error("deal normal data failed", zap.Error(err))
		}
	} else if dataWrapper.DataType == raftlog.ClearEntryLog {
		bytes := dataWrapper.Data
		index := encoding.UnmarshalUint64(bytes)
		node.Store.DeleteBefore(index)
	} else {
		logger.GetLogger().Error("not support this data type")
	}
}

func dealNormalData(dataWrapper *raftlog.DataWrapper, database string, ptId uint32, client metaclient.MetaClient,
	storage netstorage.StorageService, node *raftconn.RaftNode) error {
	tail := dataWrapper.GetData()
	ww := pointsdecoder.GetDecoderWork()
	masterShId, _, _, err := ww.DecodeShardAndRows(database, "", ptId, tail)
	if err != nil {
		logger.GetLogger().Error("decode shard and rows failed", zap.Error(err))
		pointsdecoder.PutDecoderWork(ww)
		return err
	}

	// Determine the location of this shard and whether it still exists
	db, rp, sgi := client.ShardOwner(masterShId)
	if db != database {
		logger.GetLogger().Error(fmt.Sprintf("exp db: %v, but got: %v", database, db))
		pointsdecoder.PutDecoderWork(ww)
		return errors.New("db is not same")
	}

	if sgi == nil || len(sgi.Shards) <= int(ptId) {
		pointsdecoder.PutDecoderWork(ww)
		return errors.New("sgi shards less than ptid")
	}
	shardID := sgi.Shards[ptId].ID
	var snapShotter *raftlog.SnapShotter
	if node == nil {
		snapShotter = nil
	} else {
		snapShotter = node.SnapShotter
	}
	err = storage.Write(database, rp, ww.GetRows()[0].Name, ptId, shardID, func() error {
		return storage.WriteDataFunc(database, rp, ptId, shardID, ww.GetRows(), nil, snapShotter)
	})
	if err != nil {
		pointsdecoder.PutDecoderWork(ww)
		logger.GetLogger().Error("write points to storage failed", zap.Error(err))
		return err
	}
	return nil
}
