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

package meta

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	msgservice_data "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type ReplicaModeColdSelector struct {
	store      *Store
	context    context.Context
	cancelFunc context.CancelFunc
	Logger     *logger.Logger
}

func NewReplicaModeColdSelector(store *Store, ctx context.Context) *ReplicaModeColdSelector {
	ctx2, cancelFunc := context.WithCancel(ctx)
	return &ReplicaModeColdSelector{
		store:      store,
		context:    ctx2,
		cancelFunc: cancelFunc,
		Logger:     logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "ReplicaModeColdSelector")),
	}
}

func (s *ReplicaModeColdSelector) handle(interval toml.Duration) {
	ticker := time.NewTicker(time.Duration(interval))
	go func() {
		for {
			select {
			case <-ticker.C:
				if s.store.IsLeader() {
					s.Logger.Info("start to select clear events")
					events := s.SelectClearEvents()
					s.dealEvents(events)
				}
			case <-s.context.Done():
				s.Logger.Error("Stopping event processing due to context cancellation.")
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *ReplicaModeColdSelector) Close() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

func (s *ReplicaModeColdSelector) SelectClearEvents() []*ClearEvent {
	s.store.mu.RLock()
	defer s.store.mu.RUnlock()
	data := s.store.data
	var events []*ClearEvent
	for _, db := range data.Databases {
		for _, rp := range db.RetentionPolicies {
		indexGroup:
			for i := range rp.IndexGroups {
				coldInfo := rp.IndexGroups[i].ClearInfo
				if coldInfo == nil {
					continue
				}
				for k := range coldInfo.ClearPeers {
					toClearIndexId := coldInfo.ClearPeers[k]
					for m := range rp.IndexGroups[i].Indexes {
						if rp.IndexGroups[i].Indexes[m].ID == toClearIndexId {
							if rp.IndexGroups[i].Indexes[m].Tier < util.Cold {
								break indexGroup
							}
							// assemble clear event
							pt := rp.IndexGroups[i].Indexes[m].Owners[0]
							pair := &Pair{}
							var toAdd bool
							if rp.IndexGroups[i].Indexes[m].Tier != util.Cleared {
								indexPair := &IndexPair{
									ToClearIndex: toClearIndexId,
									NoClearIndex: coldInfo.NoClearIndexId,
								}
								pair.IndexInfo = indexPair
								toAdd = true
							}
							var shardPairs []*ShardPair
							for _, sg := range rp.ShardGroups {
								shardPair := &ShardPair{}
								var flag bool
								for _, shard := range sg.Shards {
									if shard.IndexID == toClearIndexId && shard.Tier != util.Cleared {
										flag = true
										shardPair.ToClearShard = shard.ID
									}
									if shard.IndexID == coldInfo.NoClearIndexId {
										shardPair.NoClearShard = shard.ID
									}
								}
								if flag {
									shardPairs = append(shardPairs, shardPair)
								}
							}
							if len(shardPairs) > 0 {
								pair.ShardInfo = shardPairs
								toAdd = true
							}
							if toAdd {
								events = append(events, &ClearEvent{
									Db:   db.Name,
									Rp:   rp.Name,
									Pt:   pt,
									pair: pair,
								})
							}
						}
					}
				}
			}
		}
	}
	return events
}

func (s *ReplicaModeColdSelector) dealEvents(events []*ClearEvent) {
	for i := range events {
		event := events[i]
		s.Logger.Info("event is ", zap.String("db", event.Db), zap.Uint32("pt", event.Pt))
		nodeId, err := s.store.GetData().GetDbPtOwner(events[i].Db, events[i].Pt)
		if err != nil {
			s.Logger.Error("get node id error ", zap.Error(err))
			continue
		}
		events[i].NodeId = nodeId
		err = s.store.NetStore.SendClearEvents(nodeId, marshal(events[i]))
		if err != nil {
			s.Logger.Error("send clear events error", zap.String("db", events[i].Db), zap.Uint64("nodeId", nodeId), zap.Uint32("pt", events[i].Pt), zap.Error(err))
		} else {
			s.Logger.Info("send clear events success", zap.String("db", events[i].Db), zap.Uint64("nodeId", nodeId), zap.Uint32("pt", events[i].Pt))
		}
	}

}

func marshal(event *ClearEvent) transport.Codec {
	request := msgservice.NewSendClearEventsRequest()

	pair := &msgservice_data.Pair{}

	if event.pair.IndexInfo != nil {
		indexPair := &msgservice_data.IndexPair{
			ToClearIndex: &event.pair.IndexInfo.ToClearIndex,
			NoClearIndex: &event.pair.IndexInfo.NoClearIndex,
		}
		pair.IndexInfo = indexPair
	}

	if len(event.pair.ShardInfo) > 0 {
		pair.ShardInfo = make([]*msgservice_data.ShardPair, len(event.pair.ShardInfo))
	}

	for i := range event.pair.ShardInfo {
		shardPair := event.pair.ShardInfo[i]
		pair.ShardInfo[i] = &msgservice_data.ShardPair{
			ToClearShard: &shardPair.ToClearShard,
			NoClearShard: &shardPair.NoClearShard,
		}
	}

	request.Pair = pair
	request.NodeId = &event.NodeId
	request.PtId = &event.Pt
	request.Database = &event.Db
	request.Rp = &event.Rp
	return request
}

type ClearEvent struct {
	Db     string
	Rp     string
	Pt     uint32
	NodeId uint64
	pair   *Pair
}

type IndexPair struct {
	NoClearIndex uint64
	ToClearIndex uint64
}

type ShardPair struct {
	NoClearShard uint64
	ToClearShard uint64
}

type Pair struct {
	IndexInfo *IndexPair
	ShardInfo []*ShardPair
}
