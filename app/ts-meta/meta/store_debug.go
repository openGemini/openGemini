// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	mproto "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

func (s *Store) markTakeOver(enable bool) error {
	val := &mproto.MarkTakeoverCommand{
		Enable: proto.Bool(enable),
	}
	t := mproto.Command_MarkTakeoverCommand
	cmd := &mproto.Command{Type: &t}

	if err := proto.SetExtension(cmd, mproto.E_MarkTakeoverCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		return err
	}
	globalService.clusterManager.enableTakeover(enable)
	return nil
}

func (s *Store) markBalancer(enable bool) error {
	val := &mproto.MarkBalancerCommand{
		Enable: proto.Bool(enable),
	}
	t := mproto.Command_MarkBalancerCommand
	cmd := &mproto.Command{Type: &t}

	if err := proto.SetExtension(cmd, mproto.E_MarkBalancerCommand_Command, val); err != nil {
		panic(err)
	}

	return s.ApplyCmd(cmd)
}

func (s *Store) movePt(db string, pt uint32, to uint64) error {
	event, err := s.genMoveEvent(db, pt, to)
	if err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	err = s.data.CheckDataNodeAlive(event.getSrc())
	if err != nil {
		return err
	}
	err = s.data.CheckDataNodeAlive(event.getDst())
	if err != nil {
		return err
	}
	return globalService.msm.executeEvent(event)
}

func (s *Store) genMoveEvent(db string, pt uint32, to uint64) (MigrateEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.data.CheckCanMoveDb(db); err != nil {
		return nil, err
	}

	pti := &s.data.PtView[db][pt]
	if pti.Owner.NodeID == to {
		return nil, nil
	}

	if pti.Status == meta.Offline {
		return nil, errno.NewError(errno.PtIsAlreadyMigrating)
	}

	shardDurations := s.data.GetShardDurationsByDbPt(db, pt)
	ptiClone := *pti
	dbInfo := s.data.GetDBBriefInfo(db)
	dn := s.data.DataNode(to)
	if dn == nil {
		return nil, errno.NewError(errno.DataNodeNotFound)
	}
	moveEvent := NewMoveEvent(&meta.DbPtInfo{Db: db, Pti: &ptiClone, Shards: shardDurations, DBBriefInfo: dbInfo},
		pti.Owner.NodeID, to, dn.AliveConnID, true)
	return moveEvent, nil
}

func (s *Store) RepairPT(interval time.Duration) {
	if !s.config.RepairPT {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	checked := 0
	maxCheckTime := 3

	for checked < maxCheckTime {
		select {
		case <-s.stepDown:
			return
		case <-ticker.C:
			checked++
			pts := s.findOfflinePt()
			if len(pts) == 0 {
				return
			}

			for _, pt := range pts {
				s.repairPT(pt)
			}
		}
	}
}

func (s *Store) repairPT(pt *meta.DbPtInfo) {
	s.cacheMu.RLock()
	node := s.cacheData.DataNode(pt.Pti.Owner.NodeID)
	s.cacheMu.RUnlock()

	if node == nil {
		s.Logger.Error("[RepairPT] invalid node id", zap.Uint64("node id", pt.Pti.Owner.NodeID))
		return
	}

	if node.Status != serf.StatusAlive {
		s.Logger.Error("[RepairPT] data node is not alive", zap.Uint64("node id", node.ID))
		return
	}

	s.Logger.Info("[RepairPT] begin reload pt",
		zap.Uint64("node id", node.ID), zap.Uint32("pt id", pt.Pti.PtId))

	event := NewAssignEvent(pt, node.ID, node.AliveConnID, false)
	event.curState = StartAssign

	_, err := startAssignHandler(event)
	if err != nil {
		s.Logger.Error("[RepairPT] fail to send assign event",
			zap.Error(err), zap.Uint64("node id", node.ID), zap.Uint32("pt id", pt.Pti.PtId))
	}
}

func (s *Store) findOfflinePt() []*meta.DbPtInfo {
	var offlinePt []*meta.DbPtInfo

	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	s.cacheData.WalkPtView(func(db string, pt *meta.PtInfo) {
		if pt.Status != meta.Offline {
			return
		}

		dbi := s.cacheData.Database(db)
		if dbi == nil {
			return
		}

		offlinePt = append(offlinePt, &meta.DbPtInfo{
			Db:     db,
			Pti:    pt.Copy(),
			Shards: make(map[uint64]*meta.ShardDurationInfo),
			DBBriefInfo: &meta.DatabaseBriefInfo{
				Name:           db,
				EnableTagArray: dbi.EnableTagArray,
				Replicas:       dbi.ReplicaN,
			},
		})
	})

	return offlinePt
}
