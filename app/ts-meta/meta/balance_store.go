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
	"math"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

func (s *Store) selectDbPtsToMove() []*MoveEvent {
	var moveEvents []*MoveEvent
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !config.GetHaEnable() || !s.data.BalancerEnabled {
		return nil
	}

	var aliveNodes []uint64
	for _, dataNode := range s.data.DataNodes {
		if dataNode.Status == serf.StatusAlive {
			aliveNodes = append(aliveNodes, dataNode.ID)
		}
	}
	if len(aliveNodes) == 0 {
		return nil
	}

	for db, _ := range s.data.PtView {
		if s.data.CheckCanMoveDb(db) != nil {
			continue
		}
		nodePtsMap := make(map[uint64]meta.DBPtInfos)
		for _, ptInfo := range s.data.PtView[db] {
			nodePtsMap[ptInfo.Owner.NodeID] = append(nodePtsMap[ptInfo.Owner.NodeID], ptInfo)
		}

		maxPtNum := -1
		minPtNum := math.MaxInt32
		var from uint64
		var to uint64
		for i := range aliveNodes {
			if _, ok := nodePtsMap[aliveNodes[i]]; !ok {
				nodePtsMap[aliveNodes[i]] = []meta.PtInfo{}
			}
			if minPtNum > len(nodePtsMap[aliveNodes[i]]) {
				minPtNum = len(nodePtsMap[aliveNodes[i]])
				to = aliveNodes[i]
			}
			if maxPtNum < len(nodePtsMap[aliveNodes[i]]) {
				maxPtNum = len(nodePtsMap[aliveNodes[i]])
				from = aliveNodes[i]
			}
		}

		if maxPtNum-minPtNum > 1 {
			moveEvents = s.balanceByPtNum(db, from, to, nodePtsMap, moveEvents)
		} else {
			moveEvents = s.balanceByOldAndNew(db, aliveNodes, nodePtsMap, moveEvents)
		}
	}
	return moveEvents
}

func (s *Store) balanceByPtNum(db string, from, to uint64, nodePtsMap map[uint64]meta.DBPtInfos, moveEvents []*MoveEvent) []*MoveEvent {
	var finalPt *meta.PtInfo
	isMoveOld := s.canMigrateOldPt(db, from, to, nodePtsMap)
	for i := 0; i <= len(nodePtsMap[from])-1; i++ {
		pt := nodePtsMap[from][i]
		if pt.Status != meta.Online {
			continue
		}
		isOldPt := s.isOldPt(db, pt)
		finalPt = &pt
		if (isMoveOld && isOldPt) || (!isMoveOld && !isOldPt) {
			shardDurations := s.data.GetShardDurationsByDbPt(db, pt.PtId)
			moveEvents = append(moveEvents, NewMoveEvent(&meta.DbPtInfo{Db: db, Pti: &pt, Shards: shardDurations},
				from, to, false))
			return moveEvents
		}
	}
	// expect to migrate old pt but all is new, or expect to migrate new pt but all is old
	if finalPt != nil {
		shardDurations := s.data.GetShardDurationsByDbPt(db, finalPt.PtId)
		moveEvents = append(moveEvents, NewMoveEvent(&meta.DbPtInfo{Db: db, Pti: finalPt, Shards: shardDurations},
			from, to, false))
	}
	return moveEvents
}

/*
balance by old pt and new pt, assume [0, ptNum/2) is old, and [ptNum/2, ptNum) is new

if ptNum = num(nodes) * 2, threshold = 1
if ptNum > num(nodes) * 2, threshold = maxPtNumInNodes/2+maxPtNumInNodes%2

	for example:
		ptNum = 6, num(nodes)=2, then threshold=3/2+3%2=2,ptDist [0,3,2],[1,4,5]
		ptNum=8, num(nodes)=3, then threshold=3/2+3%2=2,ptDist [0,5,6] [2,3,7],[1,4]

if ptNum < num(nodes)*2

	for example:
		ptNum=2, num(nodes)=2, threshold=1, ptDist [0] [1]
		ptNum=2, num(nodes)=3, threshold=1,ptDist [0] [1] []
		ptNum=4, num(nodes)=3, threshold=1, ptDist [0,1] [2] [3]
*/
func (s *Store) balanceByOldAndNew(db string, aliveNodes []uint64, nodePtsMap map[uint64]meta.DBPtInfos, moveEvents []*MoveEvent) []*MoveEvent {
	if len(s.data.PtView[db])%2 == 1 {
		return moveEvents
	}
	var fromPt meta.PtInfo
	var toPt meta.PtInfo
	var from uint64 = 0
	var to uint64 = 0
	threshold := s.calPtNumThresholdPerNode(db, aliveNodes)

	for i := range aliveNodes {
		oldPtNum := 0
		newPtNum := 0
		for _, ptInfo := range nodePtsMap[aliveNodes[i]] {
			if ptInfo.Status != meta.Online {
				continue
			}
			if s.isOldPt(db, ptInfo) {
				oldPtNum++
				if oldPtNum > threshold {
					from = aliveNodes[i]
					fromPt = ptInfo
					break
				}
			} else {
				newPtNum++
				if newPtNum > threshold {
					to = aliveNodes[i]
					toPt = ptInfo
					break
				}
			}
		}

		if from > 0 && to > 0 {
			shardDurations := s.data.GetShardDurationsByDbPt(db, fromPt.PtId)
			moveEvents = append(moveEvents, NewMoveEvent(
				&meta.DbPtInfo{Db: db, Pti: &fromPt, Shards: shardDurations}, from, to, false))
			shardDurations = s.data.GetShardDurationsByDbPt(db, toPt.PtId)
			moveEvents = append(moveEvents, NewMoveEvent(
				&meta.DbPtInfo{Db: db, Pti: &toPt, Shards: shardDurations}, to, from, false))
			break
		}
	}
	return moveEvents
}

func (s *Store) refreshShards(e MigrateEvent) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e.getPtInfo().Shards = s.data.GetShardDurationsByDbPt(e.getPtInfo().Db, e.getPtInfo().Pti.PtId)
}

func (s *Store) calPtNumThresholdPerNode(db string, aliveNodes []uint64) int {
	offset := 0
	if len(s.data.PtView[db])%len(aliveNodes) > 0 {
		offset = 1
	}
	// averagePtNum is the floor num of pt in every alive store node
	averagePtNum := len(s.data.PtView[db])/len(aliveNodes) + offset
	threshold := averagePtNum/2 + averagePtNum%2
	return threshold
}

func (s *Store) canMigrateOldPt(db string, from, to uint64, nodePtsMap map[uint64]meta.DBPtInfos) bool {
	oldPtNumFrom, newPtNumFrom := s.getOldNewPtNum(db, from, nodePtsMap)
	if oldPtNumFrom != newPtNumFrom {
		return oldPtNumFrom > newPtNumFrom
	}
	oldPtNumTo, newPtNumTo := s.getOldNewPtNum(db, to, nodePtsMap)
	return oldPtNumTo <= newPtNumTo
}

func (s *Store) getOldNewPtNum(db string, nodeId uint64, nodePtsMap map[uint64]meta.DBPtInfos) (int, int) {
	oldPtNum := 0
	newPtNum := 0
	for i := 0; i <= len(nodePtsMap[nodeId])-1; i++ {
		pt := nodePtsMap[nodeId][i]
		if pt.Status != meta.Online {
			continue
		}
		if s.isOldPt(db, pt) {
			oldPtNum++
		} else {
			newPtNum++
		}
	}
	return oldPtNum, newPtNum
}

func (s *Store) isOldPt(db string, ptInfo meta.PtInfo) bool {
	halfPtId := len(s.data.PtView[db]) / 2
	return ptInfo.PtId < uint32(halfPtId)
}
