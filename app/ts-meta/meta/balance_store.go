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
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

type assignPtData struct {
	from    *mvPtInfos
	to      *[]uint32
	needNum *int
	ptTH    int
	dnId    uint64
	tasks   *taskDatas
}

type taskData struct {
	ptId   uint32 // id of the PT to be moved
	srcDn  uint64 // id of the node to which the PT belongs
	destDn uint64 // id of the destination node to be moved by the PT.
}
type taskDatas []taskData

func (t taskData) String() string {
	return fmt.Sprintf("(%d:%d->%d)", t.ptId, t.srcDn, t.destDn)
}

func (ts taskDatas) String() string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for _, t := range ts {
		sb.WriteString(t.String())
		sb.WriteString(",")
	}
	sb.WriteString("]")
	return sb.String()
}

type extractPtData struct {
	from  *[]uint32
	to    *mvPtInfos
	mvNum *int
	ptTH  int
	dnId  uint64
}

type ruleExtrData struct {
	selLow            *bool
	fromLow, fromHigh *[]uint32
	toLow, toHigh     *mvPtInfos
	mvNum             *int
	ptTH              int
	dnId              uint64
}

type mvPtInfo struct {
	ptId uint32 // id of the PT to be moved
	dnId uint64 // id of the node to which the PT belongs
}
type mvPtInfos []mvPtInfo

func (mp mvPtInfos) String() string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for _, pi := range mp {
		sb.WriteString(fmt.Sprintf("%d(%d),", pi.ptId, pi.dnId))
	}
	sb.WriteString("]")
	return sb.String()
}

type DbPtIds struct {
	lowIds  []uint32 // PT id list in the range [0, half)
	highIds []uint32 // PT id list in the range [half, ptNum)
}

// halfPtId: median value of PT id, that is half.
// max(min): maximum(minimum) number of PTs assigned to each node
// ptTHNum: threshold assigned to each node PT (in [0, half) or [half, ptNum)
type dbBalanceBasicData struct {
	dbName                      string
	totalNumOfPt, aliveDnNum    int // total number of PTs, number of available data nodes
	halfPtId, max, min, ptTHNum int
	highLvlNum                  int                // number of nodes to which the number of PTs that can be assigned is max.
	minOfLH                     int                // mininum number of PTs assigned to each node PT (in low:[0, half) or high:[half, ptNum).
	dnPtIds                     map[uint64]DbPtIds // key: node id
	aliveDns                    *[]uint64          // list of available data nodes
}

func (s *Store) balanceDBPts() []*MoveEvent {
	var moveEvents []*MoveEvent
	s.mu.RLock()
	defer s.mu.RUnlock()

	if config.GetHaPolicy() != config.SharedStorage || !s.data.BalancerEnabled {
		return nil
	}

	var aliveNodes []uint64
	for _, dataNode := range s.data.DataNodes {
		if dataNode.SegregateStatus == meta.Normal && dataNode.Status == serf.StatusAlive && dataNode.AliveConnID == dataNode.ConnID {
			aliveNodes = append(aliveNodes, dataNode.ID)
		}
	}
	if len(aliveNodes) == 0 {
		return nil
	}
	sort.Slice(aliveNodes, func(i, j int) bool {
		return aliveNodes[i] < aliveNodes[j]
	})

	var dbLst []string
	for k := range s.data.PtView {
		dbLst = append(dbLst, k)
	}
	sort.Strings(dbLst)

	for _, db := range dbLst {
		if err := s.data.CheckCanMoveDb(db); err != nil {
			logger.GetLogger().Error("balance pts db error", zap.String("db", db), zap.String("error", err.Error()))
			continue
		}
		dbInfo := s.data.GetDBBriefInfo(db)
		moveEvents = s.balanceOneDBPts(db, &aliveNodes, moveEvents, dbInfo)
	}
	return moveEvents
}

func (s *Store) extractNeedMovePtLst(bbd *dbBalanceBasicData) (low, high mvPtInfos) {
	var mvlowPts, mvhighPts mvPtInfos

	//step1. deal: dbpt count is any number
	generalSceneExtract(bbd, &mvlowPts, &mvhighPts)

	//step2. deal: dbpt count in (x * dnCount, x*dnCount + dnCount) and  x>=2
	specialSceneExtract(bbd, &mvlowPts, &mvhighPts)
	return mvlowPts, mvhighPts
}

// Special scenario: pt count in (x * dnCount, x*dnCount + dnCount) x>=2
func specialSceneExtract(bbd *dbBalanceBasicData, toLowPts, toHighPts *mvPtInfos) {
	if bbd.min < 2 || bbd.min == bbd.max {
		return
	}
	var lowCnt, highCnt int
	for dn := range bbd.dnPtIds {
		lc := len(bbd.dnPtIds[dn].lowIds)
		if lc < bbd.minOfLH {
			lowCnt += (bbd.minOfLH - lc)
		}

		hc := len(bbd.dnPtIds[dn].highIds)
		if hc < bbd.minOfLH {
			highCnt += (bbd.minOfLH - hc)
		}
	}
	lowCnt -= len(*toLowPts)
	highCnt -= len(*toHighPts)

	// use slices to control the traversal sequence of maps
	onceSpecExtrace := func(skipMax bool) {
		if lowCnt <= 0 && highCnt <= 0 {
			return
		}
		for _, dn := range *(bbd.aliveDns) {
			low := bbd.dnPtIds[dn].lowIds
			high := bbd.dnPtIds[dn].highIds
			if skipMax && (len(low)+len(high) == bbd.max) {
				continue // reduce the number of dns participating in movept.
			}

			epdLow := extractPtData{from: &low, to: toLowPts, mvNum: &lowCnt, ptTH: bbd.minOfLH, dnId: dn}
			for {
				if !extractPtId(&epdLow) {
					break
				}
			}

			epdHigh := extractPtData{from: &high, to: toHighPts, mvNum: &highCnt, ptTH: bbd.minOfLH, dnId: dn}
			for {
				if !extractPtId(&epdHigh) {
					break
				}
			}
			bbd.dnPtIds[dn] = DbPtIds{lowIds: low, highIds: high}
			if lowCnt <= 0 && highCnt <= 0 {
				break
			}
		}
	}

	onceSpecExtrace(true) // extract: skip max data node
	onceSpecExtrace(false)

	if lowCnt > 0 || highCnt > 0 {
		logger.GetLogger().Error("spceial scence extrace", zap.String("db", bbd.dbName),
			zap.Int("low count", lowCnt), zap.Int("high", highCnt))
	}
}

func (s *Store) balanceOneDBPts(db string, aliveNodes *[]uint64, moveEvents []*MoveEvent, dbBriefInfo *meta.DatabaseBriefInfo) []*MoveEvent {
	st := time.Now()
	bbd := dbBalanceBasicData{dbName: db, aliveDns: aliveNodes}
	if err := s.genDBBalanceBasicData(&bbd); err != nil {
		logger.GetLogger().Error("can not balance", zap.String("db", db), zap.Error(err))
		return nil
	}

	mvlowPts, mvhighPts := s.extractNeedMovePtLst(&bbd)
	tasks := s.balanceAssignPts(&bbd, &mvlowPts, &mvhighPts)

	// create move pt tasks
	moveEvents = s.addMovePtTasks(db, &tasks, moveEvents, dbBriefInfo)
	if len(moveEvents) != 0 {
		logger.GetLogger().Info("[time use] balance pts", zap.String("duration", time.Since(st).String()),
			zap.String("db", db), zap.String("tasks", tasks.String()))
	}
	return moveEvents
}

func (s *Store) addMovePtTasks(db string, tasks *taskDatas, moveEvents []*MoveEvent, dbBriefInfo *meta.DatabaseBriefInfo) []*MoveEvent {
	for _, task := range *tasks {
		if task.srcDn == task.destDn {
			logger.GetLogger().Info("no need exec mvpt task", zap.String("db", db), zap.String("info", task.String()))
			continue
		}
		shardDurations := s.data.GetShardDurationsByDbPt(db, task.ptId)
		pt := s.data.GetPtInfo(db, task.ptId)
		dn := s.data.DataNode(task.destDn)
		e := NewMoveEvent(&meta.DbPtInfo{Db: db, Pti: pt, Shards: shardDurations, DBBriefInfo: dbBriefInfo}, task.srcDn, task.destDn, dn.AliveConnID, false)
		logger.GetLogger().Info("new move event", zap.String("event", e.String()))
		moveEvents = append(moveEvents, e)
	}
	return moveEvents
}

func (s *Store) balanceAssignPts(bbd *dbBalanceBasicData, lowPts, highPts *mvPtInfos) taskDatas {
	var tasks taskDatas

	//step1. pre-assign, for a specified scenario, ensure that the minimum number of low and high ptids are assigned to each data
	specialSecneAssign(bbd, lowPts, highPts, &tasks)

	//step2. assign to all nodes
	assignAllPts(bbd, bbd.ptTHNum, lowPts, highPts, &tasks)
	assignAllPts(bbd, math.MaxInt32, lowPts, highPts, &tasks) // replenishment of assignment is required to prevent omissions
	return tasks
}

func assignAllPts(bbd *dbBalanceBasicData, ptTHNum int, fromLow, fromHigh *mvPtInfos, tds *taskDatas) {
	if len(*fromLow) == 0 && len(*fromHigh) == 0 {
		return
	}

	offsetNum := bbd.highLvlNum
	if offsetNum > 0 {
		// adjust offsetNum: deduct the nodes that have reached bbd.max.
		for _, dn := range *(bbd.aliveDns) {
			if len(bbd.dnPtIds[dn].lowIds)+len(bbd.dnPtIds[dn].highIds) >= bbd.max {
				offsetNum--
			}
		}
		if offsetNum < 0 {
			logger.GetLogger().Error("assignAllpts fail, offset < 0", zap.String("db", bbd.dbName), zap.Int("offset", offsetNum))
		}
	}

	// use slices to control the traversal sequence of maps.
	for _, dn := range *(bbd.aliveDns) {
		ptTotalNum := len(bbd.dnPtIds[dn].lowIds) + len(bbd.dnPtIds[dn].highIds)
		if ptTotalNum >= bbd.max {
			continue
		}
		needNum := bbd.min - ptTotalNum
		if offsetNum > 0 {
			needNum++
		}

		low := bbd.dnPtIds[dn].lowIds
		high := bbd.dnPtIds[dn].highIds
		apdLow := assignPtData{from: fromLow, to: &low, needNum: &needNum, ptTH: ptTHNum, dnId: dn, tasks: tds}
		apdHigh := assignPtData{from: fromHigh, to: &high, needNum: &needNum, ptTH: ptTHNum, dnId: dn, tasks: tds}
		for needNum > 0 {
			if assignDnPts(&apdLow) || assignDnPts(&apdHigh) {
				continue
			}
			break
		}
		bbd.dnPtIds[dn] = DbPtIds{lowIds: low, highIds: high}
		if needNum <= 0 && offsetNum > 0 {
			offsetNum--
		}
	}

	if len(*fromLow) > 0 || len(*fromHigh) > 0 {
		logger.GetLogger().Error("assignAllpts fail, exist no assgin ptids", zap.String("db", bbd.dbName),
			zap.String("low", (*fromLow).String()), zap.String("high", (*fromHigh).String()))
	}
}

// Special scenario: ptCount in (x * dnCount, x*dnCount + dnCount) x>=2
func specialSecneAssign(bbd *dbBalanceBasicData, fromLow, fromHigh *mvPtInfos, tds *taskDatas) {
	if len(*fromLow) == 0 && len(*fromHigh) == 0 {
		return
	}
	if bbd.min < 2 || bbd.min == bbd.max {
		return
	}
	// use slices to control the traversal sequence of maps.
	for _, dn := range *(bbd.aliveDns) {
		low := bbd.dnPtIds[dn].lowIds
		needCnt := bbd.minOfLH - len(low)
		apdLow := assignPtData{from: fromLow, to: &low, needNum: &needCnt, ptTH: bbd.minOfLH, dnId: dn, tasks: tds}
		for needCnt > 0 {
			if !assignDnPts(&apdLow) {
				break
			}
		}

		high := bbd.dnPtIds[dn].highIds
		needCnt = bbd.minOfLH - len(high)
		apdHigh := assignPtData{from: fromHigh, to: &high, needNum: &needCnt, ptTH: bbd.minOfLH, dnId: dn, tasks: tds}
		for needCnt > 0 {
			if !assignDnPts(&apdHigh) {
				break
			}
		}
		bbd.dnPtIds[dn] = DbPtIds{lowIds: low, highIds: high}
	}
}

func assignDnPts(apd *assignPtData) bool {
	if *(apd.needNum) <= 0 {
		return false
	}

	if len(*(apd.from)) > 0 && len(*(apd.to)) < apd.ptTH {
		t := taskData{
			ptId:   (*(apd.from))[0].ptId,
			srcDn:  (*(apd.from))[0].dnId,
			destDn: apd.dnId,
		}
		*(apd.tasks) = append(*(apd.tasks), t)
		*(apd.to) = append(*(apd.to), (*(apd.from))[0].ptId)
		*(apd.from) = (*(apd.from))[1:]
		*(apd.needNum)--
		return true
	}
	return false
}

func generalSceneExtract(bbd *dbBalanceBasicData, toLowPts, toHighPts *mvPtInfos) {
	selFir, selSec := true, true
	highNum := bbd.highLvlNum
	retainNum := bbd.max
	// use slices to control the traversal sequence of maps
	for _, dn := range *(bbd.aliveDns) {
		if highNum <= 0 {
			retainNum = bbd.min
		}

		low := bbd.dnPtIds[dn].lowIds
		high := bbd.dnPtIds[dn].highIds
		ptTotalNum := len(low) + len(high)
		if ptTotalNum >= retainNum {
			mvNum := ptTotalNum - retainNum
			redTH := ruleExtrData{selLow: &selFir,
				fromLow: &low, fromHigh: &high, toLow: toLowPts, toHigh: toHighPts,
				mvNum: &mvNum, ptTH: bbd.ptTHNum, dnId: dn}
			redSupp := ruleExtrData{selLow: &selSec,
				fromLow: &low, fromHigh: &high, toLow: toLowPts, toHigh: toHighPts,
				mvNum: &mvNum, ptTH: 0, dnId: dn}
			for mvNum > 0 {
				if ruleExtract(&redTH) {
					continue
				}

				if ruleExtract(&redSupp) {
					continue
				}
			}
			if highNum > 0 {
				highNum--
			}
		}

		// for scenarios where data skew exists
		mvNum := math.MaxInt32
		epdLow := extractPtData{from: &low, to: toLowPts, mvNum: &mvNum, ptTH: bbd.ptTHNum, dnId: dn}
		epdHigh := extractPtData{from: &high, to: toHighPts, mvNum: &mvNum, ptTH: bbd.ptTHNum, dnId: dn}
		for {
			if extractPtId(&epdLow) || extractPtId(&epdHigh) {
				continue
			}
			break
		}
		bbd.dnPtIds[dn] = DbPtIds{lowIds: low, highIds: high}
	}
}

// If low and high are all greater than the thresholds, extract ptid from low and high alternately.
// Otherwise, low first, high second. If low is successfully extracted, high is not executed.
func ruleExtract(re *ruleExtrData) bool {
	if *(re.mvNum) <= 0 {
		return false
	}

	currSelLow := true
	if len(*(re.fromLow)) > re.ptTH && len(*(re.fromHigh)) > re.ptTH {
		currSelLow = *(re.selLow)
		*(re.selLow) = !currSelLow
	}

	if currSelLow && extractPtId(&extractPtData{
		from: re.fromLow, to: re.toLow, mvNum: re.mvNum, ptTH: re.ptTH, dnId: re.dnId}) {
		return true
	}

	return extractPtId(&extractPtData{
		from: re.fromHigh, to: re.toHigh, mvNum: re.mvNum, ptTH: re.ptTH, dnId: re.dnId})
}

func extractPtId(epd *extractPtData) bool {
	if *(epd.mvNum) <= 0 {
		return false
	}

	if len(*(epd.from)) > epd.ptTH {
		mpi := mvPtInfo{ptId: (*(epd.from))[len(*(epd.from))-1], dnId: epd.dnId}
		*(epd.to) = append(*(epd.to), mpi)
		*(epd.from) = (*(epd.from))[:len(*(epd.from))-1]
		*(epd.mvNum)--
		return true
	}
	return false
}

func (s *Store) genDBBalanceBasicData(bbd *dbBalanceBasicData) error {
	dnPts := make(map[uint64]meta.DBPtInfos)
	for _, ptInfo := range s.data.PtView[bbd.dbName] {
		dnId := ptInfo.Owner.NodeID
		dnPts[dnId] = append(dnPts[dnId], ptInfo)
	}

	bbd.totalNumOfPt = len(s.data.PtView[bbd.dbName])
	totalPts := func(pts map[uint64]meta.DBPtInfos) int {
		var num int
		for _, dnId := range *(bbd.aliveDns) {
			num += len(pts[dnId])
		}
		return num
	}(dnPts)

	if totalPts != bbd.totalNumOfPt {
		return fmt.Errorf("alive node load all pts is not complete, %d != %d", totalPts, bbd.totalNumOfPt)
	}

	bbd.aliveDnNum = len(*bbd.aliveDns)
	bbd.halfPtId = bbd.totalNumOfPt / 2
	bbd.highLvlNum = bbd.totalNumOfPt % bbd.aliveDnNum
	avgPtNum := bbd.totalNumOfPt / bbd.aliveDnNum
	bbd.min = avgPtNum
	if bbd.highLvlNum > 0 {
		avgPtNum += 1
	}
	bbd.max = avgPtNum
	bbd.ptTHNum = avgPtNum/2 + avgPtNum%2
	bbd.minOfLH = bbd.halfPtId / bbd.aliveDnNum

	// obtains all PTs distribution of the current active node.
	bbd.dnPtIds = func(pts map[uint64]meta.DBPtInfos) map[uint64]DbPtIds {
		dnPtIds := make(map[uint64]DbPtIds)
		for _, dnId := range *(bbd.aliveDns) {
			if _, ok := pts[dnId]; !ok {
				dnPtIds[dnId] = DbPtIds{}
				continue
			}

			for _, pt := range pts[dnId] {
				low := dnPtIds[dnId].lowIds
				high := dnPtIds[dnId].highIds
				if pt.PtId < uint32(bbd.halfPtId) {
					low = append(low, pt.PtId)
				} else {
					high = append(high, pt.PtId)
				}
				dnPtIds[dnId] = DbPtIds{lowIds: low, highIds: high}
			}
		}
		return dnPtIds
	}(dnPts)

	return nil
}

func (s *Store) selectDbPtsToMove() []*MoveEvent {
	var moveEvents []*MoveEvent
	s.mu.RLock()
	defer s.mu.RUnlock()

	if config.GetHaPolicy() != config.SharedStorage || !s.data.BalancerEnabled {
		return nil
	}

	var aliveNodes []uint64
	for _, dataNode := range s.data.DataNodes {
		if dataNode.SegregateStatus == meta.Normal && dataNode.Status == serf.StatusAlive && dataNode.AliveConnID == dataNode.ConnID {
			aliveNodes = append(aliveNodes, dataNode.ID)
		}
	}
	if len(aliveNodes) == 0 {
		return nil
	}

	for db := range s.data.PtView {
		if s.data.CheckCanMoveDb(db) != nil {
			continue
		}
		nodePtsMap := make(map[uint64]meta.DBPtInfos)
		for _, ptInfo := range s.data.PtView[db] {
			nodePtsMap[ptInfo.Owner.NodeID] = append(nodePtsMap[ptInfo.Owner.NodeID], ptInfo)
		}

		dbInfo := s.data.GetDBBriefInfo(db)
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
			moveEvents = s.balanceByPtNum(db, from, to, nodePtsMap, moveEvents, dbInfo)
		} else {
			moveEvents = s.balanceByOldAndNew(db, aliveNodes, nodePtsMap, moveEvents, dbInfo)
		}
	}
	return moveEvents
}

func (s *Store) balanceByPtNum(db string, from, to uint64, nodePtsMap map[uint64]meta.DBPtInfos, moveEvents []*MoveEvent, dbBriefInfo *meta.DatabaseBriefInfo) []*MoveEvent {
	dn := s.data.DataNode(to)
	if dn == nil {
		return moveEvents
	}
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
			moveEvents = append(moveEvents, NewMoveEvent(&meta.DbPtInfo{Db: db, Pti: &pt, Shards: shardDurations, DBBriefInfo: dbBriefInfo},
				from, to, dn.AliveConnID, false))
			return moveEvents
		}
	}
	// expect to migrate old pt but all is new, or expect to migrate new pt but all is old
	if finalPt != nil {
		shardDurations := s.data.GetShardDurationsByDbPt(db, finalPt.PtId)
		moveEvents = append(moveEvents, NewMoveEvent(&meta.DbPtInfo{Db: db, Pti: finalPt, Shards: shardDurations, DBBriefInfo: dbBriefInfo},
			from, to, dn.AliveConnID, false))
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
func (s *Store) balanceByOldAndNew(db string, aliveNodes []uint64, nodePtsMap map[uint64]meta.DBPtInfos, moveEvents []*MoveEvent, dbBriefInfo *meta.DatabaseBriefInfo) []*MoveEvent {
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
			srcDn := s.data.DataNode(from)
			dstDn := s.data.DataNode(to)
			if srcDn == nil || dstDn == nil {
				return moveEvents
			}
			shardDurations := s.data.GetShardDurationsByDbPt(db, fromPt.PtId)
			moveEvents = append(moveEvents, NewMoveEvent(
				&meta.DbPtInfo{Db: db, Pti: &fromPt, Shards: shardDurations, DBBriefInfo: dbBriefInfo}, from, to, dstDn.AliveConnID, false))
			shardDurations = s.data.GetShardDurationsByDbPt(db, toPt.PtId)
			moveEvents = append(moveEvents, NewMoveEvent(
				&meta.DbPtInfo{Db: db, Pti: &toPt, Shards: shardDurations, DBBriefInfo: dbBriefInfo}, to, from, srcDn.AliveConnID, false))
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

// 1.pt online; 2.pt owner node alive; 3.pt owner node is not overLimit after Elect
func (s *Store) CanElectAsNewRgMaster(newMasterPt uint32, db string, masterPtNumMap *map[uint64]int, limit int) (uint64, bool) {
	pts := s.data.PtView[db]
	for _, pt := range pts {
		if pt.PtId == newMasterPt {
			if pt.Status == meta.Online && s.data.DataNode(pt.Owner.NodeID).Status == serf.StatusAlive &&
				(*masterPtNumMap)[pt.Owner.NodeID]+1 <= limit {
				return pt.Owner.NodeID, true
			} else {
				return 0, false
			}
		}
	}
	return 0, false
}

func (s *Store) ElectNewRgMaster(rg *meta.ReplicaGroup, limit int, db string, masterPtNumMap *map[uint64]int) (uint64, uint32, bool) {
	// 1.no slavePt to elect
	if len(rg.Peers) == 0 {
		return 0, 0, false
	}

	// 2.elect a satisficing slave pt as newRgMaster
	var newMasterPt uint32
	for _, peer := range rg.Peers {
		newMasterPt = peer.ID
		dstNode, ok := s.CanElectAsNewRgMaster(newMasterPt, db, masterPtNumMap, limit)
		if ok {
			return dstNode, newMasterPt, true
		}
	}
	return 0, 0, false
}

func (s *Store) GetMasterPtLimitPerNode() int {
	rgn := s.data.DBRGN()
	if len(s.data.DataNodes) == 0 {
		return 0
	}
	if rgn%len(s.data.DataNodes) > 0 {
		return rgn/len(s.data.DataNodes) + 1
	}
	return rgn / len(s.data.DataNodes)
}

func (s *Store) selectUpdateRGEvents() ([]string, []uint32, []uint32, [][]meta.Peer) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if config.GetHaPolicy() != config.Replication || !s.data.BalancerEnabled {
		return nil, nil, nil, nil
	}
	limit := s.GetMasterPtLimitPerNode()
	masterPtNumMap := make(map[uint64]int) // <nodeId, materPtN>

	rgss := make([][]*meta.ReplicaGroup, 0) // overLimit nodes' masterPtRgs
	dbss := make([][]string, 0)
	nodes := make([]uint64, 0)

	eventPts := make([]uint32, 0)
	eventPeers := make([][]meta.Peer, 0)
	eventDbs := make([]string, 0)
	eventRgs := make([]uint32, 0)

	// 1.cal masterPtN each alive node; get masterPtRgs each alive overLimit node
	for _, node := range s.data.DataNodes {
		if node.Status != serf.StatusAlive {
			continue
		}
		rgs, dbs := s.data.GetRgsOfNodeMasterPts(node)
		masterPtNumMap[node.ID] = len(rgs)
		if len(rgs) > limit {
			rgss = append(rgss, rgs)
			dbss = append(dbss, dbs)
			nodes = append(nodes, node.ID)
		}
	}

	var srcNode uint64
	// 2.elect newRgMaster of each rg of each overLimit node until it's not overLimit
	for i, rgs := range rgss {
		srcNode = nodes[i]
		for j, rg := range rgs {
			dstNode, newMasterPt, ok := s.ElectNewRgMaster(rg, limit, dbss[i][j], &masterPtNumMap)
			if ok {
				masterPtNumMap[srcNode] = masterPtNumMap[srcNode] - 1
				masterPtNumMap[dstNode] = masterPtNumMap[dstNode] + 1
				eventPts = append(eventPts, newMasterPt)
				eventPeers = append(eventPeers, rg.GenerateNewPeer(newMasterPt))
				eventDbs = append(eventDbs, dbss[i][j])
				eventRgs = append(eventRgs, rg.ID)
				if masterPtNumMap[srcNode] <= limit {
					break
				}
			}
		}
	}
	return eventDbs, eventRgs, eventPts, eventPeers
}

type rpToBalance struct {
	rp   string
	msts map[string]mstToBalance
}

type mstToBalance struct {
	mstName     string
	prevIdxes   []int
	numOfShards int32
}

type DBShardingPlans map[string]RPShardingPlans

type RPShardingPlans map[string]ShardingPlans

type ShardingPlans map[string][]int

// RP balance for PTs
func (s *Store) balanceRPPTWithLoads() (DBShardingPlans, error) {
	shardingPlans := make(DBShardingPlans)
	toPlan, ptNum := s.getRPPTToBalance()
	for dbName, rps := range toPlan {
		rpShardingPlans := make(RPShardingPlans)
		for _, rp := range rps {
			rpPtWriteStatus, err := s.GetPtWriteStatus(dbName, rp.rp)
			if err != nil {
				return nil, err
			}
			plan, err := s.balanceRPPT(dbName, rp, rpPtWriteStatus, ptNum)
			if err != nil {
				return nil, err
			}
			if len(plan) == 0 {
				continue
			}
			rpShardingPlans[rp.rp] = plan
		}
		shardingPlans[dbName] = rpShardingPlans
	}
	return shardingPlans, nil
}

func (s *Store) getRPPTToBalance() (map[string][]rpToBalance, uint32) {
	toPlan := make(map[string][]rpToBalance)
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	ptNum := s.cacheData.ClusterPtNum
	s.cacheData.WalkDatabases(func(dbInfo *meta.DatabaseInfo) {
		var rpToPlan []rpToBalance
		dbInfo.WalkRetentionPolicy(func(rpInfo *meta.RetentionPolicyInfo) {
			rp := rpToBalance{
				rp:   rpInfo.Name,
				msts: make(map[string]mstToBalance, len(rpInfo.Measurements)),
			}
			prevPtId := rpInfo.MaxShardGroupID()
			for _, mstInfo := range rpInfo.Measurements {
				prevShardIdxes := mstInfo.ShardIdexes[prevPtId]
				mst := mstToBalance{
					mstName:     mstInfo.Name,
					prevIdxes:   prevShardIdxes,
					numOfShards: mstInfo.InitNumOfShards,
				}
				rp.msts[mst.mstName] = mst
			}
			rpToPlan = append(rpToPlan, rp)
		})
		toPlan[dbInfo.Name] = rpToPlan
	})
	return toPlan, ptNum
}

func combineRPPTWriteStatus(dst MstPtStatus, status meta.PTMstWriteStatus) {
	for ptId, mstWriteStatus := range status {
		for mst, writeStatus := range mstWriteStatus {
			shards := dst[mst]
			shards = append(shards, &mstSharding{
				ptId,
				writeStatus,
			})
			dst[mst] = shards
		}
	}
}

func (s *Store) GetPtWriteStatus(db, rp string) (MstPtStatus, error) {
	s.cacheMu.RLock()
	ptInfos, ok := s.cacheData.PtView[db]
	if !ok {
		s.cacheMu.RUnlock()
		return nil, fmt.Errorf("database %s not found", db)
	}
	errChan := make(chan error, 1)
	nodes := make(map[uint64]*meta.DataNode)
	for i := range ptInfos {
		node := s.cacheData.DataNode(ptInfos[i].Owner.NodeID)
		if node == nil {
			s.Logger.Warn("[GetStorageStatus] DataNode not found", zap.Uint64("nodeId", ptInfos[i].Owner.NodeID))
			continue
		}
		nodes[node.ID] = node
	}
	var wg sync.WaitGroup
	statusChan := make(chan meta.PTMstWriteStatus, len(nodes))
	for _, node := range nodes {
		wg.Add(1)
		go func(node *meta.DataNode, db, rp string) {
			defer wg.Done()
			status, err := s.NetStore.GetRPPTWriteStatus(node, db, rp)
			if err != nil {
				s.Logger.Error("[GetStorageStatus] netStore GetStorageStatus failed", zap.Error(err), zap.Uint64("nodeId", node.ID))
				select {
				case errChan <- err:
				default:
				}
				return
			}
			statusChan <- status
		}(node, db, rp)
	}
	s.cacheMu.RUnlock()

	go func() {
		wg.Wait()
		close(statusChan)
		close(errChan)
	}()

	mstPtStatus := make(MstPtStatus)
	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				continue
			}
			if err != nil {
				return nil, err
			}
		case status, ok := <-statusChan:
			if !ok {
				return mstPtStatus, nil
			}
			combineRPPTWriteStatus(mstPtStatus, status)
		}
	}
}

type MstPtStatus map[string][]*mstSharding

type mstSharding struct {
	ptId  uint32
	write int64
}

type mstForBalance struct {
	mst         string
	prevIdxes   map[int]int
	shards      []*mstSharding
	totalFlow   int64
	numOfShards int
}

type ptAllocation struct {
	ptId  uint32
	load  int64
	index int
}

type ptAllocationHeap []*ptAllocation

func (h ptAllocationHeap) Len() int { return len(h) }
func (h ptAllocationHeap) Less(i, j int) bool {
	if h[i].load == h[j].load {
		return h[i].ptId < h[j].ptId
	}
	return h[i].load < h[j].load
}
func (h ptAllocationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *ptAllocationHeap) Push(x interface{}) {
	node, ok := x.(*ptAllocation)
	if !ok {
		// node must be *ptAllocation
		panic("Bug: ptAllocationHeap.Push: cannot push non-*ptAllocation type")
	}

	node.index = len(*h)
	*h = append(*h, node)
}

func (h *ptAllocationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	node := old[n-1]
	node.index = -1
	*h = old[0 : n-1]
	return node
}

// balanceRPPT balances the rp through all pt, as input, write traffic from mst to each pt during last shard group duration
// is provided. After balance, which mst shard is written to which pt will be determined.
func (s *Store) balanceRPPT(db string, rp rpToBalance, writeStatus MstPtStatus, ptNum uint32) (ShardingPlans, error) {
	balancer := NewRPPTBalancer(db, rp, writeStatus, int(ptNum))
	return balancer.Balance()
}

type RPPTBalancer struct {
	PTs         []*ptAllocation
	PTHeap      ptAllocationHeap
	MSTs        []*mstForBalance
	Allocations map[string][]int
	MstPtMap    map[string]map[uint32]interface{}
	PTNum       int
}

func (b *RPPTBalancer) sortMSTsByTotalFlow() {
	sort.Slice(b.MSTs, func(i, j int) bool {
		if b.MSTs[i].totalFlow == b.MSTs[j].totalFlow {
			return b.MSTs[i].mst < b.MSTs[j].mst
		}
		return b.MSTs[i].totalFlow > b.MSTs[j].totalFlow
	})
}

func (b *RPPTBalancer) sortMstShardsByFlow(m *mstForBalance) []*mstSharding {
	shards := make([]*mstSharding, len(m.shards))
	copy(shards, m.shards)
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].write == shards[j].write {
			return shards[i].ptId < shards[j].ptId
		}
		return shards[i].write > shards[j].write
	})
	return shards
}

// getMinLoadAvailablePt Obtain the smallest available load pt (excluding nodes that have already been assigned this table partition)
func (b *RPPTBalancer) getMinLoadAvailablePt(mst string) *ptAllocation {
	tempPT := make([]*ptAllocation, 0)

	var availablePT *ptAllocation
	for b.PTHeap.Len() > 0 {
		pt, ok := heap.Pop(&b.PTHeap).(*ptAllocation)
		if !ok {
			// pt must be *ptAllocation type
			return nil
		}
		tempPT = append(tempPT, pt)

		// Check whether the node has been assigned the partition of this table.
		if usedPT, exist := b.MstPtMap[mst]; exist {
			if _, exist = usedPT[pt.ptId]; exist {
				continue
			}
			availablePT = pt
			break
		}
	}

	for _, pt := range tempPT {
		heap.Push(&b.PTHeap, pt)
	}
	return availablePT
}

func (b *RPPTBalancer) Balance() (ShardingPlans, error) {
	idxes := make(map[string][]int, len(b.MSTs))

	// 1.Sort mst by total flow in descending order.
	b.sortMSTsByTotalFlow()

	// 2. Traverse each table for allocation
	for _, mst := range b.MSTs {
		alloc := make([]int, mst.numOfShards)
		mstName := mst.mst
		b.MstPtMap[mstName] = make(map[uint32]interface{})

		// Sort the table partitions in descending order of traffic.
		sortedShards := b.sortMstShardsByFlow(mst)

		// allocate shards for the mst
		for _, shard := range sortedShards {
			idx, ok := mst.prevIdxes[int(shard.ptId)]
			if !ok {
				continue
			}
			tarPt := b.getMinLoadAvailablePt(mstName)
			if tarPt == nil {
				return nil, errors.New("RPPT Balance failed: no available pts for measurement shards")
			}

			tarPt.load += shard.write
			b.MstPtMap[mstName][tarPt.ptId] = struct{}{}
			alloc[idx] = int(tarPt.ptId)
		}
		idxes[mstName] = alloc
	}
	b.Allocations = idxes
	return b.Allocations, nil
}

func NewRPPTBalancer(db string, rp rpToBalance, status MstPtStatus, ptNum int) *RPPTBalancer {
	nodes := make([]*ptAllocation, 0, ptNum)
	nodeHeap := make(ptAllocationHeap, 0, ptNum)
	for i := 0; i < ptNum; i++ {
		node := &ptAllocation{ptId: uint32(i)}
		nodes = append(nodes, node)
		nodeHeap = append(nodeHeap, node)
	}
	heap.Init(&nodeHeap)

	mstMap := make(map[string]*mstForBalance)
	for mst, writeStatus := range status {
		mstStatus, ok := mstMap[mst]
		if !ok {
			mstTB := rp.msts[mst]
			// if the numOfShards change, just random assign at first time
			numOfShards := int(mstTB.numOfShards)
			if numOfShards == 0 {
				numOfShards = ptNum
			}
			// for case numOfShards is 0, which means map to all pts, then the prevIdxes should be nil
			if numOfShards != len(mstTB.prevIdxes) && len(mstTB.prevIdxes) != 0 {
				continue
			}
			mstStatus = &mstForBalance{
				mst:         mst,
				numOfShards: numOfShards,
				prevIdxes:   idxSliceToMap(mstTB.prevIdxes, numOfShards),
			}
			mstMap[mst] = mstStatus
		}
		mstStatus.shards = writeStatus
		for _, sh := range writeStatus {
			mstStatus.totalFlow += sh.write
		}
	}

	// for shards no write record, fill with zero values
	msts := make([]*mstForBalance, 0, len(mstMap))
	shs := make(map[uint32]interface{}, ptNum)
	for _, mst := range mstMap {
		for _, sh := range mst.shards {
			shs[sh.ptId] = struct{}{}
		}
		for _, idx := range mst.prevIdxes {
			ptId := uint32(idx)
			if _, ok := shs[ptId]; !ok {
				mst.shards = append(mst.shards, &mstSharding{
					ptId: ptId,
				})
			}
		}
		msts = append(msts, mst)
		clear(shs)
	}

	return &RPPTBalancer{
		PTs:      nodes,
		PTHeap:   nodeHeap,
		MSTs:     msts,
		MstPtMap: make(map[string]map[uint32]interface{}),
		PTNum:    ptNum,
	}
}

func idxSliceToMap(slice []int, numOfShards int) map[int]int {
	if len(slice) == 0 {
		// for  case numOfShards in mstInfo is 0
		res := make(map[int]int, numOfShards)
		for i := 0; i < numOfShards; i++ {
			res[i] = i
		}
		return res
	}
	res := make(map[int]int, len(slice))
	for i, v := range slice {
		res[v] = i
	}
	return res
}
