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
	"fmt"
	"math"
	"sort"
	"strings"
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
		moveEvents = append(moveEvents, NewMoveEvent(&meta.DbPtInfo{Db: db, Pti: pt, Shards: shardDurations, DBBriefInfo: dbBriefInfo},
			task.srcDn, task.destDn, dn.AliveConnID, false))
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
