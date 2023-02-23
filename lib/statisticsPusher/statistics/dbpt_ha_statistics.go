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

package statistics

import (
	"strconv"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type LoadStatus int

const (
	DBPTLoading LoadStatus = iota
	DBPTLoaded
	DBPTLoadErr

	dbptTasks     = "dbpt_tasks"
	indexTasks    = "index_tasks"
	shardTasks    = "shard_tasks"
	loadPointsCap = 1024
)

var StoreTaskInstance *StoreTaskDuration

func init() {
	StoreTaskInstance = NewStoreTaskDuration(false)
}

type StoreTaskDuration struct {
	enable bool

	mu         sync.RWMutex
	dbptTasks  map[uint64]*DBPTStatistics  // {"opId": DBPTStatistics}
	indexTasks map[uint64]*IndexStatistics // {"IndexId": IndexStatistics}
	shardTasks map[uint64]*ShardStatistics // {"shardId": ShardStatistics}

	pointMu    sync.RWMutex
	loadPoints []byte

	log *logger.Logger
}

func NewStoreTaskDuration(enable bool) *StoreTaskDuration {
	return &StoreTaskDuration{
		enable: enable,

		dbptTasks:  make(map[uint64]*DBPTStatistics),
		indexTasks: make(map[uint64]*IndexStatistics),
		shardTasks: make(map[uint64]*ShardStatistics),
		loadPoints: make([]byte, 0, loadPointsCap),

		log: logger.NewLogger(errno.ModuleHA),
	}
}

type DBPTStatistics struct {
	tries int // repeat load times

	// tag
	opId string
	step string

	// field
	db        string
	pt        int
	cost      int64 // ns
	totalCost int64 // ns
	status    LoadStatus
	err       string

	time time.Time
}

func DBPTTaskInit(opId uint64, db string, ptId uint32) {
	if !StoreTaskInstance.enable {
		return
	}
	StoreTaskInstance.mu.Lock()
	defer StoreTaskInstance.mu.Unlock()
	if task, ok := StoreTaskInstance.dbptTasks[opId]; ok {
		task.tries++
		StoreTaskInstance.log.Warn("db pt load repeated",
			zap.String("opId", task.opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.Int("tries", task.tries),
		)
		return
	}
	StoreTaskInstance.dbptTasks[opId] = &DBPTStatistics{
		tries:  1,
		opId:   strconv.Itoa(int(opId)),
		db:     db,
		pt:     int(ptId),
		status: DBPTLoading,
	}
}

func DBPTStepDuration(opId uint64, step string, d int64, status LoadStatus, errMsg string) {
	if !StoreTaskInstance.enable {
		return
	}

	StoreTaskInstance.mu.Lock()
	defer StoreTaskInstance.mu.Unlock()

	if _, ok := StoreTaskInstance.dbptTasks[opId]; !ok {
		StoreTaskInstance.log.Warn("db pt step not found",
			zap.Uint64("opId", opId),
			zap.String("step", step),
		)
		return
	}
	StoreTaskInstance.dbptTasks[opId].step = step
	StoreTaskInstance.dbptTasks[opId].cost = d
	StoreTaskInstance.dbptTasks[opId].totalCost += d
	StoreTaskInstance.dbptTasks[opId].status = status
	StoreTaskInstance.dbptTasks[opId].err = errMsg
	StoreTaskInstance.dbptTasks[opId].time = time.Now()

	task := StoreTaskInstance.dbptTasks[opId]
	tagMap := map[string]string{
		"OpId": task.opId,
		"Step": task.step,
	}
	valueMap := map[string]interface{}{
		"Db":        task.db,
		"Pt":        int64(task.pt),
		"Cost":      task.cost,
		"TotalCost": task.totalCost,
		"Status":    int64(task.status),
		"Err":       task.err,
	}
	buff := bufferpool.Get()
	point := AddTimeToBuffer(dbptTasks, tagMap, valueMap, task.time, buff)
	StoreTaskInstance.sendPointToChan(point)

	if status != DBPTLoading {
		task.tries--
		StoreTaskInstance.log.Info("db pt load step finish",
			zap.Uint64("opId", opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.Int("tries", task.tries),
		)
		if task.tries == 0 {
			delete(StoreTaskInstance.dbptTasks, opId)
		}
	}
}

type IndexStatistics struct {
	tries int // repeat load times

	// tag
	indexId string // index id
	opId    string
	step    string

	// field
	db        string
	pt        int
	rp        string
	cost      int64 // ns
	totalCost int64 // ns

	time time.Time
}

func IndexTaskInit(indexId uint64, opId uint64, db string, ptId uint32, rp string) {
	if !StoreTaskInstance.enable {
		return
	}
	if opId == 0 {
		// ignore non-HA case
		return
	}
	StoreTaskInstance.mu.Lock()
	defer StoreTaskInstance.mu.Unlock()

	if task, ok := StoreTaskInstance.indexTasks[indexId]; ok {
		task.tries++
		StoreTaskInstance.log.Warn("index task load repeated",
			zap.Uint64("indexId", indexId),
			zap.Uint64("opId", opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.String("rp", task.rp),
			zap.Int("tries", task.tries),
		)
		return
	}
	StoreTaskInstance.indexTasks[indexId] = &IndexStatistics{
		tries:   1,
		indexId: strconv.Itoa(int(indexId)),
		opId:    strconv.Itoa(int(opId)),

		db: db,
		pt: int(ptId),
		rp: rp,
	}
}

func IndexStepDuration(indexId uint64, opId uint64, step string, cost int64, isOver bool) {
	if !StoreTaskInstance.enable {
		return
	}
	if opId == 0 {
		// ignore non-HA case
		return
	}

	StoreTaskInstance.mu.Lock()
	defer StoreTaskInstance.mu.Unlock()

	if _, ok := StoreTaskInstance.indexTasks[indexId]; !ok {
		StoreTaskInstance.log.Warn("index step not found",
			zap.Uint64("indexId", indexId),
			zap.Uint64("opId", opId),
			zap.String("step", step),
		)
		return
	}

	StoreTaskInstance.indexTasks[indexId].step = step
	StoreTaskInstance.indexTasks[indexId].cost = cost
	StoreTaskInstance.indexTasks[indexId].totalCost += cost
	StoreTaskInstance.indexTasks[indexId].time = time.Now()

	task := StoreTaskInstance.indexTasks[indexId]
	tagMap := map[string]string{
		"IndexId": task.indexId,
		"OpId":    task.opId,
		"Step":    task.step,
	}
	valueMap := newValueMap(task.db, task.pt, task.rp, task.cost, task.totalCost)
	buff := bufferpool.Get()
	point := AddTimeToBuffer(indexTasks, tagMap, valueMap, task.time, buff)
	StoreTaskInstance.sendPointToChan(point)

	if isOver {
		task.tries--
		StoreTaskInstance.log.Info("index task load finish",
			zap.Uint64("indexId", indexId),
			zap.Uint64("opId", opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.String("rp", task.rp),
			zap.Int("tries", task.tries),
		)
		if task.tries == 0 {
			delete(StoreTaskInstance.indexTasks, indexId)
		}
	}
}

type ShardStatistics struct {
	tries int // repeat load times

	// tag
	sid  string // shard id
	opId string
	step string

	// field
	db        string
	pt        int
	rp        string
	cost      int64 // ns
	totalCost int64 // ns

	time time.Time
}

func ShardTaskInit(opId uint64, db string, ptId uint32, rp string, sid uint64) {
	if !StoreTaskInstance.enable {
		return
	}
	if opId == 0 {
		// ignore write rows create shard
		return
	}
	StoreTaskInstance.mu.Lock()
	defer StoreTaskInstance.mu.Unlock()

	if task, ok := StoreTaskInstance.shardTasks[sid]; ok {
		task.tries++
		StoreTaskInstance.log.Warn("shard task load repeated",
			zap.String("sid", task.sid),
			zap.Uint64("opId", opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.String("rp", task.rp),
			zap.Int("tries", task.tries),
		)
		return
	}

	StoreTaskInstance.shardTasks[sid] = &ShardStatistics{
		tries: 1,
		sid:   strconv.Itoa(int(sid)),
		opId:  strconv.Itoa(int(opId)),

		db: db,
		pt: int(ptId),
		rp: rp,
	}
}

func newValueMap(db string, pt int, rp string, cost int64, totalCost int64) map[string]interface{} {
	return map[string]interface{}{
		"Db":        db,
		"Pt":        int64(pt),
		"Rp":        rp,
		"Cost":      cost,
		"TotalCost": totalCost,
	}
}

func ShardStepDuration(sid uint64, opId uint64, step string, cost int64, isOver bool) {
	if !StoreTaskInstance.enable {
		return
	}
	if opId == 0 {
		// ignore write rows create shard
		return
	}
	StoreTaskInstance.mu.Lock()
	defer StoreTaskInstance.mu.Unlock()

	if _, ok := StoreTaskInstance.shardTasks[sid]; !ok {
		StoreTaskInstance.log.Warn("shard step not found",
			zap.Uint64("sid", sid),
			zap.Uint64("opId", opId),
			zap.String("step", step),
		)
		return
	}

	StoreTaskInstance.shardTasks[sid].step = step
	StoreTaskInstance.shardTasks[sid].cost = cost
	StoreTaskInstance.shardTasks[sid].totalCost += cost
	StoreTaskInstance.shardTasks[sid].time = time.Now()

	task := StoreTaskInstance.shardTasks[sid]
	tagMap := map[string]string{
		"Sid":  task.sid,
		"OpId": task.opId,
		"Step": task.step,
	}
	valueMap := newValueMap(task.db, task.pt, task.rp, task.cost, task.totalCost)
	buff := bufferpool.Get()
	point := AddTimeToBuffer(shardTasks, tagMap, valueMap, task.time, buff)
	StoreTaskInstance.sendPointToChan(point)

	if isOver {
		task.tries--
		StoreTaskInstance.log.Info("shard task load finish",
			zap.String("sid", task.sid),
			zap.Uint64("opId", opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.String("rp", task.rp),
			zap.Int("tries", task.tries),
		)
		if task.tries == 0 {
			delete(StoreTaskInstance.shardTasks, sid)
		}
	}
}

func (s *StoreTaskDuration) sendPointToChan(point []byte) {
	MetaTaskInstance.pointMu.Lock()
	StoreTaskInstance.loadPoints = append(StoreTaskInstance.loadPoints, point...)
	MetaTaskInstance.pointMu.Unlock()
}

func (s *StoreTaskDuration) Collect(buffer []byte) ([]byte, error) {
	s.pointMu.Lock()
	points := s.loadPoints
	s.loadPoints = s.loadPoints[:0]
	s.pointMu.Unlock()
	if len(points) > 0 {
		buffer = append(buffer, points...)
		bufferpool.Put(points)
	}
	return buffer, nil
}
