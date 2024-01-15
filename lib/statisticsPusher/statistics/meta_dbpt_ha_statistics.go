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
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

const (
	metaDbptTasks = "meta_dbpt_tasks"
)

var statBufPool = bufferpool.NewByteBufferPool(0, cpu.GetCpuNum(), bufferpool.MaxLocalCacheLen)
var MetaTaskInstance *MetaTaskDuration

func init() {
	MetaTaskInstance = NewMetaTaskDuration(false)
}

type MetaTaskDuration struct {
	enable bool

	mu        sync.RWMutex
	dbptTasks map[uint64]*DBPTStatistics // {"opId": DBPTStatistics}

	pointMu    sync.RWMutex
	loadPoints []byte

	log *logger.Logger
}

func NewMetaTaskDuration(enable bool) *MetaTaskDuration {
	return &MetaTaskDuration{
		enable: enable,

		dbptTasks:  make(map[uint64]*DBPTStatistics),
		loadPoints: make([]byte, 0, loadPointsCap),

		log: logger.NewLogger(errno.ModuleMeta),
	}
}

func MetaDBPTTaskInit(opId uint64, db string, ptId uint32) {
	if !MetaTaskInstance.enable {
		return
	}
	MetaTaskInstance.mu.Lock()
	defer MetaTaskInstance.mu.Unlock()

	if task, ok := MetaTaskInstance.dbptTasks[opId]; ok {
		task.tries++
		MetaTaskInstance.log.Warn("db pt load repeated",
			zap.String("opId", task.opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.Int("tries", task.tries),
		)
		return
	}

	MetaTaskInstance.dbptTasks[opId] = &DBPTStatistics{
		tries:  1,
		opId:   strconv.Itoa(int(opId)),
		db:     db,
		pt:     int(ptId),
		status: DBPTLoading,
	}
}

func MetaDBPTStepDuration(event string, opId uint64, step string, src, dst uint64, d int64, status LoadStatus, errMsg string) {
	if !MetaTaskInstance.enable {
		return
	}

	MetaTaskInstance.mu.Lock()
	defer MetaTaskInstance.mu.Unlock()

	if _, ok := MetaTaskInstance.dbptTasks[opId]; !ok {
		MetaTaskInstance.log.Warn("meta task db pt step not found",
			zap.Uint64("opId", opId),
			zap.String("step", step),
		)
		return
	}

	MetaTaskInstance.dbptTasks[opId].step = step
	MetaTaskInstance.dbptTasks[opId].cost = d
	MetaTaskInstance.dbptTasks[opId].totalCost += d
	MetaTaskInstance.dbptTasks[opId].status = status
	MetaTaskInstance.dbptTasks[opId].err = errMsg
	MetaTaskInstance.dbptTasks[opId].time = time.Now().UTC()

	task := MetaTaskInstance.dbptTasks[opId]
	tagMap := map[string]string{
		"OpId":      task.opId,
		"Step":      task.step,
		"EventType": event,
	}
	valueMap := map[string]interface{}{
		"Db":        task.db,
		"Pt":        int64(task.pt),
		"Src":       int64(src),
		"Dst":       int64(dst),
		"Cost":      task.cost,
		"TotalCost": task.totalCost,
		"Status":    int64(task.status),
		"Err":       task.err,
	}
	buff := statBufPool.Get()
	point := AddTimeToBuffer(metaDbptTasks, tagMap, valueMap, task.time, buff)
	MetaTaskInstance.sendPointToChan(point)
	statBufPool.Put(point)

	if status != DBPTLoading {
		task.tries--
		MetaTaskInstance.log.Info("db pt load step finish",
			zap.Uint64("opId", opId),
			zap.String("db", task.db),
			zap.Int("pt", task.pt),
			zap.Int("tries", task.tries),
		)
		if task.tries == 0 {
			delete(MetaTaskInstance.dbptTasks, opId)
		}
	}
}

func (s *MetaTaskDuration) sendPointToChan(point []byte) {
	s.pointMu.Lock()
	s.loadPoints = append(s.loadPoints, point...)
	s.pointMu.Unlock()
}

func (s *MetaTaskDuration) Collect(buffer []byte) ([]byte, error) {
	s.pointMu.Lock()
	buffer = append(buffer, s.loadPoints...)
	s.loadPoints = s.loadPoints[:0]
	s.pointMu.Unlock()
	return buffer, nil
}
