/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package resourceallocator

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/bucket"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

var log = logger.NewLogger(errno.ModuleStorageEngine)

type NodeMemBucket struct {
	once      sync.Once
	memBucket bucket.ResourceBucket
	TimeOut   time.Duration
}

var NodeMutableLimit NodeMemBucket

func (nodeLimit *NodeMemBucket) InitNodeMemBucket(timeOut time.Duration, memThreshold int64) {
	nodeLimit.once.Do(func() {
		log.Info("New node mem limit bucket", zap.Int64("node mutable size limit", memThreshold),
			zap.Duration("max write hang duration", timeOut))
		nodeLimit.memBucket = bucket.NewInt64Bucket(timeOut, memThreshold, false)
		nodeLimit.TimeOut = timeOut
		memStat := statistics.NewNodeMemStat()
		memStat.GetTotalResource = nodeLimit.memBucket.GetTotalResource
		memStat.GetFreeResource = nodeLimit.memBucket.GetFreeResource
		memStat.GetBlockExecutor = nodeLimit.memBucket.GetBlockExecutor
	})
}

func (nodeLimit *NodeMemBucket) AllocResource(r int64, timer *time.Timer) error {
	return nodeLimit.memBucket.GetResDetected(r, timer)
}

func (nodeLimit *NodeMemBucket) FreeResource(r int64) {
	nodeLimit.memBucket.ReleaseResource(r)
}
