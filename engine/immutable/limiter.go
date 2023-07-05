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

package immutable

import (
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/fileops"
	"golang.org/x/time/rate"
)

var (
	compWriteLimiter           = fileops.NewLimiter(48*1024*1024, 64*1024*1024)
	snapshotWriteLimiter       = fileops.NewLimiter(48*1024*1024, 64*1024*1024)
	snapshotNoLimit      int32 = 0
)

func SnapshotLimit() bool {
	return atomic.LoadInt32(&snapshotNoLimit) == 0
}

func SetCompactLimit(bytesPerSec int64, burstLimit int64) {
	if burstLimit < bytesPerSec {
		burstLimit = bytesPerSec
	}
	compWriteLimiter.SetLimit(rate.Limit(bytesPerSec))
	compWriteLimiter.SetBurst(int(burstLimit))
}

func SetSnapshotLimit(bytesPerSec int64, burstLimit int64) {
	if bytesPerSec == 0 {
		atomic.StoreInt32(&snapshotNoLimit, 1)
		return
	}
	if burstLimit < bytesPerSec {
		burstLimit = bytesPerSec
	}
	snapshotWriteLimiter.SetLimit(rate.Limit(bytesPerSec))
	snapshotWriteLimiter.SetBurst(int(burstLimit))
}
