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

package executor

import (
	"sync/atomic"
)

var (
	EnableBinaryTreeMerge     int64 = 0
	EnablePrintLogicalPlan    int64 = 0
	EnableSlidingWindowPushUp int64 = 0
	EnableForceBroadcastQuery int64 = 0
	OnSlidingWindowPushUp     int64 = 0
	OnPrintLogicalPlan        int64 = 1
	OnForceBroadcastQuery     int64 = 1

	querySeriesLimit int = 0 // query schema upper bound
)

func SetEnableBinaryTreeMerge(enabled int64) {
	atomic.StoreInt64(&EnableBinaryTreeMerge, enabled)
}

func GetEnableBinaryTreeMerge() int64 {
	return atomic.LoadInt64(&EnableBinaryTreeMerge)
}

func SetEnablePrintLogicalPlan(enabled int64) {
	atomic.StoreInt64(&EnablePrintLogicalPlan, enabled)
}

func GetEnablePrintLogicalPlan() int64 {
	return atomic.LoadInt64(&EnablePrintLogicalPlan)
}

func SetEnableSlidingWindowPushUp(enabled int64) {
	atomic.StoreInt64(&EnableSlidingWindowPushUp, enabled)
}

func GetEnableSlidingWindowPushUp() int64 {
	return atomic.LoadInt64(&EnableSlidingWindowPushUp)
}

func SetEnableForceBroadcastQuery(enabled int64) {
	atomic.StoreInt64(&EnableForceBroadcastQuery, enabled)
}

func GetEnableForceBroadcastQuery() int64 {
	return atomic.LoadInt64(&EnableForceBroadcastQuery)
}

func SetQuerySchemaLimit(limit int) {
	querySeriesLimit = limit
}

func GetQuerySchemaLimit() int {
	return querySeriesLimit
}
