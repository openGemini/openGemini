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

package statistics

const storeNodeMemBucketStatisticsName = "node_mutable_mem"

var nodeMem = &NodeMem{}

func init() {
	NewCollector().Register(nodeMem)
}

func NewNodeMemStat() *NodeMem {
	nodeMem.enabled = true
	return nodeMem
}

type NodeMem struct {
	BaseCollector
	TotalResource *ItemInt64
	FreeResource  *ItemInt64
	BlockExecutor *ItemInt64

	GetTotalResource func() int64
	GetFreeResource  func() int64
	GetBlockExecutor func() int64
}

func (n *NodeMem) BeforeCollect() {
	if n.GetTotalResource != nil {
		n.TotalResource.Store(n.GetTotalResource())
	}
	if n.GetFreeResource != nil {
		n.FreeResource.Store(n.GetFreeResource())
	}
	if n.GetBlockExecutor != nil {
		n.BlockExecutor.Store(n.GetBlockExecutor())
	}
}

func (*NodeMem) MeasurementName() string {
	return storeNodeMemBucketStatisticsName
}
