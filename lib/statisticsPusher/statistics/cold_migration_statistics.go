// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package statistics

type ToObsCold struct {
	BaseCollector

	ShardToColdSuccessSum *ItemInt64
	ShardToColdFailSum    *ItemInt64
	ShardToColdSum        *ItemInt64

	IndexToColdSuccessSum *ItemInt64
	IndexToColdFailSum    *ItemInt64
	IndexToColdSum        *ItemInt64
}

var toObsCold = &ToObsCold{}

func init() {
	NewCollector().Register(toObsCold)
}

func GetToObsCold() *ToObsCold {
	toObsCold.enabled = true
	return toObsCold
}

func (c *ToObsCold) AddShardToColdSuccessSum() {
	c.ShardToColdSuccessSum.Incr()
}

func (c *ToObsCold) AddShardToColdFailSum() {
	c.ShardToColdFailSum.Incr()
}

func (c *ToObsCold) AddShardToColdSum() {
	c.ShardToColdSum.Incr()
}

func (c *ToObsCold) AddIndexToColdSuccessSum() {
	c.IndexToColdSuccessSum.Incr()
}

func (c *ToObsCold) AddIndexToColdFailSum() {
	c.IndexToColdFailSum.Incr()
}

func (c *ToObsCold) AddIndexToColdSum() {
	c.IndexToColdSum.Incr()
}
