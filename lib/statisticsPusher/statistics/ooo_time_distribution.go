/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"math"
	"sync/atomic"
	"time"
)

const (
	OOOTimeDistributionMst = "ooo_time_distribution"
)

var oooTimeDistribution *OOOTimeDistribution

func init() {
	oooTimeDistribution = &OOOTimeDistribution{
		intervals: []int64{15, 30, 60, 120, 240, 480, 960, math.MaxInt64}, // Second
		keys:      []string{"less15", "less30", "less60", "less120", "less240", "less480", "less960", "more960"},
	}

	oooTimeDistribution.counts = make([]int64, len(oooTimeDistribution.intervals))
	oooTimeDistribution.swap = make([]int64, len(oooTimeDistribution.intervals))
}

func NewOOOTimeDistribution() *OOOTimeDistribution {
	return oooTimeDistribution
}

type OOOTimeDistribution struct {
	Metric

	counts    []int64
	swap      []int64
	intervals []int64
	keys      []string
}

func (d *OOOTimeDistribution) Add(interval int64, n int64) {
	interval /= int64(time.Second)

	for i, v := range d.intervals {
		if interval < v {
			atomic.AddInt64(&(d.counts[i]), n)
			return
		}
	}
}

func (d *OOOTimeDistribution) Collect(buffer []byte) ([]byte, error) {
	copy(d.swap, d.counts)

	valueMap := map[string]interface{}{}
	for i := range d.counts {
		atomic.AddInt64(&(d.counts[i]), -d.swap[i])
		valueMap[d.keys[i]] = d.swap[i]
	}

	buffer = AddPointToBuffer(OOOTimeDistributionMst, d.tags, valueMap, buffer)
	return buffer, nil
}
