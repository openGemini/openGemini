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
	"sync"
)

type MutableStats struct {
	MutableSize int64
	MutableRows int64
}

// Statistics keeps statistics related to the Mutable
type MutableStatistics struct {
	Mu    sync.RWMutex
	Stats map[string]*MutableStats
}

const (
	StatMutablePath = "path"
	StatMutableSize = "mutableSize"
)

var MutableStat = NewMutableStatistics()
var MutableTagMap map[string]string
var MutableStatisticsName = "mutable"

func NewMutableStatistics() *MutableStatistics {
	return &MutableStatistics{
		Stats: make(map[string]*MutableStats),
	}
}

func InitMutableStatistics(tags map[string]string) {
	MutableStat = NewMutableStatistics()
	MutableTagMap = tags
}

func (mu *MutableStatistics) AddMutableSize(path string, size int64) {
	mu.Mu.Lock()
	defer mu.Mu.Unlock()

	if s, ok := MutableStat.Stats[path]; ok {
		s.MutableSize += size
		return
	}

	MutableStat.Stats[path] = &MutableStats{
		MutableSize: size,
		MutableRows: 0,
	}
}

func CollectMutableStatistics(buffer []byte) ([]byte, error) {
	MutableStat.Mu.RLock()
	defer MutableStat.Mu.RUnlock()

	for path, stats := range MutableStat.Stats {
		tagMap := make(map[string]string)
		AllocTagMap(tagMap, MutableTagMap)
		tagMap[StatMutablePath] = path
		valueMap := map[string]interface{}{
			StatMutableSize: stats.MutableSize,
		}

		buffer = AddPointToBuffer(MutableStatisticsName, tagMap, valueMap, buffer)
	}

	return buffer, nil
}
