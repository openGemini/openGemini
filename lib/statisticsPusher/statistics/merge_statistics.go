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
	"time"
)

func (s *MergeStatItem) StatOutOfOrderFile(size int64, num int) {
	s.OutOfOrderFileCount += int64(num)
	s.OutOfOrderFileSize += size
}

func (s *MergeStatItem) StatOrderFile(size int64, num int) {
	s.OrderFileCount += int64(num)
	s.OrderFileSize += size
}

func (s *MergeStatItem) StatMergedFile(size int64, num int) {
	s.MergedFileCount += int64(num)
	s.MergedFileSize += size
}

func NewMergeStatItem(mst string, shId uint64) *MergeStatItem {
	return &MergeStatItem{
		validateHandle: func(item *MergeStatItem) bool {
			return item.MergedFileCount > 0
		},
		begin:       time.Now(),
		Measurement: mst,
		ShardID:     strconv.FormatUint(shId, 10),
	}
}
