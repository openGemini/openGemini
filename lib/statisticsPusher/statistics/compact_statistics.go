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

var (
	cacheCompactionTags map[string]string
	compactionActions   = map[bool]string{false: "level", true: "full"}
	compactionLevels    = []string{"0", "1", "2", "3", "4", "5", "6", "7"}
)

type CompactStatItem struct {
	begin    time.Time
	duration time.Duration

	Level   uint16
	Full    bool
	mst     string
	shardID uint64

	// File information before compact
	OriginalFileCount int64
	OriginalFileSize  int64

	// File information after compact
	CompactedFileCount int64
	CompactedFileSize  int64
}

func NewCompactStatItem(mst string, shardID uint64) *CompactStatItem {
	return &CompactStatItem{
		begin:   time.Now(),
		mst:     mst,
		shardID: shardID,
	}
}

func (i *CompactStatItem) Duration() time.Duration {
	if i.duration == 0 {
		i.duration = time.Since(i.begin)
	}
	return i.duration
}

func (s *CompactStatistics) makeTags(level uint16, full bool) map[string]string {
	if cacheCompactionTags == nil {
		cacheCompactionTags = map[string]string{}
		AllocTagMap(cacheCompactionTags, s.tags)
	}
	if int(level) >= len(compactionLevels) {
		level = 0
	}

	cacheCompactionTags["level"] = compactionLevels[level]
	cacheCompactionTags["action"] = compactionActions[full]
	return cacheCompactionTags
}

func (s *CompactStatistics) PushCompaction(item *CompactStatItem) {
	if item.OriginalFileSize == 0 || item.OriginalFileCount == 0 {
		return
	}

	data := map[string]interface{}{
		"Duration":           item.Duration().Milliseconds(),
		"OriginalFileCount":  item.OriginalFileCount,
		"OriginalFileSize":   item.OriginalFileSize,
		"CompactedFileCount": item.CompactedFileCount,
		"CompactedFileSize":  item.CompactedFileSize,
	}
	data["Ratio"] = float64(item.CompactedFileSize) / float64(item.OriginalFileSize)

	s.mu.Lock()
	defer s.mu.Unlock()

	tags := s.makeTags(item.Level, item.Full)
	tags["measurement"] = item.mst
	tags["shard_id"] = strconv.FormatUint(item.shardID, 10)
	s.buf = AddPointToBuffer("compact", tags, data, s.buf)
}
