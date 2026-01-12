// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

import (
	"strconv"
	"time"
)

const (
	StatFileCount           = "FileCount"
	StatFileSize            = "FileSize"
	FileStatisticsName      = "filestat"
	levelFileStatisticsName = "filestat_level"

	fileStatisticsReportRate     = 6
	fileStatisticsReportInterval = time.Minute * 5
)

func init() {
	NewCollector().Register(indexFileStat)
}

var indexFileStat = &IndexFileStat{}

func NewIndexFileStat() *IndexFileStat {
	indexFileStat.enabled = true
	return indexFileStat
}

type IndexFileStat struct {
	BaseCollector

	IndexFileTotal *ItemInt64
}

var FileTagMap map[string]string

type FileStatistics struct {
	lastReport time.Time
}

func NewFileStatistics() *FileStatistics {
	return &FileStatistics{
		lastReport: time.Now().Add(-fileStatisticsReportInterval),
	}
}

func InitFileStatistics(tags map[string]string) {
	FileTagMap = tags
}

func FileStatisticsLimited(i int64) bool {
	return i%fileStatisticsReportRate != 1
}

func (s *FileStatistics) Collect(buffer []byte, originTags map[string]string, fileStat *FileStat) ([]byte, error) {
	buffer, err := s.CollectMst(buffer, originTags, fileStat.mst)
	if err != nil {
		return nil, err
	}

	return s.CollectLevel(buffer, map[string]string{"database": originTags["database"]}, fileStat.level)
}

func (s *FileStatistics) CollectMst(buffer []byte, originTags map[string]string, mstFileStat map[string]*FileStatItem) ([]byte, error) {
	if time.Since(s.lastReport) < fileStatisticsReportInterval {
		return buffer, nil
	}
	s.lastReport = time.Now()

	tags := s.merge(originTags, FileTagMap)
	var fileSize, fileCount int64

	for _, stat := range mstFileStat {
		fileSize += stat.FileSize
		fileCount += int64(stat.FileCount)
	}

	valueMap := map[string]interface{}{
		StatFileSize:  fileSize,
		StatFileCount: fileCount,
	}
	buffer = AddPointToBuffer(FileStatisticsName, tags, valueMap, buffer)

	return buffer, nil
}

func (s *FileStatistics) CollectLevel(buffer []byte, originTags map[string]string, fileStat map[uint16]*FileStatItem) ([]byte, error) {
	tags := s.merge(originTags, FileTagMap)
	for level, stat := range fileStat {
		valueMap := map[string]interface{}{
			StatFileSize:  stat.FileSize,
			StatFileCount: int64(stat.FileCount),
		}
		tags["level"] = strconv.FormatUint(uint64(level), 10)

		buffer = AddPointToBuffer(levelFileStatisticsName, tags, valueMap, buffer)
	}

	return buffer, nil
}

func (s *FileStatistics) merge(originTags map[string]string, newTags map[string]string) map[string]string {
	// Add everything in tags to the result.
	out := make(map[string]string, len(newTags))
	for k, v := range newTags {
		out[k] = v
	}

	// Only add values from t that don't appear in tags.
	for k, v := range originTags {
		if _, ok := newTags[k]; !ok {
			out[k] = v
		}
	}
	return out
}

type FileStatItem struct {
	FileCount int
	FileSize  int64
}

func NewFileStatItem(count int, size int64) *FileStatItem {
	return &FileStatItem{
		FileCount: count,
		FileSize:  size,
	}
}

func (s *FileStatItem) Add(count int, size int64) {
	s.FileCount += count
	s.FileSize += size
}

type FileStat struct {
	mst   map[string]*FileStatItem
	level map[uint16]*FileStatItem
}

func NewFileStat() *FileStat {
	return &FileStat{
		mst:   make(map[string]*FileStatItem),
		level: make(map[uint16]*FileStatItem),
	}
}

func (s *FileStat) AddMst(mst string, count int, size int64) {
	item, ok := s.mst[mst]
	if !ok {
		s.mst[mst] = NewFileStatItem(count, size)
		return
	}
	item.Add(count, size)
}

func (s *FileStat) AddLevel(level uint16, size int64) {
	item, ok := s.level[level]
	if !ok {
		s.level[level] = NewFileStatItem(1, size)
		return
	}
	item.Add(1, size)
}
