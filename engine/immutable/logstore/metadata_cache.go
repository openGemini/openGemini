/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package logstore

import (
	"time"

	"github.com/openGemini/openGemini/lib/cache"
)

const (
	QueryMetaCacheTTL            = 10 * time.Minute
	QueryMetaDataCacheSize int64 = 50 * 1024 * 1024 * int64(META_DATA_SIZE)
)

var QueryMetaDataCache = cache.NewCache(QueryMetaDataCacheSize, QueryMetaCacheTTL)

type SegmentMetaDataEntry struct {
	segmentID string
	metaData  []*MetaData
	time      time.Time
}

func NewSegmentMetaDataEntry(segmentID string) *SegmentMetaDataEntry {
	return &SegmentMetaDataEntry{segmentID: segmentID}
}

func (e *SegmentMetaDataEntry) SetTime(time time.Time) {
	e.time = time
}

func (e *SegmentMetaDataEntry) GetTime() time.Time {
	return e.time
}

func (e *SegmentMetaDataEntry) SetValue(value interface{}) {
	e.metaData = value.([]*MetaData)
}

func (e *SegmentMetaDataEntry) GetValue() interface{} {
	return e.metaData
}

func (e *SegmentMetaDataEntry) GetKey() string {
	return e.segmentID
}

func (e *SegmentMetaDataEntry) Size() int64 {
	return int64(cap(e.metaData))*int64(META_DATA_SIZE) + int64(len(e.segmentID)) + 24
}

func GetMetaData(queryID string) ([]*MetaData, bool) {
	entry, ok := QueryMetaDataCache.Get(queryID)
	if !ok {
		return nil, false
	}
	meta, ok := entry.GetValue().([]*MetaData)
	return meta, ok
}

func PutMetaData(queryID string, meta []*MetaData) {
	entry := NewSegmentMetaDataEntry(queryID)
	entry.SetValue(meta)
	QueryMetaDataCache.Put(queryID, entry, UpdateMetaData)
}

func UpdateMetaData(old, new cache.Entry) bool {
	return true
}
