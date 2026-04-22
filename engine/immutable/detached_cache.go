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
package immutable

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/fragment"
	"go.uber.org/zap"
)

const (
	QueryMetaCacheTTL            = 10 * time.Minute
	QueryMetaDataCacheSize int64 = 50 * 1024 * 1024 * int64(MetaIndexLen+128*int(unsafe.Sizeof(fragment.FragmentRange{})))
)

var DetachedMetaDataCache = cache.NewCache(QueryMetaDataCacheSize, QueryMetaCacheTTL)

type IndexFrags interface {
	BasePath() string
	FragCount() int64
	IndexCount() int
	Indexes() interface{}
	AppendIndexes(...interface{})
	FragRanges() []fragment.FragmentRanges
	AppendFragRanges(...fragment.FragmentRanges)
	AddFragCount(int64)
	SetErr(error)
	GetErr() error
	Size() int
}

type DetachedSegmentEntry struct {
	segmentID string
	task      IndexFrags
	time      time.Time
}

func NewSegmentMetaDataEntry(segmentID string) *DetachedSegmentEntry {
	return &DetachedSegmentEntry{segmentID: segmentID}
}

func (e *DetachedSegmentEntry) SetTime(time time.Time) {
	e.time = time
}

func (e *DetachedSegmentEntry) GetTime() time.Time {
	return e.time
}

func (e *DetachedSegmentEntry) SetValue(value interface{}) {
	v, ok := value.(IndexFrags)
	if !ok {
		log.Error("DetachedSegmentEntry", zap.Error(fmt.Errorf("invalid element type")))
	}
	e.task = v
}

func (e *DetachedSegmentEntry) GetValue() interface{} {
	return e.task
}

func (e *DetachedSegmentEntry) GetKey() string {
	return e.segmentID
}

func (e *DetachedSegmentEntry) Size() int64 {
	return int64(unsafe.Sizeof(DetachedSegmentEntry{})) + int64(len(e.segmentID)+e.task.Size())
}

func GetDetachedSegmentTask(queryID string) (IndexFrags, bool) {
	entry, ok := DetachedMetaDataCache.Get(queryID)
	if !ok {
		return nil, false
	}
	meta, ok := entry.GetValue().(IndexFrags)
	return meta, ok
}

func PutDetachedSegmentTask(queryID string, meta IndexFrags) {
	entry := NewSegmentMetaDataEntry(queryID)
	entry.SetValue(meta)
	DetachedMetaDataCache.Put(queryID, entry, UpdateDetachedMetaDataCache)
}

func UpdateDetachedMetaDataCache(old, new cache.Entry) bool {
	return true
}
