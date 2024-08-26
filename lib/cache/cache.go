// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package cache

import (
	"container/list"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

var log = logger.NewLogger(errno.ModuleQueryEngine)

const (
	IncIterNumCacheSize int64 = 1 * 1024 * 1024
	IncIterNumCacheTTL        = 10 * time.Minute
)

const TimeSizeBytes = int(unsafe.Sizeof(time.Time{}))

var NodeIncIterNumCache = NewCache(IncIterNumCacheSize, IncIterNumCacheTTL)
var GlobalIncIterNumCache = NewCache(IncIterNumCacheSize, IncIterNumCacheTTL)

type Entry interface {
	SetTime(time time.Time)
	GetTime() time.Time
	SetValue(value interface{})
	GetValue() interface{}
	GetKey() string
	Size() int64
}

type Cache struct {
	mutex sync.RWMutex

	// Maximum capacity (unit: byte)
	maxBytes int64

	// Current Used Capacity
	usedBytes int64

	// Bidirectional linked list for storing key-value pairs
	ll *list.List

	// Stores the map of the key-value pair.
	cache map[string]*list.Element

	// timeout duration
	expireTime time.Duration
}

// NewCache used to create a cache instance.
func NewCache(maxBytes int64, expireTime time.Duration) *Cache {
	return &Cache{
		maxBytes:   maxBytes,
		expireTime: expireTime,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
	}
}

// Get used to obtain the value of the key and returns whether the key is obtained successfully.
func (c *Cache) Get(key string) (Entry, bool) {
	c.mutex.RLock()

	if ele, ok := c.cache[key]; ok {
		c.mutex.RUnlock()
		value, ok := ele.Value.(Entry)
		if !ok {
			log.Error(fmt.Sprintf("Invalid cache element, key: %s, value: %v. [Put]", key, value))
			return nil, false
		}
		// Check whether the validity period expires.
		if time.Since(value.GetTime()) > c.expireTime {
			c.mutex.Lock()
			c.removeElement(ele)
			c.mutex.Unlock()
		} else {
			c.mutex.Lock()
			c.ll.MoveToFront(ele)
			c.mutex.Unlock()
		}
		return value, true
	}
	c.mutex.RUnlock()
	return nil, false
}

// Put used to add or update a Key-Value Pair
func (c *Cache) Put(key string, value Entry, needUpdate func(old Entry, new Entry) bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if ele, ok := c.cache[key]; ok {
		oriValue, ok := ele.Value.(Entry)
		if !ok {
			log.Error(fmt.Sprintf("Invalid cache element, key: %s, value: %d. [Put]", key, value))
			return
		}
		// If the key exists, update the value and access sequence and check whether the key expires.
		if time.Since(oriValue.GetTime()) > c.expireTime || needUpdate(oriValue, value) {
			c.usedBytes -= oriValue.Size()
			c.usedBytes += value.Size()
			oriValue.SetValue(value.GetValue())
			oriValue.SetTime(time.Now())
			c.ll.MoveToFront(ele)
			return
		}
		oriValue.SetTime(time.Now())
		c.ll.MoveToFront(ele)
		return
	}

	// If the key does not exist, add it.
	value.SetTime(time.Now())
	ele := c.ll.PushFront(value)
	c.cache[key] = ele
	c.usedBytes += value.Size()

	// If the capacity exceeds the maximum capacity, the oldest element is deleted.
	for c.maxBytes != 0 && c.usedBytes > c.maxBytes {
		c.removeOldest()
	}
}

// Len used to obtain the number of elements in the cache.
func (c *Cache) Len() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.ll.Len()
}

// Remove to delete an element with a specified key.
func (c *Cache) Remove(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if ele, ok := c.cache[key]; ok {
		c.removeElement(ele)
	}
}

// removeOldest used to delete the longest unused element.
func (c *Cache) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

// removeElement used to delete a specified element.
func (c *Cache) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	et, ok := ele.Value.(Entry)
	if !ok {
		log.Error(fmt.Sprintf("Invalid cache element, value: %d. [remove]", ele.Value))
		return
	}
	delete(c.cache, et.GetKey())
	c.usedBytes -= et.Size()
}

type IncIterNumEntry struct {
	queryID string
	iterNum int32
	time    time.Time
}

func NewIncIterNumEntry(queryID string) *IncIterNumEntry {
	return &IncIterNumEntry{queryID: queryID}
}

func (e *IncIterNumEntry) SetTime(time time.Time) {
	e.time = time
}

func (e *IncIterNumEntry) GetTime() time.Time {
	return e.time
}

func (e *IncIterNumEntry) SetValue(value interface{}) {
	iterNum, ok := value.(int32)
	if !ok {
		log.Error("IncIterNumEntry", zap.Error(fmt.Errorf("invalid element type")))
	}
	e.iterNum = iterNum
}

func (e *IncIterNumEntry) GetValue() interface{} {
	return e.iterNum
}

func (e *IncIterNumEntry) GetKey() string {
	return e.queryID
}

func (e *IncIterNumEntry) Size() int64 {
	var size int64
	size += int64(len(e.queryID))      // queryID
	size += int64(util.Int32SizeBytes) // iterNum
	size += int64(TimeSizeBytes)       // time
	return size
}

func UpdateMetaData(_, _ Entry) bool {
	return true
}

func UpdateIterNumFunc(old, new Entry) bool {
	oldVal, ok := old.GetValue().(int32)
	if !ok {
		return true
	}
	newVal, ok := new.GetValue().(int32)
	if !ok {
		return false
	}
	return oldVal < newVal
}

func PutNodeIterNum(queryID string, iterCount int32) {
	entry := NewIncIterNumEntry(queryID)
	entry.SetValue(iterCount)
	NodeIncIterNumCache.Put(queryID, entry, UpdateIterNumFunc)
}

func GetNodeIterNum(queryID string) (int32, bool) {
	entry, ok := NodeIncIterNumCache.Get(queryID)
	if !ok {
		return 0, false
	}
	iterNum, ok := entry.GetValue().(int32)
	return iterNum, ok
}

func PutGlobalIterNum(queryID string, iterCount int32) {
	entry := NewIncIterNumEntry(queryID)
	entry.SetValue(iterCount)
	GlobalIncIterNumCache.Put(queryID, entry, UpdateIterNumFunc)
}

func GetGlobalIterNum(queryID string) (int32, bool) {
	entry, ok := GlobalIncIterNumCache.Get(queryID)
	if !ok {
		return 0, false
	}
	iterNum, ok := entry.GetValue().(int32)
	return iterNum, ok
}
