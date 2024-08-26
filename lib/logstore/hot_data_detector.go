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

package logstore

import (
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"go.uber.org/zap"
)

var hotDataLogger = logger.NewLogger(errno.ModuleLogStore)

const (
	MaxReadRequestChanSize = 512
	MaxTimeBuf             = 256
	TimeInverval           = 30 * time.Minute
	FrequencyThreshold     = 10
)

var (
	PrefetchTimerInterval = 5 * time.Second
)

type PrefetchType int

const (
	PiecePrefetch PrefetchType = iota
	GroupPrefetch
)

type LogPath struct {
	Path     string
	FileName string
	Version  uint32
	ObsOpt   *obs.ObsOptions
}

type LogReadRequest struct {
	logPath *LogPath
	hashes  []uint64
}

type HitHashes struct {
	hashes    []uint64
	visitTime int64
}

type HotDataValue struct {
	logPath       *LogPath
	hitHashes     *RingBuf[*HitHashes]
	storeReader   BloomFilterReader
	pieceLruCache BlockCache
	groupLruCache BlockCache
	bloomfilter   bloomfilter.Bloomfilter
	segmentPath   string
	pathId        uint64
	visitTime     int64
	startOffset   int64
	closing       chan struct{}
	fetchType     PrefetchType
}

type HotDataValues []*HotDataValue

func (n HotDataValues) Len() int { return len(n) }

func (n HotDataValues) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

func (n HotDataValues) Less(i, j int) bool {
	if n[i].hitHashes.Len() == n[j].hitHashes.Len() {
		return n[i].visitTime > n[j].visitTime
	}
	return n[i].hitHashes.Len() > n[j].hitHashes.Len()
}

func NewHotDataValue(logPath *LogPath) *HotDataValue {
	pathName := obs.Join(logPath.Path, logPath.FileName)
	pathId := xxhash.Sum64String(pathName)
	value := &HotDataValue{
		hitHashes:   NewRingBuf[*HitHashes](MaxTimeBuf),
		logPath:     logPath,
		pathId:      pathId,
		segmentPath: logPath.Path,
		closing:     make(chan struct{}),
	}
	verticalFilterStoreReader, err := NewBasicBloomfilterReader(logPath.ObsOpt, logPath.Path, logPath.FileName)
	if err != nil {
		hotDataLogger.Error("new obsStore reader failed", zap.String("path", logPath.Path), zap.Error(err))
	} else {
		value.storeReader = verticalFilterStoreReader
	}
	if config.GetLogStoreConfig().IsVlmCachePiecePrefetch() {
		value.pieceLruCache = GetHotPieceLruCache()
		value.fetchType = PiecePrefetch
	}
	if config.GetLogStoreConfig().IsVlmCacheGroupPrefetch() {
		value.groupLruCache = GetHotGroupLruCache()
		value.fetchType = GroupPrefetch
	}
	value.bloomfilter = bloomfilter.DefaultOneHitBloomFilter(logPath.Version, GetConstant(logPath.Version).FilterDataMemSize)
	return value
}

func (v *HotDataValue) SetVisitTime(visitTime int64, hashes []uint64) {
	v.hitHashes.Write(&HitHashes{hashes: hashes, visitTime: visitTime})
	v.visitTime = visitTime
}

func (v *HotDataValue) Frequency() int {
	return v.hitHashes.Len()
}

func (v *HotDataValue) GroupPrefetch() {
	filterLen, err := v.storeReader.Size()
	if err != nil {
		return
	}
	verticalGroupDiskSize := GetConstant(v.logPath.Version).VerticalGroupDiskSize
	for v.startOffset+verticalGroupDiskSize <= filterLen {
		result := make(map[int64][]byte)
		err = v.storeReader.ReadBatch([]int64{v.startOffset}, []int64{verticalGroupDiskSize}, -1, true, result)
		if err != nil {
			hotDataLogger.Error("prefetch read failed", zap.String("path", v.segmentPath), zap.Error(err))
			return
		}
		key := BlockCacheKey{PathId: v.pathId, Position: uint64(v.startOffset)}
		v.groupLruCache.Store(key, v.logPath.Version, result[v.startOffset])
		v.startOffset = v.startOffset + verticalGroupDiskSize
	}
}

func (v *HotDataValue) getAllOffsets() ([]int64, []int64) {
	verticalPieceDiskSize := GetConstant(v.logPath.Version).VerticalPieceDiskSize
	offsetsSet := make(map[int64]struct{}, 0)
	it := NewIterator(v.hitHashes)
	hitHashes, ok := it.Next()
	for ok {
		if hitHashes != nil {
			for _, hash := range hitHashes.hashes {
				bytesOffset := v.bloomfilter.GetBytesOffset(hash)
				pieceOffset := (bytesOffset >> 3) * verticalPieceDiskSize
				offsetsSet[pieceOffset] = struct{}{}
				if (bytesOffset % 8) != 0 {
					offsetsSet[pieceOffset+verticalPieceDiskSize] = struct{}{}
				}
			}
		}
		hitHashes, ok = it.Next()
	}

	offsets := make([]int64, 0, len(offsetsSet))
	lens := make([]int64, 0, len(offsetsSet))
	for offset := range offsetsSet {
		offsets = append(offsets, offset)
		lens = append(lens, verticalPieceDiskSize)
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	return offsets, lens
}

func (v *HotDataValue) getFileOffsets(offsets []int64, start int64) []int64 {
	fileOffsets := make([]int64, len(offsets))
	for i := 0; i < len(offsets); i++ {
		fileOffsets[i] = offsets[i] + start
	}
	return fileOffsets
}

func (v *HotDataValue) PiecePrefetch() {
	filterLen, err := v.storeReader.Size()
	if err != nil {
		return
	}
	verticalGroupDiskSize := GetConstant(v.logPath.Version).VerticalGroupDiskSize
	if v.startOffset+verticalGroupDiskSize > filterLen {
		return
	}
	offsets, lens := v.getAllOffsets()
	for v.startOffset+verticalGroupDiskSize <= filterLen {
		fileOffsets := v.getFileOffsets(offsets, v.startOffset)
		results := make(map[int64][]byte, len(fileOffsets))
		err := v.storeReader.ReadBatch(fileOffsets, lens, -1, true, results)
		if err != nil {
			hotDataLogger.Error("prefetch filterData failed", zap.String("path", v.segmentPath), zap.Error(err))
			continue
		}
		for offset, result := range results {
			key := BlockCacheKey{PathId: v.pathId, Position: uint64(offset)}
			v.pieceLruCache.Store(key, v.logPath.Version, result)
		}
		v.startOffset = v.startOffset + verticalGroupDiskSize
	}
}

func (v *HotDataValue) PrefetchRutine() {
	ticker := time.NewTicker(PrefetchTimerInterval)
	defer ticker.Stop()
	hotDataLogger.Info("PrefetchRutine start", zap.String("segmentPath", v.segmentPath))
	for {
		select {
		case <-v.closing:
			hotDataLogger.Info("PrefetchRutine stop", zap.String("segmentPath", v.segmentPath))
			if v.storeReader != nil {
				v.storeReader.Close()
			}
			return
		case <-ticker.C:
			if v.fetchType == PiecePrefetch {
				v.PiecePrefetch()
			} else {
				v.GroupPrefetch()
			}
		}
	}
}

func (v *HotDataValue) Open() {
	if v.storeReader == nil {
		return
	}
	go v.PrefetchRutine()
}

func (v *HotDataValue) Close() {
	if v.closing != nil {
		close(v.closing)
	}
}

type HotDataDetector struct {
	logRequestChan chan *LogReadRequest
	itemsMap       map[string]*HotDataValue // map-key is segmentPath
	items          []*HotDataValue
}

func NewHotDataDetector() *HotDataDetector {
	return &HotDataDetector{
		logRequestChan: make(chan *LogReadRequest, MaxReadRequestChanSize),
		itemsMap:       make(map[string]*HotDataValue, 0),
		items:          make([]*HotDataValue, 0),
	}
}

func (d *HotDataDetector) Close() {
	close(d.logRequestChan)
	// close reader
	for i := 0; i < len(d.items); i++ {
		d.items[i].Close()
	}
}

func (d *HotDataDetector) Len() int {
	return len(d.items)
}

func (d *HotDataDetector) evictTime() bool {
	canEvict := false
	t := time.Now().UTC().UnixNano()
	for i := 0; i < len(d.items); i++ {
		hitHashes := d.items[i].hitHashes
		for !hitHashes.Empty() {
			hitHash := hitHashes.ReadOldest()
			if hitHash == nil {
				continue
			}
			if hitHash.visitTime+int64(TimeInverval) > t {
				break
			}
			hitHashes.RemoveOldest()
		}
		if d.items[i].Frequency() < FrequencyThreshold {
			canEvict = true
		}
	}
	if canEvict {
		sort.Sort(HotDataValues(d.items))
	}
	return canEvict
}

func (d *HotDataDetector) Evict() bool {
	if !d.evictTime() {
		return false
	}
	i := len(d.items)
	for ; i > 0; i-- {
		if d.items[i-1].Frequency() != 0 {
			break
		}
		d.items[i-1].Close()
		hotDataLogger.Info("Evict logPath prefetcher", zap.String("segmentPath", d.items[i-1].segmentPath))
		delete(d.itemsMap, d.items[i-1].segmentPath)
	}
	d.items = d.items[:i]
	return true
}

func (d *HotDataDetector) ForceEvict() bool {
	i := len(d.items)
	if i == 0 {
		return true
	}
	// evict
	if d.items[i-1].Frequency() > FrequencyThreshold {
		return false
	}
	d.items[i-1].Close()
	hotDataLogger.Info("Evict", zap.String("segmentPath", d.items[i-1].segmentPath))
	delete(d.itemsMap, d.items[i-1].segmentPath)
	d.items = d.items[:i-1]
	return true
}

func (d *HotDataDetector) addNewItems(request *LogReadRequest) {
	canForceEvict := d.Evict()
	// add new value
	prefetchNums := int(config.GetLogStoreConfig().GetVlmCachePrefetchNums())
	if (len(d.items) < prefetchNums) || (canForceEvict && d.ForceEvict()) {
		t := time.Now().UTC().UnixNano()
		value := NewHotDataValue(request.logPath)
		value.SetVisitTime(t, request.hashes)
		d.itemsMap[value.segmentPath] = value
		d.items = append(d.items, value)
		value.Open()
		return
	}
}

func (d *HotDataDetector) DetectHotLogPath(request *LogReadRequest) {
	if request == nil || request.logPath == nil {
		return
	}
	logInfo, err := obs.ParseLogPath(request.logPath.Path)
	if err != nil {
		hotDataLogger.Error("ParseLogPath failed", zap.Error(err))
		return
	}
	t := time.Now().UTC().UnixNano()
	if !logInfo.Contains(t) {
		return
	}
	value, ok := d.itemsMap[request.logPath.Path]
	if ok {
		value.SetVisitTime(t, request.hashes)
		return
	}
	d.addNewItems(request)
}

func (d *HotDataDetector) Running() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	hotDataLogger.Info("hot data detector start")
	for {
		select {
		case request, ok := <-d.logRequestChan:
			if !ok {
				hotDataLogger.Info("hot data detector stop")
				return
			}
			d.DetectHotLogPath(request)
		case <-ticker.C:
			d.Evict()
		}
	}
}

var hotDataDetector = NewHotDataDetector()

func SendLogRequest(readRequest *LogReadRequest) {
	hotDataDetector.logRequestChan <- readRequest
}

func SendLogRequestWithHash(object *LogPath, queryHashes map[string][]uint64) {
	if !GetVlmCacheInitialized() || !config.GetLogStoreConfig().IsVlmPrefetchEnable() {
		return
	}

	hashes := make([]uint64, 0)
	hashesMap := make(map[uint64]struct{}, 0)
	for _, tmpHashes := range queryHashes {
		for _, hash := range tmpHashes {
			if _, ok := hashesMap[hash]; ok {
				continue
			}
			hashesMap[hash] = struct{}{}
			hashes = append(hashes, hash)
		}
	}
	SendLogRequest(&LogReadRequest{logPath: object, hashes: hashes})
}

func HotDataDetectorTaskLen() int {
	return hotDataDetector.Len()
}

func StartHotDataDetector() {
	go hotDataDetector.Running()
}

func StopHotDataDetector() {
	hotDataDetector.Close()
}
