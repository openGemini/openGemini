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

package logstore

import (
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/tracing"
)

const (
	filterReaderCacheHitSpan  = "vertical_filter_cache_hit"
	filterReaderCacheMissSpan = "vertical_filter_cache_miss"
)

type fileReader struct {
	reader fileops.BasicFileReader
}

func NewFileReader(reader fileops.BasicFileReader) *fileReader {
	return &fileReader{reader: reader}
}

func (r *fileReader) Size() (int64, error) {
	return r.reader.Size()
}

func (r *fileReader) Close() error {
	return r.reader.Close()
}

func (r *fileReader) ReadBatch(offs, sizes []int64, limit int, isStat bool, results map[int64][]byte) error {
	c := make(chan *request.StreamReader, 1)
	r.reader.StreamReadBatch(offs, sizes, c, limit, isStat)
	for sr := range c {
		if sr.Err != nil {
			return sr.Err
		}
		results[sr.Offset] = sr.Content
	}
	return nil
}

func (r *fileReader) StartSpan(span *tracing.Span) {
}

func (r *fileReader) EndSpan() {
}

type VlmCacheReader struct {
	basicReader    BloomFilterReader
	pieceLruCache  *BlockLruCache
	groupLruCache  *BlockLruCache
	pathId         uint64
	version        uint32
	enableHotCache bool
	span           *tracing.Span
}

func NewVlmCacheReader(reader *fileReader, path, fileName string, version uint32) *VlmCacheReader {
	pathName := obs.Join(path, fileName)
	pathId := xxhash.Sum64String(pathName)
	vlmReader := &VlmCacheReader{
		basicReader:    reader,
		pieceLruCache:  HotPieceLruCache,
		groupLruCache:  HotGroupLruCache,
		pathId:         pathId,
		version:        version,
		enableHotCache: config.GetLogStoreConfig().IsVlmCacheHotData(),
	}
	return vlmReader
}

func (r *VlmCacheReader) Size() (int64, error) {
	return r.basicReader.Size()
}

func (r *VlmCacheReader) Close() error {
	return r.basicReader.Close()
}

func (r *VlmCacheReader) ReadBatch(offs, sizes []int64, limit int, isStat bool, result map[int64][]byte) error {
	missOffsets := make([]int64, 0)
	missSizes := make([]int64, 0)
	atomic.AddUint64(&VlmCacheReaderBeforeCache, 1)
	for i, offset := range offs {
		// try to fetch from hot group cache
		if r.groupLruCache != nil {
			key := BlockCacheKey{PathId: r.pathId, Position: uint64(offset)}
			bytes, success := r.groupLruCache.FetchWith(key, r.version, sizes[i])
			if success {
				result[offset] = bytes
				continue
			}
		}
		// try to fetch from hot piece cache
		if r.pieceLruCache != nil {
			key := BlockCacheKey{PathId: r.pathId, Position: uint64(offset)}
			bytes, success := r.pieceLruCache.Fetch(key)
			if success {
				result[offset] = bytes
				continue
			}
		}
		// record miss cache offsets and sizes
		missOffsets = append(missOffsets, offset)
		missSizes = append(missSizes, sizes[i])
	}

	atomic.AddUint64(&PieceCacheHitNum, uint64(len(result)))
	atomic.AddUint64(&PieceCacheHitMiss, uint64(len(missOffsets)))
	atomic.AddUint64(&VlmCacheReaderAfterCache, 1)
	if r.span != nil {
		r.span.Count(filterReaderCacheHitSpan, int64(len(result)))
		r.span.Count(filterReaderCacheMissSpan, int64(len(missOffsets)))
	}

	if len(missOffsets) == 0 {
		return nil
	}

	atomic.AddUint64(&VlmCacheReaderBeforRead, 1)
	err := r.basicReader.ReadBatch(missOffsets, missSizes, limit, isStat, result)
	if err != nil {
		return err
	}
	atomic.AddUint64(&VlmCacheReaderAfterRead, 1)
	if r.enableHotCache {
		for _, offset := range missOffsets {
			key := BlockCacheKey{PathId: r.pathId, Position: uint64(offset)}
			r.pieceLruCache.Store(key, r.version, result[offset])
		}
	}
	atomic.AddUint64(&VlmCacheReaderAfterStore, 1)
	return nil
}

func (r *VlmCacheReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	r.span = span
	r.span.CreateCounter(filterReaderCacheHitSpan, "")
	r.span.CreateCounter(filterReaderCacheMissSpan, "")
}

func (r *VlmCacheReader) EndSpan() {
}

type BloomFilterReader interface {
	Size() (int64, error)
	ReadBatch(offs, sizes []int64, limit int, isStat bool, results map[int64][]byte) error
	StartSpan(span *tracing.Span)
	EndSpan()
	Close() error
}

func NewBloomfilterReader(obsOpts *obs.ObsOptions, path, fileName string, version uint32) (BloomFilterReader, error) {
	fd, err := fileops.OpenObsFile(path, fileName, obsOpts, true)
	if err != nil {
		return nil, err
	}
	dr := NewFileReader(fileops.NewFileReader(fd, nil))
	if GetVlmCacheInitialized() {
		return NewVlmCacheReader(dr, path, fileName, version), nil
	}
	return dr, nil
}
