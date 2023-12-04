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

	"github.com/openGemini/openGemini/lib/readcache"
	"go.uber.org/zap"
)

type PageCacheReader struct {
	r             *tsspFileReader
	trailer       *Trailer
	init          bool
	startOffset   int64
	endOffset     int64
	maxPageId     int64
	maxPageOffset int64
	read          func(offset int64, size uint32, buf *[]byte, ioPriority int) ([]byte, *readcache.CachePage, error)
}

func NewPageCacheReader(t *Trailer, r *tsspFileReader) *PageCacheReader {
	pcr := &PageCacheReader{
		trailer: t,
		r:       r,
	}
	if readcache.IsPageSizeVariable {
		pcr.read = pcr.ReadVariablePageSize
	} else {
		pcr.read = pcr.ReadFixPageSize
	}
	return pcr
}

func (pcr *PageCacheReader) Init() {
	pcr.startOffset = pcr.trailer.dataOffset
	pcr.endOffset, _ = pcr.trailer.metaOffsetSize()
	pcr.maxPageId, pcr.maxPageOffset = pcr.GetMaxPageIdAndOffset()
}

// get all cache pageIds containning bytes from start to start + size
func (pcr *PageCacheReader) GetCachePageIdsAndOffsets(start int64, size uint32) ([]int64, []int64, error) {
	end := (start + int64(size))
	if start < pcr.startOffset || start >= pcr.endOffset || end < pcr.startOffset || end > pcr.endOffset {
		return nil, nil, fmt.Errorf("invalid read offset of GetCachePageIdsAndOffsets() start:%v end:%v startOffset:%v endOffset:%v", start, end, pcr.startOffset, pcr.endOffset)
	}

	pageIds := make([]int64, 0, int64(size)/readcache.PageSize+1)
	pageOffsets := make([]int64, 0, int64(size)/readcache.PageSize+1)
	startPageId := ((start - pcr.startOffset) / readcache.PageSize) + 1
	startPageOffset := (startPageId-1)*readcache.PageSize + pcr.startOffset

	for ; startPageOffset < end; startPageOffset += readcache.PageSize {
		pageIds = append(pageIds, startPageId)
		pageOffsets = append(pageOffsets, startPageOffset)
		startPageId++
	}
	return pageIds, pageOffsets, nil
}

func (pcr *PageCacheReader) GetMaxPageIdAndOffset() (int64, int64) {
	over := (pcr.endOffset - pcr.startOffset) % readcache.PageSize
	if over > 0 {
		over = 1
	}
	endPageId := (pcr.endOffset-pcr.startOffset)/readcache.PageSize + over
	endPageOffset := (endPageId-1)*readcache.PageSize + pcr.startOffset
	return endPageId, endPageOffset
}

func (pcr *PageCacheReader) ReadSinglePage(cacheKey string, pageOffset int64, pageSize int64, buf *[]byte, ioPriority int) (*readcache.CachePage, []byte, error) {
	cacheIns := readcache.GetReadDataCacheIns()
	var pageCache *readcache.CachePage
	var ok bool
	if value, isGet := cacheIns.GetPageCache(cacheKey); isGet {
		pageCache, ok = value.(*readcache.CachePage)
		if !ok {
			return nil, nil, fmt.Errorf("cacheValue is not a page")
		}
		return pageCache, pageCache.Value, nil
	} else {
		pageCache := readcache.CachePagePool.Get()
		pageCache.Ref()
		tempPage, err := pcr.r.Read(pageOffset, uint32(pageSize), &pageCache.Value, ioPriority)
		if err != nil {
			pageCache.Unref()
			log.Error("read TSSPFile failed", zap.Error(err))
			return nil, nil, err
		}
		pageCache.Size = int64(len(tempPage))
		cacheIns.AddPageCache(cacheKey, pageCache, int64(len(tempPage)))
		return pageCache, tempPage, nil
	}
}

func (pcr *PageCacheReader) Read(offset int64, size uint32, buf *[]byte, ioPriority int) ([]byte, *readcache.CachePage, error) {
	return pcr.read(offset, size, buf, ioPriority)
}

func (pcr *PageCacheReader) ReadVariablePageSize(offset int64, size uint32, buf *[]byte, ioPriority int) ([]byte, *readcache.CachePage, error) {
	var err error
	cacheIns := readcache.GetReadDataCacheIns()
	cacheKey := cacheIns.CreateCacheKey(pcr.r.FileName(), offset)
	var b []byte
	var page *readcache.CachePage
	var ok bool
	if value, isGet := cacheIns.Get(cacheKey); isGet {
		page, ok = value.(*readcache.CachePage)
		if !ok {
			return nil, nil, fmt.Errorf("cacheValue is not a page")
		}
		if page.Size >= int64(size) {
			b = page.Value[:size]
			return b, nil, nil
		}
	}

	b, err = pcr.r.Read(offset, size, buf, ioPriority)
	if err != nil {
		log.Error("read TSSPFile failed", zap.Error(err))
		return nil, nil, err
	}
	cacheIns.AddPage(cacheKey, b, int64(size))
	return b, nil, nil
}

// read fileBytes of pages
func (pcr *PageCacheReader) ReadFixPageSize(offset int64, size uint32, buf *[]byte, ioPriority int) ([]byte, *readcache.CachePage, error) {
	if !pcr.init {
		pcr.Init()
		pcr.init = true
	}
	var err error
	var pageBuf []byte
	var cachePage *readcache.CachePage
	cacheIns := readcache.GetReadDataCacheIns()
	pageIds, pageOffsets, err := pcr.GetCachePageIdsAndOffsets(offset, size)
	if err != nil || len(pageIds) == 0 {
		return nil, nil, err
	}
	cacheKeys := cacheIns.CreateCacheKeys(pcr.r.FileName(), pageIds)
	tempEnd := readcache.PageSize
	if int64(size) < readcache.PageSize && len(pageIds) == 1 {
		tempEnd = int64(size) + (offset - (pageOffsets)[0])
	}
	tempPageSize := readcache.PageSize
	if (pageIds)[0] == pcr.maxPageId {
		tempPageSize = pcr.endOffset - pcr.maxPageOffset
	}
	// 1. fast: read one page
	if len(cacheKeys) == 1 {
		cachePage, pageBuf, err = pcr.ReadSinglePage(cacheKeys[0], pageOffsets[0], tempPageSize, buf, ioPriority)
		if err != nil {
			return nil, nil, err
		}
		newb := pageBuf[(offset - pageOffsets[0]):tempEnd]
		return newb, cachePage, nil
	}

	*buf = (*buf)[:0]
	b := buf
	// 2. read multi pages
	// 2.1. first page
	cachePage, pageBuf, err = pcr.ReadSinglePage(cacheKeys[0], pageOffsets[0], tempPageSize, buf, ioPriority)
	if err != nil {
		return nil, nil, err
	}
	*b = append(*b, pageBuf[(offset-pageOffsets[0]):tempEnd]...)
	pcr.r.UnrefCachePage(cachePage)

	// 2.2. middle pages
	for i := 1; i < len(cacheKeys)-1; i++ {
		cachePage, pageBuf, err = pcr.ReadSinglePage(cacheKeys[i], pageOffsets[i], readcache.PageSize, buf, ioPriority)
		if err != nil {
			return nil, nil, err
		}
		*b = append(*b, pageBuf...)
		pcr.r.UnrefCachePage(cachePage)
	}
	// 2.3. last page
	if pageIds[len(pageIds)-1] == pcr.maxPageId {
		tempPageSize = pcr.endOffset - pcr.maxPageOffset
	}
	cachePage, pageBuf, err = pcr.ReadSinglePage(cacheKeys[len(cacheKeys)-1], pageOffsets[len(pageOffsets)-1], tempPageSize, buf, ioPriority)
	if err != nil {
		return nil, nil, err
	}
	*b = append(*b, pageBuf[:int64(size)-int64(len(*b))]...)
	pcr.r.UnrefCachePage(cachePage)
	return *b, nil, nil
}
