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

package resourceallocator

import (
	"errors"
	"time"
)

type ResourceType int64

var ErrResTypeNotFound = errors.New("resource type not found")

const (
	ChunkReaderRes ResourceType = iota
	ShardsParallelismRes
	SeriesParallelismRes
	Bottom
)

var (
	resourceArr []ResourceManager
)

func init() {
	resourceArr = make([]ResourceManager, Bottom)
}

func InitResAllocator(threshold, minAllocNum, minShardsAllocNum, funcType int64, resType ResourceType, maxWaitTime time.Duration, ptNumPerNode uint32) error {
	switch resType {
	case ChunkReaderRes:
		chunkReaderResAllocator, e := NewChunkReaderResAllocator(threshold, minAllocNum, funcType)
		if e != nil {
			return e
		}
		resourceArr[ChunkReaderRes] = chunkReaderResAllocator
	case ShardsParallelismRes:
		shardsAllocator, e := NewShardsParallelismAllocator(maxWaitTime, threshold, minShardsAllocNum, ptNumPerNode)
		if e != nil {
			return e
		}
		resourceArr[ShardsParallelismRes] = shardsAllocator
	case SeriesParallelismRes:
		resourceArr[SeriesParallelismRes] = NewSeriesParallelismAllocator(maxWaitTime, threshold)
	default:
		return ErrResTypeNotFound
	}
	return nil
}

func AllocRes(resourceType ResourceType, num int64) (int64, int64, error) {
	if resourceType >= Bottom {
		return 0, 0, ErrResTypeNotFound
	}
	r := resourceArr[resourceType]
	return r.Alloc(num)
}

func AllocParallelismRes(resourceType ResourceType, num int64) (int64, error) {
	if resourceType >= Bottom {
		return 0, ErrResTypeNotFound
	}
	r := resourceArr[resourceType]
	return r.AllocParallelism(num)
}

func FreeParallelismRes(resourceType ResourceType, num, totalNum int64) error {
	if resourceType >= Bottom {
		return ErrResTypeNotFound
	}
	r := resourceArr[resourceType]
	r.FreeParallelism(num, totalNum)
	return nil
}

func FreeRes(resourceType ResourceType, num, totalNum int64) error {
	if resourceType >= Bottom {
		return ErrResTypeNotFound
	}
	r := resourceArr[resourceType]
	r.Free(num, totalNum)
	return nil
}

func DefaultSeriesAllocateFunc(seriesNum int64) error {
	if _, _, e := AllocRes(SeriesParallelismRes, seriesNum); e != nil {
		return e
	}
	return nil
}
