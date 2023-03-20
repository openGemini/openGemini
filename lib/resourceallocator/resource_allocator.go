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

package resourceallocator

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/bucket"
	"github.com/openGemini/openGemini/lib/cpu"
)

const (
	GradientDesc = iota
)

const (
	DefaultMaxSeriesParallelismNumRatio = 1000000
	DefaultMinimumConcurrency           = 1
	DefaultWaitTimeOut                  = time.Minute
)

type ResourceManager interface {
	Alloc(num int64) (int64, error)
	Free(num int64)
}

type ShardParallelismAllocator struct {
	sharedParallelismPool   bucket.ResourceBucket
	maxParallelismAllocator *BaseResAllocator
}

func NewShardsParallelismAllocator(maxTime time.Duration, maxShardsParallelism, minShardsAllocNum int64) (*ShardParallelismAllocator, error) {
	if maxTime <= 0 {
		maxTime = DefaultWaitTimeOut
	}
	if maxShardsParallelism <= 0 {
		maxShardsParallelism = int64(cpu.GetCpuNum()) * 2
	}
	alloc, err := NewBaseResAllocator(maxShardsParallelism/2, minShardsAllocNum, GradientDesc)
	if err != nil {
		return nil, err
	}
	return &ShardParallelismAllocator{
		sharedParallelismPool:   bucket.NewInt64Bucket(maxTime, maxShardsParallelism),
		maxParallelismAllocator: alloc,
	}, nil
}

func (s *ShardParallelismAllocator) Alloc(num int64) (int64, error) {
	n, _ := s.maxParallelismAllocator.Alloc(num)
	if e := s.sharedParallelismPool.GetResource(n); e != nil {
		s.maxParallelismAllocator.Free(n)
		return 0, e
	}
	return n, nil
}

func (s *ShardParallelismAllocator) Free(num int64) {
	s.sharedParallelismPool.ReleaseResource(num)
	s.maxParallelismAllocator.Free(num)
}

type SeriesResAllocator struct {
	seriesParallelismPool bucket.ResourceBucket
}

func NewSeriesParallelismAllocator(maxTime time.Duration, maxSeriesParallelism int64) *SeriesResAllocator {
	if maxTime <= 0 {
		maxTime = DefaultWaitTimeOut
	}
	if maxSeriesParallelism <= 0 {
		maxSeriesParallelism = int64(DefaultMaxSeriesParallelismNumRatio * cpu.GetCpuNum())
	}
	return &SeriesResAllocator{
		seriesParallelismPool: bucket.NewInt64Bucket(maxTime, maxSeriesParallelism),
	}
}

func (s *SeriesResAllocator) Alloc(num int64) (int64, error) {
	return num, s.seriesParallelismPool.GetResource(num)
}

func (s *SeriesResAllocator) Free(num int64) {
	s.seriesParallelismPool.ReleaseResource(num)
}

type BaseResAllocator struct {
	threshold   int64
	minAllocNum int64
	aliveCount  int64
	computeALGO func(threshold, aliveCount, AllocNum, minAllocNum int64) int64
}

func NewBaseResAllocator(threshold, minAllocNum, funcType int64) (*BaseResAllocator, error) {
	//maxNum represents the max pipeline number for the limiter and minimumAppNum represents the minimum pipeline number for one query.
	r := &BaseResAllocator{
		threshold:   threshold,
		minAllocNum: minAllocNum,
	}
	if r.threshold <= 0 {
		r.threshold = int64(cpu.GetCpuNum() * 2)
	}
	if r.minAllocNum <= 0 {
		r.minAllocNum = DefaultMinimumConcurrency
	}
	switch funcType {
	case GradientDesc:
		r.computeALGO = gradientDescAllocation
	default:
		return nil, errors.New("unknown function type")
	}
	return r, nil
}

func (p *BaseResAllocator) Alloc(num int64) (int64, error) {
	allocNum := p.computeALGO(p.threshold, atomic.LoadInt64(&p.aliveCount), num, p.minAllocNum)
	atomic.AddInt64(&p.aliveCount, allocNum)
	return allocNum, nil
}

func (p *BaseResAllocator) Free(num int64) {
	atomic.AddInt64(&p.aliveCount, -num)
}

type ChunkReaderResAllocator struct {
	allocator *BaseResAllocator
}

func NewChunkReaderResAllocator(threshold, minAllocNum, funcType int64) (*ChunkReaderResAllocator, error) {
	allocator, err := NewBaseResAllocator(threshold, minAllocNum, funcType)
	return &ChunkReaderResAllocator{allocator: allocator}, err
}

func (p *ChunkReaderResAllocator) Alloc(num int64) (int64, error) {
	return p.allocator.Alloc(num)
}

func (p *ChunkReaderResAllocator) Free(num int64) {
	p.allocator.Free(num)
}

func gradientDescAllocation(threshold, aliveNum, allocNum, minimumAllocNum int64) (returnNum int64) {
	returnNum = allocNum
	if allocNum < minimumAllocNum {
		return
	}
	freeNum := threshold - aliveNum
	if freeNum < minimumAllocNum {
		returnNum = minimumAllocNum
		return
	}
	for returnNum > freeNum/2 && returnNum > minimumAllocNum {
		returnNum = returnNum >> 1
	}
	if returnNum < minimumAllocNum {
		returnNum = minimumAllocNum
	}
	return
}
