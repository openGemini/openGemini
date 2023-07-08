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
	Alloc(num int64) (allocNum int64, totalNum int64, err error)
	Free(num, totalNum int64)
	AllocParallelism(num int64) (int64, error)
	FreeParallelism(num int64, totalNum int64)
}

type ShardParallelismAllocator struct {
	sharedParallelismPool   bucket.ResourceBucket
	maxParallelismAllocator *BaseResAllocator
}

func NewShardsParallelismAllocator(maxTime time.Duration, maxShardsParallelism, minShardsAllocNum int64, ptNumPerNode uint32) (*ShardParallelismAllocator, error) {
	if maxTime <= 0 {
		maxTime = DefaultWaitTimeOut
	}
	if ptNumPerNode == 0 {
		ptNumPerNode = 1
	}
	if maxShardsParallelism <= 0 {
		maxShardsParallelism = int64(cpu.GetCpuNum()) * 2 * int64(ptNumPerNode)
	} else {
		maxShardsParallelism *= int64(ptNumPerNode)
	}
	allocator, err := NewBaseResAllocator(maxShardsParallelism/8, minShardsAllocNum, GradientDesc)
	if err != nil {
		return nil, err
	}
	return &ShardParallelismAllocator{
		sharedParallelismPool:   bucket.NewInt64Bucket(maxTime, maxShardsParallelism, true),
		maxParallelismAllocator: allocator,
	}, nil
}

func (s *ShardParallelismAllocator) AllocParallelism(num int64) (int64, error) {
	n, m, err := s.maxParallelismAllocator.Alloc(num)
	if n != m || err != nil {
		return n, nil
	}
	return n, nil
}

func (s *ShardParallelismAllocator) Alloc(num int64) (int64, int64, error) {
	n, _, _ := s.maxParallelismAllocator.Alloc(num)
	if e := s.sharedParallelismPool.GetResource(num); e != nil {
		s.maxParallelismAllocator.Free(n, n)
		return 0, 0, e
	}
	return n, num, nil
}

func (s *ShardParallelismAllocator) FreeParallelism(num int64, totalNum int64) {
	s.maxParallelismAllocator.Free(num, totalNum)
}

func (s *ShardParallelismAllocator) Free(num, totalNum int64) {
	s.sharedParallelismPool.ReleaseResource(totalNum)
	s.maxParallelismAllocator.Free(num, totalNum)
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
		seriesParallelismPool: bucket.NewInt64Bucket(maxTime, maxSeriesParallelism, false),
	}
}

func (s *SeriesResAllocator) Alloc(num int64) (int64, int64, error) {
	return num, num, s.seriesParallelismPool.GetResource(num)
}

// not use
func (s *SeriesResAllocator) AllocParallelism(num int64) (int64, error) {
	return 0, nil
}

func (s *SeriesResAllocator) Free(num, totalNum int64) {
	s.seriesParallelismPool.ReleaseResource(num)
}

// not use
func (s *SeriesResAllocator) FreeParallelism(num int64, totalNum int64) {}

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

func (p *BaseResAllocator) Alloc(num int64) (int64, int64, error) {
	allocNum := p.computeALGO(p.threshold, atomic.LoadInt64(&p.aliveCount), num, p.minAllocNum)
	atomic.AddInt64(&p.aliveCount, allocNum)
	return allocNum, allocNum, nil
}

func (p *BaseResAllocator) Free(num, totalNum int64) {
	atomic.AddInt64(&p.aliveCount, -num)
}

type ChunkReaderResAllocator struct {
	allocator *BaseResAllocator
}

func NewChunkReaderResAllocator(threshold, minAllocNum, funcType int64) (*ChunkReaderResAllocator, error) {
	allocator, err := NewBaseResAllocator(threshold, minAllocNum, funcType)
	return &ChunkReaderResAllocator{allocator: allocator}, err
}

func (p *ChunkReaderResAllocator) Alloc(num int64) (int64, int64, error) {
	return p.allocator.Alloc(num)
}

// not use
func (p *ChunkReaderResAllocator) AllocParallelism(num int64) (int64, error) {
	return 0, nil
}

func (p *ChunkReaderResAllocator) Free(num, totalNum int64) {
	p.allocator.Free(num, totalNum)
}

func (p *ChunkReaderResAllocator) FreeParallelism(num int64, totalNum int64) {
	p.allocator.Free(num, totalNum)
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
