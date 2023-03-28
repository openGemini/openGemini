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

package executor

import (
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bucket"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

const (
	//fixme: default bytes for string and tag?
	DefaultStringSize  = 20
	DefaultBooleanSize = 1
	DefaultIntegerSize = 8
	DefaultFloatSize   = 8
	KBytes2Bytes       = int64(1024)
	DefaultTimeOut     = 300 * time.Second
)

type PipelineExecutorInfo struct {
	MemoryOccupation int64
	CPUOccupation    float64
	ProcessorNum     int64
}

type PipelineExecutorManager struct {
	memBucket bucket.ResourceBucket
}

func NewPipelineExecutorManager() *PipelineExecutorManager {
	p := &PipelineExecutorManager{}
	mem, _ := memory.SysMem()
	p.memBucket = bucket.NewInt64Bucket(DefaultTimeOut, mem*KBytes2Bytes, false)
	return p
}

func (p *PipelineExecutorManager) GetMemBucket() bucket.ResourceBucket {
	return p.memBucket
}

func (p *PipelineExecutorManager) ManageMemResource(exec *PipelineExecutor) error {
	exec.WaitTimeStats.Begin()
	defer exec.WaitTimeStats.End()

	MemoryEstimator(exec)
	if e := p.memBucket.GetResource(exec.info.MemoryOccupation); e != nil {
		return e
	}
	defer p.UpdateAccumulator(exec)
	return nil
}

func (p *PipelineExecutorManager) SetManagerParas(TotalRes int64, timeout time.Duration) {
	b := p.memBucket.(*bucket.Int64bucket)
	if TotalRes != 0 {
		b.SetTotalSource(TotalRes)
	}
	if timeout != 0 {
		b.SetTimeDuration(timeout)
	}
}

func SetPipelineExecutorResourceManagerParas(TotalRes int64, timeout time.Duration) {
	b := pipelineExecutorResourceManager.memBucket
	if TotalRes >= b.GetTotalResource() && b.GetTotalResource() != 0 {
		TotalRes = 0
	}
	if timeout >= b.GetTimeDuration() && b.GetTimeDuration() != 0 {
		timeout = 0
	}
	pipelineExecutorResourceManager.SetManagerParas(TotalRes, timeout)
}

func (p *PipelineExecutorManager) Reset() {
	p.memBucket.Reset()
}

func (p *PipelineExecutorManager) UpdateAccumulator(exec *PipelineExecutor) {
	statistics.ExecutorStat.Memory.Push(exec.info.MemoryOccupation)
	statistics.ExecutorStat.Goroutine.Push(exec.info.ProcessorNum)
}

func (p *PipelineExecutorManager) ReleaseMem(exec *PipelineExecutor) {
	p.memBucket.ReleaseResource(exec.info.MemoryOccupation)
}

func MemoryEstimator(exec *PipelineExecutor) {
	var memCost int64
	var visit func(v *TransformVertex)
	chunkSize := int64(exec.root.node.Schema().Options().ChunkSizeNum())

	visit = func(v *TransformVertex) {
		memCost += NodeMemEstimator(v.node.RowDataType(), chunkSize)
		if len(exec.dag.mapVertexToInfo[v].backwardEdges) == 0 {
			return
		}
		for i := range exec.dag.mapVertexToInfo[v].backwardEdges {
			visit(exec.dag.mapVertexToInfo[v].backwardEdges[i].from)
		}
	}

	visit(exec.root)
	exec.info = &PipelineExecutorInfo{
		MemoryOccupation: memCost,
		ProcessorNum:     int64(len(exec.processors)),
	}
}

func NodeMemEstimator(rt hybridqp.RowDataType, chunkSize int64) int64 {
	var size int64
	for i := range rt.Fields() {
		f, ok := rt.Fields()[i].Expr.(*influxql.VarRef)
		if !ok {
			panic("RowDataType Field Should Be Varef!")
		}
		switch f.Type {
		case influxql.Integer:
			size += chunkSize * DefaultIntegerSize
		case influxql.Float:
			size += chunkSize * DefaultFloatSize
		case influxql.Boolean:
			size += chunkSize * DefaultBooleanSize
		case influxql.String, influxql.Tag:
			size += chunkSize * DefaultStringSize
		}
	}
	//Time column:
	size += chunkSize * DefaultIntegerSize
	return size * CircularChunkNum
}
