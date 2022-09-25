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
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
)

const (
	INVALID_NUMBER int = -1
)

const (
	PORT_CHAN_SIZE int = 1
)

var SkipDefault = interface{}(0)

type Port interface {
	Equal(to Port) bool
	Connect(to Port)
	Redirect(to Port)
	ConnectionId() uintptr
	Close()
	Release()
}

type Ports []Port

func (ports Ports) Close() {
	for _, port := range ports {
		port.Close()
	}
}

func (ports Ports) Release() {
	for _, port := range ports {
		port.Release()
	}
}

type ChunkPort struct {
	RowDataType hybridqp.RowDataType
	State       chan Chunk
	OrigiState  chan Chunk
	Redirected  bool
	once        *sync.Once
}

func NewChunkPort(rowDataType hybridqp.RowDataType) *ChunkPort {
	return &ChunkPort{
		RowDataType: rowDataType,
		State:       nil,
		OrigiState:  nil,
		Redirected:  false,
		once:        new(sync.Once),
	}
}

func (p *ChunkPort) Equal(to Port) bool {
	return p.RowDataType.Equal(to.(*ChunkPort).RowDataType)
}

func (p *ChunkPort) Connect(to Port) {
	p.State = make(chan Chunk, PORT_CHAN_SIZE)
	to.(*ChunkPort).State = p.State
}

func (p *ChunkPort) Redirect(to Port) {
	if to.(*ChunkPort).State == nil {
		panic("redirect port to nil")
	}

	if !p.Redirected {
		p.Redirected = true
		p.OrigiState = p.State
	}

	p.State = to.(*ChunkPort).State
}

func (p *ChunkPort) ConnectionId() uintptr {
	id := *(*uintptr)(unsafe.Pointer(&p.State))

	if id == 0 {
		panic("Obtain connection id from unconnected arrow port")
	}

	return id
}

func (p *ChunkPort) Close() {
	p.once.Do(func() {
		if p.Redirected {
			p.State = p.OrigiState
		}
		if p.State != nil {
			close(p.State)
		}
	})
}

func (p *ChunkPort) Release() {}

func Connect(from Port, to Port) error {
	if !from.Equal(to) {
		return fmt.Errorf("Can't connect different ports(%v and %v)", from, to)
	}

	from.Connect(to)

	return nil
}

type ChunkPorts []*ChunkPort

func (ps ChunkPorts) Close() {
	for _, p := range ps {
		p.Close()
	}
}

type Processor interface {
	Work(ctx context.Context) error
	Close()
	Release() error
	Name() string
	GetOutputs() Ports
	GetInputs() Ports
	GetOutputNumber(port Port) int
	GetInputNumber(port Port) int
	IsSink() bool
	Explain() []ValuePair
	Analyze(span *tracing.Span)
	StartSpan(name string, withPP bool) *tracing.Span
	FinishSpan()
}

type BaseProcessor struct {
	span  *tracing.Span
	begin time.Time
	once  *sync.Once
}

func (bp *BaseProcessor) IsSink() bool {
	return false
}

func (bp *BaseProcessor) Analyze(span *tracing.Span) {
	bp.begin = time.Now()
	bp.span = span
}

func (bp *BaseProcessor) StartSpan(name string, withPP bool) *tracing.Span {
	if bp.span == nil {
		return nil
	}
	span := bp.span.StartSpan(name)
	if withPP {
		span.StartPP()
	}
	return span
}

func (bp *BaseProcessor) FinishSpan() {
	if bp.span == nil {
		return
	}

	bp.span.Finish()
}

func (bp *BaseProcessor) BaseSpan() *tracing.Span {
	return bp.span
}

func (bp *BaseProcessor) Once(fn func()) {
	if bp.once == nil {
		fn()
		return
	}
	bp.once.Do(fn)
}

func (bp *BaseProcessor) InitOnce() {
	bp.once = new(sync.Once)
}

func (bp *BaseProcessor) Release() error {
	return nil
}

type Processors []Processor

func (ps *Processors) Push(p Processor) {
	*ps = append(*ps, p)
}

func (ps *Processors) Pop() Processor {
	p := (*ps)[len(*ps)-1]
	*ps = (*ps)[:len(*ps)-1]
	return p
}

func (ps Processors) Peek() Processor {
	return ps[len(ps)-1]
}

func (ps Processors) Size() int {
	return len(ps)
}

func (ps Processors) Empty() bool {
	return len(ps) <= 0
}

func (ps Processors) Close() {
	for _, p := range ps {
		p.Close()
	}
}
