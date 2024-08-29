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

package executor

type IteratorParams struct {
	sameInterval bool
	sameTag      bool
	lastChunk    bool
	start        int
	end          int
	chunkLen     int
	err          error
	Table        ReflectionTable
	winIdx       [][2]int
}

func (i *IteratorParams) GetErr() error {
	return i.err
}

type EndPointPair struct {
	Chunk   Chunk
	Ordinal int
}

type IteratorEndpoint struct {
	InputPoint  EndPointPair
	OutputPoint EndPointPair
}

type Iterator interface {
	Next(*IteratorEndpoint, *IteratorParams)
}

type Routine interface {
	WorkOnChunk(Chunk, Chunk, *IteratorParams)
}

type RoutineImpl struct {
	iterator   Iterator
	inOrdinal  int
	outOrdinal int
	endPoint   *IteratorEndpoint
}

func NewRoutineImpl(iterator Iterator, inOrdinal int, outOrdinal int) *RoutineImpl {
	return &RoutineImpl{
		iterator:   iterator,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		endPoint: &IteratorEndpoint{
			InputPoint:  EndPointPair{},
			OutputPoint: EndPointPair{},
		},
	}
}

func (r *RoutineImpl) WorkOnChunk(in Chunk, out Chunk, params *IteratorParams) {
	r.endPoint.InputPoint.Chunk = in
	r.endPoint.InputPoint.Ordinal = r.inOrdinal
	r.endPoint.OutputPoint.Chunk = out
	r.endPoint.OutputPoint.Ordinal = r.outOrdinal
	r.iterator.Next(r.endPoint, params)
}

type CoProcessor interface {
	WorkOnChunk(Chunk, Chunk, *IteratorParams)
}

type CoProcessorImpl struct {
	Routines []Routine
}

func NewCoProcessorImpl(routines ...Routine) *CoProcessorImpl {
	return &CoProcessorImpl{
		Routines: routines,
	}
}

func (p *CoProcessorImpl) AppendRoutine(routines ...Routine) {
	p.Routines = append(p.Routines, routines...)
}

func (p *CoProcessorImpl) WorkOnChunk(in Chunk, out Chunk, params *IteratorParams) {
	for _, r := range p.Routines {
		r.WorkOnChunk(in, out, params)
	}
}

type WideRoutineImpl struct {
	iterator Iterator
	endPoint *IteratorEndpoint
}

func NewWideRoutineImpl(iterator Iterator) *WideRoutineImpl {
	return &WideRoutineImpl{
		iterator: iterator,
		endPoint: &IteratorEndpoint{
			InputPoint:  EndPointPair{},
			OutputPoint: EndPointPair{},
		},
	}
}

func (r *WideRoutineImpl) WorkOnChunk(in Chunk, out Chunk, params *IteratorParams) {
	r.endPoint.InputPoint.Chunk = in
	r.endPoint.OutputPoint.Chunk = out
	r.iterator.Next(r.endPoint, params)
}

type WideCoProcessorImpl struct {
	Routine *WideRoutineImpl
}

func NewWideCoProcessorImpl(routine *WideRoutineImpl) *WideCoProcessorImpl {
	return &WideCoProcessorImpl{
		Routine: routine,
	}
}

func (p *WideCoProcessorImpl) WorkOnChunk(in Chunk, out Chunk, params *IteratorParams) {
	p.Routine.WorkOnChunk(in, out, params)
}
