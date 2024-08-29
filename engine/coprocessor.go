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

package engine

import (
	"github.com/openGemini/openGemini/lib/record"
)

type ReducerParams struct {
	multiCall     bool
	sameWindow    bool
	lastRec       bool
	firstStep     int64
	lastStep      int64
	step          int64
	offset        int64
	rangeDuration int64
	lookBackDelta int64
	intervalIndex []uint16
}

type EndPointPair struct {
	Record  *record.Record
	Ordinal int
}

type ReducerEndpoint struct {
	InputPoint  EndPointPair
	OutputPoint EndPointPair
}

type Reducer interface {
	Aggregate(*ReducerEndpoint, *ReducerParams)
}

type Routine interface {
	WorkOnRecord(*record.Record, *record.Record, *ReducerParams)
}

type RoutineImpl struct {
	reducer    Reducer
	inOrdinal  int
	outOrdinal int
	endPoint   *ReducerEndpoint
}

func NewRoutineImpl(reducer Reducer, inOrdinal int, outOrdinal int) *RoutineImpl {
	return &RoutineImpl{
		reducer:    reducer,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		endPoint: &ReducerEndpoint{
			InputPoint:  EndPointPair{},
			OutputPoint: EndPointPair{},
		},
	}
}

func (r *RoutineImpl) WorkOnRecord(in *record.Record, out *record.Record, params *ReducerParams) {
	r.endPoint.InputPoint.Record = in
	r.endPoint.InputPoint.Ordinal = r.inOrdinal
	r.endPoint.OutputPoint.Record = out
	r.endPoint.OutputPoint.Ordinal = r.outOrdinal
	r.reducer.Aggregate(r.endPoint, params)
}

type CoProcessor interface {
	WorkOnRecord(*record.Record, *record.Record, *ReducerParams)
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

func (p *CoProcessorImpl) WorkOnRecord(in *record.Record, out *record.Record, params *ReducerParams) {
	for _, r := range p.Routines {
		r.WorkOnRecord(in, out, params)
	}
}
