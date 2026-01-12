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

package tracing

import (
	"sync"

	"github.com/influxdata/influxdb/pkg/tracing"
)

type Trace struct {
	trace *tracing.Trace
	subs  map[uint64]*Trace
	mu    sync.RWMutex
}

func NewTrace(name string, opt ...tracing.StartSpanOption) (*Trace, *Span) {
	t, s := tracing.NewTrace(name, opt...)
	trace := &Trace{trace: t, subs: make(map[uint64]*Trace)}
	span := &Span{span: s, trace: trace}
	return trace, span
}

func (t *Trace) MarshalBinary() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.trace.MarshalBinary()
}

func (t *Trace) UnmarshalBinary(data []byte) error {
	return t.trace.UnmarshalBinary(data)
}

func (t *Trace) AddSub(trace *Trace, span *Span) {
	t.mu.Lock()
	t.subs[span.Context().SpanID] = trace
	t.mu.Unlock()
}

func (t *Trace) Tree() *tracing.TreeNode {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.trace.Tree()
}

func (t *Trace) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tree := t.trace.Tree()
	if len(t.subs) != 0 {
		mv := newMergeVisitor(t.subs)
		tracing.Walk(mv, tree)
	}

	tv := newTreeVisitor()
	tracing.Walk(tv, tree)
	return tv.root.String()
}
