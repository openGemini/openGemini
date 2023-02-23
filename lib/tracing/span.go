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

package tracing

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/pkg/tracing/fields"
)

const (
	nameValuePrefix = "__name__"
)

type Span struct {
	span  *tracing.Span
	trace *Trace

	mu       sync.Mutex
	pp       bool
	start    time.Time
	elapsed  int64
	counters map[string]*SpanCounter
}

func (s *Span) AppendNameValue(key string, val interface{}) {
	s.span.MergeFields(fields.String(nameValuePrefix+key, fmt.Sprintf("%s=%v", key, val)))
}

func (s *Span) SetNameValue(val string) {
	s.span.MergeFields(fields.String(nameValuePrefix+val, fmt.Sprintf("%v", val)))
}

func (s *Span) AddStringField(key, val string) {
	s.span.MergeFields(fields.String(key, val))
}

func (s *Span) AddIntField(key string, val int) {
	s.span.MergeFields(fields.Int64(key, int64(val)))
}

func (s *Span) AddIntFields(key []string, val []int) {
	if len(key) != len(val) {
		return
	}
	fs := make(fields.Fields, len(key))
	for i := range key {
		fs[i] = fields.Int64(key[i], int64(val[i]))
	}
	s.span.MergeFields(fs...)
}

func (s *Span) StartSpan(name string, opt ...tracing.StartSpanOption) *Span {
	span := s.span.StartSpan(name, opt...)

	return &Span{span: span, trace: s.trace}
}

func (s *Span) Context() tracing.SpanContext {
	return s.span.Context()
}

func (s *Span) SetLabels(args ...string) {
	s.span.SetLabels(args...)
}

func (s *Span) MergeLabels(args ...string) {
	s.span.MergeLabels(args...)
}

func (s *Span) SetFields(set fields.Fields) {
	s.span.SetFields(set)
}

func (s *Span) MergeFields(args ...fields.Field) {
	s.span.MergeFields(args...)
}

func (s *Span) StartPP() *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pp {
		return s
	}

	s.pp = true
	s.start = time.Now()
	s.elapsed++
	return s
}

func (s *Span) EndPP() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.pp {
		return
	}
	s.pp = false
	s.elapsed += time.Since(s.start).Nanoseconds()
}

func (s *Span) Finish() {
	s.EndPP()
	if s.elapsed > 0 {
		s.SetNameValue("pp=" + time.Duration(s.elapsed).String())
	}

	if s.counters != nil {
		for _, item := range s.counters {
			s.AddStringField(item.Name(), item.Value())
		}
	}

	s.trace.mu.Lock()
	s.span.Finish()
	s.trace.mu.Unlock()
}

func (s *Span) CreateCounter(name string, unit string) *SpanCounter {
	if s.counters == nil {
		s.counters = make(map[string]*SpanCounter, 2)
	}
	if counter, ok := s.counters[name]; ok {
		return counter
	}

	counter := &SpanCounter{0, name, unit}
	s.counters[name] = counter
	return counter
}

func (s *Span) Count(name string, x int64) {
	counter, ok := s.counters[name]
	if !ok {
		return
	}
	atomic.AddInt64(&counter.val, x)
}

type SpanCounter struct {
	val  int64
	name string
	unit string
}

func (c *SpanCounter) Name() string {
	return c.name
}

func (c *SpanCounter) Value() string {
	s := fmt.Sprintf("%d", c.val)
	if c.unit != "" {
		s += " " + c.unit
	}
	return s
}

func SpanElapsed(span *Span, fn func()) {
	if span != nil {
		span.StartPP()
		defer span.EndPP()
	}

	fn()
}

func Start(span *Span, name string, withPP bool) *Span {
	if span == nil {
		return nil
	}
	ret := span.StartSpan(name)
	if withPP {
		ret.StartPP()
	}
	return ret
}

func Finish(spans ...*Span) {
	for _, span := range spans {
		if span == nil {
			continue
		}
		span.Finish()
	}
}

func StartPP(span *Span) {
	if span != nil {
		span.StartPP()
	}
}

func EndPP(span *Span) {
	if span != nil {
		span.EndPP()
	}
}

func AddPP(span *Span, begin time.Time) {
	if span == nil {
		return
	}

	atomic.AddInt64(&span.elapsed, time.Since(begin).Nanoseconds())
}
