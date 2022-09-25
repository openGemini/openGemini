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

package statistics

import (
	"sync/atomic"
	"time"
)

type NamedAccumulator interface {
	NamedCount() (string, interface{})
	NamedSum() (string, interface{})
	NamedLast() (string, interface{})
}
type Accumulator interface {
	Push(interface{})
	Increase()
	Decrease()
	Count() interface{}
	Sum() interface{}
	Last() interface{}
}

type Int64Accumulator struct {
	value int64
	count int64
	sum   int64
}

func (a *Int64Accumulator) Push(data interface{}) {
	value := data.(int64)
	atomic.StoreInt64(&a.value, value)
	atomic.AddInt64(&a.count, 1)
	atomic.AddInt64(&a.sum, value)
}

func (a *Int64Accumulator) Increase() {
	value := atomic.AddInt64(&a.value, 1)
	atomic.AddInt64(&a.count, 1)
	atomic.AddInt64(&a.sum, value)
}

func (a *Int64Accumulator) Decrease() {
	value := atomic.AddInt64(&a.value, -1)
	atomic.AddInt64(&a.count, 1)
	atomic.AddInt64(&a.sum, value)
}

func (a *Int64Accumulator) Count() interface{} {
	return atomic.LoadInt64(&a.count)
}

func (a *Int64Accumulator) Sum() interface{} {
	return atomic.LoadInt64(&a.sum)
}

func (a *Int64Accumulator) Last() interface{} {
	return atomic.LoadInt64(&a.value)
}

const (
	UNDERLINE  = "_"
	COUNT_NAME = "count"
	SUM_NAME   = "sum"
	LAST_NAME  = "last"
)

type NamedInt64Accumulator struct {
	Int64Accumulator
	name string
}

func NewNamedInt64Accumulator(name string) *NamedInt64Accumulator {
	return &NamedInt64Accumulator{
		name: name,
	}
}

func (n *NamedInt64Accumulator) NamedCount() (string, interface{}) {
	return n.name + UNDERLINE + COUNT_NAME, n.Count()
}

func (n *NamedInt64Accumulator) NamedSum() (string, interface{}) {
	return n.name + UNDERLINE + SUM_NAME, n.Sum()
}

func (n *NamedInt64Accumulator) NamedLast() (string, interface{}) {
	return n.name + UNDERLINE + LAST_NAME, n.Last()
}

type StatisticTimer struct {
	accumulator Accumulator
	begin       time.Time
}

func NewStatisticTimer(accu Accumulator) *StatisticTimer {
	return &StatisticTimer{
		accumulator: accu,
	}
}

func (st *StatisticTimer) Begin() {
	st.begin = time.Now()
}

func (st *StatisticTimer) End() {
	st.accumulator.Push(time.Since(st.begin).Nanoseconds())
}
