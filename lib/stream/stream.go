/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package stream

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	atomic2 "github.com/openGemini/openGemini/lib/atomic"
)

type FieldCalls []*FieldCall

func (f FieldCalls) Len() int {
	return len(f)
}

func (f FieldCalls) Less(i, j int) bool {
	return f[i].Alias < f[j].Alias
}

func (f FieldCalls) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

type FieldCall struct {
	Name             string
	Alias            string
	Call             string
	InFieldType      int32
	OutFieldType     int32
	ConcurrencyFunc  func(*float64, float64) float64
	SingleThreadFunc func(float64, float64) float64
}

func NewFieldCall(inFieldType, outFieldType int32, name, alias, call string, concurrency bool) (*FieldCall, error) {
	fieldCall := &FieldCall{
		InFieldType:  inFieldType,
		OutFieldType: outFieldType,
		Name:         name,
		Alias:        alias,
		Call:         call,
	}
	if concurrency {
		err := BuildConcurrencyFunc(fieldCall)
		if err != nil {
			return nil, err
		}
	} else {
		err := BuildSingleThreadFunc(fieldCall)
		if err != nil {
			return nil, err
		}
	}
	return fieldCall, nil
}

func BuildConcurrencyFunc(fieldCall *FieldCall) error {
	switch fieldCall.Call {
	case "min":
		fieldCall.ConcurrencyFunc = atomic2.CompareAndSwapMinFloat64
	case "max":
		fieldCall.ConcurrencyFunc = atomic2.CompareAndSwapMaxFloat64
	case "sum":
		fieldCall.ConcurrencyFunc = atomic2.AddFloat64
	case "count":
		fieldCall.ConcurrencyFunc = atomic2.AddFloat64
	default:
		return fmt.Errorf("not support stream func %v", fieldCall.Call)
	}
	return nil
}

func BuildSingleThreadFunc(fieldCall *FieldCall) error {
	switch fieldCall.Call {
	case "min":
		fieldCall.SingleThreadFunc = func(f float64, f2 float64) float64 {
			if f > f2 {
				return f2
			}
			return f
		}
	case "max":
		fieldCall.SingleThreadFunc = func(f float64, f2 float64) float64 {
			if f < f2 {
				return f2
			}
			return f
		}
	case "sum":
		fieldCall.SingleThreadFunc = func(f float64, f2 float64) float64 {
			return f + f2
		}
	case "count":
		fieldCall.SingleThreadFunc = func(f float64, f2 float64) float64 {
			return f + f2
		}
	default:
		return fmt.Errorf("not support stream func %v", fieldCall.Call)
	}
	return nil
}

func NewBuilderPool() *BuilderPool {
	p := &BuilderPool{}
	return p
}

type BuilderPool struct {
	pool   sync.Pool
	size   int64
	length int64
}

func (p *BuilderPool) Get() *StringBuilder {
	c := p.pool.Get()
	if c == nil {
		atomic.AddInt64(&p.size, 1)
		return &StringBuilder{}
	}
	atomic.AddInt64(&p.length, -1)
	return c.(*StringBuilder)
}

func (p *BuilderPool) Put(r *StringBuilder) {
	p.pool.Put(r)
	atomic.AddInt64(&p.length, 1)
}

func (p *BuilderPool) Len() int64 {
	return atomic.LoadInt64(&p.length)
}

func (p *BuilderPool) Size() int64 {
	return atomic.LoadInt64(&p.size)
}

type StringBuilder struct {
	buf []byte
}

func (b *StringBuilder) String() string {
	return *(*string)(unsafe.Pointer(&b.buf))
}

func (b *StringBuilder) NewString() string {
	if len(b.buf) == 0 {
		return ""
	}
	s := make([]byte, len(b.buf))
	copy(s, b.buf)
	return *(*string)(unsafe.Pointer(&s))
}

func (b *StringBuilder) Reset() {
	b.buf = b.buf[:0]
}

func (b *StringBuilder) AppendByte(c byte) {
	b.buf = append(b.buf, c)
}

func (b *StringBuilder) AppendString(s string) {
	b.buf = append(b.buf, s...)
}
