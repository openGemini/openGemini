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

package strings

import (
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

func UnionSlice(s []string) []string {
	if len(s) <= 1 {
		return s
	}
	m := make(map[string]struct{}, len(s))

	for i := 0; i < len(s); i++ {
		m[s[i]] = struct{}{}
	}

	n := 0
	for k := range m {
		s[n] = k
		n++
	}

	return s[:n]
}

func ContainsInterface(s interface{}, sub string) bool {
	ss, ok := s.(string)
	if !ok {
		return false
	}
	return strings.Contains(ss, sub)
}

func EqualInterface(i interface{}, s string) bool {
	ss, ok := i.(string)
	if !ok {
		return false
	}
	return ss == s
}

// SortIsEqual compares if two sorted strings are the equal
func SortIsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

func NewStringBuilder(size int) *StringBuilder {
	return &StringBuilder{buf: make([]byte, 0, size)}
}

func (b *StringBuilder) String() string {
	return *(*string)(unsafe.Pointer(&b.buf))
}

func (b *StringBuilder) Bytes() []byte {
	return b.buf
}

func (b *StringBuilder) Truncate(size int) {
	b.buf = b.buf[:size]
}

func (b *StringBuilder) Size() int {
	return len(b.buf)
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
