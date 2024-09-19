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

package bufferpool

import (
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/util/lifted/bytebufferpool"
)

const (
	maxDefaultSize               = 1024 * 1024 // 1M
	minDefaultSize               = 64
	MaxLocalCacheLen             = 8
	MaxChunkMetaBufLocalCacheLen = 1024
	MaxLocalCacheSize            = 32 * 1024 * 1024 // 32M
)

type Pool struct {
	defaultSize uint64
	pool        bytebufferpool.Pool
	localCache  chan []byte
}

var defaultPool = NewByteBufferPool(0, cpu.GetCpuNum(), MaxLocalCacheLen)

func NewByteBufferPool(defaultSize uint64, localCacheNum int, maxCacheLen int) *Pool {
	if defaultSize > maxDefaultSize {
		defaultSize = maxDefaultSize
	}
	if defaultSize < minDefaultSize {
		defaultSize = minDefaultSize
	}
	var n int
	if localCacheNum == 0 {
		n = cpu.GetCpuNum()
	} else {
		n = localCacheNum
	}
	if n > maxCacheLen {
		n = maxCacheLen
	}
	return &Pool{defaultSize: defaultSize, localCache: make(chan []byte, n)}
}

func Get() []byte {
	return defaultPool.Get()
}

func Put(b []byte) {
	defaultPool.Put(b)
}

func (p *Pool) Get() []byte {
	select {
	case bb := <-p.localCache:
		return bb
	default:
		v := p.pool.Get()
		if v != nil {
			return v.Bytes()
		}
		return make([]byte, 0, atomic.LoadUint64(&p.defaultSize))
	}
}

func (p *Pool) Put(b []byte) {
	b = b[:0]
	if cap(b) > MaxLocalCacheSize {
		p.pool.Put(&bytebufferpool.ByteBuffer{B: b})
		return
	}

	select {
	case p.localCache <- b:
	default:
		p.pool.Put(&bytebufferpool.ByteBuffer{B: b})
	}
}

func Resize(b []byte, n int) []byte {
	if nn := n - cap(b); nn > 0 {
		b = append(b[:cap(b)], make([]byte, nn)...)
	}
	return b[:n]
}
