// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package util

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/cpu"
)

type TimerPool struct {
	sp    sync.Pool
	cache chan *time.Timer
}

func NewTimePool() *TimerPool {
	p := &TimerPool{
		sp: sync.Pool{
			New: func() interface{} {
				timer := time.NewTimer(time.Hour * 1e6)
				timer.Stop()
				return timer
			},
		},
		cache: make(chan *time.Timer, cpu.GetCpuNum()*2),
	}
	return p
}

func (p *TimerPool) GetTimer(timeout time.Duration) *time.Timer {
	select {
	case timer := <-p.cache:
		timer.Reset(timeout)
		return timer
	default:
		timer, ok := p.sp.Get().(*time.Timer)
		if !ok {
			timer = time.NewTimer(timeout)
		}
		timer.Reset(timeout)
		return timer
	}
}

func (p *TimerPool) PutTimer(timer *time.Timer) {
	timer.Stop()
	select {
	case <-timer.C:
	default:
	}
	select {
	case p.cache <- timer:
	default:
		p.sp.Put(timer)
	}
}
