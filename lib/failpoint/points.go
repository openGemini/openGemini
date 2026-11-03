// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package failpoint

import (
	"sync"
	"sync/atomic"
)

var points = NewPoints()

type Point struct {
	mu      sync.Mutex
	val     *atomic.Value
	patches patches
}

func (p *Point) StoreValue(val any) {
	if val == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.val == nil {
		p.val = &atomic.Value{}
	}
	p.val.Store(val)
}

func (p *Point) Value() *atomic.Value {
	return p.val
}

type Points struct {
	mu     sync.RWMutex
	points map[string]*Point
}

func NewPoints() *Points {
	return &Points{points: make(map[string]*Point)}
}

func (ps *Points) Enable(name string, val any) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	point, ok := ps.points[name]
	if !ok {
		point = &Point{}
		ps.points[name] = point
	}
	point.StoreValue(val)
}

func (ps *Points) Disable(name string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	point, ok := ps.points[name]
	if ok {
		point.ResetPatches()
		delete(ps.points, name)
	}
}

func (ps *Points) GetPoint(name string) (*Point, bool) {
	ps.mu.RLock()
	point, ok := ps.points[name]
	ps.mu.RUnlock()
	return point, ok
}
