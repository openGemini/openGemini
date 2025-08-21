// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
// nolint
package statistics

import (
	"runtime"
	"time"
)

const (
	RuntimeCollectInterval = 6 // 6*10*time.Second
)

var rt = &Runtime{}

func init() {
	NewCollector().Register(rt)
}

func RuntimeIns() *Runtime {
	rt.enabled = true
	return rt
}

type Runtime struct {
	BaseCollector
	Sys               *ItemInt64
	Alloc             *ItemInt64
	HeapAlloc         *ItemInt64
	HeapSys           *ItemInt64
	HeapInUse         *ItemInt64
	HeapIdle          *ItemInt64
	HeapObjects       *ItemInt64
	HeapReleased      *ItemInt64
	TotalAlloc        *ItemInt64
	Lookups           *ItemInt64
	Mallocs           *ItemInt64
	Frees             *ItemInt64
	PauseTotalNs      *ItemInt64
	NumGC             *ItemInt64
	NumGoroutine      *ItemInt64
	Version           *ItemString
	SpdyCertExpireAt  *ItemString
	HttpsCertExpireAt *ItemString
}

func (r *Runtime) BeforeCollect() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	r.Alloc.Store(int64(stats.Alloc))
	r.HeapAlloc.Store(int64(stats.HeapAlloc))
	r.HeapSys.Store(int64(stats.HeapSys))
	r.HeapInUse.Store(int64(stats.HeapInuse))
	r.HeapIdle.Store(int64(stats.HeapIdle))
	r.HeapObjects.Store(int64(stats.HeapObjects))
	r.HeapReleased.Store(int64(stats.HeapReleased))
	r.TotalAlloc.Store(int64(stats.TotalAlloc))
	r.Lookups.Store(int64(stats.Lookups))
	r.Mallocs.Store(int64(stats.Mallocs))
	r.Frees.Store(int64(stats.Frees))
	r.PauseTotalNs.Store(int64(stats.PauseTotalNs))
	r.NumGC.Store(int64(stats.NumGC))
	r.NumGoroutine.Store(int64(runtime.NumGoroutine()))
}

func (r *Runtime) Interval() int {
	return RuntimeCollectInterval
}

func (r *Runtime) SetVersion(v string) {
	r.Version.Store(v)
}

func (r *Runtime) SetHttpsCertExpireAt(expire time.Time) {
	r.HttpsCertExpireAt.Store(expire.Format(time.DateTime))
}

func (r *Runtime) SetSpdyCertExpireAt(expire time.Time) {
	r.SpdyCertExpireAt.Store(expire.Format(time.DateTime))
}
