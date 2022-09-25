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

package hybridqp

import (
	"sync/atomic"
)

type SelectDuration struct {
	TotalDuration    int64
	PrepareDuration  int64
	IteratorDuration int64
	LocalDuration    ShardSelectDuration
	RemoteDuration   ShardSelectDuration
	EmitDuration     int64
	Query            string
	DB               string
	QueryBatch       int
}

func (s *SelectDuration) Duration(name string, d int64) {
	if s == nil {
		return
	}
	switch name {
	case "TotalDuration":
		td := atomic.LoadInt64(&s.TotalDuration)
		if d > td {
			atomic.StoreInt64(&s.TotalDuration, d)
		}
	case "PrepareDuration":
		td := atomic.LoadInt64(&s.PrepareDuration)
		if d > td {
			atomic.StoreInt64(&s.PrepareDuration, d)
		}
	case "IteratorDuration":
		td := atomic.LoadInt64(&s.IteratorDuration)
		if d > td {
			atomic.StoreInt64(&s.IteratorDuration, d)
		}
	case "LocalIteratorDuration":
		td := atomic.LoadInt64(&s.LocalDuration.IteratorDuration)
		if d > td {
			atomic.StoreInt64(&s.LocalDuration.IteratorDuration, d)
		}
	case "LocalTagSetDuration":
		td := atomic.LoadInt64(&s.LocalDuration.TagSetDuration)
		if d > td {
			atomic.StoreInt64(&s.LocalDuration.TagSetDuration, d)
		}
	case "LocalLocationDuration":
		td := atomic.LoadInt64(&s.LocalDuration.LocationDuration)
		if d > td {
			atomic.StoreInt64(&s.LocalDuration.LocationDuration, d)
		}
	case "RemoteIteratorDuration":
		td := atomic.LoadInt64(&s.RemoteDuration.IteratorDuration)
		if d > td {
			atomic.StoreInt64(&s.RemoteDuration.IteratorDuration, d)
		}
	case "RemoteTagSetDuration":
		td := atomic.LoadInt64(&s.RemoteDuration.TagSetDuration)
		if d > td {
			atomic.StoreInt64(&s.RemoteDuration.TagSetDuration, d)
		}
	case "RemoteLocationDuration":
		td := atomic.LoadInt64(&s.RemoteDuration.LocationDuration)
		if d > td {
			atomic.StoreInt64(&s.RemoteDuration.LocationDuration, d)
		}
	case "EmitDuration":
		td := atomic.LoadInt64(&s.EmitDuration)
		if d > td {
			atomic.StoreInt64(&s.EmitDuration, d)
		}
	}
}

func (s *SelectDuration) SetQuery(q string) {
	if s != nil {
		s.Query = q
	}
}

func (s *SelectDuration) SetQueryBatch(n int) {
	if s != nil {
		s.QueryBatch = n
	}
}

func (s *SelectDuration) SetDatabase(db string) {
	if s != nil {
		s.DB = db
	}
}

type ShardSelectDuration struct {
	IteratorDuration int64
	TagSetDuration   int64
	LocationDuration int64
}

func (r *ShardSelectDuration) Duration(name string, d int64) {
	if r == nil {
		return
	}

	switch name {
	case "IteratorDuration":
		td := atomic.LoadInt64(&r.IteratorDuration)
		if d > td {
			atomic.StoreInt64(&r.IteratorDuration, d)
		}
	case "TagSetDuration":
		td := atomic.LoadInt64(&r.TagSetDuration)
		if d > td {
			atomic.StoreInt64(&r.TagSetDuration, d)
		}
	case "LocationDuration":
		td := atomic.LoadInt64(&r.LocationDuration)
		if d > td {
			atomic.StoreInt64(&r.LocationDuration, d)
		}
	}
}
