//go:build failpoint

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
	"time"

	"github.com/spf13/cast"
)

func Call(name string, fn func(val *Value)) {
	name = RewriteFailPointName(name, fn)
	p, ok := points.GetPoint(name)
	if !ok {
		return
	}

	fn(&Value{val: p.Value()})
}

func Sleep(name string, fn func()) {
	name = RewriteFailPointName(name, fn)
	p, ok := points.GetPoint(name)
	if !ok || p.Value() == nil {
		return
	}

	dur := cast.ToDuration(p.Value().Load())
	if dur > 0 {
		time.Sleep(dur)
	}
}

func ApplyFunc(name string, target, double any) {
	name = RewriteFailPointName(name, double)
	p, ok := points.GetPoint(name)
	if ok {
		p.ApplyFunc(target, double)
	}
}

func ApplyMethod(name string, target any, method string, double any) {
	name = RewriteFailPointName(name, double)
	p, ok := points.GetPoint(name)
	if ok {
		p.ApplyMethod(target, method, double)
	}
}

func ApplyPrivateMethod(name string, target any, method string, double any) {
	name = RewriteFailPointName(name, double)
	p, ok := points.GetPoint(name)
	if ok {
		p.ApplyPrivateMethod(target, method, double)
	}
}
