/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package engine

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/smartystreets/goconvey/convey"
	assert2 "github.com/stretchr/testify/assert"
)

func TestIsCompactMemUsageLimit(t *testing.T) {
	old := compactMemUsageLimit
	defer func() {
		compactMemUsageLimit = old
	}()

	SetCompactMemUsageLimit(100)
	t.Run("threshold disable", func(t *testing.T) {
		got := IsCompactMemUsageExceeded()
		assert2.False(t, got)
	})

	SetCompactMemUsageLimit(80)
	t.Run("below threshold", func(t *testing.T) {
		convey.Convey("mock MemUsedPct", t, func() {
			m := memory.GetMemMonitor()
			p := gomonkey.ApplyMethodReturn(m, "MemUsedPct", 60.0)
			defer p.Reset()

			got := IsCompactMemUsageExceeded()
			assert2.False(t, got)
		})
	})
	t.Run("exceed threshold", func(t *testing.T) {
		t.Skip()
		convey.Convey("mock MemUsedPct", t, func() {
			m := memory.GetMemMonitor()
			p := gomonkey.ApplyMethodReturn(m, "MemUsedPct", 85.0)
			defer p.Reset()

			got := IsCompactMemUsageExceeded()
			assert2.True(t, got)
		})
	})
}

func TestCompactor_Compact(t *testing.T) {
	c := &Compactor{sources: map[uint64]*shard{1: nil}}
	convey.Convey("exceed mem usage", t, func() {
		p := gomonkey.ApplyFuncReturn(IsCompactMemUsageExceeded, true)
		defer p.Reset()
		assert2.NotPanics(t, c.compact)
	})
}
