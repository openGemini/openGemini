//go:build amd64 || arm64 || 386 || loong64

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

package failpoint_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/failpoint"
	"github.com/stretchr/testify/require"
)

type pointPatchTarget struct {
	base int
}

//go:noinline
func (p *pointPatchTarget) Value(delta int) int {
	return p.base + delta
}

//go:noinline
func (p *pointPatchTarget) privateValue(delta int) int {
	return p.base - delta
}

//go:noinline
func pointPatchFunc(value int) int {
	return 10 + value
}

func TestPointApplyFunc(t *testing.T) {
	point := &failpoint.Point{}
	t.Cleanup(point.ResetPatches)
	require.Equal(t, 12, pointPatchFunc(2))

	point.ApplyFunc(pointPatchFunc, func(value int) int { return 100 + value })
	require.Equal(t, 102, pointPatchFunc(2))
	point.ApplyFunc(pointPatchFunc, func(value int) int { return 200 + value })
	require.Equal(t, 202, pointPatchFunc(2))

	point.ResetPatches()
	require.Equal(t, 12, pointPatchFunc(2))
}

func TestPointApplyMethod(t *testing.T) {
	target := &pointPatchTarget{base: 10}
	point := &failpoint.Point{}
	t.Cleanup(point.ResetPatches)
	require.Equal(t, 12, target.Value(2))

	point.ApplyMethod(target, "Value", func(delta int) int { return 100 + delta })
	require.Equal(t, 102, target.Value(2))
	point.ApplyMethod(target, "Value", func(delta int) int { return 200 + delta })
	require.Equal(t, 202, target.Value(2))

	point.ResetPatches()
	require.Equal(t, 12, target.Value(2))
}

func TestPointApplyPrivateMethod(t *testing.T) {
	target := &pointPatchTarget{base: 10}
	point := &failpoint.Point{}
	t.Cleanup(point.ResetPatches)
	require.Equal(t, 8, target.privateValue(2))

	point.ApplyPrivateMethod(target, "privateValue", func(p *pointPatchTarget, delta int) int {
		return p.base + 100 + delta
	})
	require.Equal(t, 112, target.privateValue(2))
	point.ApplyPrivateMethod(target, "privateValue", func(p *pointPatchTarget, delta int) int {
		return p.base + 200 + delta
	})
	require.Equal(t, 212, target.privateValue(2))

	point.ResetPatches()
	require.Equal(t, 8, target.privateValue(2))
}
