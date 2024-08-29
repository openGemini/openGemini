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

package util_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeObject struct {
	err error
}

func (o *closeObject) Close() error {
	return o.err
}

type String string

func (s String) Close() error {
	fmt.Println(111)
	return fmt.Errorf("%s", s)
}

func TestMustClose(t *testing.T) {
	var o *closeObject
	util.MustClose(o)

	o = &closeObject{err: fmt.Errorf("some error")}
	util.MustClose(o)

	var s String
	util.MustClose(s)
}

func TestMustRun(t *testing.T) {
	o := &closeObject{err: fmt.Errorf("some error")}
	util.MustRun(o.Close)

	util.MustRun(nil)
}

func BenchmarkIsObjectNil(b *testing.B) {
	o := &closeObject{err: fmt.Errorf("some error")}
	var s String

	for i := 0; i < b.N; i++ {
		util.IsObjectNil(o)
		util.IsObjectNil(s)
	}
}

func TestZeroToDefault(t *testing.T) {
	vInt := 0
	vUint64 := uint64(0)
	vFloat64 := 0.0
	vString := ""
	vTomlDuration := toml.Duration(0)

	def := util.NewCorrector(0, 0)
	def.Int(&vInt, 1)
	def.Uint64(&vUint64, 1)
	def.Float64(&vFloat64, 1.1)
	def.String(&vString, "a")
	def.TomlDuration(&vTomlDuration, toml.Duration(1))

	assert.Equal(t, vInt, 1)
	assert.Equal(t, vUint64, uint64(1))
	assert.Equal(t, vFloat64, 1.1)
	assert.Equal(t, vString, "a")
	assert.Equal(t, vTomlDuration, toml.Duration(1))
}

func TestCeilToPower2(t *testing.T) {
	assert.Equal(t, uint64(1), util.CeilToPower2(1))
	assert.Equal(t, uint64(2), util.CeilToPower2(2))
	assert.Equal(t, uint64(4), util.CeilToPower2(4))
	assert.Equal(t, uint64(8), util.CeilToPower2(5))
	assert.Equal(t, uint64(16), util.CeilToPower2(9))
	assert.Equal(t, uint64(32), util.CeilToPower2(26))
}

func TestNumberOfTrailingZeros(t *testing.T) {
	assert.Equal(t, 63, util.NumberOfTrailingZeros(0x8000000000000000))
}

func TestIntLimit(t *testing.T) {
	assert.Equal(t, 8, util.IntLimit(8, 64, 0))
	assert.Equal(t, 64, util.IntLimit(8, 64, 66))
	assert.Equal(t, 32, util.IntLimit(8, 64, 32))
}

func TestIntMin(t *testing.T) {
	assert.Equal(t, 5, util.Min(5, 7))
	assert.Equal(t, 5, util.Min(7, 5))
}

func TestAllocSlice(t *testing.T) {
	buf := make([]byte, 0, 30)
	var sub []byte

	size := 20
	buf, sub = util.AllocSlice(buf, size)
	require.Equal(t, size, len(buf))
	require.Equal(t, size, len(sub))

	buf, sub = util.AllocSlice(buf, size)
	require.Equal(t, size*cpu.GetCpuNum(), cap(buf))
	require.Equal(t, size, len(sub))
}

func TestDivisionCeil(t *testing.T) {
	require.Equal(t, 0, util.DivisionCeil(10, 0))
	require.Equal(t, 1, util.DivisionCeil(10, 11))
	require.Equal(t, 4, util.DivisionCeil(10, 3))
	require.Equal(t, 2, util.DivisionCeil(10, 8))
	require.Equal(t, 2, util.DivisionCeil(10, 9))
	require.Equal(t, 1, util.DivisionCeil(10, 10))
}
