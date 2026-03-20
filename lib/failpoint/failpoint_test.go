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

package failpoint_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/failpoint"
	"github.com/stretchr/testify/require"
)

func TestCall(t *testing.T) {
	v := 1
	name := "test-call"
	fullName := failpoint.RewriteFailPointName(name, func() {})

	var call = func() {
		failpoint.Call(name, func(val *failpoint.Value) {
			v = val.Int()
		})
	}

	call()
	require.Equal(t, v, 1)

	failpoint.Enable(fullName, 100)
	defer failpoint.Disable(fullName)

	call()
	require.Equal(t, v, 100)
}

func TestValue(t *testing.T) {
	name := "test-value"

	var run = func(v any, comp func(val *failpoint.Value) bool) {
		fullName := failpoint.RewriteFailPointName(name, func() {})
		failpoint.Enable(fullName, v)
		defer failpoint.Disable(fullName)

		val := failpoint.GetValue(name, func() {})
		require.True(t, comp(val))
	}

	run(100.1, func(val *failpoint.Value) bool {
		return val.Float64() == 100.1
	})
	run(100, func(val *failpoint.Value) bool {
		return val.Int() == 100
	})
	run("foo", func(val *failpoint.Value) bool {
		return val.String() == "foo"
	})
	run(true, func(val *failpoint.Value) bool {
		return val.Bool() == true
	})

	require.Empty(t, failpoint.GetValue(name, func() {}))

	emptyVal := &failpoint.Value{}
	require.Equal(t, "", emptyVal.String())
	require.Equal(t, 0, emptyVal.Int())
	require.Equal(t, 0.0, emptyVal.Float64())
	require.Equal(t, false, emptyVal.Bool())
}

func TestRewriteFailPointName(t *testing.T) {
	name := "test"
	fullName := failpoint.RewriteFailPointName(name, func() {})

	require.Equal(t, "github.com/openGemini/openGemini/lib/failpoint_test/test", fullName)
}

func TestFetchValue(t *testing.T) {
	var fetch = func(val, exp string) {
		require.Equal(t, exp, failpoint.FetchValue(val))
	}

	fetch("Sleep(10)", "10")
	fetch("term=Sleep(11)", "11")
	fetch("return(12)", "12")
	fetch("term=RETURN(13)", "13")
	fetch("RETURN_A(13)", "RETURN_A(13)")
}

func TestSleep(t *testing.T) {
	name := "test-value"
	fullName := failpoint.RewriteFailPointName(name, func() {})
	failpoint.Enable(fullName, "Sleep(1s)")

	start := time.Now()
	failpoint.Sleep(name, func() {})
	require.True(t, time.Since(start).Seconds() >= 1)
}
