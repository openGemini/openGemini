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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

const (
	N = 10
)

func TestInt64Accumulator(t *testing.T) {
	expectCount := int64(N)
	expectSum := int64(45)
	expectLast := int64(9)
	accumulator := statistics.Int64Accumulator{}
	for i := 0; i < N; i++ {
		accumulator.Push(int64(i))
	}

	if accumulator.Count() != expectCount {
		t.Errorf("expect count: %d, but %d", expectCount, accumulator.Count())
	}

	if accumulator.Sum() != expectSum {
		t.Errorf("expect sum: %d, but %d", expectSum, accumulator.Sum())
	}

	if accumulator.Last() != expectLast {
		t.Errorf("expect last: %d, but %d", expectLast, accumulator.Last())
	}
}

func TestPushNamedInt64Accumulator(t *testing.T) {
	expectCount := int64(N)
	expectSum := int64(45)
	expectLast := int64(9)
	expectName := "accumulator"
	accumulator := statistics.NewNamedInt64Accumulator(expectName)
	for i := 0; i < N; i++ {
		accumulator.Push(int64(i))
	}

	if accumulator.Count() != expectCount {
		t.Errorf("expect count: %d, but %d", expectCount, accumulator.Count())
	}

	if accumulator.Sum() != expectSum {
		t.Errorf("expect sum: %d, but %d", expectSum, accumulator.Sum())
	}

	if accumulator.Last() != expectLast {
		t.Errorf("expect last: %d, but %d", expectLast, accumulator.Last())
	}

	if name, value := accumulator.NamedCount(); name != expectName+"_count" || value != expectCount {
		t.Errorf("expect name and value of count: %s,%d, but %s,%d", expectName, expectCount, name, value)
	}

	if name, value := accumulator.NamedSum(); name != expectName+"_sum" || value != expectSum {
		t.Errorf("expect name and value of sum: %s,%d, but %s,%d", expectName, expectSum, name, value)
	}

	if name, value := accumulator.NamedLast(); name != expectName+"_last" || value != expectLast {
		t.Errorf("expect name and value of last: %s,%d, but %s,%d", expectName, expectLast, name, value)
	}
}

func TestIncAndDecNamedInt64Accumulator(t *testing.T) {
	expectCount := int64(N) * 2
	expectSum := int64(100)
	expectLast := int64(0)
	expectName := "accumulator"
	accumulator := statistics.NewNamedInt64Accumulator(expectName)
	for i := 0; i < N; i++ {
		accumulator.Increase()
	}

	for i := 0; i < N; i++ {
		accumulator.Decrease()
	}

	if accumulator.Count() != expectCount {
		t.Errorf("expect count: %d, but %d", expectCount, accumulator.Count())
	}

	if accumulator.Sum() != expectSum {
		t.Errorf("expect sum: %d, but %d", expectSum, accumulator.Sum())
	}

	if accumulator.Last() != expectLast {
		t.Errorf("expect last: %d, but %d", expectLast, accumulator.Last())
	}

	if name, value := accumulator.NamedCount(); name != expectName+"_count" || value != expectCount {
		t.Errorf("expect name and value of count: %s,%d, but %s,%d", expectName, expectCount, name, value)
	}

	if name, value := accumulator.NamedSum(); name != expectName+"_sum" || value != expectSum {
		t.Errorf("expect name and value of sum: %s,%d, but %s,%d", expectName, expectSum, name, value)
	}

	if name, value := accumulator.NamedLast(); name != expectName+"_last" || value != expectLast {
		t.Errorf("expect name and value of last: %s,%d, but %s,%d", expectName, expectLast, name, value)
	}
}

func BenchmarkInt64Accumulator(b *testing.B) {
	accumulator := statistics.Int64Accumulator{}
	b.SetBytes(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		accumulator.Push(int64(i))
	}
}

func TestStatisticTimer(t *testing.T) {
	accu := &statistics.Int64Accumulator{}
	st := statistics.NewStatisticTimer(accu)
	expectMinDuration := 1 * time.Second
	expectMaxDuration := 2 * time.Second

	st.Begin()
	time.Sleep(1 * time.Second)
	st.End()

	if accu.Count() != int64(1) {
		t.Errorf("expect time count: %d, but %d", 1, accu.Count())
	}

	if accu.Last() != accu.Sum() {
		t.Errorf("last(%d) is not equal to sum(%d)", accu.Last(), accu.Sum())
	}

	if accu.Last().(int64) < expectMinDuration.Nanoseconds() || accu.Last().(int64) > expectMaxDuration.Nanoseconds() {
		t.Errorf("last(%d) does not between %d and %d", accu.Last(), expectMinDuration.Nanoseconds(), expectMaxDuration.Nanoseconds())
	}
}
