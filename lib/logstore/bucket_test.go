// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package logstore_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/stretchr/testify/assert"
)

type inputTimeBucket struct {
	start    int64
	end      int64
	expected int64
}

func TestGetAdaptiveTimeBucket(t *testing.T) {
	assertWindowFunc := func(start, end, expected int64) {
		startTime := time.Unix(0, start)
		endTime := time.Unix(0, end)
		outputInterval := logstore.GetAdaptiveTimeBucket(startTime, endTime, true)
		expectInterval := time.Duration(expected) * time.Second
		assert.Equal(t, outputInterval.Seconds(), expectInterval.Seconds())
	}

	assertBucketFunc := func(start, end, expected int64) {
		res := (end - start) / expected
		if res <= 0 || res > 60 {
			t.Fatal("time bucket is wrong")
		}
		fmt.Println(start, "-", end, "-", expected, "-", res)
	}

	testCases := []inputTimeBucket{
		{start: 0, end: 10, expected: 1},
		{start: 0, end: 60, expected: 1},
		{start: 0, end: 60 * 60, expected: 60},
		{start: 0, end: 24 * 60 * 60, expected: 1800},
		{start: 0, end: 30 * 24 * 60 * 60, expected: 43200},
		{start: 0, end: 12 * 30 * 24 * 60 * 60, expected: 518400},
		{start: 0, end: 100 * 12 * 30 * 24 * 60 * 60, expected: 6.2208e+07},
		{start: 0, end: 200 * 12 * 30 * 24 * 60 * 60, expected: 1.24416e+08},

		{start: 30 / 10, end: 60, expected: 1},
		{start: 60 * 60 / 10, end: 60 * 60, expected: 55},
		{start: 24 * 60 * 60 / 10, end: 24 * 60 * 60, expected: 1500},
		{start: 30 * 24 * 60 * 60 / 10, end: 30 * 24 * 60 * 60, expected: 39600},
		{start: 12 * 30 * 24 * 60 * 60 / 10, end: 12 * 30 * 24 * 60 * 60, expected: 518400},
		{start: 100 * 12 * 30 * 24 * 60 * 60 / 10, end: 100 * 12 * 30 * 24 * 60 * 60, expected: 6.2208e+07},
		{start: 100 * 12 * 30 * 24 * 60 * 60 / 10, end: 200 * 12 * 30 * 24 * 60 * 60, expected: 1.24416e+08},
	}
	for i := range testCases {
		assertWindowFunc(testCases[i].start*1e9, testCases[i].end*1e9, testCases[i].expected)
		assertBucketFunc(testCases[i].start, testCases[i].end, testCases[i].expected)
	}
}
