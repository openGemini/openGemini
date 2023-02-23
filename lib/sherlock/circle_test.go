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

package sherlock

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_MetricCircle(t *testing.T) {
	stat := newMetricCircle(0)
	stat.push(999)
	require.Equal(t, []int{}, stat.data)
	require.Equal(t, 0, stat.mean())

	stat = newMetricCircle(10)
	for i := 0; i < 15; i++ {
		stat.push(i)
	}
	require.Equal(t, []int{10, 11, 12, 13, 14, 5, 6, 7, 8, 9}, stat.data)
	require.Equal(t, 95, stat.sum)
	require.Equal(t, 5, stat.dataIdx)
	require.Equal(t, 10, stat.dataCap)
	require.Equal(t, 9, stat.mean())
}

func Test_MetricCircle_sequentialData(t *testing.T) {
	stat := newMetricCircle(10)
	for i := 0; i < 5; i++ {
		stat.push(i)
	}
	require.Equal(t, []int{0, 1, 2, 3, 4}, stat.data)
	require.Equal(t, []int{0, 1, 2, 3, 4, 0, 0, 0, 0, 0}, stat.sequentialData())

	stat = newMetricCircle(10)
	for i := 0; i < 15; i++ {
		stat.push(i)
	}
	require.Equal(t, []int{10, 11, 12, 13, 14, 5, 6, 7, 8, 9}, stat.data)
	require.Equal(t, []int{5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, stat.sequentialData())
}
