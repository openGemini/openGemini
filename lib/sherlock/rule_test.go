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

func Test_matchRule(t *testing.T) {
	metrics := newMetricCircle(5)
	for i := 20; i < 50; i += 5 {
		metrics.push(i)
	}
	// < ruleMin
	match, rule := matchRule(metrics, 2, 20, 25, 80)
	require.Equal(t, false, match)
	require.Equal(t, RuleCurLessMin, rule)

	// > ruleAbs
	match, rule = matchRule(metrics, 99, 20, 25, 80)
	require.Equal(t, true, match)
	require.Equal(t, RuleCurGreaterAbs, rule)

	// > ruleDiff
	match, rule = matchRule(metrics, 60, 20, 25, 80)
	require.Equal(t, true, match)
	require.Equal(t, RuleDiff, rule)

	// not trigger rules
	match, rule = matchRule(metrics, 35, 20, 25, 80)
	require.Equal(t, false, match)
	require.Equal(t, RuleCurlGreaterMin, rule)
}
