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

package sherlock

type RuleType uint8

const (
	RuleHistoryLessMin RuleType = iota
	RuleCurlGreaterMin
	RuleCurGreaterAbs
	RuleDiff
)

func matchRule(history *MetricCircle, curVal, ruleMin, ruleDiff, ruleAbs int) (bool, RuleType) {
	for i := range history.data {
		if history.data[i] < ruleMin {
			return false, RuleHistoryLessMin
		}
	}

	if curVal > ruleAbs {
		return true, RuleCurGreaterAbs
	}

	mean := history.mean()
	if curVal >= mean*(100+ruleDiff)/100 {
		return true, RuleDiff
	}

	return false, RuleCurlGreaterMin
}
