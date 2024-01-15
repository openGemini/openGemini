/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package immutable

import (
	"regexp"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type UnnestMatch interface {
	Get([][]byte) [][]byte
}

type UnnestMatchAll struct {
	unnest *influxql.Unnest
	re     *regexp.Regexp
}

func NewUnnestMatchAll(unnest *influxql.Unnest) (*UnnestMatchAll, error) {
	unnestExpr, ok := unnest.Expr.(*influxql.Call)
	if !ok {
		return nil, nil
	}
	var err error
	reg, err := regexp.Compile(unnestExpr.Args[0].(*influxql.VarRef).Val)
	if err != nil {
		return nil, err
	}
	r := &UnnestMatchAll{
		unnest: unnest,
		re:     reg,
	}

	return r, nil
}

func (r *UnnestMatchAll) Get(s string) map[string]string {
	result := make(map[string]string)
	for _, v := range r.unnest.Aliases {
		result[v] = ""
	}
	matchesAll := r.re.FindStringSubmatch(s)
	if matchesAll == nil {
		return result
	}
	for k, v := range r.unnest.Aliases {
		result[v] = matchesAll[k+1]
	}
	return result
}
