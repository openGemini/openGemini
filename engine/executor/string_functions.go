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

package executor

import (
	"strings"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type StringValuer struct{}

var _ influxql.CallValuer = StringValuer{}

func (StringValuer) Value(_ string) (interface{}, bool) {
	return nil, false
}

func (StringValuer) SetValuer(_ influxql.Valuer, _ int) {

}

func (v StringValuer) Call(name string, args []interface{}) (interface{}, bool) {
	switch name {
	case "strlen":
		if len(args) != 1 {
			return nil, false
		}
		if arg0, ok := args[0].(string); ok {
			return StrLenFunc(arg0), true
		}
		return nil, true
	case "str":
		if len(args) != 2 {
			return nil, false
		}
		arg0, ok := args[0].(string)
		if !ok {
			return nil, true
		}
		arg1, ok := args[1].(string)
		if !ok {
			return nil, true
		}
		return StrFunc(arg0, arg1), true
	case "substr":
		var (
			arg0 string
			arg1 int64
			ok   bool
		)
		arg0, ok = args[0].(string)
		if !ok {
			return nil, true
		}
		arg1, ok = args[1].(int64)
		if !ok {
			return nil, true
		}
		if len(args) == 2 {
			return SubStrTwoParaFunc(arg0, arg1), true
		}
		if len(args) == 3 {
			arg2, ok := args[2].(int64)
			if !ok {
				return nil, true
			}
			return SubStrThreeParaFunc(arg0, arg1, arg2), true
		}
		return nil, false
	default:
		return nil, false
	}
}

func StrLenFunc(srcStr string) int64 {
	return int64(len(srcStr))
}

func StrFunc(srcStr string, subStr string) bool {
	return strings.Contains(srcStr, subStr)
}

func SubStrTwoParaFunc(srcStr string, start int64) string {
	if start > int64(len(srcStr)) {
		return ""
	}
	oriStr := srcStr[start:]
	newStr := make([]byte, len(oriStr))
	copy(newStr, oriStr)
	return util.Bytes2str(newStr)
}

func SubStrThreeParaFunc(srcStr string, start, subStrLen int64) string {
	if start >= int64(len(srcStr)) {
		return ""
	}
	var oriStr string
	if start+subStrLen >= int64(len(srcStr)) {
		oriStr = srcStr[start:]
	} else {
		oriStr = srcStr[start : start+subStrLen]
	}
	newStr := make([]byte, len(oriStr))
	copy(newStr, oriStr)
	return util.Bytes2str(newStr)
}
