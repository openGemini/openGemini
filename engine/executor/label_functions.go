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

package executor

import (
	"regexp"
	"strings"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryLabelFunction("label_replace", &labelReplaceFunc{})
	RegistryLabelFunction("label_join", &labelJoinFunc{})
}

type LabelFunction interface {
	CallFunc(name string, args []interface{}) (interface{}, bool)
}

var labelFuncInstance = make(map[string]LabelFunction)

func RegistryLabelFunction(name string, labelFunc LabelFunction) {
	_, ok := labelFuncInstance[name]
	if ok {
		return
	}
	labelFuncInstance[name] = labelFunc
}

type labelReplaceFunc struct{}

func (s *labelReplaceFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	tags, ok := args[0].([]ChunkTags)
	if !ok {
		return args[0], true
	}
	dst, ok := args[1].(string)
	if !ok {
		return nil, true
	}
	repl, ok := args[2].(string)
	if !ok {
		return nil, true
	}
	src, ok := args[3].(string)
	if !ok {
		return nil, true
	}
	regexStr, ok := args[4].(string)
	if !ok {
		return nil, true
	}

	regex, err := regexp.Compile("^(?:" + regexStr + ")$")
	if err != nil {
		return nil, true
	}
	newTags := make([]ChunkTags, 0, len(tags))
	for _, tag := range tags {
		v, ok := tag.GetChunkTagValue(src)
		if !ok {
			newTags = append(newTags, tag)
			continue
		}
		index := regex.FindStringSubmatchIndex(v)
		if index == nil {
			newTags = append(newTags, tag)
			continue
		}
		res := regex.ExpandString([]byte{}, repl, v, index)
		keys, values := tag.GetChunkTagAndValues()
		keys = append(keys, dst)
		values = append(values, string(res))
		newTags = append(newTags, *NewChunkTagsByTagKVs(keys, values))
	}

	return newTags, true
}

type labelJoinFunc struct{}

func (s *labelJoinFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	tags, ok := args[0].([]ChunkTags)
	if !ok {
		return args[0], true
	}
	dst, ok := args[1].(string)
	if !ok {
		return nil, true
	}
	sep, ok := args[2].(string)
	if !ok {
		return nil, true
	}
	srcLabels := make([]string, len(args)-3)
	for i := 3; i < len(args); i++ {
		if src, ok := args[i].(string); ok {
			srcLabels[i-3] = src
		} else {
			return nil, true
		}
	}

	newTags := make([]ChunkTags, 0, len(tags))
	srcVals := make([]string, len(srcLabels))
	for _, tag := range tags {
		for i, src := range srcLabels {
			v, ok := tag.GetChunkTagValue(src)
			if ok {
				srcVals[i] = v
			}
		}

		res := strings.Join(srcVals, sep)
		keys, values := tag.GetChunkTagAndValues()
		keys = append(keys, dst)
		values = append(values, res)
		newTags = append(newTags, *NewChunkTagsByTagKVs(keys, values))
	}
	return newTags, true
}

// valuer
type LabelValuer struct{}

var _ influxql.CallValuer = LabelValuer{}

func (LabelValuer) Value(_ string) (interface{}, bool) {
	return nil, false
}

func (LabelValuer) SetValuer(_ influxql.Valuer, _ int) {
}

func (v LabelValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if labelFunc := labelFuncInstance[name]; labelFunc != nil {
		return labelFunc.CallFunc(name, args)
	}
	return nil, false
}
