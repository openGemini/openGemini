/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/stretchr/testify/assert"
)

func TestPromLabelReplace(t *testing.T) {
	labelValuer := executor.LabelValuer{}
	callName := "label_replace"
	chunkTags := []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"instance", "job"}, []string{"localhost:9090", "prometheus"})}
	var expects interface{}

	// 1.normal return
	callArgs := []interface{}{chunkTags, "host", "$2", "instance", "(.*):(.*)"}
	expects = []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"instance", "job", "host"}, []string{"localhost:9090", "prometheus", "9090"})}
	act, _ := labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 2.not chunkTags
	callArgs = []interface{}{123, "host", "$2", "instance", "(.*):(.*)"}
	expects = 123
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 3. check args1
	callArgs = []interface{}{chunkTags, 1, "$2", "instance", "(.*):(.*)"}
	expects = nil
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 4. check args2
	callArgs = []interface{}{chunkTags, "host", 1, "instance", "(.*):(.*)"}
	expects = nil
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 5. check args3
	callArgs = []interface{}{chunkTags, "host", "$2", 1, "(.*):(.*)"}
	expects = nil
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 6. check args4
	callArgs = []interface{}{chunkTags, "host", "$2", "instance", 1}
	expects = nil
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

}

func TestPromLabelJoin(t *testing.T) {
	labelValuer := executor.LabelValuer{}
	callName := "label_join"
	chunkTags := []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"instance", "job"}, []string{"localhost:9090", "prometheus"})}
	var expects interface{}

	// 1.normal return
	callArgs := []interface{}{chunkTags, "joinStr", "-", "instance", "job"}
	expects = []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"instance", "job", "joinStr"}, []string{"localhost:9090", "prometheus", "localhost:9090-prometheus"})}
	act, _ := labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 2.not chunkTags
	callArgs = []interface{}{123, "joinStr", "-", "instance", "job"}
	expects = 123
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 3. check args1
	callArgs = []interface{}{chunkTags, 1, "-", "instance", "job"}
	expects = nil
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 4. check args1
	callArgs = []interface{}{chunkTags, "joinStr", 1, "instance", "job"}
	expects = nil
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 5. check args1
	callArgs = []interface{}{chunkTags, "joinStr", "-", 1, "job"}
	expects = nil
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 6. wrong label name
	callArgs = []interface{}{chunkTags, "joinStr", "-", "instance1", "job"}
	expects = []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"instance", "job", "joinStr"}, []string{"localhost:9090", "prometheus", "-prometheus"})}
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 7. wrong label name
	callArgs = []interface{}{chunkTags, "joinStr", "-", "instance", "job1"}
	expects = []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"instance", "job", "joinStr"}, []string{"localhost:9090", "prometheus", "localhost:9090-"})}
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)

	// 8. wrong label name
	callArgs = []interface{}{chunkTags, "joinStr", "-", "instance1", "job1"}
	expects = []executor.ChunkTags{*executor.NewChunkTagsByTagKVs([]string{"instance", "job", "joinStr"}, []string{"localhost:9090", "prometheus", "-"})}
	act, _ = labelValuer.Call(callName, callArgs)
	assert.Equal(t, expects, act)
}
