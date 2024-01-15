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

package executor

import (
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func TestSchemaOverLimit(t *testing.T) {
	statement := &preparedStatement{}
	statement.stmt = &influxql.SelectStatement{}
	ok, err := statement.isSchemaOverLimit(nil)
	if err != nil {
		t.Error("isSchemaOverLimit error")
	}
	if ok {
		t.Error("expect ok")
	}
}

func Test_defaultQueryExecutorBuilderCreator(t *testing.T) {
	tests := []struct {
		name string
		want hybridqp.PipelineExecutorBuilder
	}{
		{
			name: "test",
			want: &ExecutorBuilder{
				dag:                   NewTransformDag(),
				root:                  nil,
				traits:                nil,
				currConsumer:          0,
				enableBinaryTreeMerge: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultQueryExecutorBuilderCreator(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("defaultQueryExecutorBuilderCreator() = %v, want %v", got, tt.want)
			}
		})
	}
}
