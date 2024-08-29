// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
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

func retNodeHashAggCheck(t *testing.T, node hybridqp.QueryNode) {
	if _, ok := node.(*LogicalHashAgg); !ok {
		t.Fatal("retNodeHashAggCheck err")
	}
}

func retNodeStreamAggCheck(t *testing.T, node hybridqp.QueryNode) {
	if _, ok := node.(*LogicalAggregate); !ok {
		t.Fatal("retNodeStreamAggCheck err")
	}
}

func Test_BuildAggNode(t *testing.T) {
	lowerOpt := &query.ProcessorOptions{
		PromQuery: true,
	}
	upperOpt := &query.ProcessorOptions{
		LowerOpt:       lowerOpt,
		PromQuery:      true,
		GroupByAllDims: true,
	}
	schema := &QuerySchema{
		opt: upperOpt,
	}
	builder := NewLogicalPlanBuilderImpl(schema)
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.GroupByAllDims = false
	upperOpt.Without = true
	lowerOpt.GroupByAllDims = true
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	lowerOpt.GroupByAllDims = false
	lowerOpt.Without = true
	lowerOpt.Dimensions = []string{"a", "b"}
	upperOpt.Dimensions = []string{"a"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.Dimensions = []string{"c"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	lowerOpt.Without = false
	upperOpt.Dimensions = []string{"b"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.Dimensions = []string{"a"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	upperOpt.Without = false
	lowerOpt.GroupByAllDims = true
	lowerOpt.Dimensions = nil
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	lowerOpt.GroupByAllDims = false
	lowerOpt.Without = true
	lowerOpt.Dimensions = []string{"a"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	lowerOpt.Dimensions = []string{"b"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	lowerOpt.Without = false
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())

	upperOpt.Dimensions = []string{"b"}
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeStreamAggCheck(t, builder.stack.Pop())

	upperOpt.BinOp = true
	builder.Series()
	buildAggNode(builder, schema, false)
	retNodeHashAggCheck(t, builder.stack.Pop())
}
