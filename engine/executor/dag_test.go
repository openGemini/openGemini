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

package executor_test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func verifyProcessors(processors executor.Processors) bool {
	names := make([]string, 0, len(processors))
	expectNames := []string{
		"HttpSenderTransform",
		"MaterializeTransform",
		"MergeTransform",
		"StreamAggregateTransform",
		"MocReaderTransform",
	}
	sort.Strings(expectNames)

	for _, p := range processors {
		names = append(names, p.Name())
	}

	sort.Strings(names)

	if !reflect.DeepEqual(expectNames, names) {
		return false
	}

	return true
}

func TestDagBuilder(t *testing.T) {
	executor.RegistryTransformCreator(&executor.LogicalHttpSender{}, &executor.HttpSenderTransformCreator{})
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &MocReaderTransformCreator{})
	schema := createQuerySchema()
	schema.Options().(*query.ProcessorOptions).Sources = influxql.Sources{createMeasurement()}
	logicMst := executor.NewLogicalMst(hybridqp.NewRowDataTypeImpl(schema.MakeRefs()...))
	reader := executor.NewLogicalReader(logicMst, schema)
	agg := executor.NewLogicalAggregate(reader, schema)
	merge := executor.NewLogicalMerge([]hybridqp.QueryNode{agg}, schema)
	project := executor.NewLogicalProject(merge, schema)
	sender := executor.NewLogicalHttpSender(project, schema)

	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	sender.Explain(planWriter)
	fmt.Println(planWriter.String())

	builder := executor.NewDAGBuilder(*schema.Options().(*query.ProcessorOptions))
	dag, err := builder.Build(sender)

	if err != nil {
		t.Errorf("error found %v", err)
	}

	vertexWriter := dag.Explain(func(dag *executor.DAG, sb *strings.Builder) executor.VertexWriter {
		return executor.NewVertexWriterImpl(dag, sb)
	})

	fmt.Println(vertexWriter.String())

	if !verifyProcessors(dag.Processors()) {
		t.Errorf("processors from dag builder aren't correct")
	}
}

func TestFulljoinDagBuilder(t *testing.T) {
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &MocReaderTransformCreator{})
	joinCase := buildJoinCase()
	var joinCases []*influxql.Join
	joinCases = append(joinCases, joinCase)
	schema := buildFullJoinSchema()
	schema = executor.NewQuerySchemaWithJoinCase(schema.Fields(), schema.Sources(), schema.GetColumnNames(), schema.Options(), joinCases, nil, nil, nil)
	schema.Options().(*query.ProcessorOptions).Sources = influxql.Sources{createMeasurement()}
	logicMst1 := executor.NewLogicalMst(hybridqp.NewRowDataTypeImpl(schema.MakeRefs()...))
	logicMst2 := executor.NewLogicalMst(hybridqp.NewRowDataTypeImpl(schema.MakeRefs()...))
	reader1 := executor.NewLogicalReader(logicMst1, schema)
	reader2 := executor.NewLogicalReader(logicMst2, schema)
	leftSubquery := executor.NewLogicalSubQuery(reader1, schema)
	rightSubquery := executor.NewLogicalSubQuery(reader2, schema)
	fullJoin := executor.NewLogicalFullJoin(leftSubquery, rightSubquery, nil, schema)
	sender := executor.NewLogicalHttpSender(fullJoin, schema)
	planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
	sender.Explain(planWriter)
	fmt.Println(planWriter.String())
	builder := executor.NewDAGBuilder(*schema.Options().(*query.ProcessorOptions))
	dag, err := builder.Build(sender)
	if err != nil {
		t.Errorf("error found %v", err)
	}
	vertexWriter := dag.Explain(func(dag *executor.DAG, sb *strings.Builder) executor.VertexWriter {
		return executor.NewVertexWriterImpl(dag, sb)
	})
	fmt.Println(vertexWriter.String())
}
