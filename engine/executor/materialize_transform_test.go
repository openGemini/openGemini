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

package executor_test

import (
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func createMaterializeExprOptions() []hybridqp.ExprOptions {
	rt := createRowDataType()

	ops := make([]hybridqp.ExprOptions, 0, len(rt.Fields()))
	for _, f := range rt.Fields() {
		ops = append(ops, hybridqp.ExprOptions{
			Expr: f.Expr,
			Ref:  *(f.Expr.(*influxql.VarRef)),
		})
	}

	return ops
}

type CmpChunkWriter struct {
	expect executor.Chunk
	t      *testing.T
}

func NewCmpChunkWriter(expect executor.Chunk, t *testing.T) *CmpChunkWriter {
	return &CmpChunkWriter{
		expect: expect,
		t:      t,
	}
}

func (w *CmpChunkWriter) Write(chunk executor.Chunk) {
	if !reflect.DeepEqual(w.expect, chunk) {
		w.t.Error("expect chunk isn't equal to output chunk")
	}
}

func (w *CmpChunkWriter) Close() {

}

func TestForwardMaterializeTransform(t *testing.T) {
	schema := createQuerySchema()
	measurement := createMeasurement()
	refs := createVarRefsFromFields()
	table := executor.NewQueryTable(measurement, refs)
	scan := executor.NewTableScanFromSingleChunk(createRowDataType(), table, *schema.Options().(*query.ProcessorOptions))

	ops := createMaterializeExprOptions()
	chunkBuilder := executor.NewChunkBuilder(createRowDataType())
	expect := chunkBuilder.NewChunk(table.Name())

	materialize := executor.NewMaterializeTransform(createRowDataType(), createRowDataType(), ops, *schema.Options().(*query.ProcessorOptions), NewCmpChunkWriter(expect, t), schema)
	httpSender := executor.NewHttpSenderTransform(createRowDataType(), schema)

	executor.Connect(scan.GetOutputs()[0], materialize.GetInputs()[0])
	executor.Connect(materialize.GetOutputs()[0], httpSender.GetInputs()[0])

	var processors executor.Processors
	processors = append(processors, scan)
	processors = append(processors, materialize)
	processors = append(processors, httpSender)

	dag := executor.NewDAG(processors)
	if dag.CyclicGraph() {
		t.Errorf("cyclic dag graph found")
	}
}
