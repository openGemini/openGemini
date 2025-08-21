// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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
	"context"
	"sync"

	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type TableFunctionTransformCreator struct {
}

type TableFunctionTransform struct {
	BaseProcessor

	InputsWithMetas []*ChunkPortsWithMeta
	Output          *ChunkPort

	TableFunctionName string
	param             string

	workTracing *tracing.Span
}

func (trans *TableFunctionTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	tableFunctionTrans := NewTableFunctionTransform(plan)
	return tableFunctionTrans, nil
}

func NewTableFunctionTransform(plan LogicalPlan) *TableFunctionTransform {
	outRowDataType := plan.RowDataType()
	InputsWithMetas := make([]*ChunkPortsWithMeta, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		ordinalMap := make(map[string]int, inPlan.RowDataType().Fields().Len())
		for i, field := range inPlan.Schema().Fields() {
			fieldName := field.Name()
			ordinalMap[fieldName] = i
		}

		input := NewChunkPort(inPlan.RowDataType())
		ChunkPortsWithMeta := &ChunkPortsWithMeta{
			ChunkPort: input,
			Meta:      ordinalMap,
		}
		InputsWithMetas = append(InputsWithMetas, ChunkPortsWithMeta)
	}
	tableFunctionTrans := &TableFunctionTransform{
		Output:            NewChunkPort(outRowDataType),
		TableFunctionName: plan.(*LogicalTableFunction).TableFunctionName,
		param:             plan.(*LogicalTableFunction).param,
		InputsWithMetas:   InputsWithMetas,
	}
	return tableFunctionTrans
}

func (trans *TableFunctionTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[tableFunctionTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_tableFunctionTransform", false)
	defer func() {
		trans.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(len(trans.InputsWithMetas))

	for i := range trans.InputsWithMetas {
		go trans.runnable(i, &wg, ctx)
	}
	wg.Wait()

	err := trans.functionRoute()
	if err != nil {
		return err
	}
	return nil
}

func (trans *TableFunctionTransform) runnable(i int, wg *sync.WaitGroup, ctx context.Context) {
	defer func() {
		wg.Done()
	}()
	for {
		select {
		case chunk, ok := <-trans.InputsWithMetas[i].ChunkPort.State:
			if !ok {
				return
			}
			if chunk.GetGraph() != nil {
				trans.InputsWithMetas[i].IGraph = chunk.GetGraph()
			} else {
				trans.InputsWithMetas[i].Chunks = append(trans.InputsWithMetas[i].Chunks, chunk.Clone())
			}
		case <-ctx.Done():
			return
		}
	}
}

func (trans *TableFunctionTransform) functionRoute() error {
	tableFunctionParams := &TableFunctionParams{
		ChunkPortsWithMetas: trans.InputsWithMetas,
		Param:               trans.param,
	}
	result, err := GetTableFunctionOperator(trans.TableFunctionName).Run(tableFunctionParams)
	if err != nil {
		return err
	}
	for _, resultChunk := range result {
		trans.Output.State <- resultChunk
	}
	return nil
}

type ChunkPortsWithMeta struct {
	ChunkPort *ChunkPort
	Chunks    []Chunk
	IGraph    IGraph
	Meta      map[string]int
}

var _ = RegistryTransformCreator(&LogicalTableFunction{}, &TableFunctionTransformCreator{})

func (trans *TableFunctionTransform) Close() {
	trans.Output.Close()
}

func (trans *TableFunctionTransform) Name() string {
	return "TableFunctionTransform"
}

func (trans *TableFunctionTransform) GetOutputs() Ports {
	return Ports{trans.Output}
}

func (trans *TableFunctionTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.InputsWithMetas))

	for _, inputsWithMeta := range trans.InputsWithMetas {
		ports = append(ports, inputsWithMeta.ChunkPort)
	}
	return ports
}

func (trans *TableFunctionTransform) GetOutputNumber(port Port) int {
	return 0
}

func (trans *TableFunctionTransform) GetInputNumber(port Port) int {
	for i, inputsWithMeta := range trans.InputsWithMetas {
		if inputsWithMeta.ChunkPort == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *TableFunctionTransform) Explain() []ValuePair {
	return nil
}
