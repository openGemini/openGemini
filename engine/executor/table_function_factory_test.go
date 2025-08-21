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

package executor_test

import (
	"errors"
	"testing"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/stretchr/testify/assert"
)

func BuildGraphChunk1() executor.Chunk {
	schema := buildTableFunctionSchema1()

	b := executor.NewChunkBuilder(schema)
	chunk := b.NewChunk("mst")
	graph := executor.NewGraph()
	graph.CreateGraph(mockGetTimeGraph())
	chunk.SetGraph(graph)
	return chunk
}

func BuildEventChunk1() executor.Chunk {
	time1 := make([]int64, 1000)
	v1 := make([]float64, 1000)
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	for i := 0; i < 500; i++ {
		time1[i] = int64(i)
		v1[i] = float64(i)
	}
	schema := buildTableFunctionSchema1()

	b := executor.NewChunkBuilder(schema)

	chunk := b.NewChunk("mst")

	chunk.AppendTimes(time1)
	chunk.Column(0).AppendFloatValues(v1)
	chunk.Column(0).AppendManyNotNil(999)

	return chunk
}

func TestRcaRun(t *testing.T) {
	chunkPortsWithMeta1 := &executor.ChunkPortsWithMeta{
		ChunkPort: nil,
		Chunks:    []executor.Chunk{BuildEventChunk1()},
		Meta:      nil,
	}
	chunkPortsWithMetaArray1 := []*executor.ChunkPortsWithMeta{
		chunkPortsWithMeta1,
	}
	tableFunctionParams1 := &executor.TableFunctionParams{
		ChunkPortsWithMetas: chunkPortsWithMetaArray1,
		Param:               "{}",
	}
	gomonkey.ApplyFunc(executor.FaultDemarcation, func(chunks []executor.Chunk, subTopo *executor.Graph, algoParams executor.AlgoParam, colMap map[string]int) (graph *executor.Graph, err error) {
		return nil, nil
	})
	_, err := executor.GetTableFunctionOperator("rca").Run(tableFunctionParams1)
	assert.ErrorContains(t, err, "rca chunk length is not 2")

	chunkPortsWithMetaArray1 = []*executor.ChunkPortsWithMeta{
		chunkPortsWithMeta1, chunkPortsWithMeta1,
	}
	tableFunctionParams1 = &executor.TableFunctionParams{
		ChunkPortsWithMetas: chunkPortsWithMetaArray1,
		Param:               "{}",
	}
	_, err = executor.GetTableFunctionOperator("rca").Run(tableFunctionParams1)
	assert.ErrorContains(t, err, "rca param error")

	chunkPortsWithMeta2 := &executor.ChunkPortsWithMeta{
		ChunkPort: nil,
		Meta:      nil,
		IGraph:    BuildGraphChunk1().GetGraph(),
	}
	chunkPortsWithMetaArray2 := []*executor.ChunkPortsWithMeta{
		chunkPortsWithMeta1,
		chunkPortsWithMeta2,
	}
	tableFunctionParams2 := &executor.TableFunctionParams{
		ChunkPortsWithMetas: chunkPortsWithMetaArray2,
		Param:               "{}",
	}
	_, err = executor.GetTableFunctionOperator("rca").Run(tableFunctionParams2)
	assert.NoError(t, err)

	gomonkey.ApplyFunc(executor.FaultDemarcation, func(chunks []executor.Chunk, subTopo *executor.Graph, algoParams executor.AlgoParam, colMap map[string]int) (graph *executor.Graph, err error) {
		return nil, errors.New("error")
	})
	_, err = executor.GetTableFunctionOperator("rca").Run(tableFunctionParams2)
	assert.ErrorContains(t, err, "error")

}
