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
	"encoding/json"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryTableFunctionOp("rca", &RCAOp{})
}

type TableFunctionOperator interface {
	Run(params *TableFunctionParams) ([]Chunk, error)
}

type TableFunctionParams struct {
	ChunkPortsWithMetas []*ChunkPortsWithMeta
	Param               string
}

var tableFunctionFactoryInstance = make(map[string]TableFunctionOperator)

func GetTableFunctionOperator(name string) TableFunctionOperator {
	return tableFunctionFactoryInstance[name]
}

type TableFunctionFactory map[string]TableFunctionOperator

func RegistryTableFunctionOp(name string, tableFunctionOp TableFunctionOperator) {
	_, ok := tableFunctionFactoryInstance[name]
	if ok {
		return
	}
	tableFunctionFactoryInstance[name] = tableFunctionOp
}

func (c *RCAOp) Run(params *TableFunctionParams) ([]Chunk, error) {
	ChunkPortsWithMetas := params.ChunkPortsWithMetas
	if len(ChunkPortsWithMetas) != 2 {
		return nil, errno.NewError(errno.InternalError, "rca", " rca chunk length is not 2")
	}

	opsEventChunks := make([]Chunk, 0)
	opsEventChunkMeta := make(map[string]int)
	topoIGraph := *new(IGraph)
	for _, ChunkPortsWithMeta := range ChunkPortsWithMetas {
		if ChunkPortsWithMeta.IGraph != nil {
			topoIGraph = ChunkPortsWithMeta.IGraph
		} else {
			opsEventChunks = ChunkPortsWithMeta.Chunks
			opsEventChunkMeta = ChunkPortsWithMeta.Meta
		}
	}

	if topoIGraph == nil || len(opsEventChunks) == 0 {
		return nil, errno.NewError(errno.InternalError, "rca", " rca param error")
	}

	var algoParams AlgoParam
	err := json.Unmarshal([]byte(params.Param), &algoParams)
	if err != nil {
		return nil, errno.NewError(errno.InternalError, "rca", " algo_params unmarshall fail")
	}

	topoGraph, ok := topoIGraph.(*Graph)
	if !ok {
		return nil, errno.NewError(errno.InternalError, " topoIGraph type convert fail")
	}
	graph, err := FaultDemarcation(opsEventChunks, topoGraph, algoParams, opsEventChunkMeta)
	if err != nil {
		return nil, err
	}

	chunkBuilder := *NewChunkBuilder(hybridqp.NewRowDataTypeImpl(*influxql.DefaultGraphVarRef()))
	chunk := chunkBuilder.NewChunk("")
	chunk.SetName("graph")
	chunk.SetGraph(graph)
	return []Chunk{chunk}, nil
}
