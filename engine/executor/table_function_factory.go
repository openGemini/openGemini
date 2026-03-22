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

const GetTopoUidFuncName = "ms_get_node_id"
const RcaFuncName = "ms_rca"

func init() {
	RegistryTableFunctionOp(RcaFuncName, &RCAOp{})
	RegistryTableFunctionOp(GetTopoUidFuncName, &GetNodeIdOp{})
}

type GetNodeIdOp struct{}

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

func (c *GetNodeIdOp) Run(params *TableFunctionParams) ([]Chunk, error) {
	ChunkPortsWithMetas := params.ChunkPortsWithMetas
	if len(ChunkPortsWithMetas) != 1 {
		return nil, errno.NewError(errno.InternalError, GetTopoUidFuncName, GetTopoUidFuncName+" chunk length is not 1")
	}

	topoIGraph := ChunkPortsWithMetas[0].IGraph
	if topoIGraph == nil {
		return nil, errno.NewError(errno.InternalError, GetTopoUidFuncName, GetTopoUidFuncName+" chunk has no graph")
	}

	graph, ok := topoIGraph.(*Graph)
	if !ok {
		return nil, errno.NewError(errno.InternalError, GetTopoUidFuncName, GetTopoUidFuncName+" chunk is not graph")
	}

	nodes := graph.Nodes
	uids := make([]string, 0, len(nodes))
	for uid := range nodes {
		uids = append(uids, uid)
	}
	// To-be-optimized: assemble chunks based on the incoming select schema
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "uid", Type: influxql.String},
	)
	chunk := NewChunkBuilder(rowDataType).NewChunk("topo_uids")

	chunkLength := len(uids)
	times := make([]int64, chunkLength)
	chunk.AppendTimes(times)
	chunk.AppendTagsAndIndex(ChunkTags{}, 0)
	chunk.AppendIntervalIndexes([]int{0})

	chunk.Column(0).AppendStringValues(uids)
	chunk.Column(0).AppendManyNotNil(chunkLength)
	return []Chunk{chunk}, nil
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

	if topoIGraph == nil {
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
	chunks := []Chunk{chunk}

	if graph != nil {
		events, err := FilterEventChunks(opsEventChunks, graph.Nodes, opsEventChunkMeta)
		if err != nil {
			return nil, err
		}

		for _, event := range events {
			event.SetName("event")
			chunks = append(chunks, event)
		}
	}

	return chunks, nil
}
