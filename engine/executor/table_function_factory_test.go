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
	"sort"
	"testing"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
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
	_, err := executor.GetTableFunctionOperator(executor.RcaFuncName).Run(tableFunctionParams1)
	assert.ErrorContains(t, err, "rca chunk length is not 2")

	chunkPortsWithMetaArray1 = []*executor.ChunkPortsWithMeta{
		chunkPortsWithMeta1, chunkPortsWithMeta1,
	}
	tableFunctionParams1 = &executor.TableFunctionParams{
		ChunkPortsWithMetas: chunkPortsWithMetaArray1,
		Param:               "{}",
	}
	_, err = executor.GetTableFunctionOperator(executor.RcaFuncName).Run(tableFunctionParams1)
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
	_, err = executor.GetTableFunctionOperator(executor.RcaFuncName).Run(tableFunctionParams2)
	assert.NoError(t, err)

	gomonkey.ApplyFunc(executor.FaultDemarcation, func(chunks []executor.Chunk, subTopo *executor.Graph, algoParams executor.AlgoParam, colMap map[string]int) (graph *executor.Graph, err error) {
		return nil, errors.New("error")
	})
	_, err = executor.GetTableFunctionOperator(executor.RcaFuncName).Run(tableFunctionParams2)
	assert.ErrorContains(t, err, "error")

}

func TestGetNodeIdRun(t *testing.T) {
	chunkPortsWithMeta := &executor.ChunkPortsWithMeta{
		IGraph: nil,
	}
	chunkPortsWithMetaArray := []*executor.ChunkPortsWithMeta{
		chunkPortsWithMeta,
		chunkPortsWithMeta,
	}
	tableFunctionParams := &executor.TableFunctionParams{
		ChunkPortsWithMetas: chunkPortsWithMetaArray,
	}
	_, err := executor.GetTableFunctionOperator(executor.GetTopoUidFuncName).Run(tableFunctionParams)
	assert.ErrorContains(t, err, executor.GetTopoUidFuncName+" chunk length is not 1")

	chunkPortsWithMetaArray = []*executor.ChunkPortsWithMeta{
		chunkPortsWithMeta,
	}
	tableFunctionParams = &executor.TableFunctionParams{
		ChunkPortsWithMetas: chunkPortsWithMetaArray,
	}
	_, err = executor.GetTableFunctionOperator(executor.GetTopoUidFuncName).Run(tableFunctionParams)
	assert.ErrorContains(t, err, executor.GetTopoUidFuncName+" chunk has no graph")

	chunkPortsWithMeta = &executor.ChunkPortsWithMeta{
		IGraph: BuildGraphChunk1().GetGraph(),
	}
	chunkPortsWithMetaArray = []*executor.ChunkPortsWithMeta{
		chunkPortsWithMeta,
	}
	tableFunctionParams = &executor.TableFunctionParams{
		ChunkPortsWithMetas: chunkPortsWithMetaArray,
	}
	chunks, err := executor.GetTableFunctionOperator(executor.GetTopoUidFuncName).Run(tableFunctionParams)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(chunks))

	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "uid", Type: influxql.String},
	)
	dstChunk := executor.NewChunkBuilder(rowDataType).NewChunk("topo_uids")
	dstUids := []string{"ELB", "Nginx-ingress1", "Nginx-ingress2", "Service1", "Service2", "Service3", "Service4", "VM1", "VM2", "VM3", "VM4", "VM5", "rds1", "rds2"}
	chunkLength := len(dstUids)
	dstTimes := make([]int64, chunkLength)
	dstChunk.AppendTimes(dstTimes)
	dstChunk.AppendTagsAndIndex(executor.ChunkTags{}, 0)
	dstChunk.AppendIntervalIndexes([]int{0})
	dstChunk.Column(0).AppendStringValues(dstUids)
	dstChunk.Column(0).AppendManyNotNil(chunkLength)

	assert.Equal(t, dstChunk.Name(), chunks[0].Name())
	assert.Equal(t, dstChunk.Time(), chunks[0].Time())
	assert.Equal(t, dstChunk.TagIndex(), chunks[0].TagIndex())
	for k := range chunks[0].Tags() {
		assert.Equal(t, dstChunk.Tags()[k].PointTags(), chunks[0].Tags()[k].PointTags())
	}
	assert.Equal(t, dstChunk.IntervalIndex(), chunks[0].IntervalIndex())
	uids := chunks[0].Column(0).StringValuesV2(nil)
	sort.Strings(uids)
	assert.Equal(t, dstUids, uids)
}
