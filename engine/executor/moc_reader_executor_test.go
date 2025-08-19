// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// nolint
package executor_test

import (
	"context"
	"sort"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

var DataSources = map[string]ChunkStruct{
	"cpu": {
		chunk: []executor.Chunk{BuildSQLChunk1(), BuildSQLChunk2(), BuildSQLChunk3(), BuildSQLChunk4()},
		stringMap: map[string]int{
			"id":    0,
			"name":  1,
			"value": 2,
		},
	},
	"mst1": {
		chunk: []executor.Chunk{BuildSQLChunk5()},
		stringMap: map[string]int{
			"value": 0,
			"id":    1,
			"name":  2,
		},
	},
	"mst2": {
		chunk: []executor.Chunk{BuildSQLChunk6()},
		stringMap: map[string]int{
			"id":   0,
			"name": 1,
		},
	},
	"mst3": {
		chunk: []executor.Chunk{BuildSQLChunk7()},
		stringMap: map[string]int{
			"id": 0,
		},
	},
}

func AddDataSources(name string, chunks []executor.Chunk, m map[string]int) {
	DataSources[name] = ChunkStruct{
		chunk:     chunks,
		stringMap: m,
	}
}

type ChunkStruct struct {
	chunk     []executor.Chunk
	stringMap map[string]int
}

type DataMapStruct struct {
	name    string
	dataMap map[int]int
}

type DataMaps struct {
	items []DataMapStruct
}

func (d DataMaps) Len() int {
	return len(d.items)
}

func (d DataMaps) Less(i, j int) bool {
	return d.items[i].name < d.items[j].name
}

func (d DataMaps) Swap(i, j int) {
	d.items[i], d.items[j] = d.items[j], d.items[i]
}

type MocReaderTransform struct {
	executor.BaseProcessor

	Output   *executor.ChunkPort
	MstRt    hybridqp.RowDataType
	Ops      []hybridqp.ExprOptions
	Opt      *query.ProcessorOptions
	Builder  *executor.ChunkBuilder
	NewChunk []executor.Chunk
	Count    int
	DataMap  DataMaps
}

func NewMocReaderTransform(mstRt hybridqp.RowDataType, outRt hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt *query.ProcessorOptions) *MocReaderTransform {
	trans := &MocReaderTransform{
		Output: executor.NewChunkPort(outRt),
		MstRt:  mstRt,
		Ops:    ops,
		Opt:    opt,
		DataMap: DataMaps{
			items: make([]DataMapStruct, len(opt.Sources)),
		},
		Builder: executor.NewChunkBuilder(outRt),
	}
	for i := range opt.Sources {
		tableName := opt.Sources[i].(*influxql.Measurement).Name
		trans.DataMap.items[i] = DataMapStruct{
			name:    tableName,
			dataMap: make(map[int]int),
		}
		for _, option := range trans.Ops {
			if index, ok := DataSources[tableName].stringMap[option.Expr.(*influxql.VarRef).Val]; ok {
				trans.DataMap.items[i].dataMap[SearchIndex(trans.Output.RowDataType, option.Ref)] = index
			}
		}
	}
	sort.Sort(trans.DataMap)
	for _, data := range trans.DataMap.items {
		trans.InitChunk(data)
	}
	return trans
}

type MocReaderTransformCreator struct {
}

func (c *MocReaderTransformCreator) Create(plan executor.LogicalPlan, opt *query.ProcessorOptions) (executor.Processor, error) {
	p := NewMocReaderTransform(nil, plan.RowDataType(), plan.RowExprOptions(), opt)
	return p, nil
}

func SearchIndex(outRt hybridqp.RowDataType, ref influxql.VarRef) int {
	return outRt.FieldIndex(ref.Val)
}

func (trans *MocReaderTransform) Name() string {
	return "MocReaderTransform"
}

func (trans *MocReaderTransform) Close() {
	return
}

func (trans *MocReaderTransform) Work(ctx context.Context) error {
	defer func() {
		trans.Output.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if trans.Count >= len(trans.NewChunk) {
				return nil
			}

			trans.Output.State <- trans.NewChunk[trans.Count]
			trans.Count++
		}
	}
}

func (trans *MocReaderTransform) InitChunk(dataMap DataMapStruct) {
	name := dataMap.name
	sources := DataSources[name].chunk
	for i := range sources {
		chunk := trans.Builder.NewChunk(name)
		chunk.AppendTimes(sources[i].Time())
		for j := range sources[i].Tags() {
			chunk.AppendTagsAndIndex(sources[i].Tags()[j], sources[i].TagIndex()[j])
		}
		for k, v := range dataMap.dataMap {
			chunk.SetColumn(sources[i].Column(v), k)
		}
		IntervalIndexGenerator(chunk, trans.Opt)
		trans.NewChunk = append(trans.NewChunk, chunk)
	}
}

func (trans *MocReaderTransform) GetOutputs() executor.Ports {
	return executor.Ports{trans.Output}
}

func (trans *MocReaderTransform) GetInputs() executor.Ports {
	return executor.Ports{}
}

func (trans *MocReaderTransform) GetOutputNumber(port executor.Port) int {
	return 1
}

func (trans *MocReaderTransform) Explain() []executor.ValuePair {
	return nil
}

func (trans *MocReaderTransform) GetInputNumber(port executor.Port) int {
	return executor.INVALID_NUMBER
}

func GetMapFromReader(rowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions) map[int]string {
	m := make(map[int]string)
	for _, option := range ops {
		m[SearchIndex(rowDataType, option.Ref)] = option.Expr.(*influxql.VarRef).Val
	}
	return m
}

func BuildSQLChunk1() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("cpu")

	chunk.AppendTimes([]int64{1, 2, 13, 14, 21})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=A"), 0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"})
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4})
	chunk.Column(2).AppendManyNotNil(4)

	return chunk
}

func BuildSQLChunk2() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("cpu")

	chunk.AppendTimes([]int64{22, 32, 33, 24, 25})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=B"), 3)

	chunk.Column(0).AppendIntegerValues([]int64{2, 5, 8, 12})
	chunk.Column(0).AppendNilsV2(true, true, true, false, true)

	chunk.Column(1).AppendStringValues([]string{"tomB", "vergilB", "danteB", "martino"})
	chunk.Column(1).AppendNilsV2(true, false, true, true, true)

	chunk.Column(2).AppendFloatValues([]float64{2.2, 2.3, 2.4, 2.5, 2.6})
	chunk.Column(2).AppendManyNotNil(5)

	return chunk
}

func BuildSQLChunk3() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("cpu")

	chunk.AppendTimes([]int64{26, 32, 33, 34, 35})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=B"), 0)
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=C"), 2)

	chunk.Column(0).AppendIntegerValues([]int64{19, 21, 31, 33})
	chunk.Column(0).AppendNilsV2(false, true, true, true, true)

	chunk.Column(1).AppendStringValues([]string{"tomC", "jerryC", "vergilC", "danteC", "martino"})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendFloatValues([]float64{3.2, 3.3, 3.4, 3.5, 3.6})
	chunk.Column(2).AppendManyNotNil(5)

	return chunk
}

func BuildSQLChunk4() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("cpu")

	chunk.AppendTimes([]int64{41, 42})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=C"), 0)

	chunk.Column(0).AppendIntegerValues([]int64{7, 8})
	chunk.Column(0).AppendManyNotNil(2)

	chunk.Column(1).AppendStringValues([]string{"tomD", "jerryD"})
	chunk.Column(1).AppendManyNotNil(2)

	chunk.Column(2).AppendFloatValues([]float64{4.2, 4.3})
	chunk.Column(2).AppendManyNotNil(2)

	return chunk
}

func BuildSQLChunk5() executor.Chunk {
	rowDataType := buildAnotherRowDataType1()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst1")

	chunk.AppendTimes([]int64{1, 2, 13, 14, 21})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=A"), 0)

	chunk.Column(0).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(0).AppendNil()

	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"})
	chunk.Column(2).AppendNilsV2(true, true, false, true, true)

	return chunk
}

func BuildSQLChunk6() executor.Chunk {
	rowDataType := buildAnotherRowDataType2()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst2")

	chunk.AppendTimes([]int64{1, 2, 13, 14, 21})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=A"), 0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"})
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	return chunk
}

func BuildSQLChunk7() executor.Chunk {
	rowDataType := buildAnotherRowDataType2()

	b := executor.NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst3")

	chunk.AppendTimes([]int64{1, 2, 13, 14, 21})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=A"), 0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
	chunk.Column(0).AppendManyNotNil(5)

	return chunk
}

func buildAnotherRowDataType1() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
		influxql.VarRef{Val: "id", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
	)

	return rowDataType
}

func buildAnotherRowDataType2() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "id", Type: influxql.Integer},
		influxql.VarRef{Val: "name", Type: influxql.String},
	)

	return rowDataType
}

func IntervalIndexGenerator(c executor.Chunk, opt *query.ProcessorOptions) {
	c.AppendIntervalIndex(0)
	if opt.Interval.IsZero() {
		c.AppendIntervalIndexes(c.TagIndex()[1:])
		return
	}
	tagIndex := c.TagIndex()[1:]
	time := c.Time()
	duration := opt.Interval.Duration
	for i := 1; i < len(time); i++ {
		if len(tagIndex) > 0 && i == tagIndex[0] {
			c.AppendIntervalIndex(tagIndex[0])
			tagIndex = tagIndex[1:]
			continue
		} else if time[i]/int64(duration) != time[i-1]/int64(duration) {
			c.AppendIntervalIndex(i)
		}
	}
	return
}
