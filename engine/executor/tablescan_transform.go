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

package executor

import (
	"context"
	"strconv"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

var RowSize = 1024

func generateBooleanData(column Column) {
	for i := 0; i < RowSize; i++ {
		column.AppendBooleanValue(i%2 == 0)
	}
}

func generateIntegerData(column Column) {
	for i := 0; i < RowSize; i++ {
		column.AppendIntegerValue(int64(i))
	}
}

func generateFloatData(column Column) {
	for i := 0; i < RowSize; i++ {
		column.AppendFloatValue(float64(i))
	}
}

func generateStringData(column Column) {
	for i := 0; i < RowSize; i++ {
		column.AppendStringValue(strconv.Itoa(i))
	}
}

func generateData4Chunk(chunk Chunk) {
	for _, c := range chunk.Columns() {
		switch c.DataType() {
		case influxql.Boolean:
			generateBooleanData(c)
		case influxql.Integer:
			generateIntegerData(c)
		case influxql.Float:
			generateFloatData(c)
		case influxql.String:
			generateStringData(c)
		default:
			panic("unsupport data type of column")
		}
	}
}

type TableScanFromSingleChunk struct {
	BaseProcessor

	Output  *ChunkPort
	Table   *QueryTable
	Builder *ChunkBuilder
	Chunk   Chunk
}

func NewTableScanFromSingleChunk(rowDataType hybridqp.RowDataType, table *QueryTable, _ query.ProcessorOptions) *TableScanFromSingleChunk {
	scan := &TableScanFromSingleChunk{
		Output: NewChunkPort(rowDataType),
		Table:  table,
	}

	scan.Builder = NewChunkBuilder(rowDataType)
	scan.Chunk = scan.Builder.NewChunk(scan.Table.Name())

	generateData4Chunk(scan.Chunk)

	return scan
}

func (scan *TableScanFromSingleChunk) Name() string {
	return "TableScanFromSingleChunk"
}

func (scan *TableScanFromSingleChunk) Explain() []ValuePair {
	var pairs []ValuePair

	pairs = append(pairs, ValuePair{First: "attr", Second: scan.Output.RowDataType.Fields().Names()})
	pairs = append(pairs, ValuePair{First: "table", Second: scan.Table.RowDataType().Fields().Names()})

	return pairs
}

func (scan *TableScanFromSingleChunk) Close() {
	scan.Output.Close()
}

func (scan *TableScanFromSingleChunk) Work(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if scan.Chunk == nil {
				scan.Output.Close()
				return nil
			}

			scan.Output.State <- scan.Chunk
			scan.Chunk = nil
		}
	}
}

func (scan *TableScanFromSingleChunk) GetOutputs() Ports {
	return Ports{scan.Output}
}

func (scan *TableScanFromSingleChunk) GetInputs() Ports {
	return Ports{}
}

func (scan *TableScanFromSingleChunk) GetOutputNumber(_ Port) int {
	return 0
}

func (scan *TableScanFromSingleChunk) GetInputNumber(_ Port) int {
	return INVALID_NUMBER
}
