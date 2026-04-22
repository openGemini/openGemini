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
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type ChunkBuilder struct {
	rowDataType hybridqp.RowDataType
	dimDataType hybridqp.RowDataType
}

func NewChunkBuilder(rowDataType hybridqp.RowDataType) *ChunkBuilder {
	return &ChunkBuilder{
		rowDataType: rowDataType,
	}
}

func (b *ChunkBuilder) SetDim(dimDataType hybridqp.RowDataType) {
	b.dimDataType = dimDataType
}

func (b *ChunkBuilder) NewChunk(name string) Chunk {
	chunk := NewChunkImpl(b.rowDataType, name)
	for i := range b.rowDataType.Fields() {
		chunk.AddColumn(NewColumnImpl(b.rowDataType.Field(i).Expr.(*influxql.VarRef).Type))
	}
	if b.dimDataType == nil || b.dimDataType.NumColumn() == 0 {
		return chunk
	}
	for i := range b.dimDataType.Fields() {
		chunk.AddDim(NewColumnImpl(b.dimDataType.Field(i).Expr.(*influxql.VarRef).Type))
	}
	return chunk
}
