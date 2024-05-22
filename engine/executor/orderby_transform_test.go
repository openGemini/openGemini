/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func buildOrderByTransformRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)
	return rowDataType
}

func buildOrderByTransformInChunk() executor.Chunk {
	rowDataType := buildOrderByTransformRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("tag1")
	chunk.AppendTimes([]int64{1, 3, 4})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=tag1val,tag2=tag2val"), 0)
	chunk.AddIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{1.1, 3.3, 4.4}, []bool{true, true, true})
	return chunk
}

func TestOrderByTransformInitPromDims1(t *testing.T) {
	inRowDataType := buildOrderByTransformRowDataType()
	outRowDataType := inRowDataType
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "val0", Type: influxql.Float},
		},
	}
	chunk := buildOrderByTransformInChunk()
	opt1 := query.ProcessorOptions{
		PromQuery:      true,
		GroupByAllDims: true,
	}
	trans1 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt1, nil)
	trans1.GetTagAndIndexes(chunk)
	assert.Equal(t, trans1.GetCurrTags(0), "\x04\x00\x05\x00\r\x00\x12\x00\x1a\x00tag1\x00tag1val\x00tag2\x00tag2val\x00")
	opt2 := query.ProcessorOptions{
		PromQuery:  true,
		Without:    true,
		Dimensions: []string{"tag1"},
	}
	trans2 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt2, nil)
	trans2.GetTagAndIndexes(chunk)
	assert.Equal(t, trans2.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag2\x00tag2val\x00")
	opt3 := query.ProcessorOptions{
		PromQuery:  true,
		Dimensions: []string{"tag1"},
	}
	trans3 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt3, opt3.Dimensions)
	trans3.GetTagAndIndexes(chunk)
	assert.Equal(t, trans3.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag1\x00tag1val\x00")
}

func TestOrderByTransformInitPromDims2(t *testing.T) {
	inRowDataType := buildOrderByTransformRowDataType()
	outRowDataType := inRowDataType
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "val0", Type: influxql.Float},
		},
	}
	chunk := buildOrderByTransformInChunk()
	opt1 := query.ProcessorOptions{
		PromQuery:      true,
		GroupByAllDims: true,
	}
	trans1 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt1, nil)
	trans1.GetTagsResetTagIndexes(chunk)
	assert.Equal(t, trans1.GetCurrTags(0), "\x04\x00\x05\x00\r\x00\x12\x00\x1a\x00tag1\x00tag1val\x00tag2\x00tag2val\x00")
	opt2 := query.ProcessorOptions{
		PromQuery:  true,
		Without:    true,
		Dimensions: []string{"tag1"},
	}
	trans2 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt2, nil)
	trans2.GetTagsResetTagIndexes(chunk)
	assert.Equal(t, trans2.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag2\x00tag2val\x00")
	opt3 := query.ProcessorOptions{
		PromQuery:  true,
		Dimensions: []string{"tag1"},
	}
	trans3 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt3, opt3.Dimensions)
	trans3.GetTagsResetTagIndexes(chunk)
	assert.Equal(t, trans3.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag1\x00tag1val\x00")
}
