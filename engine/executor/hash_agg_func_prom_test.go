// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"math"
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestHashAggTransUnusefulPromFn(t *testing.T) {
	countPromOp := NewCountPromOperator()
	countPromOp.SetNullFill(nil, 0, 0)
	countPromOp.SetNumFill(nil, 0, nil, 0)
	countPromOp.GetTime()
	minPromOp := NewMinPromOperator()
	minPromOp.SetNullFill(nil, 0, 0)
	minPromOp.SetNumFill(nil, 0, nil, 0)
	minPromOp.GetTime()
	maxPromOp := NewMaxPromOperator()
	maxPromOp.SetNullFill(nil, 0, 0)
	maxPromOp.SetNumFill(nil, 0, nil, 0)
	maxPromOp.GetTime()
	groupPromOp := NewGroupPromOperator()
	groupPromOp.SetNullFill(nil, 0, 0)
	groupPromOp.SetNumFill(nil, 0, nil, 0)
	groupPromOp.GetTime()
}

func TestNewGroupPromFunc_InvalidRowDataType(t *testing.T) {
	inRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
	)
	outRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val1", Type: influxql.Float},
	)
	exprOpt := hybridqp.ExprOptions{
		Expr: &influxql.Call{Name: "group", Args: []influxql.Expr{hybridqp.MustParseExpr("val")}},
		Ref:  influxql.VarRef{Val: "val", Type: influxql.Float},
	}
	_, err := NewGroupPromFunc(inRowDataType, outRowDataType, exprOpt)
	assert.NotNil(t, err)
}

func SetupTestEnvironment(flag bool, values []float64) (*stdPromOperator, Chunk) {
	stdPromOp := &stdPromOperator{
		count:      0,
		floatMean:  0.0,
		floatValue: 0.0,
		val:        0.0,
		isStddev:   flag,
	}
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
	)
	chunk := NewChunkBuilder(rowDataType).NewChunk("m1")
	chunk.Column(0).AppendFloatValues(values)
	return stdPromOp, chunk
}

func SetupTestEnvironment2(values []float64) (*groupPromOperator, Chunk) {
	grouoPromOp := NewGroupPromOperator()
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
	)
	chunk := NewChunkBuilder(rowDataType).NewChunk("m1")
	chunk.Column(0).AppendFloatValues(values)
	return grouoPromOp.(*groupPromOperator), chunk
}

func TestHashAggStdDevPromOperator_Compute(t *testing.T) {
	// Test case 1: Empty chunk.
	stdPromOp, chunk := SetupTestEnvironment(true, []float64{})
	stdPromOp.Compute(chunk, 0, 0, 0, nil)
	assert.Equal(t, float64(0), stdPromOp.val)

	// Test case 2: Single valid value.
	stdPromOp, chunk = SetupTestEnvironment(true, []float64{1.1})
	stdPromOp.Compute(chunk, 0, 0, 1, nil)
	assert.Equal(t, float64(0), stdPromOp.val)

	// Test case 3: Compute all values at once with multiple valid inputs.
	stdPromOp, chunk = SetupTestEnvironment(true, []float64{1.1, 2.2, 3.3, 4.4})
	stdPromOp.Compute(chunk, 0, 0, 4, nil)
	assert.Equal(t, float64(1.2298373876248845), stdPromOp.val)

	// Test case 4: Compute all values four times with multiple valid inputs.
	stdPromOp, chunk = SetupTestEnvironment(true, []float64{1.1, 2.2, 3.3, 4.4})
	stdPromOp.Compute(chunk, 0, 0, 1, nil)
	stdPromOp.Compute(chunk, 0, 1, 2, nil)
	stdPromOp.Compute(chunk, 0, 2, 3, nil)
	stdPromOp.Compute(chunk, 0, 3, 4, nil)
	assert.Equal(t, float64(1.2298373876248845), stdPromOp.val)

	// Test case 5: included NaN values.
	stdPromOp, chunk = SetupTestEnvironment(true, []float64{1.1, float64(math.NaN()), 3.3})
	stdPromOp.Compute(chunk, 0, 0, 3, nil)
	assert.True(t, math.IsNaN(stdPromOp.val))

	// Test case 6: included Inf values.
	stdPromOp, chunk = SetupTestEnvironment(true, []float64{1.1, float64(math.Inf(1)), float64(math.Inf(-1)), 3.3})
	stdPromOp.Compute(chunk, 0, 0, 4, nil)
	assert.True(t, math.IsNaN(stdPromOp.val))
}

func TestHashAggStdVarPromOperator_Compute(t *testing.T) {
	// Test case 1: Empty chunk.
	stdPromOp, chunk := SetupTestEnvironment(false, []float64{})
	stdPromOp.Compute(chunk, 0, 0, 0, nil)
	assert.Equal(t, float64(0), stdPromOp.val)

	// Test case 2: Single valid value.
	stdPromOp, chunk = SetupTestEnvironment(false, []float64{1.1})
	stdPromOp.Compute(chunk, 0, 0, 1, nil)
	assert.Equal(t, float64(0), stdPromOp.val)

	// Test case 3: Compute all values at once with multiple valid inputs.
	stdPromOp, chunk = SetupTestEnvironment(false, []float64{1.1, 2.2, 3.3, 4.4})
	stdPromOp.Compute(chunk, 0, 0, 4, nil)
	assert.Equal(t, float64(1.5125000000000002), stdPromOp.val)

	// Test case 4: Compute all values four times with multiple valid inputs.
	stdPromOp, chunk = SetupTestEnvironment(false, []float64{1.1, 2.2, 3.3, 4.4})
	stdPromOp.Compute(chunk, 0, 0, 1, nil)
	stdPromOp.Compute(chunk, 0, 1, 2, nil)
	stdPromOp.Compute(chunk, 0, 2, 3, nil)
	stdPromOp.Compute(chunk, 0, 3, 4, nil)
	assert.Equal(t, float64(1.5125000000000002), stdPromOp.val)

	// Test case 5: included NaN values.
	stdPromOp, chunk = SetupTestEnvironment(false, []float64{1.1, float64(math.NaN()), 3.3})
	stdPromOp.Compute(chunk, 0, 0, 3, nil)
	assert.True(t, math.IsNaN(stdPromOp.val))

	// Test case 6: included Inf values.
	stdPromOp, chunk = SetupTestEnvironment(false, []float64{1.1, float64(math.Inf(1)), float64(math.Inf(-1)), 3.3})
	stdPromOp.Compute(chunk, 0, 0, 4, nil)
	assert.True(t, math.IsNaN(stdPromOp.val))
}

func TestHashAggGroupPromOperator_Compute(t *testing.T) {
	data1 := []float64{1.0, 2.0, 3.0, 4.0}
	promOp, chunk := SetupTestEnvironment2(data1)
	promOp.Compute(chunk, 0, 0, len(data1), nil)
	assert.Equal(t, promOp.hasVal, true)

	data2 := []float64{1.0, math.NaN(), math.Inf(0)}
	promOp, chunk = SetupTestEnvironment2(data2)
	promOp.Compute(chunk, 0, 0, len(data2), nil)
	assert.Equal(t, promOp.hasVal, true)
}
