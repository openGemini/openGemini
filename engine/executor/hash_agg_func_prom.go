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
	"fmt"
	"math"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

var newPromFunc map[string]func(hybridqp.RowDataType, hybridqp.RowDataType, hybridqp.ExprOptions) (*aggFunc, error)

func init() {
	newPromFunc = make(map[string]func(hybridqp.RowDataType, hybridqp.RowDataType, hybridqp.ExprOptions) (*aggFunc, error))
	newPromFunc["count_prom"] = NewCountPromFunc
	newPromFunc["min_prom"] = NewMinPromFunc
	newPromFunc["max_prom"] = NewMaxPromFunc
	newPromFunc["stdvar_prom"] = NewStdvarPromFunc
	newPromFunc["stddev_prom"] = NewStddevPromFunc
	newPromFunc["group_prom"] = NewGroupPromFunc
}

func GetOrdinal(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (int, int) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	return inOrdinal, outOrdinal
}

func NewPromFunc(inRowDataType hybridqp.RowDataType, typ AggFuncType, op func() aggOperator, inOrdinal, outOrdinal int) (*aggFunc, error) {
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewAggFunc(typ, op, inOrdinal, outOrdinal, 0), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "prom", dataType.String())
	}
}

func NewCountPromFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal, outOrdinal := GetOrdinal(inRowDataType, outRowDataType, opt)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for count_prom iterator")
	}
	return NewPromFunc(inRowDataType, countPromFunc, NewCountPromOperator, inOrdinal, outOrdinal)
}

func NewMinPromFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal, outOrdinal := GetOrdinal(inRowDataType, outRowDataType, opt)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for min_prom iterator")
	}
	return NewPromFunc(inRowDataType, minPromFunc, NewMinPromOperator, inOrdinal, outOrdinal)
}

func NewMaxPromFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal, outOrdinal := GetOrdinal(inRowDataType, outRowDataType, opt)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for max_prom iterator")
	}
	return NewPromFunc(inRowDataType, maxPromFunc, NewMaxPromOperator, inOrdinal, outOrdinal)
}

func NewStdvarPromFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal, outOrdinal := GetOrdinal(inRowDataType, outRowDataType, opt)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for stdvar_prom iterator")
	}
	return NewPromFunc(inRowDataType, stdvarPromFunc, func() aggOperator { return NewStdPromOperator(false) }, inOrdinal, outOrdinal)
}

func NewStddevPromFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal, outOrdinal := GetOrdinal(inRowDataType, outRowDataType, opt)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for stddev_prom iterator")
	}
	return NewPromFunc(inRowDataType, stddevPromFunc, func() aggOperator { return NewStdPromOperator(true) }, inOrdinal, outOrdinal)
}

func NewGroupPromFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal, outOrdinal := GetOrdinal(inRowDataType, outRowDataType, opt)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for group_prom iterator")
	}
	return NewPromFunc(inRowDataType, groupPromFunc, NewGroupPromOperator, inOrdinal, outOrdinal)
}

type countPromOperator struct {
	val int64 // count_prom
}

func NewCountPromOperator() aggOperator {
	return &countPromOperator{
		val: 0,
	}
}

func (s *countPromOperator) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int, _ any) error {
	s.val += int64(endRowLoc) - int64(startRowLoc)
	return nil
}

func (s *countPromOperator) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendFloatValue(float64(s.val))
	c.Column(colLoc).AppendNotNil()
}

// not use
func (s *countPromOperator) SetNullFill(oc Chunk, colLoc int, time int64) {
}

func (s *countPromOperator) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
}

func (s *countPromOperator) GetTime() int64 {
	return DefaultTime
}

type minPromOperator struct {
	val float64
}

func NewMinPromOperator() aggOperator {
	return &minPromOperator{
		val: math.MaxFloat64,
	}
}

func (s *minPromOperator) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int, _ any) error {
	vs := c.Column(colLoc).FloatValues()[startRowLoc:endRowLoc]
	for i := 0; i < endRowLoc-startRowLoc; i++ {
		if vs[i] < s.val || math.IsNaN(s.val) {
			s.val = vs[i]
		}
	}
	return nil
}

func (s *minPromOperator) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendFloatValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

// not use
func (s *minPromOperator) SetNullFill(oc Chunk, colLoc int, time int64) {
}

func (s *minPromOperator) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
}

func (s *minPromOperator) GetTime() int64 {
	return DefaultTime
}

type maxPromOperator struct {
	val float64
}

func NewMaxPromOperator() aggOperator {
	return &maxPromOperator{
		val: -math.MaxFloat64,
	}
}

func (s *maxPromOperator) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int, _ any) error {
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		val := c.Column(colLoc).FloatValue(startRowLoc)
		if val > s.val || math.IsNaN(s.val) {
			s.val = val
		}
	}
	return nil
}

func (s *maxPromOperator) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendFloatValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

// not use
func (s *maxPromOperator) SetNullFill(oc Chunk, colLoc int, time int64) {
}

func (s *maxPromOperator) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
}

func (s *maxPromOperator) GetTime() int64 {
	return DefaultTime
}

type stdPromOperator struct {
	count      int
	floatMean  float64
	floatValue float64
	val        float64
	isStddev   bool // true: calculate standard deviation, false: calculate variance.
}

func NewStdPromOperator(flag bool) aggOperator {
	return &stdPromOperator{
		isStddev: flag,
	}
}

func (s *stdPromOperator) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int, _ any) error {
	for i := startRowLoc; i < endRowLoc; i++ {
		val := c.Column(colLoc).FloatValue(i)
		if s.count == 0 {
			s.floatMean = val
		}
		s.count++
		delta := val - s.floatMean
		s.floatMean += delta / float64(s.count)
		s.floatValue += delta * (val - s.floatMean)
	}
	if s.count <= 1 {
		s.val = 0
	} else {
		s.val = s.floatValue / float64(s.count)
		if s.isStddev {
			s.val = math.Sqrt(s.val)
		}
	}
	return nil
}

func (s *stdPromOperator) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendFloatValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

type groupPromOperator struct {
	hasVal bool
}

func NewGroupPromOperator() aggOperator {
	return &groupPromOperator{}
}

func (s *groupPromOperator) Compute(c Chunk, colLoc, startRowLoc, endRowLoc int, _ any) error {
	s.hasVal = s.hasVal || (startRowLoc < endRowLoc)
	return nil
}

func (s *groupPromOperator) SetOutVal(c Chunk, colLoc int, _ any) {
	if s.hasVal {
		c.Column(colLoc).AppendFloatValue(1)
		c.Column(colLoc).AppendNotNil()
	}
}

// not use
func (s *stdPromOperator) SetNullFill(oc Chunk, colLoc int, time int64) {
}

func (s *stdPromOperator) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
}

func (s *stdPromOperator) GetTime() int64 {
	return DefaultTime
}

func (s *groupPromOperator) SetNullFill(oc Chunk, colLoc int, time int64) {
}

func (s *groupPromOperator) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
}

func (s *groupPromOperator) GetTime() int64 {
	return DefaultTime
}
