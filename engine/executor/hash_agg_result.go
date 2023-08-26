// Generated by tmpl
// https://github.com/benbjohnson/tmpl
//
// DO NOT EDIT!
// Source: hash_agg_result.go.tmpl

/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package executor

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

type AggFuncType uint32

const (
	sumFunc AggFuncType = iota
	countFunc
	firstFunc
	lastFunc
	minFunc
	maxFunc
	percentileFunc
)

type NewAggOperator func() aggOperator
type aggFunc struct {
	funcType         AggFuncType
	newAggOperatorFn NewAggOperator
	inIdx            int
	outIdx           int
	percentile       float64
}

func NewAggFunc(aggType AggFuncType, fn NewAggOperator, inIdx int, outIdx int, p float64) *aggFunc {
	return &aggFunc{
		funcType:         aggType,
		newAggOperatorFn: fn,
		inIdx:            inIdx,
		outIdx:           outIdx,
		percentile:       p,
	}
}

func (af *aggFunc) NewAggOperator() aggOperator {
	return af.newAggOperatorFn()
}

type aggOperator interface {
	Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error
	SetOutVal(c Chunk, colLoc int, para any)
	SetNullFill(oc Chunk, colLoc int, time int64)
	SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64)
}

type aggOperatorMsg struct {
	results           []aggOperator
	intervalStartTime int64 // interval time
	time              int64 // true time
}

type sumAggOperator4Float struct {
	val float64 // sum
}

func NewSumAggOperator4Float() aggOperator {
	return &sumAggOperator4Float{
		val: 0,
	}
}

func (s *sumAggOperator4Float) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		s.val += c.Column(colLoc).FloatValue(startRowLoc)
	}
	return nil
}

func (s *sumAggOperator4Float) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendFloatValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

func (s *sumAggOperator4Float) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *sumAggOperator4Float) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToFloat(fillVal)
	oc.Column(colLoc).AppendFloatValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type sumAggOperator4Integer struct {
	val int64 // sum
}

func NewSumAggOperator4Integer() aggOperator {
	result := &sumAggOperator4Integer{
		val: 0,
	}
	return result
}

func (s *sumAggOperator4Integer) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		s.val += c.Column(colLoc).IntegerValue(startRowLoc)
	}
	return nil
}

func (s *sumAggOperator4Integer) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendIntegerValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

func (s *sumAggOperator4Integer) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *sumAggOperator4Integer) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToInteger(fillVal)
	oc.Column(colLoc).AppendIntegerValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type countAggOperator struct {
	val int64 // count
}

func NewCountAggOperator() aggOperator {
	result := &countAggOperator{
		val: 0,
	}
	return result
}

func (s *countAggOperator) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	s.val += int64(endRowLoc) - int64(startRowLoc)
	return nil
}

func (s *countAggOperator) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendIntegerValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

func (s *countAggOperator) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *countAggOperator) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToInteger(fillVal)
	oc.Column(colLoc).AppendIntegerValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type firstAggOperator4Float struct {
	val     float64 // first
	time    int64
	loc     int
	nilFlag bool
}

func NewFirstAggOperator4Float() aggOperator {
	return &firstAggOperator4Float{
		val:     0,
		time:    influxql.MaxTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *firstAggOperator4Float) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newFirst := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) < s.time {
			s.time = c.TimeByIndex(startRowLoc)
			s.loc = startRowLoc
			newFirst = true
		}
	}
	if !newFirst {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).FloatValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *firstAggOperator4Float) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendFloatValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *firstAggOperator4Float) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *firstAggOperator4Float) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToFloat(fillVal)
	oc.Column(colLoc).AppendFloatValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type firstAggOperator4Integer struct {
	val     int64 // first
	time    int64
	loc     int
	nilFlag bool
}

func NewFirstAggOperator4Integer() aggOperator {
	return &firstAggOperator4Integer{
		val:     0,
		time:    influxql.MaxTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *firstAggOperator4Integer) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newFirst := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) < s.time {
			s.time = c.TimeByIndex(startRowLoc)
			s.loc = startRowLoc
			newFirst = true
		}
	}
	if !newFirst {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).IntegerValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *firstAggOperator4Integer) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendIntegerValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *firstAggOperator4Integer) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *firstAggOperator4Integer) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToInteger(fillVal)
	oc.Column(colLoc).AppendIntegerValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type firstAggOperator4String struct {
	val     string // first
	time    int64
	loc     int
	nilFlag bool
}

func NewFirstAggOperator4String() aggOperator {
	return &firstAggOperator4String{
		val:     "",
		time:    influxql.MaxTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *firstAggOperator4String) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newFirst := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) < s.time {
			s.time = c.TimeByIndex(startRowLoc)
			s.loc = startRowLoc
			newFirst = true
		}
	}
	if !newFirst {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).StringValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *firstAggOperator4String) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendStringValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *firstAggOperator4String) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *firstAggOperator4String) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToString(fillVal)
	oc.Column(colLoc).AppendStringValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type firstAggOperator4Boolean struct {
	val     bool // first
	time    int64
	loc     int
	nilFlag bool
}

func NewFirstAggOperator4Boolean() aggOperator {
	return &firstAggOperator4Boolean{
		val:     false,
		time:    influxql.MaxTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *firstAggOperator4Boolean) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newFirst := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) < s.time {
			s.time = c.TimeByIndex(startRowLoc)
			s.loc = startRowLoc
			newFirst = true
		}
	}
	if !newFirst {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).BooleanValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *firstAggOperator4Boolean) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendBooleanValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *firstAggOperator4Boolean) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *firstAggOperator4Boolean) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToBoolean(fillVal)
	oc.Column(colLoc).AppendBooleanValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type lastAggOperator4Float struct {
	val     float64
	time    int64
	loc     int
	nilFlag bool
}

func NewLastAggOperator4Float() aggOperator {
	return &lastAggOperator4Float{
		val:     0,
		time:    influxql.MinTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *lastAggOperator4Float) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newLast := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) > s.time {
			s.loc = startRowLoc
			s.time = c.TimeByIndex(startRowLoc)
			newLast = true
		}
	}
	if !newLast {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).FloatValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *lastAggOperator4Float) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendFloatValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *lastAggOperator4Float) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *lastAggOperator4Float) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToFloat(fillVal)
	oc.Column(colLoc).AppendFloatValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type lastAggOperator4Integer struct {
	val     int64
	time    int64
	loc     int
	nilFlag bool
}

func NewLastAggOperator4Integer() aggOperator {
	return &lastAggOperator4Integer{
		val:     0,
		time:    influxql.MinTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *lastAggOperator4Integer) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newLast := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) > s.time {
			s.loc = startRowLoc
			s.time = c.TimeByIndex(startRowLoc)
			newLast = true
		}
	}
	if !newLast {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).IntegerValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *lastAggOperator4Integer) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendIntegerValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *lastAggOperator4Integer) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *lastAggOperator4Integer) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToInteger(fillVal)
	oc.Column(colLoc).AppendIntegerValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type lastAggOperator4String struct {
	val     string
	time    int64
	loc     int
	nilFlag bool
}

func NewLastAggOperator4String() aggOperator {
	return &lastAggOperator4String{
		val:     "",
		time:    influxql.MinTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *lastAggOperator4String) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newLast := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) > s.time {
			s.loc = startRowLoc
			s.time = c.TimeByIndex(startRowLoc)
			newLast = true
		}
	}
	if !newLast {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).StringValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *lastAggOperator4String) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendStringValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *lastAggOperator4String) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *lastAggOperator4String) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToString(fillVal)
	oc.Column(colLoc).AppendStringValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type lastAggOperator4Boolean struct {
	val     bool
	time    int64
	loc     int
	nilFlag bool
}

func NewLastAggOperator4Boolean() aggOperator {
	return &lastAggOperator4Boolean{
		val:     false,
		time:    influxql.MinTime,
		loc:     0,
		nilFlag: true,
	}
}

func (s *lastAggOperator4Boolean) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	newLast := false
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		if c.TimeByIndex(startRowLoc) > s.time {
			s.loc = startRowLoc
			s.time = c.TimeByIndex(startRowLoc)
			newLast = true
		}
	}
	if !newLast {
		return nil
	}
	if !c.Column(colLoc).IsNilV2(s.loc) {
		rowLoc := c.Column(colLoc).GetValueIndexV2(s.loc)
		s.val = c.Column(colLoc).BooleanValue(rowLoc)
		s.nilFlag = false
	} else {
		s.nilFlag = true
	}
	return nil
}

func (s *lastAggOperator4Boolean) SetOutVal(c Chunk, colLoc int, _ any) {
	c.Column(colLoc).AppendColumnTime(s.time)
	if !s.nilFlag {
		c.Column(colLoc).AppendBooleanValue(s.val)
		c.Column(colLoc).AppendNotNil()
	} else {
		c.Column(colLoc).AppendNil()
	}
}

func (s *lastAggOperator4Boolean) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendColumnTime(s.time)
	oc.Column(colLoc).AppendNil()
}

func (s *lastAggOperator4Boolean) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToBoolean(fillVal)
	oc.Column(colLoc).AppendBooleanValue(val)
	oc.Column(colLoc).AppendNotNil()
	oc.Column(colLoc).AppendColumnTime(time)
}

type minAggOperator4Float struct {
	val     float64
	nilFlag bool
}

func NewMinAggOperator4Float() aggOperator {
	return &minAggOperator4Float{
		val:     math.MaxFloat64,
		nilFlag: true,
	}
}

func (s *minAggOperator4Float) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		val := c.Column(colLoc).FloatValue(startRowLoc)
		if val < s.val {
			s.val = val
			s.nilFlag = false
		}
	}
	return nil
}

func (s *minAggOperator4Float) SetOutVal(c Chunk, colLoc int, _ any) {
	if s.nilFlag {
		c.Column(colLoc).AppendManyNil(1)
		return
	}
	c.Column(colLoc).AppendFloatValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

func (s *minAggOperator4Float) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *minAggOperator4Float) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToFloat(fillVal)
	oc.Column(colLoc).AppendFloatValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type minAggOperator4Integer struct {
	val     int64
	nilFlag bool
}

func NewMinAggOperator4Integer() aggOperator {
	return &minAggOperator4Integer{
		val:     math.MaxInt64,
		nilFlag: true,
	}
}

func (s *minAggOperator4Integer) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		val := c.Column(colLoc).IntegerValue(startRowLoc)
		if val < s.val {
			s.val = val
			s.nilFlag = false
		}
	}
	return nil
}

func (s *minAggOperator4Integer) SetOutVal(c Chunk, colLoc int, _ any) {
	if s.nilFlag {
		c.Column(colLoc).AppendManyNil(1)
		return
	}
	c.Column(colLoc).AppendIntegerValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

func (s *minAggOperator4Integer) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *minAggOperator4Integer) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToInteger(fillVal)
	oc.Column(colLoc).AppendIntegerValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type maxAggOperator4Float struct {
	val     float64
	nilFlag bool
}

func NewMaxAggOperator4Float() aggOperator {
	return &maxAggOperator4Float{
		val:     -math.MaxFloat64,
		nilFlag: true,
	}
}

func (s *maxAggOperator4Float) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		val := c.Column(colLoc).FloatValue(startRowLoc)
		if val > s.val {
			s.val = val
			s.nilFlag = false
		}
	}
	return nil
}

func (s *maxAggOperator4Float) SetOutVal(c Chunk, colLoc int, _ any) {
	if s.nilFlag {
		c.Column(colLoc).AppendManyNil(1)
		return
	}
	c.Column(colLoc).AppendFloatValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

func (s *maxAggOperator4Float) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *maxAggOperator4Float) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToFloat(fillVal)
	oc.Column(colLoc).AppendFloatValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type maxAggOperator4Integer struct {
	val     int64
	nilFlag bool
}

func NewMaxAggOperator4Integer() aggOperator {
	return &maxAggOperator4Integer{
		val:     math.MinInt64,
		nilFlag: true,
	}
}

func (s *maxAggOperator4Integer) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	for ; startRowLoc < endRowLoc; startRowLoc++ {
		val := c.Column(colLoc).IntegerValue(startRowLoc)
		if val > s.val {
			s.val = val
			s.nilFlag = false
		}
	}
	return nil
}

func (s *maxAggOperator4Integer) SetOutVal(c Chunk, colLoc int, _ any) {
	if s.nilFlag {
		c.Column(colLoc).AppendManyNil(1)
		return
	}
	c.Column(colLoc).AppendIntegerValue(s.val)
	c.Column(colLoc).AppendNotNil()
}

func (s *maxAggOperator4Integer) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *maxAggOperator4Integer) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToInteger(fillVal)
	oc.Column(colLoc).AppendIntegerValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type percentileAggOperator4Float struct {
	val []float64
}

func NewPercentileAggOperator4Float() aggOperator {
	return &percentileAggOperator4Float{
		val: make([]float64, 0),
	}
}

func (s *percentileAggOperator4Float) Len() int {
	return len(s.val)
}

func (s *percentileAggOperator4Float) Less(i, j int) bool {
	return s.val[i] < s.val[j]
}

func (s *percentileAggOperator4Float) Swap(i, j int) {
	s.val[i], s.val[j] = s.val[j], s.val[i]
}

func (s *percentileAggOperator4Float) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	s.val = append(s.val, c.Column(colLoc).FloatValues()[startRowLoc:endRowLoc]...)
	return nil
}

func (s *percentileAggOperator4Float) SetOutVal(c Chunk, colLoc int, percentile any) {
	if len(s.val) == 0 {
		c.Column(colLoc).AppendNil()
		return
	}
	sort.Sort(s)
	i := int(math.Floor(float64(len(s.val))*(percentile.(float64))/100.0+0.5)) - 1
	if i < 0 || i >= len(s.val) {
		c.Column(colLoc).AppendNil()
		return
	}
	c.Column(colLoc).AppendFloatValue(s.val[i])
	c.Column(colLoc).AppendNotNil()
}

func (s *percentileAggOperator4Float) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *percentileAggOperator4Float) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToFloat(fillVal)
	oc.Column(colLoc).AppendFloatValue(val)
	oc.Column(colLoc).AppendNotNil()
}

type percentileAggOperator4Integer struct {
	val []int64
}

func NewPercentileAggOperator4Integer() aggOperator {
	return &percentileAggOperator4Integer{
		val: make([]int64, 0),
	}
}

func (s *percentileAggOperator4Integer) Len() int {
	return len(s.val)
}

func (s *percentileAggOperator4Integer) Less(i, j int) bool {
	return s.val[i] < s.val[j]
}

func (s *percentileAggOperator4Integer) Swap(i, j int) {
	s.val[i], s.val[j] = s.val[j], s.val[i]
}

func (s *percentileAggOperator4Integer) Compute(c Chunk, colLoc int, startRowLoc int, endRowLoc int) error {
	if c.Column(colLoc).NilCount() != 0 {
		startRowLoc, endRowLoc = c.Column(colLoc).GetRangeValueIndexV2(startRowLoc, endRowLoc)
	}
	s.val = append(s.val, c.Column(colLoc).IntegerValues()[startRowLoc:endRowLoc]...)
	return nil
}

func (s *percentileAggOperator4Integer) SetOutVal(c Chunk, colLoc int, percentile any) {
	if len(s.val) == 0 {
		c.Column(colLoc).AppendNil()
		return
	}
	sort.Sort(s)
	i := int(math.Floor(float64(len(s.val))*(percentile.(float64))/100.0+0.5)) - 1
	if i < 0 || i >= len(s.val) {
		c.Column(colLoc).AppendNil()
		return
	}
	c.Column(colLoc).AppendIntegerValue(s.val[i])
	c.Column(colLoc).AppendNotNil()
}

func (s *percentileAggOperator4Integer) SetNullFill(oc Chunk, colLoc int, time int64) {
	oc.Column(colLoc).AppendNil()
}

func (s *percentileAggOperator4Integer) SetNumFill(oc Chunk, colLoc int, fillVal interface{}, time int64) {
	val, _ := hybridqp.TransToInteger(fillVal)
	oc.Column(colLoc).AppendIntegerValue(val)
	oc.Column(colLoc).AppendNotNil()
}

func NewCountFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for count iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer, influxql.Float, influxql.String, influxql.Boolean, influxql.Tag:
		return NewAggFunc(countFunc, NewCountAggOperator, inOrdinal, outOrdinal, 0), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "count/mean", dataType.String())
	}
}

func NewSumFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for sum iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewAggFunc(sumFunc, NewSumAggOperator4Integer, inOrdinal, outOrdinal, 0), nil
	case influxql.Float:
		return NewAggFunc(sumFunc, NewSumAggOperator4Float, inOrdinal, outOrdinal, 0), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "sum/mean", dataType.String())
	}
}

func NewFirstFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for first iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewAggFunc(firstFunc, NewFirstAggOperator4Integer, inOrdinal, outOrdinal, 0), nil
	case influxql.Float:
		return NewAggFunc(firstFunc, NewFirstAggOperator4Float, inOrdinal, outOrdinal, 0), nil
	case influxql.String, influxql.Tag:
		return NewAggFunc(firstFunc, NewFirstAggOperator4String, inOrdinal, outOrdinal, 0), nil
	case influxql.Boolean:
		return NewAggFunc(firstFunc, NewFirstAggOperator4Boolean, inOrdinal, outOrdinal, 0), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "first", dataType.String())
	}
}

func NewLastFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for last iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewAggFunc(lastFunc, NewLastAggOperator4Integer, inOrdinal, outOrdinal, 0), nil
	case influxql.Float:
		return NewAggFunc(lastFunc, NewLastAggOperator4Float, inOrdinal, outOrdinal, 0), nil
	case influxql.String, influxql.Tag:
		return NewAggFunc(lastFunc, NewLastAggOperator4String, inOrdinal, outOrdinal, 0), nil
	case influxql.Boolean:
		return NewAggFunc(lastFunc, NewLastAggOperator4Boolean, inOrdinal, outOrdinal, 0), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "last", dataType.String())
	}
}

func NewMinFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for min iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewAggFunc(minFunc, NewMinAggOperator4Integer, inOrdinal, outOrdinal, 0), nil
	case influxql.Float:
		return NewAggFunc(minFunc, NewMinAggOperator4Float, inOrdinal, outOrdinal, 0), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "min", dataType.String())
	}
}

func NewMaxFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for max iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewAggFunc(maxFunc, NewMaxAggOperator4Integer, inOrdinal, outOrdinal, 0), nil
	case influxql.Float:
		return NewAggFunc(maxFunc, NewMaxAggOperator4Float, inOrdinal, outOrdinal, 0), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "max", dataType.String())
	}
}

func NewPercentileFunc(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (*aggFunc, error) {
	var percentile float64
	switch arg := opt.Expr.(*influxql.Call).Args[1].(type) {
	case *influxql.NumberLiteral:
		percentile = arg.Val
	case *influxql.IntegerLiteral:
		percentile = float64(arg.Val)
	default:
		return nil, fmt.Errorf("the type of input args of percentile iterator is unsupported")
	}
	if percentile < 0 || percentile > 100 {
		return nil, errors.New("invalid percentile, the value range must be 0 to 100")
	}
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for Percentile iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewAggFunc(percentileFunc, NewPercentileAggOperator4Integer, inOrdinal, outOrdinal, percentile), nil
	case influxql.Float:
		return NewAggFunc(percentileFunc, NewPercentileAggOperator4Float, inOrdinal, outOrdinal, percentile), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "Percentile", dataType.String())
	}
}
