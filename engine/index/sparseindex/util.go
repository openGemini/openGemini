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

package sparseindex

import (
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const Empty = "empty"
const Continuous = "continuous"

func genRPNElementByOp(logicalOp influxql.Token, value *FieldRef, res *RPNElement) bool {
	switch logicalOp {
	case influxql.EQ:
		res.op = rpn.InRange
		res.rg = NewRange(value, value, true, true)
	case influxql.NEQ:
		res.op = rpn.NotInRange
		res.rg = NewRange(value, value, true, true)
	case influxql.LT:
		res.op = rpn.InRange
		res.rg = createRightBounded(value, false, false)
	case influxql.GT:
		res.op = rpn.InRange
		res.rg = createLeftBounded(value, false, false)
	case influxql.LTE:
		res.op = rpn.InRange
		res.rg = createRightBounded(value, true, false)
	case influxql.GTE:
		res.op = rpn.InRange
		res.rg = createLeftBounded(value, true, false)
	case influxql.IN:
		res.op = rpn.InSet
	case influxql.MATCHPHRASE:
		res.op = rpn.InRange
		res.rg = NewRange(value, value, true, true)
	default:
		res.op = rpn.UNKNOWN
		return false
	}
	return true
}

type setIndex struct {
}

func (si *setIndex) Not() bool {
	return false
}

func (si *setIndex) checkInRange(_ []*Range, _ []int, _ bool) Mark {
	return NewMark(false, false)
}

type FunctionBase struct {
}

// RPNElement means that Reverse Polish notation (RPN) is a method for conveying mathematical expressions
// without the use of separators such as brackets and parentheses. In this notation, the operators follow
// their operands, hence removing the need for brackets to define evaluation priority.
// More details: https://en.wikipedia.org/wiki/Reverse_Polish_notation.
type RPNElement struct {
	op        rpn.Op
	rg        *Range
	setIndex  *setIndex
	keyColumn int
	// monotonicChains as a chain of possibly monotone functions.
	// if the key column is wrapped in functions that can be monotonous in some value ranges.
	// such as (2023.06.01, 2023.06.15) -> toMonth() -> [202306, 202306]
	monotonicChains []*FunctionBase
}

func NewRPNElement(op rpn.Op) *RPNElement {
	return &RPNElement{op: op}
}

type IndexProperty struct {
	RowsNumPerFragment  int
	CoarseIndexFragment int
	MinRowsForSeek      int
}

func NewIndexProperty(rowsNumPerFragment, coarseIndexFragment, minRowsForSeek int) *IndexProperty {
	return &IndexProperty{
		RowsNumPerFragment:  rowsNumPerFragment,
		CoarseIndexFragment: coarseIndexFragment,
		MinRowsForSeek:      minRowsForSeek,
	}
}

func getDataTypesFromPk(pk record.Schemas) []int {
	dataTypes := make([]int, len(pk))
	for i := range pk {
		dataTypes[i] = pk[i].Type
	}
	return dataTypes
}

type ColumnRef struct {
	name     string
	dataType int
	column   *record.ColVal
}

func NewColumnRef(name string, dataType int, column *record.ColVal) *ColumnRef {
	return &ColumnRef{name: name, dataType: dataType, column: column}
}

func genIndexColumnsBySchema(schemas record.Schemas) []*ColumnRef {
	indexColumns := make([]*ColumnRef, len(schemas))
	colVals := make([]record.ColVal, len(schemas))
	for i := range indexColumns {
		indexColumns[i] = &ColumnRef{name: schemas[i].Name, dataType: schemas[i].Type, column: &colVals[i]}
	}
	return indexColumns
}

func getSearchStatus(fr fragment.FragmentRanges) string {
	if len(fr) == 0 {
		return Empty
	}
	return Continuous + ": " + fr.String()
}
