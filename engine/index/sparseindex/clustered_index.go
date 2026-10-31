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

package sparseindex

import (
	"errors"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// Error definitions
var (
	ErrInvalidRPN       = errors.New("invalid RPN expression")
	ErrInvalidRPNResult = errors.New("invalid RPN result count")
	ErrUnknownOperator  = func(op string) error { return fmt.Errorf("unknown operator: %s", op) }
	ErrEmptyRPN         = errors.New("RPN element should not be empty")
	ErrInvalidCondition = errors.New("invalid condition")
	ErrConditionType    = func(cType interface{}) error {
		return fmt.Errorf("invalid condition type: expected [string|float64|int64|boolean], got [%T]", cType)
	}
)

var EmptyRoaingBitMap = roaring.New()

// Condition represents a query condition (field name, operator, value)
type Condition struct {
	Field string
	Op    influxql.Token
	Value interface{}
}

// FieldCluster represents a clustered index for a specific field, organizing segments and documents by field values
type FieldCluster struct {
	// Mapping from field values to bitmaps of segment IDs containing the value
	ValueToSegments map[interface{}]*roaring.Bitmap
	// Stores the field values in sorted order
	SortedValues []interface{}
	FieldType    int
}

// NewFieldCluster creates a new FieldCluster instance
func NewFieldCluster(fieldType int) *FieldCluster {
	return &FieldCluster{
		ValueToSegments: make(map[interface{}]*roaring.Bitmap),
		SortedValues:    make([]interface{}, 0),
		FieldType:       fieldType,
	}
}

// ClusterIndex represents the top-level clustered index structure, organized by fields
type ClusterIndex struct {
	fieldClusters map[string]*FieldCluster // Mapping from field names to their FieldCluster instances
	maxSegmentID  uint32
	indexFragment fragment.IndexFragment
}

// NewClusterIndex creates a new ClusterIndex instance
func NewClusterIndex() *ClusterIndex {
	return &ClusterIndex{
		fieldClusters: make(map[string]*FieldCluster),
		maxSegmentID:  0,
	}
}

type FieldToWrite struct {
	fType  int
	fValue interface{}
}

// Write indexes a document into the specified segment
// data from the same field must be of the same type
func (ci *ClusterIndex) Write(segNumID uint32, fields map[string]*FieldToWrite) error {
	// Index each field-value pair
	for field, value := range fields {
		// Ensure the field cluster exists
		fieldCluster, fieldExists := ci.fieldClusters[field]

		if !fieldExists {
			ci.fieldClusters[field] = NewFieldCluster(value.fType)
			fieldCluster = ci.fieldClusters[field]
		}

		// Ensure the value's segment bitmap exists
		if _, exists := fieldCluster.ValueToSegments[value.fValue]; !exists {
			fieldCluster.ValueToSegments[value.fValue] = roaring.New()
			updateFieldSortedValues(fieldCluster, value.fValue)
		}

		// Add the segment ID to the value's bitmap
		fieldCluster.ValueToSegments[value.fValue].Add(segNumID)

		if segNumID > ci.maxSegmentID {
			ci.maxSegmentID = segNumID
		}
	}

	return nil
}

func updateFieldSortedValues(fieldCluster *FieldCluster, value interface{}) {
	// Insert value into SortedValues at the correct position to maintain order
	insertIndex := sort.Search(len(fieldCluster.SortedValues), func(i int) bool {
		return compareGreatEqual(fieldCluster.SortedValues[i], value)
	})
	fieldCluster.SortedValues = append(fieldCluster.SortedValues[:insertIndex], append([]interface{}{value}, fieldCluster.SortedValues[insertIndex:]...)...)
}

// a and b have the same type
func compareGreat(a, b interface{}) bool {
	switch aVal := a.(type) {
	case string:
		return aVal > b.(string)
	case int64:
		return aVal > b.(int64)
	case float64:
		return aVal > b.(float64)
	case bool:
		return aVal && !b.(bool)
	default:
		return false
	}
}

// a and b have the same type
func compareGreatEqual(a, b interface{}) bool {
	switch aVal := a.(type) {
	case string:
		return aVal >= b.(string)
	case int64:
		return aVal >= b.(int64)
	case float64:
		return aVal >= b.(float64)
	case bool:
		return aVal || !b.(bool)
	default:
		return false
	}
}

// WritePkRec indexes a primary record into the specified segment
func (ci *ClusterIndex) WritePkRec(pk *record.Record) error {
	if pk == nil || pk.RowNums() == 0 {
		return nil
	}

	fragIdx := pk.FieldIndexs(record.FragmentField)
	if fragIdx < 0 || fragIdx >= pk.Len() {
		return fmt.Errorf("invalid %s", record.FragmentField)
	}
	frags := pk.Column(fragIdx).IntegerValues()

	for i, field := range pk.Schema {
		if i == fragIdx {
			continue
		}
		fieldCluster, fieldExists := ci.fieldClusters[field.Name]
		if !fieldExists {
			ci.fieldClusters[field.Name] = NewFieldCluster(field.Type)
			fieldCluster = ci.fieldClusters[field.Name]
		}
		if err := ci.writeSegment(field, fieldCluster, pk.Column(i), frags); err != nil {
			return err
		}
	}
	return nil
}

// WritePkRecAndMark indexes a primary record into the specified segment
func (ci *ClusterIndex) WritePkRecAndMark(pk *record.Record, indexFrag fragment.IndexFragment) error {
	ci.indexFragment = indexFrag
	return ci.WritePkRec(pk)
}

func (ci *ClusterIndex) writeSegment(field record.Field, fieldCluster *FieldCluster, col *record.ColVal, frags []int64) error {
	var start uint32 = 0
	updateFieldCluster := func(value interface{}, fragIdx int) {
		// Ensure the value's segment bitmap exists
		if _, exists := fieldCluster.ValueToSegments[value]; !exists {
			fieldCluster.ValueToSegments[value] = roaring.New()
			updateFieldSortedValues(fieldCluster, value)
		}
		if fragIdx > 0 {
			start, _ = util.SplitInt64(frags[fragIdx-1])
		}
		end, _ := util.SplitInt64(frags[fragIdx])

		// Add the segment ID to the value's bitmap
		for segId := start; segId < end; segId++ {
			fieldCluster.ValueToSegments[value].Add(segId)
		}

		if uint32(end)-1 > ci.maxSegmentID {
			ci.maxSegmentID = uint32(end) - 1
		}
	}
	switch field.Type {
	case influx.Field_Type_Tag, influx.Field_Type_String:
		values := col.StringValues(nil)
		for i := range values {
			updateFieldCluster(values[i], i)
		}
	case influx.Field_Type_Int:
		values := col.IntegerValues()
		for i := range values {
			updateFieldCluster(values[i], i)
		}
	case influx.Field_Type_Float:
		values := col.FloatValues()
		for i := range values {
			updateFieldCluster(values[i], i)
		}
	case influx.Field_Type_Boolean:
		values := col.BooleanValues()
		for i := range values {
			updateFieldCluster(values[i], i)
		}
	default:
		return fmt.Errorf("invalid type: %v", field.Type)
	}
	return nil
}

// Query executes a query and returns the resulting segment IDs
func (ci *ClusterIndex) Query(expr influxql.Expr) (*roaring.Bitmap, error) {
	if expr == nil {
		bm := roaring.New()
		bm.AddRange(0, uint64(ci.maxSegmentID+1))
		return bm, nil
	}
	// Convert the expression to RPN
	rpn, err := ConvertToRPN(expr)
	if err != nil {
		return nil, err
	}

	// Evaluate the RPN expression
	resultSegments, err := ci.evaluateRPN(rpn)
	if err != nil {
		return nil, err
	}

	return resultSegments, nil
}

// GetRowCount executes a query and returns the row count
func (ci *ClusterIndex) GetRowCount(expr influxql.Expr) (uint64, error) {
	bits, err := ci.Query(expr)
	if err != nil {
		return 0, err
	}
	return getRowCountWithBitmap(bits, ci.indexFragment)
}

// BitmapToIDs Get IDs from Bitmap
func BitmapToIDs(bm *roaring.Bitmap) []int {
	if bm == nil || bm.IsEmpty() {
		return []int{}
	}

	ids := bm.ToArray()
	intIds := make([]int, len(ids))
	for i, v := range ids {
		intIds[i] = int(v)
	}

	return intIds
}

// BitmapToRanges Get Ranges from Bitmap
func BitmapToRanges(bm *roaring.Bitmap) fragment.FragmentRanges {
	if bm == nil || bm.IsEmpty() {
		return fragment.FragmentRanges(nil)
	}

	var ranges fragment.FragmentRanges
	var currentStart uint32
	var prevVal uint32
	first := true

	it := bm.Iterator()
	for it.HasNext() {
		val := it.Next()

		if first {
			currentStart = val
			prevVal = val
			first = false
		} else if val == prevVal+1 {
			prevVal = val
		} else {
			ranges = append(ranges, &fragment.FragmentRange{
				Start: currentStart,
				End:   prevVal + 1,
			})
			currentStart = val
			prevVal = val
		}
	}

	ranges = append(ranges, &fragment.FragmentRange{
		Start: currentStart,
		End:   prevVal + 1,
	})

	return ranges
}

func getRowCountWithFragment(start, end uint32, frag fragment.IndexFragment) (uint64, error) {
	fragRanges := frag.GetSegmentsFromFragmentRange()
	startIdx := sort.Search(len(fragRanges), func(i int) bool {
		return fragRanges[i].Start >= start
	})
	if !(startIdx < len(fragRanges) && fragRanges[startIdx].Start == start) {
		return 0, fmt.Errorf("not found start: %d", start)
	}
	endIdx := sort.Search(len(fragRanges[startIdx:]), func(i int) bool {
		return fragRanges[startIdx+i].End >= end
	})
	endIdx += startIdx
	if !(endIdx < len(fragRanges) && fragRanges[endIdx].End == end) {
		return 0, fmt.Errorf("not found end: %d", end)
	}
	return frag.GetRowsCountInRange(startIdx, endIdx+1), nil
}

func getRowCountWithBitmap(bm *roaring.Bitmap, frag fragment.IndexFragment) (uint64, error) {
	if bm == nil || bm.IsEmpty() {
		return 0, nil
	}
	var rowCount uint64
	var currentStart uint32
	var prevVal uint32
	first := true

	it := bm.Iterator()
	for it.HasNext() {
		val := it.Next()

		if first {
			currentStart = val
			prevVal = val
			first = false
		} else if val == prevVal+1 {
			prevVal = val
		} else {
			rowCnt, err := getRowCountWithFragment(currentStart, prevVal+1, frag)
			if err != nil {
				return 0, err
			}
			rowCount += rowCnt
			currentStart = val
			prevVal = val
		}
	}
	rowCnt, err := getRowCountWithFragment(currentStart, prevVal+1, frag)
	if err != nil {
		return 0, err
	}
	rowCount += rowCnt
	return rowCount, nil
}

// evaluateRPN evaluates a Reverse Polish Notation (RPN) expression
func (ci *ClusterIndex) evaluateRPN(rpn []interface{}) (*roaring.Bitmap, error) {
	var stack []*roaring.Bitmap
	for _, token := range rpn {
		switch t := token.(type) {
		case *Condition:
			// Evaluate the condition and push the result onto the stack
			segments, err := ci.evaluateCondition(t)
			if err != nil {
				return nil, err
			}
			stack = append(stack, segments)

		case influxql.Token:
			// Process the operator
			if len(stack) < 2 {
				return nil, ErrInvalidRPN
			}

			// Pop the top two elements from the stack
			right := stack[len(stack)-1]
			left := stack[len(stack)-2]
			stack = stack[:len(stack)-2]

			// Apply the operator to the two bitmaps
			var resultSegments *roaring.Bitmap
			switch t {
			case influxql.AND:
				resultSegments = roaring.And(left, right)
			case influxql.OR:
				resultSegments = roaring.Or(left, right)
			default:
				return nil, ErrUnknownOperator(t.String())
			}
			stack = append(stack, resultSegments)
		default:
			return nil, ErrInvalidRPN
		}
	}

	if len(stack) != 1 {
		return nil, ErrInvalidRPNResult
	}

	return stack[0], nil
}

// evaluateCondition evaluates a single condition and returns the matching segments
func (ci *ClusterIndex) evaluateCondition(cond *Condition) (*roaring.Bitmap, error) {
	fieldCluster, exists := ci.fieldClusters[cond.Field]
	if !exists {
		// Field not indexed, return all ranges
		bm := roaring.New()
		bm.AddRange(0, uint64(ci.maxSegmentID+1))
		return bm, nil
	}

	sortValues := fieldCluster.SortedValues
	valuesCount := len(fieldCluster.SortedValues)
	valuesBitmaps := fieldCluster.ValueToSegments

	switch v := cond.Value.(type) {
	case string:
		if fieldCluster.FieldType != influx.Field_Type_String {
			return nil, errno.NewError(errno.ErrRPNElemType, cond.Field, influx.FieldTypeName[fieldCluster.FieldType], cond.Op.String(), "String")
		}
	case int64:
		if fieldCluster.FieldType != influx.Field_Type_Int {
			return nil, errno.NewError(errno.ErrRPNElemType, cond.Field, influx.FieldTypeName[fieldCluster.FieldType], cond.Op.String(), "Integer")
		}
	case float64:
		if fieldCluster.FieldType != influx.Field_Type_Float {
			return nil, errno.NewError(errno.ErrRPNElemType, cond.Field, influx.FieldTypeName[fieldCluster.FieldType], cond.Op.String(), "Float")
		}
	case bool:
		if fieldCluster.FieldType != influx.Field_Type_Boolean {
			return nil, errno.NewError(errno.ErrRPNElemType, cond.Field, influx.FieldTypeName[fieldCluster.FieldType], cond.Op.String(), "Boolean")
		}
	case map[interface{}]bool:
		if !influx.IsBasicDataType(fieldCluster.FieldType) {
			return nil, errno.NewError(errno.ErrRPNElemType, cond.Field, influx.FieldTypeName[fieldCluster.FieldType], cond.Op.String(), "set")
		}
	default:
		return nil, ErrConditionType(v)
	}

	// Apply the operator
	switch cond.Op {
	case influxql.EQ:
		result, exists := valuesBitmaps[cond.Value]
		if !exists {
			return EmptyRoaingBitMap, nil
		}
		return result, nil
	case influxql.NEQ:
		bm := roaring.New()
		bm.AddRange(0, uint64(ci.maxSegmentID+1))
		result, exists := valuesBitmaps[cond.Value]
		if !exists {
			return bm, nil
		}
		return roaring.AndNot(bm, result), nil
	case influxql.GT:
		idx := sort.Search(valuesCount, func(i int) bool {
			return compareGreat(sortValues[i], cond.Value)
		})
		result := roaring.New()
		for i := idx; i < valuesCount; i++ {
			result = roaring.Or(result, valuesBitmaps[sortValues[i]])
		}
		return result, nil
	case influxql.GTE:
		idx := sort.Search(valuesCount, func(i int) bool {
			return compareGreatEqual(sortValues[i], cond.Value)
		})
		result := roaring.New()
		for i := idx; i < valuesCount; i++ {
			result = roaring.Or(result, valuesBitmaps[sortValues[i]])
		}
		return result, nil
	case influxql.LT:
		idx := sort.Search(valuesCount, func(i int) bool {
			return compareGreatEqual(sortValues[i], cond.Value)
		})
		result := roaring.New()
		for i := 0; i < idx; i++ {
			result = roaring.Or(result, valuesBitmaps[sortValues[i]])
		}
		return result, nil
	case influxql.LTE:
		idx := sort.Search(valuesCount, func(i int) bool {
			return compareGreat(sortValues[i], cond.Value)
		})
		result := roaring.New()
		for i := 0; i < idx; i++ {
			result = roaring.Or(result, valuesBitmaps[sortValues[i]])
		}
		return result, nil
	case influxql.IN:
		values, ok := cond.Value.(map[interface{}]bool)
		if !ok {
			return nil, fmt.Errorf("invalid cond value %v", cond.Value)
		}
		result := roaring.New()
		for value := range values {
			vBits, exists := valuesBitmaps[value]
			if !exists {
				continue
			}
			result = roaring.Or(result, vBits)
		}
		return result, nil
	default:
		return nil, ErrUnknownOperator(cond.Op.String())
	}
}

// getLiteralValue extracts the value from a literal expression
func getLiteralValue(expr influxql.Expr) (interface{}, error) {
	switch node := expr.(type) {
	case *influxql.StringLiteral:
		return node.Val, nil
	case *influxql.IntegerLiteral:
		return node.Val, nil
	case *influxql.NumberLiteral:
		return node.Val, nil
	case *influxql.BooleanLiteral:
		return node.Val, nil
	case *influxql.SetLiteral:
		return node.Vals, nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", node)
	}
}

// ConvertToRPN converts an InfluxQL expression to Reverse Polish Notation (RPN)
func ConvertToRPN(expr influxql.Expr) ([]interface{}, error) {
	if expr == nil {
		return nil, nil
	}
	var output []interface{}

	// Recursive function to process the expression tree
	var processExpr func(e influxql.Expr) error
	processExpr = func(e influxql.Expr) error {
		switch node := e.(type) {
		case *influxql.ParenExpr:
			return processExpr(node.Expr)
		case *influxql.BinaryExpr:
			if IsCompareOp(node.Op) {
				output = append(output, &Condition{Op: node.Op})
			}
			if err := processExpr(node.LHS); err != nil {
				return err
			}
			if err := processExpr(node.RHS); err != nil {
				return err
			}
			if IsLogicalOp(node.Op) {
				output = append(output, node.Op)
			}
			if !IsLogicalOp(node.Op) && !IsCompareOp(node.Op) {
				return ErrUnknownOperator(node.Op.String())
			}
			return nil
		case *influxql.VarRef:
			if len(output) == 0 {
				return ErrEmptyRPN
			}
			condition, ok := output[len(output)-1].(*Condition)
			if !ok {
				return ErrInvalidCondition
			}
			condition.Field = node.Val
			return nil
		case *influxql.StringLiteral, *influxql.NumberLiteral, *influxql.IntegerLiteral, *influxql.BooleanLiteral, *influxql.SetLiteral:
			value, err := getLiteralValue(node)
			if err != nil {
				return err
			}
			if len(output) == 0 {
				return ErrEmptyRPN
			}
			condition, ok := output[len(output)-1].(*Condition)
			if !ok {
				return ErrInvalidCondition
			}
			condition.Value = value
			return nil
		default:
			return fmt.Errorf("unsupported expression type: %T", node)
		}
	}

	if err := processExpr(expr); err != nil {
		return nil, err
	}
	return output, nil
}

var logicalOps = map[influxql.Token]bool{
	influxql.AND: true,
	influxql.OR:  true,
}

var compareOps = map[influxql.Token]bool{
	influxql.EQ:            true,
	influxql.LT:            true,
	influxql.LTE:           true,
	influxql.GT:            true,
	influxql.GTE:           true,
	influxql.NEQ:           true,
	influxql.IN:            true,
	influxql.MATCHPHRASE:   true,
	influxql.UNMATCHPHRASE: true,
	influxql.MATCH:         true,
	influxql.LIKE:          true,
	influxql.IPINRANGE:     true,
}

func IsLogicalOp(op influxql.Token) bool {
	return logicalOps[op]

}

func IsCompareOp(op influxql.Token) bool {
	return compareOps[op]
}
