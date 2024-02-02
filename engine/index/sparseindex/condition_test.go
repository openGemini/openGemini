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

package sparseindex_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestKeyConditionImpl_CheckInRange(t *testing.T) {
	pkRec := buildPKRecord()
	pkSchema := pkRec.Schema
	conStr := "UserID='U1' and URL='W3'"
	keyCondition, err := sparseindex.NewKeyCondition(nil, MustParseExpr(conStr), pkSchema)
	if err != nil {
		t.Fatal(err)
	}
	cols := make([]*sparseindex.ColumnRef, 2)
	for i := 0; i < 2; i++ {
		cols[i] = sparseindex.NewColumnRef(pkRec.Schema[i].Name, pkRec.Schema[i].Type, pkRec.Column(i))
	}
	rgs := []*sparseindex.Range{
		sparseindex.NewRange(sparseindex.NewFieldRef(cols, 0, 0), sparseindex.NewFieldRef(cols, 0, 0), true, true),
		sparseindex.NewRange(sparseindex.NewFieldRef(cols, 1, 0), sparseindex.NewFieldRef(cols, 1, 4), true, true),
	}
	dataTypes := []int{4, 4}
	var rpns, rpn1, rpn2 []*sparseindex.RPNElement
	rpns = append(rpns, keyCondition.GetRPN()...)
	rpn1 = append(rpn1, rpns[:len(rpns)-1]...)
	keyCondition.SetRPN(rpn1)
	_, err = keyCondition.CheckInRange(rgs, dataTypes)
	assert.Equal(t, errno.Equal(err, errno.ErrInvalidStackInCondition), true)

	rpn2 = append(rpn2, rpns[2:]...)
	keyCondition.SetRPN(rpn2)
	_, err = keyCondition.CheckInRange(rgs, dataTypes)
	assert.Equal(t, errno.Equal(err, errno.ErrRPNIsNullForAnd), true)
}

func TestKeyConditionImpl_AlwaysInRange(t *testing.T) {
	pkRec := buildPKRecord()
	pkSchema := pkRec.Schema
	conStr := "UserID='U1' and URL='W3'"
	keyCondition, err := sparseindex.NewKeyCondition(nil, MustParseExpr(conStr), pkSchema)
	if err != nil {
		t.Fatal(err)
	}
	conStr1 := "UserID='U1' or URL='W3'"
	keyCondition1, err := sparseindex.NewKeyCondition(nil, MustParseExpr(conStr1), pkSchema)
	if err != nil {
		t.Fatal(err)
	}

	var rpns, rpn1, rpn2 []*sparseindex.RPNElement
	rpns = append(rpns, keyCondition.GetRPN()...)
	rpn1 = append(rpn1, rpns[:len(rpns)-1]...)
	rpn2 = append(rpn2, rpns[2:]...)

	keyCondition.SetRPN(rpns)
	_, err = keyCondition.AlwaysInRange()
	assert.Equal(t, err, nil)

	keyCondition.SetRPN(rpn1)
	_, err = keyCondition.AlwaysInRange()
	assert.Equal(t, errno.Equal(err, errno.ErrInvalidStackInCondition), true)

	keyCondition.SetRPN(rpn2)
	_, err = keyCondition.AlwaysInRange()
	assert.Equal(t, errno.Equal(err, errno.ErrRPNIsNullForAnd), true)

	keyCondition1.SetRPN(keyCondition1.GetRPN())
	_, err = keyCondition1.AlwaysInRange()
	assert.Equal(t, err, nil)

	keyCondition1.SetRPN(keyCondition1.GetRPN()[2:])
	_, err = keyCondition1.AlwaysInRange()
	assert.Equal(t, errno.Equal(err, errno.ErrRPNIsNullForOR), true)

	keyCondition1.SetRPN([]*sparseindex.RPNElement{sparseindex.NewRPNElement(rpn.UNKNOWN)})
	ok, _ := keyCondition1.AlwaysInRange()
	assert.Equal(t, ok, true)

	keyCondition1.SetRPN([]*sparseindex.RPNElement{sparseindex.NewRPNElement(10)})
	_, err = keyCondition1.AlwaysInRange()
	assert.Equal(t, err, nil)
}

type MockSKBaseReader struct {
	err error
}

func (m *MockSKBaseReader) IsExist(_ int64, _ *rpn.SKRPNElement) (bool, error) {
	return true, m.err
}

func TestSKCondition(t *testing.T) {
	skRec := buildPKRecordAllFinal()
	skSchema := skRec.Schema

	conStr := "stringKey"
	expr := MustParseExpr(conStr)
	rpnExpr := rpn.ConvertToRPNExpr(expr)
	_, err := sparseindex.NewSKCondition(rpnExpr, skSchema)
	assert.True(t, errno.Equal(err, errno.ErrRPNElemNum))

	expr = &influxql.BinaryExpr{
		Op: influxql.ADD,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "intKey", Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: 2},
		}}
	rpnExpr = rpn.ConvertToRPNExpr(expr)
	_, err = sparseindex.NewSKCondition(rpnExpr, skSchema)
	assert.True(t, errno.Equal(err, errno.ErrRPNOp))

	rpnExpr = &rpn.RPNExpr{Val: []interface{}{&influxql.BinaryExpr{}}}
	_, err = sparseindex.NewSKCondition(rpnExpr, skSchema)
	assert.True(t, errno.Equal(err, errno.ErrRPNExpr))

	conStr = "stringKey='W1'"
	expr = MustParseExpr(conStr)
	rpnExpr = rpn.ConvertToRPNExpr(expr)
	skCondition, _ := sparseindex.NewSKCondition(rpnExpr, skSchema)
	_, err = skCondition.IsExist(0, &MockSKBaseReader{err: errno.NewError(errno.ErrInvalidStackInCondition)})
	assert.True(t, errno.Equal(err, errno.ErrInvalidStackInCondition))

	expr = &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "intKey", Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: 2},
		}}
	rpnExpr = rpn.ConvertToRPNExpr(expr)
	skCondition, _ = sparseindex.NewSKCondition(rpnExpr, skSchema)
	_, err = skCondition.IsExist(0, &MockSKBaseReader{})
	assert.True(t, errno.Equal(err, errno.ErrRPNIsNullForAnd))

	expr = &influxql.BinaryExpr{
		Op: influxql.OR,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "intKey", Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: 2},
		}}
	rpnExpr = rpn.ConvertToRPNExpr(expr)
	skCondition, _ = sparseindex.NewSKCondition(rpnExpr, skSchema)
	_, err = skCondition.IsExist(0, &MockSKBaseReader{})
	assert.True(t, errno.Equal(err, errno.ErrRPNIsNullForOR))

	skCondition, _ = sparseindex.NewSKCondition(&rpn.RPNExpr{}, skSchema)
	_, err = skCondition.IsExist(0, &MockSKBaseReader{})
	assert.True(t, errno.Equal(err, errno.ErrInvalidStackInCondition))

	conStr = "stringKey='W1' or boolKey=true and intKey=1 and floatKey=1.0 and UserID='U1'"
	expr = MustParseExpr(conStr)
	rpnExpr = rpn.ConvertToRPNExpr(expr)
	skCondition, _ = sparseindex.NewSKCondition(rpnExpr, skSchema)
	ok, _ := skCondition.IsExist(0, &MockSKBaseReader{})
	assert.True(t, ok)

	conStr = "__log___='W1'"
	fieldMap["__log___"] = influxql.String
	expr = MustParseExpr(conStr)
	rpnExpr = rpn.ConvertToRPNExpr(expr)
	skCondition, _ = sparseindex.NewSKCondition(rpnExpr, skSchema)
	ok, _ = skCondition.IsExist(0, &MockSKBaseReader{})
	assert.True(t, ok)

	conStr = "__log___=1"
	fieldMap["__log___"] = influxql.Integer
	expr = MustParseExpr(conStr)
	rpnExpr = rpn.ConvertToRPNExpr(expr)
	_, err = sparseindex.NewSKCondition(rpnExpr, skSchema)
	assert.True(t, errno.Equal(err, errno.ErrValueTypeFullTextIndex))
}
