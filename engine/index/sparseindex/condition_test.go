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
	"github.com/stretchr/testify/assert"
)

func TestCheckInRange(t *testing.T) {
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
	var rpn, rpn1, rpn2 []*sparseindex.RPNElement
	rpn = append(rpn, keyCondition.GetRPN()...)
	rpn1 = append(rpn1, rpn[:len(rpn)-1]...)
	keyCondition.SetRPN(rpn1)
	_, err = keyCondition.CheckInRange(rgs, dataTypes)
	assert.Equal(t, errno.Equal(err, errno.ErrInvalidStackInCondition), true)

	rpn2 = append(rpn2, rpn[2:]...)
	keyCondition.SetRPN(rpn2)
	_, err = keyCondition.CheckInRange(rgs, dataTypes)
	assert.Equal(t, errno.Equal(err, errno.ErrRPNIsNullForAnd), true)
}
