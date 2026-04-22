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

package influxql

import (
	"fmt"
	"sort"
	"strings"
)

// GetJoinKeyByCondition used to extract the keys of the left and right expr of the join from the join condition.
func GetJoinKeyByCondition(joinCondition Expr, joinKeyMap map[string]string) error {
	if joinCondition == nil {
		return fmt.Errorf("join condition is null")
	}
	switch expr := joinCondition.(type) {
	case *ParenExpr:
		return GetJoinKeyByCondition(expr.Expr, joinKeyMap)
	case *BinaryExpr:
		if expr.Op != AND && expr.Op != EQ {
			return fmt.Errorf("only support the equal join")
		}
		lVal, ok1 := expr.LHS.(*VarRef)
		rVal, ok2 := expr.RHS.(*VarRef)
		if expr.Op == EQ && ok1 && ok2 {
			joinKeyMap[lVal.Val] = rVal.Val
		}
		if err := GetJoinKeyByCondition(expr.LHS, joinKeyMap); err != nil {
			return err
		}
		if err := GetJoinKeyByCondition(expr.RHS, joinKeyMap); err != nil {
			return err
		}
		return nil
	default:
		return nil
	}
}

// getJoinKeyByName used to extract the relevant join key according to the join table name or alias
func getJoinKeyByName(joinKeyMap map[string]string, mst string) []string {
	prefix := mst + "."
	joinKeys := make([]string, 0, len(joinKeyMap))
	for leftKey, rightKey := range joinKeyMap {
		if strings.HasPrefix(leftKey, prefix) {
			joinKeys = append(joinKeys, leftKey)
		} else if strings.HasPrefix(rightKey, prefix) {
			joinKeys = append(joinKeys, rightKey)
		}
	}
	sort.Strings(joinKeys)
	return joinKeys
}

// getAuxiliaryField used to get the auxiliary fields
func getAuxiliaryField(fields []*Field) []*Field {
	auxFields := make([]*Field, 0)
	for _, field := range fields {
		if field.Auxiliary {
			auxFields = append(auxFields, field)
		}
	}
	return auxFields
}

// rewriteAuxiliaryField used to rewrite the auxiliary fields to VarRef.
func rewriteAuxiliaryField(fields []*Field) []*Field {
	auxFields := make([]*Field, len(fields))
	for i := range fields {
		auxFields[i] = &Field{
			Expr:      &VarRef{Val: fields[i].Alias, Type: 0},
			Auxiliary: true,
			depth:     fields[i].depth,
		}
	}
	return auxFields
}

// rewriteAuxiliaryStmt used to rewrite the join query stmt with aux fields into a subquery,
// so that achieve the mapping of join fields that are not in select.
func rewriteAuxiliaryStmt(other *SelectStatement, sources []Source, auxFields []*Field,
	m FieldMapper, batchEn bool, hasJoin bool) (*SelectStatement, error) {
	oriFields := make([]*Field, 0, len(other.Fields))
	oriFields = append(oriFields, other.Fields...)
	other.Fields = append(other.Fields, rewriteAuxiliaryField(auxFields)...)
	var err error
	other, err = other.RewriteFields(m, batchEn, hasJoin)
	if err != nil {
		return nil, err
	}
	other.Sources = sources
	newQuery := other.Clone()
	other = &SelectStatement{
		Fields:     oriFields,
		Sources:    []Source{&SubQuery{Statement: newQuery, depth: 1 + newQuery.depth}},
		Dimensions: newQuery.Dimensions,
		StmtId:     newQuery.StmtId,
		OmitTime:   newQuery.OmitTime,
		TimeAlias:  newQuery.TimeAlias,
		depth:      2 + newQuery.depth,
	}
	return other, nil
}
