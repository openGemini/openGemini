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

package meta

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var unsupportedCharsInDBName = `,:;./\`
var unsupportedCharsInMstName = `,;/\`

// ValidName checks to see if the given name can would be valid for DB/RP name
func ValidName(name string) bool {
	return validName(name, unsupportedCharsInDBName)
}

func ValidMeasurementName(name string) bool {
	if name == "." || name == ".." {
		return false
	}
	return validName(name, unsupportedCharsInMstName)
}

func validName(name string, unsupported string) bool {
	for _, r := range name {
		if !unicode.IsPrint(r) {
			return false
		}
	}

	return name != "" && !strings.ContainsAny(name, unsupported)
}

func ValidSecondaryIndex(stmt *influxql.CreateMeasurementStatement) error {
	if err := validIndexLen(stmt.IndexType, stmt.IndexList, stmt.IndexParams); err != nil {
		return err
	}

	if err := validIndexType(stmt); err != nil {
		return err
	}

	if err := validIndexList(stmt.IndexList); err != nil {
		return err
	}
	return nil
}

func validIndexLen(types []string, columns [][]string, params [][]influxql.Expr) error {
	if len(types) != len(columns) {
		return errors.New("the IndexType does not match the number of index fields")
	}

	if len(types) != len(params) {
		return errors.New("the IndexParams don't match the number of index fields")
	}
	return nil
}

// indextype can't be duplicate
// primarykey can't be secondary index
// text index only support tag/string field
// bloomfilter_universal index not support bool field
// minmax index only support float field/int field
func validIndexType(stmt *influxql.CreateMeasurementStatement) error {
	if stmt.EngineType == config.EngineType2String[config.TSSTORE] {
		return nil
	}
	primaryKey := stmt.PrimaryKey
	pkSet := make(map[string]struct{}, len(primaryKey))
	for _, s := range primaryKey {
		pkSet[s] = struct{}{}
	}

	for i, indexTypeName := range stmt.IndexType {

		if indexTypeName == index.TimeClusterIndex {
			continue
		}

		indexList := stmt.IndexList[i]
		indexParams := stmt.IndexParams[i]

		for _, col := range indexList {
			_, inTag := stmt.Tags[col]
			typ, inField := stmt.Fields[col]
			if err := validIndexColumn(inTag, inField, indexTypeName, col, pkSet); err != nil {
				return err
			}
			switch indexTypeName {
			case index.TextIndex:
				if err := validTypeForTextIndex(inField, typ, col); err != nil {
					return err
				}
			case index.BloomFilterUniversalIndex:
				if err := validTypeForBloomFilterUniversalIndex(inField, typ, indexParams, col); err != nil {
					return err
				}
			case index.MinMaxIndex:
				if err := validTypeForMinMaxIndex(inTag, typ, col); err != nil {
					return err
				}
			default:
			}
		}
	}
	return nil
}

func validIndexColumn(inTag bool, inField bool, indexTypeName string, col string, pkSet map[string]struct{}) error {
	if !inTag && !inField {
		return fmt.Errorf("%s index: %s is not a tag or a field", indexTypeName, col)
	}

	if _, exists := pkSet[col]; exists {
		return fmt.Errorf("the primary key column: %s can't be the %s index", col, indexTypeName)
	}
	return nil
}

func validTypeForTextIndex(inField bool, typ int32, col string) error {
	if inField && (typ != influx.Field_Type_String) {
		return fmt.Errorf("text index only support Tag or String field, but the column: %s is %s field", col, influx.FieldTypeName[int(typ)])
	}
	return nil
}

func validTypeForBloomFilterUniversalIndex(inField bool, typ int32, indexParams []influxql.Expr, col string) error {
	if inField && (typ == influx.Field_Type_Boolean) {
		return fmt.Errorf("bloomfilter_universal index does not support Boolean field, but the column: %s is %s field", col, influx.FieldTypeName[int(typ)])
	}

	if len(indexParams) > 1 {
		return errors.New("only expect one parameter for bloomfilter_universal")
	}

	if len(indexParams) == 0 {
		return nil
	}

	if v, ok := indexParams[0].(*influxql.NumberLiteral); !ok || v.Val <= 0 || v.Val >= 1 {
		return errors.New("expect one Float value between (0,1) for bloomfilter_universal")
	}
	return nil
}

func validTypeForMinMaxIndex(inTag bool, typ int32, col string) error {
	if inTag {
		return fmt.Errorf("minmax index only support Integer field or Float field, but the column: %s is Tag", col)
	}
	if typ == influx.Field_Type_String || typ == influx.Field_Type_Boolean {
		return fmt.Errorf("minmax index only support Integer field or Float field, but the column: %s is %s", col, influx.FieldTypeName[int(typ)])
	}
	return nil
}

func validIndexList(indexList [][]string) error {
	for _, index := range indexList {
		seen := make(map[string]struct{})
		for _, item := range index {
			if _, ok := seen[item]; ok {
				return fmt.Errorf("duplicate column of create index found: %s", item)
			}
			seen[item] = struct{}{}
		}
	}
	return nil
}
