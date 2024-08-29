// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package colstore

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func appendIntegerColumn(encData []byte, col *record.ColVal, coder *encoding.CoderContext) error {
	col.Init()
	if len(encData) != 0 {
		values, err := encoding.DecodeIntegerBlock(encData, &col.Val, coder)
		if err != nil {
			return err
		}
		col.Len += len(values)
	}

	return nil
}

func appendFloatColumn(encData []byte, col *record.ColVal, coder *encoding.CoderContext) error {
	col.Init()
	if len(encData) != 0 {
		values, err := encoding.DecodeFloatBlock(encData, &col.Val, coder)
		if err != nil {
			return err
		}
		col.Len += len(values)
	}

	return nil
}

func appendBooleanColumn(encData []byte, col *record.ColVal, coder *encoding.CoderContext) error {
	col.Init()
	if len(encData) != 0 {
		values, err := encoding.DecodeBooleanBlock(encData, &col.Val, coder)
		if err != nil {
			return err
		}
		col.Len += len(values)
	}

	return nil
}

func appendStringColumn(encData []byte, col *record.ColVal, coder *encoding.CoderContext) error {
	col.Init()
	if len(encData) != 0 {
		values, _, err := encoding.DecodeStringBlock(encData, &col.Val, &col.Offset, coder)
		if err != nil {
			return err
		}
		col.Len += len(values)
	}

	return nil
}

var decFuncs = make(map[int]func(encData []byte, col *record.ColVal, coder *encoding.CoderContext) error, 4)

func InitDecFunctions() {
	decFuncs[influx.Field_Type_Int] = appendIntegerColumn
	decFuncs[influx.Field_Type_Float] = appendFloatColumn
	decFuncs[influx.Field_Type_Boolean] = appendBooleanColumn
	decFuncs[influx.Field_Type_String] = appendStringColumn
	decFuncs[influx.Field_Type_Tag] = appendStringColumn
}

func appendColumnData(dataType int, encData []byte, col *record.ColVal, coder *encoding.CoderContext) error {
	decFun, ok := decFuncs[dataType]
	if !ok {
		panic(fmt.Sprintf("invalid column type %v", dataType))
	}

	return decFun(encData, col, coder)
}

func decodeColumnData(ref *record.Field, data []byte, col *record.ColVal, coder *encoding.CoderContext) error {
	pos := 0
	dataType := int(data[0])
	pos += 1
	if dataType != ref.Type {
		return fmt.Errorf("type(%v) in table not eq select type(%v)", dataType, ref.Type)
	}

	return appendColumnData(dataType, data[pos:], col, coder)
}
