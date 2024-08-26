// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type ColumnBuilder struct {
	data  []byte
	log   *Log.Logger
	coder *encoding.CoderContext
}

func NewColumnBuilder() *ColumnBuilder {
	return &ColumnBuilder{coder: encoding.NewCoderContext()}
}

func (b *ColumnBuilder) encIntegerColumn(col *record.ColVal) error {
	b.data = append(b.data, encoding.BlockInteger)
	var err error
	b.data, err = encoding.EncodeIntegerBlock(col.Val, b.data, b.coder)
	if err != nil {
		b.log.Error("encode integer value fail", zap.Error(err))
		return err
	}

	return nil
}

func (b *ColumnBuilder) encFloatColumn(col *record.ColVal) error {
	b.data = append(b.data, encoding.BlockFloat64)
	var err error
	b.data, err = encoding.EncodeFloatBlock(col.Val, b.data, b.coder)
	if err != nil {
		b.log.Error("encode float value fail", zap.Error(err))
		return err
	}

	return nil
}

func (b *ColumnBuilder) encStringColumn(col *record.ColVal) error {
	b.data = append(b.data, encoding.BlockString)
	var err error
	b.data, err = encoding.EncodeStringBlock(col.Val, col.Offset, b.data, b.coder)
	if err != nil {
		b.log.Error("encode string value fail", zap.Error(err))
		return err
	}

	return nil
}

func (b *ColumnBuilder) encTagColumn(col *record.ColVal) error {
	b.data = append(b.data, encoding.BlockTag)
	var err error
	b.data, err = encoding.EncodeStringBlock(col.Val, col.Offset, b.data, b.coder)
	if err != nil {
		b.log.Error("encode string value fail", zap.Error(err))
		return err
	}

	return nil
}

func (b *ColumnBuilder) encBooleanColumn(col *record.ColVal) error {
	b.data = append(b.data, encoding.BlockBoolean)
	var err error

	b.data, err = encoding.EncodeBooleanBlock(col.Val, b.data, b.coder)
	if err != nil {
		b.log.Error("encode boolean value fail", zap.Error(err))
		return err
	}

	return nil
}

func (b *ColumnBuilder) EncodeColumn(ref *record.Field, col *record.ColVal) ([]byte, error) {
	var err error

	switch ref.Type {
	case influx.Field_Type_Int:
		err = b.encIntegerColumn(col)
	case influx.Field_Type_Float:
		err = b.encFloatColumn(col)
	case influx.Field_Type_String:
		err = b.encStringColumn(col)
	case influx.Field_Type_Boolean:
		err = b.encBooleanColumn(col)
	case influx.Field_Type_Tag:
		err = b.encTagColumn(col)
	default:
		return nil, fmt.Errorf("invalid column type %v", ref.Type)
	}

	if err != nil {
		return nil, err
	}

	return b.data, nil
}

func (b *ColumnBuilder) set(dst []byte) {
	if b == nil {
		return
	}
	b.data = dst
}
