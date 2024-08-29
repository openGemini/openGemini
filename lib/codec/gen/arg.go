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

package gen

import (
	"fmt"
	"strings"
)

type BaseArguments struct {
	FieldName string
	FuncName  string
	Typ       string
	Ref       string
}

type SizeArguments struct {
	BaseArguments
}

func NewSizeArguments(fn string, f *Field) SizeArguments {
	a := SizeArguments{}
	a.FieldName = f.name
	a.FuncName = fn
	a.Typ = strings.Join(f.typList, typeSep)
	a.Ref = fmt.Sprintf("%t", f.ref)
	return a
}

type EncodeArguments struct {
	BaseArguments
}

func NewEncodeArguments(fn []string, f *Field) EncodeArguments {
	a := EncodeArguments{}
	a.FieldName = f.name
	if len(fn) > 0 {
		a.FuncName = fn[0]
	}
	a.Typ = strings.Join(f.typList, typeSep)
	a.Ref = fmt.Sprintf("%t", f.ref)
	return a
}

type DecodeArguments struct {
	BaseArguments
	SubStruct    string
	NewSubStruct string
}

func NewDecodeArguments(fn []string, f *Field) DecodeArguments {
	a := DecodeArguments{}
	a.FieldName = f.name
	if len(fn) > 1 {
		a.FuncName = fn[1]
	}
	a.Typ = strings.Join(f.typList, "_")
	a.Ref = fmt.Sprintf("%t", f.ref)

	a.NewSubStruct = f.subObjectName
	a.SubStruct = f.subObjectName

	if f.ref {
		a.NewSubStruct = "&" + f.subObjectName
		a.SubStruct = "*" + f.subObjectName
	}
	if f.interfaceName != "" {
		a.SubStruct = f.interfaceName
	}

	return a
}

type Arguments struct {
	StructName string
	Size       []SizeArguments
	Encode     []EncodeArguments
	Decode     []DecodeArguments
}

func NewArguments() *Arguments {
	return &Arguments{}
}

func (a *Arguments) SetStructName(name string) {
	a.StructName = name
}

func (a *Arguments) Add(field *Field) {
	if len(field.typList) == 0 {
		return
	}

	typ := strings.Join(field.typList, "_")
	if _, ok := allowTypes[typ]; !ok {
		panic(fmt.Sprintf("unsupported type: %s", typ))
	}

	switch field.typList[0] {
	case typeString, typeNormal, typeObject:
		a.AddNormal(field)
	case typeSlice:
		a.AddSlice(field)
	case typeMap:
		a.AddMap(field)
	}
}

func (a *Arguments) AddNormal(field *Field) {
	a.Size = append(a.Size, NewSizeArguments(sizeFuncMap[field.kind], field))
	a.Encode = append(a.Encode, NewEncodeArguments(encodeFuncMap[field.kind], field))
	a.Decode = append(a.Decode, NewDecodeArguments(encodeFuncMap[field.kind], field))
}

func (a *Arguments) AddSlice(field *Field) {
	a.Size = append(a.Size, NewSizeArguments(sliceSizeFuncMap[field.kind], field))
	a.Encode = append(a.Encode, NewEncodeArguments(encodeSliceFuncMap[field.kind], field))
	a.Decode = append(a.Decode, NewDecodeArguments(encodeSliceFuncMap[field.kind], field))
}

func (a *Arguments) AddMap(field *Field) {
	a.Size = append(a.Size, NewSizeArguments(mapSizeFuncMap[field.mapKeyKind][field.kind], field))
	a.Encode = append(a.Encode, NewEncodeArguments(encodeMapFuncMap[field.mapKeyKind][field.kind], field))
	a.Decode = append(a.Decode, NewDecodeArguments(encodeMapFuncMap[field.mapKeyKind][field.kind], field))
}
