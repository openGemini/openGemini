/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package gen

import "reflect"

var encodeFuncMap = map[reflect.Kind][]string{
	reflect.Uint8:  {"AppendUint8", "Uint8"},
	reflect.Uint16: {"AppendUint16", "Uint16"},
	reflect.Uint32: {"AppendUint32", "Uint32"},
	reflect.Uint64: {"AppendUint64", "Uint64"},

	reflect.Int:   {"AppendInt", "Int"},
	reflect.Int16: {"AppendInt16", "Int16"},
	reflect.Int32: {"AppendInt32", "Int32"},
	reflect.Int64: {"AppendInt64", "Int64"},

	reflect.Float32: {"AppendFloat32", "Float32"},
	reflect.Float64: {"AppendFloat64", "Float64"},
	reflect.String:  {"AppendString", "String"},
	reflect.Bool:    {"AppendBool", "Bool"},
}

var encodeSliceFuncMap = map[reflect.Kind][]string{
	reflect.Int:   {"AppendIntSlice", "IntSlice"},
	reflect.Int16: {"AppendInt16Slice", "Int16Slice"},
	reflect.Int32: {"AppendInt32Slice", "Int32Slice"},
	reflect.Int64: {"AppendInt64Slice", "Int64Slice"},

	reflect.Uint8:  {"AppendBytes", "Bytes"},
	reflect.Uint16: {"AppendUint16Slice", "Uint16Slice"},
	reflect.Uint32: {"AppendUint32Slice", "Uint32Slice"},
	reflect.Uint64: {"AppendUint64Slice", "Uint64Slice"},

	reflect.Float32: {"AppendFloat32Slice", "Float32Slice"},
	reflect.Float64: {"AppendFloat64Slice", "Float64Slice"},
	reflect.Bool:    {"AppendBoolSlice", "BoolSlice"},
	reflect.String:  {"AppendStringSlice", "StringSlice"},
}

var encodeMapFuncMap = map[reflect.Kind]map[reflect.Kind][]string{
	reflect.String: {
		reflect.String: {"AppendMapStringString", "MapStringString"},
	},
}

var sizeFuncMap = map[reflect.Kind]string{
	reflect.Int:     "SizeOfInt",
	reflect.Int8:    "SizeOfInt8",
	reflect.Int16:   "SizeOfInt16",
	reflect.Int32:   "SizeOfInt32",
	reflect.Int64:   "SizeOfInt64",
	reflect.Uint8:   "SizeOfUint8",
	reflect.Uint16:  "SizeOfUint16",
	reflect.Uint32:  "SizeOfUint32",
	reflect.Uint64:  "SizeOfUint64",
	reflect.Float32: "SizeOfFloat32",
	reflect.Float64: "SizeOfFloat64",
	reflect.Bool:    "SizeOfBool",
	reflect.String:  "SizeOfString",
}

var sliceSizeFuncMap = map[reflect.Kind]string{
	reflect.Int:     "SizeOfIntSlice",
	reflect.Int16:   "SizeOfInt16Slice",
	reflect.Int32:   "SizeOfInt32Slice",
	reflect.Int64:   "SizeOfInt64Slice",
	reflect.Uint16:  "SizeOfUint16Slice",
	reflect.Uint32:  "SizeOfUint32Slice",
	reflect.Uint64:  "SizeOfUint64Slice",
	reflect.Float32: "SizeOfFloat32Slice",
	reflect.Float64: "SizeOfFloat64Slice",
	reflect.Bool:    "SizeOfBoolSlice",
	reflect.String:  "SizeOfStringSlice",
	reflect.Uint8:   "SizeOfByteSlice",
}

var mapSizeFuncMap = map[reflect.Kind]map[reflect.Kind]string{
	reflect.String: {
		reflect.String: "SizeOfMapStringString",
	},
}
