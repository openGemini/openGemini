// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

var codecTpl = `
func (o *{{.StructName}}) Marshal(buf []byte) ([]byte, error) {
	var err error

	{{- range .Encode }}
	{{- if eq .Typ "object"}}

	func() {
		{{- if eq .Ref "true"}}
		if o.{{.FieldName}} == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		{{- end}}
		buf = codec.AppendUint32(buf, uint32(o.{{.FieldName}}.Size()))
		buf, err = o.{{.FieldName}}.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}
	{{- else if eq .Typ "slice_object"}}

	buf = codec.AppendUint32(buf, uint32(len(o.{{.FieldName}})))
	for _, item := range o.{{.FieldName}} {
		{{- if eq .Ref "true"}}
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		{{- end}}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}
	{{- else if eq .Typ "map_string_object"}}

	buf = codec.AppendUint32(buf, uint32(len(o.{{.FieldName}})))
	for k, item := range o.{{.FieldName}} {
		buf = codec.AppendString(buf, k)
		{{- if eq .Ref "true"}}
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		{{- end}}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}
	{{- else}}
	buf = codec.{{.FuncName}}(buf, o.{{.FieldName}})
	{{- end}}
	{{- end}}

	return buf, err
}

func (o *{{.StructName}}) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)

	{{- range .Decode }}
	{{- if eq .Typ "object"}}

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.{{.FieldName}} = {{.NewSubStruct}}{}
		err = o.{{.FieldName}}.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}
	{{- else if eq .Typ "slice_object"}}

	{{.FieldName}}Len := int(dec.Uint32())
    if {{.FieldName}}Len > 0 {
		o.{{.FieldName}} = make([]{{.SubStruct}}, {{.FieldName}}Len)
		for i := 0; i < {{.FieldName}}Len; i++ {
			subBuf := dec.BytesNoCopy()
			if len(subBuf) == 0 {
				continue
			}

			o.{{.FieldName}}[i] = {{.NewSubStruct}}{}
			if err := o.{{.FieldName}}[i].Unmarshal(subBuf); err != nil {
				return err
			}
		}
	}
	{{- else if eq .Typ "map_string_object"}}

	{{.FieldName}}Len := int(dec.Uint32())
	o.{{.FieldName}} = make(map[string]{{.SubStruct}}, {{.FieldName}}Len)
	for i := 0; i < {{.FieldName}}Len; i++ {
		k := dec.String()
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			{{- if eq .Ref "true"}}
			o.{{.FieldName}}[k] = nil
			{{- else}}
			o.{{.FieldName}}[k] = {{.NewSubStruct}}{}
			{{- end}}
			continue
		}

		item := {{.NewSubStruct}}{}
		if err := item.Unmarshal(subBuf); err != nil {
			return err
		}
		o.{{.FieldName}}[k] = item
	}
	{{- else}}
	o.{{.FieldName}} = dec.{{.FuncName}}()
	{{- end}}
	{{- end}}

	return err
}

func (o *{{.StructName}}) Size() int {
	size := 0

	{{- range .Size }}
	{{- if eq .Typ "normal"}}
	size += codec.{{.FuncName}}()
	{{- else if eq .Typ "object"}}

	size += codec.SizeOfUint32()
	{{- if eq .Ref "true"}}
	if o.{{.FieldName}} != nil {
		size += o.{{.FieldName}}.Size()
	}
	{{- else}}
	size += o.{{.FieldName}}.Size()
	{{- end}}
	{{- else if eq .Typ "slice_object"}}

	size += codec.MaxSliceSize
	for _, item := range o.{{.FieldName}} {
		size += codec.SizeOfUint32()
		{{- if eq .Ref "true"}}
		if item == nil {
			continue
		}
		{{- end}}
		size += item.Size()
	}
	{{- else if eq .Typ "map_string_object"}}

	size += codec.MaxSliceSize
	for k, item := range o.{{.FieldName}} {
		size += codec.SizeOfString(k)
		size += codec.SizeOfUint32()
		{{- if eq .Ref "true"}}
		if item == nil {
			continue
		}
		{{- end}}
		size += item.Size()
	}
	{{- else}}
	size += codec.{{.FuncName}}(o.{{.FieldName}})
	{{- end}}
	{{- end}}

	return size
}

func (o *{{.StructName}}) Instance() transport.Codec {
	return &{{.StructName}}{}
}
`
