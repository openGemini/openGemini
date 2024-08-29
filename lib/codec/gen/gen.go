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
	"log"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"text/template"

	"github.com/openGemini/openGemini/lib/util"
)

type FieldType string

const (
	typeString = "string"
	typeObject = "object"
	typeSlice  = "slice"
	typeMap    = "map"
	typeNormal = "normal"
	typeSep    = "_"
)

var allowTypes = map[string]struct{}{
	typeString:          struct{}{},
	"slice_string":      struct{}{},
	"map_string_string": struct{}{},

	typeNormal:          struct{}{},
	"slice_normal":      struct{}{},
	"map_string_normal": struct{}{},

	typeObject:          struct{}{},
	"slice_object":      struct{}{},
	"map_string_object": struct{}{},
}

func (ft FieldType) String() string {
	return string(ft)
}

type SubStruct struct {
	typ reflect.Type
	val *reflect.Value
}

type CodecGen struct {
	pkg     string
	imports []string

	subStructs map[string]SubStruct

	a  *Arguments
	am map[string]*Arguments

	excludes []string
}

type Field struct {
	// indicates whether the type is Reference
	ref bool

	// structure field name
	name string

	// structure field interface name
	interfaceName string
	subObjectName string

	kind       reflect.Kind
	mapKeyKind reflect.Kind

	typList []string
}

func (f *Field) SetType(typ FieldType) {

}

func (f *Field) addTyp(typ string) {
	f.typList = append(f.typList, typ)
}

func NewCodecGen(pkg string) *CodecGen {
	return &CodecGen{
		pkg: pkg,
		imports: []string{
			"github.com/openGemini/openGemini/engine/executor/spdy/transport",
			"github.com/openGemini/openGemini/lib/codec",
		},
		am:         make(map[string]*Arguments),
		subStructs: make(map[string]SubStruct),
		excludes:   []string{"codec.EmptyCodec"},
	}
}

func (g *CodecGen) Gen(obj interface{}) {
	typ := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	g.gen(typ, &v)
}

func (g *CodecGen) gen(typ reflect.Type, val *reflect.Value) {
	switch typ.Kind() {
	case reflect.Ptr:
		v := (*val).Elem()
		g.gen(typ.Elem(), &v)
		return
	case reflect.Struct:
	default:
		panic(fmt.Sprintf("unsupported type: %s", typ.Kind()))
	}

	n := typ.NumField()
	struckName := typ.Name()
	g.a = NewArguments()
	g.a.SetStructName(struckName)

	for i := 0; i < n; i++ {
		f := typ.Field(i)
		v := getFiled(val, i)
		field := &Field{
			ref:           false,
			name:          f.Name,
			interfaceName: "",
		}

		g.genItem(f.Type, v, field)
		g.a.Add(field)
	}

	g.am[struckName] = g.a
	g.a = nil

	subs := g.subStructs
	g.subStructs = make(map[string]SubStruct)
	for _, sub := range subs {
		g.gen(sub.typ, sub.val)
	}
}

func (g *CodecGen) genItem(t reflect.Type, v *reflect.Value, field *Field) bool {
	ts := t.String()
	for _, e := range g.excludes {
		if strings.HasSuffix(ts, e) {
			return true
		}
	}

	var addSub = func(t reflect.Type, v *reflect.Value) {
		field.subObjectName = t.Name()
		g.subStructs[t.Name()] = SubStruct{
			typ: t,
			val: v,
		}
	}

	field.kind = t.Kind()
	switch t.Kind() {
	case reflect.Slice:
		field.addTyp(typeSlice)
		if t.Elem().Kind() == reflect.Interface {
			v0 := (*v).Index(0)
			return g.genItem(v.Type().Elem(), &v0, field)
		}
		return g.genItem(t.Elem(), nil, field)
	case reflect.Struct:
		field.addTyp(typeObject)
		field.ref = false
		addSub(t, v)
	case reflect.Ptr:
		field.addTyp(typeObject)
		field.ref = true
		addSub(t.Elem(), getElem(v))
	case reflect.Interface:
		field.interfaceName = t.Name()
		return g.genItem(v.Elem().Type(), getElem(v), field)
	case reflect.Map:
		field.addTyp(typeMap)
		field.addTyp(t.Key().Kind().String())
		field.mapKeyKind = t.Key().Kind()

		if t.Elem().Kind() == reflect.Interface {
			m0 := v.MapIndex(v.MapKeys()[0])
			return g.genItem(v.Type().Elem(), &m0, field)
		}
		return g.genItem(t.Elem(), nil, field)
	case reflect.String:
		field.addTyp(typeString)
	default:
		field.addTyp(typeNormal)
	}
	return true
}

func (g *CodecGen) Exclude(s ...string) {
	g.excludes = append(g.excludes, s...)
}

func (g *CodecGen) Imports(s ...string) {
	g.imports = append(g.imports, s...)
}

func (g *CodecGen) SaveTo(filename string) {
	f, err := os.OpenFile(path.Clean(filename), os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Println(err)
		return
	}
	defer util.MustClose(f)

	header := append([]string{}, "package "+g.pkg+"\n", "import (")
	for _, s := range g.imports {
		header = append(header, fmt.Sprintf(`%s"%s"`, "\t", s))
	}
	header = append(header, ")\n")
	_, err = f.WriteString(strings.Join(header, "\n"))
	if err != nil {
		panic(err)
	}

	tpl, err := template.New("codec").Parse(codecTpl)
	if err != nil {
		panic(err)
	}

	var args []*Arguments
	for _, a := range g.am {
		args = append(args, a)
	}
	sort.Slice(args, func(i, j int) bool {
		return args[i].StructName < args[j].StructName
	})
	for _, a := range args {
		if err := tpl.Execute(f, a); err != nil {
			panic(err)
		}
	}
}

func getFiled(v *reflect.Value, i int) *reflect.Value {
	if v == nil {
		return nil
	}

	if v.Kind() == reflect.Invalid || (*v).IsZero() {
		return nil
	}

	vf := (*v).Field(i)
	return &vf
}

func getElem(v *reflect.Value) *reflect.Value {
	if v == nil {
		return nil
	}
	switch v.Type().Kind() {
	case reflect.Struct:
		return nil
	}

	ve := (*v).Elem()
	return &ve
}
