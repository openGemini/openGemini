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

package gen_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/codec/gen"
	"github.com/stretchr/testify/assert"
)

func TestGen(t *testing.T) {
	tmpDir := t.TempDir()
	obj := &CodecObject{
		mo: &MapObject{
			mi: map[string]IObject{"obj": &SubString{s: "tttt"}},
		},
		subInterface:      &SubString{s: "tttt"},
		subInterfaceSlice: []IObject{&SubString{}},
	}
	g := gen.NewCodecGen("gen_test")
	g.Gen(obj)

	path := filepath.Join(tmpDir, "codec.gen_test.go")
	g.SaveTo(path)

	_, err := os.Stat(path)
	assert.NoError(t, err)
}

type IObject interface {
	transport.Codec
	String() string
}

type SubObject struct {
	codec.EmptyCodec
	a int64
	b string
	c float64
}

type SubString struct {
	codec.EmptyCodec
	s string
}

func (s *SubString) String() string {
	return s.s
}

type UintObject struct {
	ui8  uint8
	ui16 uint16
	ui32 uint32
	ui64 uint64

	sui16 []uint16
	sui32 []uint32
	sui64 []uint64
}

type IntObject struct {
	i16 int16
	i32 int32
	i64 int64

	si16 []int16
	si32 []int32
	si64 []int64
}

type FloatObject struct {
	f32 float32
	f64 float64

	sf32 []float32
	sf64 []float64
}

type MapObject struct {
	ss map[string]string
	mo map[string]IntObject
	mr map[string]*IntObject
	mi map[string]IObject
}

type CodecObject struct {
	codec.EmptyCodec
	a string
	b []int
	c []byte
	d []bool
	e []string
	f bool
	g int

	mo  *MapObject
	io  IntObject
	uio *UintObject
	fo  *FloatObject

	sub      *SubObject
	subSlice []*SubObject

	subInterface      IObject
	subInterfaceSlice []IObject
}
