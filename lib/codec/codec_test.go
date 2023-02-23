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

package codec_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/codec/gen"
	"github.com/stretchr/testify/assert"
)

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

func TestGen(t *testing.T) {
	tmpDir := t.TempDir()
	obj := &CodecObject{
		mo: &MapObject{
			mi: map[string]IObject{"obj": &SubString{s: "tttt"}},
		},
		subInterface:      &SubString{s: "tttt"},
		subInterfaceSlice: []IObject{&SubString{}},
	}
	g := gen.NewCodecGen("codec_test")
	g.Gen(obj)

	g.SaveTo(filepath.Join(tmpDir, "codec.gen_test.go"))
}

func NewCodecObject() *CodecObject {
	io := IntObject{
		i16:  1<<15 - 1,
		i32:  0,
		i64:  -1 << 61,
		si16: []int16{1, 2, 1<<15 - 1, 1 << 15 * -1, -1, 0},
		si32: []int32{1, 2, 1<<31 - 1, -1 << 31, -1, 0},
		si64: []int64{1, 2, 1<<61 - 1, -1 << 61, -1, 0},
	}

	return &CodecObject{
		a: "sandy",
		b: []int{-1 * (2 << 31), -1, 0, 1, 2, 3, 4, 6},
		c: []byte{1, 2, 3, 4, 6, 0},
		d: []bool{true, true, false, false, true, false, false, false, false, false, true, true},
		e: []string{"aaa", "bbb", "2222", "tttt"},
		f: true,
		g: 123,

		mo: &MapObject{
			ss: map[string]string{"a": "aaa", "b": "bbbb", "c": "ccccc"},
			mo: map[string]IntObject{"a": io},
			mr: map[string]*IntObject{"a": &io, "b": nil},
			mi: map[string]IObject{"a": &SubString{s: "aa"}, "b": nil},
		},

		io: io,
		uio: &UintObject{
			ui8:   1,
			ui16:  1<<16 - 1,
			ui32:  1<<32 - 1,
			ui64:  0,
			sui16: []uint16{1, 2, 1<<16 - 1, 1 << 15, 0},
			sui32: []uint32{1, 2, 1<<32 - 1, 1 << 15, 0},
			sui64: []uint64{1, 2, 1<<64 - 1, 1 << 15, 0},
		},
		fo: &FloatObject{
			f32:  1.10001,
			f64:  9.90001,
			sf32: []float32{11.1, 2.22, 36 * 167.333, -3 * 167.333, 0},
			sf64: []float64{11.1, 2.22, 365 * 167.333, -365 * 167.333, 0},
		},

		sub:               nil,
		subSlice:          []*SubObject{{a: 1, c: 2.2}, nil, {a: 10, c: 0.2}},
		subInterface:      &SubString{s: "aa"},
		subInterfaceSlice: []IObject{&SubString{s: "aa"}, nil},
	}
}

func TestCodec(t *testing.T) {
	o := NewCodecObject()

	size := o.Size()
	buf, _ := o.Marshal(nil)
	if len(buf) != size {
		t.Fatalf("error size, exp: %d; got: %d", len(buf), size)
	}

	newO := &CodecObject{}
	err := newO.Unmarshal(buf)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !reflect.DeepEqual(o.mo, newO.mo) {
		fmt.Printf("old: %+v \n", *o.mo)
		fmt.Printf("new: %+v \n", *newO.mo)

		t.Fatalf("codec MapObject failed")
	}

	if !reflect.DeepEqual(o, newO) {
		fmt.Printf("old: %+v \n", *o)
		fmt.Printf("new: %+v \n", *newO)

		t.Fatalf("codec failed")
	}
}

func TestCodecEmpty(t *testing.T) {
	obj := &CodecObject{
		a:  "",
		b:  nil,
		c:  nil,
		d:  nil,
		e:  nil,
		f:  false,
		g:  0,
		mo: &MapObject{},
		io: IntObject{
			si16: nil,
			si32: nil,
			si64: nil,
		},
		uio: &UintObject{
			sui16: nil,
			sui32: nil,
			sui64: nil,
		},
		fo: &FloatObject{
			sf32: nil,
			sf64: nil,
		},
	}

	buf, _ := obj.Marshal(nil)
	other := &CodecObject{}
	assert.NoError(t, other.Unmarshal(buf))

	assert.Equal(t, obj.a, other.a)
	assert.Equal(t, obj.b, other.b)
	assert.Equal(t, obj.c, other.c)
	assert.Equal(t, obj.d, other.d)
	assert.Equal(t, obj.e, other.e)
	assert.Equal(t, obj.f, other.f)
	assert.Equal(t, obj.g, other.g)
	assert.Equal(t, obj.io.si16, other.io.si16)
	assert.Equal(t, obj.io.si32, other.io.si32)
	assert.Equal(t, obj.io.si64, other.io.si64)
	assert.Equal(t, obj.uio.sui16, other.uio.sui16)
	assert.Equal(t, obj.uio.sui32, other.uio.sui32)
	assert.Equal(t, obj.uio.sui64, other.uio.sui64)
	assert.Equal(t, obj.fo.sf32, other.fo.sf32)
	assert.Equal(t, obj.fo.sf64, other.fo.sf64)
}

// go test -bench=BenchmarkCodec -run="BenchmarkCodec" -cpuprofile cpu.out
// go tool pprof -http=":8199" cpu.out
// BenchmarkCodec-12           9974            105888 ns/op
func BenchmarkCodec(b *testing.B) {
	o := NewCodecObject()
	for i := 0; i < 2048; i++ {
		o.io.si64 = append(o.io.si64, int64(i))
		o.fo.sf64 = append(o.fo.sf64, float64(i)*788.986)
		o.e = append(o.e, fmt.Sprintf("s_%d", i))
	}
	for i := 0; i < 512; i++ {
		o.subSlice = append(o.subSlice, &SubObject{a: int64(i) * 17, c: float64(i) * 133.43})
		o.subInterfaceSlice = append(o.subInterfaceSlice, &SubString{s: fmt.Sprintf("sub_%d", i)})
	}

	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, o.Size())
		buf, _ = o.Marshal(buf)
		_ = buf

		newO := &CodecObject{}
		_ = newO.Unmarshal(buf)
	}
}

func BenchmarkBuffer(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 0, 10000))

	b100 := make([]byte, 100)
	b10 := make([]byte, 10)
	b1000 := make([]byte, 1000)

	for i := 0; i < b.N; i++ {

		for k := 0; k < 10; k++ {
			buf.Write(b100)
			buf.Write(b10)
			buf.Write(b1000)
		}

	}

}

func BenchmarkBuffer2(b *testing.B) {
	var buf []byte = make([]byte, 0, 10000)

	b100 := make([]byte, 100)
	b10 := make([]byte, 10)
	b1000 := make([]byte, 1000)

	for i := 0; i < b.N; i++ {

		for k := 0; k < 10; k++ {
			buf = append(buf, b100...)
			buf = append(buf, b10...)
			buf = append(buf, b1000...)
		}

	}
}
