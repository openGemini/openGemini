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

package encoding

import (
	safeRand "crypto/rand"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var decs = NewCoderContext()

func genTimes(n int, precision time.Duration, isRand bool) []int64 {
	t := time.Date(2021, 1, 1, 12, 30, 30, 0, time.UTC).UnixNano()
	a := make([]int64, n)
	var buf [1]byte
	for i := 0; i < n; i++ {
		if isRand {
			_, _ = safeRand.Read(buf[:])
			t += (time.Duration(buf[0]) * precision).Nanoseconds()
		} else {
			t += (time.Duration(60) * precision).Nanoseconds()
		}
		a[i] = t
	}
	return a
}

func TestEncoding_FloatBlock(t *testing.T) {
	testFloat := func(outPrefix []byte, valueCount int) {
		var err error
		var buf [1024]byte

		values := make([]float64, valueCount)
		for i := range values {
			values[i] = float64(i) + 1.351
		}

		in := util.Float64Slice2byte(values)
		out := buf[:0]
		out = append(out, outPrefix...)
		priLen := len(out)
		out, err = EncodeFloatBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if priLen > 0 {
			if !reflect.DeepEqual(out[:priLen], outPrefix) {
				t.Fatalf("invlid compressed prefix data, exp:%v, get:%v", outPrefix, out[:priLen])
			}
		}

		decodedValues := make([]float64, 0, valueCount)
		decOut := util.Float64Slice2byte(decodedValues)
		decOut = append(decOut, outPrefix...)
		outValues, err := DecodeFloatBlock(out[priLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if priLen > 0 {
			exp := util.Bytes2Float64Slice(outPrefix)
			if !reflect.DeepEqual(outValues[:priLen/8], exp) {
				t.Fatalf("invlid uncompressed prefix data, exp:%v, get:%v", exp, decOut[:priLen])
			}
		}

		if !reflect.DeepEqual(outValues[priLen/8:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues[priLen:], values)
		}
	}

	arr := []float64{11, 22, 333, 444, 555}
	rows := []int{0, 1, 100, 100}
	for _, row := range rows {
		testFloat(nil, row)
		testFloat(util.Float64Slice2byte(arr), row)
	}
}

func TestEncoding_FloatBlock_SimilarFloats(t *testing.T) {
	var err error

	values := make([]float64, 12)
	values[0] = 6.00065e+06
	values[1] = 6.000656e+06
	values[2] = 6.000657e+06
	values[3] = 6.000659e+06
	values[4] = 6.000661e+06
	values[6] = 6.000663e+06
	values[7] = 6.000667e+06
	values[8] = 6.000669e+06
	values[9] = 6.000671e+06
	values[10] = 6.000681e+06
	values[11] = 6.000691e+06

	var buf [128]byte
	in := util.Float64Slice2byte(values)
	out := buf[:0]
	out, err = EncodeFloatBlock(in, out, decs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues [12]float64
	decOut := util.Float64Slice2byte(decodedValues[:0])
	outValues, err := DecodeFloatBlock(out, &decOut, decs)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(values, outValues) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues, values)
	}
}

func TestEncoding_FloatBlock_SameValues(t *testing.T) {
	var err error
	values := make([]float64, 1000)
	testFloat := func(allSame bool) {
		genFloat := func() {
			if allSame {
				for i := range values {
					values[i] = 1.1
				}
				return
			}

			for i := 0; i < 500; i++ {
				values[i] = 1.1
			}
			for i := 500; i < 1000; i++ {
				values[i] = 2.2
			}
		}

		genFloat()

		var buf [128]byte
		in := util.Float64Slice2byte(values)
		out := buf[:0]
		out, err = EncodeFloatBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var decodedValues [1000]float64
		decOut := util.Float64Slice2byte(decodedValues[:0])
		outValues, err := DecodeFloatBlock(out, &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if !reflect.DeepEqual(values, outValues) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues, in)
		}
	}

	testFloat(true)
	testFloat(false)
}

func TestEncoding_IntBlock(t *testing.T) {
	testInt := func(prefix []byte, values []int64) {
		var err error
		var buf [1024]byte
		in := util.Int64Slice2byte(values)
		out := buf[:0]
		out = append(out, prefix...)
		preLen := len(out)

		out, err = EncodeIntegerBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if preLen > 0 {
			if !reflect.DeepEqual(out[:preLen], prefix) {
				t.Fatalf("invlid compressed prefix data, exp:%v, get:%v", prefix, out[:preLen])
			}
		}

		decodedValues := make([]int64, 0, len(values))
		decOut := util.Int64Slice2byte(decodedValues)
		decOut = append(decOut, prefix...)
		outValues, err := DecodeIntegerBlock(out[preLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if preLen > 0 {
			get := util.Int64Slice2byte(outValues[:preLen/8])
			if !reflect.DeepEqual(get, prefix) {
				t.Fatalf("invlid uncompressed prefix data, exp:%v, get:%v", prefix, get)
			}
		}

		if len(values) > 0 && !reflect.DeepEqual(outValues[preLen/8:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues[:preLen/8], in)
		}
	}

	inValues := [][]int64{
		[]int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		[]int64{5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5},
		[]int64{-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}
	arr := []int64{11, 22, 33, 44, 55, 66}
	for _, values := range inValues {
		testInt(nil, values)
		testInt(util.Int64Slice2byte(arr), values)
	}
}

func TestEncoding_IntBlock_Basic(t *testing.T) {
	testInt := func(prefix []byte, valueCount int) {
		values := genTimes(valueCount, time.Second, false)

		var err error
		var buf [1024]byte
		in := util.Int64Slice2byte(values)
		out := buf[:0]
		out = append(out, prefix...)
		preLen := len(out)

		out, err = EncodeIntegerBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if preLen > 0 {
			if !reflect.DeepEqual(out[:preLen], prefix) {
				t.Fatalf("invlid compressed prefix data, exp:%v, get:%v", prefix, out[:preLen])
			}
		}

		decodedValues := make([]int64, 0, valueCount)
		decOut := util.Int64Slice2byte(decodedValues)
		decOut = append(decOut, prefix...)
		outValues, err := DecodeIntegerBlock(out[preLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if preLen > 0 {
			get := util.Int64Slice2byte(outValues[:preLen/8])
			if !reflect.DeepEqual(get, prefix) {
				t.Fatalf("invlid uncompressed prefix data, exp:%v, get:%v", prefix, get)
			}
		}

		if !reflect.DeepEqual(outValues[preLen/8:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues[:preLen/8], in)
		}
	}

	counts := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000}
	arr := []int64{11, 22, 33, 44, 55, 66}
	for _, count := range counts {
		testInt(nil, count)
		testInt(util.Int64Slice2byte(arr), count)
	}
}

func TestEncoding_IntBlock_randomdata(t *testing.T) {
	valueCount := 100
	values := make([]int64, valueCount)
	intTest := func(prefix []byte) {
		_, err := safeRand.Read(util.Int64Slice2byte(values))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < valueCount; i++ {
			if values[i] < 0 {
				values[i] = -values[i]
			}
		}

		var buf [1024]byte
		in := util.Int64Slice2byte(values)
		out := buf[:0]
		out = append(out, prefix...)
		preLen := len(out)

		out, err = EncodeIntegerBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if preLen > 0 {
			if !reflect.DeepEqual(out[:preLen], prefix) {
				t.Fatalf("invlid compressed prefix data, exp:%v, get:%v", prefix, out[:preLen])
			}
		}

		decodedValues := make([]int64, 0, valueCount)
		decOut := util.Int64Slice2byte(decodedValues[:])
		decOut = append(decOut[:0], prefix...)
		outValue, err := DecodeIntegerBlock(out[preLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		if preLen > 0 {
			get := util.Int64Slice2byte(outValue[:preLen/8])
			if !reflect.DeepEqual(get, prefix) {
				t.Fatalf("invlid uncompressed prefix data, exp:%v, get:%v", prefix, get)
			}
		}

		if !reflect.DeepEqual(outValue[preLen/8:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValue[preLen/8:], values)
		}
	}

	intTest(nil)
	arr := []int64{1, 2, 3, 3, 2, 1}
	intTest(util.Int64Slice2byte(arr))
}

func TestEncoding_IntBlock_Negatives(t *testing.T) {
	valueCount := 1000
	intTest := func(prefix []byte) {
		values := make([]int64, valueCount)
		for i := range values {
			v := int64(i)
			if i%2 == 0 {
				v = -v
			}
			values[i] = v
		}

		var err error
		var buf [1024]byte
		in := util.Int64Slice2byte(values)
		out := buf[:0]
		out = append(out, prefix...)
		prefixLen := len(prefix)
		out, err = EncodeIntegerBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decodedValues := make([]int64, 0, valueCount)
		decOut := util.Int64Slice2byte(decodedValues[:])
		decOut = append(decOut[:0], prefix...)
		outValues, err := DecodeIntegerBlock(out[prefixLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if !reflect.DeepEqual(outValues[prefixLen/8:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues[prefixLen/8:], values)
		}
	}

	intTest(nil)
	arr := []int64{10, 11, 12, 33, 112233}
	intTest(util.Int64Slice2byte(arr))
}

func TestEncoding_BooleanBlock_Basic(t *testing.T) {
	boolTest := func(preData []byte, valueCount int) {
		values := make([]bool, valueCount)
		for i := range values {
			v := true
			if i%2 == 0 {
				v = false
			}
			values[i] = v
		}

		var err error
		var buf [64]byte
		in := util.BooleanSlice2byte(values)
		out := buf[:0]
		out = append(out, preData...)
		preLen := len(preData)
		out, err = EncodeBooleanBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decodedValues := make([]bool, 0, valueCount)
		decOut := util.BooleanSlice2byte(decodedValues[:])
		decOut = append(decOut, preData...)
		outValues, err := DecodeBooleanBlock(out[preLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if !reflect.DeepEqual(outValues[preLen:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues[preLen:], values)
		}
	}

	arr := []bool{true, false, false, true, true}
	counts := []int{0, 1, 10, 1000}
	for _, count := range counts {
		boolTest(nil, count)
		boolTest(util.BooleanSlice2byte(arr), count)
	}
}

func TestEncoding_StringBlock_Basic(t *testing.T) {
	var tmpBuf [4096]byte

	testFun := func(valueCount int, alg int, prefix []byte) {
		values := make([]byte, 0, valueCount*64)
		offset := make([]uint32, 0, valueCount)
		for i := 0; i < valueCount; i++ {
			v := fmt.Sprintf("test string value %d", i)
			offset = append(offset, uint32(len(values)))
			values = append(values, v...)
		}

		if decs.stringCoder == nil {
			decs.stringCoder = GetStringCoder()
		}
		decs.stringCoder.encodingType = alg

		var err error
		var buf [1024]byte
		out := buf[:0]
		out = append(out, prefix...)
		prefixLen := len(prefix)
		out, err = EncodeStringBlock(values, offset, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if prefixLen > 0 {
			if !reflect.DeepEqual(out[:prefixLen], prefix) {
				t.Fatalf("encode output prefix not eq, exp:%v, get:%v", prefix, out[:prefixLen])
			}
		}

		var decOff []uint32
		decOut := tmpBuf[:0]
		decOut = append(decOut, prefix...)
		decOut, decOff, err = DecodeStringBlock(out[prefixLen:], &decOut, &decOff, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if len(offset) > 0 {
			if !reflect.DeepEqual(decOff, offset) {
				t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOff, offset)
			}
		}

		if prefixLen > 0 {
			if !reflect.DeepEqual(decOut[:prefixLen], prefix) {
				t.Fatalf("decode output prefix not eq, exp:%v, get:%v", prefix, decOut[:prefixLen])
			}
		}

		if !reflect.DeepEqual(decOut[prefixLen:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOut[prefixLen:], values)
		}
	}

	items := []int{0, 1, 10, 100, 500, 1024}
	compAlg := []int{1, 2}
	prefixs := [][]byte{nil, []byte("test string compression data |")}
	for _, alg := range compAlg {
		for _, count := range items {
			for _, prefix := range prefixs {
				testFun(count, alg, prefix)
			}
		}
	}
}

func TestEncoding_StringBlock_Uncompressed(t *testing.T) {
	var offs []uint32
	var do []byte

	uncompTest := func(alg int) {
		valueCount := 1500
		values := make([]byte, 0, valueCount*64)
		offset := make([]uint32, 0, valueCount)
		var tmp [8]byte
		for i := 0; i < valueCount; i++ {
			_, _ = safeRand.Read(tmp[:])
			offset = append(offset, uint32(len(values)))
			values = append(values, tmp[:]...)
		}

		if decs.stringCoder == nil {
			decs.stringCoder = GetStringCoder()
		}
		decs.stringCoder.SetEncodingType(alg)

		var err error
		var buf [1024]byte
		out := buf[:0]
		out, err = EncodeStringBlock(values, offset, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		do = do[:0]
		offs = offs[:0]
		decOut, decOffset, err := DecodeStringBlock(out, &do, &offs, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		if !reflect.DeepEqual(decOut, values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOut, values)
		}
		if !reflect.DeepEqual(decOffset, offset) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOffset, values)
		}
	}

	uncompTest(1)
	uncompTest(2)
}

func TestStringEncodingVersion_Compatibility(t *testing.T) {
	var tmpBuf [4096]byte

	testFun := func(valueCount int, alg int, prefix []byte) {
		values := make([]byte, 0, valueCount*64)
		offset := make([]uint32, 0, valueCount)
		for i := 0; i < valueCount; i++ {
			v := fmt.Sprintf("test string value %d", i)
			offset = append(offset, uint32(len(values)))
			values = append(values, v...)
		}

		if decs.stringCoder == nil {
			decs.stringCoder = GetStringCoder()
		}
		decs.stringCoder.encodingType = alg

		var err error
		var buf [1024]byte
		out := buf[:0]
		out = append(out, prefix...)
		prefixLen := len(prefix)
		if len(values) == 0 {
			return
		}
		// pack string by version V1
		src := packStringV1(values, offset, decs)
		out, err = decs.stringCoder.Encoding(src, out)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if prefixLen > 0 {
			if !reflect.DeepEqual(out[:prefixLen], prefix) {
				t.Fatalf("encode output prefix not eq, exp:%v, get:%v", prefix, out[:prefixLen])
			}
		}

		var decOff []uint32
		decOut := tmpBuf[:0]
		decOut = append(decOut, prefix...)
		// decode string with version
		decOut, decOff, err = DecodeStringBlock(out[prefixLen:], &decOut, &decOff, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if len(offset) > 0 {
			if !reflect.DeepEqual(decOff, offset) {
				t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOff, offset)
			}
		}

		if prefixLen > 0 {
			if !reflect.DeepEqual(decOut[:prefixLen], prefix) {
				t.Fatalf("decode output prefix not eq, exp:%v, get:%v", prefix, decOut[:prefixLen])
			}
		}

		if !reflect.DeepEqual(decOut[prefixLen:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOut[prefixLen:], values)
		}
	}

	items := []int{0, 1, 10, 100, 500, 1024, 2048}
	compAlg := []int{1, 2}
	prefixs := [][]byte{nil, []byte("test string compression data |")}
	for _, alg := range compAlg {
		for _, count := range items {
			for _, prefix := range prefixs {
				testFun(count, alg, prefix)
			}
		}
	}
}

func TestStringEncodingVersion_CompatibilityV1(t *testing.T) {
	var tmpBuf [4096]byte

	testFun := func(valueCount int, alg int, prefix []byte) {
		values := make([]byte, 0, valueCount*64)
		offset := make([]uint32, 0, valueCount)
		for i := 0; i < valueCount; i++ {
			v := fmt.Sprintf("test string value %d", i)
			offset = append(offset, uint32(len(values)))
			values = append(values, v...)
		}

		if decs.stringCoder == nil {
			decs.stringCoder = GetStringCoder()

		}
		decs.stringCoder.encodingType = alg

		var err error
		var buf [1024]byte
		out := buf[:0]
		out = append(out, prefix...)
		prefixLen := len(prefix)
		if len(values) == 0 {
			return
		}
		// pack string by version V1
		src := packStringV1(values, offset, decs)
		out, err = decs.stringCoder.Encoding(src, out)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if prefixLen > 0 {
			if !reflect.DeepEqual(out[:prefixLen], prefix) {
				t.Fatalf("encode output prefix not eq, exp:%v, get:%v", prefix, out[:prefixLen])
			}
		}

		var decOff []uint32
		decOut := tmpBuf[:0]
		decOut = append(decOut, prefix...)
		// decode string with version
		decOut, decOff, err = DecodeStringBlock(out[prefixLen:], &decOut, &decOff, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if len(offset) > 0 {
			if !reflect.DeepEqual(decOff, offset) {
				t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOff, offset)
			}
		}

		if prefixLen > 0 {
			if !reflect.DeepEqual(decOut[:prefixLen], prefix) {
				t.Fatalf("decode output prefix not eq, exp:%v, get:%v", prefix, decOut[:prefixLen])
			}
		}

		if !reflect.DeepEqual(decOut[prefixLen:], values) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decOut[prefixLen:], values)
		}
	}

	items := []int{0, 1}
	compAlg := []int{3}
	prefixs := [][]byte{nil, []byte("test string compression data |")}
	for _, alg := range compAlg {
		for _, count := range items {
			for _, prefix := range prefixs {
				testFun(count, alg, prefix)
			}
		}
	}
}

func TestDecodingWithLz4Error(t *testing.T) {
	if decs.stringCoder == nil {
		decs.stringCoder = GetStringCoder()

	}
	decs.stringCoder.encodingType = StringCompressedLz4
	in := make([]byte, 0, 4)
	in = append(in, byte(StringCompressedLz4)<<4)
	buf := make([]byte, 4096)
	in = append(in, buf...)
	var out []byte
	out, err := decs.stringCoder.Decoding(in, out)
	assert.NotNil(t, err)
}

func TestEncoding_Timestamp_Second(t *testing.T) {
	valueCount := 1000
	tmTest := func(precision time.Duration, readTm bool, prefix []byte) {
		times := genTimes(valueCount, precision, readTm)

		var err error
		var buf [1024]byte
		in := util.Int64Slice2byte(times)
		out := append(buf[:0], prefix...)
		preLen := len(prefix)
		out, err = EncodeIntegerBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if preLen > 0 {
			if !reflect.DeepEqual(out[:preLen], prefix) {
				t.Fatalf("invalid encoding prefix, exp:%v, get:%v", prefix, out[:preLen])
			}
		}

		decOut := make([]byte, 0, valueCount*8)
		decOut = append(decOut, prefix...)
		outValues, err := DecodeIntegerBlock(out[preLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}
		if preLen > 0 {
			preValue := util.Bytes2Int64Slice(prefix)
			if !reflect.DeepEqual(preValue, outValues[:preLen/8]) {
				t.Fatalf("invalid decode prefix, exp:%v, get:%v", preValue, outValues[:preLen/8])
			}
		}

		if !reflect.DeepEqual(outValues[preLen/8:], times) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues[preLen/8:], times)
		}

		t.Logf("inLen:%v, outLen:%v, comptRatio:%v", len(in), len(out), float64(len(out))/float64(len(in)))
	}

	arr := []int64{11, 112, 113, 114, 115, 116, 117}
	tmTest(time.Second, false, nil)
	tmTest(time.Millisecond, false, nil)
	tmTest(time.Nanosecond, false, util.Int64Slice2byte(arr))

	tmTest(time.Second, true, nil)
	tmTest(time.Second, true, util.Int64Slice2byte(arr))
	tmTest(time.Millisecond, true, nil)
	tmTest(time.Millisecond, true, util.Int64Slice2byte(arr))
	tmTest(time.Nanosecond, true, nil)
	tmTest(time.Nanosecond, true, util.Int64Slice2byte(arr))
}

func TestEncodeIncompressibleInteger(t *testing.T) {
	origArr := make([]int64, 500)
	values := make([]int64, 500)
	buf := util.Int64Slice2byte(origArr)
	_, err := safeRand.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	copy(values, origArr)

	in := util.Int64Slice2byte(values)
	out := make([]byte, 0, 2048)
	out, err = EncodeIntegerBlock(in, out, decs)
	if err != nil {
		t.Fatal(err)
	}

	ob := make([]byte, 0, 510*8)
	decOut, err := DecodeIntegerBlock(out, &ob, decs)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(values, decOut) {
		t.Fatalf("exp:%v, \nget:%v\n", in, decOut)
	}
}

func TestEncodeTimestampBlock(t *testing.T) {
	valueCount := 1000
	tmTest := func(precision time.Duration, randTm bool, prefix []byte) {
		times := genTimes(valueCount, precision, randTm)
		sort.Slice(times, func(i, j int) bool {
			return times[i] < times[j]
		})

		var err error
		var buf [1024]byte
		in := util.Int64Slice2byte(times)
		out := append(buf[:0], prefix...)
		preLen := len(prefix)
		out, err = EncodeTimestampBlock(in, out, decs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if preLen > 0 {
			if !reflect.DeepEqual(out[:preLen], prefix) {
				t.Fatalf("invalid encoding prefix, exp:%v, get:%v", prefix, out[:preLen])
			}
		}

		decOut := make([]byte, 0, valueCount*8)
		decOut = append(decOut, prefix...)
		outValues, err := DecodeTimestampBlock(out[preLen:], &decOut, decs)
		if err != nil {
			t.Fatalf("unexpected error decoding block: %v", err)
		}

		if preLen > 0 {
			preValue := util.Bytes2Int64Slice(prefix)
			if !reflect.DeepEqual(preValue, outValues[:preLen/8]) {
				t.Fatalf("invalid decode prefix, exp:%v, get:%v", preValue, outValues[:preLen/8])
			}
		}

		if !reflect.DeepEqual(outValues[preLen/8:], times) {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", outValues[preLen/8:], times)
		}

		t.Logf("inLen:%v, outLen:%v, comptRatio:%v", len(in), len(out), float64(len(out))/float64(len(in)))
	}

	arr := []int64{11, 112, 113, 114, 115, 116, 117}
	tmTest(time.Second, false, nil)
	tmTest(time.Millisecond, false, nil)
	tmTest(time.Nanosecond, false, util.Int64Slice2byte(arr))

	tmTest(time.Second, true, nil)
	tmTest(time.Millisecond, true, nil)
	tmTest(time.Nanosecond, true, util.Int64Slice2byte(arr))
}

func TestGetCompressAlgo(t *testing.T) {
	conf := config.GetStoreConfig()
	defer func() {
		conf.StringCompressAlgo = config.CompressAlgoSnappy
	}()

	conf.StringCompressAlgo = config.CompressAlgoLZ4
	require.Equal(t, StringCompressedLz4, GetCompressAlgo())

	conf.StringCompressAlgo = config.CompressAlgoZSTD
	require.Equal(t, StringCompressedZstd, GetCompressAlgo())

	conf.StringCompressAlgo = "xxx"
	require.Equal(t, stringCompressedSnappy, GetCompressAlgo())
}
