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

package compress_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/compress"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/stretchr/testify/require"
)

type codecFunc func(in []byte, out []byte) ([]byte, error)

var zeroData = make([]float64, 1000)
var sameData = make([]float64, 1000)
var rleData = make([]float64, 1000)
var randData = make([]float64, 1000)
var randIntData = make([]float64, 1000)

func init() {
	sameData[0] = 1.1

	for i := 1; i < 1000; i++ {
		sameData[i] = 1.1
		randData[i] = rand.Float64() * 10000
		randIntData[i] = float64(rand.Int63())

		rleData[i] = rleData[i-1]
		if i%200 == 0 {
			rleData[i] += 10.7
		}
	}
}

func runCodecTest(t *testing.T, enc codecFunc, dec codecFunc, data []float64) {
	buf := record.Float64Slice2byte(data)

	var encOut []byte

	encBuf, err := enc(buf, encOut)
	require.NoError(t, err)

	decBuf, err := dec(encBuf, nil)
	require.NoError(t, err)
	require.Equal(t, buf, decBuf)
}

func TestSameValue(t *testing.T) {
	rle := compress.NewRLE(record.Float64SizeBytes)

	runCodecTest(t, rle.SameValueEncoding, rle.SameValueDecoding, zeroData)
	runCodecTest(t, rle.SameValueEncoding, rle.SameValueDecoding, sameData)
}

func TestSnappy(t *testing.T) {
	runCodecTest(t, compress.SnappyEncoding, compress.SnappyDecoding, zeroData)
	runCodecTest(t, compress.SnappyEncoding, compress.SnappyDecoding, randData)
	runCodecTest(t, compress.SnappyEncoding, compress.SnappyDecoding, randIntData)
}

func TestRLE(t *testing.T) {
	rle := compress.NewRLE(record.Float64SizeBytes)
	runCodecTest(t, rle.Encoding, rle.Decoding, zeroData)
	runCodecTest(t, rle.Encoding, rle.Decoding, rleData)
}

func TestGorilla(t *testing.T) {
	runCodecTest(t, compress.GorillaEncoding, compress.GorillaDecoding, zeroData)
	runCodecTest(t, compress.GorillaEncoding, compress.GorillaDecoding, rleData)
	runCodecTest(t, compress.GorillaEncoding, compress.GorillaDecoding, randData)
	runCodecTest(t, compress.GorillaEncoding, compress.GorillaDecoding, randIntData)
}

func runBenchmark(b *testing.B, name string, enc codecFunc, dec codecFunc, data []float64) {
	in := record.Float64Slice2byte(data)
	var encOut []byte
	var decOut []byte

	b.Run(name, func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			encOut, _ = enc(in, encOut[:0])
			decOut, _ = dec(encOut, decOut[:0])
		}
	})
}

func BenchmarkRLEData(b *testing.B) {
	rle := compress.NewRLE(record.Float64SizeBytes)
	runBenchmark(b, "RLE", rle.Encoding, rle.Decoding, rleData)
	runBenchmark(b, "Gorilla", compress.GorillaEncoding, compress.GorillaDecoding, rleData)
	runBenchmark(b, "Snappy", compress.SnappyEncoding, compress.SnappyDecoding, rleData)
}

func BenchmarkSameData(b *testing.B) {
	rle := compress.NewRLE(record.Float64SizeBytes)
	runBenchmark(b, "Same", rle.SameValueEncoding, rle.SameValueEncoding, sameData)
	runBenchmark(b, "RLE", rle.Encoding, rle.Decoding, sameData)
	runBenchmark(b, "Gorilla", compress.GorillaEncoding, compress.GorillaDecoding, sameData)
	runBenchmark(b, "Snappy", compress.SnappyEncoding, compress.SnappyDecoding, sameData)
}

func BenchmarkRandData(b *testing.B) {
	runBenchmark(b, "Gorilla", compress.GorillaEncoding, compress.GorillaDecoding, randData)
	runBenchmark(b, "Snappy", compress.SnappyEncoding, compress.SnappyDecoding, randData)
}

func BenchmarkRandIntData(b *testing.B) {
	runBenchmark(b, "Gorilla", compress.GorillaEncoding, compress.GorillaDecoding, randIntData)
	runBenchmark(b, "Snappy", compress.SnappyEncoding, compress.SnappyDecoding, randIntData)
}
