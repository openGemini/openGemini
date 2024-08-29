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

package compress_test

import (
	"math"
	"testing"

	"github.com/openGemini/openGemini/lib/compress"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/require"
)

func codecFloatBlockWithMLF(t *testing.T, values []float64) {
	config.GetStoreConfig().FloatCompressAlgorithm = compress.FloatCompressAlgorithmMLF
	compress.Init()

	defer func() {
		config.GetStoreConfig().FloatCompressAlgorithm = ""
		compress.Init()
	}()

	codecFloatBlock(t, values)
}

func codecFloatBlock(t *testing.T, data []float64) {
	values := append([]float64{}, data...)

	size := len(values)
	float := compress.NewFloat()
	in := util.Float64Slice2byte(values)
	var encOut, decOut []byte
	var err error

	encOut, err = float.AdaptiveEncoding(in, nil)
	require.NoError(t, err)

	decOut, err = float.AdaptiveDecoding(encOut, decOut)
	other := util.Bytes2Float64Slice(decOut)
	require.NoError(t, err)
	require.Equal(t, size, len(other))
	for i := 0; i < size; i++ {
		if math.IsNaN(values[i]) {
			require.True(t, math.IsNaN(other[i]))
			continue
		}

		require.Equal(t, values[i], other[i])
	}
}

func TestCodecFloatBlock_one(t *testing.T) {
	var values = []float64{0}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_rand(t *testing.T) {
	var values []float64
	for i := 0; i < 1000; i++ {
		values = append(values, rand.Float64()*1000)
	}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_small(t *testing.T) {
	var values []float64
	for i := 0; i < 4; i++ {
		values = append(values, float64(rand.Int63()%10000)/100)
	}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_same(t *testing.T) {
	var values []float64
	v := rand.Float64() * 1000

	for i := 0; i < 1000; i++ {
		values = append(values, v)
	}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)

	for i := 0; i < 1000; i++ {
		values[i] = 0
	}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_int(t *testing.T) {
	var values []float64
	for i := 0; i < 1000; i++ {
		values = append(values, float64(rand.Int31n(100)))
	}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_smallDelta(t *testing.T) {
	var values []float64
	for i := 0; i < 1000; i++ {
		values = append(values, 2+0.1+rand.Float64()/10)
	}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_RLE(t *testing.T) {
	var values []float64
	for i := 0; i < 1000; i++ {
		values = append(values, float64(i/180))
	}
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_Snappy(t *testing.T) {
	var values []float64
	for i := 0; i < 1000; i++ {
		values = append(values, float64(i/180))
	}
	codecFloatBlock(t, values)
}

func TestCodecFloatBlock_NaN(t *testing.T) {
	var values []float64
	for i := 0; i < 1000; i++ {
		values = append(values, float64(i/180))
	}
	values[1] = math.NaN()
	codecFloatBlock(t, values)
	codecFloatBlockWithMLF(t, values)
}

func TestCodecFloatBlock_abnormal(t *testing.T) {
	enc := compress.NewFloat()
	out, err := enc.AdaptiveEncoding(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(out))

	out, err = enc.AdaptiveDecoding([]byte{10 << 4, 1}, nil)
	require.NotEmpty(t, err)
	require.Equal(t, errno.NewError(errno.InvalidFloatBuffer, 10).Error(), err.Error())
}

func BenchmarkAdaptive(b *testing.B) {
	enc := compress.NewFloat()

	runBenchmark(b, "Adaptive_zeroData", enc.AdaptiveEncoding, enc.AdaptiveDecoding, zeroData)
	runBenchmark(b, "Adaptive_rleData", enc.AdaptiveEncoding, enc.AdaptiveDecoding, rleData)
	runBenchmark(b, "Snappy_rleData", compress.SnappyEncoding, compress.SnappyDecoding, rleData)
}

func BenchmarkGenerateContext(b *testing.B) {
	var values []float64
	for i := 0; i < 1000000; i++ {
		values = append(values, float64(i/180))
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			compress.GenerateContext(values)
		}
	}
}
