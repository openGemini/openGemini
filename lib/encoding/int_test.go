// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package encoding_test

import (
	"math/rand/v2"
	"testing"

	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/require"
)

func BenchmarkIntDecode(b *testing.B) {
	coder := encoding.GetIntCoder()
	data := make([]int64, 0, 64)
	for i := 0; i < 800; i++ {
		data = append(data, 123+rand.Int64N(300))
	}

	encBuf, _ := coder.Encoding(util.Int64Slice2byte(data), nil)

	swap := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		swap, _ = coder.Decoding(encBuf, swap[:0])
	}
}

func TestIntDecode(t *testing.T) {
	var testCodec = func(data []int64) {
		exp := append([]int64{}, data...)

		coder := encoding.GetIntCoder()
		encBuf, err := coder.Encoding(util.Int64Slice2byte(data), nil)
		require.NoError(t, err)

		decData, err := coder.Decoding(encBuf, nil)
		require.NoError(t, err)

		require.Equal(t, exp, util.Bytes2Int64Slice(decData))
	}

	var run = func(N int64) {
		data := make([]int64, 0, 64)
		for i := 0; i < 500; i++ {
			data = append(data, rand.Int64N(N+1))
		}
		testCodec(data)
	}
	for i := range 60 {
		run(1 << i)
	}

	data := make([]int64, 100)
	data[0] = 1
	testCodec(data)
	data[0] = -1
	testCodec(data)
}
