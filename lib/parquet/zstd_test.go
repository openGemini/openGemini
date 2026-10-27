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

package parquet

import (
	"crypto/rand"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/stretchr/testify/require"
)

func TestZSTDCodec(t *testing.T) {
	codec, err := compress.GetCodec(compress.Codecs.Zstd)
	require.NoError(t, err)

	buf := make([]byte, 64)
	_, err = rand.Read(buf)
	require.NoError(t, err)
	for range 3 {
		buf = append(buf, buf...)
	}

	var decode = func(src []byte) {
		require.True(t, len(src) > 0)
		decData := codec.Decode(nil, src)
		require.Equal(t, buf, decData)
	}

	decode(codec.EncodeLevel(nil, buf, 1))
	decode(codec.EncodeLevel(nil, buf, 10))
	decode(codec.Encode(nil, buf))

	require.NotEmpty(t, codec.NewWriter(nil))
	require.NotEmpty(t, codec.NewReader(nil))

	_, err = codec.NewWriterLevel(nil, 1)
	require.NoError(t, err)

	require.Less(t, int64(10), codec.CompressBound(10))
}

func TestZSTDCodecConcurrency(t *testing.T) {
	codec, err := compress.GetCodec(compress.Codecs.Zstd)
	require.NoError(t, err)

	buf := make([]byte, 64)
	_, err = rand.Read(buf)
	require.NoError(t, err)
	for range 4 {
		buf = append(buf, buf...)
	}

	const C = 8
	const N = 4000

	var decode = func(src []byte) {
		require.True(t, len(src) > 0)
		decData := codec.Decode(nil, src)
		require.Equal(t, buf, decData)
	}

	var run = func() {
		var dst []byte
		for range N {
			dst = codec.EncodeLevel(dst[:0], buf, 1)
			decode(dst)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(C)
	for range C {
		go func() {
			defer wg.Done()
			run()
		}()
	}
	wg.Wait()
}
