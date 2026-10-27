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
	"io"
	"sync"
	_ "unsafe" // for go linkname

	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/cpu"
)

//go:linkname codecs github.com/apache/arrow/go/v13/parquet/compress.codecs
var codecs map[compress.Compression]compress.Codec
var defaultZSTDCodec compress.Codec

func init() {
	defaultZSTDCodec = codecs[compress.Codecs.Zstd]
	codecs[compress.Codecs.Zstd] = &zstdCodec{}

}

type zstdCodec struct {
	once     sync.Once
	encoders [zstd.SpeedBestCompression + 1]*zstd.Encoder
}

func (z *zstdCodec) Decode(dst, src []byte) []byte {
	return defaultZSTDCodec.Decode(dst, src)
}

func (z *zstdCodec) NewReader(r io.Reader) io.ReadCloser {
	return defaultZSTDCodec.NewReader(r)
}

func (z *zstdCodec) NewWriter(w io.Writer) io.WriteCloser {
	return defaultZSTDCodec.NewWriter(w)
}

func (z *zstdCodec) NewWriterLevel(w io.Writer, level int) (io.WriteCloser, error) {
	return defaultZSTDCodec.NewWriterLevel(w, level)
}

func (z *zstdCodec) Encode(dst, src []byte) []byte {
	z.onceInit()
	enc := z.encoders[zstd.SpeedDefault]
	return enc.EncodeAll(src, dst[:0])
}

func (z *zstdCodec) EncodeLevel(dst, src []byte, level int) []byte {
	z.onceInit()
	enc := z.encoders[zstd.EncoderLevelFromZstd(level)]
	return enc.EncodeAll(src, dst[:0])
}

func (z *zstdCodec) CompressBound(n int64) int64 {
	return defaultZSTDCodec.CompressBound(n)
}

func (z *zstdCodec) onceInit() {
	z.once.Do(func() {
		levels := []zstd.EncoderLevel{
			zstd.SpeedFastest,
			zstd.SpeedDefault,
			zstd.SpeedBetterCompression,
			zstd.SpeedBestCompression,
		}
		for _, level := range levels {
			z.encoders[level] = newZSTDEncoder(level)
		}
	})
}

func newZSTDEncoder(level zstd.EncoderLevel) *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithZeroFrames(true),
		zstd.WithEncoderConcurrency(max(1, cpu.GetCpuNum()/2)),
		zstd.WithEncoderLevel(level))

	if err != nil {
		panic("[BUG] failed to create zstd encoder: " + err.Error())
	}
	return enc
}
