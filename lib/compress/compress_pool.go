/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package compress

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/pierrec/lz4/v4"
)

// #region Gzip Writer Pool
var gzipWriterPool *pool.UnionPool[gzip.Writer] = pool.NewUnionPool[gzip.Writer](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *gzip.Writer {
		return gzip.NewWriter(nil)
	})

func GetGzipWriter(w io.Writer) *gzip.Writer {
	gz := gzipWriterPool.Get()
	gz.Reset(w)
	return gz
}

func PutGzipWriter(gz *gzip.Writer) {
	gz.Close()
	gzipWriterPool.Put(gz)
}

// #endregion

// #region Gzip Reader Pool
var gzipReaderPool sync.Pool

func GetGzipReader(r io.Reader) (*gzip.Reader, error) {
	v := gzipReaderPool.Get()
	if v == nil {
		return gzip.NewReader(r)
	}
	zr := v.(*gzip.Reader)
	if err := zr.Reset(r); err != nil {
		return nil, err
	}
	return zr, nil
}

// PutGzipReader returns back gzip reader obtained via GetGzipReader.
func PutGzipReader(zr *gzip.Reader) {
	_ = zr.Close()
	gzipReaderPool.Put(zr)
}

// #endregion

// #region Zstd Writer Pool
var zstdWriterPool *pool.UnionPool[zstd.Encoder] = pool.NewUnionPool[zstd.Encoder](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *zstd.Encoder {
		encoder, _ := zstd.NewWriter(nil)
		return encoder
	})

func GetZstdWriter(w io.Writer) *zstd.Encoder {
	zstdEncoder := zstdWriterPool.Get()
	zstdEncoder.Reset(w)
	return zstdEncoder
}

func PutZstdWriter(zstdEncoder *zstd.Encoder) {
	zstdEncoder.Close()
	zstdWriterPool.Put(zstdEncoder)
}

// #endregion

// #region Zstd Reader Pool
var zstdReaderPool *pool.UnionPool[zstd.Decoder] = pool.NewUnionPool[zstd.Decoder](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *zstd.Decoder {
		decoder, _ := zstd.NewReader(nil)
		return decoder
	})

func GetZstdReader(r io.Reader) *zstd.Decoder {
	zstdDecoder := zstdReaderPool.Get()
	err := zstdDecoder.Reset(r)
	if err != nil {
		return nil
	}
	return zstdDecoder
}

func PutZstdReader(zstdDecoder *zstd.Decoder) {
	zstdDecoder.Close()
	zstdReaderPool.Put(zstdDecoder)
}

// #endregion

// #region sanppy Writer Pool
var snappyWriterPool *pool.UnionPool[snappy.Writer] = pool.NewUnionPool[snappy.Writer](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *snappy.Writer {
		return snappy.NewBufferedWriter(nil)
	})

func GetSnappyWriter(w io.Writer) *snappy.Writer {
	snappyWriter := snappyWriterPool.Get()
	snappyWriter.Reset(w)
	return snappyWriter
}

func PutSnappyWriter(snappyWriter *snappy.Writer) {
	snappyWriter.Close()
	snappyWriterPool.Put(snappyWriter)
}

// #endregion

// #region Snappy Reader Pool
var snappyReaderPool *pool.UnionPool[snappy.Reader] = pool.NewUnionPool[snappy.Reader](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *snappy.Reader {
		return snappy.NewReader(nil)
	})

func GetSnappyReader(r io.Reader) *snappy.Reader {
	snappyReader := snappyReaderPool.Get()
	snappyReader.Reset(r)
	return snappyReader
}

func PutSnappyReader(snappyReader *snappy.Reader) {
	snappyReaderPool.Put(snappyReader)
}

// #endregion

// #region lz4 Writer Pool
var lz4WriterPool *pool.UnionPool[lz4.Writer] = pool.NewUnionPool[lz4.Writer](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *lz4.Writer {
		return lz4.NewWriter(nil)
	})

func GetLz4Writer(w io.Writer) *lz4.Writer {
	lz4Writer := lz4WriterPool.Get()
	lz4Writer.Reset(w)
	return lz4Writer
}

func PutLz4Writer(lz4Writer *lz4.Writer) {
	lz4Writer.Close()
	lz4WriterPool.Put(lz4Writer)
}

// #endregion
