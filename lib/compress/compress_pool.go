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
	"github.com/openGemini/openGemini/lib/util"
	"github.com/pierrec/lz4/v4"
)

var gzipWriterPool = pool.NewUnionPool[gzip.Writer](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *gzip.Writer {
		return gzip.NewWriter(nil)
	})

func GetGzipWriter(w io.Writer) (io.Writer, func()) {
	gz := gzipWriterPool.Get()
	gz.Reset(w)
	return gz, func() {
		util.MustClose(gz)
		gzipWriterPool.PutWithMemSize(gz, 0)
	}
}

var gzipReaderPool sync.Pool

func GetGzipReader(r io.Reader) (*gzip.Reader, error) {
	zr, ok := gzipReaderPool.Get().(*gzip.Reader)
	if zr == nil || !ok {
		return gzip.NewReader(r)
	}

	if err := zr.Reset(r); err != nil {
		return nil, err
	}
	return zr, nil
}

// PutGzipReader returns back gzip reader obtained via GetGzipReader.
func PutGzipReader(zr *gzip.Reader) {
	util.MustClose(zr)
	gzipReaderPool.Put(zr)
}

var zstdWriterPool = pool.NewUnionPool[zstd.Encoder](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *zstd.Encoder {
		encoder, _ := zstd.NewWriter(nil)
		return encoder
	})

func GetZstdWriter(w io.Writer) (io.Writer, func()) {
	writer := zstdWriterPool.Get()
	writer.Reset(w)
	return writer, func() {
		util.MustClose(writer)
		writer.Reset(nil)
		zstdWriterPool.PutWithMemSize(writer, 0)
	}
}

var snappyBlockWriterPool = pool.NewUnionPool[SnappyBlockWriter](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *SnappyBlockWriter { return NewSnappyBlockWriter(nil) })

func GetSnappyBlockWriter(w io.Writer) (io.Writer, func()) {
	writer := snappyBlockWriterPool.Get()
	writer.Reset(w)
	return writer, func() {
		util.MustClose(writer)
		writer.Reset(nil)
		snappyBlockWriterPool.PutWithMemSize(writer, 0)
	}
}

var snappyWriterPool = pool.NewUnionPool[snappy.Writer](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *snappy.Writer {
		return snappy.NewBufferedWriter(nil)
	})

func GetSnappyWriter(w io.Writer) (io.Writer, func()) {
	writer := snappyWriterPool.Get()
	writer.Reset(w)
	return writer, func() {
		util.MustClose(writer)
		writer.Reset(nil)
		snappyWriterPool.PutWithMemSize(writer, 0)
	}
}

var lz4WriterPool = pool.NewUnionPool[lz4.Writer](
	cpu.GetCpuNum()*2, pool.DefaultMaxEleMemSize, pool.DefaultMaxLocalEleMemSize,
	func() *lz4.Writer {
		return lz4.NewWriter(nil)
	})

func GetLz4Writer(w io.Writer) (io.Writer, func()) {
	writer := lz4WriterPool.Get()
	writer.Reset(w)
	return writer, func() {
		util.MustClose(writer)
		writer.Reset(nil)
		lz4WriterPool.PutWithMemSize(writer, 0)
	}
}
