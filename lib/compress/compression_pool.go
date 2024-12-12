// Copyright 2024 openGemini Authors
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

package compress

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/pool"
)

// #region Gzip Writer Pool
var gzipWriterPool *pool.FixedPoolV2[*gzip.Writer] = pool.NewFixedPoolV2[*gzip.Writer](func() *gzip.Writer {
	return gzip.NewWriter(nil)
}, cpu.GetCpuNum()*2)

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
var zstdWriterPool *pool.FixedPoolV2[*zstd.Encoder] = pool.NewFixedPoolV2[*zstd.Encoder](func() *zstd.Encoder {
	encoder, _ := zstd.NewWriter(nil)
	return encoder
}, cpu.GetCpuNum()*2)

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
var snappyWriterPool = pool.NewFixedPoolV2[*snappy.Writer](func() *snappy.Writer {
	return snappy.NewBufferedWriter(nil)
}, cpu.GetCpuNum()*2)

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
