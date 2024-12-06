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

package httpd

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/cpu"
)

type lazyCompressResponseWriter struct {
	io.Writer
	http.ResponseWriter
	http.Flusher
	http.CloseNotifier
	wroteHeader bool
}

// compressFilter determines if the client can accept compressed responses, and encodes accordingly.
func compressFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var writer io.Writer = w
		acceptEncoding := r.Header.Get("Accept-Encoding")
		switch {
		case strings.Contains(acceptEncoding, "gzip"):
			gz := getGzipWriter(w)
			defer gz.Close()
			writer = gz
			w.Header().Set("Content-Encoding", "gzip")
		case strings.Contains(acceptEncoding, "zstd"):
			enc := getZstdWriter(w)
			defer enc.Close()
			writer = enc
			w.Header().Set("Content-Encoding", "zstd")
		case strings.Contains(acceptEncoding, "snappy"):
			sn := getSnappyWriter(w)
			defer sn.Close()
			writer = sn
			w.Header().Set("Content-Encoding", "snappy")
		default:
			inner.ServeHTTP(w, r)
			return
		}

		compressEnabledWriter := &lazyCompressResponseWriter{ResponseWriter: w, Writer: writer}

		if f, ok := w.(http.Flusher); ok {
			compressEnabledWriter.Flusher = f
		}

		if cn, ok := w.(http.CloseNotifier); ok {
			compressEnabledWriter.CloseNotifier = cn
		}

		defer compressEnabledWriter.Close()

		inner.ServeHTTP(compressEnabledWriter, r)
	})
}

func (w *lazyCompressResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.ResponseWriter.Header().Del("Content-Length")
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *lazyCompressResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Writer.Write(p)
}

func (w *lazyCompressResponseWriter) Flush() {
	// Flush writer, if supported
	if f, ok := w.Writer.(interface {
		Flush()
	}); ok {
		f.Flush()
	}

	// Flush the HTTP response
	if w.Flusher != nil {
		w.Flusher.Flush()
	}
}

func (w *lazyCompressResponseWriter) Close() error {
	if gw, ok := w.Writer.(*gzip.Writer); ok {
		putGzipWriter(gw)
	}
	if zw, ok := w.Writer.(*zstd.Encoder); ok {
		putZstdWriter(zw)
	}
	return nil
}

type FixedCachePool struct {
	cache chan interface{}
	sp    sync.Pool
}

func NewFixedCachePool(size int, newFunc func() interface{}) *FixedCachePool {
	return &FixedCachePool{
		cache: make(chan interface{}, size),
		sp: sync.Pool{
			New: newFunc,
		},
	}
}

func (p *FixedCachePool) Get() interface{} {
	select {
	case item := <-p.cache:
		return item
	default:
		item := p.sp.Get()
		if item == nil {
			item = p.sp.New()
		}
		return item
	}
}

func (p *FixedCachePool) Put(item interface{}) {
	select {
	case p.cache <- item:
	default:
		p.sp.Put(item)
	}
}

// ******************** region gzip write pool ***********************
type GzipWriterPool struct {
	pool *FixedCachePool
}

func NewGzipWriterPool() *GzipWriterPool {
	p := &GzipWriterPool{
		pool: NewFixedCachePool(cpu.GetCpuNum()*2, func() interface{} {
			return gzip.NewWriter(nil)
		}),
	}
	return p
}

func (p *GzipWriterPool) Get() *gzip.Writer {
	return p.pool.Get().(*gzip.Writer)
}

func (p *GzipWriterPool) Put(gz *gzip.Writer) {
	p.pool.Put(gz)
}

var gzipWriterPool = NewGzipWriterPool()

func getGzipWriter(w io.Writer) *gzip.Writer {
	gz := gzipWriterPool.Get()
	gz.Reset(w)
	return gz
}

func putGzipWriter(gz *gzip.Writer) {
	gz.Close()
	gzipWriterPool.Put(gz)
}

// ******************** endregion gzip write pool ***********************

// ******************** region gzip read pool ***************************

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

var gzipReaderPool sync.Pool

// ******************** endregion gzip read pool ***************************

// ******************** region zstd write pool ***********************
type ZstdWriterPool struct {
	pool *FixedCachePool
}

func NewZstdWriterPool() *ZstdWriterPool {
	p := &ZstdWriterPool{
		pool: NewFixedCachePool(cpu.GetCpuNum()*2, func() interface{} {
			encoder, _ := zstd.NewWriter(nil)
			return encoder
		}),
	}
	return p
}

func (p *ZstdWriterPool) Get() *zstd.Encoder {
	return p.pool.Get().(*zstd.Encoder)
}

func (p *ZstdWriterPool) Put(zstdEncoder *zstd.Encoder) {
	p.pool.Put(zstdEncoder)
}

var zstdWriterPool = NewZstdWriterPool()

func getZstdWriter(w io.Writer) *zstd.Encoder {
	zstdEncoder := zstdWriterPool.Get()
	zstdEncoder.Reset(w)
	return zstdEncoder
}

func putZstdWriter(zstdEncoder *zstd.Encoder) {
	zstdEncoder.Close()
	zstdWriterPool.Put(zstdEncoder)
}

// ******************** endregion zstd write pool ***********************

// ******************** region snappy write pool ***************************
type SnappyWriterPool struct {
	pool *FixedCachePool
}

func NewSnappyWriterPool() *SnappyWriterPool {
	p := &SnappyWriterPool{
		pool: NewFixedCachePool(cpu.GetCpuNum()*2, func() interface{} {
			return snappy.NewBufferedWriter(nil)
		}),
	}
	return p
}

func (p *SnappyWriterPool) Get() *snappy.Writer {
	return p.pool.Get().(*snappy.Writer)
}

func (p *SnappyWriterPool) Put(snappyWriter *snappy.Writer) {
	p.pool.Put(snappyWriter)
}

var snappyWriterPool = NewSnappyWriterPool()

func getSnappyWriter(w io.Writer) *snappy.Writer {
	snappyWriter := snappyWriterPool.Get()
	snappyWriter.Reset(w)
	return snappyWriter
}

func putSnappyWriter(snappyWriter *snappy.Writer) {
	snappyWriter.Close()
	snappyWriterPool.Put(snappyWriter)
}

// ******************** endregion snappy write pool ***************************
