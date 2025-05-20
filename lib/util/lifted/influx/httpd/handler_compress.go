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

package httpd

import (
	"io"
	"net/http"
	"strings"

	compression "github.com/openGemini/openGemini/lib/compress"
)

type lazyCompressResponseWriter struct {
	io.Writer
	http.ResponseWriter
	http.Flusher
	http.CloseNotifier
	wroteHeader bool

	acceptEncoding string
	release        func()
}

// compressFilter determines if the client can accept compressed responses, and encodes accordingly.
func compressFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressEnabledWriter := &lazyCompressResponseWriter{
			ResponseWriter: w,
			Writer:         w,
			acceptEncoding: strings.ToLower(r.Header.Get("Accept-Encoding")),
		}

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
	if w.wroteHeader {
		return
	}

	w.wroteHeader = true
	if code == http.StatusOK {
		w.rewriteWriter()
	}

	w.ResponseWriter.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(code)
}

type lazyCompressWriterCreator func(io.Writer) (io.Writer, func())

var lazyCompressWriterFactory = map[string]lazyCompressWriterCreator{
	"gzip":          compression.GetGzipWriter,
	"lz4":           compression.GetLz4Writer,
	"zstd":          compression.GetZstdWriter,
	"snappy-stream": compression.GetSnappyWriter,
	"snappy":        compression.GetSnappyBlockWriter,
}

func (w *lazyCompressResponseWriter) rewriteWriter() {
	for key, creator := range lazyCompressWriterFactory {
		if strings.Contains(w.acceptEncoding, key) {
			w.Header().Set("Content-Encoding", key)
			w.Writer, w.release = creator(w.Writer)
			break
		}
	}
}

func (w *lazyCompressResponseWriter) Close() {
	if w.release != nil {
		w.release()
		w.release = nil
	}
}

func (w *lazyCompressResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Writer.Write(p)
}

type Flushable interface {
	Flush()
}

func (w *lazyCompressResponseWriter) Flush() {
	// Flush writer, if supported
	if f, ok := w.Writer.(Flushable); ok {
		f.Flush()
	}

	// Flush the HTTP response
	if w.Flusher != nil {
		w.Flusher.Flush()
	}
}
