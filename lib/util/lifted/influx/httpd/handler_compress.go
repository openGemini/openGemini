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

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	compression "github.com/openGemini/openGemini/lib/compress"
	"github.com/pierrec/lz4/v4"
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
		acceptEncoding := strings.ToLower(r.Header.Get("Accept-Encoding"))
		switch {
		case strings.Contains(acceptEncoding, "gzip"):
			gz := compression.GetGzipWriter(w)
			defer gz.Close()
			writer = gz
			w.Header().Set("Content-Encoding", "gzip")
		case strings.Contains(acceptEncoding, "zstd"):
			enc := compression.GetZstdWriter(w)
			defer enc.Close()
			writer = enc
			w.Header().Set("Content-Encoding", "zstd")
		case strings.Contains(acceptEncoding, "snappy"):
			sn := compression.GetSnappyWriter(w)
			defer sn.Close()
			writer = sn
			w.Header().Set("Content-Encoding", "snappy")
		case strings.Contains(acceptEncoding, "lz4"):
			lz4w := compression.GetLz4Writer(w)
			defer lz4w.Close()
			writer = lz4w
			w.Header().Set("Content-Encoding", "lz4")
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

func (w *lazyCompressResponseWriter) Close() error {
	switch writer := w.Writer.(type) {
	case *gzip.Writer:
		compression.PutGzipWriter(writer)
	case *zstd.Encoder:
		compression.PutZstdWriter(writer)
	case *snappy.Writer:
		compression.PutSnappyWriter(writer)
	case *lz4.Writer:
		compression.PutLz4Writer(writer)
	default:
		return nil
	}
	return nil
}
