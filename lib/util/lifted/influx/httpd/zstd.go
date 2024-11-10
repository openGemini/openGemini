package httpd

import (
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/cpu"
)

type lazyZstdResponseWriter struct {
	io.Writer
	http.ResponseWriter
	http.Flusher
	http.CloseNotifier
	wroteHeader bool
}

func zstdFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") {
			inner.ServeHTTP(w, r)
			return
		}

		zwWriter := &lazyZstdResponseWriter{ResponseWriter: w, Writer: w}

		if f, ok := w.(http.Flusher); ok {
			zwWriter.Flusher = f
		}

		if cn, ok := w.(http.CloseNotifier); ok {
			zwWriter.CloseNotifier = cn
		}

		defer zwWriter.Close()

		inner.ServeHTTP(zwWriter, r)
	})
}

func (w *lazyZstdResponseWriter) Close() error {
	if zw, ok := w.Writer.(*zstd.Encoder); ok {
		putZstdWriter(zw)
	}
	return nil
}

func (w *lazyZstdResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *lazyZstdResponseWriter) Flush() {
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

func (w *lazyZstdResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	if statusCode == http.StatusOK {
		w.Header().Set("Content-Encoding", "zstd")
		// Add zstd compressor
		if _, ok := w.Writer.(*zstd.Encoder); !ok {
			w.Writer = getZstdWriter(w.Writer)
		}
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

// Write implements http.ResponseWriter.
func (w *lazyZstdResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Writer.Write(p)
}

type ZstdWriterPool struct {
	sp    sync.Pool
	cache chan *zstd.Encoder
}

func NewZstdWriterPool() *ZstdWriterPool {
	p := &ZstdWriterPool{
		sp: sync.Pool{
			New: func() interface{} {
				encoder, _ := zstd.NewWriter(nil)
				return encoder
			},
		},
		cache: make(chan *zstd.Encoder, cpu.GetCpuNum()*2),
	}
	return p
}

func (p *ZstdWriterPool) Get() *zstd.Encoder {
	select {
	case encoder := <-p.cache:
		return encoder
	default:
		encoder, ok := p.sp.Get().(*zstd.Encoder)
		if !ok {
			encoder, _ = zstd.NewWriter(nil)
		}
		return encoder
	}
}

func (p *ZstdWriterPool) Put(encoder *zstd.Encoder) {
	select {
	case p.cache <- encoder:
	default:
		p.sp.Put(encoder)
	}
}

func (p *ZstdWriterPool) Close() {
	close(p.cache)
}

var zstdWriterPool = NewZstdWriterPool()

func getZstdWriter(w io.Writer) *zstd.Encoder {
	encoder := zstdWriterPool.Get()
	encoder.Reset(w)
	return encoder
}

func putZstdWriter(encoder *zstd.Encoder) {
	encoder.Close()
	zstdWriterPool.Put(encoder)
}

// var zstdReaderPool sync.Pool

// func getZstdReader(r io.Reader) (*zstd.Decoder, error) {
// 	v := zstdReaderPool.Get()
// 	if v == nil {
// 		reader, err := zstd.NewReader(r)
// 		if err == nil {
// 			return reader, nil
// 		}
// 		return nil, err
// 	}
// 	zr := v.(*zstd.Decoder)
// 	if err := zr.Reset(r); err != nil {
// 		return nil, err
// 	}
// 	return zr, nil
// }

// func putZstdReader(zr *zstd.Decoder) {
// 	zr.Close()
// 	zstdReaderPool.Put(zr)
// }
