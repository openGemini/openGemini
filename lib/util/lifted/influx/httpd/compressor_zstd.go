package httpd

import (
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/cpu"
)

type lazyZSTDResponseWriter struct {
	BaseCompressedResponseWriter
}

// Header implements http.ResponseWriter.
func (w *lazyZSTDResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func NewZSTDResponseWriter(w http.ResponseWriter) *lazyZSTDResponseWriter {
	encoder := getZSTDEncoder(w)
	return &lazyZSTDResponseWriter{
		BaseCompressedResponseWriter: BaseCompressedResponseWriter{
			ResponseWriter: w,
			Writer:         encoder,
			Flusher:        w.(http.Flusher),
		},
	}
}

// ZSTDFilter determines if the client can accept compressed responses, and encodes accordingly.
func ZSTDFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") {
			inner.ServeHTTP(w, r)
			return
		}

		zw := NewZSTDResponseWriter(w)
		if f, ok := w.(http.Flusher); ok {
			zw.Flusher = f
		}

		if cn, ok := w.(http.CloseNotifier); ok {
			zw.CloseNotifier = cn
		}

		defer zw.Close()

		inner.ServeHTTP(zw, r)
	})
}

func (w *lazyZSTDResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}

	w.wroteHeader = true
	if code == http.StatusOK {
		w.Header().Set("Content-Encoding", "zstd")
		// Add zstd compressor
		if _, ok := w.Writer.(*zstd.Encoder); !ok {
			w.Writer = getZSTDEncoder(w.Writer)
		}
	}

	w.ResponseWriter.WriteHeader(code)
}

func (w *lazyZSTDResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Writer.Write(p)
}

func (w *lazyZSTDResponseWriter) Flush() {
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

func (w *lazyZSTDResponseWriter) Close() error {
	if zw, ok := w.Writer.(*zstd.Encoder); ok {
		putZSTDEncoder(zw)
	}

	return nil
}

type ZSTDWriterPool struct {
	sp    sync.Pool
	cache chan *zstd.Encoder
}

func NewZSTDWriterPool() *ZSTDWriterPool {
	p := &ZSTDWriterPool{
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
func (p *ZSTDWriterPool) Get() *zstd.Encoder {
	select {
	case encoder := <-p.cache:
		return encoder
	default:
		zstdEncoder, ok := p.sp.Get().(*zstd.Encoder)
		if !ok {
			zstdEncoder, _ = zstd.NewWriter(nil)
		}
		return zstdEncoder
	}
}

func (p *ZSTDWriterPool) Put(encoder *zstd.Encoder) {
	select {
	case p.cache <- encoder:
	default:
		p.sp.Put(encoder)
	}
}

var zstdWriterPool = NewZSTDWriterPool()

func getZSTDEncoder(w io.Writer) *zstd.Encoder {
	encoder := zstdWriterPool.Get()
	encoder.Reset(w)
	return encoder
}

func putZSTDEncoder(encoder *zstd.Encoder) {
	encoder.Reset(nil)
	zstdWriterPool.Put(encoder)
}
