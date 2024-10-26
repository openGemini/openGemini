package httpd

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/lib/cpu"
)

type lazyGzipResponseWriter struct {
	BaseCompressedResponseWriter
}

// Header implements http.ResponseWriter.
func (w *lazyGzipResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func NewGzipResponseWriter(w http.ResponseWriter) *lazyGzipResponseWriter {
	return &lazyGzipResponseWriter{
		BaseCompressedResponseWriter: BaseCompressedResponseWriter{
			ResponseWriter: w,
			Writer:         w,
			Flusher:        w.(http.Flusher),
		},
	}
}

// gzipFilter determines if the client can accept compressed responses, and encodes accordingly.
func gzipFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}

		gw := NewGzipResponseWriter(w)

		if f, ok := w.(http.Flusher); ok {
			gw.Flusher = f
		}

		if cn, ok := w.(http.CloseNotifier); ok {
			gw.CloseNotifier = cn
		}

		defer gw.Close()

		inner.ServeHTTP(gw, r)
	})
}

func (w *lazyGzipResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}

	w.wroteHeader = true
	if code == http.StatusOK {
		w.Header().Set("Content-Encoding", "gzip")
		// Add gzip compressor
		if _, ok := w.Writer.(*gzip.Writer); !ok {
			w.Writer = getGzipWriter(w.Writer)
		}
	}

	w.ResponseWriter.WriteHeader(code)
}

func (w *lazyGzipResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Writer.Write(p)
}

func (w *lazyGzipResponseWriter) Flush() {
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

func (w *lazyGzipResponseWriter) Close() error {
	if gw, ok := w.Writer.(*gzip.Writer); ok {
		putGzipWriter(gw)
	}

	return nil
}

type GzipWriterPool struct {
	sp    sync.Pool
	cache chan *gzip.Writer
}

func NewGzipWriterPool() *GzipWriterPool {
	p := &GzipWriterPool{
		sp: sync.Pool{
			New: func() interface{} {
				return gzip.NewWriter(nil)
			},
		},
		cache: make(chan *gzip.Writer, cpu.GetCpuNum()*2),
	}
	return p
}

func (p *GzipWriterPool) Get() *gzip.Writer {
	select {
	case gz := <-p.cache:
		return gz
	default:
		gz, ok := p.sp.Get().(*gzip.Writer)
		if !ok {
			gz = gzip.NewWriter(nil)
		}
		return gz
	}
}

func (p *GzipWriterPool) Put(gz *gzip.Writer) {
	select {
	case p.cache <- gz:
	default:
		p.sp.Put(gz)
	}
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
