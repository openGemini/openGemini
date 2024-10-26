package httpd

import (
	"io"
	"net/http"
)

type CompressedResponseWriter interface {
    Write([]byte) (int, error)
    Flush()
    Close() error
}

type BaseCompressedResponseWriter struct {
	ResponseWriter http.ResponseWriter
	Writer         io.Writer
	Flusher        http.Flusher
	CloseNotifier  http.CloseNotifier
	wroteHeader    bool
}

func (w *BaseCompressedResponseWriter) WriteHeader(code int) {
    if !w.wroteHeader {
        w.ResponseWriter.WriteHeader(code)
        w.wroteHeader = true
    }
}

func (w *BaseCompressedResponseWriter) Write(p []byte) (int, error) {
    if !w.wroteHeader {
        w.WriteHeader(http.StatusOK)
    }
    return w.Writer.Write(p)
}

func (w *BaseCompressedResponseWriter) Flush() {
    if f, ok := w.Writer.(interface {
        Flush()
    }); ok {
        f.Flush()
    }
    if w.Flusher != nil {
        w.Flusher.Flush()
    }
}