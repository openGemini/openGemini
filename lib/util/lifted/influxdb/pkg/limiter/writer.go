package limiter

import (
	"context"
	"io"
	"os"
)

type Writer struct {
	w       io.WriteCloser
	limiter Rate
	ctx     context.Context
}

type Rate interface {
	WaitN(ctx context.Context, n int) error
	Burst() int
}

// Write writes bytes from b.
func (s *Writer) Write(b []byte) (int, error) {
	if s.limiter == nil {
		return s.w.Write(b)
	}

	var n int
	for n < len(b) {
		wantToWriteN := len(b[n:])
		if wantToWriteN > s.limiter.Burst() {
			wantToWriteN = s.limiter.Burst()
		}

		wroteN, err := s.w.Write(b[n : n+wantToWriteN])
		if err != nil {
			return n, err
		}
		n += wroteN

		if err := s.limiter.WaitN(s.ctx, wroteN); err != nil {
			return n, err
		}
	}

	return n, nil
}

func (s *Writer) Sync() error {
	if f, ok := s.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (s *Writer) Name() string {
	if f, ok := s.w.(*os.File); ok {
		return f.Name()
	}
	return ""
}

func (s *Writer) Close() error {
	return s.w.Close()
}
