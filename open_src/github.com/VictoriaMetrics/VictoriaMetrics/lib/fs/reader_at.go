package fs

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/openGemini/openGemini/lib/fileops"
)

// Disable mmap by default
var disableMmap = false

// MustReadAtCloser is rand-access read interface.
type MustReadAtCloser interface {
	// MustReadAt must read len(p) bytes from offset off to p.
	MustReadAt(p []byte, off int64)

	// MustClose must close the reader.
	MustClose()
}

// ReaderAt implements rand-access reader.
type ReaderAt struct {
	f        fileops.File
	mmapData []byte
}

// MustReadAt reads len(p) bytes at off from r.
func (r *ReaderAt) MustReadAt(p []byte, off int64) {
	if len(p) == 0 {
		return
	}
	if off < 0 {
		logger.Panicf("off=%d cannot be negative", off)
	}
	if len(r.mmapData) == 0 {
		n, err := r.f.ReadAt(p, off)
		if err != nil {
			logger.Panicf("FATAL: cannot read %d bytes at offset %d of file %q: %s", len(p), off, r.f.Name(), err)
		}
		if n != len(p) {
			logger.Panicf("FATAL: unexpected number of bytes read; got %d; want %d", n, len(p))
		}
	} else {
		if off > int64(len(r.mmapData)-len(p)) {
			logger.Panicf("off=%d is out of allowed range [0...%d] for len(p)=%d", off, len(r.mmapData)-len(p), len(p))
		}
		src := r.mmapData[off:]
		// The copy() below may result in thread block as described at https://valyala.medium.com/mmap-in-go-considered-harmful-d92a25cb161d .
		// But production workload proved this is OK in most cases, so use it without fear :)
		copy(p, src)
	}
}

// MustClose closes r.
func (r *ReaderAt) MustClose() {
	fname := r.f.Name()
	if len(r.mmapData) > 0 {
		if err := fileops.MUnmap(r.mmapData[:cap(r.mmapData)]); err != nil {
			logger.Panicf("FATAL: cannot unmap data for file %q: %s", fname, err)
		}
		r.mmapData = nil
	}
	MustClose(r.f)
	r.f = nil
}

// MustFadviseSequentialRead hints the OS that f is read mostly sequentially.
//
// if prefetch is set, then the OS is hinted to prefetch f data.
func (r *ReaderAt) MustFadviseSequentialRead(prefetch bool) {
	if err := fadviseSequentialRead(r.f, prefetch); err != nil {
		logger.Panicf("FATAL: error in fadviseSequentialRead(%q, %v): %s", r.f.Name(), prefetch, err)
	}
}

// MustOpenReaderAt opens ReaderAt for reading from filename.
//
// MustClose must be called on the returned ReaderAt when it is no longer needed.
func MustOpenReaderAt(path string) *ReaderAt {
	f, err := fileops.Open(path)
	if err != nil {
		logger.Panicf("FATAL: cannot open file %q for reading: %s", path, err)
	}
	var r ReaderAt
	r.f = f
	if !disableMmap {
		fi, err := f.Stat()
		if err != nil {
			MustClose(f)
			logger.Panicf("FATAL: error in fstat(%q): %s", path, err)
		}
		size := fi.Size()
		data, err := mmapFile(f, size)
		if err != nil {
			MustClose(f)
			logger.Panicf("FATAL: cannot mmap %q: %s", path, err)
		}
		r.mmapData = data
	}
	return &r
}

func mmapFile(f fileops.File, size int64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	if size < 0 {
		return nil, fmt.Errorf("got negative file size: %d bytes", size)
	}
	if int64(int(size)) != size {
		return nil, fmt.Errorf("file is too big to be mmap'ed: %d bytes", size)
	}
	// Round size to multiple of 4KB pages as `man 2 mmap` recommends.
	// This may help preventing SIGBUS panic at https://github.com/VictoriaMetrics/VictoriaMetrics/issues/581
	// The SIGBUS could occur if standard copy(dst, src) function may read beyond src bounds.
	sizeOrig := size
	if size%4096 != 0 {
		size += 4096 - size%4096
	}
	data, err := fileops.Mmap(int(f.Fd()), 0, int(size))
	if err != nil {
		return nil, fmt.Errorf("cannot mmap file with size %d: %w", size, err)
	}
	return data[:sizeOrig], nil
}
