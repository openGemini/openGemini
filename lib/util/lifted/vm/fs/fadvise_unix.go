//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package fs

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/fileops"
)

func fadviseSequentialRead(f fileops.File, prefetch bool) error {
	fd := int(f.Fd())
	mode := fileops.FADV_SEQUENTIAL
	if prefetch {
		mode |= fileops.FADV_WILLNEED
	}
	if err := fileops.Fadvise(fd, 0, 0, mode); err != nil {
		return fmt.Errorf("error returned from fileops.Fadvise(%d): %w", mode, err)
	}
	return nil
}
