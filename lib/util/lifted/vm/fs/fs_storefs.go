//go:build !streamfs && !windows && storefs
// +build !streamfs,!windows,storefs

package fs

import (
	"math"
	"syscall"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
)

func mustSyncPath(path string) {
	d, err := fileops.Open(path)
	if err != nil {
		logger.GetLogger().Panic(fmt.Sprintf("FATAL: cannot open %q: %s", path, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		logger.GetLogger().Panic(fmt.Sprintf("FATAL: cannot flush %q to storage: %s", path, err)
	}
	if err := d.Close(); err != nil {
		logger.GetLogger().Panic(fmt.Sprintf("FATAL: cannot close %q: %s", path, err)
	}
}

func mustGetFreeSpace(path string) uint64 {
	return math.MaxUint64
}

func freeSpace(stat syscall.Statfs_t) uint64 {
	return stat.Bavail * uint64(stat.Bsize)
}
