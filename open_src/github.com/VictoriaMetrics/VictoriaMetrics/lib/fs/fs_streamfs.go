//go:build streamfs
// +build streamfs

package fs

import (
	"math"
)

// Streamfs does not support sync directory.
func mustSyncPath(path string) {
	return
}

func mustGetFreeSpace(path string) uint64 {
	return math.MaxUint64
}

func init() {
	*disableMmap = true
}
