package fs

import "math"

// panic at windows, if file already open by another process.
// one of possible solutions - change files opening process with correct flags.
// https://github.com/dgraph-io/badger/issues/699
// https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers
func mustSyncPath(string) {
}

func mustGetFreeSpace(path string) uint64 {
	// not implement in windows
	// it will caused panic after go1.24
	return math.MaxUint64
}
