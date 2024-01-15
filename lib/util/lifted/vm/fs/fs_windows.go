package fs

import (
	"unsafe"

	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
	"golang.org/x/sys/windows"
)

var (
	kernelDLL = windows.MustLoadDLL("kernel32.dll")
	procDisk  = kernelDLL.MustFindProc("GetDiskFreeSpaceExW")
)

// panic at windows, if file already open by another process.
// one of possible solutions - change files opening process with correct flags.
// https://github.com/dgraph-io/badger/issues/699
// https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers
func mustSyncPath(string) {
}

func mustGetFreeSpace(path string) uint64 {
	var freeBytes int64
	r, _, err := procDisk.Call(uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&freeBytes)))
	if r == 0 {
		logger.GetLogger().Error("cannot get free space: %v", zap.Error(err))
		return 0
	}
	return uint64(freeBytes)
}
