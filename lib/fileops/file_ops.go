/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fileops

import (
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

var (
	IO_PRIORITY_ULTRA_HIGH = 0
	IO_PRIORITY_HIGH       = 1
	IO_PRIORITY_NORMAL     = 2
	IO_PRIORITY_LOW        = 3
	IO_PRIORITY_LOW_READ   = 4
)

const (
	opsTypeWrite = iota
	opsTypeRead
	opsTypeSync
)

const (
	FADV_NORMAL     = 0x0
	FADV_RANDOM     = 0x1
	FADV_SEQUENTIAL = 0x2
	FADV_WILLNEED   = 0x3
	FADV_DONTNEED   = 0x4
	FADV_NOREUSE    = 0x5
)

type File interface {
	io.Closer
	io.Reader
	io.Seeker
	io.Writer
	io.ReaderAt
	Name() string
	Truncate(size int64) error
	Sync() error
	Stat() (os.FileInfo, error)
	SyncUpdateLength() error
	Fd() uintptr
}

type FSOption interface {
	Parameter() interface{}
}

type FilePriorityOption int
type FileLockOption string

func (opt FilePriorityOption) Parameter() interface{} {
	return int(opt)
}

func (opt FileLockOption) Parameter() interface{} {
	return string(opt)
}

type VFS interface {
	// Open opens the named file with specified options.
	// the optional opt is: (FileLockOption,FilePriorityOption)
	Open(name string, opt ...FSOption) (File, error)
	// OpenFile opens the named file with specified flag and other options.
	// the optional opt is: (FileLockOption,FilePriorityOption)
	OpenFile(name string, flag int, perm os.FileMode, opt ...FSOption) (File, error)
	// Create creates or truncates the named file. If the file already exists, it is truncated.
	// If the file does not exist, it is created with mode 0666
	// the optional opt is: (FileLockOption,FilePriorityOption)
	Create(name string, opt ...FSOption) (File, error)

	CreateV1(name string, opt ...FSOption) (File, error)

	// Remove removes the named file or (empty) directory.
	// the optional opt is: FileLockOption
	Remove(name string, opt ...FSOption) error
	// RemoveAll removes path and any children it contains.
	// the optional opt is: FileLockOption
	RemoveAll(path string, opt ...FSOption) error
	// Mkdir creates a directory named path, it's parents directory must exist.
	// the optional opt is: FileLockOption
	Mkdir(path string, perm os.FileMode, opt ...FSOption) error
	// MkdirAll creates a directory named path, along with any necessary parents
	// the optional opt is: FileLockOption
	MkdirAll(path string, perm os.FileMode, opt ...FSOption) error
	// ReadDir reads the directory named by dirname and returns
	// a list of fs.FileInfo for the directory's contents, sorted by filename.
	ReadDir(dirname string) ([]os.FileInfo, error)
	// Glob returns the names of all files matching pattern or nil if there is no matching file.
	Glob(pattern string) ([]string, error)
	// RenameFile renames (moves) oldPath to newPath.
	// If newPath already exists and is not a directory, Rename replaces it.
	// the optional opt is: FileLockOption
	RenameFile(oldPath, newPath string, opt ...FSOption) error

	// Stat returns a FileInfo describing the named file.
	Stat(name string) (os.FileInfo, error)

	// WriteFile writes data to a file named by filename.
	// If the file does not exist, WriteFile creates it with permissions perm
	// the optional opt is: (FileLockOption,FilePriorityOption)
	WriteFile(filename string, data []byte, perm os.FileMode, opt ...FSOption) error
	// ReadFile reads the file named by filename and returns the contents.
	// the optional opt is: FilePriorityOption
	ReadFile(filename string, opt ...FSOption) ([]byte, error)
	// CopyFile copys file content from srcFile to dstFile until either EOF is reached on srcFile or an errors accurs.
	// the optional opt is: (FileLockOption,FilePriorityOption)
	CopyFile(srcFile, dstFile string, opt ...FSOption) (written int64, err error)

	CreateTime(name string) (*time.Time, error)

	// Truncate changes the size of the file to size.
	// the optional opt is: (FileLockOption)
	Truncate(name string, size int64, opt ...FSOption) error
}

var targetFS = NewFS()

// Open opens the named file with specified options.
// the optional opt is: (FileLockOption,FilePriorityOption)
func Open(name string, opt ...FSOption) (File, error) {
	return targetFS.Open(name, opt...)
}

// OpenFile opens the named file with specified flag and other options.
// the optional opt is: (FileLockOption,FilePriorityOption)
func OpenFile(name string, flag int, perm os.FileMode, opt ...FSOption) (File, error) {
	return targetFS.OpenFile(name, flag, perm, opt...)
}

// Create creates or truncates the named file. If the file already exists, it is truncated.
// If the file does not exist, it is created with mode 0666
// the optional opt is: (FileLockOption,FilePriorityOption)
func Create(name string, opt ...FSOption) (File, error) {
	return targetFS.Create(name, opt...)
}

func CreateV1(name string, opt ...FSOption) (File, error) {
	return targetFS.CreateV1(name, opt...)
}

// Remove removes the named file or (empty) directory.
// the optional opt is: FileLockOption
func Remove(name string, opt ...FSOption) error {
	return targetFS.Remove(name, opt...)
}

// RemoveAll removes path and any children it contains.
// the optional opt is: FileLockOption
func RemoveAll(path string, opt ...FSOption) error {
	return targetFS.RemoveAll(path, opt...)
}

// Mkdir creates a directory named path, it's parents directory must exist.
// the optional opt is: FileLockOption
func Mkdir(path string, perm os.FileMode, opt ...FSOption) error {
	return targetFS.Mkdir(path, perm)
}

// MkdirAll creates a directory named path, along with any necessary parents
// the optional opt is: FileLockOption
func MkdirAll(path string, perm os.FileMode, opt ...FSOption) error {
	return targetFS.MkdirAll(path, perm, opt...)
}

// ReadDir reads the directory named by dirname and returns
// a list of fs.FileInfo for the directory's contents, sorted by filename.
func ReadDir(dirname string) ([]os.FileInfo, error) {
	return targetFS.ReadDir(dirname)
}

// Glob returns the names of all files matching pattern or nil if there is no matching file.
func Glob(pattern string) ([]string, error) {
	return targetFS.Glob(pattern)
}

// RenameFile renames (moves) oldPath to newPath.
// If newPath already exists and is not a directory, Rename replaces it.
// the optional opt is: FileLockOption
func RenameFile(oldPath, newPath string, opt ...FSOption) error {
	return targetFS.RenameFile(oldPath, newPath, opt...)
}

// Stat returns a FileInfo describing the named file.
func Stat(name string) (os.FileInfo, error) {
	return targetFS.Stat(name)
}

// WriteFile writes data to a file named by filename.
// If the file does not exist, WriteFile creates it with permissions perm
// the optional opt is: (FileLockOption,FilePriorityOption)
func WriteFile(filename string, data []byte, perm os.FileMode, opt ...FSOption) error {
	return targetFS.WriteFile(filename, data, perm, opt...)
}

// ReadFile reads the file named by filename and returns the contents.
// the optional opt is: FilePriorityOption
func ReadFile(filename string, opt ...FSOption) ([]byte, error) {
	return targetFS.ReadFile(filename, opt...)
}

// CopyFile copys file content from srcFile to dstFile until either EOF is reached on srcFile or an errors accurs.
// the optional opt is: (FileLockOption,FilePriorityOption)
func CopyFile(srcFile, dstFile string, opt ...FSOption) (written int64, err error) {
	return targetFS.CopyFile(srcFile, dstFile, opt...)
}

func CreateTime(name string) (*time.Time, error) {
	return targetFS.CreateTime(name)
}

func Truncate(name string, size int64) error {
	return targetFS.Truncate(name, size)
}

func opsStatEnd(startTime int64, opsType int, bytes int64) {
	end := time.Now().UnixNano()
	t := end - startTime

	if opsType == opsTypeWrite {
		atomic.AddInt64(&statistics.IOStat.IOWriteDuration, t)
		atomic.AddInt64(&statistics.IOStat.IOWriteOkBytes, bytes)
		atomic.AddInt64(&statistics.IOStat.IOWriteOkCount, 1)
	} else if opsType == opsTypeRead {
		atomic.AddInt64(&statistics.IOStat.IOReadDuration, t)
		atomic.AddInt64(&statistics.IOStat.IOReadOkBytes, bytes)
		atomic.AddInt64(&statistics.IOStat.IOReadOkCount, 1)
	} else {
		atomic.AddInt64(&statistics.IOStat.IOSyncDuration, t)
		atomic.AddInt64(&statistics.IOStat.IOSyncOkCount, 1)
	}
}
