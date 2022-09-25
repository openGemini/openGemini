//go:build !streamfs
// +build !streamfs

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

//nolint
package fileops

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type file struct {
	of *os.File
}

func (f *file) Close() error {
	return f.of.Close()
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	return f.of.Seek(offset, whence)
}

func (f *file) Write(b []byte) (int, error) {
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOWriteTotalCount, 1)
	atomic.AddInt64(&statistics.IOStat.IOWriteTotalBytes, int64(len(b)))
	res, err := f.of.Write(b)
	opsStatEnd(begin.UnixNano(), opsTypeWrite, int64(res))
	return res, err
}

func (f *file) ReadAt(b []byte, off int64) (int, error) {
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOReadTotalCount, 1)
	atomic.AddInt64(&statistics.IOStat.IOReadTotalBytes, int64(len(b)))
	res, err := f.of.ReadAt(b, off)
	opsStatEnd(begin.UnixNano(), opsTypeRead, int64(res))
	return res, err
}

func (f *file) Read(b []byte) (int, error) {
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOReadTotalCount, 1)
	atomic.AddInt64(&statistics.IOStat.IOReadTotalBytes, int64(len(b)))
	res, err := f.of.Read(b)
	opsStatEnd(begin.UnixNano(), opsTypeRead, int64(res))
	return res, err
}

func (f *file) Name() string {
	return f.of.Name()
}

func (f *file) Truncate(size int64) error {
	return f.of.Truncate(size)
}

func (f *file) Sync() error {
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOSyncTotalCount, 1)
	err := f.of.Sync()
	if err != nil {
		return err
	}
	opsStatEnd(begin.UnixNano(), opsTypeSync, int64(0))
	return err
}

func (f *file) SyncUpdateLength() error {
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOSyncTotalCount, 1)
	err := f.of.Sync()
	if err != nil {
		return err
	}
	opsStatEnd(begin.UnixNano(), opsTypeSync, int64(0))
	return err
}

func (f *file) Stat() (os.FileInfo, error) {
	return f.of.Stat()
}

func (f *file) Readdir(n int) ([]os.FileInfo, error) {
	return f.of.Readdir(n)
}

func (f *file) Fd() uintptr {
	return f.of.Fd()
}

type vfs struct{}

func NewFS() VFS {
	return &vfs{}
}

func (vfs) Open(name string, _ ...FSOption) (File, error) {
	f, err := os.Open(path.Clean(name))
	return &file{of: f}, err
}

func (vfs) OpenFile(name string, flag int, perm os.FileMode, _ ...FSOption) (File, error) {
	fd, err := os.OpenFile(path.Clean(name), flag, perm) // #nosec
	if err != nil {
		return nil, err
	}

	return &file{of: fd}, nil
}

func (vfs) Create(name string, _ ...FSOption) (File, error) {
	f, err := os.Create(path.Clean(name))
	return &file{of: f}, err
}

func (vfs) Remove(name string, _ ...FSOption) error {
	return os.Remove(name)
}

func (vfs) RemoveAll(path string, _ ...FSOption) error {
	logger.GetLogger().Info("remove path", zap.String("path", path))
	return os.RemoveAll(path)
}

func (vfs) Mkdir(path string, perm os.FileMode, _ ...FSOption) error {
	return os.Mkdir(path, perm)
}

func (vfs) MkdirAll(path string, perm os.FileMode, _ ...FSOption) error {
	return os.MkdirAll(path, perm)
}

func (vfs) ReadDir(dirname string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

func (vfs) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

func (vfs) RenameFile(oldPath, newPath string, _ ...FSOption) error {
	return os.Rename(oldPath, newPath)
}

func (vfs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (vfs) WriteFile(filename string, data []byte, perm os.FileMode, _ ...FSOption) error {
	return ioutil.WriteFile(filename, data, perm)
}

func (vfs) ReadFile(filename string, _ ...FSOption) ([]byte, error) {
	return ioutil.ReadFile(path.Clean(filename))
}

func (vfs) CreateTime(name string) (*time.Time, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return nil, err
	}

	st := fi.Sys().(*syscall.Stat_t)
	tm := time.Unix(st.Ctim.Sec, st.Ctim.Nsec)
	return &tm, nil
}

func (vfs) Truncate(name string, size int64, _ ...FSOption) error {
	return os.Truncate(name, size)
}

func (f vfs) CopyFile(srcFile, dstFile string, opt ...FSOption) (written int64, err error) {
	srcFd, err := f.Open(srcFile, opt...)
	if err != nil {
		return 0, err
	}
	defer util.MustClose(srcFd)

	dstFd, err := f.Create(dstFile, opt...)
	if err != nil {
		return 0, err
	}
	defer util.MustClose(dstFd)

	return io.Copy(dstFd, srcFd)
}
