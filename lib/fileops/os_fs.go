//go:build !streamfs
// +build !streamfs

// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// nolint
package fileops

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/sysinfo"
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

func (f *file) StreamReadBatch(offs []int64, sizes []int64, minBlockSize int64, c chan *request.StreamReader, obsRangeSize int, isStat bool) {
	for i, offset := range offs {
		content := make([]byte, sizes[i])
		_, err := f.ReadAt(content, offset)
		c <- &request.StreamReader{
			Offset:  offset,
			Err:     err,
			Content: content,
		}
		if err != nil {
			break
		}
	}
	close(c)
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

func (f *file) Size() (int64, error) {
	info, err := f.of.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
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
	f, err := os.OpenFile(path.Clean(name), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	return &file{of: f}, err
}

func (vfs) CreateV1(name string, _ ...FSOption) (File, error) {
	return Create(name)
}

func (vfs) CreateV2(name string, _ ...FSOption) (File, error) {
	return Create(name)
}

func (vfs) Remove(name string, _ ...FSOption) error {
	return os.Remove(name)
}

func (vfs) RemoveLocal(name string, _ ...FSOption) error {
	return os.Remove(name)
}

func (vfs) RemoveLocalEnabled(obsOptValid bool) bool {
	return obsOptValid
}

func (vfs) RemoveAll(path string, _ ...FSOption) error {
	logger.GetLogger().Info("remove path", zap.String("path", path))
	return os.RemoveAll(path)
}

// not used
func (vfs) RemoveAllWithOutDir(path string, _ ...FSOption) error {
	dirs, err := ReadDir(path)
	if err != nil {
		return err
	}
	var errs []error
	for _, dir := range dirs {
		if dir.IsDir() {
			continue
		}
		err := os.Remove(filepath.Join(path, dir.Name()))
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		er := fmt.Errorf("error remove with out dir  %v", errs[0])
		for i, err := range errs {
			if i == 0 {
				continue
			}
			er = errors.Join(er, err)
		}
		return er
	}
	return nil
}

func (vfs) Mkdir(path string, perm os.FileMode, _ ...FSOption) error {
	return os.Mkdir(path, perm)
}

func (vfs) MkdirAll(path string, perm os.FileMode, _ ...FSOption) error {
	return os.MkdirAll(path, perm)
}

func (vfs) NormalizeDirPath(path string) string {
	return path
}

func (vfs) ReadDir(dirname string) ([]fs.FileInfo, error) {
	entries, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	infos := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
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
	return os.WriteFile(filename, data, perm)
}

func (vfs) ReadFile(filename string, _ ...FSOption) ([]byte, error) {
	return os.ReadFile(path.Clean(filename))
}

func (vfs) CreateTime(name string) (*time.Time, error) {
	return sysinfo.CreateTime(name)
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

func (f vfs) IsObsFile(path string) (bool, error) {
	return false, nil
}

func (f vfs) CopyFileFromDFVToOBS(srcPath, dstPath string, opt ...FSOption) error {
	dstFd, err := OpenFile(dstPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		log.Error("create dst file fail", zap.String("name", dstPath), zap.Error(err))
		return err
	}
	defer util.MustClose(dstFd)

	srcFd, err := f.Open(srcPath, opt...)
	if err != nil {
		return err
	}
	defer util.MustClose(srcFd)

	_, err = io.Copy(dstFd, srcFd)
	return err
}

func (f vfs) GetAllFilesSizeInPath(path string) (int64, int64, int64, error) {
	fi, err := f.Stat(path)
	if err != nil {
		return 0, 0, 0, err
	}
	size := fi.Size()
	return size, 0, 0, nil
}

func (f vfs) GetOBSTmpFileName(path string, obsOption *obs.ObsOptions) string {
	if obsOption != nil {
		path = path[len(obs.GetPrefixDataPath()):]
		path = filepath.Join(obsOption.BasePath, path) + obs.ObsFileSuffix + obs.ObsFileTmpSuffix
		path = EncodeObsPath(obsOption.Endpoint, obsOption.BucketName, path, obsOption.Ak, DecryptObsSk(obsOption.Sk))
		return path
	}
	return path + obs.ObsFileTmpSuffix
}

func (f vfs) GetOBSTmpIndexFileName(path string, obsOption *obs.ObsOptions) string {
	if obsOption != nil {
		path = path[len(obs.GetPrefixDataPath()):]
		path = filepath.Join(obsOption.BasePath, path)
		path = EncodeObsPath(obsOption.Endpoint, obsOption.BucketName, path, obsOption.Ak, DecryptObsSk(obsOption.Sk))
		return path
	}
	return path
}

func (f vfs) DecodeRemotePathToLocal(path string) (string, error) {
	return "", nil
}

func SetLogger() {
}
