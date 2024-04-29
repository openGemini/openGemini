//go:build streamfs
// +build streamfs

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

// #cgo CFLAGS: -I./../../c-deps/stream
// #cgo LDFLAGS: -lstdc++ -L/usr/lib64/libstream/lib -lstream
// #include "stdbool.h"
// #include "stdlib.h"
// #include "stream.h"
/*
int streamFileInfoSize() {
	return sizeof(streamFileInfo);
}
*/
import "C"
import (
	"errors"
	"fmt"
	"io"
	"os"
	p "path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

type StreamClient struct {
	fs     C.streamFS
	Logger *zap.Logger
}

type StreamFile struct {
	mu       sync.RWMutex
	fd       C.streamFile
	name     string
	flag     int
	closed   bool
	lockFile string
}

var (
	StreamExists      = 0
	StreamDoesntExist = -1
	ReturnError       = -1

	sc                    *StreamClient
	ErrStreamFileNotExist = os.ErrNotExist
	InvalidPreadParam     = errors.New("invalid param")
	PreadFailErr          = errors.New("pread fail")
	getFileSizeInpPthFail = errors.New("get file size in path fail")
	readLimiter           = limiter.NewFixed(950)
)

const (
	ObsStoragePolicy = "OBS"
	DfvStoragePolicy = "DFV"
)

func GetStreamClient() *StreamClient {
	return sc
}

func NewStreamClient() *StreamClient {
	nn := C.CString("NameSpace-1")
	defer C.free(unsafe.Pointer(nn))

	port := C.ushort(0)

	usr := C.CString("Ruby")
	defer C.free(unsafe.Pointer(usr))

	f := C.streamConnectAsUser(nn, port, usr)
	log := logger.GetLogger()
	sc = &StreamClient{
		fs:     f,
		Logger: log.With(zap.String("service", "streamfs")),
	}

	return sc
}

func (sc *StreamClient) WithLogger(log *zap.Logger) {
	sc.Logger = log.With(zap.String("service", "streamfs"))
}

func (sc *StreamClient) OpenFile(name string, flag int, priority int) (*StreamFile, error) {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))

	if flag == os.O_RDONLY {
		if err := sc.FileExists(name); err != nil {
			return nil, err
		}
	}

	prior := sc.SetStreamIOPriority(priority)
	cflag := C.int(flag) | prior
	if fd := C.streamOpenFile(sc.fs, path, cflag, 0, nil, C.bool(false), 0, 0); fd != nil {
		f := &StreamFile{mu: sync.RWMutex{}, name: name, fd: fd, flag: flag, closed: false}
		return f, nil
	}

	return nil, fmt.Errorf("open file(%s) failed, flag(%d)", name, flag)
}

func (sc *StreamClient) OpenFileV2(name string, flag int, lockFilePath string, priority int) (*StreamFile, error) {
	return sc.OpenFileByStoragePolicy(name, flag, lockFilePath, priority, DfvStoragePolicy)
}

func (sc *StreamClient) OpenFileV3(name string, flag int, lockFilePath string, priority int) (*StreamFile, error) {
	return sc.OpenFileByStoragePolicy(name, flag, lockFilePath, priority, ObsStoragePolicy)
}

func (sc *StreamClient) OpenFileByStoragePolicy(name string, flag int, lockFilePath string, priority int, storagePolicy string) (*StreamFile, error) {
	if flag == os.O_RDONLY {
		if err := sc.FileExists(name); err != nil {
			return nil, err
		}
	}

	cpath := C.CString(name)
	defer C.free(unsafe.Pointer(cpath))

	lockPath := C.CString(lockFilePath)
	defer C.free(unsafe.Pointer(lockPath))

	cpriority := sc.SetStreamIOPriority(priority)
	flags := C.int(flag) | cpriority

	if storagePolicy == DfvStoragePolicy {
		if fd := C.streamOpenFileV2(sc.fs, cpath, flags, 0, nil, C.bool(false), 0, 0, lockPath); fd != nil {
			return &StreamFile{mu: sync.RWMutex{}, name: name, fd: fd, flag: flag, closed: false, lockFile: lockFilePath}, nil
		}
	} else if storagePolicy == ObsStoragePolicy {
		sp := C.CString(storagePolicy)
		defer C.free(unsafe.Pointer(sp))

		if fd := C.streamOpenFileV3(sc.fs, cpath, flags, 0, nil, C.bool(false), 0, 0, lockPath, sp); fd != nil {
			return &StreamFile{mu: sync.RWMutex{}, name: name, fd: fd, flag: flag, closed: false, lockFile: lockFilePath}, nil
		}
	}
	return nil, fmt.Errorf("OpenFileByStoragePolicy(%s) failed, flag(%d) lock(%s) storagePolicy(%s)", name, flag, lockFilePath, storagePolicy)
}

func (sc *StreamClient) CopyFileFromDFVToOBS(srcPath, dstPath, lockPath string) error {
	src := C.CString(srcPath)
	defer C.free(unsafe.Pointer(src))

	dst := C.CString(dstPath)
	defer C.free(unsafe.Pointer(dst))

	storagePolicyName := C.CString(ObsStoragePolicy)
	defer C.free(unsafe.Pointer(storagePolicyName))

	lock := C.CString(lockPath)
	defer C.free(unsafe.Pointer(lock))

	if res := C.streamCopyDfvObs(sc.fs, src, dst, storagePolicyName, lock, C.bool(true)); res != 0 {
		return fmt.Errorf("copy file from dfv %s to obs %s (lock: %s) err %d", srcPath, dstPath, lockPath, int(res))
	}
	return nil
}

func (sc *StreamClient) closeFile(file *StreamFile) error {
	if res := C.streamCloseFile(sc.fs, file.fd); res != 0 {
		return fmt.Errorf("close file(%s) failed, flag(%d)", file.Name(), int(res))
	}
	file.closed = true
	file.flag = 0

	return nil
}

func (sc *StreamClient) closeFileV2(file *StreamFile, lockFilePath string) error {
	lockFile := C.CString(lockFilePath)
	defer C.free(unsafe.Pointer(lockFile))

	if res := C.streamCloseFileV2(sc.fs, file.fd, lockFile); res != 0 {
		return fmt.Errorf("close fileV2(%s) failed, lock file(%s), res(%d), err: %s", file.Name(), lockFilePath, int(res), getStreamErrorString())
	}
	file.closed = true
	file.flag = 0

	return nil
}

func (sc *StreamClient) Read(file *StreamFile, p []byte, size int) (int, error) {
	select {
	case readLimiter <- struct{}{}:
		defer readLimiter.Release()
	}

	ptr := unsafe.Pointer(&p[0])

	remainingBytes := size
	bytesRead := 0
	totalBytesRead := 0
	var readError error = nil

	for 0 < remainingBytes {
		bytesRead = int(C.streamRead(sc.fs, file.fd, ptr, C.int(remainingBytes)))
		if 0 > bytesRead {
			if eof, _ := sc.StreamReachTheEnd(file); eof {
				readError = io.EOF
			} else {
				sc.Logger.Error(fmt.Sprintf("Read stream faild: %s", getStreamErrorString()))
				readError = errors.New("read failed")
			}
			break
		} else if 0 == bytesRead {
			readError = io.EOF
			break
		} else if bytesRead > remainingBytes {
			return 0, errors.New(fmt.Sprintf("read failed with bytes_read %d > remaining_bytes %d ", bytesRead, remainingBytes))
		}

		totalBytesRead += bytesRead
		remainingBytes -= bytesRead
		if remainingBytes == 0 {
			break
		}
		ptr = unsafe.Pointer(&p[totalBytesRead])

		if totalBytesRead > size {
			return 0, fmt.Errorf("read file(%s) faild: total_bytes_read(%d) > size(%d)", file.Name(), totalBytesRead, size)
		}
	}

	return totalBytesRead, readError
}

func (sc *StreamClient) Append(file *StreamFile, data []byte) (int, error) {
	wLen := len(data)

	res := C.streamWrite(sc.fs, file.fd, unsafe.Pointer(&data[0]), C.int(wLen))
	if wLen != int(res) {
		err := fmt.Errorf("write file(%s) faild: data leng(%d) != write size(%d)", file.Name(), wLen, int(res))
		sc.Logger.Error("write fail", zap.Error(err))
		panic(err)
	}

	return int(res), nil
}

func (sc *StreamClient) Seek(file *StreamFile, offset int) int {
	coffset := C.int(offset)
	cres := C.streamSeek(sc.fs, file.fd, C.long(coffset))
	if int(cres) < 0 {
		return -1
	}
	return int(cres)
}

func (sc *StreamClient) getFileSize(name string) (int64, error) {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))

	pFileInfo := C.streamGetPathInfo(sc.fs, path)
	if nil != pFileInfo {
		size := pFileInfo.mSize
		C.streamFreeFileInfo(pFileInfo, 1)
		return int64(size), nil
	}

	return 0, fmt.Errorf("get file(%s) size failed", name)
}

func (sc *StreamClient) pread(file *StreamFile, p []byte, offset int, size int) (int, error) {
	if len(p) == 0 || len(p) < size || size <= 0 {
		sc.Logger.Error("invalid pread param", zap.String("file", file.Name()), zap.Int("bufLen", len(p)), zap.Int("offset", offset), zap.Int("size", size))
		return 0, InvalidPreadParam
	}

	select {
	case readLimiter <- struct{}{}:
		defer readLimiter.Release()
	}

	ptr := unsafe.Pointer(&p[0])
	bytesRead := 0
	bytesRead = (int)(C.streamPread(sc.fs, file.fd, ptr, C.long(offset), C.int(size)))
	if 0 > bytesRead {
		fileSize, err := sc.getFileSize(file.name)
		if err != nil {
			sc.Logger.Error("get file size failed", zap.String("file", file.name), zap.Error(err), zap.Int64("fileSize", fileSize))
			return 0, PreadFailErr
		}

		if int(fileSize) <= offset {
			sc.Logger.Warn("pread file, offset reach end of file", zap.String("file", file.name), zap.Int("offset", offset), zap.Int64("fileSize", fileSize))
			return 0, io.EOF
		}
		errStr := getStreamErrorString()
		sc.Logger.Error("pread fail", zap.String("file", file.name), zap.Int("offset", offset),
			zap.Int("size", len(p)), zap.String("stream err", errStr), zap.Int64("fileSize", fileSize))
		return 0, PreadFailErr
	}

	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}

	return int(bytesRead), nil
}

func (sc *StreamClient) readAllBytes(name string, priority int) ([]byte, error) {
	fd, err := sc.OpenFile(name, os.O_RDONLY, priority)
	if err != nil {
		sc.Logger.Error("open file fail", zap.String("file", name), zap.Error(err))
		return nil, err
	}
	defer sc.closeFile(fd)

	size, err := sc.getFileSize(name)
	if err != nil {
		return nil, err
	}

	data := make([]byte, int(size))
	_, err = sc.Read(fd, data, (int)(size))
	if (err != nil) && err != io.EOF {
		return nil, err
	}

	return data, err
}

func (sc *StreamClient) ReadAllByOffset(name string, offset int, priority int) ([]byte, error) {
	fd, err := sc.OpenFile(name, os.O_RDONLY, priority)
	if err != nil {
		return nil, err
	}
	defer sc.closeFile(fd)

	size, err := sc.getFileSize(name)
	if err != nil {
		return nil, err
	}

	if size == 0 {
		return nil, io.EOF
	}

	if offset > int(size) {
		sc.Logger.Error("invalid read param:offset bigger than file size", zap.Int("offset", offset), zap.Int64("size", size))
		return nil, errors.New("read offset bigger than file size")
	}
	readSize := int(size) - offset
	data := make([]byte, readSize)

	_, err = sc.pread(fd, data, offset, readSize)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (sc *StreamClient) ReadAll(name string, p []byte, priority int) error {
	fd, err := sc.OpenFile(name, os.O_RDONLY, priority)
	if err != nil {
		return err
	}
	defer sc.closeFile(fd)

	size, err := sc.getFileSize(name)
	if err != nil {
		return err
	}
	rSize := int(size)
	if len(p) < rSize {
		rSize = len(p)
	}
	_, err = sc.Read(fd, p, rSize)
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (sc *StreamClient) FileExists(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("file name is nil")
	}

	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))

	switch value := C.streamExists(sc.fs, path); value {
	case C.int(StreamExists):
		return nil
	case C.int(StreamDoesntExist):
		return ErrStreamFileNotExist
	default: // anything else should be an error
		return fmt.Errorf("check file(%s) exist failed,error number:%d", name, int(value))
	}
}

func (sc *StreamClient) RenameFile(src string, target string) error {
	sc.Logger.Info("[remove file] Rename File ", zap.String("src", src), zap.String("target", target))
	srcPath := C.CString(src)
	defer C.free(unsafe.Pointer(srcPath))

	targetPath := C.CString(target)
	defer C.free(unsafe.Pointer(targetPath))

	res := C.streamRename(sc.fs, srcPath, targetPath, C.bool(true))
	if 0 == int(res) {
		return nil
	}
	msg := fmt.Sprintf("rename file(%s) to(%s) fail", src, target)
	sc.Logger.Error(msg)
	return fmt.Errorf(msg)
}

func (sc *StreamClient) RenameFileV2(src string, target string, LockFilePath string) error {
	sc.Logger.Info("[remove file] RenameFileV2 File ", zap.String("src", src), zap.String("target", target), zap.String("LockFilePath", LockFilePath))
	srcPath := C.CString(src)
	defer C.free(unsafe.Pointer(srcPath))

	targetPath := C.CString(target)
	defer C.free(unsafe.Pointer(targetPath))

	lockFile := C.CString(LockFilePath)
	defer C.free(unsafe.Pointer(lockFile))

	res := C.streamRenameV2(sc.fs, srcPath, targetPath, C.bool(true), lockFile)
	if 0 == int(res) {
		return nil
	}
	msg := fmt.Sprintf("rename fileV2(%s) to(%s) fail, lock file(%s), res(%d), err: %s", src, target, LockFilePath, int(res), getStreamErrorString())
	sc.Logger.Error(msg)
	return fmt.Errorf(msg)
}

func (sc *StreamClient) CreateFile(name string, priority int) (*StreamFile, error) {
	flags := os.O_WRONLY

	fd, err := sc.OpenFile(name, flags, priority)
	if fd == nil {
		sc.Logger.Error("open failed ", zap.Error(err))
		return nil, err
	}

	return fd, nil
}

func (sc *StreamClient) CreateFileV2(name string, lockFilePath string, priority int) (*StreamFile, error) {
	flags := os.O_WRONLY
	if err := sc.FileExists(name); err == nil {
		flags = os.O_RDWR
	} else if err == ErrStreamFileNotExist {
		flags = os.O_CREATE
	} else {
		return nil, fmt.Errorf("file(%s) exist with create flag(%d)", name, flags)
	}

	fd, err := sc.OpenFileV2(name, flags, lockFilePath, priority)
	if fd == nil {
		sc.Logger.Error("open fileV2 failed ", zap.Error(err))
		return nil, err
	}

	return fd, nil
}

func (sc *StreamClient) CreateOBSFile(name string, lockFilePath string, priority int) (*StreamFile, error) {
	flags := os.O_WRONLY
	if err := sc.FileExists(name); err == nil {
		flags = os.O_RDWR
	} else if err == ErrStreamFileNotExist {
		flags = os.O_CREATE
	} else {
		return nil, fmt.Errorf("file(%s) exist with create flag(%d): %s", name, flags, err.Error())
	}

	fd, err := sc.OpenFileV3(name, flags, lockFilePath, priority)
	if err != nil {
		sc.Logger.Error("CreateOBS failed ", zap.Error(err))
		return nil, err
	}
	return fd, nil
}

func (sc *StreamClient) CreateDir(name string) error {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))

	if res := C.streamCreateDirectory(sc.fs, path); 0 == int(res) {
		return nil
	}
	sc.Logger.Error("CreateDir failed", zap.String("name", name))
	return fmt.Errorf("create dir(%s) failed", name)
}

func (sc *StreamClient) CreateDirV2(name string, lockFilePath string) error {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))
	lockFile := C.CString(lockFilePath)
	defer C.free(unsafe.Pointer(lockFile))

	res := C.streamCreateDirectoryV2(sc.fs, path, lockFile)
	if 0 == int(res) {
		return nil
	}
	sc.Logger.Error("CreateDirV2 failed", zap.String("name", name), zap.String("lockFilePath", lockFilePath), zap.Int("res", int(res)))
	return fmt.Errorf("create dirV2(%s) failed, lock file(%s), res(%d), err: %s", name, lockFilePath, int(res), getStreamErrorString())
}

func (sc *StreamClient) fsync(file *StreamFile, trueSync bool) error {
	value := C.streamSync(sc.fs, file.fd, C.bool(trueSync))
	if int(value) == ReturnError {
		err := fmt.Errorf("fsync file(%s) failed, true sync: %v", file.Name(), trueSync)
		sc.Logger.Error("sync fail", zap.Error(err))
		panic(err)
		return err
	}

	return nil
}

func (sc *StreamClient) fsyncV2(file *StreamFile, trueSync bool, lockFilePath string) error {
	lockFile := C.CString(lockFilePath)
	defer C.free(unsafe.Pointer(lockFile))

	value := C.streamSyncV2(sc.fs, file.fd, C.bool(trueSync), lockFile)
	if int(value) == ReturnError {
		err := fmt.Errorf("fsyncV2 file(%s) failed, true sync: %v, lock file: %s, err: %s",
			file.Name(), trueSync, lockFilePath, getStreamErrorString())
		sc.Logger.Error("syncV2 fail", zap.Error(err))
		panic(err)
		return err
	}

	return nil
}

// Glob only support pattern: "/usr2/influxdb-sqc/wal/db0/autogen/1/_*.wal"
func (sc *StreamClient) Glob(pattern string) ([]string, error) {
	dir, filePattern := filepath.Split(pattern)
	dir = dir[:len(dir)-1]
	err := sc.FileExists(dir)
	if err != nil {
		return nil, err
	}

	cPath := C.CString(dir)
	defer C.free(unsafe.Pointer(cPath))

	var entry C.int
	dirPtr := C.streamListDirectory(sc.fs, cPath, &entry)
	if dirPtr == nil {
		sc.Logger.Error("Glob.streamListDirectory failed", zap.String("pattern", pattern))
		return nil, fmt.Errorf("Glob(%s).streamListDirectory failed", pattern)
	}
	defer C.streamFreeFileInfo(dirPtr, entry)

	reg, err := regexp.Compile(filePattern)
	if err != nil {
		return nil, err
	}

	var matches []string
	eSize := int(C.streamFileInfoSize())
	eptr := uintptr(unsafe.Pointer(dirPtr))
	for i := 0; i < int(entry); i++ {
		infoSlice := (*C.streamFileInfo)(unsafe.Pointer(eptr))
		if infoSlice.mKind == C.kObjectKindFile {
			fName := C.GoString(infoSlice.mName)
			if find := reg.FindString(fName); find != "" {
				matches = append(matches, fName)
			}
		}
		eptr += uintptr(eSize)
	}
	sort.Strings(matches)

	return matches, nil
}

func (sc *StreamClient) AllFilesSizeInPath(path string) (int64, int64, int64, error) {
	_, err := sc.Stat(path)
	if err != nil && err == ErrStreamFileNotExist {
		return 0, 0, 0, nil
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	pInfo := C.streamGetContentSummary(sc.fs, cPath)
	if pInfo == nil {
		return 0, 0, 0, getFileSizeInpPthFail
	}

	totalSize := int64(pInfo.length)
	dfvSize := int64(pInfo.dfvLength)
	obsSize := int64(pInfo.obsLength)
	C.streamFreeContentSummary(pInfo)

	return totalSize, dfvSize, obsSize, nil
}

func (sc *StreamClient) RecoverLease(name string) error {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))

	res := C.streamRecoverLease(sc.fs, path)
	if 0 != int(res) {
		return fmt.Errorf("recover lease failed, file(%s)", name)
	}

	return nil
}

func (sc *StreamClient) SealFileV2(name string, lockFilePath string) error {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))
	lockFile := C.CString(lockFilePath)
	defer C.free(unsafe.Pointer(lockFile))

	res := C.streamSealFileV2(sc.fs, path, lockFile)
	if 0 != int(res) {
		return fmt.Errorf("seal fileV2(%s) failed, lock file(%s)", name, lockFilePath)
	}

	return nil
}

type StreamFileInfo struct {
	isFile  bool
	name    string
	modTime time.Time
	size    int64
	perm    uint32
}

func (si *StreamFileInfo) Name() string {
	return si.name
}

func (si *StreamFileInfo) Size() int64 {
	return si.size
}

func (si *StreamFileInfo) ModTime() time.Time {
	return si.modTime
}

func (si *StreamFileInfo) IsDir() bool {
	return !si.isFile
}

func (si *StreamFileInfo) Mode() os.FileMode {
	return os.FileMode(si.perm)
}

func (si *StreamFileInfo) Sys() interface{} {
	return nil
}

func (sc *StreamClient) IsObsFile(path string) (bool, error) {
	err := sc.FileExists(path)
	if err != nil {
		return false, err
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var ret C.int
	ret = C.streamCheckObsType(sc.fs, cPath)
	if ret == -1 {
		return false, fmt.Errorf("stream check obs type error")
	}

	return ret == 1, nil
}

func (sc *StreamClient) Stat(path string) (os.FileInfo, error) {
	var info *C.streamFileInfo
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	err := sc.FileExists(path)
	if err != nil {
		return nil, err
	}

	info = C.streamGetPathInfo(sc.fs, cPath)
	if info == nil {
		return nil, fmt.Errorf("stat(%s) failed", path)
	}
	defer C.streamFreeFileInfo(info, C.int(1))

	sinfo := &StreamFileInfo{
		isFile:  info.mKind == C.kObjectKindFile,
		name:    C.GoString(info.mName),
		size:    int64(info.mSize),
		modTime: time.Unix(0, int64(info.mLastMod)*int64(time.Millisecond)),
		perm:    uint32(info.mPermissions),
	}

	return sinfo, nil
}

func (sc *StreamClient) Readdir(path string) ([]os.FileInfo, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	err := sc.FileExists(path)
	if err != nil {
		return nil, err
	}

	var entry C.int
	dirPtr := C.streamListDirectory(sc.fs, cPath, &entry)
	if dirPtr == nil {
		msg := fmt.Sprintf("Readdir.streamListDirectory(%s) failed", path)
		sc.Logger.Error(msg)
		return nil, errors.New(msg)
	}
	defer C.streamFreeFileInfo(dirPtr, entry)

	var fInfo []os.FileInfo
	infoSlice := (*[1 << 20]C.streamFileInfo)(unsafe.Pointer(dirPtr))[:entry:entry]
	for _, info := range infoSlice {
		_, fileName := filepath.Split(C.GoString(info.mName))
		info := &StreamFileInfo{
			isFile:  info.mKind == C.kObjectKindFile,
			name:    fileName,
			size:    int64(info.mSize),
			modTime: time.Unix(0, int64(info.mLastMod)*int64(time.Millisecond)),
			perm:    uint32(info.mPermissions),
		}

		fInfo = append(fInfo, info)
	}
	sort.Slice(fInfo, func(i, j int) bool { return fInfo[i].Name() < fInfo[j].Name() })

	return fInfo, nil
}

func (sc *StreamClient) WriteFile(filename string, data []byte, priority int) error {
	_ = sc.Remove(filename)
	f, err := sc.CreateFile(filename, priority)
	if err != nil {
		sc.Logger.Error("CreateFile failed: ", zap.String("filename", filename))
		return err
	}

	n, err := sc.Append(f, data)
	if err == nil && n < len(data) {
		sc.Logger.Error("write file(%s) failed", zap.String("file", filename), zap.Int("data len", len(data)), zap.Error(err))
		err = io.ErrShortWrite
	}

	err = sc.closeFile(f)

	return err
}

func (sc *StreamClient) WriteFileV2(filename string, data []byte, lockFilePath string, priority int) error {
	_ = sc.RemoveV2(filename, lockFilePath)
	f, err := sc.CreateFileV2(filename, lockFilePath, priority)
	if err != nil {
		sc.Logger.Error("CreateFileV2 failed: ", zap.String("filename", filename), zap.String("lockfile", lockFilePath))
		return err
	}

	n, err := sc.Append(f, data)
	if err == nil && n < len(data) {
		sc.Logger.Error("write file(%s) failed", zap.String("file", filename), zap.Int("data len", len(data)), zap.Error(err))
		err = io.ErrShortWrite
	}

	err = sc.closeFileV2(f, lockFilePath)

	return err
}

func (sc *StreamClient) WriteFileV3(filename string, data []byte, lockFilePath string, priority int) error {
	_ = sc.RemoveV2(filename, lockFilePath)
	f, err := sc.CreateOBSFile(filename, lockFilePath, priority)
	if err != nil {
		sc.Logger.Error("CreateFileV2 failed: ", zap.String("filename", filename), zap.String("lockfile", lockFilePath))
		return err
	}

	n, err := sc.Append(f, data)
	if err == nil && n < len(data) {
		sc.Logger.Error("write file(%s) failed", zap.String("file", filename), zap.Int("data len", len(data)), zap.Error(err))
		err = io.ErrShortWrite
	}

	err = sc.closeFileV2(f, lockFilePath)

	return err
}

func (sc *StreamClient) Remove(path string) error {
	sc.Logger.Info("[remove file] Remove File ", zap.String("path", path))
	if len(path) == 0 {
		return fmt.Errorf("path is nil")
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if value := C.streamDelete(sc.fs, cPath, 0); int(value) == ReturnError {
		return fmt.Errorf("remove file(%s) failed", path)
	}

	return nil
}

func (sc *StreamClient) RemoveV2(path string, lockFilePath string) error {
	if len(path) == 0 {
		return fmt.Errorf("path is nil")
	}
	sc.Logger.Info("[remove file] RemoveV2 File ", zap.String("path", path), zap.String("lockFilePath", lockFilePath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	lockFile := C.CString(lockFilePath)
	defer C.free(unsafe.Pointer(lockFile))

	if value := C.streamDeleteV2(sc.fs, cPath, 0, lockFile); int(value) == ReturnError {
		return fmt.Errorf("remove fileV2(%s) failed, lock file(%s)", path, lockFilePath)
	}

	return nil
}

func (sc *StreamClient) RemoveAll(path string) error {
	sc.Logger.Info("[remove file] RemoveAll File ", zap.String("path", path))
	if len(path) == 0 {
		return fmt.Errorf("path is nil")
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	err := sc.FileExists(path)
	if err == ErrStreamFileNotExist {
		return nil
	}

	info, err := sc.Stat(path)
	if err != nil {
		return err
	}

	var recursive C.int
	if info.IsDir() {
		recursive = 1
	}

	if value := C.streamDelete(sc.fs, cPath, recursive); int(value) == ReturnError {
		sc.Logger.Error("delete file failed: ", zap.String("file", path), zap.String("return", strconv.Itoa(int(value))))
		return fmt.Errorf("remove all file in dir(%s) failed", path)
	}

	return nil
}

func (sc *StreamClient) RemoveAllV2(path string, lockFilePath string) error {
	if len(path) == 0 {
		return fmt.Errorf("path is nil")
	}

	sc.Logger.Info("[remove file] RemoveAllV2 File ", zap.String("path", path), zap.String("lockFilePath", lockFilePath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	lockFile := C.CString(lockFilePath)
	defer C.free(unsafe.Pointer(lockFile))

	err := sc.FileExists(path)
	if err == ErrStreamFileNotExist {
		return nil
	}

	info, err := sc.Stat(path)
	if err != nil {
		return err
	}

	var recursive C.int
	if info.IsDir() {
		recursive = 1
	}

	if value := C.streamDeleteV2(sc.fs, cPath, recursive, lockFile); int(value) == ReturnError {
		sc.Logger.Error("delete fileV2 failed: ", zap.String("file", path), zap.String("return", strconv.Itoa(int(value))),
			zap.String("lock file", lockFilePath))
		return fmt.Errorf("remove all fileV2 in dir(%s) failed, lock file(%s)", path, lockFilePath)
	}

	return nil
}

func (sc *StreamClient) StreamReachTheEnd(file *StreamFile) (bool, error) {
	size, err := sc.getFileSize(file.Name())
	if err != nil {
		return false, err
	}

	pos := C.streamTell(sc.fs, file.fd)
	if int(pos) == -1 {
		return false, fmt.Errorf("get file(%s) pos faild", file.Name())
	}

	return uint32(size) == uint32(pos), nil
}

func (sc *StreamClient) SetStreamIOPriority(flag int) C.int {
	if flag == IO_PRIORITY_ULTRA_HIGH {
		return C.STREAM_IO_PRIORITY_ULTRA_HIGH
	} else if flag == IO_PRIORITY_HIGH {
		return C.STREAM_IO_PRIORITY_HIGH
	} else if flag == IO_PRIORITY_NORMAL {
		return C.STREAM_IO_PRIORITY_NORMAL
	} else if flag == IO_PRIORITY_LOW {
		return C.STREAM_IO_PRIORITY_LOW
	}
	return C.STREAM_IO_PRIORITY_LOW
}

func (sc *StreamClient) Truncate(fileName string, size int64, lock string, shouldWait bool) error {
	cPath := C.CString(fileName)
	lockPath := C.CString(lock)
	defer func() {
		C.free(unsafe.Pointer(cPath))
		C.free(unsafe.Pointer(lockPath))
	}()

	var wait C.int
	if shouldWait {
		wait = 1
	}

	ret := C.streamTruncateV2(sc.fs, cPath, C.tOffset(size), &wait, lockPath)
	if int(ret) == ReturnError {
		err := fmt.Errorf("truncat(%v) to %v fail", fileName, size)
		sc.Logger.Error("truncat file fail", zap.Error(err))
		return err
	}

	return nil
}

func (f *StreamFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, fmt.Errorf("file closed")
	}
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOWriteTotalCount, 1)
	atomic.AddInt64(&statistics.IOStat.IOWriteTotalBytes, int64(len(p)))
	res, err := GetStreamClient().Append(f, p)
	opsStatEnd(begin.UnixNano(), opsTypeWrite, int64(res))
	return res, err
}

func (f *StreamFile) WriteLimitIoSize(p []byte, ioLimitSize int) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, fmt.Errorf("file closed")
	}

	if len(p) <= ioLimitSize {
		n, err = GetStreamClient().Append(f, p)
	} else {
		wLen := len(p)
		wSize := 0

		for wSize < wLen && (err == nil) {
			var w int
			buf := p[wSize:]
			if len(buf) > ioLimitSize {
				buf = buf[:ioLimitSize]
			}
			w, err = GetStreamClient().Append(f, buf)
			wSize += w
			n += w
		}
	}

	return n, err
}

func (f *StreamFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return fmt.Errorf("file closed")
	}
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOSyncTotalCount, 1)
	err := GetStreamClient().fsyncV2(f, false, f.lockFile)
	opsStatEnd(begin.UnixNano(), opsTypeSync, int64(0))
	return err
}

func (f *StreamFile) SyncTrue() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return fmt.Errorf("file closed")
	}
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOSyncTotalCount, 1)
	err := GetStreamClient().fsync(f, true)
	opsStatEnd(begin.UnixNano(), opsTypeSync, int64(0))
	return err
}

func (f *StreamFile) StreamReadBatch(offs []int64, sizes []int64, minBlockSize int64, c chan *request.StreamReader,
	obsRangeSize int, isStat bool) {
}

func (f *StreamFile) SyncUpdateLength() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return fmt.Errorf("file closed")
	}
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOSyncTotalCount, 1)
	err := GetStreamClient().fsyncV2(f, true, f.lockFile)
	opsStatEnd(begin.UnixNano(), opsTypeSync, int64(0))

	return err
}

func (f *StreamFile) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return fmt.Errorf("file closed")
	}
	return GetStreamClient().fsyncV2(f, false, f.lockFile)
}

func (f *StreamFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return fmt.Errorf("file closed")
	}

	return GetStreamClient().closeFileV2(f, f.lockFile)
}

func (f *StreamFile) RecoverLease() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return GetStreamClient().RecoverLease(f.lockFile)
}

func (f *StreamFile) Read(buf []byte) (int, error) {
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOReadTotalCount, 1)
	atomic.AddInt64(&statistics.IOStat.IOReadTotalBytes, int64(len(buf)))
	f.mu.RLock()
	n, err := GetStreamClient().Read(f, buf, len(buf))
	f.mu.RUnlock()
	opsStatEnd(begin.UnixNano(), opsTypeRead, int64(n))
	return n, err
}

func (f *StreamFile) Stat() (os.FileInfo, error) {
	return GetStreamClient().Stat(f.name)
}

func (f *StreamFile) Name() string {
	return f.name
}

func (f *StreamFile) LockFile() string {
	return f.lockFile
}

func (f *StreamFile) Seek(offset int64, whence int) (ret int64, err error) {
	n := offset
	if f.flag == os.O_RDONLY {
		n = int64(GetStreamClient().Seek(f, int(offset)))
		if n < 0 {
			return n, fmt.Errorf("file(%s) seek(%d, %d) faild", f.Name(), uint32(offset), whence)
		}
	}
	return n, nil
}

func (f *StreamFile) Truncate(size int64) error {
	return GetStreamClient().Truncate(f.name, size, f.lockFile, true)
}

func (f *StreamFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return 0, fmt.Errorf("file closed")
	}
	if len(p) == 0 {
		return 0, fmt.Errorf("ReadAt(%s, buf, %d)target buffer size is 0", f.Name(), off)
	}

	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOReadTotalCount, 1)
	atomic.AddInt64(&statistics.IOStat.IOReadTotalBytes, int64(len(p)))

	n, err := GetStreamClient().pread(f, p, int(off), len(p))
	if err != nil && err != io.EOF {
		return 0, err
	}
	opsStatEnd(begin.UnixNano(), opsTypeRead, int64(n))

	return n, err
}

func (f *StreamFile) ReadAtLimitIoSize(p []byte, off int64, ioSizeLimit int) (int, error) {
	if len(p) == 0 {
		return 0, fmt.Errorf("ReadAt(%s, buf, %d)target buffer size is 0", f.Name(), off)
	}

	if len(p) <= ioSizeLimit {
		return f.ReadAt(p, off)
	}

	var rErr error
	rLen := len(p)
	readSize := 0
	rOff := int(off)

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return 0, fmt.Errorf("file closed")
	}
	for readSize < rLen && (rErr == nil) {
		var n int
		buf := p[readSize:]
		if len(buf) > ioSizeLimit {
			buf = buf[:ioSizeLimit]
		}
		n, rErr = GetStreamClient().pread(f, buf, rOff, len(buf))
		if rErr != nil {
			if rErr != io.EOF {
				break
			}
		}
		readSize += n
		rOff += n
	}

	return readSize, rErr
}

func (f *StreamFile) Fsync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fmt.Errorf("file closed")
	}
	begin := time.Now()
	atomic.AddInt64(&statistics.IOStat.IOSyncTotalCount, 1)
	err := GetStreamClient().fsyncV2(f, true, f.lockFile)
	opsStatEnd(begin.UnixNano(), opsTypeSync, int64(0))
	return err
}

func (f *StreamFile) Size() (int64, error) {
	return GetStreamClient().getFileSize(f.Name())
}

func (f *StreamFile) Readdir(path string) ([]os.FileInfo, error) {
	return GetStreamClient().Readdir(path)
}

func (f *StreamFile) Fd() uintptr {
	return 0
}

func getStreamErrorString() string {
	str := C.streamGetLastError()
	if str != nil {
		return C.GoString(str)
	}

	return ""
}

type streamVfs struct {
	sc *StreamClient
}

func NewFS() VFS {
	return &streamVfs{
		sc: NewStreamClient(),
	}
}

func lockAndPriorityOpt(lock *string, priority *int, opts ...FSOption) error {
	for _, opt := range opts {
		switch t := opt.(type) {
		case FileLockOption:
			*lock = t.Parameter().(string)
		case FilePriorityOption:
			*priority = t.Parameter().(int)
		default:
			return fmt.Errorf("unknow open parameter opt:%v", t)
		}
	}
	return nil
}

func priorityOpt(priority *int, opts ...FSOption) error {
	for _, opt := range opts {
		switch t := opt.(type) {
		case FilePriorityOption:
			*priority = t.Parameter().(int)
		case FileLockOption:
		default:
			return fmt.Errorf("unknow open parameter opt:%v", t)
		}
	}
	return nil
}

func lockOpt(lock *string, opts ...FSOption) error {
	for _, opt := range opts {
		switch t := opt.(type) {
		case FileLockOption:
			*lock = t.Parameter().(string)
		case FilePriorityOption:
		default:
			return fmt.Errorf("unknow open parameter opt:%v", t)
		}
	}
	return nil
}

func (fs *streamVfs) Open(name string, opts ...FSOption) (File, error) {
	var lock string
	var priority int
	if err := lockAndPriorityOpt(&lock, &priority, opts...); err != nil {
		return nil, err
	}

	f, err := fs.sc.OpenFileV2(name, os.O_RDONLY, lock, priority)
	return f, err
}

func (fs *streamVfs) OpenFile(name string, flag int, _ os.FileMode, opts ...FSOption) (File, error) {
	var lock string
	var priority int
	if err := lockAndPriorityOpt(&lock, &priority, opts...); err != nil {
		return nil, err
	}

	var flags int
	if (flag&os.O_RDWR == os.O_RDWR) || (flag&os.O_CREATE == os.O_CREATE) || (flag&os.O_WRONLY == os.O_WRONLY) {
		flags = os.O_WRONLY
	} else if flag&os.O_RDONLY == os.O_RDONLY {
		flags = os.O_RDONLY
	} else {
		f := flag
		panic(fmt.Sprintf("invald open flag: %d", f))
	}

	f, err := fs.sc.OpenFileV2(name, flags, lock, priority)
	return f, err
}

func (fs *streamVfs) Create(name string, opts ...FSOption) (File, error) {
	var lock string
	var priority int
	if err := lockAndPriorityOpt(&lock, &priority, opts...); err != nil {
		return nil, err
	}
	f, err := fs.sc.CreateFileV2(name, lock, priority)
	return f, err
}

func (fs *streamVfs) CreateV1(name string, opt ...FSOption) (File, error) {
	var priority int
	if err := priorityOpt(&priority, opt...); err != nil {
		return nil, err
	}
	f, err := fs.sc.CreateFile(name, priority)
	return f, err
}

func (fs *streamVfs) Remove(name string, opts ...FSOption) error {
	var lock string
	if err := lockOpt(&lock, opts...); err != nil {
		return err
	}
	return fs.sc.RemoveV2(name, lock)
}

func (fs *streamVfs) RemoveLocal(path string, _ ...FSOption) error {
	return nil
}

func (fs *streamVfs) RemoveAll(path string, opts ...FSOption) error {
	var lock string
	if err := lockOpt(&lock, opts...); err != nil {
		return err
	}
	logger.GetLogger().Info("remove path", zap.String("path", path))
	return fs.sc.RemoveAllV2(path, lock)
}

func (fs *streamVfs) Mkdir(path string, perm os.FileMode, opts ...FSOption) error {
	parentDir := p.Dir(path)
	if err := fs.sc.FileExists(parentDir); err != nil {
		return err
	}
	return fs.MkdirAll(path, perm)
}

func (fs *streamVfs) MkdirAll(path string, _ os.FileMode, opts ...FSOption) error {
	var lock string
	if err := lockOpt(&lock, opts...); err != nil {
		return err
	}
	if lock != "" {
		return fs.sc.CreateDirV2(path, lock)
	}
	return fs.sc.CreateDir(path)
}

func (fs *streamVfs) ReadDir(dirname string) ([]os.FileInfo, error) {
	return fs.sc.Readdir(dirname)
}

func (fs *streamVfs) Glob(pattern string) ([]string, error) {
	var path string
	dir, filePattern := filepath.Split(pattern)
	if strings.HasPrefix(filePattern, "*.") {
		path = fmt.Sprintf("%s\\%s$", dir, filePattern[1:])
	} else {
		path = pattern + "$"
	}
	return fs.sc.Glob(path)
}

func (fs *streamVfs) RenameFile(oldPath, newPath string, opts ...FSOption) error {
	var lock string
	if err := lockOpt(&lock, opts...); err != nil {
		return err
	}
	return fs.sc.RenameFileV2(oldPath, newPath, lock)
}

func (fs *streamVfs) Stat(name string) (os.FileInfo, error) {
	return fs.sc.Stat(name)
}

func (fs *streamVfs) WriteFile(filename string, data []byte, _ os.FileMode, opts ...FSOption) error {
	var lock string
	var priority int
	if err := lockAndPriorityOpt(&lock, &priority, opts...); err != nil {
		return err
	}
	return fs.sc.WriteFileV3(filename, data, lock, priority)
}

func (fs *streamVfs) ReadFile(filename string, opts ...FSOption) ([]byte, error) {
	var priority int
	if err := priorityOpt(&priority, opts...); err != nil {
		return nil, err
	}
	return fs.sc.ReadAllByOffset(filename, 0, priority)
}

func (fs *streamVfs) CreateTime(name string) (*time.Time, error) {
	fi, err := fs.sc.Stat(name)
	if err != nil {
		return nil, err
	}

	tm := fi.ModTime()
	return &tm, nil
}

func (fs *streamVfs) Truncate(name string, size int64, opts ...FSOption) error {
	var lock string
	if err := lockOpt(&lock, opts...); err != nil {
		return err
	}

	return fs.sc.Truncate(name, size, lock, true)
}

func (fs *streamVfs) CopyFile(srcFile, dstFile string, opts ...FSOption) (written int64, err error) {
	srcFd, err := fs.Open(srcFile, opts...)
	if err != nil {
		return 0, err
	}
	defer srcFd.Close()

	dstFd, err := fs.Create(dstFile, opts...)
	if err != nil {
		return 0, err
	}
	defer dstFd.Close()

	return io.Copy(dstFd, srcFd)
}

func (fs *streamVfs) IsObsFile(path string) (bool, error) {
	return fs.sc.IsObsFile(path)
}

func (fs *streamVfs) CopyFileFromDFVToOBS(srcPath, dstPath string, opts ...FSOption) error {
	var lock string
	if err := lockOpt(&lock, opts...); err != nil {
		return err
	}
	return fs.sc.CopyFileFromDFVToOBS(srcPath, dstPath, lock)
}

func (fs *streamVfs) CreateV2(name string, opts ...FSOption) (File, error) {
	var lock string
	var priority int
	if err := lockAndPriorityOpt(&lock, &priority, opts...); err != nil {
		return nil, err
	}
	return fs.sc.CreateOBSFile(name, lock, priority)
}

func (fs *streamVfs) GetAllFilesSizeInPath(path string) (int64, int64, int64, error) {
	return fs.sc.AllFilesSizeInPath(path)
}

func (fs *streamVfs) GetOBSTmpFileName(path string, obsOption *obs.ObsOptions) string {
	return path + obs.ObsFileTmpSuffix
}

func (fs *streamVfs) DecodeRemotePathToLocal(path string) (string, error) {
	return "", nil
}

func Mmap(fd int, offset int64, length int) (data []byte, err error) {
	return
}

func MUnmap(data []byte) error {
	return nil
}

func Fadvise(fd int, offset int64, length int64, advice int) (err error) {
	return
}

func Fdatasync(file File) (err error) {
	return file.Sync()
}

func RecoverLease(lock string) error {
	return GetStreamClient().RecoverLease(lock)
}

func SealFile(path, lock string) error {
	return GetStreamClient().SealFileV2(path, lock)
}
