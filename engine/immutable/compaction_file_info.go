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

package immutable

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type CompactedFileInfo struct {
	Name    string // measurement name with version
	IsOrder bool
	OldFile []string
	NewFile []string
}

func (info CompactedFileInfo) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.Name)))
	dst = append(dst, info.Name...)
	dst = numberenc.MarshalBool(dst, info.IsOrder)
	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.OldFile)))
	for i := range info.OldFile {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.OldFile[i])))
		dst = append(dst, info.OldFile[i]...)
	}

	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.NewFile)))
	for i := range info.NewFile {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.NewFile[i])))
		dst = append(dst, info.NewFile[i]...)
	}

	return dst
}

func (info *CompactedFileInfo) reset() {
	info.Name = ""
	info.OldFile = info.OldFile[:0]
	info.NewFile = info.NewFile[:0]
}

func (info *CompactedFileInfo) unmarshal(src []byte) error {
	if len(src) < 2 {
		return fmt.Errorf("too small data for name length, %v", len(src))
	}
	l := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < l+3 {
		return fmt.Errorf("too small data for name, %v < %v", len(src), l+3)
	}
	info.Name, src = util.Bytes2str(src[:l]), src[l:]
	info.IsOrder, src = numberenc.UnmarshalBool(src[0]), src[1:]

	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	if cap(info.OldFile) < l {
		info.OldFile = make([]string, l)
	}
	info.OldFile = info.OldFile[:l]
	for i := range info.OldFile {
		if len(src) < 2 {
			return fmt.Errorf("too small data for old file name, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]

		if len(src) < l {
			return fmt.Errorf("too small data for name, %v < %v", len(src), l)
		}
		info.OldFile[i], src = util.Bytes2str(src[:l]), src[l:]
	}

	if len(src) < 2 {
		return fmt.Errorf("too small data new file count, %v", len(src))
	}
	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	info.NewFile = make([]string, l)
	if cap(info.NewFile) < l {
		info.NewFile = make([]string, l)
	}
	info.NewFile = info.NewFile[:l]
	for i := range info.NewFile {
		if len(src) < 2 {
			return fmt.Errorf("too small data for new file name, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]

		if len(src) < l {
			return fmt.Errorf("too small data for name, %v < %v", len(src), l)
		}
		info.NewFile[i], src = util.Bytes2str(src[:l]), src[l:]
	}

	return nil
}

var ErrDirtyLog = errors.New("incomplete log file")
var compLogMagic = []byte("2021A5A5")

func GenLogFileName(logSeq *uint64) string {
	var buf [16]byte
	if _, err := rand.Read(buf[:8]); err != nil {
		log.Error("read random error", zap.Error(err))
		panic(err)
	}
	seq := atomic.AddUint64(logSeq, 1)
	copy(buf[8:], record.Uint64ToBytesUnsafe(seq))
	return fmt.Sprintf("%08x-%08x", buf[:8], buf[8:])
}

func (m *MmsTables) writeCompactedFileInfo(name string, oldFiles, newFiles []TSSPFile, shardDir string, isOrder bool) (string, error) {
	info := &CompactedFileInfo{
		Name:    name,
		IsOrder: isOrder,
		OldFile: make([]string, len(oldFiles)),
		NewFile: make([]string, len(newFiles)),
	}

	for i, f := range oldFiles {
		info.OldFile[i] = filepath.Base(f.Path())
	}

	for i, f := range newFiles {
		info.NewFile[i] = filepath.Base(f.Path())
	}

	fDir := filepath.Join(shardDir, compactLogDir)
	lock := fileops.FileLockOption(*m.lock)
	if err := fileops.MkdirAll(fDir, 0750, lock); err != nil {
		log.Error("mkdir error", zap.String("dir name", fDir), zap.Error(err))
		return "", err
	}
	fName := filepath.Join(fDir, GenLogFileName(&compLogSeq))

	buf := bufferpool.Get()
	defer bufferpool.Put(buf)

	buf = info.marshal(buf[:0])
	buf = append(buf, compLogMagic...)

	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(fName, os.O_CREATE|os.O_WRONLY, 0600, lock, pri)
	if err != nil {
		log.Error("create file error", zap.String("name", fName), zap.Error(err))
		return "", err
	}

	s, err := fd.Write(buf)
	if err != nil || s != len(buf) {
		err = fmt.Errorf("write compact file info log fail, write %v, size %v, err:%v", s, len(buf), err)
		log.Error("write compact file info log fail", zap.Int("write", s), zap.Int("size", len(buf)), zap.Error(err))
		panic(err)
	}

	if err = fd.Sync(); err != nil {
		log.Error("sync compact log file file")
		panic(err)
	}

	return fName, fd.Close()
}

func readCompactLogFile(name string, info *CompactedFileInfo) error {
	fi, err := fileops.Stat(name)
	if err != nil {
		log.Error("stat compact file fail", zap.String("name", name), zap.Error(err))
		return err
	}

	fSize := fi.Size()
	if fSize < int64(len(compLogMagic)) {
		err = fmt.Errorf("too small compact log file(%v) size %v", name, fSize)
		log.Error(err.Error())
		return ErrDirtyLog
	}

	buf := make([]byte, int(fSize))
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(name, os.O_RDONLY, 0600, lock, pri)
	if err != nil {
		log.Error("read compact log file fail", zap.String("name", name), zap.Error(err))
		return err
	}
	defer util.MustClose(fd)

	n, err := fd.Read(buf)
	if err != nil || n != len(buf) {
		err = fmt.Errorf("read compact log file(%v) fail, file size:%v, read size:%v", name, fSize, n)
		log.Error(err.Error())
		return err
	}

	magic := buf[fSize-int64(len(compLogMagic)):]
	if !bytes.Equal(magic, compLogMagic) {
		err = fmt.Errorf("invalid compact log file(%v) magic: exp:%v, read:%v",
			name, util.Bytes2str(compLogMagic), util.Bytes2str(magic))
		log.Error(err.Error())
		return ErrDirtyLog
	}

	if err = info.unmarshal(buf); err != nil {
		log.Error("unmarshal compact log fail", zap.String("name", name), zap.Error(err))
		return err
	}

	return nil
}

func processLog(shardDir string, info *CompactedFileInfo, lockPath *string, engineType config.EngineType) error {
	mmDir := filepath.Join(GetDir(engineType, shardDir), info.Name)
	dirs, err := fileops.ReadDir(mmDir)
	if err != nil {
		log.Error("read dir fail", zap.String("path", mmDir), zap.Error(err))
		return err
	}

	newFileExist, oldFileExist, renameFile := getProcessLogFuncs(dirs, mmDir, lockPath)
	n := 0
	for i := range info.NewFile {
		if newFileExist(info.NewFile[i]) {
			n++
		}
	}

	if n != len(info.NewFile) {
		count := 0
		for i := range info.OldFile {
			if oldFileExist(info.OldFile[i]) {
				count++
			}
		}

		// all old file exist
		if count == len(info.OldFile) {
			for i := range info.OldFile {
				oName := info.OldFile[i]
				if err := renameFile(oName + tmpFileSuffix); err != nil {
					return err
				}
			}
			return nil
		}

		err = fmt.Errorf("invalid compact log file, name:%v, oldFiles:%v, newFiles:%v, order:%v, dirs:%v",
			info.Name, info.OldFile, info.NewFile, info.IsOrder, dirs)
		log.Error(err.Error())
		return err
	}

	err = processFiles(info, oldFileExist, renameFile, mmDir, lockPath)
	if err != nil {
		return err
	}

	return nil
}

func getProcessLogFuncs(dirs []os.FileInfo, mmDir string, lockPath *string) (func(string) bool, func(string) bool, func(string) error) {
	newFileExist := func(newFile string) bool {
		normalName := newFile[:len(newFile)-len(tmpFileSuffix)]
		for i := range dirs {
			name := dirs[i].Name()
			if name == normalName || newFile == name {
				return true
			}
		}
		return false
	}

	oldFileExist := func(oldFile string) bool {
		for i := range dirs {
			name := dirs[i].Name()
			tmp := oldFile + tmpFileSuffix
			if name == oldFile || tmp == name {
				return true
			}
		}
		return false
	}

	renameFile := func(nameInLog string) error {
		for i := range dirs {
			name := dirs[i].Name()
			if nameInLog == name {
				lock := fileops.FileLockOption(*lockPath)
				normalName := nameInLog[:len(nameInLog)-len(tmpFileSuffix)]
				oldName := filepath.Join(mmDir, nameInLog)
				newName := filepath.Join(mmDir, normalName)
				return fileops.RenameFile(oldName, newName, lock)
			}
		}
		return nil
	}
	return newFileExist, oldFileExist, renameFile
}

func processFiles(info *CompactedFileInfo, oldFileExist func(oldFile string) bool, renameFile func(nameInLog string) error,
	mmDir string, lockPath *string) error {
	var err error
	// rename all new files
	for i := range info.NewFile {
		if err = renameFile(info.NewFile[i]); err != nil {
			log.Error("rename file fail", zap.String("name", info.NewFile[i]), zap.Error(err))
			return err
		}
	}

	// delete all old files
	for i := range info.OldFile {
		oldName := info.OldFile[i]
		if oldFileExist(oldName) {
			fName := filepath.Join(mmDir, oldName)
			if _, err = fileops.Stat(fName); os.IsNotExist(err) {
				continue
			}
			lock := fileops.FileLockOption(*lockPath)
			if err = fileops.Remove(fName, lock); err != nil {
				return err
			}
		}
	}
	return err
}

func procCompactLog(shardDir string, logDir string, lockPath *string, engineType config.EngineType) error {
	dirs, err := fileops.ReadDir(logDir)
	if err != nil {
		log.Error("read compact log dir fail", zap.String("path", logDir), zap.Error(err))
		return err
	}

	logInfo := &CompactedFileInfo{}
	for i := range dirs {
		logName := dirs[i].Name()
		logFile := filepath.Join(logDir, logName)
		logInfo.reset()
		err = readCompactLogFile(logFile, logInfo)
		if err != nil {
			if err != ErrDirtyLog {
				return err
			}
			continue
		}

		// continue to handle the rest compact log
		if err = processLog(shardDir, logInfo, lockPath, engineType); err != nil {
			errInfo := errno.NewError(errno.ProcessCompactLogFailed, logInfo.Name, err.Error())
			log.Error("", zap.Error(errInfo))
		}

		lock := fileops.FileLockOption(*lockPath)
		if err = fileops.Remove(logFile, lock); err != nil {
			log.Error("remove log file failed", zap.String("log file", logFile), zap.Error(err))
		}
	}
	return nil
}

func GetDir(engineType config.EngineType, path string) string {
	var filePath string
	switch engineType {
	case config.TSSTORE:
		filePath = filepath.Join(path, TsspDirName)
	case config.COLUMNSTORE:
		filePath = filepath.Join(path, ColumnStoreDirName)
	}
	return filePath
}

func GetUnorderDir(mstPath string) string {
	return path.Join(mstPath, unorderedDir)
}
