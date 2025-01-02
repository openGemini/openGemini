// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package backup

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/openGemini/openGemini/lib/fileops"
)

const (
	DataBackupDir = "/data_backup"
	MetaBackupDir = "/meta_backup"
	FullBackupLog = "full_backup_log.json"
	IncBackupLog  = "inc_backup_log.json"
	MetaBackupLog = "meta_backup_log.json"
	BackupLogPath = "/backup_log"
	ResultLog     = "result"

	IsInc            = "isInc"
	IsRemote         = "isRemote"
	IsNode           = "isNode"
	BackupPath       = "backupPath"
	OnlyBackupMaster = "onlyBackupMaster"
)

func FileCopy(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	if _, err := os.Stat(dst); err != nil {
		dstPath, _ := filepath.Split(dst)
		srcPath, _ := filepath.Split(src)
		sourceFileStat, err := os.Stat(srcPath)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(dstPath, sourceFileStat.Mode()); err != nil {
			return fmt.Errorf("mkdir failed,perm: %d,err: %w", sourceFileStat.Mode(), err)
		}
	}

	_, err = fileops.CopyFile(src, dst)
	if err != nil {
		return err
	}

	return nil
}

func FolderCopy(src, dst string) error {
	var err error
	var fds []fs.FileInfo
	var srcInfo fs.FileInfo

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}
	if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return fmt.Errorf("mkdir failed,perm: %d,err: %w", srcInfo.Mode(), err)
	}

	if fds, err = fileops.ReadDir(src); err != nil {
		return err
	}

	for _, fd := range fds {
		srcfp := path.Join(src, fd.Name())
		dstfp := path.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = FolderCopy(srcfp, dstfp); err != nil {
				return err
			}
		} else {
			if err = FileCopy(srcfp, dstfp); err != nil {
				return err
			}
		}
	}
	return nil
}

func FileMove(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	if _, err := os.Stat(dst); err != nil {
		dstPath, _ := filepath.Split(dst)
		srcPath, _ := filepath.Split(src)
		sourceFileStat, err := os.Stat(srcPath)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(dstPath, sourceFileStat.Mode()); err != nil {
			return fmt.Errorf("mkdir failed,perm: %d,err: %w", sourceFileStat.Mode(), err)
		}
	}

	err = fileops.RenameFile(src, dst)
	if err != nil {
		return err
	}

	return nil
}

func FolderMove(src, dst string) error {
	var err error
	var fds []fs.FileInfo
	var srcInfo fs.FileInfo

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}
	if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return fmt.Errorf("mkdir failed,perm: %d,err: %w", srcInfo.Mode(), err)
	}

	if fds, err = fileops.ReadDir(src); err != nil {
		return err
	}

	for _, fd := range fds {
		srcfp := path.Join(src, fd.Name())
		dstfp := path.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = FolderMove(srcfp, dstfp); err != nil {
				return err
			}
		} else {
			if err = FileMove(srcfp, dstfp); err != nil {
				return err
			}
		}
	}
	return nil
}

func WriteBackupLogFile(content []byte, path string, logName string) error {
	path = filepath.Join(path, BackupLogPath)
	if err := fileops.MkdirAll(path, 0750); err != nil {
		return err
	}
	fName := filepath.Join(path, logName)

	err := os.WriteFile(fName, content, 0640)
	if err != nil {
		return err
	}

	return nil
}

func ReadBackupLogFile(filePath string, backuplog interface{}) error {
	buf, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(buf, backuplog); err != nil {
		return err
	}
	return nil
}
