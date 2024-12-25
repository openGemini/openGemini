/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package recover

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	fileops "github.com/openGemini/openGemini/lib/fileops"
)

const FullAndIncRecoverMode = "1"
const FullRecoverMode = "2"

type RecoverConfig struct {
	RecoverMode        string
	ConfigPath         string
	FullBackupDataPath string
	IncBackupDataPath  string
}

type RecoverFunc func(rc *RecoverConfig, path string) error

func BackupRecover(opt *RecoverConfig, tsRecover *config.TsRecover) error {
	if opt.FullBackupDataPath == "" {
		return fmt.Errorf("`missing required config: fullBackupDataPath")
	}
	if opt.RecoverMode == "1" && opt.IncBackupDataPath == "" {
		return fmt.Errorf("`missing required parameter: incBackupDataPath")
	}
	var err error
	switch opt.RecoverMode {
	case FullAndIncRecoverMode:
		err = recoverWithFullAndInc(tsRecover, opt)
	case FullRecoverMode:
		err = recoverWithFull(tsRecover, opt)
	default:
		return fmt.Errorf("invalid recovermode")
	}
	if err != nil {
		return err
	}

	return nil
}

func recoverWithFull(tsRecover *config.TsRecover, rc *RecoverConfig) error {
	if err := recoverMeta(tsRecover, rc, false); err != nil {
		return err
	}

	dataPath := filepath.Join(tsRecover.Data.DataDir, config.DataDirectory)
	if err := os.RemoveAll(dataPath); err != nil {
		return err
	}
	backupDataPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir, dataPath)
	if err := traversalBackupLogFile(rc, backupDataPath, copyWithFull, false); err != nil {
		return err
	}

	return nil
}

func recoverWithFullAndInc(tsRecover *config.TsRecover, rc *RecoverConfig) error {

	if err := recoverMeta(tsRecover, rc, true); err != nil {
		return err
	}

	dataPath := filepath.Join(tsRecover.Data.DataDir, config.DataDirectory)
	if err := os.RemoveAll(dataPath); err != nil {
		return err
	}
	// recover full_backup
	fullBackupDataPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir, dataPath)
	if err := traversalBackupLogFile(rc, fullBackupDataPath, copyWithFullAndInc, true); err != nil {
		return err
	}
	// recover inc_backup
	incBackupDataPath := filepath.Join(rc.IncBackupDataPath, backup.DataBackupDir, dataPath)
	if err := traversalIncBackupLogFile(rc, incBackupDataPath); err != nil {
		return err
	}
	return nil
}

func recoverMeta(tsRecover *config.TsRecover, rc *RecoverConfig, isInc bool) error {
	var backupPath string
	if isInc {
		backupPath = rc.IncBackupDataPath
	} else {
		backupPath = rc.FullBackupDataPath
	}
	backupMetaPath := filepath.Join(backupPath, backup.MetaBackupDir)
	backupLog := &backup.MetaBackupLogInfo{}
	var noMeta bool
	if err := backup.ReadBackupLogFile(path.Join(backupMetaPath, backup.BackupLogPath, backup.MetaBackupLog), backupLog); err != nil {
		noMeta = true
		backupLog.IsNode = true
		backupLog.MetaIds = make([]string, 0)
	}

	metaPath, err := removeMeta(tsRecover, backupLog)
	if err != nil {
		return nil
	}

	if !noMeta {
		// single node
		if len(backupLog.MetaIds) == 1 || backupLog.IsNode {
			if err := backup.FolderMove(backupMetaPath, metaPath); err != nil {
				return err
			}
		} else {
			for _, id := range backupLog.MetaIds {
				if err := backup.FolderCopy(backupMetaPath, filepath.Join(metaPath, id)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func removeMeta(tsRecover *config.TsRecover, backupLog *backup.MetaBackupLogInfo) (metaPath string, err error) {
	//single node
	if len(backupLog.MetaIds) == 1 || backupLog.IsNode {
		metaPath = tsRecover.Data.MetaDir
	} else {
		metaPath, _ = filepath.Split(tsRecover.Data.MetaDir)
	}

	if err = os.RemoveAll(metaPath); err != nil {
		return
	}

	return
}

func traversalBackupLogFile(rc *RecoverConfig, path string, fn RecoverFunc, isInc bool) error {
	var err error
	var fds []fs.FileInfo

	if _, err = os.Stat(path); err != nil {
		return err
	}
	if fds, err = fileops.ReadDir(path); err != nil {
		return err
	}

	for _, fd := range fds {
		srcfp := filepath.Join(path, fd.Name())

		if fd.IsDir() {
			if fd.Name() == "index" && !isInc {
				outPath := strings.Replace(srcfp, filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FolderMove(srcfp, outPath); err != nil {
					return err
				}
				continue
			}
			if err = traversalBackupLogFile(rc, srcfp, fn, isInc); err != nil {
				return err
			}
		} else {
			if fd.Name() == backup.FullBackupLog {
				if err := fn(rc, srcfp); err != nil {
					return err
				}
				outPath := strings.Replace(srcfp, filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FileMove(srcfp, outPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func traversalIncBackupLogFile(rc *RecoverConfig, path string) error {
	var err error
	var fds []fs.FileInfo

	if _, err = os.Stat(path); err != nil {
		return err
	}
	if fds, err = fileops.ReadDir(path); err != nil {
		return err
	}

	for _, fd := range fds {
		srcfp := filepath.Join(path, fd.Name())

		if fd.IsDir() {
			if fd.Name() == "index" {
				outPath := strings.Replace(srcfp, filepath.Join(rc.IncBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FolderMove(srcfp, outPath); err != nil {
					return err
				}
				continue
			}
			if err = traversalIncBackupLogFile(rc, srcfp); err != nil {
				return err
			}
		} else {
			if fd.Name() == backup.IncBackupLog {
				if err := copyWithInc(rc, srcfp); err != nil {
					return err
				}
				// recover log file
				outPath := strings.Replace(srcfp, filepath.Join(rc.IncBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FileMove(srcfp, outPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func copyWithFull(rc *RecoverConfig, path string) error {
	backupLog := &backup.BackupLogInfo{}
	if err := backup.ReadBackupLogFile(path, backupLog); err != nil {
		return err
	}

	basicPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir)
	for _, fileList := range backupLog.OrderFileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}

	for _, fileList := range backupLog.OutOfOrderFileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func copyWithFullAndInc(rc *RecoverConfig, fullPath string) error {
	p, _ := path.Split(strings.Replace(fullPath, rc.FullBackupDataPath, rc.IncBackupDataPath, -1))
	incPath := path.Join(p, backup.IncBackupLog)
	incBackupLog := &backup.IncBackupLogInfo{}
	_, err := fileops.Stat(incPath)
	if err != nil {
		err = copyWithFull(rc, fullPath)
		return err
	}
	if err := backup.ReadBackupLogFile(incPath, incBackupLog); err != nil {
		return err
	}

	backupLog := &backup.BackupLogInfo{}
	if err := backup.ReadBackupLogFile(fullPath, backupLog); err != nil {
		return err
	}

	err = mergeFileList(rc, backupLog.OrderFileListMap, incBackupLog.DelOrderFileListMap)
	if err != nil {
		return err
	}
	err = mergeFileList(rc, backupLog.OutOfOrderFileListMap, incBackupLog.DelOutOfOrderFileListMap)
	if err != nil {
		return err
	}

	return nil
}

func copyWithInc(rc *RecoverConfig, incPath string) error {
	backupLog := &backup.IncBackupLogInfo{}
	if err := backup.ReadBackupLogFile(incPath, backupLog); err != nil {
		return err
	}

	basicPath := filepath.Join(rc.IncBackupDataPath, backup.DataBackupDir)
	for _, fileList := range backupLog.AddOrderFileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}

	for _, fileList := range backupLog.AddOutOfOrderFileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func mergeFileList(rc *RecoverConfig, listMap, delListMap map[string][][]string) error {
	fullBasicPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir)
	for name, fileList := range listMap {
		dListSeen := make(map[string]bool)
		if dLists, ok := delListMap[name]; ok {
			for _, files := range dLists {
				dListSeen[files[0]] = true
			}
		}
		for _, files := range fileList {
			// delete
			if dListSeen[files[0]] {
				continue
			}
			srcPath := filepath.Join(fullBasicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}

	}
	return nil
}
