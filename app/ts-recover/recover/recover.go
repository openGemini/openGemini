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

package recover

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
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
	DataDir            string
	FullBackupDataPath string
	IncBackupDataPath  string
	SSL                bool
	InsecureTLS        bool
	Force              bool
	// leader meta address
	Host string
}

type RecoverFunc func(rc *RecoverConfig, path string) error

func BackupRecover(opt *RecoverConfig) error {
	if opt.FullBackupDataPath == "" {
		return fmt.Errorf("`missing required parameter: fullBackupDataPath")
	}
	if opt.RecoverMode == "1" && opt.IncBackupDataPath == "" {
		return fmt.Errorf("`missing required parameter: incBackupDataPath")
	}
	var err error
	switch opt.RecoverMode {
	case FullAndIncRecoverMode:
		err = runRecover(opt, true)
	case FullRecoverMode:
		err = runRecover(opt, false)
	default:
		return fmt.Errorf("invalid recovermode")
	}
	if err != nil {
		return err
	}

	return nil
}

func runRecover(rc *RecoverConfig, isInc bool) error {
	dbs, err := getDatabases(rc)
	if err != nil {
		return err
	}
	if err := recoverData(rc, dbs, isInc); err != nil {
		return err
	}

	return recoverMeta(rc, isInc, dbs)
}

func recoverData(rc *RecoverConfig, dbs []string, isInc bool) error {
	dataPath := filepath.Join(rc.DataDir, config.DataDirectory)
	// check clear data path
	if len(dbs) > 0 {
		for _, db := range dbs {
			p := filepath.Join(dataPath, db)
			_, err := os.Stat(p)
			if !rc.Force && err == nil {
				return fmt.Errorf("target database file exist,db : %s.if you still recover,please use --force", db)
			}
			if err := os.RemoveAll(p); err != nil {
				return err
			}
		}
	} else {
		_, err := os.Stat(dataPath)
		if !rc.Force && err == nil {
			return fmt.Errorf("data file exist.if you still recover,please use --force")
		}
		if err := os.RemoveAll(filepath.Join(dataPath)); err != nil {
			return err
		}
	}

	copyFunc := copyWithFull
	if isInc {
		copyFunc = copyWithFullAndInc
	}

	// recover full_backup
	fullBackupDataPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir, dataPath)
	if _, err := os.Stat(fullBackupDataPath); err != nil {
		return errors.New("backupDataPath empty")
	}
	if err := traversalBackupLogFile(rc, fullBackupDataPath, copyFunc, isInc); err != nil {
		return err
	}

	if !isInc {
		return nil
	}

	// recover inc_backup
	incBackupDataPath := filepath.Join(rc.IncBackupDataPath, backup.DataBackupDir, dataPath)
	if _, err := os.Stat(incBackupDataPath); err != nil {
		fmt.Println("incBackupDataPath empty !")
		return nil
	}
	if err := traversalIncBackupLogFile(rc, incBackupDataPath); err != nil {
		return err
	}

	return nil
}

func recoverMeta(rc *RecoverConfig, isInc bool, dbs []string) error {
	var backupPath string
	if isInc {
		backupPath = rc.IncBackupDataPath
	} else {
		backupPath = rc.FullBackupDataPath
	}
	backupMetaPath := filepath.Join(backupPath, backup.MetaBackupDir)
	buf, err := os.ReadFile(filepath.Join(backupMetaPath, backup.MetaInfo))
	if err != nil {
		return err
	}

	return sendRequestToMeta(rc, dbs, string(buf))
}

func sendRequestToMeta(rc *RecoverConfig, dbs []string, metaData string) error {
	protocol := "http"
	if rc.SSL {
		protocol = "https"
	}
	urlValues := url.Values{}
	urlValues.Add(backup.DataBases, strings.Join(dbs, ","))
	urlValues.Add(backup.MetaData, metaData)

	Url, err := url.Parse(fmt.Sprintf("%s://%s/recoverMeta", protocol, rc.Host))
	if err != nil {
		return err
	}
	Url.RawQuery = urlValues.Encode()

	transport := &http.Transport{}
	client := &http.Client{
		Transport: transport,
	}
	if rc.InsecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	res, err := client.PostForm(Url.String(), urlValues)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return resolveResponseError(res)
}

func resolveResponseError(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("recover meta request error,read response body faild,%s", err.Error())
	}

	return fmt.Errorf("recover meta error,%s", string(b))
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
	for _, fileList := range backupLog.FileListMap {
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
	p, _ := filepath.Split(strings.Replace(fullPath, rc.FullBackupDataPath, rc.IncBackupDataPath, -1))
	incPath := filepath.Join(p, backup.IncBackupLog)
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

	err = mergeFileList(rc, backupLog.FileListMap, incBackupLog.DelFileListMap)
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
	for _, fileList := range backupLog.AddFileListMap {
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

func getDatabases(rc *RecoverConfig) ([]string, error) {
	path := fileops.Join(rc.FullBackupDataPath, backup.BackupLogPath, backup.ResultLog)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	fullRes := &backup.BackupResult{}
	if err := backup.ReadBackupLogFile(path, fullRes); err != nil {
		return nil, err
	}
	databases := make([]string, 0, len(fullRes.DataBases))

	if rc.RecoverMode == FullRecoverMode {
		for db := range fullRes.DataBases {
			databases = append(databases, db)
		}
		return databases, nil
	}

	path = fileops.Join(rc.IncBackupDataPath, backup.BackupLogPath, backup.ResultLog)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	incRes := &backup.BackupResult{}
	if err := backup.ReadBackupLogFile(path, incRes); err != nil {
		return nil, err
	}

	if fullRes.Time > incRes.Time {
		return nil, errors.New("fullBackup time should earlier than incBackup")
	}

	if len(fullRes.DataBases) != len(incRes.DataBases) {
		return nil, errors.New("databases not equal in full Backup and inc Backup")
	}

	for db := range fullRes.DataBases {
		if _, ok := incRes.DataBases[db]; !ok {
			return nil, errors.New("databases not equal in full Backup and inc Backup")
		}
		databases = append(databases, db)
	}

	return databases, nil
}
