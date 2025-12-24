// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package meta

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/fileops"
)

type Backup struct {
	time       int64
	IsRemote   bool
	IsNode     bool
	BackupMeta bool
	BackupPath string
	Databases  []string
	MetaDir    string
}

func (s *Backup) RunBackupMeta() error {
	dstPath := filepath.Join(s.BackupPath, backup.MetaBackupDir)
	if err := fileops.MkdirAll(dstPath, 0700); err != nil {
		return err
	}
	s.time = time.Now().UnixNano()
	if globalService == nil || globalService.store == nil {
		return fmt.Errorf("meta global service is nil")
	}
	if !globalService.store.IsLeader() && !s.BackupMeta {
		return nil
	}

	var err error
	// snapshot
	if err = globalService.store.raft.UserSnapshot(); err != nil {
		return err
	}

	err = s.BackupMetaInfo(dstPath)
	if err != nil {
		return err
	}
	if len(s.Databases) == 0 {
		// if we need to backup all database,just back up all meta files
		err = s.BackupMetaFile(dstPath)
	}
	if err != nil {
		return err
	}
	return s.writeLogInfo()
}

func (s *Backup) BackupMetaFile(dstPath string) error {
	return backup.FolderCopy(globalService.store.path, dstPath)
}

func (s *Backup) BackupMetaInfo(dstPath string) error {
	var err error
	if err = fileops.MkdirAll(dstPath, 0700); err != nil {
		return err
	}

	info, err := globalService.store.GetMarshalData([]string{})
	if err != nil {
		return err
	}

	fName := filepath.Join(dstPath, backup.MetaInfo)
	err = os.WriteFile(fName, info, 0640)

	return err
}

func (s *Backup) writeLogInfo() error {
	logInfo := &backup.BackupResult{
		MetaDir: s.MetaDir,
	}

	dbMap := make(map[string]struct{})
	for _, dbName := range s.Databases {
		dbMap[dbName] = struct{}{}
	}
	logInfo.Databases = dbMap

	return backup.WriteResultFile(logInfo, s.BackupPath, backup.ResultLog)
}
