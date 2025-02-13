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

package meta

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/openGemini/openGemini/lib/backup"
)

type Backup struct {
	time       int64
	IsRemote   bool
	IsNode     bool
	BackupPath string
}

func (s *Backup) RunBackupMeta() error {
	s.time = time.Now().UnixNano()
	if globalService == nil {
		return fmt.Errorf("meta global service is nil")
	}
	if !globalService.store.IsLeader() {
		return nil

	}
	// snapshot
	if err := globalService.store.raft.UserSnapshot(); err != nil {
		return err
	}

	dstPath := filepath.Join(s.BackupPath, backup.MetaBackupDir)
	if err := backup.FolderCopy(globalService.store.path, dstPath); err != nil {
		return err
	}

	metaIds := make([]string, len(globalService.store.cacheData.MetaNodes))
	for i, n := range globalService.store.cacheData.MetaNodes {
		metaIds[i] = strconv.FormatUint(n.ID, 10)
	}

	backupLog := &backup.MetaBackupLogInfo{
		MetaIds: metaIds,
		IsNode:  s.IsNode,
	}
	content, err := json.MarshalIndent(&backupLog, "", "\t")
	if err != nil {
		return err
	}
	if err := backup.WriteBackupLogFile(content, dstPath, backup.MetaBackupLog); err != nil {
		return err
	}

	return nil
}
