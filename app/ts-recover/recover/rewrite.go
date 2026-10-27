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

package recover

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/raftlog"
)

const (
	TypeData = 1
	TypeWal  = 2
)

type TraverseFunc func(baseDir, name string, typ int) error

func traversePath(p string, typ int, fc TraverseFunc) error {
	var fds []fs.FileInfo
	var err error

	if fds, err = fileops.ReadDir(p); err != nil {
		return err
	}
	for _, fd := range fds {
		err = fc(p, fd.Name(), typ)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RecoverOptions) recoverDB(nodeInfoMap *backup.NodeInfoMap) TraverseFunc {
	return func(baseDir, name string, typ int) error {
		p := filepath.Join(baseDir, name)
		dbMap, ok := nodeInfoMap.DbMap[name]
		if !ok {
			return fmt.Errorf("db not found in nodeInfoMap,db: %s", name)
		}
		ptMap, ok := nodeInfoMap.PtMap[name]
		if !ok {
			return fmt.Errorf("pt not found in nodeInfoMap,db: %s", name)
		}
		return traversePath(p, typ, r.recoverPt(dbMap, ptMap))
	}
}

func (r *RecoverOptions) recoverPt(dbMap map[string]*backup.RpInfoMap, ptMap map[string]string) TraverseFunc {
	return func(baseDir, name string, typ int) error {
		var err error
		p := filepath.Join(baseDir, name)
		dstPt, ok := ptMap[name]
		if !ok {
			return fmt.Errorf("ptID not found,ptID: %s", name)
		}

		// rename pt dir
		dstPath := filepath.Join(baseDir, dstPt)
		err = fileops.RenameFile(p, dstPath)
		if err != nil {
			return err
		}
		return traversePath(dstPath, typ, r.recoverRp(dbMap))
	}
}

func (r *RecoverOptions) recoverRp(dbMap map[string]*backup.RpInfoMap) TraverseFunc {
	return func(baseDir, name string, typ int) error {
		p := filepath.Join(baseDir, name)
		if name == raftlog.RaftEntriesDir {
			dstPath := strings.Replace(p, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
			return backup.FolderMove(p, dstPath)
		}

		rpMap, ok := dbMap[name]
		if !ok {
			return fmt.Errorf("rp not found in nodeInfoMap,rp: %s", name)
		}
		return traversePath(p, typ, r.recoverShard(rpMap))
	}
}

func (r *RecoverOptions) recoverShard(rpMap *backup.RpInfoMap) TraverseFunc {
	return func(baseDir, name string, typ int) error {
		p := filepath.Join(baseDir, name)
		if name == config.IndexFileDirectory {
			return traversePath(p, typ, r.recoverIndex(rpMap))
		}
		logPath := filepath.Join(p, backup.BackupLogPath)
		_ = fileops.RemoveAll(logPath)
		// shardInfo[0]: shardID, shardInfo[1]: start time, shardInfo[2]: end time, shardInfo[3]: indexID
		shardInfo := strings.Split(name, "_")
		if len(shardInfo) != 4 {
			return fmt.Errorf("wrong shard dir,path: %s", p)
		}
		shardID, ok := rpMap.ShardMap[shardInfo[0]]
		if !ok {
			return nil
		}

		indexID, ok := rpMap.IndexMap[shardInfo[3]]
		if !ok {
			return nil
		}
		shardInfo[0] = shardID
		shardInfo[3] = indexID

		var dir string
		if typ == TypeData {
			dir = strings.Replace(baseDir, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
		} else if typ == TypeWal {
			dir = strings.Replace(baseDir, filepath.Join(r.FullBackupDataPath, backup.WalBackupDir), "", -1)
		}

		dstPath := filepath.Join(dir, strings.Join(shardInfo, "_"))
		// rename pt dir
		return backup.FolderMove(p, dstPath)
	}
}

func (r *RecoverOptions) recoverIndex(rpMap *backup.RpInfoMap) TraverseFunc {
	return func(baseDir, name string, typ int) error {
		p := filepath.Join(baseDir, name)

		indexInfo := strings.Split(name, "_")
		if len(indexInfo) != 3 {
			return fmt.Errorf("wrong index dir,path: %s", p)
		}
		indexID, ok := rpMap.IndexMap[indexInfo[0]]
		if !ok {
			return nil
		}
		indexInfo[0] = indexID
		dir := strings.Replace(baseDir, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
		dstPath := filepath.Join(dir, strings.Join(indexInfo, "_"))

		return backup.FolderMove(p, dstPath)
	}
}
