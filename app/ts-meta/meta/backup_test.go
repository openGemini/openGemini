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
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRunBackup(t *testing.T) {
	conf := config.NewMeta()
	_ = NewService(conf, nil, nil)
	metaPath := CreateMeteFile(t)
	backupPath := t.TempDir()

	t.Run("1", func(t *testing.T) {
		s := &Store{
			raft: &MockRaftForSG{isLeader: true},
			data: &meta.Data{
				DataNodes: []meta.DataNode{
					meta.DataNode{NodeInfo: meta.NodeInfo{ID: 1}},
				},
			},
			path:   metaPath,
			Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
			cacheData: &meta.Data{
				MetaNodes: []meta.NodeInfo{meta.NodeInfo{ID: 1}, meta.NodeInfo{ID: 2}},
			},
		}
		globalService.store = s

		b := &Backup{
			BackupPath: backupPath,
			IsNode:     true,
			IsRemote:   false,
		}
		err := b.RunBackupMeta()
		assert.NoError(t, err)
	})

	t.Run("2", func(t *testing.T) {
		s := &Store{
			raft: &MockRaftForSG{isLeader: false},
		}
		b := &Backup{
			BackupPath: backupPath,
			IsNode:     true,
			IsRemote:   false,
		}
		globalService.store = s
		err := b.RunBackupMeta()
		assert.NoError(t, err)
	})

	t.Run("3", func(t *testing.T) {
		s := &Store{
			raft: &MockRaftForSG{isLeader: true},
		}
		b := &Backup{
			BackupPath: backupPath,
			IsNode:     true,
			IsRemote:   false,
		}
		globalService.store = s
		err := b.RunBackupMeta()
		if err == nil {
			t.Fatal(err)
		}
	})

	t.Run("4", func(t *testing.T) {
		s := &Store{
			raft: &MockRaftForSG{isLeader: true},
			cacheData: &meta.Data{
				MetaNodes: []meta.NodeInfo{{ID: 0}},
			},
		}
		b := &Backup{
			BackupPath: backupPath,
			IsNode:     true,
			IsRemote:   false,
			Databases:  []string{"prom"},
		}
		globalService.store = s
		err := b.RunBackupMeta()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func CreateMeteFile(t *testing.T) string {
	t.Helper()

	metaDir := filepath.Join(t.TempDir(), "backup_dir", "data", "meta")
	_ = os.MkdirAll(metaDir, 0700)
	fd, _ := fileops.OpenFile(filepath.Join(metaDir, "meta.json"), os.O_CREATE|os.O_WRONLY, 0640)
	defer fd.Close()

	fd.Write([]byte("123"))
	return metaDir
}
