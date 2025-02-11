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
	"fmt"
	"os"
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
	diskDir := t.TempDir()
	CreateMeteFile(diskDir)

	t.Run("1", func(t *testing.T) {
		BackupPath := t.TempDir()
		s := &Store{
			raft: &MockRaftForSG{isLeader: true},
			data: &meta.Data{
				DataNodes: []meta.DataNode{
					meta.DataNode{NodeInfo: meta.NodeInfo{ID: 1}},
				},
			},
			path:   fmt.Sprintf("%s/openGemini/backup_dir/data/meta/", diskDir),
			Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
			cacheData: &meta.Data{
				MetaNodes: []meta.NodeInfo{meta.NodeInfo{ID: 1}, meta.NodeInfo{ID: 2}},
			},
		}
		globalService.store = s

		b := &Backup{
			IsNode:     true,
			IsRemote:   false,
			BackupPath: BackupPath,
		}
		err := b.RunBackupMeta()
		assert.NoError(t, err)
	})

	t.Run("2", func(t *testing.T) {
		BackupPath := t.TempDir()
		s := &Store{
			raft: &MockRaftForSG{isLeader: false},
		}
		b := &Backup{
			IsNode:     true,
			IsRemote:   false,
			BackupPath: BackupPath,
		}
		globalService.store = s
		err := b.RunBackupMeta()
		assert.NoError(t, err)
	})

	t.Run("3", func(t *testing.T) {
		BackupPath := t.TempDir()
		s := &Store{
			raft: &MockRaftForSG{isLeader: true},
		}
		b := &Backup{
			IsNode:     true,
			IsRemote:   false,
			BackupPath: BackupPath,
		}
		globalService.store = s
		err := b.RunBackupMeta()
		if err == nil {
			t.Fatal()
		}
	})
	os.RemoveAll(fmt.Sprintf("%s/openGemini/backup_dir", diskDir))
}

func CreateMeteFile(diskDir string) {
	dir := fmt.Sprintf("%s/openGemini/backup_dir/data/meta/", diskDir)
	mkerr := os.MkdirAll(dir, 0750)
	if mkerr != nil {
		fmt.Println(mkerr.Error())
	}
	filepath := fmt.Sprintf("%s/meta.json", dir)
	fd, err := fileops.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0750)
	if err != nil {
		fmt.Println(err.Error())
	}

	fd.Write([]byte("123"))

}
