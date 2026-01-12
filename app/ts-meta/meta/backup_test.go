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
	CreateMeteFile()

	t.Run("1", func(t *testing.T) {
		s := &Store{
			raft: &MockRaftForSG{isLeader: true},
			data: &meta.Data{
				DataNodes: []meta.DataNode{
					meta.DataNode{NodeInfo: meta.NodeInfo{ID: 1}},
				},
			},
			path:   "/tmp/openGemini/backup_dir/data/meta",
			Logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta")),
			cacheData: &meta.Data{
				MetaNodes: []meta.NodeInfo{meta.NodeInfo{ID: 1}, meta.NodeInfo{ID: 2}},
			},
		}
		globalService.store = s

		b := &Backup{
			IsNode:   true,
			IsRemote: false,
		}
		err := b.RunBackupMeta()
		assert.NoError(t, err)
	})

	t.Run("2", func(t *testing.T) {
		s := &Store{
			raft: &MockRaftForSG{isLeader: false},
		}
		b := &Backup{
			IsNode:   true,
			IsRemote: false,
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
			IsNode:   true,
			IsRemote: false,
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
			IsNode:    true,
			IsRemote:  false,
			Databases: []string{"prom"},
		}
		globalService.store = s
		err := b.RunBackupMeta()
		if err != nil {
			t.Fatal(err)
		}
	})
	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func CreateMeteFile() {
	_ = os.MkdirAll("/tmp/openGemini/backup_dir/data/meta/", 0700)
	fd, _ := fileops.OpenFile("/tmp/openGemini/backup_dir/data/meta/meta.json", os.O_CREATE|os.O_WRONLY, 0640)
	defer fd.Close()

	fd.Write([]byte("123"))

}
