//go:build streamfs
// +build streamfs

/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package meta

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	raftrocksdb "github.com/openGemini/openGemini/lib/rocksdb"
	raftstreamfs "github.com/openGemini/openGemini/lib/util/lifted/hashicorp/raft-streamfs"
	"go.uber.org/zap"
)

func (r *raftWrapper) raftStore(c *config.Meta) error {
	var err error
	l := log.New(os.Stderr, "raft", log.LstdFlags|log.Lmicroseconds)
	r.snapStore, err = raftstreamfs.NewStreamFileSnapshotStore(c.Dir, raftSnapshotsRetained, l)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	dbPath := filepath.Join(c.Dir, "rocksdb")
	store, err := raftrocksdb.NewRocksStore(filepath.Dir(c.Logging.Path), int(c.Logging.MaxSize), c.Logging.MaxNum, dbPath)
	if err != nil {
		return fmt.Errorf("new rocksdb store: %s", err)
	}
	logger.GetLogger().Info("rocksdb store", zap.String("logPath", filepath.Dir(c.Logging.Path)), zap.String("dbPath", dbPath))
	r.logStore = store
	r.stableStore = store
	return nil
}

func (r *raftWrapper) CloseStore() error {
	if r.logStore != nil {
		s, ok := r.logStore.(*raftrocksdb.RocksStore)
		if !ok {
			panic("the raft store is not rocksdb")
		}
		return s.Close()
	}
	return nil
}
