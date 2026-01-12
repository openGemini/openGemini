//go:build !streamfs
// +build !streamfs

// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	raftboltdb "github.com/openGemini/openGemini/lib/util/lifted/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

func (r *raftWrapper) raftStore(c *config.Meta) error {
	var err error
	r.snapStore, err = raft.NewFileSnapshotStore(c.Dir, raftSnapshotsRetained, nil)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	dbPath := filepath.Join(c.Dir, "raft.db")
	store, err := raftboltdb.NewBoltStore(dbPath)
	if err != nil {
		return fmt.Errorf("new boltdb store: %s", err)
	}
	logger.GetLogger().Info("boltdb store", zap.String("dbPath", dbPath))
	r.logStore = store
	r.stableStore = store
	return nil
}

func (r *raftWrapper) CloseStore() error {
	if r.logStore != nil {
		s, ok := r.logStore.(*raftboltdb.BoltStore)
		if !ok {
			panic("the raft store is not boltdb")
		}
		return s.Close()
	}
	return nil
}
