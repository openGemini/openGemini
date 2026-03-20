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

package httpd_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/app/ts-recover/recover"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

type mockMetaClient4Backup struct {
	metaclient.MetaClient
}

var isInc bool
var isMasterPt = true

func (m *mockMetaClient4Backup) DBRepGroups(database string) []meta.ReplicaGroup {
	if isMasterPt {
		return []meta.ReplicaGroup{
			{
				ID:         0,
				MasterPtID: 0,
				Peers:      []meta.Peer{{ID: 1}, {ID: 2}},
			},
		}
	} else {
		return nil
	}
}

func (m *mockMetaClient4Backup) DBPtView(database string) (meta.DBPtInfos, error) {
	return meta.DBPtInfos{
		{PtId: 0, Owner: meta.PtOwner{NodeID: 4}, RGID: 0},
		{PtId: 1, Owner: meta.PtOwner{NodeID: 5}, RGID: 0},
		{PtId: 2, Owner: meta.PtOwner{NodeID: 6}, RGID: 0},
		{PtId: 3, Owner: meta.PtOwner{NodeID: 4}, RGID: 1},
		{PtId: 4, Owner: meta.PtOwner{NodeID: 5}, RGID: 1},
		{PtId: 5, Owner: meta.PtOwner{NodeID: 6}, RGID: 1},
	}, nil
}

func (m *mockMetaClient4Backup) DataNode(nodeId uint64) (*meta.DataNode, error) {
	return &meta.DataNode{}, nil
}

func (m *mockMetaClient4Backup) IsMasterPt(ptId uint32, dbName string) bool {
	return isMasterPt
}

func (m mockMetaClient4Backup) Database(name string) (*meta.DatabaseInfo, error) {
	return &meta.DatabaseInfo{
		Name: "db0",
		RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
			"rp0": {
				ShardGroups: []meta.ShardGroupInfo{{Shards: []meta.ShardInfo{
					{ID: 1, Owners: []uint32{1}},
					{ID: 2, Owners: []uint32{2}},
				}}},
				IndexGroups: []meta.IndexGroupInfo{{Indexes: []meta.IndexInfo{
					{ID: 1, Owners: []uint32{1}},
					{ID: 2, Owners: []uint32{2}},
				}}},
			},
		},
	}, nil
}

type mockTableStore struct {
	immutable.TablesStore
	files *immutable.TSSPFiles
}

func (m *mockTableStore) GetAllMstList() []string {
	return []string{"a_0000", "b_0000"}
}

func (m *mockTableStore) GetBothFilesRef(measurement string, hasTimeFilter bool, tr util.TimeRange, flushed *bool) ([]immutable.TSSPFile, []immutable.TSSPFile, bool) {
	orderFiles := make([]immutable.TSSPFile, 0, 0)
	filePath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/%s/00000476-0001-00000000.tssp", measurement)
	orderFiles = append(orderFiles, mockTsspFile{
		path: filePath,
	})
	CreateFile(filePath)
	if isInc {
		filePath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/%s/00000476-0002-00000000.tssp", measurement)
		orderFiles = append(orderFiles, mockTsspFile{
			path: filePath,
		})
		CreateFile(filePath)
	}

	unOrderFiles := make([]immutable.TSSPFile, 0, 0)
	filePath = fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/%s/outoforder/00000476-0001-00000000.tssp", measurement)
	unOrderFiles = append(unOrderFiles, mockTsspFile{
		path: filePath,
	})
	CreateFile(filePath)
	if isInc {
		filePath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/%s/outoforder/00000476-0002-00000000.tssp", measurement)
		unOrderFiles = append(unOrderFiles, mockTsspFile{
			path: filePath,
		})
		CreateFile(filePath)
	}

	return orderFiles, unOrderFiles, false
}

func (m *mockTableStore) GetCSFilesRef(measurement string) ([]immutable.TSSPFile, bool) {
	return []immutable.TSSPFile{}, true
}

type mockTableStore2 struct {
	immutable.TablesStore
	files *immutable.TSSPFiles
}

func (m *mockTableStore2) GetAllMstList() []string {
	return []string{"a_0000", "b_0000"}
}

func (m *mockTableStore2) GetBothFilesRef(measurement string, hasTimeFilter bool, tr util.TimeRange, flushed *bool) ([]immutable.TSSPFile, []immutable.TSSPFile, bool) {
	files := make([]immutable.TSSPFile, 0, 0)
	filePath := "/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/a_0000/00000476-0001-00000000.tssp"
	files = append(files, mockTsspFile{
		path: filePath,
	})

	if isInc {
		filePath := "/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/a_0000/00000476-0002-00000000.tssp"
		files = append(files, mockTsspFile{
			path: filePath,
		})
	}

	return files, []immutable.TSSPFile{}, true
}

func (m *mockTableStore2) GetCSFilesRef(measurement string) ([]immutable.TSSPFile, bool) {
	return []immutable.TSSPFile{}, true
}

type mockTableStore3 struct {
	immutable.TablesStore
	files *immutable.TSSPFiles
}

func (m *mockTableStore3) GetAllMstList() []string {
	panic("")
	return []string{"a_0000", "b_0000"}
}

func (m *mockTableStore3) GetBothFilesRef(measurement string, hasTimeFilter bool, tr util.TimeRange, flushed *bool) ([]immutable.TSSPFile, []immutable.TSSPFile, bool) {
	return []immutable.TSSPFile{}, []immutable.TSSPFile{}, true
}

func (m *mockTableStore3) GetCSFilesRef(measurement string) ([]immutable.TSSPFile, bool) {
	csFiles := make([]immutable.TSSPFile, 0, 0)
	filePath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0001-00000000.tssp", measurement)
	csFiles = append(csFiles, mockTsspFile{
		path: filePath,
	})
	idxPath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0001-00000000.idx", measurement)
	CreateFile(filePath)
	CreateFile(idxPath)
	if isInc {
		filePath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0002-00000000.tssp", measurement)
		csFiles = append(csFiles, mockTsspFile{
			path: filePath,
		})
		idxPath = fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0002-00000000.idx", measurement)
		CreateFile(filePath)
		CreateFile(idxPath)
	}
	return csFiles, true
}

type mockTableStore4 struct {
	immutable.TablesStore
	files *immutable.TSSPFiles
}

func (m *mockTableStore4) GetAllMstList() []string {
	return []string{"a_0000", "b_0000"}
}

func (m *mockTableStore4) GetBothFilesRef(measurement string, hasTimeFilter bool, tr util.TimeRange, flushed *bool) ([]immutable.TSSPFile, []immutable.TSSPFile, bool) {
	return []immutable.TSSPFile{}, []immutable.TSSPFile{}, true
}

func (m *mockTableStore4) GetCSFilesRef(measurement string) ([]immutable.TSSPFile, bool) {
	csFiles := make([]immutable.TSSPFile, 0, 0)
	filePath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0001-00000000.tssp", measurement)
	csFiles = append(csFiles, mockCSTsspFile{
		path: filePath,
	})
	idxPath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0001-00000000.idx", measurement)
	countPath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/count.txt", measurement)
	CreateFile(filePath)
	CreateFile(idxPath)
	CreateFile(countPath)
	if isInc {
		filePath := fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0002-00000000.tssp", measurement)
		csFiles = append(csFiles, mockCSTsspFile{
			path: filePath,
		})
		idxPath = fmt.Sprintf("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/%s/00000476-0002-00000000.idx", measurement)
		CreateFile(filePath)
		CreateFile(idxPath)
	}
	return csFiles, true
}

func CreateEngine(mode int) *engine.EngineImpl {
	client := &mockMetaClient4Backup{}
	dbPtInfo := engine.NewDBPTInfo("db0", 0, "/tmp/openGemini/backup_dir/data/data/db0/0", "", nil, nil, nil)
	dbPtInfo.AddShard(11, &mockShard{mode: mode})
	dbPtInfo.AddShard(12, &mockShard{mode: mode})
	dbPtInfo.AddShard(13, &mockShard{mode: mode})
	dbPtInfo.AddShard(14, &mockShard{mode: mode})
	raftStore, _ := raftlog.Init("/tmp/openGemini/backup_dir/data/wal/db0/0/__raft_entries__", 0)
	dbPtInfo.SetNode(&raftconn.RaftNode{
		Store: raftStore,
	})

	e := &engine.EngineImpl{
		DBPartitions: map[string]map[uint32]*engine.DBPTInfo{
			"db0": {
				0: dbPtInfo,
			},
		},
	}
	e.SetMetaClient(client)
	return e

}

func TestBackup(t *testing.T) {
	_ = os.MkdirAll("/tmp/openGemini/backup_dir", 0700)

	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	b := &engine.Backup{
		IsInc:           false,
		IsRemote:        false,
		BackupPath:      fullBackupPath,
		Engine:          CreateEngine(1),
		OnlyBackupMater: true,
	}
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}

	b.IsInc = true
	isInc = true
	b.BackupPath = incBackupPath
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}

	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func TestBackupCSFile(t *testing.T) {
	_ = os.MkdirAll("/tmp/openGemini/backup_dir", 0700)

	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	b := &engine.Backup{
		IsInc:           false,
		IsRemote:        false,
		BackupPath:      fullBackupPath,
		Engine:          CreateEngine(4),
		OnlyBackupMater: true,
	}
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}

	b.IsInc = true
	isInc = true
	b.BackupPath = incBackupPath
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}

	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func TestBackupPanic(t *testing.T) {
	_ = os.MkdirAll("/tmp/openGemini/backup_dir", 0700)

	fullBackupPath := "/tmp/openGemini/backup_dir/backup"

	b := &engine.Backup{
		IsInc:           false,
		IsRemote:        false,
		BackupPath:      fullBackupPath,
		Engine:          CreateEngine(3),
		OnlyBackupMater: true,
	}
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}
}

func TestRecover(t *testing.T) {
	isMasterPt = false
	os.RemoveAll("/tmp/openGemini/backup_dir")
	_ = os.MkdirAll("/tmp/openGemini/backup_dir", 0700)

	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	b := &engine.Backup{
		IsInc:      false,
		IsRemote:   false,
		BackupPath: fullBackupPath,
		Engine:     CreateEngine(1),
	}
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}

	indexPath := fmt.Sprintf("%s/data_backup/tmp/openGemini/backup_dir/data/data/db0/0/rp0/index/index.file", fullBackupPath)
	CreateFile(indexPath)
	indexPath = fmt.Sprintf("%s/data_backup/tmp/openGemini/backup_dir/data/data/db0/0/rp0/index/index.file", incBackupPath)
	CreateFile(indexPath)

	recoverConfig := &recover.RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Force:              true,
		DataDir:            "/tmp/openGemini/backup_dir/data",
	}
	_ = os.MkdirAll(filepath.Join(fullBackupPath, backup.WalBackupDir, "wal"), 0700)

	if err := recoverConfig.BackupRecover(); err != nil {
		t.Fatal(err)
	}

	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func TestRecover2(t *testing.T) {
	isMasterPt = false
	_ = os.MkdirAll("/tmp/openGemini/backup_dir", 0700)

	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	b := &engine.Backup{
		IsInc:      false,
		IsRemote:   false,
		BackupPath: fullBackupPath,
		Engine:     CreateEngine(1),
	}
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}

	b.IsInc = true
	isInc = true
	b.BackupPath = incBackupPath
	if err := b.RunBackupData(); err != nil {
		t.Fatal(err)
	}

	indexPath := fmt.Sprintf("%s/data_backup/tmp/openGemini/backup_dir/data/data/db0/0/rp0/index/index.file", fullBackupPath)
	CreateFile(indexPath)
	indexPath = fmt.Sprintf("%s/data_backup/tmp/openGemini/backup_dir/data/data/db0/0/rp0/index/index.file", incBackupPath)
	CreateFile(indexPath)

	r := &recover.RecoverOptions{
		RecoverMode:        "1",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Force:              true,
		DataDir:            "/tmp/openGemini/backup_dir/data",
	}
	_ = os.MkdirAll(filepath.Join(incBackupPath, backup.WalBackupDir, "wal"), 0700)

	if err := r.BackupRecover(); err != nil {
		t.Fatal(err)
	}

	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func CreateFile(path string) {
	p, _ := filepath.Split(path)
	_ = os.MkdirAll(p, 0700)
	fd, _ := fileops.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0640)
	fd.Write([]byte("{}"))
	fd.Close()
}

type mockShard struct {
	mode int
	engine.Shard
}

func (ms *mockShard) GetTableStore() immutable.TablesStore {
	switch ms.mode {
	case 1:
		return &mockTableStore{}
	case 2:
		return &mockTableStore2{}
	case 3:
		return &mockTableStore3{}
	case 4:
		return &mockTableStore4{}
	}
	return &mockTableStore{}
}

func (ms *mockShard) GetIndexBuilder() *tsi.IndexBuilder {
	ib := &tsi.IndexBuilder{}

	CreateFile("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/index/index.file")
	ib.SetPath("/tmp/openGemini/backup_dir/data/data/db0/0/rp0/index")

	return ib
}

func (ms *mockShard) GetDataPath() string {
	return "/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0"
}

func (ms *mockShard) IsOpened() bool {
	return true
}

func (ms *mockShard) OpenAndEnable(m metaclient.MetaClient) error {
	return nil
}

type mockTsspFile struct {
	immutable.TSSPFile
	path string
}

func (m mockTsspFile) Path() string {
	return m.path
}

func (m mockTsspFile) Ref() {
	return
}

func (m mockTsspFile) Unref() {
	return
}

func (m mockTsspFile) RefFileReader() {
	return
}

func (m mockTsspFile) UnrefFileReader() {
	return
}

func (m mockTsspFile) GetPkInfo() *colstore.PKInfo {
	return nil
}

func (m mockTsspFile) GetSkipIndexInfo() []*colstore.SkipIndexInfo {
	return []*colstore.SkipIndexInfo{}
}

type mockCSTsspFile struct {
	immutable.TSSPFile
	path string
}

func (m mockCSTsspFile) Path() string {
	return m.path
}

func (m mockCSTsspFile) Ref() {
	return
}

func (m mockCSTsspFile) Unref() {
	return
}

func (m mockCSTsspFile) RefFileReader() {
	return
}

func (m mockCSTsspFile) UnrefFileReader() {
	return
}

func (m mockCSTsspFile) GetPkInfo() *colstore.PKInfo {
	return &colstore.PKInfo{}
}

func (m mockCSTsspFile) GetSkipIndexInfo() []*colstore.SkipIndexInfo {
	p := "/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/columnstore/a_0000/00000476-0002-00000000.skipIndex"
	info := colstore.NewSkipIndexInfo(p)
	CreateFile(p)
	return []*colstore.SkipIndexInfo{info}
}
