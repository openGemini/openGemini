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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
)

func TestRunBackupRecoverConfig(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		opt := &RecoverOptions{}
		err := opt.BackupRecover()
		if err == nil {
			t.Fail()
		}
	})
	t.Run("2", func(t *testing.T) {
		opt := &RecoverOptions{
			FullBackupDataPath: "/",
			RecoverMode:        FullAndIncRecoverMode,
		}
		err := opt.BackupRecover()
		if err == nil {
			t.Fail()
		}
	})
	t.Run("3", func(t *testing.T) {
		opt := &RecoverOptions{
			FullBackupDataPath: "/",
			RecoverMode:        RecoverMeta,
		}
		err := opt.BackupRecover()
		if err == nil {
			t.Fail()
		}
	})
}

func TestRecoverMeta(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	t.Run("isInc=true", func(t *testing.T) {
		content := ""
		CreateFile("/tmp/openGemini/backup_dir/backup_inc/meta_backup/backup_log/meta_backup_log.json", content)
		opt.recoverMeta(opt.IncBackupDataPath, nil)
	})

	t.Run("isInc=false", func(t *testing.T) {
		content := ""
		CreateFile("/tmp/openGemini/backup_dir/backup/meta_backup/backup_log/meta_backup_log.json", content)
		opt.recoverMeta(opt.FullBackupDataPath, nil)
	})

	t.Run("onlyRecoverMeta", func(t *testing.T) {
		opt := &RecoverOptions{
			RecoverMode:        "2",
			FullBackupDataPath: fullBackupPath,
			IncBackupDataPath:  incBackupPath,
		}
		content := `{"databases":{"prom":{}}}`
		CreateFile("/tmp/openGemini/backup_dir/backup/backup_log/result", content)
		opt.BackupRecover()
	})

	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func TestRecoverError(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		DataDir:            "/tmp/openGemini/backup_dir/data",
	}

	result := backup.BackupResult{Result: "success"}
	b, _ := json.Marshal(result)
	_ = backup.WriteBackupLogFile(b, fullBackupPath, backup.ResultLog)
	_ = backup.WriteBackupLogFile(b, incBackupPath, backup.ResultLog)

	t.Run("1", func(t *testing.T) {
		err := opt.runRecover(false, []string{})
		if err == nil {
			t.Fail()
		}
	})

	t.Run("2", func(t *testing.T) {
		err := opt.runRecover(true, []string{})
		if err == nil {
			t.Fail()
		}
	})

	os.RemoveAll(fullBackupPath)
	os.RemoveAll(incBackupPath)
}

func TestRecoverError2(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup_full"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"
	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Host:               "127.0.0.1:8091",
		SSL:                true,
		InsecureTLS:        true,
		DataDir:            "/tmp/openGemini/backup_dir/data",
		WalDir:             "/tmp/openGemini/backup_dir/wal",
	}
	t.Run("1", func(t *testing.T) {
		err := opt.runRecover(false, []string{})
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("2", func(t *testing.T) {
		err := opt.runRecover(true, []string{})
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("3", func(t *testing.T) {
		result := backup.BackupResult{Result: "success", Databases: map[string]struct{}{"prom": {}}}
		b, _ := json.Marshal(result)
		_ = backup.WriteBackupLogFile(b, fullBackupPath, backup.ResultLog)
		_ = backup.WriteBackupLogFile(b, incBackupPath, backup.ResultLog)
		os.MkdirAll(filepath.Join(fullBackupPath, backup.DataBackupDir, opt.DataDir, config.DataDirectory), 0700)
		os.MkdirAll(filepath.Join(fullBackupPath, backup.WalBackupDir, opt.WalDir, config.WalDirectory), 0700)

		err := opt.runRecover(false, []string{"prom"})
		os.RemoveAll(fullBackupPath)
		os.RemoveAll(incBackupPath)
		if err != nil {
			if !strings.Contains(err.Error(), "no such file or directory") {
				t.Fatal(err.Error())
			}
		}
	})

}

func Test_TraversalBackupLogFile_Error(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	err := opt.traversalBackupLogFile(fullBackupPath, opt.copyWithFull, false, false)

	if err == nil {
		t.Fatal()
	}
}

func Test_traversalIncBackupLogFile_Error(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	opt := &RecoverOptions{
		RecoverMode:        "1",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	err := opt.traversalBackupLogFile(incBackupPath, opt.copyWithInc, true, true)

	if err == nil {
		t.Fatal()
	}
}

func Test_MergeFileList(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	t.Run("1", func(t *testing.T) {
		listMap := map[string][][]string{"name1": [][]string{[]string{"/tmp/openGemini/backup_dir/path1.file"}, []string{"/tmp/openGemini/backup_dir/path2.file"}}}
		deleteListMap := map[string][][]string{"name1": [][]string{[]string{"/tmp/openGemini/backup_dir/path2.file"}}}

		CreateFile("/tmp/openGemini/backup_dir/backup/data_backup/tmp/openGemini/backup_dir/path1.file", "1")
		CreateFile("/tmp/openGemini/backup_dir/backup/data_backup/tmp/openGemini/backup_dir/path2.file", "2")
		CreateFile("/tmp/openGemini/backup_dir/backup_inc/data_backup/tmp/openGemini/backup_dir/path3.file", "3")

		err := opt.mergeFileList(listMap, deleteListMap)
		assert.NoError(t, err)

		os.RemoveAll("/tmp/openGemini/backup_dir")
	})

	t.Run("2", func(t *testing.T) {
		listMap := map[string][][]string{"name1": [][]string{[]string{"/tmp/openGemini/backup_dir/path1.file"}, []string{"/tmp/openGemini/backup_dir/path2.file"}}}
		deleteListMap := map[string][][]string{"name1": [][]string{[]string{"/tmp/openGemini/backup_dir/path2.file"}}}

		err := opt.mergeFileList(listMap, deleteListMap)
		if err == nil {
			t.Fatal(err)
		}
	})

}

func TestRecoverData(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup_full"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Host:               "127.0.0.1:8091",
		DataDir:            "/tmp/openGemini/data",
	}

	result := backup.BackupResult{Result: "success", Databases: map[string]struct{}{"prom": {}}}
	b, _ := json.Marshal(result)
	_ = backup.WriteBackupLogFile(b, fullBackupPath, backup.ResultLog)
	_ = backup.WriteBackupLogFile(b, incBackupPath, backup.ResultLog)
	dataPath := filepath.Join(fullBackupPath, backup.DataBackupDir, opt.DataDir, config.DataDirectory)
	_ = os.MkdirAll(dataPath, 0700)

	t.Run("1", func(t *testing.T) {
		err := opt.recoverData([]string{"prom"}, false)
		if err != nil {
			t.Fail()
		}
	})

	t.Run("2", func(t *testing.T) {
		err := opt.recoverData([]string{"prom"}, true)
		assert.Error(t, err)
	})

	os.RemoveAll(fullBackupPath)
	os.RemoveAll(incBackupPath)
}

func TestRewriteRecover(t *testing.T) {
	t.Skip()
	fullBackupPath := "/backup_test/tmp/openGemini/backup_full"

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		DataDir:            "/backup_test/tmp/openGemini/data",
	}
	result := backup.BackupResult{
		Result:    "success",
		Databases: map[string]struct{}{"prom": {}},
		DataDir:   "/backup_test/tmp/openGemini/data",
		WalDir:    "/backup_test/tmp/openGemini/data",
	}
	b, _ := json.Marshal(result)
	_ = backup.WriteBackupLogFile(b, fullBackupPath, backup.ResultLog)
	nodeInfoMap := &backup.NodeInfoMap{
		PtMap: map[string]map[string]string{"db1": {"0": "1"}},
		DbMap: map[string]map[string]*backup.RpInfoMap{
			"db1": {"rp1": &backup.RpInfoMap{ShardMap: map[string]string{"10": "11"}, IndexMap: map[string]string{"5": "6"}}},
		},
	}
	b, _ = json.Marshal(nodeInfoMap)
	_ = backup.WriteBackupLogFile(b, fullBackupPath, backup.NodeMapInfo)

	CreateFile("/backup_test/tmp/openGemini/backup_full/data_backup/backup_test/tmp/openGemini/data/data/db1/0/rp1/10_1763337600000000000_1763942400000000000_5/mst_0000/test_file.tssp", "1")
	CreateFile("/backup_test/tmp/openGemini/backup_full/data_backup/backup_test/tmp/openGemini/data/data/db1/0/rp1/index/5_1763337600000000000_1763942400000000000/mergeset/test_file", "1")

	CreateFile("/backup_test/tmp/openGemini/backup_full/wal_backup/backup_test/tmp/openGemini/data/wal/db1/0/rp1/10_1763337600000000000_1763942400000000000_5/0/test_file", "1")
	CreateFile("/backup_test/tmp/openGemini/backup_full/wal_backup/backup_test/tmp/openGemini/data/wal/db1/0/__raft_entries__/00001.entry", "1")

	err := opt.BackupRecover()
	assert.NoError(t, err)
	os.RemoveAll("/backup_test")
}

func TestGenNodeMap(t *testing.T) {
	t.Skip()
	fullBackupPath := "/backup_test/tmp/openGemini/backup_full"

	opt := &RecoverOptions{
		RecoverMode:        GenNodeInfoMap,
		FullBackupDataPath: fullBackupPath,
		DataDir:            "/backup_test/tmp/openGemini/data",
		SrcNode:            4,
		DstNode:            5,
	}

	metaInfo := &meta.Data{
		PtView: map[string]meta.DBPtInfos{
			"db1": {
				0: {Owner: meta.PtOwner{NodeID: uint64(4)}, PtId: 0, RGID: 0},
				1: {Owner: meta.PtOwner{NodeID: uint64(5)}, PtId: 1, RGID: 0},
				2: {Owner: meta.PtOwner{NodeID: uint64(6)}, PtId: 2, RGID: 0},
				3: {Owner: meta.PtOwner{NodeID: uint64(4)}, PtId: 3, RGID: 1},
				4: {Owner: meta.PtOwner{NodeID: uint64(5)}, PtId: 4, RGID: 1},
				5: {Owner: meta.PtOwner{NodeID: uint64(6)}, PtId: 5, RGID: 1},
			},
		},
		Databases: map[string]*meta.DatabaseInfo{
			"db1": {
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp1": {
						ShardGroups: []meta.ShardGroupInfo{{Shards: []meta.ShardInfo{
							{ID: 19, Owners: []uint32{0}},
							{ID: 20, Owners: []uint32{1}},
							{ID: 21, Owners: []uint32{2}},
							{ID: 22, Owners: []uint32{3}},
							{ID: 23, Owners: []uint32{4}},
							{ID: 24, Owners: []uint32{5}},
						}}},
						IndexGroups: []meta.IndexGroupInfo{{Indexes: []meta.IndexInfo{
							{ID: 1, Owners: []uint32{0}},
							{ID: 2, Owners: []uint32{1}},
							{ID: 3, Owners: []uint32{2}},
							{ID: 4, Owners: []uint32{3}},
							{ID: 5, Owners: []uint32{4}},
							{ID: 6, Owners: []uint32{5}},
						}}},
					},
				},
			},
		},
	}
	b, _ := json.Marshal(metaInfo)
	dstPath := filepath.Join(fullBackupPath, backup.MetaBackupDir)
	_ = fileops.MkdirAll(dstPath, 0700)
	_ = fileops.WriteFile(filepath.Join(dstPath, backup.MetaInfo), b, 0640)

	err := opt.BackupRecover()
	assert.NoError(t, err)

	opt.getNodeMap()
	nodeInfoMap := opt.NodeInfoMap
	assert.Equal(t, nodeInfoMap.PtMap["db1"]["0"], "1")
	assert.Equal(t, nodeInfoMap.PtMap["db1"]["3"], "4")

	assert.Equal(t, nodeInfoMap.DbMap["db1"]["rp1"].ShardMap["19"], "20")
	assert.Equal(t, nodeInfoMap.DbMap["db1"]["rp1"].IndexMap["1"], "2")

	metaInfo.Databases["db2"] = &meta.DatabaseInfo{}
	b, _ = json.Marshal(metaInfo)
	_ = fileops.WriteFile(filepath.Join(dstPath, backup.MetaInfo), b, 0640)
	err = opt.BackupRecover()
	assert.Error(t, err)

	os.RemoveAll("/backup_test")
}

func CreateFile(path, content string) {
	p, _ := filepath.Split(path)
	_ = os.MkdirAll(p, 0700)
	fd, _ := fileops.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0640)
	fd.Write([]byte(content))
	fd.Close()
}
