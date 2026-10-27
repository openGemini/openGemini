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

const fixedRecoverBackupDir = "/tmp/openGemini/backup_dir"

func prepareRecoverBackupDir(t *testing.T) {
	t.Helper()

	target := filepath.Join(t.TempDir(), "backup_dir")
	if err := os.MkdirAll(target, 0700); err != nil {
		t.Fatalf("create temp backup dir failed: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(fixedRecoverBackupDir), 0700); err != nil {
		t.Fatalf("create fixed backup parent dir failed: %v", err)
	}
	_ = os.RemoveAll(fixedRecoverBackupDir)
	if err := os.Symlink(target, fixedRecoverBackupDir); err != nil {
		t.Fatalf("link fixed backup dir failed: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(fixedRecoverBackupDir)
	})
}

func recoverPath(elem ...string) string {
	parts := append([]string{fixedRecoverBackupDir}, elem...)
	return filepath.Join(parts...)
}

func recoverArchivePath(backupPath, archiveDir, originalPath string) string {
	return filepath.Join(backupPath, archiveDir, strings.TrimPrefix(originalPath, string(os.PathSeparator)))
}

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
	prepareRecoverBackupDir(t)
	fullBackupPath := recoverPath("backup")
	incBackupPath := recoverPath("backup_inc")

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	t.Run("isInc=true", func(t *testing.T) {
		content := ""
		CreateFile(recoverPath("backup_inc", "meta_backup", "backup_log", "meta_backup_log.json"), content)
		opt.recoverMeta(opt.IncBackupDataPath, nil)
	})

	t.Run("isInc=false", func(t *testing.T) {
		content := ""
		CreateFile(recoverPath("backup", "meta_backup", "backup_log", "meta_backup_log.json"), content)
		opt.recoverMeta(opt.FullBackupDataPath, nil)
	})

	t.Run("onlyRecoverMeta", func(t *testing.T) {
		opt := &RecoverOptions{
			RecoverMode:        "2",
			FullBackupDataPath: fullBackupPath,
			IncBackupDataPath:  incBackupPath,
		}
		content := `{"databases":{"prom":{}}}`
		CreateFile(recoverPath("backup", "backup_log", "result"), content)
		opt.BackupRecover()
	})

	os.RemoveAll(recoverPath())
}

func TestRecoverError(t *testing.T) {
	prepareRecoverBackupDir(t)
	fullBackupPath := recoverPath("backup")
	incBackupPath := recoverPath("backup_inc")

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		DataDir:            recoverPath("data"),
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
	prepareRecoverBackupDir(t)
	fullBackupPath := recoverPath("backup_full")
	incBackupPath := recoverPath("backup_inc")
	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Host:               "127.0.0.1:8091",
		SSL:                true,
		InsecureTLS:        true,
		DataDir:            recoverPath("data"),
		WalDir:             recoverPath("wal"),
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
	prepareRecoverBackupDir(t)
	fullBackupPath := recoverPath("backup")
	incBackupPath := recoverPath("backup_inc")

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
	prepareRecoverBackupDir(t)
	fullBackupPath := recoverPath("backup")
	incBackupPath := recoverPath("backup_inc")

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
	prepareRecoverBackupDir(t)
	fullBackupPath := recoverPath("backup")
	incBackupPath := recoverPath("backup_inc")

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	t.Run("1", func(t *testing.T) {
		path1 := recoverPath("path1.file")
		path2 := recoverPath("path2.file")
		path3 := recoverPath("path3.file")
		listMap := map[string][][]string{"name1": [][]string{[]string{path1}, []string{path2}}}
		deleteListMap := map[string][][]string{"name1": [][]string{[]string{path2}}}

		CreateFile(recoverArchivePath(fullBackupPath, backup.DataBackupDir, path1), "1")
		CreateFile(recoverArchivePath(fullBackupPath, backup.DataBackupDir, path2), "2")
		CreateFile(recoverArchivePath(incBackupPath, backup.DataBackupDir, path3), "3")

		err := opt.mergeFileList(listMap, deleteListMap)
		assert.NoError(t, err)

		os.RemoveAll(recoverPath())
	})

	t.Run("2", func(t *testing.T) {
		path1 := recoverPath("path1.file")
		path2 := recoverPath("path2.file")
		listMap := map[string][][]string{"name1": [][]string{[]string{path1}, []string{path2}}}
		deleteListMap := map[string][][]string{"name1": [][]string{[]string{path2}}}

		err := opt.mergeFileList(listMap, deleteListMap)
		if err == nil {
			t.Fatal(err)
		}
	})

}

func TestRecoverData(t *testing.T) {
	prepareRecoverBackupDir(t)
	fullBackupPath := recoverPath("backup_full")
	incBackupPath := recoverPath("backup_inc")
	dataDir := filepath.Join(t.TempDir(), "openGemini", "data")

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Host:               "127.0.0.1:8091",
		DataDir:            dataDir,
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
	backupRoot := filepath.Join(t.TempDir(), "backup_test", "tmp", "openGemini")
	fullBackupPath := filepath.Join(backupRoot, "backup_full")
	dataDir := filepath.Join(backupRoot, "data")

	opt := &RecoverOptions{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		DataDir:            dataDir,
	}
	result := backup.BackupResult{
		Result:    "success",
		Databases: map[string]struct{}{"prom": {}},
		DataDir:   dataDir,
		WalDir:    dataDir,
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

	CreateFile(recoverArchivePath(fullBackupPath, backup.DataBackupDir, filepath.Join(dataDir, config.DataDirectory, "db1", "0", "rp1", "10_1763337600000000000_1763942400000000000_5", "mst_0000", "test_file.tssp")), "1")
	CreateFile(recoverArchivePath(fullBackupPath, backup.DataBackupDir, filepath.Join(dataDir, config.DataDirectory, "db1", "0", "rp1", "index", "5_1763337600000000000_1763942400000000000", "mergeset", "test_file")), "1")

	CreateFile(recoverArchivePath(fullBackupPath, backup.WalBackupDir, filepath.Join(dataDir, config.WalDirectory, "db1", "0", "rp1", "10_1763337600000000000_1763942400000000000_5", "0", "test_file")), "1")
	CreateFile(recoverArchivePath(fullBackupPath, backup.WalBackupDir, filepath.Join(dataDir, config.WalDirectory, "db1", "0", "__raft_entries__", "00001.entry")), "1")

	err := opt.BackupRecover()
	assert.NoError(t, err)
}

func TestGenNodeMap(t *testing.T) {
	t.Skip()
	backupRoot := filepath.Join(t.TempDir(), "backup_test", "tmp", "openGemini")
	fullBackupPath := filepath.Join(backupRoot, "backup_full")
	dataDir := filepath.Join(backupRoot, "data")

	opt := &RecoverOptions{
		RecoverMode:        GenNodeInfoMap,
		FullBackupDataPath: fullBackupPath,
		DataDir:            dataDir,
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
}

func CreateFile(path, content string) {
	p, _ := filepath.Split(path)
	_ = os.MkdirAll(p, 0700)
	fd, _ := fileops.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0640)
	fd.Write([]byte(content))
	fd.Close()
}
