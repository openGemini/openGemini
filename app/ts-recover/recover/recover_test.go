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
	"testing"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/stretchr/testify/assert"
)

func TestRunBackupRecoverConfig(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		rcConifg := &RecoverConfig{}
		err := BackupRecover(rcConifg)
		if err == nil {
			t.Fail()
		}
	})
	t.Run("2", func(t *testing.T) {
		rcConifg := &RecoverConfig{
			FullBackupDataPath: "/",
			RecoverMode:        FullAndIncRecoverMode,
		}
		err := BackupRecover(rcConifg)
		if err == nil {
			t.Fail()
		}
	})
	t.Run("3", func(t *testing.T) {
		rcConifg := &RecoverConfig{
			FullBackupDataPath: "/",
			RecoverMode:        "3",
		}
		err := BackupRecover(rcConifg)
		if err == nil {
			t.Fail()
		}
	})
}

func TestRecoverMeta(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	recoverConfig := &RecoverConfig{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	t.Run("isInc=true", func(t *testing.T) {
		content := `{"metaIds":["1"],"isNode":true}`
		CreateFile("/tmp/openGemini/backup_dir/backup_inc/meta_backup/backup_log/meta_backup_log.json", content)
		recoverMeta(recoverConfig, true, nil)
	})

	t.Run("isInc=false", func(t *testing.T) {
		content := `{"metaIds":["1","2"],"isNode":false}`
		CreateFile("/tmp/openGemini/backup_dir/backup/meta_backup/backup_log/meta_backup_log.json", content)
		recoverMeta(recoverConfig, false, nil)
	})

	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func TestRecoverError(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	recoverConfig := &RecoverConfig{
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
		err := runRecover(recoverConfig, false)
		if err == nil {
			t.Fail()
		}
	})

	t.Run("2", func(t *testing.T) {
		err := runRecover(recoverConfig, true)
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
	recoverConfig := &RecoverConfig{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Host:               "127.0.0.1:8091",
		SSL:                true,
		InsecureTLS:        true,
		DataDir:            "/tmp/openGemini/backup_dir/data",
	}
	t.Run("1", func(t *testing.T) {
		err := runRecover(recoverConfig, false)
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("2", func(t *testing.T) {
		err := runRecover(recoverConfig, true)
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("3", func(t *testing.T) {
		result := backup.BackupResult{Result: "success", DataBases: map[string]struct{}{"prom": {}}}
		b, _ := json.Marshal(result)
		_ = backup.WriteBackupLogFile(b, fullBackupPath, backup.ResultLog)
		_ = backup.WriteBackupLogFile(b, incBackupPath, backup.ResultLog)
		os.MkdirAll(filepath.Join(fullBackupPath, backup.DataBackupDir, recoverConfig.DataDir, config.DataDirectory), 0700)
		err := runRecover(recoverConfig, false)
		os.RemoveAll(fullBackupPath)
		os.RemoveAll(incBackupPath)
		if err == nil {
			t.Fatal(err.Error())
		}
	})

}

func Test_TraversalBackupLogFile_Error(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	recoverConfig := &RecoverConfig{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	err := traversalBackupLogFile(recoverConfig, fullBackupPath, copyWithFull, false)

	if err == nil {
		t.Fatal()
	}
}

func Test_traversalIncBackupLogFile_Error(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	recoverConfig := &RecoverConfig{
		RecoverMode:        "1",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
	}

	err := traversalIncBackupLogFile(recoverConfig, incBackupPath)

	if err == nil {
		t.Fatal()
	}
}

func Test_MergeFileList(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	recoverConfig := &RecoverConfig{
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

		err := mergeFileList(recoverConfig, listMap, deleteListMap)
		assert.NoError(t, err)

		os.RemoveAll("/tmp/openGemini/backup_dir")
	})

	t.Run("2", func(t *testing.T) {
		listMap := map[string][][]string{"name1": [][]string{[]string{"/tmp/openGemini/backup_dir/path1.file"}, []string{"/tmp/openGemini/backup_dir/path2.file"}}}
		deleteListMap := map[string][][]string{"name1": [][]string{[]string{"/tmp/openGemini/backup_dir/path2.file"}}}

		err := mergeFileList(recoverConfig, listMap, deleteListMap)
		if err == nil {
			t.Fatal(err)
		}
	})

}

func TestRecoverData(t *testing.T) {
	fullBackupPath := "/tmp/openGemini/backup_dir/backup_full"
	incBackupPath := "/tmp/openGemini/backup_dir/backup_inc"

	recoverConfig := &RecoverConfig{
		RecoverMode:        "2",
		FullBackupDataPath: fullBackupPath,
		IncBackupDataPath:  incBackupPath,
		Host:               "127.0.0.1:8091",
		DataDir:            "/tmp/openGemini/data",
	}

	result := backup.BackupResult{Result: "success", DataBases: map[string]struct{}{"prom": {}}}
	b, _ := json.Marshal(result)
	_ = backup.WriteBackupLogFile(b, fullBackupPath, backup.ResultLog)
	_ = backup.WriteBackupLogFile(b, incBackupPath, backup.ResultLog)
	dataPath := filepath.Join(fullBackupPath, backup.DataBackupDir, recoverConfig.DataDir, config.DataDirectory)
	_ = os.MkdirAll(dataPath, 0700)

	t.Run("1", func(t *testing.T) {
		err := recoverData(recoverConfig, []string{"prom"}, false)
		if err != nil {
			t.Fail()
		}
	})

	t.Run("2", func(t *testing.T) {
		err := recoverData(recoverConfig, []string{"prom"}, true)
		if err != nil {
			t.Fail()
		}
	})

	os.RemoveAll(fullBackupPath)
	os.RemoveAll(incBackupPath)
}

func CreateFile(path, content string) {
	p, _ := filepath.Split(path)
	_ = os.MkdirAll(p, 0700)
	fd, _ := fileops.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0640)
	fd.Write([]byte(content))
	fd.Close()
}
