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
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/stretchr/testify/assert"
)

func TestRunBackupRecoverConfig(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		rcConifg := &RecoverConfig{}
		err := BackupRecover(rcConifg, nil)
		if err == nil {
			t.Fail()
		}
	})
	t.Run("2", func(t *testing.T) {
		rcConifg := &RecoverConfig{
			FullBackupDataPath: "/",
			RecoverMode:        FullAndIncRecoverMode,
		}
		err := BackupRecover(rcConifg, nil)
		if err == nil {
			t.Fail()
		}
	})
	t.Run("3", func(t *testing.T) {
		rcConifg := &RecoverConfig{
			FullBackupDataPath: "/",
			RecoverMode:        "3",
		}
		err := BackupRecover(rcConifg, nil)
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
	tsRecover := &config.TsRecover{
		Data: config.Store{
			DataDir: "/tmp/openGemini/backup_dir/data",
			MetaDir: "/tmp/openGemini/backup_dir/meta",
		},
	}

	t.Run("isInc=true", func(t *testing.T) {
		content := `{"metaIds":["1"],"isNode":true}`
		CreateFile("/tmp/openGemini/backup_dir/backup_inc/meta_backup/backup_log/meta_backup_log.json", content)
		recoverMeta(tsRecover, recoverConfig, true)
	})

	t.Run("isInc=false", func(t *testing.T) {
		content := `{"metaIds":["1","2"],"isNode":false}`
		CreateFile("/tmp/openGemini/backup_dir/backup/meta_backup/backup_log/meta_backup_log.json", content)
		recoverMeta(tsRecover, recoverConfig, false)
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
	}
	tsRecover := &config.TsRecover{
		Data: config.Store{
			DataDir: "/tmp/openGemini/backup_dir/data",
			MetaDir: "/tmp/openGemini/backup_dir/meta",
		},
	}

	t.Run("1", func(t *testing.T) {
		err := recoverWithFull(tsRecover, recoverConfig)
		if err != nil {
			t.Fail()
		}
	})

	t.Run("2", func(t *testing.T) {
		err := recoverWithFullAndInc(tsRecover, recoverConfig)
		if err != nil {
			t.Fail()
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

func CreateFile(path, content string) {
	p, _ := filepath.Split(path)
	_ = os.MkdirAll(p, 0750)
	fd, _ := fileops.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0640)
	fd.Write([]byte(content))
	fd.Close()
}
