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

package backup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
)

func TestFileCopyError(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		err := FileCopy("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("2", func(t *testing.T) {
		CreateFile("/tmp/openGemini/backup_dir/abc.ss")
		err := FileCopy("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
		os.RemoveAll("/tmp/openGemini/backup_dir")
	})
	t.Run("3", func(t *testing.T) {
		filePath := "/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/abc_0000/00000476-0001-00000000.tssp"
		CreateFile(filePath)
		err := FileCopy(filePath, "")
		if err == nil {
			t.Fatal()
		}
		os.RemoveAll("/tmp/openGemini/backup_dir")
	})
}

func TestFolderCopy(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		err := FolderCopy("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("2", func(t *testing.T) {
		CreateFile("/tmp/openGemini/backup_dir/abc.ss")
		err := FolderCopy("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
		os.RemoveAll("/tmp/openGemini/backup_dir")
	})
}

func TestFileMoveError(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		err := FileMove("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("2", func(t *testing.T) {
		CreateFile("/tmp/openGemini/backup_dir/abc.ss")
		err := FileMove("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
		os.RemoveAll("/tmp/openGemini/backup_dir")
	})
	t.Run("3", func(t *testing.T) {
		filePath := "/tmp/openGemini/backup_dir/data/data/db0/0/rp0/0_0_0_0/tssp/abc_0000/00000476-0001-00000000.tssp"
		CreateFile(filePath)
		err := FileMove(filePath, "")
		if err == nil {
			t.Fatal()
		}
		os.RemoveAll("/tmp/openGemini/backup_dir")
	})
}

func TestFolderMove(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		err := FolderMove("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
	})
	t.Run("2", func(t *testing.T) {
		CreateFile("/tmp/openGemini/backup_dir/abc.ss")
		err := FolderMove("/tmp/openGemini/backup_dir", "")
		if err == nil {
			t.Fatal()
		}
		os.RemoveAll("/tmp/openGemini/backup_dir")
	})
}

func TestReadBackupLogFile(t *testing.T) {
	CreateFile("/tmp/openGemini/backup_dir/abc.ss")

	err := ReadBackupLogFile("/tmp/openGemini/backup_dir/abc.ss", nil)
	if err == nil {
		t.Fatal()
	}
	os.RemoveAll("/tmp/openGemini/backup_dir")
}

func CreateFile(path string) {
	p, _ := filepath.Split(path)
	_ = os.MkdirAll(p, 0700)
	fd, _ := fileops.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0640)
	fd.Write([]byte("123"))
	fd.Close()
}
