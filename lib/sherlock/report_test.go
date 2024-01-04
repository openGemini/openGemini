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

package sherlock

import (
	"bytes"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_createAndGetFileInfo_CPU(t *testing.T) {
	tmpDir := t.TempDir()
	dumpOpt := &dumpOptions{
		dumpPath: tmpDir,
		maxNum:   1,
		maxAge:   1,
	}
	fi, filename, err := createAndGetFileInfo(dumpOpt, CPU)
	assert.NoError(t, err)
	defer fi.Close()
	assert.Equal(t, true, strings.Contains(filename, tmpDir))
	file := path.Base(filename)
	assert.Contains(t, file, "cpu")
	assert.Contains(t, file, "pb.gz")

	fi2, filename, err := createAndGetFileInfo(dumpOpt, CPU)
	assert.NoError(t, err)
	defer fi2.Close()
	assert.Equal(t, true, strings.Contains(filename, tmpDir))
	file = path.Base(filename)
	assert.Contains(t, file, "cpu")
	assert.Contains(t, file, "pb.gz")

	files, err := filepath.Glob(filepath.Join(tmpDir, allCpuProfiles))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(files))
}

func Test_createAndGetFileInfo_Mem(t *testing.T) {
	tmpDir := t.TempDir()
	dumpOpt := &dumpOptions{
		dumpPath: tmpDir,
		maxNum:   1,
		maxAge:   0,
	}
	fi, filename, err := createAndGetFileInfo(dumpOpt, Memory)
	assert.NoError(t, err)
	defer fi.Close()
	assert.Equal(t, true, strings.Contains(filename, tmpDir))
	file := path.Base(filename)
	assert.Contains(t, file, "mem")
	assert.Contains(t, file, "pb.gz")
}

func Test_createAndGetFileInfo_Goroutine(t *testing.T) {
	tmpDir := t.TempDir()
	dumpOpt := &dumpOptions{
		dumpPath: tmpDir,
		maxNum:   1,
		maxAge:   0,
	}
	fi, filename, err := createAndGetFileInfo(dumpOpt, Goroutine)
	assert.NoError(t, err)
	defer fi.Close()
	assert.Equal(t, true, strings.Contains(filename, tmpDir))
	file := path.Base(filename)
	assert.Contains(t, file, "goroutine")
	assert.Contains(t, file, "pb.gz")
}

func Test_writeFile(t *testing.T) {
	tmpDir := t.TempDir()

	var buf bytes.Buffer
	buf.WriteString("hello world")
	filename, err := writeFile(buf, Memory, &dumpOptions{
		dumpPath: tmpDir,
	})
	assert.NoError(t, err)
	file := path.Base(filename)
	assert.Contains(t, file, "mem")
	assert.Contains(t, file, "pb.gz")
}
