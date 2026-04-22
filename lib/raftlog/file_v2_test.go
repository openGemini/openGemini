/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package raftlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileWrapV2_OpenFile(t *testing.T) {
	tmpDir := t.TempDir()
	fw1, err := OpenFileV2(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 1000)
	require.EqualError(t, err, NewFile.Error())
	defer fw1.Close()

	assert.Contains(t, fw1.Name(), "test")
	assert.Equal(t, 1000, fw1.Size())

	fw2, err := OpenFileV2(filepath.Join(tmpDir, metaName), os.O_CREATE|os.O_RDWR, 1000)
	require.EqualError(t, err, NewFile.Error())

	fw3, err := OpenFileV2(tmpDir, os.O_CREATE|os.O_RDWR, 1000)
	assert.NotEqual(t, err, nil)

	fw3 = &FileWrapV2{}
	require.Equal(t, fw3.Name(), "")

	fw2.Close()
	require.Equal(t, fw2.Size(), 0)
}

func TestGetEntryDataV2(t *testing.T) {
	fw := FileWrapV2{}
	result := fw.GetEntryData(0, 0, 0, true)
	assert.Equal(t, len(result), 0)

	tmpDir := t.TempDir()
	fw1, err := OpenFileV2(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 1000)
	require.EqualError(t, err, NewFile.Error())
	defer fw1.Close()

	result = fw1.GetEntryData(0, 0, 5, false)
	assert.Equal(t, len(result), 5)

	result = fw1.GetEntryData(0, 0, 5, false)
	assert.Equal(t, len(result), 5)

	result = fw1.GetEntryData(1, 1001, 1006, false)
	assert.Equal(t, len(result), 0)
}

func TestFileWrapV2_Write_WriteAt(t *testing.T) {
	tmpDir := t.TempDir()
	fw, err := OpenFileV2(filepath.Join(tmpDir, metaName), os.O_CREATE|os.O_RDWR, 1000)
	require.EqualError(t, err, NewFile.Error())
	defer fw.Close()

	n, err := fw.WriteAt(0, 0, []byte("meta"), true)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("meta"), fw.GetEntryData(0, 0, 4, true))

	n, err = fw.WriteAt(0, 0, []byte("data"), false)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("data"), fw.GetEntryData(0, 0, 4, false))
}

func TestFileWrapV2_TrySync(t *testing.T) {

}

func TestFileWrapV2_WriteSlice_ReadSlice(t *testing.T) {
	tmpDir := t.TempDir()
	fw, err := OpenFileV2(filepath.Join(tmpDir, metaName), os.O_CREATE|os.O_RDWR, 1000)
	require.EqualError(t, err, NewFile.Error())
	defer fw.Close()

	err = fw.WriteSlice(0, 0, 0, []byte("meta"), true, false)
	require.NoError(t, err)
	require.Equal(t, []byte("meta"), fw.ReadSlice(0, 0, true))

	err = fw.WriteSlice(0, 0, 0, []byte("data"), false, false)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), fw.ReadSlice(0, 0, false))
	require.Equal(t, fw.SliceSize(0, 0), 8)

	result := fw.ReadSlice(1, 0, false)
	require.Equal(t, []byte("data"), result)

	fw.setCurrent()
	err = fw.WriteSlice(2, 0, 0, []byte("Data"), false, false)
	require.NoError(t, err)
	require.Equal(t, []byte("Data"), fw.ReadSlice(2, 0, false))

	require.Equal(t, fw.SliceSize(3, 0), 8)

	fw.rotateCurrent()
}

func TestTruncateV2(t *testing.T) {
	tmpDir := t.TempDir()
	fw, err := OpenFileV2(filepath.Join(tmpDir, metaName), os.O_CREATE|os.O_RDWR, 1000)
	require.EqualError(t, err, NewFile.Error())
	defer fw.Close()

	err = fw.WriteSlice(0, 0, 0, []byte("Hello, world! GoodBye!!!"), false, false)
	require.NoError(t, err)

	err = fw.Truncate(5)
	require.NoError(t, err)
	require.Equal(t, 5, int(fw.Size()))
}

func TestFileWrapV2_Delete_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	file, err := fileops.OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 0640)
	require.NoError(t, err)

	fw := &FileWrapV2{}
	err = fw.Delete()
	require.NoError(t, err)
	require.NoError(t, fw.TrySync())
	fw.fd = file
	require.NoError(t, fw.TrySync())
	err = fw.Delete()
	require.NoError(t, err)
	err = fw.Delete()
	require.NotEqual(t, nil, err)

	file1, err := fileops.OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 0640)
	require.NoError(t, err)
	fw1 := &FileWrapV2{fd: file1}
	require.NoError(t, fw1.Close())
	require.NotEqual(t, nil, fw1.Close())
}
