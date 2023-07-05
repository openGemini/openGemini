/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package colstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyWriter(t *testing.T) {
	testCompDir := t.TempDir()
	fn := filepath.Join(testCompDir, "PrimaryKeyWriter.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(fn)
	fd, err := fileops.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	fw := newPrimaryKeyWriter(fd, &lockPath)
	if fn != fw.Name() {
		t.Fatalf("invalid writer name")
	}

	if fw.GetFileWriter() == nil {
		t.Fatalf("invalid file writer")
	}

	n, err := fw.WriteData([]byte("test data"))
	if err != nil {
		t.Fatal(err)
	}

	if fw.DataSize() != int64(n) {
		t.Fatalf("write data fail")
	}
	_ = fd.Close()
	fw.Close()
	_, err = fw.WriteData([]byte("test data")) // test for abnomral branch
	require.ErrorContains(t, err, "file already closed")
}

func TestPrimaryKeyWriterWriteChunkMeta(t *testing.T) {
	testCompDir := t.TempDir()
	fn := filepath.Join(testCompDir, "PrimaryKeyWriter.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(fn)
	fd, err := fileops.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	fw := newPrimaryKeyWriter(fd, &lockPath)
	defer func() {
		err := recover()
		require.Equal(t, err, "WriteChunkMeta not implement for primaryKeyWriter")
	}()
	fw.WriteChunkMeta(nil)
}

func TestPrimaryKeyWriterChunkMetaSize(t *testing.T) {
	testCompDir := t.TempDir()
	fn := filepath.Join(testCompDir, "PrimaryKeyWriter.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(fn)
	fd, err := fileops.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	fw := newPrimaryKeyWriter(fd, &lockPath)
	defer func() {
		err := recover()
		require.Equal(t, err, "ChunkMetaSize not implement for primaryKeyWriter")
	}()
	fw.ChunkMetaSize()
}

func TestPrimaryKeyWriterAppendChunkMetaToData(t *testing.T) {
	testCompDir := t.TempDir()
	fn := filepath.Join(testCompDir, "PrimaryKeyWriter.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(fn)
	fd, err := fileops.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	fw := newPrimaryKeyWriter(fd, &lockPath)
	defer func() {
		err := recover()
		require.Equal(t, err, "AppendChunkMetaToData not implement for primaryKeyWriter")
	}()
	fw.AppendChunkMetaToData()
}

func TestPrimaryKeyWriterSwitchMetaBuffer(t *testing.T) {
	testCompDir := t.TempDir()
	fn := filepath.Join(testCompDir, "PrimaryKeyWriter.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(fn)
	fd, err := fileops.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	fw := newPrimaryKeyWriter(fd, &lockPath)
	defer func() {
		err := recover()
		require.Equal(t, err, "SwitchMetaBuffer not implement for primaryKeyWriter")
	}()
	fw.SwitchMetaBuffer()
}

func TestPrimaryKeyWriterMetaDataBlocks(t *testing.T) {
	testCompDir := t.TempDir()
	fn := filepath.Join(testCompDir, "PrimaryKeyWriter.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(fn)
	fd, err := fileops.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	fw := newPrimaryKeyWriter(fd, &lockPath)
	defer func() {
		err := recover()
		require.Equal(t, err, "MetaDataBlocks not implement for primaryKeyWriter")
	}()
	fw.MetaDataBlocks(nil)
}
