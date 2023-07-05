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
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	// write primaryKey
	testCompDir := t.TempDir()
	filePath := filepath.Join(testCompDir, "fileReader.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(filePath)
	data := genData(100)
	lockPath := ""
	indexBuilder := NewIndexBuilder(&lockPath, filePath)
	err := indexBuilder.WriteData(data)
	if err != nil {
		t.Fatal("write data error")
	}
	indexBuilder.writer.Close() // sync file

	// read primaryKey
	fileops.EnableMmapRead(false)
	cfr, err := NewPrimaryKeyReader(filePath, &lockPath)
	cfr.Open()
	if err != nil {
		t.Fatal("read data error")
	}
	dst := make([]byte, 1000000)
	_, err = cfr.Read(0, 1000000, &dst)
	require.ErrorContains(t, err, "short read")
	dst = nil
	_, err = cfr.Read(-1, 100, &dst)
	require.ErrorContains(t, err, "invalid read offset")

	retData, _ := cfr.ReadData()
	require.EqualValues(t, data.Schema, retData.Schema)
	require.EqualValues(t, data.RecMeta, retData.RecMeta)
	for i := range data.ColVals {
		require.EqualValues(t, data.ColVals[i].Val, retData.ColVals[i].Val)

	}
	err = cfr.Close()
	if err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, cfr.Version(), version)
}

func TestFileNameError(t *testing.T) {
	testCompDir := t.TempDir()
	filePath := filepath.Join(testCompDir, "FileNameError.test")
	lockPath := ""
	_, err := NewPrimaryKeyReader(filePath, &lockPath)
	require.ErrorContains(t, err, "no such file or directory")
	require.ErrorContains(t, err, "stat")
}

func TestFileSizeError(t *testing.T) {
	data := []byte("hello")
	testCompDir := t.TempDir()
	filePath := filepath.Join(testCompDir, "FileSizeError.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(filePath)
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatal("writer data error")
	}
	f.Write(data)
	f.Sync()
	defer f.Close()
	lockPath := ""
	_, err = NewPrimaryKeyReader(filePath, &lockPath)
	require.ErrorContains(t, err, "invalid file")
	require.ErrorContains(t, err, "size")
}

func TestFileMagicError(t *testing.T) {
	data := []byte("hello11111111111111111111111")
	testCompDir := t.TempDir()
	filePath := filepath.Join(testCompDir, "FileMagicError.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(filePath)
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatal("writer data error")
	}
	f.Write(data)
	f.Sync()
	defer f.Close()
	lockPath := ""
	_, err = NewPrimaryKeyReader(filePath, &lockPath)
	require.ErrorContains(t, err, "invalid file")
	require.ErrorContains(t, err, "magic")
}

func TestFileHeaderError(t *testing.T) {
	data := []byte("COLX111")
	testCompDir := t.TempDir()
	filePath := filepath.Join(testCompDir, "FileHeaderError.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(filePath)
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatal("writer data error")
	}
	f.Write(data)
	f.Sync()
	defer f.Close()
	lockPath := ""
	_, err = NewPrimaryKeyReader(filePath, &lockPath)
	require.ErrorContains(t, err, "invalid file")
	require.ErrorContains(t, err, "FileHeaderError")
}

func TestAppendColumnDataPanic(t *testing.T) {
	dataType := influx.Field_Type_Unknown
	defer func() {
		err := recover()
		require.Equal(t, err, "invalid column type 0")
	}()
	appendColumnData(dataType, nil, nil, nil)
}

func TestDecodeColumnDataPanic(t *testing.T) {
	data := []byte{0}
	ref := record.Field{
		Name: "haha",
		Type: 1,
	}
	defer func() {
		err := recover()
		require.Equal(t, err, "type(0) in table not eq select type(1)")
	}()
	decodeColumnData(&ref, data, nil, nil)
}
