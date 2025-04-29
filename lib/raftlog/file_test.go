/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type badSeeker struct {
	fileops.File
}

func (badSeeker) Name() string {
	return "test file"
}

func (badSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("mock seek error")
}

type badWriter struct {
	badSeeker
}

func (badWriter) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (badWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("mock write error")
}

func TestFileWrap_OpenFile(t *testing.T) {
	tmpDir := t.TempDir()
	t.Run("OpenFile", func(t *testing.T) {
		fw, err := OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 1000)
		require.EqualError(t, err, NewFile.Error())
		defer fw.Close()

		assert.Contains(t, fw.Name(), "test")
		assert.Equal(t, 1000, fw.Size())
	})

	t.Run("OpenFile_twice", func(t *testing.T) {
		fw, err := OpenFile(filepath.Join(tmpDir, "test2"), os.O_CREATE|os.O_RDWR, 1000)
		require.EqualError(t, err, NewFile.Error())
		defer fw.Close()

		assert.Contains(t, fw.Name(), "test")

		fw2, err := OpenFile(filepath.Join(tmpDir, "test2"), os.O_CREATE|os.O_RDWR, 1000)
		require.NoError(t, err)
		defer fw2.Close()

		assert.Contains(t, fw2.Name(), "test")
		assert.Equal(t, 1000, fw2.Size())

		fw3 := &FileWrap{}
		require.Equal(t, "", fw3.Name())
	})
}

func TestGetEntryData(t *testing.T) {
	fw := FileWrap{data: []byte("Hello, World!"), current: true}

	if result := fw.GetEntryData(0, 5); string(result) != "Hello" {
		t.Errorf("Expected 'Hello', got %s", result)
	}

	tmpDir := t.TempDir()
	file, err := fileops.OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 0640)
	file.Close()
	require.NoError(t, err)
	fw.fd = file
	fw.current = false
	if result := fw.GetEntryData(0, 5); result != nil {
		t.Errorf("Expected nil result")
	}
}

func TestFileWrap_Write_WriteAt(t *testing.T) {
	tmpDir := t.TempDir()
	t.Run("WriteAt_Success", func(t *testing.T) {
		file, err := fileops.OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 0640)
		require.NoError(t, err)
		file.Write([]byte("writeahead"))

		fw := &FileWrap{fd: file, current: true}
		defer fw.Close()
		fw.data = []byte("writeahead")

		n, err := fw.WriteAt(0, []byte("hello"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, []byte("hello"), fw.data[:5])
		var buff = make([]byte, 10)
		n, err = file.ReadAt(buff, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("helloahead"), buff)
		require.Equal(t, []byte("helloahead"), fw.data)

		n, err = fw.Write([]byte("world"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		buff = make([]byte, 15)
		n, err = file.ReadAt(buff, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("helloaheadworld"), buff)
		require.Equal(t, []byte("helloaheadworld"), fw.data)

		n, err = fw.WriteAt(5, []byte("faker"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		buff = make([]byte, 15)
		n, err = file.ReadAt(buff, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("hellofakerworld"), buff)
		require.Equal(t, []byte("hellofakerworld"), fw.data)
	})

	t.Run("WriteAt_SeekError", func(t *testing.T) {
		fw := &FileWrap{fd: &badSeeker{}, data: make([]byte, 10)}

		_, err := fw.WriteAt(0, []byte("hello"))
		require.EqualError(t, err, "seek unreachable file:test file: mock seek error")
	})

	t.Run("Write_badWriter", func(t *testing.T) {
		fw := &FileWrap{fd: &badWriter{}, data: make([]byte, 10)}
		_, err := fw.Write([]byte("hello"))
		require.EqualError(t, err, "write failed:test file: mock write error")
	})

	t.Run("WriteAt_badWriter", func(t *testing.T) {
		fw := &FileWrap{fd: &badWriter{}, data: make([]byte, 10)}
		_, err := fw.WriteAt(0, []byte("hello"))
		require.EqualError(t, err, "write failed for file:test file: mock write error")
	})
}

func TestFileWrap_TrySync(t *testing.T) {
	t.Run("TrySync_fd_is_nil", func(t *testing.T) {
		fw := &FileWrap{}

		require.NoError(t, fw.TrySync())
	})
}

func TestFileWrap_WriteSlice_ReadSlice(t *testing.T) {
	tmpDir := t.TempDir()
	t.Run("WriteSlice_SeekError", func(t *testing.T) {
		ef := &FileWrap{fd: &badSeeker{}}
		ef.data = make([]byte, 10)

		err := ef.WriteSlice(0, []byte("hello"))
		require.EqualError(t, err, "seek unreachable file:test file: mock seek error")
	})

	t.Run("WriteSlice_WriteError", func(t *testing.T) {
		ef := &FileWrap{fd: &badWriter{}}
		ef.data = make([]byte, 10)

		err := ef.WriteSlice(0, []byte("hello"))
		require.EqualError(t, err, "write failed for file:test file: mock write error")
	})

	t.Run("WriteSlice_ReadSlice_Success", func(t *testing.T) {
		file, err := fileops.OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 0640)
		require.NoError(t, err)

		fw := &FileWrap{fd: file, data: make([]byte, 0), current: true}
		defer fw.Close()
		err = fw.WriteSlice(0, []byte("hello"))
		require.NoError(t, err)
		var buff = make([]byte, 9)
		n, err := file.ReadAt(buff, 0)
		require.NoError(t, err)
		require.Equal(t, 9, n)
		require.Contains(t, string(buff), "hello")
		require.Equal(t, []byte{0, 0, 0, 5, 104, 101, 108, 108, 111}, fw.data[:9]) // size+"hello"

		data := fw.ReadSlice(0)
		require.Contains(t, string(buff), "hello")
		require.Equal(t, []byte{104, 101, 108, 108, 111}, data) // "hello"
		require.Equal(t, data, fw.data[4:9])                    // "hello"

		size := fw.SliceSize(0)
		require.Equal(t, 9, size) // 4 + len("hello")
	})
}

func TestTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	t.Run("truncate a file", func(t *testing.T) {
		file, err := fileops.OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 0640)
		require.NoError(t, err)

		fw := &FileWrap{fd: file, data: make([]byte, 0)}
		defer fw.Close()
		err = fw.WriteSlice(0, []byte("Hello, world! GoodBye!!!"))
		require.NoError(t, err)

		err = fw.Truncate(5)
		require.NoError(t, err)

		fi, err := file.Stat()
		require.NoError(t, err)

		require.Equal(t, 5, int(fi.Size()))
	})
}

type badCloser struct {
	fileops.File
}

func (badCloser) Name() string {
	return "mock file"
}

func (badCloser) Truncate(size int64) error {
	return nil
}

func (badCloser) Close() error {
	return fmt.Errorf("test close error")
}

func (badCloser) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("seek error")
}

func (badCloser) Size() (int64, error) {
	return 0, fmt.Errorf("size error")
}
func TestFileWrap_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	t.Run("Delete a file successfully", func(t *testing.T) {
		file, err := fileops.OpenFile(filepath.Join(tmpDir, "test"), os.O_CREATE|os.O_RDWR, 0640)
		require.NoError(t, err)

		fw := &FileWrap{fd: file}
		err = fw.Delete()
		require.NoError(t, err)
	})

	t.Run("Delete an empty fd", func(t *testing.T) {
		fw := &FileWrap{fd: nil}
		err := fw.Delete()
		require.NoError(t, err)
	})

	t.Run("Delete a file close failed", func(t *testing.T) {
		fw := &FileWrap{fd: &badCloser{}}
		err := fw.Delete()
		require.EqualError(t, err, "while close file:mock file: test close error")
	})
}

func TestFileWrap_GetContentErr(t *testing.T) {
	fw := &FileWrap{fd: &badCloser{}}
	fw.setCurrent()
	assert.Equal(t, 0, fw.Size())
	assert.NotEqual(t, nil, fw.ReadSlice(0))
	assert.Equal(t, 0, fw.SliceSize(0))
}
