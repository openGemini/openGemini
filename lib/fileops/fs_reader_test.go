// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package fileops

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/require"
)

type FakeInfo struct {
	NameFn    func() string      // base name of the file
	SizeFn    func() int64       // length in bytes for regular files; system-dependent for others
	ModeFn    func() os.FileMode // file mode bits
	ModTimeFn func() time.Time   // modification time
	IsDirFn   func() bool        // abbreviation for Mode().IsDir()
	SysFn     func() interface{} // underlying data source (can return nil)
}

func (f *FakeInfo) Name() string       { return f.NameFn() }
func (f *FakeInfo) Size() int64        { return f.SizeFn() }
func (f *FakeInfo) Mode() os.FileMode  { return f.ModeFn() }
func (f *FakeInfo) ModTime() time.Time { return f.ModTimeFn() }
func (f *FakeInfo) IsDir() bool        { return f.IsDirFn() }
func (f *FakeInfo) Sys() interface{}   { return nil }

func TestFsReader_ReOpen_EnableMmap(t *testing.T) {
	fn := t.TempDir() + "/test_diskreader.data"
	EnableMmapRead(true)
	defer EnableMmapRead(false)

	lockPath := ""
	fd, err := OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	require.NoError(t, err)

	dr := NewFileReader(fd, &lockPath)
	defer dr.Close()

	err = dr.ReOpen()
	require.NoError(t, err)
}

func TestFsReader_Read_EnableMmap(t *testing.T) {
	fn := t.TempDir() + "/test_diskreader.data"
	var buf [4096]byte

	EnableMmapRead(true)
	defer EnableMmapRead(false)

	fd, err := OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	require.NoError(t, err)

	_, err = fd.Write(buf[:])
	require.NoError(t, err)
	require.NoError(t, fd.Sync())

	lockPath := ""
	dr := NewFileReader(fd, &lockPath)
	defer dr.Close()

	rb, err := dr.ReadAt(0, 0, nil, IO_PRIORITY_ULTRA_HIGH)
	require.NoError(t, err)
	require.Equal(t, 0, len(rb), "no data shoul be read, but read some")

	_, err = dr.ReadAt(int64(len(buf)+1), 10, nil, IO_PRIORITY_ULTRA_HIGH)
	if err == nil || !strings.Contains(err.Error(), "table store read file failed") {
		t.Fatalf("invalid error: %v", err)
	}

	dr.fileSize += 100
	_, err = dr.ReadAt(int64(len(buf)+1), 10, nil, IO_PRIORITY_ULTRA_HIGH)
	require.NotEmpty(t, err)
	dr.fileSize -= 100

	rb, _ = dr.ReadAt(10, 20, &([]byte{}), IO_PRIORITY_ULTRA_HIGH)
	require.Equal(t, 20, len(rb), "read file fail")

	dst := make([]byte, 64)
	dst[0] = 255
	dst[32] = 255
	dst[63] = 255

	rb, _ = dr.ReadAt(0, uint32(len(dst)), &dst, IO_PRIORITY_ULTRA_HIGH)
	if bytes.Compare(rb, buf[:64]) != 0 {
		t.Fatalf("read file fail")
	}

	_ = MUnmap(dr.mmapData)
	dr.mmapData = nil
	dst[0] = 255
	dst[32] = 255
	dst[63] = 255
	rb, _ = dr.ReadAt(0, uint32(len(dst)), &dst, IO_PRIORITY_ULTRA_HIGH)
	if bytes.Compare(rb, buf[:64]) != 0 {
		t.Fatalf("read file fail")
	}

	_, err = dr.ReadAt(4096-64+1, uint32(len(dst))+10, &dst, IO_PRIORITY_ULTRA_HIGH)
	require.True(t, errno.Equal(err, errno.ShortRead))
}

func TestFsReader_Rename_With_FileHandle_Optimize(t *testing.T) {
	fn := "/tmp/test_diskreader.data"
	_ = Remove(fn)
	defer func() {
		_ = Remove(fn)
	}()
	var buf [4096]byte

	fd, err := OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fd.Write(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	dr := NewFileReader(fd, &lockPath)

	err = dr.Rename("/tmp/test_diskreadernew.data")
	if err != nil || dr.fd == nil {
		t.Fatalf("test rename error fail")
	}

	err = dr.FreeFileHandle()
	if err != nil {
		t.Fatalf("FreeFileHandle error fail")
	}

	err = dr.Rename("/tmp/test_diskreadern.data")
	if err != nil {
		t.Fatalf("test rename error fail")
	}
}

func TestFsReader_Rename(t *testing.T) {
	fi := &FakeInfo{
		NameFn: func() string {
			return "/tmp/name"
		},
		SizeFn: func() int64 {
			return 4096
		},
	}
	fd := &mockFile{
		StatFn: func() (os.FileInfo, error) {
			return fi, nil
		},
		FdFn: func() uintptr {
			return 0
		},
		NameFn: func() string {
			return "/tmp/name"
		},
		CloseFn: func() error {
			return nil
		},
	}
	lockPath := ""
	dr := NewFileReader(fd, &lockPath)
	defer dr.Close()

	err := dr.Rename("/tmp/name.new")
	if err == nil || !strings.Contains(err.Error(), "table store rename file failed") {
		t.Fatalf("test rename error fail")
	}
}

func TestFsReader_RenameFileToOBS(t *testing.T) {
	fn := "/tmp/test_diskreader.data"
	_ = Remove(fn)
	defer func() {
		_ = Remove(fn)
	}()
	var buf [10]byte

	fd, err := OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fd.Write(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	MmapEn = true
	dr := NewFileReader(fd, &lockPath)
	defer func() {
		MmapEn = false
		dr.Close()
	}()
	err = dr.RenameOnObs("/tmp/test_diskreadernew.data", true, nil)
	if err == nil || !strings.Contains(err.Error(), "table store rename file failed") {
		t.Fatalf("test rename error fail")
	}
}

func TestFsReader_RenameTmpFileToOBS(t *testing.T) {
	fn := "/tmp/test_diskreader_tmp.data"
	_ = Remove(fn)
	defer func() {
		_ = Remove(fn)
	}()
	var buf [10]byte

	fd, err := OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fd.Write(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	MmapEn = true
	dr := NewFileReader(fd, &lockPath)
	defer func() {
		MmapEn = false
		dr.Close()
	}()
	err = dr.RenameOnObs("/tmp/test_diskreadernew.data", true, nil)
	if err == nil || !strings.Contains(err.Error(), "table store rename file failed") {
		t.Fatalf("test rename error fail")
	}
}
