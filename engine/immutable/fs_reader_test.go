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

package immutable

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/fileops"
)

func TestFsReader_Read(t *testing.T) {
	fn := "/tmp/test_diskreader.data"
	_ = fileops.Remove(fn)
	defer fileops.Remove(fn)
	var buf [4096]byte

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fd.Write(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	dr := NewDiskFileReader(fd, &lockPath)
	defer dr.Close()

	rb, err := dr.ReadAt(0, 0, nil)
	if err != nil || len(rb) > 0 {
		t.Fatalf("no data shoul be read, but read some")
	}

	_, err = dr.ReadAt(int64(len(buf)+1), 10, nil)
	if err == nil || !strings.Contains(err.Error(), "table store read file failed") {
		t.Fatalf("invalid error: %v", err)
	}

	rb, _ = dr.ReadAt(10, 20, nil)
	if len(rb) != 20 {
		t.Fatalf("read file fail")
	}

	dst := make([]byte, 64)
	dst[0] = 255
	dst[32] = 255
	dst[63] = 255

	rb, _ = dr.ReadAt(0, uint32(len(dst)), &dst)
	if bytes.Compare(rb, buf[:64]) != 0 {
		t.Fatalf("read file fail")
	}

	_ = fileops.MUnmap(dr.mmapData)
	dr.mmapData = nil
	dst[0] = 255
	dst[32] = 255
	dst[63] = 255
	rb, _ = dr.ReadAt(0, uint32(len(dst)), &dst)
	if bytes.Compare(rb, buf[:64]) != 0 {
		t.Fatalf("read file fail")
	}

	dst[0] = 255
	rb, _ = dr.ReadAt(4096-64+1, uint32(len(dst))+10, &dst)
	if len(rb) != 63 && bytes.Compare(rb, buf[:63]) != 0 {
		t.Fatalf("read file fail")
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
	dr := NewDiskFileReader(fd, &lockPath)
	defer dr.Close()

	err := dr.Rename("/tmp/name.new")
	if err == nil || !strings.Contains(err.Error(), "table store rename file failed") {
		t.Fatalf("test rename error fail")
	}
}

func TestFsReader_Rename_With_FileHandle_Optimize(t *testing.T) {
	fn := "/tmp/test_diskreader.data"
	_ = fileops.Remove(fn)
	defer fileops.Remove(fn)
	var buf [4096]byte

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fd.Write(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	dr := NewDiskFileReader(fd, &lockPath)

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

type mockFile struct {
	CloseFn            func() error
	ReadFn             func(p []byte) (n int, err error)
	SeekFn             func(offset int64, whence int) (int64, error)
	WriteFn            func(p []byte) (n int, err error)
	ReadAtFn           func(p []byte, off int64) (n int, err error)
	NameFn             func() string
	TruncateFn         func(size int64) error
	SyncFn             func() error
	StatFn             func() (os.FileInfo, error)
	SyncUpdateLengthFn func() error
	FdFn               func() uintptr
}

func (f *mockFile) Close() error                                  { return f.CloseFn() }
func (f *mockFile) Read(p []byte) (n int, err error)              { return f.ReadFn(p) }
func (f *mockFile) Seek(offset int64, whence int) (int64, error)  { return f.SeekFn(offset, whence) }
func (f *mockFile) Write(p []byte) (n int, err error)             { return f.WriteFn(p) }
func (f *mockFile) ReadAt(p []byte, off int64) (n int, err error) { return f.ReadAtFn(p, off) }
func (f *mockFile) Name() string                                  { return f.NameFn() }
func (f *mockFile) Truncate(size int64) error                     { return f.TruncateFn(size) }
func (f *mockFile) Stat() (os.FileInfo, error)                    { return f.StatFn() }
func (f *mockFile) Sync() error                                   { return f.SyncFn() }
func (f *mockFile) SyncUpdateLength() error                       { return f.SyncUpdateLengthFn() }
func (f *mockFile) Fd() uintptr                                   { return f.FdFn() }

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
