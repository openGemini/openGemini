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

package fileops

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/request"
)

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

func (f *mockFile) Close() error                                                                   { return f.CloseFn() }
func (f *mockFile) Read(p []byte) (n int, err error)                                               { return f.ReadFn(p) }
func (f *mockFile) Seek(offset int64, whence int) (int64, error)                                   { return f.SeekFn(offset, whence) }
func (f *mockFile) Write(p []byte) (n int, err error)                                              { return f.WriteFn(p) }
func (f *mockFile) ReadAt(p []byte, off int64) (n int, err error)                                  { return f.ReadAtFn(p, off) }
func (f *mockFile) Name() string                                                                   { return f.NameFn() }
func (f *mockFile) Truncate(size int64) error                                                      { return f.TruncateFn(size) }
func (f *mockFile) Stat() (os.FileInfo, error)                                                     { return f.StatFn() }
func (f *mockFile) Sync() error                                                                    { return f.SyncFn() }
func (f *mockFile) SyncUpdateLength() error                                                        { return f.SyncUpdateLengthFn() }
func (f *mockFile) Fd() uintptr                                                                    { return f.FdFn() }
func (f *mockFile) Size() (int64, error)                                                           { return 0, nil }
func (f *mockFile) StreamReadBatch([]int64, []int64, int64, chan *request.StreamReader, int, bool) {}
func TestDiskWriter(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "TestDiskWriter")
	_ = Remove(dir)
	defer func() {
		_ = RemoveAll(dir)
	}()
	_ = MkdirAll(dir, 0750)

	fd := &mockFile{
		NameFn: func() string {
			return name
		},
		CloseFn: func() error {
			return nil
		},
		WriteFn: func(p []byte) (n int, err error) {
			return 0, fmt.Errorf("write fail")
		},
	}

	var buf [128 * 1024]byte
	lockPath := ""
	dr := NewFileWriter(fd, 1024, &lockPath)
	if _, err := dr.Write(buf[:32]); err != nil {
		t.Fatal(err)
	}

	// write fail
	if _, err := dr.Write(buf[:]); err == nil {
		t.Fatalf("diskwrite should be wirte fail")
	}

	// flush fail
	to := &bytes.Buffer{}
	_, err := dr.CopyTo(to)
	if err == nil {
		t.Fatalf("diskwrite should be copy fail")
	}

	// close fail
	fd.WriteFn = func(p []byte) (n int, err error) { return len(p), nil }
	fd.CloseFn = func() error { return fmt.Errorf("close fail") }
	dr.GetWriter().Reset(fd)
	_, _ = dr.Write(buf[:])
	if _, err = dr.CopyTo(to); err == nil || !strings.Contains(err.Error(), "close fail") {
		t.Fatalf("diskwriter should be cloase fail")
	}

	// open fail
	fd.WriteFn = func(p []byte) (n int, err error) { return len(p), nil }
	fd.CloseFn = func() error { return nil }
	dr.GetWriter().Reset(fd)
	_, _ = dr.Write(buf[:])
	if _, err = dr.CopyTo(to); err == nil {
		t.Fatalf("diskwriter should be open fail")
	}

	// copy fail
	fd1, err := OpenFile(name, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fd1.Write(buf[:])
	_ = fd1.Close()

	fd.WriteFn = func(p []byte) (n int, err error) { return len(p), nil }
	fd.CloseFn = func() error { return nil }
	dr.Reset(fd)
	_, _ = dr.Write(buf[:])
	if _, err = dr.CopyTo(to); err != nil {
		t.Fatalf("diskwriter should be open fail")
	}
}
