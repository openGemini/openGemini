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

package fileops

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/request"
	"github.com/stretchr/testify/assert"
)

// _00001.wal
func TestGlob(t *testing.T) {
	tmpDir := t.TempDir()
	files := []string{
		"a000000001-000000001.tsm",
		"a000000002-000000001.tsm",
		"a000000002-000000001.tsm.tmp",
		"a000000002-tsm000001.txt",
		"a000000002-000000tsm",
		"_00001.wal",
		"_00002.wal",
		"_00wal.wal.wal",
		"_00002.wal.init",
		"_00wal.txt",
	}

	for _, file := range files {
		file = path.Join(tmpDir, file)
		f, err := Create(file)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write([]byte(file))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	tsmfile := files[0:2]
	sort.Strings(tsmfile)
	tsmPattern := path.Join(tmpDir, "*.tsm")
	tmpFiles, err := Glob(tsmPattern)
	if err != nil {
		t.Fatalf("Glob(%s) failed: %q", tsmPattern, err.Error())
	}
	if len(tmpFiles) != len(tsmfile) {
		t.Fatalf("Glob(%s) failed", tsmPattern)
	}
	for i := 0; i < len(tmpFiles); i++ {
		if path.Join(tmpDir, tsmfile[i]) != tmpFiles[i] {
			t.Fatalf("Glob(%s) failed: expected(%s) but(%s)", tsmPattern, tsmfile[i], tmpFiles[i])
		}
	}

	walfile := files[5:8]
	sort.Strings(walfile)
	walPattern := path.Join(tmpDir, "_*.wal")
	walFiles, err := Glob(walPattern)
	if err != nil {
		t.Fatalf("Glob(%s) failed: %q", walPattern, err.Error())
	}
	if len(walFiles) != len(walfile) {
		t.Fatalf("Glob(%s) failed", walPattern)
	}
	for i := 0; i < len(walFiles); i++ {
		if path.Join(tmpDir, walfile[i]) != walFiles[i] {
			t.Fatalf("Glob(%s) failed: expected(%s) but(%s)", walPattern, walfile[i], tmpFiles[i])
		}
	}
}

func TestFileInterface(t *testing.T) {
	rootDir := "/tmp/test_vfs"
	defer RemoveAll(rootDir)
	pri := FilePriorityOption(IO_PRIORITY_NORMAL)
	lockFile := FileLockOption("/tmp/test_vfs/lock")

	if err := RemoveAll(rootDir, lockFile); err != nil {
		t.Fatalf("remove dir fail, error:%v", err)
	}

	if err := MkdirAll(rootDir, 0750, lockFile); err != nil {
		t.Fatalf("mkdir dir fail, error:%v", err)
	}

	if err := Mkdir(rootDir+"/test1", 0755, lockFile); err != nil {
		t.Fatalf("mkdir dir fail, error:%v", err)
	}

	if err := Mkdir(rootDir+"/test/test1", 0755, lockFile); err == nil {
		t.Fatalf("Mkdir should be failed and expected err is not nil, but get nil")
	}

	fileName := filepath.Join(rootDir, "file1")
	fd, err := OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0664, lockFile, pri)
	if err != nil {
		t.Fatalf("open file(%v) fail, error:%v", fileName, err)
	}

	buf := make([]byte, 1024)
	buf[0] = 11
	buf[1023] = 128
	n, err := fd.Write(buf)
	if err != nil || n != 1024 {
		t.Fatalf("write file(%v) fail, writed:%v, error:%v", fileName, n, err)
	}

	if err = fd.Sync(); err != nil {
		t.Fatalf("sync file(%v) fail, error:%v", fileName, err)
	}
	if err = fd.SyncUpdateLength(); err != nil {
		t.Fatalf("SyncUpdateLength file(%v) fail, error:%v", fileName, err)
	}
	if fdN := fd.Fd(); fdN == 0 {
		t.Fatalf("Fd file(%v) fail, Fd:%v", fileName, fdN)
	}

	fi, err := fd.Stat()
	if err != nil {
		t.Fatalf("stat file(%v) fail, error:%v", fileName, err)
	}
	if fi.Size() != int64(len(buf)) {
		t.Fatalf("stat file(%v) fail, size(%v) !=%v", fileName, fi.Size(), len(buf))
	}

	where, err := fd.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("seek file(%v) fail, error:%v", fileName, err)
	}
	if where != 0 {
		t.Fatalf("seek file(%v) fail, where(%v)!=0", fileName, where)
	}

	readBuf := make([]byte, len(buf)+64)
	n, err = fd.Read(readBuf)
	if err != nil || n != len(buf) {
		t.Fatalf("read file(%v) fail, readSize(%v) != %v", fileName, n, len(buf))
	}
	if readBuf[0] != 11 || readBuf[1023] != 128 {
		t.Fatalf("read file(%v) fail, invalid read content", fileName)
	}

	n, err = fd.ReadAt(readBuf, fi.Size()-8)
	if err != io.EOF || n != 8 || readBuf[7] != 128 {
		t.Fatalf("read file(%v) fail, err(%v) != io.EOF", fileName, err)
	}

	if err = fd.Truncate(32); err != nil {
		t.Fatalf("Truncate file(%v) fail, error:%v", fileName, err)
	}
	fi, err = fd.Stat()
	if err != nil || fi.Size() != 32 {
		t.Fatalf("stat file(%v) fail, error:%v", fileName, err)
	}

	if err = fd.Truncate(int64(len(buf)) + 32); err != nil {
		t.Fatalf("Truncate file(%v) fail, error:%v", fileName, err)
	}
	fi, err = fd.Stat()
	if err != nil || fi.Size() != int64(len(buf))+32 {
		t.Fatalf("stat file(%v) fail, error:%v", fileName, err)
	}
	if err = fd.Close(); err != nil {
		t.Fatalf("close file(%v) fail. %v", fileName, err)
	}

	err = Truncate(fileName, 2048)
	if err != nil {
		t.Fatalf("Truncate file(%v) fail, error:%v", fileName, err)
	}

	fd, err = Open(fileName, lockFile)
	if err != nil {
		t.Fatalf("open file(%v) fail, err:%v", fileName, err)
	}
	fi, err = Stat(fileName)
	if err != nil || fi.Size() != 2048 {
		t.Fatalf("Stat(%v) fail, err:%v", fileName, err)
	}
	_ = fd.Close()
}

func TestVFS(t *testing.T) {
	rootDir := "/tmp/test_vfs"
	defer RemoveAll(rootDir)
	pri := FilePriorityOption(IO_PRIORITY_NORMAL)
	lockFile := FileLockOption("/tmp/test_vfs/lock")

	_ = MkdirAll(rootDir, 0750, lockFile)
	fileName := filepath.Join(rootDir, "file1")

	fd, err := Create(fileName, lockFile, pri)
	if err != nil {
		t.Fatalf("create(%v) fail, err:%v", fileName, err)
	}

	st := time.Now()
	n, _ := fd.Write([]byte("123"))
	if n != 3 {
		t.Fatalf("write(%v) fail, err:%v", fileName, err)
	}

	if err = fd.Sync(); err != nil {
		t.Fatalf("sync(%v) fail, err:%v", fileName, err)
	}

	fi, err := fd.Stat()
	if err != nil {
		t.Fatalf("stat(%v) fail, err:%v", fileName, err)
	}

	if fd.Name() != fileName {
		t.Fatalf("fd.Name() fail, exp:%v, get:%v", fileName, fd.Name())
	}

	mt := fi.ModTime()
	ct, err := CreateTime(fileName)
	if err != nil {
		t.Fatalf("CreateTime(%v) fail, err:%v", fileName, err)
	}

	st = st.Truncate(time.Second)
	mt = mt.Truncate(time.Second)
	cct := ct.Truncate(time.Second)
	if mt.Sub(st) > time.Second || cct.Sub(st) > time.Second {
		t.Fatalf("get create time fail, file(%v), start_time:%v, modify_time:%v, create_time:%v",
			fileName, st.String(), mt.String(), cct.String())
	}

	if err := fd.Close(); err != nil {
		t.Fatalf("close file(%v) fail, err:%v", fileName, err)
	}
	if err = Remove(fileName, lockFile); err != nil {
		t.Fatalf("close Remove(%v) fail, err:%v", fileName, err)
	}
	if _, err = Stat(fileName); err == nil {
		t.Fatalf("file(%v) should not exist", fileName)
	}
}

func TestVFS1(t *testing.T) {
	rootDir := "/tmp/test_vfs"
	defer RemoveAll(rootDir)
	pri := FilePriorityOption(IO_PRIORITY_NORMAL)
	lockFile := FileLockOption("/tmp/test_vfs/lock")

	if err := MkdirAll(rootDir, 0750, lockFile); err != nil {
		t.Fatalf("mkdir(%v) fail, err:%v", rootDir, err)
	}
	fileName := filepath.Join(rootDir, "file1")

	dir1 := filepath.Join(rootDir, "dir1")
	dir2 := filepath.Join(rootDir, "dir2")
	if err := MkdirAll(dir1, 0750, lockFile); err != nil {
		t.Fatalf("mkdir(%v) fail, err:%v", rootDir, err)
	}
	if err := MkdirAll(dir2, 0750, lockFile); err != nil {
		t.Fatalf("mkdir(%v) fail, err:%v", rootDir, err)
	}

	str := "0123456789"
	if err := WriteFile(fileName, []byte(str), 0644, lockFile, pri); err != nil {
		t.Fatalf("writefile(%v) fail, err:%v", fileName, err)
	}
	rb, err := ReadFile(fileName, pri)
	if err != nil {
		t.Fatalf("readfile(%v) fail, err:%v", fileName, err)
	}
	if str != string(rb) {
		t.Fatalf("readfile(%v) fail, exp:%v, get:%v", fileName, str, string(rb))
	}

	fileName2 := filepath.Join(rootDir, "file2")
	if n, err := CopyFile(fileName, fileName2, lockFile, pri); err != nil || int(n) != len(str) {
		t.Fatalf("copyfile(%v => %v) fail, err:%v, copyed:%v", fileName, fileName2, err, n)
	}
	rb, err = ReadFile(fileName2, pri)
	if err != nil {
		t.Fatalf("readfile(%v) fail, err:%v", fileName2, err)
	}
	if str != string(rb) {
		t.Fatalf("readfile(%v) fail, exp:%v, get:%v", fileName2, str, string(rb))
	}

	fileName3 := filepath.Join(rootDir, "file3")
	if err = RenameFile(fileName2, fileName3, lockFile); err != nil {
		t.Fatalf("renamefile(%v => %v) fail, err:%v", fileName2, fileName3, err)
	}

	dirs, err := ReadDir(rootDir)
	if err != nil {
		t.Fatalf("readdir(%v) fail, err:%v", rootDir, err)
	}
	names := []string{"dir1", "dir2", "file1", "file3"}
	sort.Strings(names)
	if len(dirs) != len(names) {
		t.Fatalf("readdir(%v) fail, exp:%v, get:%v", rootDir, names, dirs)
	}
	for i, d := range dirs {
		if names[i] != d.Name() {
			t.Fatalf("readdir(%v) fail, exp:%v, get:%v", rootDir, names, dirs)
		}
	}
}

func TestMMap(t *testing.T) {
	fp, err := os.OpenFile(t.TempDir()+"/test_mmap.data", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
	if !assert.NoError(t, err) {
		return
	}
	size := 32
	data, err := Mmap(int(fp.Fd()), 0, size)
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, MUnmap(data)) {
		return
	}

	data, err = Mmap(0, -1, 0)
	if !assert.Error(t, err) {
		return
	}

	if !assert.NoError(t, MUnmap(data)) {
		return
	}
}

func TestFdatasync(t *testing.T) {
	fp, err := os.OpenFile(t.TempDir()+"/data", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
	if !assert.NoError(t, err) {
		return
	}

	_, err = fp.Write([]byte("test"))
	if !assert.NoError(t, err) {
		return
	}

	f := &file{of: fp}
	defer f.Close()
	err = Fdatasync(f)
	if !assert.NoError(t, err) {
		return
	}
}

func TestFadvise(t *testing.T) {
	err := Fadvise(111, 0, 0, 0)
	if !assert.Error(t, err) {
		return
	}
}

func TestStreamRead(t *testing.T) {
	fp, err := os.OpenFile(t.TempDir()+"/data", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
	if !assert.NoError(t, err) {
		return
	}

	_, err = fp.Write([]byte("test"))
	if !assert.NoError(t, err) {
		return
	}

	f := &file{of: fp}
	defer f.Close()
	c := make(chan *request.StreamReader)
	go f.StreamReadBatch([]int64{0}, []int64{2}, 1, c, 1)
	for {
		select {
		case r, ok := <-c:
			if !ok {
				return
			}
			assert.Equal(t, "te", string(r.Content))
		}
	}
}
