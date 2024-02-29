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
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/request"
	"github.com/stretchr/testify/assert"
)

const defaultSecureValue = "*****"

var (
	ak         = defaultSecureValue
	sk         = defaultSecureValue
	endpoint   string
	bucketName string
	testObsFs  = &obsFs{}
)

func initEnvParam(v *string, envName string) {
	if value, ok := os.LookupEnv(envName); ok {
		*v = value
	}
}

func initializeEnv(t *testing.T) {
	initEnvParam(&ak, "AK")
	initEnvParam(&sk, "SK")
	initEnvParam(&endpoint, "ENDPOINT")
	initEnvParam(&bucketName, "BUCKET_NAME")
	if ak == defaultSecureValue || sk == defaultSecureValue {
		t.Log("Skip tests, please set AK/SK in environment variable")
		t.Skip()
	}
}

func TestObsFs_OpenFile(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_fs_open.txt", ak, sk)
	fd, err := testObsFs.OpenFile(name, os.O_CREATE, os.ModePerm)
	assert.Nil(t, err)
	assert.NotNil(t, fd)
	_, _ = fd.Write([]byte("hello"))
	_ = fd.Close()
	fd, err = testObsFs.Open(name)
	assert.Nil(t, err)
	info, _ := fd.Stat()
	assert.Equal(t, int64(5), info.Size())
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFs_Create(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_fs_create.txt", ak, sk)
	fd, err := testObsFs.Create(name)
	assert.Nil(t, err)
	assert.NotNil(t, fd)
	_, _ = fd.Write([]byte("hello"))
	_ = fd.Close()
	fd, err = testObsFs.CreateV1(name)
	assert.Nil(t, err)
	info, _ := fd.Stat()
	assert.Equal(t, int64(0), info.Size())
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFs_WriteFile(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_fs_write.txt", ak, sk)
	content := []byte("hello")
	err := testObsFs.WriteFile(name, content, os.ModePerm)
	assert.Nil(t, err)
	info, _ := testObsFs.Stat(name)
	assert.Equal(t, int64(len(content)), info.Size())
	content1 := []byte("test")
	err = testObsFs.WriteFile(name, content1, os.ModePerm)
	assert.Nil(t, err)
	info, _ = testObsFs.Stat(name)
	assert.Equal(t, int64(len(content1)), info.Size())
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFs_ReadFile(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_fs_read.txt", ak, sk)
	content := []byte("hello")
	err := testObsFs.WriteFile(name, content, os.ModePerm)
	assert.Nil(t, err)
	read, err := testObsFs.ReadFile(name)
	assert.Nil(t, err)
	assert.Equal(t, content, read)
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFs_CopyFile(t *testing.T) {
	initializeEnv(t)
	srcFile := EncodeObsPath(endpoint, bucketName, "test_obs_fs_copy.txt", ak, sk)
	dstFile := EncodeObsPath(endpoint, bucketName, "test_obs_fs_copy_bak.txt", ak, sk)
	content := []byte("hello")
	err := testObsFs.WriteFile(srcFile, content, os.ModePerm)
	assert.Nil(t, err)
	n, err := testObsFs.CopyFile(srcFile, dstFile)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(content)), n)
	read, err := testObsFs.ReadFile(dstFile)
	assert.Nil(t, err)
	assert.Equal(t, content, read)
	t.Cleanup(func() {
		_ = testObsFs.Remove(srcFile)
		_ = testObsFs.Remove(dstFile)
	})
}

func TestObsFs_Truncate(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_fs_truncate.txt", ak, sk)
	content := []byte("hello world")
	err := testObsFs.WriteFile(name, content, os.ModePerm)
	assert.Nil(t, err)
	err = testObsFs.Truncate(name, 5)
	assert.Nil(t, err)
	read, err := testObsFs.ReadFile(name)
	assert.Nil(t, err)
	assert.Equal(t, content[:5], read)
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFs_NormalizeDirPath(t *testing.T) {
	assert.Equal(t, "a/b/c/d/", testObsFs.NormalizeDirPath("/a/b/c/d/"))
	assert.Equal(t, "a/b/c/d/", testObsFs.NormalizeDirPath("a/b//c/.//d"))
}

func TestObsFs_MkdirAll(t *testing.T) {
	initializeEnv(t)
	dirname := EncodeObsPath(endpoint, bucketName, "test_obs_fs_mkdir/a/b/c/", ak, sk)
	err := testObsFs.Mkdir(dirname, os.ModePerm)
	assert.Nil(t, err)
	t.Cleanup(func() {
		_ = testObsFs.RemoveAll(EncodeObsPath(endpoint, bucketName, "test_obs_fs_mkdir", ak, sk))
	})
}

func TestObsFs_Glob(t *testing.T) {
	initializeEnv(t)
	dirname := EncodeObsPath(endpoint, bucketName, "test_obs_fs_glob/a/", ak, sk)
	f1 := EncodeObsPath(endpoint, bucketName, "test_obs_fs_glob/1.txt", ak, sk)
	f2 := EncodeObsPath(endpoint, bucketName, "test_obs_fs_glob/a/1.txt", ak, sk)
	_ = testObsFs.Mkdir(dirname, os.ModePerm)
	_ = testObsFs.WriteFile(f1, []byte("hello"), os.ModePerm)
	_ = testObsFs.WriteFile(f2, []byte("hello"), os.ModePerm)

	names, err := testObsFs.Glob(EncodeObsPath(endpoint, bucketName, "test_obs_fs_glob/", ak, sk))
	assert.Nil(t, err)
	assert.Equal(t, []string{"test_obs_fs_glob/", "test_obs_fs_glob/1.txt", "test_obs_fs_glob/a/", "test_obs_fs_glob/a/1.txt"}, names)

	t.Cleanup(func() {
		_ = testObsFs.RemoveAll(EncodeObsPath(endpoint, bucketName, "test_obs_fs_glob/", ak, sk))
	})
}

func TestObsFs_ReadDir(t *testing.T) {
	initializeEnv(t)
	dirname := EncodeObsPath(endpoint, bucketName, "test_obs_fs_read_dir/a/", ak, sk)
	f1 := EncodeObsPath(endpoint, bucketName, "test_obs_fs_read_dir/1.txt", ak, sk)
	f2 := EncodeObsPath(endpoint, bucketName, "test_obs_fs_read_dir/a/1.txt", ak, sk)
	_ = testObsFs.Mkdir(dirname, os.ModePerm)
	_ = testObsFs.WriteFile(f1, []byte("hello"), os.ModePerm)
	_ = testObsFs.WriteFile(f2, []byte("hello"), os.ModePerm)

	infos, err := testObsFs.ReadDir(EncodeObsPath(endpoint, bucketName, "test_obs_fs_read_dir/", ak, sk))
	assert.Nil(t, err)
	assert.Equal(t, 4, len(infos))

	t.Cleanup(func() {
		_ = testObsFs.RemoveAll(EncodeObsPath(endpoint, bucketName, "test_obs_fs_read_dir/", ak, sk))
	})
}

func TestObsFile_Write(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_file_write.txt", ak, sk)
	fd, err := testObsFs.Create(name)
	n, err := fd.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	n, err = fd.Write([]byte("world"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	read, _ := testObsFs.ReadFile(name)
	assert.Equal(t, []byte("helloworld"), read)
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFile_Read(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_file_read.txt", ak, sk)
	_ = testObsFs.WriteFile(name, []byte("hello world"), os.ModePerm)
	fd, _ := testObsFs.Open(name)
	read := make([]byte, 5)
	n, err := fd.Read(read)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("hello"), read)
	_, _ = fd.Seek(6, 0)
	n, err = fd.Read(read)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("world"), read)
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFile_ReadAt(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_file_read_at.txt", ak, sk)
	_ = testObsFs.WriteFile(name, []byte("hello world"), os.ModePerm)
	fd, _ := testObsFs.Open(name)
	read := make([]byte, 5)
	n, err := fd.ReadAt(read, 6)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("world"), read)
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFile_Truncate(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_file_truncate.txt", ak, sk)
	_ = testObsFs.WriteFile(name, []byte("hello world"), os.ModePerm)
	fd, _ := testObsFs.Open(name)
	err := fd.Truncate(5)
	assert.Nil(t, err)
	info, _ := fd.Stat()
	assert.Equal(t, int64(5), info.Size())
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFile_StreamReadBatch(t *testing.T) {
	initializeEnv(t)
	name := EncodeObsPath(endpoint, bucketName, "test_obs_file_stream_read.txt", ak, sk)
	_ = testObsFs.WriteFile(name, []byte("hello,world"), os.ModePerm)
	fd, _ := testObsFs.Open(name)
	c := make(chan *request.StreamReader, 2)
	go fd.StreamReadBatch([]int64{0, 2, 5, 8}, []int64{2, 2, 2, 2}, 2, c, -1)
	result := make(map[int64][]byte)
	result[0] = []byte("he")
	result[2] = []byte("ll")
	result[5] = []byte(",w")
	result[8] = []byte("rl")
	flag := false
	for {
		select {
		case content, ok := <-c:
			if !ok {
				flag = true
				break
			}
			assert.Equal(t, result[content.Offset], content.Content)
		}
		if flag {
			break
		}
	}
	t.Cleanup(func() {
		_ = testObsFs.Remove(name)
	})
}

func TestObsFile_IsObsStoragePolicy(t *testing.T) {
	tmpFile := "/tmp/test/00000001-0000-00000000.tssp.obs"
	isObsStoragePolicy, _ := testObsFs.IsObsFile(tmpFile)
	assert.Equal(t, false, isObsStoragePolicy)
}

func TestObsFile_CopyTSSPFromDFVToOBS(t *testing.T) {
	srcFile := "/tmp/test/00000001-0000-00000000.tssp.obs"
	dstFile := "/tmp/test/00000001-0000-00000000.tssp.obs"
	lockFile := FileLockOption("/tmp/test_vfs/lock")
	err := testObsFs.CopyFileFromDFVToOBS(srcFile, dstFile, lockFile)
	assert.Nil(t, err)
}

func TestObsFile_CreateOBS(t *testing.T) {
	srcFile := "/tmp/test/00000001-0000-00000000.tssp.obs"
	pri := FilePriorityOption(IO_PRIORITY_NORMAL)
	lockFile := FileLockOption("/tmp/test_vfs/lock")
	_, err := testObsFs.CreateV2(srcFile, lockFile, pri)
	assert.Nil(t, err)
}
