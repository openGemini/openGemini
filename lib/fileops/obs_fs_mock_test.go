// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/stretchr/testify/assert"
)

var mockObsConf = &obsConf{
	ak:       "mock_ak",
	sk:       "mock_sk",
	bucket:   "mock_bucket",
	endpoint: "mock_endpoint",
}

func TestMockObsFile_Write(t *testing.T) {
	client := newMockObsClient()
	fd := &obsFile{
		key:    "test_mock_obs_file_write",
		client: client,
		conf:   mockObsConf,
		offset: 0,
	}
	// write empty byte slice
	n, err := fd.Write([]byte{})
	assert.Equal(t, 0, n)
	assert.NotNil(t, err)
	// write fail with mock error
	client.mockErr = true
	n, err = fd.Write([]byte("hello"))
	assert.Equal(t, 0, n)
	assert.NotNil(t, err)
	// write success
	client.mockErr = false
	n, err = fd.Write([]byte("hello"))
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(5), fd.offset)
	assert.Nil(t, err)
	n, err = fd.Write([]byte("world"))
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(10), fd.offset)
	assert.Nil(t, err)
}

func TestMockObsFile_Read(t *testing.T) {
	client := newMockObsClient()
	fd := &obsFile{
		key:    "test_mock_obs_file_read",
		client: client,
		conf:   mockObsConf,
		offset: 0,
	}
	// read fail with mock error
	client.mockErr = true
	content := make([]byte, 5)
	n, err := fd.Read(content)
	assert.NotNil(t, err)
	assert.Equal(t, 0, n)
	// read success
	client.mockErr = false
	_, _ = fd.Write([]byte("helloworld"))
	fd.offset = 0
	n, err = fd.Read(content)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("hello"), content)
	n, err = fd.Read(content)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("world"), content)
	n, err = fd.Read(content)
	assert.NotNil(t, err)
	assert.Equal(t, 0, n)
	_, _ = fd.Seek(0, 0)
	n, err = fd.Read(content)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("hello"), content)
}

func TestMockObsFile_Seek(t *testing.T) {
	client := newMockObsClient()
	fd := &obsFile{
		key:    "test_mock_obs_file_read",
		client: client,
		conf:   mockObsConf,
		offset: 0,
	}
	_, _ = fd.Write([]byte("hello world"))
	_, err := fd.Seek(6, -1)
	assert.NotNil(t, err)
	off, err := fd.Seek(6, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(6), off)
	assert.Equal(t, int64(6), fd.offset)
	off, err = fd.Seek(3, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(9), off)
	assert.Equal(t, int64(9), fd.offset)
}

func TestMockObsFile_Truncate(t *testing.T) {
	client := newMockObsClient()
	fd := &obsFile{
		key:    "test_mock_obs_file_truncate",
		client: client,
		conf:   mockObsConf,
		offset: 0,
	}
	_, _ = fd.Write([]byte("hello world"))
	err := fd.Truncate(-1)
	assert.NotNil(t, err)
	err = fd.Truncate(5)
	assert.Nil(t, err)
	assert.Equal(t, int64(5), fd.offset)
	_, _ = fd.Write([]byte("123"))
	fd.offset = 0
	read := make([]byte, 8)
	_, _ = fd.Read(read)
	assert.Equal(t, []byte("hello123"), read)
}

func TestMockObsFile_Stat(t *testing.T) {
	client := newMockObsClient()
	fd := &obsFile{
		key:    "test_mock_obs_file_truncate",
		client: client,
		conf:   mockObsConf,
		offset: 0,
	}
	_, _ = fd.Write([]byte("hello world"))
	info, err := fd.Stat()
	assert.Nil(t, err)
	assert.Equal(t, int64(11), info.Size())
	assert.Equal(t, fd.key, info.Name())
	client.mockErr = true
	info, err = fd.Stat()
	assert.NotNil(t, err)
	assert.Nil(t, info)
}

func TestMockObsFile_StreamReadBatch(t *testing.T) {
	client := newMockObsClient()
	fd := &obsFile{
		key:    "test_mock_obs_file_stream_read",
		client: client,
		conf:   mockObsConf,
		offset: 0,
	}
	_, _ = fd.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	offs := []int64{0, 2, 5, 8}
	sizes := []int64{2, 2, 2, 2}
	ch := make(chan *request.StreamReader, 2)
	config.SetProductType(config.LogKeeperService)
	go fd.StreamReadBatch(offs, sizes, 2, ch, -1, true)
	result := make(map[int64][]byte)
	result[0] = []byte{1, 2}
	result[2] = []byte{3, 4}
	result[5] = []byte{6, 7}
	result[8] = []byte{9, 10}
	flag := false
	for {
		select {
		case content, ok := <-ch:
			if !ok {
				flag = true
				break
			}
			assert.Equal(t, nil, content.Err)
			assert.Equal(t, result[content.Offset], content.Content)
		}
		if flag {
			break
		}
	}
}

func testFs() (*obsFs, *mockObsClient) {
	fs := &obsFs{}
	client := newMockObsClient()
	PutObsClient(mockObsConf, client)
	return fs, client
}

func TestMockObsFs_ParseObsConf(t *testing.T) {
	fs, _ := testFs()
	conf, path, err := fs.parseObsConf("obs://mock_endpoint/mock_ak/mock_sk/mock_bucket/mock_dir/mock_obj")
	assert.Nil(t, err)
	assert.Equal(t, "mock_dir/mock_obj", path)
	assert.Equal(t, "mock_endpoint", conf.endpoint)
	assert.Equal(t, "mock_bucket", conf.bucket)
	assert.Equal(t, "mock_ak", conf.ak)
	assert.Equal(t, "mock_sk", conf.sk)

	_, _, err = fs.parseObsConf("obs://")
	assert.NotNil(t, err)
}

func TestMockObsFs_Create(t *testing.T) {
	fs, client := testFs()
	_, err := fs.Create("invalid_path")
	assert.NotNil(t, err)
	name := "test_obs_fs_create.txt"
	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	_, err = fs.Create(path)
	assert.Nil(t, err)
	client.mockErr = true
	_, err = fs.Create(path)
	assert.NotNil(t, err)
}

func TestMockObsFs_OpenFile(t *testing.T) {
	fs, client := testFs()
	_, err := fs.OpenFile("invalid_path", os.O_CREATE, os.ModePerm)
	assert.NotNil(t, err)

	name := "test_obs_fs_open_file.txt"
	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	_, err = fs.OpenFile(path, os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	assert.Nil(t, err)
	_, err = fs.OpenFile(path, os.O_CREATE, os.ModePerm)
	assert.Nil(t, err)
	client.mockErr = true
	_, err = fs.OpenFile(path, os.O_CREATE, os.ModePerm)
	assert.NotNil(t, err)
	client.mockErr = false
	_ = fs.WriteFile(path, []byte("hello"), os.ModePerm)
	fd, err := fs.OpenFile(path, os.O_CREATE|os.O_APPEND, os.ModePerm)
	assert.Nil(t, err)
	assert.Equal(t, int64(5), fd.(*obsFile).offset)
}

func TestMockObsFs_Remove(t *testing.T) {
	fs, client := testFs()
	err := fs.Remove("invalid_path")
	assert.NotNil(t, err)

	name := "test_obs_fs_remove.txt"
	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	_ = fs.WriteFile(path, []byte("hello"), os.ModePerm)
	_, err = fs.Stat(path)
	assert.Nil(t, err)
	err = fs.Remove(path)
	assert.Nil(t, err)
	_, err = fs.Stat(path)
	assert.NotNil(t, err)

	client.mockErr = true
	err = fs.Remove(path)
	assert.NotNil(t, err)
}

func TestMockObsFs_RemoveAll(t *testing.T) {
	fs, client := testFs()
	err := fs.RemoveAll("invalid_path")
	assert.NotNil(t, err)

	name := "test_obs_fs_remove_all"
	path1 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	path2 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name+"_a", mockObsConf.ak, mockObsConf.sk)
	path3 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name+"_b", mockObsConf.ak, mockObsConf.sk)
	_ = fs.WriteFile(path1, []byte("hello"), os.ModePerm)
	_ = fs.WriteFile(path2, []byte("hello"), os.ModePerm)
	_ = fs.WriteFile(path3, []byte("hello"), os.ModePerm)
	_, err = fs.Stat(path1)
	assert.Nil(t, err)
	_, err = fs.Stat(path2)
	assert.Nil(t, err)
	_, err = fs.Stat(path3)
	assert.Nil(t, err)

	err = fs.RemoveAll(path1)
	assert.Nil(t, err)
	_, err = fs.Stat(path1)
	assert.NotNil(t, err)
	_, err = fs.Stat(path2)
	assert.NotNil(t, err)
	_, err = fs.Stat(path3)
	assert.NotNil(t, err)

	client.mockErr = true
	err = fs.RemoveAll(path1)
	assert.NotNil(t, err)
}

func TestMockObsFs_Glob(t *testing.T) {
	fs, client := testFs()
	_, err := fs.Glob("invalid_path")
	assert.NotNil(t, err)

	name := "test_obs_fs_glob"
	path1 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	path2 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name+"_a", mockObsConf.ak, mockObsConf.sk)
	path3 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name+"_b", mockObsConf.ak, mockObsConf.sk)
	_ = fs.WriteFile(path1, []byte("hello"), os.ModePerm)
	_ = fs.WriteFile(path2, []byte("hello"), os.ModePerm)
	_ = fs.WriteFile(path3, []byte("hello"), os.ModePerm)

	files, err := fs.Glob(path1)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(files))
	sort.Strings(files)
	assert.Equal(t, []string{"test_obs_fs_glob", "test_obs_fs_glob_a", "test_obs_fs_glob_b"}, files)

	client.mockErr = true
	_, err = fs.Glob(path1)
	assert.NotNil(t, err)
}

func TestMockObsFs_ReadDir(t *testing.T) {
	fs, client := testFs()
	_, err := fs.ReadDir("invalid_path")
	assert.NotNil(t, err)

	name := "test_obs_fs_read_dir/"
	path1 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name+"a", mockObsConf.ak, mockObsConf.sk)
	path2 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name+"b", mockObsConf.ak, mockObsConf.sk)
	path3 := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name+"c", mockObsConf.ak, mockObsConf.sk)
	_ = fs.WriteFile(path1, []byte("hello"), os.ModePerm)
	_ = fs.WriteFile(path2, []byte("hello"), os.ModePerm)
	_ = fs.WriteFile(path3, []byte("hello"), os.ModePerm)

	dir := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	files, err := fs.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(files))

	client.mockErr = true
	_, err = fs.ReadDir(dir)
	assert.NotNil(t, err)
}

func TestMockObsFs_Stat(t *testing.T) {
	fs, client := testFs()
	_, err := fs.Stat("invalid_path")
	assert.NotNil(t, err)

	name := "test_obs_fs_stat.txt"
	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	_ = fs.WriteFile(path, []byte("hello"), os.ModePerm)
	_, err = fs.Stat(path)
	assert.Nil(t, err)

	client.mockErr = true
	_, err = fs.Stat(path)
	assert.NotNil(t, err)
}

func TestMockObsFs_WriteFile(t *testing.T) {
	fs, client := testFs()
	err := fs.WriteFile("invalid_path", []byte("hello"), os.ModePerm)
	assert.NotNil(t, err)

	name := "test_obs_fs_write_file.txt"
	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	err = fs.WriteFile(path, []byte("hello"), os.ModePerm)
	assert.Nil(t, err)

	client.mockErr = true
	err = fs.WriteFile(path, []byte("hello"), os.ModePerm)
	assert.NotNil(t, err)
}

func TestMockObsFs_ReadFile(t *testing.T) {
	fs, client := testFs()
	read, err := fs.ReadFile("invalid_path")
	assert.NotNil(t, err)

	name := "test_obs_fs_read_file.txt"
	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	content := []byte("hello")
	_ = fs.WriteFile(path, content, os.ModePerm)
	read, err = fs.ReadFile(path)
	assert.Nil(t, err)
	assert.Equal(t, content, read)

	client.mockErr = true
	_, err = fs.ReadFile(path)
	assert.NotNil(t, err)
}

func TestMockObsFs_CopyFile(t *testing.T) {
	nameSrc := "test_obs_fs_copy_file_src.txt"
	nameDst := "test_obs_fs_copy_file_dst.txt"
	pathSrc := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, nameSrc, mockObsConf.ak, mockObsConf.sk)
	pathDst := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, nameDst, mockObsConf.ak, mockObsConf.sk)

	fs, _ := testFs()
	_ = fs.WriteFile(pathSrc, []byte("hello"), os.ModePerm)

	n, err := fs.CopyFile("invalid_path", pathDst)
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = fs.CopyFile(pathSrc, "invalid_path")
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), n)
	n, err = fs.CopyFile(pathSrc, pathDst)
	assert.Nil(t, err)
	assert.Equal(t, int64(5), n)

	read, _ := fs.ReadFile(pathDst)
	assert.Equal(t, []byte("hello"), read)
}

func TestMockObsFs_RenameFile(t *testing.T) {
	oldName := "test_obs_fs_rename_file_old.txt"
	newName := "test_obs_fs_rename_file_new.txt"
	oldPath := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, oldName, mockObsConf.ak, mockObsConf.sk)
	newPath := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, newName, mockObsConf.ak, mockObsConf.sk)

	fs, _ := testFs()
	err := fs.RenameFile(oldPath, newPath)
	assert.Nil(t, err)
}

func TestMockObsFs_IsObsFile(t *testing.T) {
	name := "test_obs_fs_isObs_file.txt"
	fs, _ := testFs()
	_, err := fs.IsObsFile(name)
	assert.NotNil(t, err)

	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	_, err = fs.IsObsFile(path)
	assert.Nil(t, err)
}

func TestMockObsFs_RemoveLocal(t *testing.T) {
	name := "test_obs_fs_remove_local.txt"
	path := EncodeObsPath(mockObsConf.endpoint, mockObsConf.bucket, name, mockObsConf.ak, mockObsConf.sk)
	fs, _ := testFs()
	err := fs.RemoveLocal(path)
	assert.Nil(t, err)
}

type mockObsClient struct {
	mockErr bool
	dummy   map[string][]byte
}

func newMockObsClient() *mockObsClient {
	return &mockObsClient{mockErr: false, dummy: make(map[string][]byte)}
}

func (m *mockObsClient) ListBuckets(input *obs.ListBucketsInput) (*obs.ListBucketsOutput, error) {
	return nil, nil
}

func (m *mockObsClient) ListObjects(input *obs.ListObjectsInput) (*obs.ListObjectsOutput, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	objs := make([]obs.Content, 0)
	for k, v := range m.dummy {
		objs = append(objs, obs.Content{Key: k, Size: int64(len(v)), LastModified: time.Now()})
	}
	output := &obs.ListObjectsOutput{Contents: objs}
	return output, nil
}

func (m *mockObsClient) GetObject(input *obs.GetObjectInput) (*obs.GetObjectOutput, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	bytes := m.dummy[input.Key]
	if int(input.RangeStart) >= len(bytes) {
		return nil, fmt.Errorf("invalid range")
	}
	output := &obs.GetObjectOutput{}
	if input.RangeEnd != 0 {
		output.Body = newBody(bytes[input.RangeStart : input.RangeEnd+1])
	} else {
		output.Body = newBody(bytes)
	}
	output.ContentLength = int64(len(bytes))
	return output, nil
}

func (m *mockObsClient) DeleteObject(input *obs.DeleteObjectInput) (*obs.DeleteObjectOutput, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	delete(m.dummy, input.Key)
	return nil, nil
}

func (m *mockObsClient) DeleteObjects(input *obs.DeleteObjectsInput) (*obs.DeleteObjectsOutput, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	for _, obj := range input.Objects {
		delete(m.dummy, obj.Key)
	}
	return nil, nil
}

func (m *mockObsClient) ModifyObject(input *obs.ModifyObjectInput) (*obs.ModifyObjectOutput, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	dst := make([]byte, input.ContentLength)
	_, _ = input.Body.Read(dst)
	m.dummy[input.Key] = append(m.dummy[input.Key], dst...)
	return &obs.ModifyObjectOutput{}, nil
}

func (m *mockObsClient) PutObject(input *obs.PutObjectInput) (*obs.PutObjectOutput, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	dst := make([]byte, input.ContentLength)
	_, _ = input.Body.Read(dst)
	m.dummy[input.Key] = dst
	return &obs.PutObjectOutput{}, nil
}

func (m *mockObsClient) GetObjectMetadata(input *obs.GetObjectMetadataInput) (*obs.GetObjectMetadataOutput, error) {
	if m.mockErr {
		err := obs.ObsError{}
		err.StatusCode = 500
		return nil, err
	}
	if content, exist := m.dummy[input.Key]; exist {
		output := &obs.GetObjectMetadataOutput{}
		output.ContentLength = int64(len(content))
		output.LastModified = time.Now()
		return output, nil
	} else {
		err := obs.ObsError{}
		err.StatusCode = 404
		return nil, err
	}
}

func (m *mockObsClient) IsObsFile(input *obs.HeadObjectInput) (output *obs.BaseModel, err error) {
	return nil, nil
}

func (m *mockObsClient) Do(r *http.Request) (*http.Response, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	if strings.HasSuffix(r.URL.String(), "truncate") { // truncate
		reg := regexp.MustCompile(`([^/]+)\?length=(\d+)`)
		match := reg.FindStringSubmatch(r.URL.String())
		name := match[1]
		length, _ := strconv.Atoi(match[2])
		m.dummy[name] = m.dummy[name][:length]
		return nil, nil
	} else { // range read
		w := httptest.NewRecorder()
		w.Header().Set("Content-Type", "multipart/form-data; boundary=MyBoundary")
		mw := multipart.NewWriter(w.Body)
		_ = mw.SetBoundary("MyBoundary")
		part, err := mw.CreatePart(textproto.MIMEHeader{
			"Content-Disposition": {"form-data; name=\"part1\""},
			"Content-range":       {"bytes 0-3/10"},
		})
		if err != nil {
			return nil, err
		}
		_, _ = part.Write([]byte{1, 2, 3, 4})
		part, err = mw.CreatePart(textproto.MIMEHeader{
			"Content-Disposition": {"form-data; name=\"part2\""},
			"Content-range":       {"bytes 5-6/10"},
		})
		if err != nil {
			return nil, err
		}
		_, _ = part.Write([]byte{6, 7})
		_ = mw.Close()
		resp := w.Result()
		return resp, nil
	}
}

func (m *mockObsClient) RenameFile(input *obs.RenameFileInput) (*obs.RenameFileOutput, error) {
	if m.mockErr {
		return nil, fmt.Errorf("mock error")
	}
	return &obs.RenameFileOutput{}, nil
}

func newBody(b []byte) *body {
	return &body{bytes: b}
}

type body struct {
	mockErr bool
	bytes   []byte
}

func (b *body) Read(p []byte) (n int, err error) {
	if b.mockErr {
		return 0, fmt.Errorf("mock error")
	}
	return copy(p, b.bytes), nil
}

func (b *body) Close() error {
	if b.mockErr {
		return fmt.Errorf("mock error")
	}
	return nil
}
