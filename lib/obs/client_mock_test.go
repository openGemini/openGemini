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

package obs

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/stretchr/testify/assert"
)

type MockObsClient struct {
	testName string
}

var mockMap = make(map[string]map[string]int64)

func (w *MockObsClient) ListObjects(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
	switch w.testName {
	case "TestMockObsGetAllLength":
		return nil, fmt.Errorf("mock-error")
	case "TestMockObsGetAllLengthByErr":
		return nil, fmt.Errorf("mock-error")
	default:
		return nil, nil
	}
}

func (w *MockObsClient) ListBuckets(input *obs.ListBucketsInput) (output *obs.ListBucketsOutput, err error) {
	switch w.testName {
	case "TestMockObsInitializeError1":
		return nil, fmt.Errorf("mock-error")
	case "TestMockObsInitializeError2":
		bucketsOutput := &obs.ListBucketsOutput{}
		bucket := obs.Bucket{}
		bucket.Name = "wrong-bucket"
		bucketsOutput.Buckets = []obs.Bucket{bucket}
		return bucketsOutput, nil
	default:
		bucketsOutput := &obs.ListBucketsOutput{}
		bucket := obs.Bucket{}
		bucket.Name = "test-bucket"
		bucketsOutput.Buckets = []obs.Bucket{bucket}
		return bucketsOutput, nil
	}
}

type SmallBody struct{}

func (s *SmallBody) Read(p []byte) (n int, err error) {
	copy(p, []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	return 10, io.EOF
}

func (s *SmallBody) Close() error {
	return nil
}

type BigBody struct{}

var readOffset = 0

func (s *BigBody) Read(p []byte) (n int, err error) {
	i := 0
	for j := range p {
		p[j] = 1
		readOffset++
		i++
		if readOffset == 100000 {
			return i, io.EOF
		}
	}
	return i, nil
}

func (s *BigBody) Close() error {
	return nil
}

func (w *MockObsClient) GetObject(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
	switch w.testName {
	case "TestMockObsWriteAndReadByErr":
		return nil, fmt.Errorf("mock-error")
	default:
		getOutput := &obs.GetObjectOutput{}
		if input.RangeStart == 0 && input.RangeEnd == 10-1 {
			getOutput.Body = &SmallBody{}
			return getOutput, nil
		}
		if input.RangeStart == 0 && input.RangeEnd == 100000-1 {
			getOutput.Body = &BigBody{}
			return getOutput, nil
		}
		return nil, nil
	}
}

func (w *MockObsClient) DeleteObject(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
	switch w.testName {
	case "TestMockObsWriteAndReadByErr":
		return nil, fmt.Errorf("mock-error")
	default:
		mockMap[w.testName][input.Key] = 0
		return nil, nil
	}
}

func (w *MockObsClient) ModifyObject(input *obs.ModifyObjectInput) (output *obs.ModifyObjectOutput, err error) {
	if w.testName == "TestObsStoreAppenderError" {
		time.Sleep(500 * time.Millisecond)
		return nil, fmt.Errorf("mock modify error")
	}
	if mockMap[w.testName] == nil {
		mockMap[w.testName] = make(map[string]int64)
	}
	if mockMap[w.testName][input.Key] != input.Position {
		return nil, fmt.Errorf("write to wrong position")
	}
	mockMap[w.testName][input.Key] += input.ContentLength
	return nil, nil
}

func (w *MockObsClient) PutObject(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
	return nil, nil
}

func (w *MockObsClient) GetObjectMetadata(input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
	objectMetadataOutput := &obs.GetObjectMetadataOutput{}
	switch w.testName {
	case "TestMockObsTruncate":
		objectMetadataOutput.ContentLength = 5
	case "TestMockObsGetLength":
		objectMetadataOutput.ContentLength = 10
	case "TestMockObsStoreGetLenByError":
		return nil, fmt.Errorf("test error")
	case "TestMockObsGetAllLength":
		return nil, fmt.Errorf("test error")
	default:
		objectMetadataOutput.ContentLength = mockMap[w.testName][input.Key]
	}
	return objectMetadataOutput, nil
}

func TestMockObsInitialize(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	mockClient := &MockObsClient{
		testName: t.Name(),
	}
	_, err := NewObsClientByObject(conf, mockClient, nil)
	assert.Equal(t, nil, err)
}

func TestMockObsInitializeError1(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	mockClient := &MockObsClient{
		testName: t.Name(),
	}
	_, err := NewObsClientByObject(conf, mockClient, nil)
	assert.Errorf(t, err, "logkeeper client init failed, please check if endpoint, ak, sk is right")
}

func TestMockObsInitializeError2(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	mockClient := &MockObsClient{
		testName: t.Name(),
	}
	_, err := NewObsClientByObject(conf, mockClient, nil)
	assert.Errorf(t, err, "logkeeper client init failed, please check if bucket [test-bucket] is a PFS bucket")
}

func TestMockObsWriteAndRead(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{}, nil)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	targetBytes := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	obsClient.ReadAt(objectName, 0, 10, targetBytes)
	assert.Equal(t, bytes, targetBytes)
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
}

func TestMockObsWriteError(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{}, nil)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	err := obsClient.WriteTo(objectName, 0, 8, &bytes)
	if err == nil {
		t.Errorf("doesn't get err")
	}
}

func TestMockObsWriteAndReadByErr(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{testName: t.Name()}, nil)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	targetBytes := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	err := obsClient.ReadAt(objectName, 0, 10, targetBytes)
	if err == nil {
		t.Errorf("doesn't get err")
	}
	err = obsClient.Delete(objectName)
	if err == nil {
		t.Errorf("doesn't get err")
	}
}

func TestMockObsLargeWriteAndRead(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{}, nil)
	bytes := make([]byte, 100000)
	for i := 0; i < 100000; i++ {
		bytes[i] = 1
	}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 100000, &bytes)

	targetBytes := make([]byte, 100000)
	obsClient.ReadAt(objectName, 0, 100000, targetBytes)
	assert.Equal(t, bytes, targetBytes)
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
}

func TestMockObsGetLength(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{testName: t.Name()}, nil)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	length, _ := obsClient.GetLength(objectName)
	assert.Equal(t, 10, int(length))
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
}

func TestMockObsGetAllLengthByErrorObject(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{testName: t.Name()}, nil)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	length, _ := obsClient.GetAllLength(objectName)
	assert.Equal(t, 0, int(length))
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
	length, _ = obsClient.GetAllLength("test")
}

func TestMockObsGetAllLength(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{testName: t.Name()}, nil)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	length, _ := obsClient.GetAllLength(objectName)
	assert.Equal(t, 0, int(length))
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
	length, _ = obsClient.GetLength("test")
}

func TestMockObsGetAllLengthByErr(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{testName: t.Name()}, nil)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	length, _ := obsClient.GetAllLength(objectName)
	assert.Equal(t, 0, int(length))
	err := obsClient.DeleteAll(objectName)
	if err == nil {
		t.Errorf("doesn't get err")
	}
}

type MockHttpClient struct {
	testName string
}

type OneRangeBody struct{}

func (s *OneRangeBody) Read(p []byte) (n int, err error) {
	copy(p, []byte{9, 10})
	return 2, io.EOF
}

func (s *OneRangeBody) Close() error {
	return nil
}

func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	switch m.testName {
	case "TestMockObsStreamReadMultiRange":
		resp := &http.Response{}
		resp.Header = make(map[string][]string)
		if req.Header.Get("Range") == "bytes=8-9" {
			resp.Header.Add("Content-Type", "binary/octet-stream")
			resp.Header.Add("Content-range", "bytes 8-9/10")
			resp.Body = &OneRangeBody{}
		} else {
			w := httptest.NewRecorder()
			w.Header().Set("Content-Type", "multipart/form-data; boundary=MyBoundary")

			mw := multipart.NewWriter(w.Body)
			mw.SetBoundary("MyBoundary")

			part, err := mw.CreatePart(textproto.MIMEHeader{
				"Content-Disposition": {"form-data; name=\"part1\""},
				"Content-range":       {"bytes 0-3/10"},
			})
			if err != nil {
				return nil, err
			}
			part.Write([]byte{1, 2, 3, 4})

			part, err = mw.CreatePart(textproto.MIMEHeader{
				"Content-Disposition": {"form-data; name=\"part2\""},
				"Content-range":       {"bytes 5-6/10"},
			})
			if err != nil {
				return nil, err
			}
			part.Write([]byte{6, 7})

			mw.Close()
			resp = w.Result()
		}
		return resp, nil
	case "TestMockObsStoreAppenderWithTrunc":
		q, err := url.ParseQuery(req.URL.RawQuery)
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(q.Get("length"))
		if err != nil {
			return nil, err
		}
		pathParts := strings.Split(req.URL.Path, "/")
		objectName := pathParts[len(pathParts)-1]
		mockMap[m.testName][objectName] = int64(length)
		return nil, nil
	case "TestMockObsTruncateErr":
		return nil, fmt.Errorf("mock error")
	case "TestMockObsStreamReadWrongHeader":
		resp := &http.Response{}
		resp.Header = make(map[string][]string)
		if req.Header.Get("Range") == "bytes=8-9" {
			resp.Header.Add("Content-Type", "test")
			resp.Header.Add("Content-range", "bytes 8-9/10")
			resp.Body = &OneRangeBody{}
		} else {
			w := httptest.NewRecorder()
			w.Header().Set("Content-Type", "multipart/form-data; boundary=MyBoundary")

			mw := multipart.NewWriter(w.Body)
			mw.SetBoundary("MyBoundary")

			part, err := mw.CreatePart(textproto.MIMEHeader{
				"Content-Disposition": {"form-data; name=\"part1\""},
				"Content-range":       {"bytes 0-3/10"},
			})
			if err != nil {
				return nil, err
			}
			part.Write([]byte{1, 2, 3, 4})

			part, err = mw.CreatePart(textproto.MIMEHeader{
				"Content-Disposition": {"form-data; name=\"part2\""},
				"Content-range":       {"bytes 5-6/10"},
			})
			if err != nil {
				return nil, err
			}
			part.Write([]byte{6, 7})

			mw.Close()
			resp = w.Result()
		}
		return resp, nil
	default:
		return nil, nil
	}
}

func TestMockObsTruncate(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{testName: t.Name()}, &MockHttpClient{testName: t.Name()})
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	obsClient.Truncate(objectName, 5)
	length, _ := obsClient.GetLength(objectName)
	assert.Equal(t, 5, int(length))
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
}

func TestMockObsTruncateErr(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	objectName := t.Name()
	obsClient, _ := NewObsClientByObject(conf, &MockObsClient{testName: t.Name()}, &MockHttpClient{testName: t.Name()})

	err := obsClient.Truncate(objectName, -1)
	assert.NotNil(t, err)
	assert.Equal(t, "truncate size must be positive", err.Error())

	err = obsClient.Truncate(objectName, 5)
	assert.NotNil(t, err)
	assert.Equal(t, "mock error", err.Error())
}

func TestMockObsStreamReadMultiRange(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	defaultObsRangeSize = 2
	objectName := "test/test/" + t.Name()
	obsClient, err := NewObsClientByObject(conf, &MockObsClient{}, &MockHttpClient{testName: t.Name()})
	if err != nil {
		t.Errorf("NewObsClient: %s", err)
	}
	bs := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if err := obsClient.Init(objectName); err != nil {
		t.Errorf("init object failed: %s", err)
	}
	if err := obsClient.WriteTo(objectName, 0, 10, &bs); err != nil {
		t.Errorf("write to object failed: %s", err)
	}
	offs := make([]int64, 0)
	offs = append(offs, 0, 2, 5, 8)
	sizes := make([]int64, 0)
	sizes = append(sizes, 2, 2, 2, 2)
	c := make(chan *request.StreamReader, 2)
	go obsClient.StreamReadMultiRange(objectName, offs, sizes, 2, c, -1)
	result := make(map[int64][]byte)
	result[0] = []byte{1, 2}
	result[2] = []byte{3, 4}
	result[5] = []byte{6, 7}
	result[8] = []byte{9, 10}
	flag := false
	for {
		select {
		case content, ok := <-c:
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
	t.Cleanup(func() {
		if err := obsClient.Delete(objectName); err != nil {
			t.Errorf("delete object failed: %s", err)
		}
	})
}

func TestMockObsStreamReadWrongHeader(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	defaultObsRangeSize = 2
	objectName := "test/test/" + t.Name()
	obsClient, err := NewObsClientByObject(conf, &MockObsClient{}, &MockHttpClient{testName: t.Name()})
	if err != nil {
		t.Errorf("NewObsClient: %s", err)
	}
	bs := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if err := obsClient.Init(objectName); err != nil {
		t.Errorf("init object failed: %s", err)
	}
	if err := obsClient.WriteTo(objectName, 0, 10, &bs); err != nil {
		t.Errorf("write to object failed: %s", err)
	}
	offs := make([]int64, 0)
	offs = append(offs, 0, 2, 5, 8)
	sizes := make([]int64, 0)
	sizes = append(sizes, 2, 2, 2, 2)
	c := make(chan *request.StreamReader, 2)
	go obsClient.StreamReadMultiRange(objectName, offs, sizes, 2, c, -1)
	result := make(map[int64][]byte)
	result[0] = []byte{1, 2}
	result[2] = []byte{3, 4}
	result[5] = []byte{6, 7}
	result[8] = []byte{9, 10}
	flag := false
	for {
		select {
		case _, ok := <-c:
			if !ok {
				flag = true
				break
			}
		}
		if flag {
			break
		}
	}
	t.Cleanup(func() {
		if err := obsClient.DeleteAll(objectName); err != nil {
			t.Errorf("delete object failed: %s", err)
		}
		obsClient.InitAll(nil, nil, nil)
	})
}

type MockHttpClientError struct{}

func (m *MockHttpClientError) Do(req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("mock-error")
}

func TestMockObsStreamReadMultiRangeError(t *testing.T) {
	var conf = &ObsConf{}
	conf.bucketName = "test-bucket"
	defaultObsRangeSize = 2
	objectName := "test/test/" + t.Name()
	obsClient, err := NewObsClientByObject(conf, &MockObsClient{}, &MockHttpClientError{})
	if err != nil {
		t.Errorf("NewObsClient: %s", err)
	}
	bs := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if err := obsClient.Init(objectName); err != nil {
		t.Errorf("init object failed: %s", err)
	}
	if err := obsClient.WriteTo(objectName, 0, 10, &bs); err != nil {
		t.Errorf("write to object failed: %s", err)
	}
	offs := make([]int64, 0)
	offs = append(offs, 0, 2, 5, 8)
	sizes := make([]int64, 0)
	sizes = append(sizes, 2, 2, 2, 2)
	c := make(chan *request.StreamReader, 2)
	go obsClient.StreamReadMultiRange(objectName, offs, sizes, 2, c, -1)
	flag := false
	for {
		select {
		case content, ok := <-c:
			if !ok {
				flag = true
				break
			}
			assert.Error(t, content.Err)
		}
		if flag {
			break
		}
	}
	t.Cleanup(func() {
		if err := obsClient.Delete(objectName); err != nil {
			t.Errorf("delete object failed: %s", err)
		}
	})
}
