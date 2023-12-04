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
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/request"
	"github.com/stretchr/testify/assert"
)

const defaultSecureValue = "*****"

var ak = defaultSecureValue
var sk = defaultSecureValue
var endpoint = "http://obs.cn-north-5.myhuaweicloud.com"
var bucketName = "logkeeper-fs"
var basePath = ""
var conf *ObsConf

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
	initEnvParam(&basePath, "BASE_PATH")
	if ak == defaultSecureValue || sk == defaultSecureValue {
		t.Log("Skip tests, please set AK/SK in environment variable")
		t.Skip()
	}
	conf = &ObsConf{
		ak:         ak,
		sk:         sk,
		endpoint:   endpoint,
		bucketName: bucketName,
	}
}

func TestObsInitialize(t *testing.T) {
	initializeEnv(t)
	conf.bucketName = "__WRONG_NAME_PLACEHOLDER__"
	_, err := NewObsClient(conf)
	assert.Equal(t, "logkeeper client init failed, please check if bucket [__WRONG_NAME_PLACEHOLDER__] is a PFS bucket", err.Error())

	conf.sk = "***"
	_, err = NewObsClient(conf)
	assert.Equal(t, "logkeeper client init failed, please check if endpoint, ak, sk is right", err.Error())
}

func TestObsWriteAndRead(t *testing.T) {
	initializeEnv(t)
	objectName := t.Name()
	obsClient, _ := NewObsClient(conf)
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

func TestObsLargeWriteAndRead(t *testing.T) {
	initializeEnv(t)
	objectName := t.Name()
	obsClient, _ := NewObsClient(conf)
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

func TestObsGetLength(t *testing.T) {
	initializeEnv(t)
	objectName := t.Name()
	obsClient, _ := NewObsClient(conf)
	bytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	obsClient.Init(objectName)
	obsClient.WriteTo(objectName, 0, 10, &bytes)

	length, _ := obsClient.GetLength(objectName)
	assert.Equal(t, 10, int(length))
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
}

func TestObsDeleteAll(t *testing.T) {
	initializeEnv(t)
	objectName := t.Name()
	obsClient, _ := NewObsClient(conf)
	obsClient.Init(objectName + "/test1/test4/test7")
	obsClient.Init(objectName + "/test2/test5/test8")
	obsClient.Init(objectName + "/test3/test6/test9")
	obsClient.Init(objectName + "/test1/test4")
	obsClient.Init(objectName + "/test2/test5")
	obsClient.Init(objectName + "/test3/test6")
	obsClient.Init(objectName + "/test1")
	obsClient.Init(objectName + "/test2")
	obsClient.Init(objectName + "/test3")
	obsClient.Init(objectName)
	obsClient.DeleteAll(objectName)
	t.Cleanup(func() {
		obsClient.Delete(objectName)
	})
}

func TestObsStreamReadMultiRange(t *testing.T) {
	initializeEnv(t)
	defaultObsRangeSize = 2
	objectName := "test/test/" + t.Name()
	obsClient, err := NewObsClient(conf)
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
