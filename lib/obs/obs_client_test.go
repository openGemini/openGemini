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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObsClientToMap(t *testing.T) {
	obsConf := &ObsConf{}
	obsConf.bucketName = "test-bucket"
	mockClient := &MockObsClient{
		testName: t.Name(),
	}
	client, _ := NewObsClientByObject(obsConf, mockClient, nil)
	SaveObsClientToMap(*obsConf, client)
	client, _ = GetObsClient(&LogPath{BucketName: obsConf.bucketName})
	assert.Equal(t, client.obsConf.bucketName, obsConf.bucketName)
	notExistBucketName := "not exist"
	client, _ = GetObsClient(&LogPath{BucketName: notExistBucketName})
	assert.Equal(t, true, IsInterfaceNil(client))
}

func IsInterfaceNil(value interface{}) bool {
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		return val.IsNil()
	}

	if value == nil {
		return true
	}

	return false
}

func TestObsClientWrap(t *testing.T) {
	obsConf := &ObsConf{ak: "test", sk: "test", endpoint: "test", bucketName: t.Name()}

	_, err := NewWrapObsClient(obsConf)
	assert.Equal(t, true, IsInterfaceNil(err))
	_, err = NewWrapObsClient(nil)
	assert.Equal(t, false, IsInterfaceNil(err))
	c := NewWrapHttpClient()
	assert.Equal(t, false, IsInterfaceNil(c.httpClient))
}
