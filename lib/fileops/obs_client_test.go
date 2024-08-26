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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetObsClient(t *testing.T) {
	mockConf := &obsConf{
		ak:       "mock_ak",
		sk:       "mock_sk",
		bucket:   "mock_bucket",
		endpoint: "mock_endpoint",
	}
	client, err := GetObsClient(mockConf)
	assert.Nil(t, err)
	mockConf1 := &obsConf{
		ak:       "mock_ak",
		sk:       "mock_sk",
		bucket:   "mock_bucket",
		endpoint: "mock_endpoint",
	}
	client1, _ := GetObsClient(mockConf1)
	assert.Equal(t, client, client1)
	mockConf2 := &obsConf{
		ak:       "mock_ak",
		sk:       "mock_sk",
		bucket:   "mock_bucket111",
		endpoint: "mock_endpoint",
	}
	client2, _ := GetObsClient(mockConf2)
	assert.NotEqual(t, client1, client2)
}
