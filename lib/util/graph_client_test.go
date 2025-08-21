// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package util

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func NewMockHTTPSServer(handler http.Handler) *httptest.Server {
	return httptest.NewTLSServer(handler)
}

func TestSendGetRequest(t *testing.T) {
	mockServer := NewMockHTTPSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(
			`{
				"result_uid": "data_from_remote_9958e1d872df",
				"metadata": {
					"region": "global",
					"timestamp": 1724360595038,
					"topokeys": ["source0", "source1"]
				},
				"graph": {
					"vertex": [
						{
							"uid": "vm1",
							"metadata": {
								"kind": "Node",
								"region": "cn-north-1",
								"tags": [{"key": "name", "value": "db1"}]
							}
						},
						{
							"uid": "vm2",
							"metadata": {
								"kind": "Node",
								"region": "cn-north-1",
								"tags": [{"key": "name", "value": "db2"}]
							}
						},
						{
							"uid": "vm3",
							"metadata": {
								"kind": "Node",
								"region": "cn-east-1",
								"tags": [{"key": "name", "value": "db3"}]
							}
						}
					],
					"edges": [
						{
							"uid": "vm2_source0::::vm1_source0",
							"metadata": {
								"kind": "LOCATE",
								"source_uid": "vm2",
								"target_uid": "vm1",
								"tags": [{"key": "1", "value": "1"}]
							}
						},
						{
							"uid": "vm3_source1::::vm2_source1",
							"metadata": {
								"kind": "LOCATE",
								"source_uid": "vm3",
								"target_uid": "vm2",
								"tags": [{"key": "1", "value": "1"}]
							}
						}
					]
				}
            }`))
	}))
	defer mockServer.Close()

	parsedURL, err := url.Parse(mockServer.URL)
	if err != nil {
		t.Fatalf("Failed to parse URL: %v", err)
	}

	hostPort := parsedURL.Host
	ip, port, err := net.SplitHostPort(hostPort)

	baseURL := fmt.Sprintf("https://%s:%s/v1/coc--cloudmap/topology/combination", ip, port)

	if err != nil {
		t.Fatalf("Failed to split host and port: %v", err)
	}

	clientConf := &Client{
		Conf: Config{
			baseURL,
		},
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	paramTest := Param{
		ParamValue: "test",
		ParamName:  "applicationId",
	}
	startTime := "2023-01-01T00:00:00Z"
	endTime := "2023-01-01T23:59:59Z"

	response, err := clientConf.SendGetRequest(client, paramTest, startTime, endTime)
	if err != nil {
		t.Errorf("Failed to send GET request: %v", err)
	}

	expectedResponse := `{
				"result_uid": "data_from_remote_9958e1d872df",
				"metadata": {
					"region": "global",
					"timestamp": 1724360595038,
					"topokeys": ["source0", "source1"]
				},
				"graph": {
					"vertex": [
						{
							"uid": "vm1",
							"metadata": {
								"kind": "Node",
								"region": "cn-north-1",
								"tags": [{"key": "name", "value": "db1"}]
							}
						},
						{
							"uid": "vm2",
							"metadata": {
								"kind": "Node",
								"region": "cn-north-1",
								"tags": [{"key": "name", "value": "db2"}]
							}
						},
						{
							"uid": "vm3",
							"metadata": {
								"kind": "Node",
								"region": "cn-east-1",
								"tags": [{"key": "name", "value": "db3"}]
							}
						}
					],
					"edges": [
						{
							"uid": "vm2_source0::::vm1_source0",
							"metadata": {
								"kind": "LOCATE",
								"source_uid": "vm2",
								"target_uid": "vm1",
								"tags": [{"key": "1", "value": "1"}]
							}
						},
						{
							"uid": "vm3_source1::::vm2_source1",
							"metadata": {
								"kind": "LOCATE",
								"source_uid": "vm3",
								"target_uid": "vm2",
								"tags": [{"key": "1", "value": "1"}]
							}
						}
					]
				}
            }`
	if response != expectedResponse {
		t.Errorf("Expected response: %s, got: %s", expectedResponse, response)
	}
}

func TestPushURL(t *testing.T) {
	convey.Convey("common condition", t, func() {
		paramTest := Param{
			ParamValue: "14qwegtf12345",
			ParamName:  "applicationId",
		}

		clientConf := &Client{
			Conf: Config{
				"https://127.0.0.0:80/v1/coc--cloudmap/topology/combination",
			},
		}
		pushURL, err := clientConf.PushURL(paramTest, "1423452355699", "1423452355699")
		assert.Equal(t, err, nil)
		assert.Equal(t, pushURL, "https://127.0.0.0:80/v1/coc--cloudmap/topology/combination?applicationId=14qwegtf12345&time=1423452355699")
	})
	convey.Convey("err condition : invalid param name", t, func() {
		paramTest := Param{
			ParamValue: "14qwegtf12345",
			ParamName:  "applicatioid",
		}

		clientConf := &Client{
			Conf: Config{
				"https://127.0.0.0:80/v1/coc--cloudmap/topology/combination",
			},
		}
		_, err := clientConf.PushURL(paramTest, "1423452355699", "1423452355699")
		assert.Equal(t, "invalid param name", err.Error())
	})
}
