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

package usage_client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"
)

var URL = "http://opengemini.org/api/connect/cluster"

type Client struct {
	URL    string // Defaults to `usage_client.URL`
	logger *zap.Logger
}

func NewClient() *Client {
	return &Client{
		URL: URL,
	}
}

func (c *Client) WithLogger(log *zap.Logger) {
	c.logger = log
}

type UsageValues map[string]string

// Send sends usage statistics about the system.
func (c *Client) Send(usage UsageValues) (*http.Response, error) {
	b, err := json.Marshal(usage)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, c.URL, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		TLSHandshakeTimeout: 10 * time.Second,
	}
	cli := &http.Client{Transport: tr}
	resp, err := cli.Do(req)
	if err != nil {
		c.logger.Error("send usage failed", zap.Error(err))
		return resp, err
	}
	bytesBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp, err
	}
	err = resp.Body.Close()
	if err != nil {
		return resp, err
	}
	c.logger.Info("send usage response", zap.Int("status_code", resp.StatusCode), zap.ByteString("content", bytesBody))
	return resp, nil
}
