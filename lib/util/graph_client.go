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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Config struct {
	Path string
}

type Client struct {
	Conf Config
}

var path string

type Param struct {
	ParamName  string
	ParamValue string
}

func SetTopoManagerUrl(topoManagerUrl string) {
	path = topoManagerUrl
}

var HttpsClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}

func GetClientConf() *Client {
	return &Client{
		Conf: Config{
			Path: path,
		},
	}
}

func (c *Client) PushURL(param Param, startTime, endTime string) (string, error) {
	query := url.Values{}
	switch {
	case startTime == endTime && endTime != "":
		query.Add("time", startTime)
	case startTime != "" && endTime != "":
		query.Add("startTime", startTime)
		query.Add("endTime", endTime)
	default:
	}

	if param.ParamName != "" && param.ParamValue != "" {
		if param.ParamName != "applicationId" {
			return "", errors.New("invalid param name")
		}
		query.Add(param.ParamName, param.ParamValue)
	}
	u, err := url.Parse(c.Conf.Path)
	if err != nil {
		return "", fmt.Errorf("invalid base path: %s", c.Conf.Path)
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func (c *Client) SendGetRequest(httpClient *http.Client, param Param, startTime string, endTime string) (string, error) {
	pushURL, err := c.PushURL(param, startTime, endTime)
	if err != nil {
		return "", err
	}
	// todo: Implement a feature to periodically obtain the X-Auth-Token
	req, err := http.NewRequest("GET", pushURL, nil)
	req.Header.Set("X-Auth-Token", "")
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return "", err
	}

	writeResp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer writeResp.Body.Close()

	body, err := io.ReadAll(writeResp.Body)
	if writeResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("http get request failed, status code: %d, reason: %s, response body: %s", writeResp.StatusCode, writeResp.Status, string(body))
	}

	if err != nil {
		return "", err
	}

	return string(body), nil
}
