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

package pusher

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
)

type Http struct {
	conf         *HttpConfig
	logger       *logger.Logger
	storeCreated bool
}

type HttpConfig struct {
	Auth     []string
	Https    bool
	EndPoint string
	Database string
	RP       string
	Duration time.Duration
	RepN     int
	Gzipped  bool
}

// #nosec
var defaultHttpClient = &http.Client{Transport: &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}}

func (c *HttpConfig) PushURL() string {
	schema := "http"
	if c.Https {
		schema = "https"
	}
	return fmt.Sprintf("%s://%s/write?db=%s&rp=%s", schema, c.EndPoint, c.Database, c.RP)
}

func (c *HttpConfig) CreateURL() string {
	schema := "http"
	if c.Https {
		schema = "https"
	}
	return fmt.Sprintf("%s://%s/query", schema, c.EndPoint)
}

func (c *HttpConfig) BasicAuth() (string, string) {
	if len(c.Auth) != 2 {
		return "", ""
	}

	return c.Auth[0], c.Auth[1]
}

func NewHttp(conf *HttpConfig, logger *logger.Logger) *Http {
	return &Http{conf: conf, logger: logger}
}

func (p *Http) Push(data []byte) error {
	if err := p.createInternalStorage(); err != nil {
		return err
	}

	if p.conf.Gzipped {
		buf := bytes.Buffer{}
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(data); err != nil {
			return err
		}

		if err := zw.Close(); err != nil {
			return err
		}

		data = buf.Bytes()
	}

	// new write request
	req, err := http.NewRequest("POST", p.conf.PushURL(), bytes.NewReader(data))
	if err != nil {
		return err
	}
	p.setBasicAuth(req)

	if p.conf.Gzipped {
		req.Header.Set("Content-Encoding", "gzip")
	}
	req.Header.Set("Content-Type", "")

	// send statistics data
	writeResp, err := defaultHttpClient.Do(req)
	if err != nil {
		return err
	}
	_ = writeResp.Body.Close()
	return nil
}

func (p *Http) createInternalDatabase() error {
	buffer := fmt.Sprintf("CREATE DATABASE %s", p.conf.Database)
	data := url.Values{}
	data.Set("q", buffer)

	return p.post(p.conf.CreateURL(), data)
}

func (p *Http) createInternalRP() error {
	buffer := fmt.Sprintf("CREATE retention policy %s on %s duration %dh replication %d default",
		p.conf.RP, p.conf.Database, int(p.conf.Duration.Hours()), p.conf.RepN)
	data := url.Values{}
	data.Set("q", buffer)

	return p.post(p.conf.CreateURL(), data)
}

func (p *Http) post(url string, data url.Values) error {
	buf := data.Encode()
	req, err := http.NewRequest("POST", url, strings.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	p.setBasicAuth(req)

	writeResp, err := defaultHttpClient.Do(req)
	if err != nil {
		return err
	}
	_ = writeResp.Body.Close()

	return nil
}

// createInternalStorage ensures the internal storage has been created.
func (p *Http) createInternalStorage() error {
	if p.storeCreated {
		return nil
	}

	if p.conf.Database == "" {
		return errors.New("database must entered")
	}

	err := p.createInternalDatabase()
	if err != nil {
		return err
	}

	if p.conf.RP != "" {
		err := p.createInternalRP()
		if err != nil {
			return err
		}
	}

	// Mark storage creation complete.
	p.storeCreated = true
	return nil
}

func (p *Http) setBasicAuth(req *http.Request) {
	username, passwd := p.conf.BasicAuth()
	if username != "" && passwd != "" {
		req.SetBasicAuth(username, passwd)
	}
}

func (p *Http) Stop() {

}
