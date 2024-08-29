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

package config_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/stretchr/testify/assert"
)

func TestSpdy(t *testing.T) {
	conf := config.NewSpdy()
	conf.RecvWindowSize = 0
	conf.ConcurrentAcceptSession = 10
	conf.OpenSessionTimeout = 10
	conf.SessionSelectTimeout = 0

	config.FormatSpdy(&conf)
	assert.Equal(t, config.DefaultRecvWindowSize, conf.RecvWindowSize)
	assert.Equal(t, config.MinConcurrentAcceptSession, conf.ConcurrentAcceptSession)
	assert.Equal(t, config.MinOpenSessionTimeout, conf.OpenSessionTimeout)
	assert.Equal(t, config.DefaultSessionSelectTimeout, conf.SessionSelectTimeout)

	assert.Equal(t, time.Duration(conf.OpenSessionTimeout), conf.GetOpenSessionTimeout())
	assert.Equal(t, time.Duration(conf.SessionSelectTimeout), conf.GetSessionSelectTimeout())
	assert.Equal(t, time.Duration(conf.TCPDialTimeout), conf.GetTCPDialTimeout())
}

func TestSpdyTLSConfig(t *testing.T) {
	var err error
	if !assert.NoError(t, execCommand([]string{
		genCAKey, genCACsr, genCACer,
		genServerKey, genServerCsr, genServerCer,
		genClientKey, genClientCsr, genClientCer})) {
		return
	}

	conf := config.NewSpdy()
	tls, err := conf.NewTLSConfig()
	assert.EqualError(t, err, errno.NewError(errno.InvalidTLSConfig).Error())
	assert.Empty(t, tls)

	conf.TLSEnable = true
	conf.TLSClientAuth = true
	conf.TLSCertificate = "/tmp/gemini/server.cer"
	conf.TLSPrivateKey = "/tmp/gemini/server.key"
	conf.TLSClientCertificate = "/tmp/gemini/client.cer"
	conf.TLSClientPrivateKey = "/tmp/gemini/client.key"
	conf.TLSCARoot = "/tmp/gemini/ca.cer"
	config.FormatSpdy(&conf)

	assert.NoError(t, conf.Validate())

	tls, err = conf.NewTLSConfig()
	assert.NoError(t, err)

	_, err = conf.NewClientTLSConfig()
	assert.NoError(t, err)
}

const (
	subj = `-subj "/C=CN/ST=BJ/L=local/O=huawei/OU=gemini/CN=gemini"`
	req  = `openssl x509 -req -days 365 -sha256 -extensions`
	ca   = `-CA /tmp/gemini/ca.cer -CAkey /tmp/gemini/ca.key -CAcreateserial`

	genServerKey = "openssl genrsa -out /tmp/gemini/server.key 2048"
	genServerCsr = `openssl req -new -key /tmp/gemini/server.key -out /tmp/gemini/server.csr ` + subj
	genServerCer = req + ` v3_req ` + ca + ` -in /tmp/gemini/server.csr -out /tmp/gemini/server.cer`

	genClientKey = "openssl genrsa -out /tmp/gemini/client.key 2048"
	genClientCsr = `openssl req -new -key /tmp/gemini/client.key -out /tmp/gemini/client.csr ` + subj
	genClientCer = req + ` v3_req ` + ca + ` -in /tmp/gemini/client.csr -out /tmp/gemini/client.cer`

	genCAKey = "openssl genrsa -out /tmp/gemini/ca.key 2048"
	genCACsr = `openssl req -new -key /tmp/gemini/ca.key -out /tmp/gemini/ca.csr ` + subj
	genCACer = req + ` v3_ca -signkey /tmp/gemini/ca.key -in /tmp/gemini/ca.csr -out /tmp/gemini/ca.cer`
)

func execCommand(cmdList []string) error {
	_ = os.Mkdir("/tmp/gemini", 0700)

	for _, c := range cmdList {
		err := exec.Command("/bin/bash", "-c", c).Run()
		if err != nil {
			return err
		}
	}

	return nil
}
