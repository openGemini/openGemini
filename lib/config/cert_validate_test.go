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
	"fmt"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
)

func TestExpireCertificate(t *testing.T) {
	if !assert.NoError(t, execCommand([]string{genCrtExpire})) {
		return
	}

	cfg := config.NewSpdy()
	cfg.TLSEnable = true
	cfg.TLSCertificate = "/tmp/gemini/expire.crt"
	cfg.TLSPrivateKey = "/tmp/gemini/private_expire.key"
	config.FormatSpdy(&cfg)

	cv := config.NewCertValidator(cfg.TLSCertificate, cfg.TLSPrivateKey)
	assert.NoError(t, cv.Validate())
}

func Test1024BitCertificate(t *testing.T) {
	if !assert.NoError(t, execCommand([]string{genCrt1024})) {
		return
	}

	cfg := config.NewSpdy()
	cfg.TLSEnable = true
	cfg.TLSCertificate = "/tmp/gemini/1024.crt"
	cfg.TLSPrivateKey = "/tmp/gemini/private_1024.key"
	config.FormatSpdy(&cfg)

	cv := config.NewCertValidator(cfg.TLSCertificate, cfg.TLSPrivateKey)
	err := cv.Validate()
	fmt.Println(err)
	if err == nil || !strings.HasPrefix(err.Error(), "public key is too short") {
		t.Fatalf("failed, expected error: `public key is too short ...`")
	}
}

func TestSha1AlgoCertificate(t *testing.T) {
	if !assert.NoError(t, execCommand([]string{genCrtSHA1})) {
		return
	}

	cfg := config.NewSpdy()
	cfg.TLSEnable = true
	cfg.TLSCertificate = "/tmp/gemini/sha1.crt"
	cfg.TLSPrivateKey = "/tmp/gemini/private_sha1.key"
	config.FormatSpdy(&cfg)

	cv := config.NewCertValidator(cfg.TLSCertificate, cfg.TLSPrivateKey)
	err := cv.Validate()
	fmt.Println(err)
	if err == nil || !strings.HasPrefix(err.Error(), "unsupported signature algorithm") {
		t.Fatalf("failed, expected error: `unsupported signature algorithm: xxx`")
	}
}

const (
	genCrt1024   = `openssl req -newkey rsa:1024 -nodes -keyout /tmp/gemini/private_1024.key -x509 -days 365 -out /tmp/gemini/1024.crt ` + subj
	genCrtSHA1   = `openssl req -newkey rsa:2048 -nodes -keyout /tmp/gemini/private_sha1.key -x509 -days 365 -sha1 -out /tmp/gemini/sha1.crt ` + subj
	genCrtExpire = `openssl req -newkey rsa:2048 -nodes -keyout /tmp/gemini/private_expire.key -x509 -days 3 -out /tmp/gemini/expire.crt ` + subj
)
