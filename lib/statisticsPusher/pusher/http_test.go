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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHttpConfig(t *testing.T) {
	cfg := &HttpConfig{
		Username: "admin",
		Password: "abc123",
	}
	name, pwd := cfg.BasicAuth()
	assert.Equal(t, name, cfg.Username)
	assert.Equal(t, pwd, cfg.Password)

	cfg.Username = ""
	name, pwd = cfg.BasicAuth()
	assert.Equal(t, name, "")
	assert.Equal(t, pwd, "")
}

func TestSetBasicAuth(t *testing.T) {
	cfg := &HttpConfig{
		Username: "admin",
		Password: "abc123",
	}

	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1", bytes.NewBuffer(nil))
	obj := NewHttp(cfg, nil)
	obj.setBasicAuth(req)

	assert.NotEmpty(t, req.Header["Authorization"])
}
