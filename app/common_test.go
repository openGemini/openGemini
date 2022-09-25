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

package app_test

import (
	"os"
	"path"
	"testing"

	"github.com/openGemini/openGemini/app"
	"github.com/stretchr/testify/assert"
)

func TestWritePIDFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pid")
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(tempDir)

	pidfile := path.Join(tempDir, "meta", "meta.pid")
	err = app.WritePIDFile(pidfile)
	if !assert.NoError(t, err) {
		return
	}
}
