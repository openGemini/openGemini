//go:build streamfs
// +build streamfs

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

func TestStreamMmap(t *testing.T) {
	data, err := Mmap(0, 0, 0)
	if !assert.NoError(t, err) {
		return
	}

	err = MUnmap(data)
	if !assert.NoError(t, err) {
		return
	}
}

func TestStreamFadvise(t *testing.T) {
	err := Fadvise(0, 0, 0, 0)
	if !assert.NoError(t, err) {
		return
	}
}

func TestStreamFdatasync(t *testing.T) {
	err := Fdatasync(nil)
	if !assert.Error(t, err) {
		return
	}
}
