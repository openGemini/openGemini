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

package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestService_Validate(t *testing.T) {
	conf := NewHierarchicalConfig()
	conf.RunInterval = 0
	err := conf.Validate()
	assert.Equal(t, err, errors.New("run-interval must be positive"))

	conf.RunInterval = 10
	conf.MaxProcessN = 0
	err = conf.Validate()
	assert.Equal(t, err, errors.New("max-process-hs-number must be positive"))
}
