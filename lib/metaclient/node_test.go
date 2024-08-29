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

package metaclient_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/metaclient"
)

func TestLoadLogicalClock(t *testing.T) {
	path := t.TempDir()
	node := metaclient.NewNode(path)

	clockPath := filepath.Join(path, metaclient.ClockFileName)
	file, er := os.Create(clockPath)
	assert.NoError(t, er)
	file.Close()

	err := node.LoadLogicalClock()
	assert.NoError(t, err)
}
