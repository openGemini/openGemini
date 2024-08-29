// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package colstore

import (
	"path/filepath"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
)

func TestBloomFilterBuilder(t *testing.T) {
	testCompDir := t.TempDir()
	filePath := filepath.Join(testCompDir, "chunkBuilder.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer func() {
		_ = fileops.Remove(filePath)
	}()

	data := []byte("write bloomfilter test")
	lockPath := ""
	indexBuilder := NewSkipIndexBuilder(&lockPath, filePath)
	defer indexBuilder.Reset()
	err := indexBuilder.WriteData(data)
	if err != nil {
		t.Fatal("write data error")
	}
}
