// Copyright 2026 openGemini Authors.
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

package workingsetcache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveReturnsFilesystemError(t *testing.T) {
	cache := New(1024 * 1024)
	defer cache.Stop()
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := Save(cache, filepath.Join(parent, "cache")); err == nil {
		t.Fatal("expected an error saving a cache under a regular file")
	}
}
