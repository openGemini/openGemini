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

package sherlock

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRotateProfilesFiles(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create some test profile files
	createTestProfileFile(tempDir, "ts-store.cpu.1.pb.gz", time.Now().Add(-48*time.Hour)) // Expired
	createTestProfileFile(tempDir, "ts-store.cpu.2.pb.gz", time.Now().Add(-24*time.Hour)) // Expired
	createTestProfileFile(tempDir, "ts-store.cpu.3.pb.gz", time.Now())                    // Not expired
	createTestProfileFile(tempDir, "ts-store.cpu.4.pb.gz", time.Now())                    // Not expired
	createTestProfileFile(tempDir, "ts-store.cpu.5.pb.gz", time.Now())                    // Not expired

	// Test rotateProfilesFiles
	err := rotateProfilesFiles(tempDir, CPU, 3, 36*time.Hour)
	assert.NoError(t, err, "Error rotating profile files")

	// Validate the result
	files, err := listProfileFiles(tempDir, CPU)
	assert.NoError(t, err, "Error listing profile files")
	// Expecting 3 files to remain after rotation
	assert.Equal(t, 3, len(files), "Expected 3 profile files after rotation")
}

// createTestProfileFile creates a test profile file with the specified modification time
func createTestProfileFile(directory, filename string, modTime time.Time) {
	filePath := filepath.Join(directory, filename)
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	file.Close()

	// Set the file's modification time
	err = os.Chtimes(filePath, modTime, modTime)
	if err != nil {
		panic(err)
	}
}
