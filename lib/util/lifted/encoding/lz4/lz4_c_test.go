/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package lz4

import (
	"crypto/rand"
	"testing"
)

func TestCompressDefaultDecompressSafe(t *testing.T) {
	data := make([]byte, 4096)
	rand.Read(data)

	compressed := make([]byte, CompressBlockBound(4096))
	size, err := CompressBlock(data, compressed)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	decompressed := make([]byte, 4096)
	if _, err := DecompressSafe(compressed[:size], decompressed); err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}
}
