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

func BenchmarkLz4(b *testing.B) {
	size := 4096
	data := make([]byte, size)
	rand.Read(data)
	compressed := make([]byte, CompressBlockBound(size))
	decompressed := make([]byte, size)
	for i := 0; i < b.N; i++ {
		compressAndDecompress(data, decompressed, compressed, b)
	}
}

type logger interface {
	Fatalf(format string, args ...any)
}

func compressAndDecompress(src, dst, buf []byte, t logger) {
	size, err := CompressBlock(src, buf)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if _, err := DecompressSafe(buf[:size], dst); err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}
}

func TestCompressDefaultDecompressSafe(t *testing.T) {
	size := 4096
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	compressed := make([]byte, CompressBlockBound(size))
	decompressed := make([]byte, size)
	compressAndDecompress(data, decompressed, compressed, t)
	for i := 0; i < len(decompressed); i++ {
		if data[i] != decompressed[i] {
			t.Fatalf("unexpect")
		}
	}
}
