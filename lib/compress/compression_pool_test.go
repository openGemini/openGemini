// Copyright 2024 openGemini Authors
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

package compress

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func TestGzipWriterPool(t *testing.T) {
	var buf bytes.Buffer
	writer := GetGzipWriter(&buf)
	_, err := writer.Write([]byte("test data"))
	assert.NoError(t, err)
	PutGzipWriter(writer)

	reader, err := gzip.NewReader(&buf)
	assert.NoError(t, err)
	defer reader.Close()

	result := new(bytes.Buffer)
	_, err = io.Copy(result, reader)
	assert.NoError(t, err)
	assert.Equal(t, "test data", result.String())
}

func TestZstdWriterPool(t *testing.T) {
	var buf bytes.Buffer
	writer := GetZstdWriter(&buf)
	_, err := writer.Write([]byte("test data"))
	assert.NoError(t, err)
	PutZstdWriter(writer)

	reader, err := zstd.NewReader(&buf)
	assert.NoError(t, err)
	defer reader.Close()

	result := new(bytes.Buffer)
	_, err = io.Copy(result, reader)
	assert.NoError(t, err)
	assert.Equal(t, "test data", result.String())
}

func TestSnappyWriterPool(t *testing.T) {
	var buf bytes.Buffer
	writer := GetSnappyWriter(&buf)
	_, err := writer.Write([]byte("test data"))
	assert.NoError(t, err)
	PutSnappyWriter(writer)

	reader := snappy.NewReader(&buf)
	result := new(bytes.Buffer)
	_, err = io.Copy(result, reader)
	assert.NoError(t, err)
	assert.Equal(t, "test data", result.String())
}

func TestLz4WriterPool(t *testing.T) {
	var buf bytes.Buffer
	writer := GetLz4Writer(&buf)
	_, err := writer.Write([]byte("test data"))
	assert.NoError(t, err)
	PutLz4Writer(writer)

	reader := lz4.NewReader(&buf)
	result := new(bytes.Buffer)
	_, err = io.Copy(result, reader)
	assert.NoError(t, err)
	assert.Equal(t, "test data", result.String())
}
