/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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

package compress

import (
	"bytes"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func TestGzipWriterPool(t *testing.T) {
	for i := 0; i < 2; i++ {
		var buf bytes.Buffer
		writer, release := GetGzipWriter(&buf)
		_, err := writer.Write([]byte("test data"))
		assert.NoError(t, err)
		release()

		reader, err := GetGzipReader(&buf)
		assert.NoError(t, err)
		result := new(bytes.Buffer)
		_, err = io.Copy(result, reader)
		assert.NoError(t, err)
		assert.Equal(t, "test data", result.String())
		PutGzipReader(reader)

		reader, err = GetGzipReader(&buf)
		assert.Error(t, err)
	}
}

func TestZstdWriterPool(t *testing.T) {
	var buf bytes.Buffer
	writer, release := GetZstdWriter(&buf)
	_, err := writer.Write([]byte("test data"))
	assert.NoError(t, err)
	release()

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
	writer, release := GetSnappyWriter(&buf)
	_, err := writer.Write([]byte("test data"))
	assert.NoError(t, err)
	release()

	reader := snappy.NewReader(&buf)
	result := new(bytes.Buffer)
	_, err = io.Copy(result, reader)
	assert.NoError(t, err)
	assert.Equal(t, "test data", result.String())
}

func TestLz4WriterPool(t *testing.T) {
	var buf bytes.Buffer
	writer, release := GetLz4Writer(&buf)
	_, err := writer.Write([]byte("test data"))
	assert.NoError(t, err)
	release()

	reader := lz4.NewReader(&buf)
	result := new(bytes.Buffer)
	_, err = io.Copy(result, reader)
	assert.NoError(t, err)
	assert.Equal(t, "test data", result.String())
}
