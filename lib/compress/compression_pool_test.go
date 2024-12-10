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
