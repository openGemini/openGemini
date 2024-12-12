// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package httpd

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

func TestCompressFilter_Gzip(t *testing.T) {
	handler := compressFilter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test response"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("expected gzip encoding, got %s", resp.Header.Get("Content-Encoding"))
	}

	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	body, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("failed to read gzip body: %v", err)
	}

	if string(body) != "test response" {
		t.Errorf("expected body 'test response', got %s", string(body))
	}
}

func TestCompressFilter_Zstd(t *testing.T) {
	handler := compressFilter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test response"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "zstd")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "zstd" {
		t.Errorf("expected zstd encoding, got %s", resp.Header.Get("Content-Encoding"))
	}

	zstdReader, err := zstd.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("failed to create zstd reader: %v", err)
	}
	defer zstdReader.Close()

	body, err := io.ReadAll(zstdReader)
	if err != nil {
		t.Fatalf("failed to read zstd body: %v", err)
	}

	if string(body) != "test response" {
		t.Errorf("expected body 'test response', got %s", string(body))
	}
}

func TestCompressFilter_Snappy(t *testing.T) {
	handler := compressFilter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test response"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "snappy")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "snappy" {
		t.Errorf("expected snappy encoding, got %s", resp.Header.Get("Content-Encoding"))
	}

	snappyReader := snappy.NewReader(resp.Body)

	body, err := io.ReadAll(snappyReader)
	if err != nil {
		t.Fatalf("failed to read snappy body: %v", err)
	}

	if string(body) != "test response" {
		t.Errorf("expected body 'test response', got %s", string(body))
	}

}

func TestCompressFilter_NoEncoding(t *testing.T) {
	handler := compressFilter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test response"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("expected no encoding, got %s", resp.Header.Get("Content-Encoding"))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	if string(body) != "test response" {
		t.Errorf("expected body 'test response', got %s", string(body))
	}
}

func TestCompressFilter_UnsupportedEncoding(t *testing.T) {
	handler := compressFilter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test response"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "br")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("expected no encoding, got %s", resp.Header.Get("Content-Encoding"))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	if string(body) != "test response" {
		t.Errorf("expected body 'test response', got %s", string(body))
	}
}
