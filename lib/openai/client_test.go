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

package openai_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/openai"
	"github.com/stretchr/testify/assert"
)

// TestCreateChatCompletion_Success tests the successful call to CreateChatCompletion
func TestCreateChatCompletion_Success(t *testing.T) {
	// Create a test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request path and method
		assert.Equal(t, "/chat/completions", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		// Return a mock response
		response := openai.ChatCompletionResponse{
			ID:      "test-id",
			Object:  "chat.completion",
			Created: time.Now().Unix(),
			Model:   "gpt-3.5-turbo",
			Choices: []struct {
				Index   int                          `json:"index"`
				Message openai.ChatCompletionMessage `json:"message"`
			}([]struct {
				Index   int
				Message openai.ChatCompletionMessage
			}{
				{
					Index: 0,
					Message: openai.ChatCompletionMessage{
						Role:    openai.ChatMessageRoleAssistant,
						Content: "test response",
					},
				},
			}),
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer ts.Close()

	// Create a client
	client := openai.NewClient(ts.URL, "test-api-key")

	// Create a request
	req := openai.ChatCompletionRequest{
		Model: "gpt-3.5-turbo",
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: "test message",
			},
		},
		Temperature: openai.DefaultTemperature,
		MaxTokens:   openai.DefaultMaxTokens,
	}

	// Call the method
	resp, err := client.CreateChatCompletion(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, "test-id", resp.ID)
	assert.Equal(t, "test response", resp.Choices[0].Message.Content)
}

// TestCreateChatCompletionStream_Success tests the successful call to CreateChatCompletionStream
func TestCreateChatCompletionStream_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/chat/completions", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		// Simulate a streaming response
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		// Send streaming data
		_, _ = fmt.Fprintf(w, "data: %s\n\n", `{"id":"test-id","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"test response"}}]}`)
		_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
	}))
	defer ts.Close()

	client := openai.NewClient(ts.URL, "test-api-key")
	req := openai.ChatCompletionRequest{
		Model: "gpt-3.5-turbo",
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: "test message",
			},
		},
		Stream: true,
	}

	stream, err := client.CreateChatCompletionStream(context.Background(), req)
	assert.NoError(t, err)

	// Receive streaming data
	resp, err := stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, "test-id", resp.ID)
	assert.Equal(t, "test response", resp.Choices[0].Delta.Content)

	// Test stream closing
	err = stream.Close()
	assert.NoError(t, err)

	// Test bad response
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()
	client = openai.NewClient(ts.URL, "test-api-key")
	stream, err = client.CreateChatCompletionStream(context.Background(), req)
	assert.Error(t, err, "streaming API request failed")

	// Test error response
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block the request until timeout
		time.Sleep(time.Second)
	}))
	defer ts.Close()
	client = openai.NewClient(ts.URL, "test-api-key", openai.WithTimeout(time.Millisecond*100))
	stream, err = client.CreateChatCompletionStream(context.Background(), req)
	assert.Error(t, err, "Timeout")
}

// TestCreateChatCompletion_NetworkError tests a network error scenario
func TestCreateChatCompletion_NetworkError(t *testing.T) {
	// Create a client with an unreachable URL
	client := openai.NewClient("http://invalid.url", "test-api-key")

	req := openai.ChatCompletionRequest{
		Model: "gpt-3.5-turbo",
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: "test message",
			},
		},
	}

	_, err := client.CreateChatCompletion(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such host")
}

// TestCreateChatCompletion_Timeout tests a timeout scenario
func TestCreateChatCompletion_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block the request until timeout
		time.Sleep(time.Second)
	}))
	defer ts.Close()

	client := openai.NewClient(ts.URL, "test-api-key", openai.WithTimeout(time.Millisecond*100))

	req := openai.ChatCompletionRequest{
		Model: "gpt-3.5-turbo",
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleSystem,
				Content: "test message",
			},
		},
	}

	_, err := client.CreateChatCompletion(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Timeout")
}

// TestCreateChatCompletion_HTTPError tests an HTTP error status code
func TestCreateChatCompletion_HTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	client := openai.NewClient(ts.URL, "test-api-key")

	req := openai.ChatCompletionRequest{
		Model: "gpt-3.5-turbo",
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleSystem,
				Content: "test message",
			},
		},
	}

	_, err := client.CreateChatCompletion(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

// TestRetryPolicy tests the retry policy
func TestRetryPolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a 500 error
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	client := openai.NewClient(ts.URL, "test-api-key", openai.WithRetryPolicy(3, time.Millisecond*100))

	req := openai.ChatCompletionRequest{
		Model: "gpt-3.5-turbo",
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: "test message",
			},
		},
	}

	_, err := client.CreateChatCompletion(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

// errorReader 模拟读取时出现错误
type errorReader struct {
	io.Reader
	err error
}

func (e *errorReader) ReadBytes(delim byte) ([]byte, error) {
	return nil, e.err
}

// errorCloser 模拟关闭时出现错误
type errorCloser struct {
	io.Reader
	err error
}

func (e *errorCloser) Close() error {
	return e.err
}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}

func TestStreamReader_Recv(t *testing.T) {
	// case 1: empty response
	reader := bufio.NewReader(strings.NewReader(""))
	streamLogger := logger.NewLogger(errno.ModuleQueryEngine)
	stream := openai.NewStreamReader(nil, reader, false, streamLogger)
	_, err := stream.Recv()
	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}

	// case 2: invalid data
	invalidData := "invalid json data"
	reader = bufio.NewReader(strings.NewReader(invalidData))
	stream = openai.NewStreamReader(nil, reader, false, streamLogger)
	_, err = stream.Recv()
	assert.Error(t, err, io.EOF)

	// case 3: network error
	errReader := &errorReader{err: errors.New("network error")}
	resp := &http.Response{Body: &errorCloser{err: errors.New("close error")}}
	reader = bufio.NewReader(errReader)
	stream = openai.NewStreamReader(resp, reader, false, streamLogger)
	_, err = stream.Recv()
	assert.Error(t, err, "error reading stream")
	err = stream.Close()
	assert.Error(t, err, "resp")
	_, err = stream.Recv()
	assert.Error(t, io.EOF)
}

func TestStreamReader_Close(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(""))
	streamLogger := logger.NewLogger(errno.ModuleQueryEngine)
	resp := &http.Response{Body: &errorCloser{err: errors.New("close error")}}
	stream := openai.NewStreamReader(resp, reader, false, streamLogger)
	err := stream.Close()
	assert.Error(t, err, "resp")
	err = stream.Close()
	if err != nil {
		t.Errorf("Unexpected error on second close: %v", err)
	}
}
