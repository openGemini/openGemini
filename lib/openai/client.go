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

package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

const (
	DefaultTimeOut     = 30 * time.Second
	DefaultMaxRetries  = 3
	DefaultRetryDelay  = 1 * time.Second
	DefaultTemperature = 0.1
	DefaultMaxTokens   = 10000
)

// Client interface defines the contract for chat completion service
type Client interface {
	CreateChatCompletion(ctx context.Context, req ChatCompletionRequest) (ChatCompletionResponse, error)
	CreateChatCompletionStream(ctx context.Context, req ChatCompletionRequest) (Stream, error)
}

// ClientOption configures the client
type ClientOption func(*openAIClient)

// Stream handles streaming responses
type Stream interface {
	Recv() (ChatCompletionStreamResponse, error)
	Close() error
}

// Chat message roles
const (
	ChatMessageRoleSystem    = "system"
	ChatMessageRoleUser      = "user"
	ChatMessageRoleAssistant = "assistant"
)

// ChatCompletionMessage represents a single message in conversation
type ChatCompletionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatCompletionRequest defines chat completion parameters
type ChatCompletionRequest struct {
	Model       string                  `json:"model"`
	Messages    []ChatCompletionMessage `json:"messages"`
	Stream      bool                    `json:"stream,omitempty"`
	Temperature float64                 `json:"temperature,omitempty"` // Controls randomness (0-2)
	MaxTokens   int                     `json:"max_tokens,omitempty"`  // Maximum tokens to generate
}

// ChatCompletionResponse contains full API response
type ChatCompletionResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index   int                   `json:"index"`
		Message ChatCompletionMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage,omitempty"`
}

// ChatCompletionStreamResponse contains streaming chunk
type ChatCompletionStreamResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int                   `json:"index"`
		Delta        ChatCompletionMessage `json:"delta"`
		FinishReason *string               `json:"finish_reason,omitempty"`
	} `json:"choices"`
}

// streamReader implements Stream interface using bufio.Reader
type streamReader struct {
	resp   *http.Response
	reader *bufio.Reader // Use bufio.Reader which has ReadBytes method
	closed bool
	logger *logger.Logger
}

func NewStreamReader(resp *http.Response, reader *bufio.Reader, closed bool, logger *logger.Logger) Stream {
	return &streamReader{
		resp:   resp,
		reader: reader,
		closed: closed,
		logger: logger,
	}
}

// Recv gets next streaming chunk using bufio.Reader
func (s *streamReader) Recv() (ChatCompletionStreamResponse, error) {
	if s.closed {
		return ChatCompletionStreamResponse{}, io.EOF
	}

	var response ChatCompletionStreamResponse
	var line []byte
	var err error

	// Read lines until we find a data chunk
	for {
		// Use bufio.Reader's ReadBytes method which exists
		line, err = s.reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return response, io.EOF
			}
			return response, fmt.Errorf("error reading stream: %w", err)
		}

		// Process SSE format
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue // Skip empty lines
		}
		if bytes.HasPrefix(line, []byte(":")) {
			continue // Skip comment lines
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			// Extract the data part
			line = bytes.TrimPrefix(line, []byte("data:"))
			line = bytes.TrimSpace(line)

			// Check for end signal
			if string(line) == "[DONE]" {
				return response, io.EOF
			}
			break // Found our data line
		}
	}

	// Parse the JSON data
	if err := json.Unmarshal(line, &response); err != nil {
		s.logger.Error("Failed to parse streaming response", zap.Error(err), zap.String("data", string(line)))
		return response, fmt.Errorf("failed to parse streaming response: %w", err)
	}
	return response, nil
}

// Close cleans up stream resources
func (s *streamReader) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	if err := s.resp.Body.Close(); err != nil {
		s.logger.Error("Error closing response body", zap.Error(err))
		return err
	}
	return nil
}

// openAIClient implements the Client interface
type openAIClient struct {
	apiKey     string
	endPoint   string
	maxRetries int
	httpClient *http.Client
	logger     *logger.Logger
	timeout    time.Duration
	retryDelay time.Duration
}

// NewClient creates a new client with options
func NewClient(endPoint, apiKey string, opts ...ClientOption) Client {
	client := &openAIClient{
		httpClient: &http.Client{},
		logger:     logger.NewLogger(errno.ModuleQueryEngine),
		apiKey:     apiKey,
		endPoint:   endPoint,
		timeout:    DefaultTimeOut,
		maxRetries: DefaultMaxRetries,
		retryDelay: DefaultRetryDelay,
	}

	for _, opt := range opts {
		opt(client)
	}

	client.httpClient.Timeout = client.timeout

	return client
}

// WithTimeout sets request timeout
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *openAIClient) {
		c.timeout = timeout
	}
}

// WithRetryPolicy configures retry behavior
func WithRetryPolicy(maxRetries int, retryDelay time.Duration) ClientOption {
	return func(c *openAIClient) {
		c.maxRetries = maxRetries
		c.retryDelay = retryDelay
	}
}

// CreateChatCompletion sends non-streaming request
func (c *openAIClient) CreateChatCompletion(ctx context.Context, req ChatCompletionRequest) (ChatCompletionResponse, error) {
	var response ChatCompletionResponse
	req.Stream = false
	var httpStatus int
	err := c.retryWithBackoff(ctx, func() (int, error) {
		resp, err := c.sendRequest(ctx, "POST", "/chat/completions", req)
		if resp != nil {
			httpStatus = resp.StatusCode
		} else {
			httpStatus = http.StatusNotFound
		}
		if err != nil {
			return httpStatus, err
		}
		defer func() { err = resp.Body.Close() }()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			c.logger.Error("Failed to read response body", zap.Error(err))
			return httpStatus, fmt.Errorf("failed to read response body: %w", err)
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			c.logger.Error(fmt.Sprintf("API request failed with status code: %d", resp.StatusCode))
			return httpStatus, fmt.Errorf("API request failed with status code: %d", resp.StatusCode)
		}

		if err = json.Unmarshal(body, &response); err != nil {
			c.logger.Error("Failed to parse response", zap.Error(err))
			return httpStatus, fmt.Errorf("failed to parse response: %w", err)
		}
		return httpStatus, nil
	})

	return response, err
}

// CreateChatCompletionStream sends streaming request
func (c *openAIClient) CreateChatCompletionStream(ctx context.Context, req ChatCompletionRequest) (Stream, error) {
	req.Stream = true

	resp, err := c.sendRequest(ctx, "POST", "/chat/completions", req)
	if err != nil {
		c.logger.Error("Failed to send streaming request", zap.Error(err))
		return nil, err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		err = resp.Body.Close()
		c.logger.Error(fmt.Sprintf("Streaming API request failed with status code: %d", resp.StatusCode), zap.Error(err))
		return nil, fmt.Errorf("streaming API request failed with status code: %d, resp close err: %v", resp.StatusCode, err)
	}

	// Use bufio.Reader instead of json.Decoder for stream reading
	stream := &streamReader{
		resp:   resp,
		reader: bufio.NewReader(resp.Body), // Correctly initialize with bufio.Reader
		logger: c.logger,
	}

	go func() {
		<-ctx.Done()
		if err := stream.Close(); err != nil {
			c.logger.Warn("Error closing stream on context cancellation", zap.Error(err))
		}
	}()

	return stream, nil
}

// sendRequest handles HTTP communication
func (c *openAIClient) sendRequest(ctx context.Context, method, path string, requestBody interface{}) (*http.Response, error) {
	url := c.endPoint + path

	body, err := json.Marshal(requestBody)
	if err != nil {
		c.logger.Error("Failed to serialize request body", zap.Error(err))
		return nil, fmt.Errorf("failed to serialize request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		c.logger.Error("Failed to create request", zap.Error(err))
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			c.logger.Error("Request cancelled", zap.Error(err))
			return nil, fmt.Errorf("request cancelled: %w", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error("Request timed out", zap.Error(err))
			return nil, fmt.Errorf("request timed out: %w", err)
		}

		c.logger.Error("Failed to send request", zap.Error(err))
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	return resp, nil
}

// retryWithBackoff implements retry logic
func (c *openAIClient) retryWithBackoff(ctx context.Context, fn func() (int, error)) error {
	var lastErr error
	var httpStatus int

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(c.retryDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		httpStatus, lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if !c.shouldRetry(httpStatus, lastErr) {
			break
		}

		c.logger.Error(fmt.Sprintf("Attempt %d failed, will retry", attempt+1))
	}

	return lastErr
}

// shouldRetry determines if retry is needed
func (c *openAIClient) shouldRetry(httpStatus int, err error) bool {
	if httpStatus == http.StatusInternalServerError ||
		httpStatus == http.StatusBadGateway ||
		httpStatus == http.StatusServiceUnavailable ||
		httpStatus == http.StatusGatewayTimeout {
		return true
	}
	if strings.Contains(err.Error(), "timeout") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "network error") {
		return true
	}
	return false
}
