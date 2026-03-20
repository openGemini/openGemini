// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package httpd

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestSanitize_SelectQuery tests the case where the query starts with "select" (lowercase).
// It ensures that the query is not sanitized and RawQuery remains unchanged.
func TestSanitize_SelectQuery(t *testing.T) {
	req := &http.Request{
		URL: &url.URL{
			RawQuery: "q=select * from table",
		},
	}
	sanitize(req)
	if req.URL.RawQuery != "q=select * from table" {
		t.Error("RawQuery should not be updated")
	}
}

// TestSanitize_NonSelectQuery tests the case where the query does not start with "select".
// It verifies that the query is sanitized and RawQuery is updated.
func TestSanitize_NonSelectQuery(t *testing.T) {
	req := &http.Request{
		URL: &url.URL{
			RawQuery: "q=show databases",
		},
	}
	sanitize(req)
	if req.URL.RawQuery == "q=show databases" {
		t.Error("RawQuery should be updated")
	}
}

// TestSanitize_NoQuery tests the case where there is no "q" parameter in the request.
// It ensures that the method does nothing and RawQuery remains unchanged.
func TestSanitize_NoQuery(t *testing.T) {
	req := &http.Request{
		URL: &url.URL{},
	}
	sanitize(req)
	if req.URL.RawQuery != "" {
		t.Error("RawQuery should remain empty")
	}
}

// TestSanitize_EmptyQuery tests the case where the query is an empty string.
// It ensures that the method handles empty queries gracefully without errors.
func TestSanitize_EmptyQuery(t *testing.T) {
	req := &http.Request{
		URL: &url.URL{
			RawQuery: "q=",
		},
	}
	sanitize(req)
	values := req.URL.Query()
	if values.Get("q") != "" {
		t.Error("Expected q to remain empty")
	}
	if req.URL.RawQuery != "q=" {
		t.Error("RawQuery should remain unchanged")
	}
}

var singleGroupBy181 = "SELECT max(usage_user) from cpu where (hostname = 'host_3324' or hostname = 'host_248' or hostname = 'host_3651' or hostname = 'host_1141' or hostname = 'host_2826' or hostname = 'host_1705' or hostname = 'host_4163' or hostname = 'host_1275') and time >= '2021-11-02T03:18:45Z' and time < '2021-11-02T04:18:45Z' group by time(1m)"

func BenchmarkMethod_toLower_TrimSpace_HasPrefix(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(singleGroupBy181)), "select")
	}
}

func BenchmarkMethod_TrimSpace_HasPrefix(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := strings.TrimSpace(singleGroupBy181)
		_ = strings.HasPrefix(q, "select") || strings.HasPrefix(q, "SELECT")
	}
}

func BenchmarkMethod_HasPrefix(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = strings.HasPrefix(singleGroupBy181, "select") || strings.HasPrefix(singleGroupBy181, "SELECT")
	}
}

func BenchmarkMethod_Contains(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = strings.Contains(singleGroupBy181, "password") || strings.HasPrefix(singleGroupBy181, "PASSWORD")
	}
}
