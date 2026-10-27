// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package util_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
)

func TestHttpError(t *testing.T) {
	for _, tc := range []struct {
		message string
		status  int
	}{
		{"StatusUnauthorized", http.StatusUnauthorized},
		{"StatusNotFound", http.StatusNotFound},
	} {
		t.Run(tc.message, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpd.HttpError(w, tc.message, tc.status)
			}))
			defer server.Close()
			resp, err := server.Client().Get(server.URL + "/test")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.status || !strings.Contains(string(body), tc.message) {
				t.Fatalf("unexpected HTTP error response: status=%d body=%s", resp.StatusCode, body)
			}
		})
	}
}
