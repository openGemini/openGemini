// Copyright 2026 openGemini Authors.
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

package httpserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openGemini/openGemini/lib/httpserver"
	"github.com/stretchr/testify/require"
)

func TestIsPingRequest(t *testing.T) {
	require.True(t, httpserver.IsPingRequest(httptest.NewRequest(http.MethodGet, "/ping", nil)))
	require.True(t, httpserver.IsPingRequest(httptest.NewRequest(http.MethodHead, "/ping", nil)))
	require.True(t, httpserver.IsPingRequest(httptest.NewRequest(http.MethodGet, "/ping?verbose=true", nil)))

	require.False(t, httpserver.IsPingRequest(httptest.NewRequest(http.MethodPost, "/ping", nil)))
	require.False(t, httpserver.IsPingRequest(httptest.NewRequest(http.MethodGet, "/ping/", nil)))
	require.False(t, httpserver.IsPingRequest(httptest.NewRequest(http.MethodGet, "/debug/vars", nil)))
}

func TestServePing(t *testing.T) {
	resp := httptest.NewRecorder()
	httpserver.ServePing(resp)
	require.Equal(t, http.StatusNoContent, resp.Code)
	require.Empty(t, resp.Body.String())
}
