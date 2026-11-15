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

package run

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/stretchr/testify/require"
)

// adminMetaClient reports that an admin user exists, so authentication is enforced.
type adminMetaClient struct {
	metaclient.MetaClient
}

func (c *adminMetaClient) AdminUserExists() bool {
	return true
}

func TestHttpHandler_Ping(t *testing.T) {
	newHandler := func(authEnabled bool) *httpHandler {
		h := NewHttpHandler(&config.Store{OpsMonitor: &config.OpsMonitor{AuthEnabled: authEnabled}})
		h.metaClient = &adminMetaClient{}
		return h
	}

	for _, authEnabled := range []bool{false, true} {
		h := newHandler(authEnabled)
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, httptest.NewRequest(method, "/ping", nil))
			require.Equal(t, http.StatusNoContent, resp.Code, "auth-enabled=%v method=%s", authEnabled, method)
			require.Empty(t, resp.Body.String())
		}

		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/ping", nil))
		require.Equal(t, http.StatusBadRequest, resp.Code)
	}

	// Other endpoints still require authentication.
	resp := httptest.NewRecorder()
	newHandler(true).ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))
	require.Equal(t, http.StatusUnauthorized, resp.Code)
}
