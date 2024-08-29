// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package usage_client_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openGemini/openGemini/lib/usage_client"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_ClientSend(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		msg, _ := json.Marshal(map[string]string{"status": "0", "mst": "success"})
		w.Write(msg)
	}))
	defer ts.Close()

	c := usage_client.NewClient()
	c.WithLogger(zap.NewNop())
	c.URL = ts.URL

	usage := map[string]string{
		"product": "openGemini",
	}
	res, err := c.Send(usage)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}
