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

package httpserver

import "net/http"

// PingPath is the liveness endpoint of the ts-meta and ts-store HTTP services.
const PingPath = "/ping"

// IsPingRequest reports whether r is a GET or HEAD request for PingPath.
func IsPingRequest(r *http.Request) bool {
	return r.URL.Path == PingPath && (r.Method == http.MethodGet || r.Method == http.MethodHead)
}

// ServePing answers a liveness probe, such as a Kubernetes livenessProbe,
// with 204 No Content. Like /ping of ts-sql, it is served without
// authentication, so that probes do not need credentials.
func ServePing(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}
