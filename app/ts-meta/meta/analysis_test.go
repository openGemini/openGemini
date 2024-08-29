// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package meta

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/require"
)

type IStoreImpl struct {
}

func (s *IStoreImpl) IsLeader() bool {
	return true
}
func MockCreateHandler() *httpHandler {
	h := newHttpHandler(&config.Meta{}, &MockIStore{})
	h.initAnalysisCache(time.Now())
	h.analysisCache = &AnalysisCache{
		Index:      1,
		Heart:      map[string]time.Time{"user1": time.Now(), "user2": time.Now()},
		LockHolder: "user2",
		TaskLock:   map[string]string{"task1": "user1", "task2": "user2"},
	}
	return h
}
func TestAnalysisCache_clone(t *testing.T) {
	type fields struct {
		Index      int
		LockHolder string
		TaskLock   map[string]string
		Heart      map[string]time.Time
	}
	type args struct {
		cache AnalysisCache
	}
	tt := AnalysisCache{
		Index:      1,
		Heart:      map[string]time.Time{"user1": time.Now(), "user2": time.Now()},
		LockHolder: "user1",
		TaskLock:   map[string]string{"task1": "user1", "task2": "user2"},
	}
	m := &AnalysisCache{}
	m.clone(tt)
	require.EqualValues(t, tt.Index, m.Index)
	require.EqualValues(t, tt.Heart, m.Heart)
	require.EqualValues(t, tt.LockHolder, m.LockHolder)
	require.EqualValues(t, tt.TaskLock, m.TaskLock)

}

func Test_httpHandler_serveAnalysisHeartInfo(t *testing.T) {
	h := MockCreateHandler()
	req, err := http.NewRequest("GET", "/analysis/heart", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	w := httptest.NewRecorder()
	h.serveAnalysisHeartInfo(w, req)
	require.Equal(t, w.Code, http.StatusOK, "The handler returns an incorrect status code.")
	require.Equal(t, w.Header().Get("Content-Type"), "application/octet-stream", "The handler returns an incorrect Content-Type.")
	var res AnalysisResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&res), "Error decoding response")
	require.True(t, res.Ok, "The handler returns an incorrect response.")
}
func Test_httpHandler_serveAnalysisHeart(t *testing.T) {
	h := MockCreateHandler()
	myObj := AnalysisRequest{}
	myObj.Req = 1
	mockBody, err := json.Marshal(myObj)
	req, err := http.NewRequest("GET", "/analysis/heart?nodeId=1", bytes.NewBuffer(mockBody))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	w := httptest.NewRecorder()
	h.serveAnalysisHeart(w, req)
	require.Equal(t, w.Code, http.StatusOK, "The handler returns an incorrect status code.")
	require.Equal(t, w.Header().Get("Content-Type"), "application/octet-stream", "The handler returns an incorrect Content-Type.")
	var res AnalysisResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&res), "Error decoding response")

}

func Test_httpHandler_analysisDealLock(t *testing.T) {
	h := MockCreateHandler()
	now_Index := h.analysisCache.Index
	req := &AnalysisRequest{
		Req: 0,
	}
	w := &AnalysisResponse{}
	h.analysisDealLock(req, w, "user2", time.Now())
	require.Equal(t, w.LockHolder, "user2")
	require.Equal(t, w.Index, now_Index+1)
	now_Index = now_Index + 1
	req1 := &AnalysisRequest{
		Req: 1,
	}
	h.analysisDealLock(req1, w, "user2", time.Now())
	require.Equal(t, w.AnalysisCache.LockHolder, "")
	require.Equal(t, w.Index, now_Index+1)
}

func Test_httpHandler_analysisDealHeart(t *testing.T) {
	h := MockCreateHandler()
	h.analysisLockStart = time.Now().Add(-15 * time.Second)
	req := &AnalysisRequest{}
	h.analysisCache.Heart["user1"] = time.Now().Add(-15 * time.Second)
	w := &AnalysisResponse{}
	h.analysisDealHeart(req, w, "user2", time.Now())
	require.Equal(t, h.analysisCache.LockHolder, "")
	_, ok := h.analysisCache.Heart["user1"]
	require.False(t, ok, "user1 is deleted")
}
