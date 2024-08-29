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

package meta

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type AnalysisCache struct {
	Index      int                  `json:"index"`
	LockHolder string               `json:"lockHolder"`
	TaskLock   map[string]string    `json:"taskLock"`
	Heart      map[string]time.Time `json:"heart"`
}

func (m *AnalysisCache) clone(cache AnalysisCache) {
	m.Index = cache.Index
	m.LockHolder = cache.LockHolder
	m.Heart = map[string]time.Time{}
	for k, v := range cache.Heart {
		m.Heart[k] = v
	}
	m.TaskLock = map[string]string{}
	for k, v := range cache.TaskLock {
		m.TaskLock[k] = v
	}
}

type AnalysisResponse struct {
	AnalysisCache
	Error string `json:"error"`
	Ok    bool   `json:"ok"`
}

type AnalysisRequest struct {
	AnalysisCache
	Req AnalysisRequestType `json:"req"`
}

type AnalysisRequestType int

const (
	AquireLock AnalysisRequestType = iota
	Heart
	ReleaseLock
)

const (
	HeartTimeOut = 15
	LockTimeOut  = 15
)

func (h *httpHandler) serveAnalysisHeartInfo(w http.ResponseWriter, r *http.Request) {
	h.analysisLock.RLock()
	defer h.analysisLock.RUnlock()
	if !h.store.IsLeader() {
		h.resetAnalysis()
		h.httpErr(errors.New("no leader"), w, http.StatusServiceUnavailable)
		return
	}
	var buf bytes.Buffer
	res := &AnalysisResponse{}
	res.Ok = true
	res.clone(*h.analysisCache)
	err := json.NewEncoder(&buf).Encode(res)
	if err != nil {
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/octet-stream")
	_, _ = w.Write(buf.Bytes())
}

func (h *httpHandler) resetAnalysis() {
	h.analysisCache = &AnalysisCache{
		Index:      0,
		Heart:      map[string]time.Time{},
		LockHolder: "",
		TaskLock:   map[string]string{},
	}
	h.analysisStartUp = time.Unix(0, 0)
	h.analysisLockStart = time.Unix(0, 0)
}

func (h *httpHandler) initAnalysisCache(now time.Time) {
	if h.analysisStartUp == time.Unix(0, 0) {
		h.analysisStartUp = now
	}
	if h.analysisLockStart == time.Unix(0, 0) {
		h.analysisLockStart = now
	}
}

func (h *httpHandler) serveAnalysisHeart(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// Get the requested lease name.
	nodeId := q.Get("nodeId")
	if nodeId == "" {
		h.httpErr(fmt.Errorf("invalid node id"), w, http.StatusBadRequest)
		return
	}
	h.analysisLock.Lock()
	defer h.analysisLock.Unlock()
	if !h.store.IsLeader() {
		h.resetAnalysis()
		h.httpErr(errors.New("no leader"), w, http.StatusServiceUnavailable)
		return
	}
	req := &AnalysisRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	now := time.Now()
	h.initAnalysisCache(now)

	res := &AnalysisResponse{}
	if req.Req == Heart {
		h.analysisDealHeart(req, res, nodeId, now)
		res.clone(*h.analysisCache)
	} else {
		h.analysisDealLock(req, res, nodeId, now)
	}

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(res)
	if err != nil {
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}
	// Send response to client.
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/octet-stream")
	_, _ = w.Write(buf.Bytes())
}

func (h *httpHandler) analysisDealLock(req *AnalysisRequest, res *AnalysisResponse, nodeId string, now time.Time) {
	res.Ok = false
	if h.analysisCache.LockHolder == nodeId || h.analysisCache.LockHolder == "" {
		res.Ok = true
		h.analysisCache.Index = h.analysisCache.Index + 1
		if req.Req == AquireLock {
			h.analysisCache.LockHolder = nodeId
			h.analysisLockStart = now
		} else {
			h.analysisCache.LockHolder = ""
		}
	} else {
		res.Error = fmt.Sprintf("already locked by %v", h.analysisCache.LockHolder)
	}
	res.clone(*h.analysisCache)
}

func (h *httpHandler) analysisDealHeart(req *AnalysisRequest, res *AnalysisResponse, nodeId string, now time.Time) {
	res.Ok = true
	if h.analysisCache.Index == 0 {
		if 0 < req.Index {
			h.analysisCache.clone(req.AnalysisCache)
		}
	}
	_, ok := h.analysisCache.Heart[nodeId]
	if !ok {
		h.analysisCache.Index = h.analysisCache.Index + 1
	}
	h.analysisCache.Heart[nodeId] = now
	for node, t := range h.analysisCache.Heart {
		if now.Sub(t).Seconds() > HeartTimeOut {
			delete(h.analysisCache.Heart, node)
			h.analysisCache.Index = h.analysisCache.Index + 1
		}
	}
	if now.Sub(h.analysisLockStart).Seconds() > LockTimeOut && h.analysisCache.LockHolder != "" {
		h.logger.Info(fmt.Sprintf("release timeout lock id %v", h.analysisCache.LockHolder))
		h.analysisCache.LockHolder = ""
		h.analysisCache.Index = h.analysisCache.Index + 1
	}
}
