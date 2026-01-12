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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/pool"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services/fence"
	"go.uber.org/zap"
)

type FenceMatchResponse struct {
	Code    int    `json:"code"`
	FenceID string `json:"fence_id"`
	Error   string `json:"error"`
}

type BatchFenceMatchResponse struct {
	Code   int           `json:"code"`
	Error  string        `json:"error"`
	Result []MatchResult `json:"result"`
}

type MatchResult struct {
	Seq     int      `json:"seq"`
	FenceID []string `json:"fence_id"`
}

var ctxPool = pool.NewDefaultUnionPool[fence.PointMatchCtx](func() *fence.PointMatchCtx {
	return &fence.PointMatchCtx{}
})

func (h *Handler) batchFenceMatch(w http.ResponseWriter, r *http.Request, user meta2.User) {
	database := r.URL.Query().Get("db")
	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			err := errno.NewError(errno.HttpForbidden)
			h.Logger.Error("write error: user is required to write to database", zap.Error(err), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			err := errno.NewError(errno.HttpForbidden)
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			h.Logger.Error("write error:user is not authorized to write to database", zap.Error(err), zap.String("db", database), zap.String("user", user.ID()))
			handlerStat.Write400ErrRequests.Incr()
			return
		}
	}
	resp := &BatchFenceMatchResponse{}

	ret, err := batchFenceMatch(r.URL.Query().Get("points"))
	if err != nil {
		resp.Error = "invalid points"
		resp.Code = -1
	} else {
		resp.Result = ret
	}

	h.writeHeader(w, http.StatusOK)
	b, _ := json2.Marshal(resp)
	_, _ = w.Write(b)
}

func (h *Handler) fenceDelete(w http.ResponseWriter, r *http.Request, user meta2.User) {

	database := r.URL.Query().Get("db")
	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			err := errno.NewError(errno.HttpForbidden)
			h.Logger.Error("write error: user is required to write to database", zap.Error(err), zap.String("db", database))
			handlerStat.Write400ErrRequests.Incr()
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			err := errno.NewError(errno.HttpForbidden)
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			h.Logger.Error("write error:user is not authorized to write to database", zap.Error(err), zap.String("db", database), zap.String("user", user.ID()))
			handlerStat.Write400ErrRequests.Incr()
			return
		}
	}
	resp := &BatchFenceMatchResponse{}

	err := fence.ManagerIns().DeleteFenceByID(r.URL.Query().Get("fenceId"))
	if err != nil {
		resp.Error = "remove fence file err"
		resp.Code = -1
	}

	h.writeHeader(w, http.StatusOK)
	b, _ := json2.Marshal(resp)
	_, _ = w.Write(b)
}

func batchFenceMatch(s string) ([]MatchResult, error) {
	if s == "" {
		return nil, errors.New("invalid param points")
	}

	var points []float64
	err := json.Unmarshal([]byte(s), &points)
	if err != nil {
		return nil, errors.New("invalid param points")
	}

	ctx := ctxPool.Get()
	if ctx == nil {
		return nil, errors.New("ctx is nil")
	}
	defer func() {
		ctxPool.PutWithMemSize(ctx, 0)
	}()

	if len(points)%2 != 0 {
		return nil, errors.New("length of points err")
	}
	n := len(points) / 2
	resp := make([]MatchResult, n)
	for i := range n {
		idx := i * 2
		resp[i].Seq = i
		fenceIds := fence.ManagerIns().MatchPoint(ctx, points[idx], points[idx+1])
		if len(fenceIds) > 0 {
			resp[i].FenceID = append([]string{}, fenceIds...)
		}
	}
	return resp, err
}
