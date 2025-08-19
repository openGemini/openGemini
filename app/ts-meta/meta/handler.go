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

package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/httpserver"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

type IStore interface {
	index() uint64
	userSnapshot(version uint32) error
	MeteRecover()
	otherMetaServersHTTP() []string
	showDebugInfo(witch string) ([]byte, error)
	GetData() *meta.Data //get the Data in the store
	GetMarshalData(parts []string) ([]byte, error)
	IsLeader() bool
	markTakeOver(enable bool) error
	markBalancer(enable bool) error
	movePt(db string, pt uint32, to uint64) error
	ExpandGroups() error
	leaderHTTP() string
	leadershipTransfer() error
	SpecialCtlData(cmd string) error
	ModifyRepDBMasterPt(db string, rgId uint32, newMasterPtId uint32) error
	RecoverMetaData(databases []string, metaData []byte, node map[uint64]uint64) error
}

var httpScheme = map[bool]string{
	true:  "https",
	false: "http",
}

// #nosec
var httpsClient = &http.Client{Transport: &http.Transport{
	TLSClientConfig: config.NewTLSConfig(true),
}}

// handler represents an HTTP handler for the meta service.
type httpHandler struct {
	config           *config.Meta
	logger           *logger.Logger
	store            IStore
	statisticsPusher *statisticsPusher.StatisticsPusher
	client           *metaclient.Client

	analysisLock      sync.RWMutex
	analysisCache     *AnalysisCache
	analysisStartUp   time.Time
	analysisLockStart time.Time
}

// newHandler returns a new instance of handler with routes.
func newHttpHandler(c *config.Meta, store IStore) *httpHandler {
	h := &httpHandler{
		config: c,
		logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http_handler")),
		store:  store,
	}
	h.resetAnalysis()

	return h
}

func (h *httpHandler) SetstatisticsPusher(pusher *statisticsPusher.StatisticsPusher) {
	h.statisticsPusher = pusher
}

func (h *httpHandler) WrapHandler(hf http.HandlerFunc) http.Handler {
	return httpserver.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l := httpd.NewResponseLogger(w)
		hf.ServeHTTP(l, r)
	}), h.client, h.config.AuthEnabled)
}

// ServeHTTP responds to HTTP request to the handler.
func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/debug":
			h.WrapHandler(h.serveDebug).ServeHTTP(w, r)
		case "/getdata":
			h.WrapHandler(h.serveGetdata).ServeHTTP(w, r) //get the Data in the store
		case "/analysisCache":
			h.WrapHandler(h.serveAnalysisHeartInfo).ServeHTTP(w, r)
		case "/debug/vars":
			h.WrapHandler(h.serveExpvar).ServeHTTP(w, r)
		}
	case "POST":
		switch r.URL.Path {
		case "/userSnapshot":
			h.WrapHandler(h.userSnapshot).ServeHTTP(w, r)
		case "/metaRecover":
			h.WrapHandler(h.metaRecover).ServeHTTP(w, r)
		case "/analysisCache":
			h.WrapHandler(h.serveAnalysisHeart).ServeHTTP(w, r)
		case "/takeover":
			h.logger.Info("serverTakeover")
			h.WrapHandler(h.serveTakeover).ServeHTTP(w, r)
		case "/balance":
			h.logger.Info("serveBalance")
			h.WrapHandler(h.serveBalancer).ServeHTTP(w, r)
		case "/movePt":
			h.logger.Info("serveMovePt")
			h.WrapHandler(h.serveMovePt).ServeHTTP(w, r)
		case "/expandGroups":
			h.WrapHandler(h.serveExpandGroups).ServeHTTP(w, r)
		case "/leadershiptransfer":
			h.WrapHandler(h.leadershipTransfer).ServeHTTP(w, r)
		case "/specialCtlData":
			h.WrapHandler(h.specialCtlData).ServeHTTP(w, r)
		case "/modifyRepDBMasterPt":
			h.WrapHandler(h.modifyRepDBMasterPt).ServeHTTP(w, r)
		case "/recoverMeta":
			h.WrapHandler(h.recoverMeta).ServeHTTP(w, r)
		}
	default:
		http.Error(w, "", http.StatusBadRequest)
	}
	h.logger.Info("serve http", zap.String("method", r.Method), zap.String("path", r.URL.Path), zap.String("from", r.RemoteAddr))
}

// for got raf node status
// get all raf node, use like this: curl -i -GET 'http://localhost:8091/debug?witch=raft-stat' -H 'all:y'
// get one raf node, use like this: curl -i -GET 'http://localhost:8091/debug?witch=raft-stat'
// only application/json format
func (h *httpHandler) serveDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	errorMap := make(map[string]string)
	status := make(map[string]map[string]string)
	// got local stat
	q := r.URL.Query()
	witch := q.Get("witch")
	h.logger.Info("show debug info: " + witch)
	info, err := h.store.showDebugInfo(witch)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			errorMap["Error"] = "meta server lost contact"
		} else {
			errorMap["Error"] = fmt.Sprintf("%s", err)
		}

		status[h.config.CombineDomain(h.config.HTTPBindAddress)] = errorMap
	} else {
		var m map[string]string
		if err = json.Unmarshal(info, &m); err != nil {
			h.logger.Error("fail to unmarshal debug info", zap.Error(err))
		}

		status[h.config.CombineDomain(h.config.HTTPBindAddress)] = m
		h.logger.Info(fmt.Sprintf("local raft stat: %s", status))
	}

	// get other stat
	h.getOtherStat(errorMap, status, r)

	buf, err := json.Marshal(status)
	if err != nil {
		h.logger.Error("failed", zap.Error(err))
		h.httpErr(err, w, http.StatusInternalServerError)
	} else {
		if _, err = w.Write(buf); err != nil {
			h.logger.Error("serve debug write result failed", zap.Error(err))
		}
	}
}

func (h *httpHandler) getOtherStat(errorMap map[string]string, status map[string]map[string]string, r *http.Request) {
	allNode := r.Header.Get("all") == "y"
	if allNode {
		for _, n := range h.store.otherMetaServersHTTP() {
			url := httpScheme[h.config.HTTPSEnabled] + "://" + n + "/debug?witch=raft-stat"
			var err error
			var resp *http.Response
			if h.config.HTTPSEnabled {
				resp, err = httpsClient.Get(url)
			} else {
				resp, err = http.DefaultClient.Get(url)
			}

			if err != nil {
				if strings.Contains(err.Error(), "connection refused") {
					errorMap["Error"] = "meta server lost contact"
				} else {
					errorMap["Error"] = fmt.Sprintf("%s", err)
				}
				status[n] = errorMap
				h.logger.Error(fmt.Sprintf("get node[%s] raft stat failed: %s", n, err))
				continue
			}

			b, err := io.ReadAll(resp.Body)
			defer util.MustClose(resp.Body)
			if err != nil {
				errorMap["Error"] = fmt.Sprintf("%s", err)
				status[n] = errorMap
				h.logger.Error(fmt.Sprintf("read node[%s] body failed: %s", n, err))
				continue
			}
			var m map[string]map[string]string
			er := json.Unmarshal(b, &m)
			if er != nil {
				h.logger.Error("failed", zap.Error(er))
			}
			if value, ok := m[n]; ok {
				status[n] = value
				h.logger.Info(fmt.Sprintf("get node[%s] raft stat: %s", n, value))

			} else {
				h.logger.Info(fmt.Sprintf("failed: get node[%s] raft stat", n))
			}
		}
	}
}

func (h *httpHandler) httpErr(err error, w http.ResponseWriter, status int) {
	http.Error(w, err.Error(), status)
}

// get the status of Data include MetaNodes and DataNodes
// do this way:curl -i GET 'http://127.0.0.1:8091/getdata?nodeStatus=ok'
// if want get part info, do this way: curl -i GET 'http://127.0.0.1:8091/getdata?parts=MetaNodes,DataNodes...'
func (h *httpHandler) serveGetdata(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	q := r.URL.Query()
	nodeStatus := q.Get("nodeStatus")
	h.logger.Info("show nodeStatus:" + nodeStatus)

	partsData := q.Get("parts")
	parts := strings.FieldsFunc(partsData, func(r rune) bool {
		return r == ','
	})
	if len(parts) != 0 {
		h.logger.Info("get metadata filter by parts: " + partsData)
	}
	result, err := h.store.GetMarshalData(parts)

	if err != nil {
		h.logger.Error("get data failed", zap.Error(err))
		h.httpErr(err, w, http.StatusInternalServerError)
	} else {
		if _, err = w.Write(result); err != nil {
			h.logger.Error("get data failed", zap.Error(err))
		}
	}
}

func (h *httpHandler) userSnapshot(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	version, err := strconv.ParseUint(q.Get("version"), 10, 64)
	if err != nil {
		h.logger.Error("error parsing snapshot version")
		http.Error(w, "please check user snapshot parameter is valid\n", http.StatusBadRequest)
		return
	}
	if version < uint64(0) {
		h.logger.Error("error parsing snapshot version", zap.Uint64("version", version))
		http.Error(w, "please check user snapshot parameter is valid\n", http.StatusBadRequest)
		return
	}
	if err = h.store.userSnapshot(uint32(version)); err != nil {
		h.logger.Error("error snapshot version", zap.Error(err))
		http.Error(w, "please check user snapshot parameter is valid\n", http.StatusInternalServerError)
		return
	}

	resp := &proto2.Response{
		OK:    proto.Bool(true),
		Index: proto.Uint64(h.store.index()),
	}

	// marshal the response.
	b, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("user snapshot error marshal resp StatusInternalServerError\n", zap.Error(err))
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.Header().Add("Content-Type", "application/octet-stream")
	if _, err = w.Write(b); err != nil {
		h.logger.Error("user snapshot fail to write result", zap.Error(err))
	}
}
func (h *httpHandler) metaRecover(w http.ResponseWriter, r *http.Request) {
	h.store.MeteRecover()
}
func (h *httpHandler) serveExpvar(w http.ResponseWriter, r *http.Request) {
	httpd.SetStatsResponse(h.statisticsPusher, w, r)
}

func (h *httpHandler) executeCmdOnStore(w http.ResponseWriter, r *http.Request, f func(store IStore, enable bool) error) error {
	q := r.URL.Query()
	open, err := strconv.ParseBool(q.Get("open"))
	if err != nil {
		http.Error(w, "please check open parameter is valid\n", http.StatusBadRequest)
		return err
	}

	err = f(h.store, open)
	h.handleResponse(w, err)
	return err
}

// curl -i -XPOST 'http://127.0.0.1:8091/takeover?open=false'
func (h *httpHandler) serveTakeover(w http.ResponseWriter, r *http.Request) {
	err := h.executeCmdOnStore(w, r, func(store IStore, enable bool) error {
		return store.markTakeOver(enable)
	})
	h.logger.Info("serveTakeover finished", zap.Error(err))
}

// curl -i -XPOST 'http://127.0.0.1:8091/balance?open=false'
func (h *httpHandler) serveBalancer(w http.ResponseWriter, r *http.Request) {
	err := h.executeCmdOnStore(w, r, func(store IStore, enable bool) error {
		return store.markBalancer(enable)
	})
	h.logger.Info("serveBalancer finished", zap.Error(err))
}

// curl -i -XPOST 'http://127.0.0.1:8091/movePt?db=db0&ptId=0&to=5'
func (h *httpHandler) serveMovePt(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db := q.Get("db")
	h.logger.Info("movePT query", zap.String("q", fmt.Sprintln(q)))
	if db == "" {
		h.logger.Error("missing parameter db")
		http.Error(w, "you should input db in movePt command", http.StatusBadRequest)
		return
	}
	ptId, err := strconv.ParseUint(q.Get("ptId"), 10, 32)
	if err != nil {
		h.logger.Error("error parsing ptId", zap.Error(err))
		http.Error(w, "error parsing ptId", http.StatusBadRequest)
		return
	}
	to, err := strconv.ParseUint(q.Get("to"), 10, 64)
	if err != nil {
		h.logger.Error("error parsing to")
		http.Error(w, "error parsing to", http.StatusBadRequest)
		return
	}

	err = h.store.movePt(db, uint32(ptId), to)
	h.handleResponse(w, err)
	h.logger.Info("movePT", zap.String("db", db), zap.Uint64("ptID", ptId), zap.Uint64("to", to), zap.Error(err))
}

func (h *httpHandler) handleResponse(w http.ResponseWriter, err error) {
	var resp *proto2.Response
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		resp = &proto2.Response{
			OK:    proto.Bool(true),
			Index: proto.Uint64(h.store.index()),
		}
	}

	// Marshal the response.
	b, err := json.Marshal(resp)
	if err != nil {
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.Header().Add("Content-Type", "application/octet-stream")
	_, _ = w.Write(b)
}

// curl -i -XPOST 'http://127.0.0.1:8091/expandGroups'
func (h *httpHandler) serveExpandGroups(w http.ResponseWriter, r *http.Request) {
	err := h.store.ExpandGroups()
	if err != nil {
		h.logger.Error("error expand groups", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := &proto2.Response{
		OK:    proto.Bool(true),
		Index: proto.Uint64(h.store.index()),
	}

	// marshal the response.
	b, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("expand groups error marshal resp StatusInternalServerError\n", zap.Error(err))
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.Header().Add("Content-Type", "application/octet-stream")
	_, err = w.Write(b)
	if err != nil {
		h.logger.Error("write result failed", zap.Error(err))
	}
}

// curl -i -XPOST 'http://127.0.0.1:8091/leadershiptransfer'
func (h *httpHandler) leadershipTransfer(w http.ResponseWriter, r *http.Request) {
	err := h.store.leadershipTransfer()
	if err != nil && strings.Contains(err.Error(), "node is not the leader") {
		l := h.store.leaderHTTP()
		if l == "" {
			h.httpErr(errors.New("no raft leader"), w, http.StatusServiceUnavailable)
		}
		scheme := "http"
		if h.config.HTTPSEnabled {
			scheme = "https"
		}

		url := fmt.Sprintf("%s://%s/leadershiptransfer", scheme, l)
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}

	if err != nil {
		h.logger.Error("store.leadershiptransfer error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := &proto2.Response{
		OK:    proto.Bool(true),
		Index: proto.Uint64(h.store.index()),
	}

	b, err := json.Marshal(resp)
	if err != nil {
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/octet-stream")
	_, err = w.Write(b)
	if err != nil {
		h.logger.Error("write result failed", zap.Error(err))
	}
	_, _ = w.Write([]byte("\n"))
}

// cmd sample: limit|192.168.0.11,192.168.0.12 or unlimit|xxxx  or delete|xxxx
func (h *httpHandler) specialCtlData(w http.ResponseWriter, r *http.Request) {
	h.logger.Info(fmt.Sprintf("url: %v, param: %v\n", r.URL.Path, r.URL.RawQuery))
	q := r.URL.Query()
	cmd := q.Get("cmdDetail")
	if cmd == "" {
		h.httpErr(fmt.Errorf("invalid cmd"), w, http.StatusBadRequest)
		return
	}

	err := h.store.SpecialCtlData(cmd)
	if errno.Equal(err, errno.MetaIsNotLeader) {
		l := h.store.leaderHTTP()
		if l == "" {
			h.httpErr(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		scheme := "http://"
		if h.config.HTTPSEnabled {
			scheme = "https://"
		}

		url := fmt.Sprintf("%s%s/specialCtlData?%s", scheme, l, r.URL.RawQuery)
		h.logger.Info("specialCtlData Redirect", zap.String("url", url))
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}

	if err != nil {
		h.logger.Error("store.specialCtlData error", zap.String("cmd", cmd), zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Apply was successful. Return the new store index to the client.
	resp := &proto2.Response{
		OK:    proto.Bool(true),
		Index: proto.Uint64(h.store.index()),
	}

	h.logger.Info("specialCtlData resp", zap.String("cmd", cmd), zap.Any("resp", resp))
	// Marshal the response.
	b, err := json.Marshal(resp)
	if err != nil {
		h.httpErr(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/octet-stream")
	b = append(b, "\n"...)
	_, err = w.Write(b)
	if err != nil {
		h.logger.Error("write result failed", zap.Error(err))
	}
}

// curl -i -XPOST 'http://127.0.0.1:8091/modifyRepDBMasterPt?db=db0&rgId=0&newMasterPtId=0'
func (h *httpHandler) modifyRepDBMasterPt(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db := q.Get("db")
	h.logger.Info("modifyRepDBMasterPt query", zap.String("q", fmt.Sprintln(q)))
	if db == "" {
		h.logger.Error("missing parameter db")
		http.Error(w, "you should input db in modifyRepDBMasterPt command", http.StatusBadRequest)
		return
	}
	rgId, err := strconv.ParseUint(q.Get("rgId"), 10, 32)
	if err != nil {
		h.logger.Error("error parsing rgId", zap.Error(err))
		http.Error(w, "error parsing rgId", http.StatusBadRequest)
		return
	}
	newMasterPtId, err := strconv.ParseUint(q.Get("newMasterPtId"), 10, 32)
	if err != nil {
		h.logger.Error("error parsing newMasterPtId")
		http.Error(w, "error parsing newMasterPtId", http.StatusBadRequest)
		return
	}

	err = h.store.ModifyRepDBMasterPt(db, uint32(rgId), uint32(newMasterPtId))
	h.handleResponse(w, err)
	h.logger.Info("modifyRepDBMasterPt", zap.String("db", db), zap.Uint64("rgId", rgId), zap.Uint64("newMasterPtId", newMasterPtId), zap.Error(err))
}

func (h *httpHandler) recoverMeta(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	h.logger.Info("recoverMeta query", zap.String("q", fmt.Sprintln(q)))
	databases := make([]string, 0)
	dbs := q.Get(backup.DataBases)
	if dbs != "" {
		databases = strings.Split(dbs, ",")
	}
	m := q.Get(backup.MetaData)
	metaData := &meta.Data{}
	err := json.Unmarshal([]byte(m), metaData)
	if err != nil {
		http.Error(w, fmt.Sprintf("unmarshal meta data error,metaData: %s", m), http.StatusBadRequest)
		return
	}

	data := h.store.GetData()
	nodeMap, err := meta.GenNodeMap(data, metaData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	defer func() {
		err = h.store.userSnapshot(0)
		if err != nil {
			h.logger.Error("run userSnapshot error", zap.Error(err))
		}
	}()
	err = h.store.RecoverMetaData(databases, []byte(m), nodeMap)
	if err != nil {
		http.Error(w, fmt.Sprintf("recover meta error: %s", err.Error()), http.StatusBadRequest)
		return
	}

	if len(databases) != 0 {
		for _, d := range databases {
			dbPts := globalService.store.getDbPtsByDbnameV2(d)
			if err := assignDbpt(dbPts); err != nil {
				http.Error(w, fmt.Sprintf("recover meta error: %s", err.Error()), http.StatusBadRequest)
				return
			}
		}
	} else {
		for d := range data.Databases {
			dbPts := globalService.store.getDbPtsByDbnameV2(d)
			if err := assignDbpt(dbPts); err != nil {
				http.Error(w, fmt.Sprintf("recover meta error: %s", err.Error()), http.StatusBadRequest)
				return
			}
		}
	}
}
