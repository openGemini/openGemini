/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package httpd

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/uuid"
	immutable "github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/proxy"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	query2 "github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/logparser"
	"github.com/valyala/fastjson"
	"go.uber.org/zap"
)

const (
	// LogReqErr default error
	LogReqErr = "CSSOP.00050001"

	LogRetryTag = "symbol:repeatLog"
)

// bad req
var (
	ErrLogRepoEmpty         = errors.New("repository name should not be none")
	ErrLogStreamEmpty       = errors.New("logstream name should not be none")
	ErrLogStreamDeleted     = errors.New("logstrem being deleted")
	ErrLogStreamInvalid     = errors.New("logstrem invalid in retentionPolicy")
	ErrInvalidRepoName      = errors.New("invalid repository name")
	ErrInvalidLogStreamName = errors.New("invalid logstream name")
	ErrInvalidWriteNode     = errors.New("this data node is not used for writing")
)

const (
	MaxTtl               int64 = 3000
	PermanentSaveTtl     int64 = 3650
	ScannerBufferSize    int   = 10 * 1024 * 1024
	MaxRequestBodyLength int64 = 100 * 1024 * 1024
	UnixTimestampMaxMs   int64 = 4102416000000
	UnixTimestampMinMs   int64 = 1e10
	NewlineLen           int64 = 1
	TagsSplitterChar           = byte(6)

	MaxRowLen                                = 3500
	DefaultAggLogQueryTimeout                = 500
	IncAggLogQueryRetryCount             int = 3
	DefaultMaxLogStoreAnalyzeResponseNum     = 100

	MaxSplitCharLen int = 128
)

func transLogStoreTtl(ttl int64) (int64, bool) {
	if ttl == PermanentSaveTtl {
		return 0, true
	}

	if ttl < 1 || ttl > MaxTtl {
		return ttl, false
	}

	return ttl, true
}

func validateSplitChar(splitChar string) error {
	if len(splitChar) > MaxSplitCharLen {
		logger.GetLogger().Error(fmt.Sprintf("the length of delimiter has exceeded the maximum length of %d", MaxSplitCharLen))
		return fmt.Errorf("the length of delimiter has exceeded the maximum length of %d", MaxSplitCharLen)
	}

	for i := 0; i < len(splitChar); i++ {
		if splitChar[i] > 127 {
			logger.GetLogger().Error("delimiter only supports asscii codes")
			return fmt.Errorf("delimiter only supports asscii codes")
		}
	}
	return nil
}

func validateLogstreamOptions(opt *meta2.Options) error {
	var ok bool
	opt.Ttl, ok = transLogStoreTtl(opt.Ttl)
	if !ok {
		logger.GetLogger().Error(fmt.Sprintf("serveCreateLogstream, wrong ttl: %d", opt.Ttl))
		return fmt.Errorf("the Data Retention Period value error")
	}

	splitChar := opt.GetSplitChar()
	if len(splitChar) != 0 {
		err := validateSplitChar(splitChar)
		if err != nil {
			return err
		}
	} else {
		logger.GetLogger().Warn("there is no delimiter of content was passed in, the default delimiter will be used")
		opt.SplitChar = tokenizer.CONTENT_SPLITTER
	}

	tagsSpiltChar := opt.GetTagSplitChar()
	if len(tagsSpiltChar) != 0 {
		err := validateSplitChar(tagsSpiltChar)
		if err != nil {
			return err
		}
	} else {
		logger.GetLogger().Warn("there is no delimiter of tag was passed in, the default delimiter will be used")
		opt.TagsSplit = tokenizer.TAGS_SPLITTER_BEFORE
	}

	return nil
}

func ValidateRepository(repoName string) error {
	if repoName == "" {
		return ErrLogRepoEmpty
	}
	if !meta2.ValidName(repoName) {
		return ErrInvalidRepoName
	}
	return nil
}

func ValidateLogStream(streamName string) error {
	if streamName == "" {
		return ErrLogStreamEmpty
	}
	if !meta2.ValidMeasurementName(streamName) {
		return ErrInvalidLogStreamName
	}
	return nil
}

func ValidateRepoAndLogStream(repoName, streamName string) error {
	if err := ValidateRepository(repoName); err != nil {
		return err
	}
	if err := ValidateLogStream(streamName); err != nil {
		return err
	}
	return nil
}

func (h *Handler) serveCreateRepository(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepository(repository); err != nil {
		logger.GetLogger().Error("serveCreateRepository", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	options := &obs.ObsOptions{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(options); err != nil {
		logger.GetLogger().Error("serveCreateRepository, decode CreateRepositoryOptions", zap.Error(err))
		if err != nil && err.Error() != "EOF" { // with body
			h.httpErrorRsp(w, ErrorResponse("parse body error: "+err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
	}
	if options.Validate() {
		options.Enabled = true
	}
	if options.Sk != "" {
		options.Sk = crypto.Decrypt(options.Sk)
		if options.Sk == "" {
			h.httpErrorRsp(w, ErrorResponse("obs sk decrypt failed", LogReqErr), http.StatusBadRequest)
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	}
	host, _, err := net.SplitHostPort(options.Endpoint)
	if err != nil {
		if err.(*net.AddrError).Err == "missing port in address" {
			host = options.Endpoint
		} else {
			h.httpErrorRsp(w, ErrorResponse("obs sk decrypt failed", LogReqErr), http.StatusBadRequest)
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	}
	options.Endpoint = host
	logger.GetLogger().Info("serveCreateRepository", zap.String("repository", repository))
	if _, err = h.MetaClient.CreateDatabase(repository, false, 1, options); err != nil {
		logger.GetLogger().Error("serveCreateRepository, CreateLogRepository", zap.Error(err))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.writeHeader(w, http.StatusOK)
}

func (h *Handler) serveDeleteRepository(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepository(repository); err != nil {
		logger.GetLogger().Error("serveDeleteRepository", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	logger.GetLogger().Info("serveDeleteRepository", zap.String("repository", repository))
	err := h.MetaClient.MarkDatabaseDelete(repository)
	if err != nil {
		logger.GetLogger().Error("serveDeleteRepository, DeleteLogRepository", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	h.writeHeader(w, http.StatusOK)
}

func (h *Handler) serveListRepository(w http.ResponseWriter, r *http.Request, user meta2.User) {
	h.Logger.Info("serveListRepository")
	repositories := h.MetaClient.Databases()
	repoList := []string{}
	h.Logger.Info(fmt.Sprintf("serveListRepository all len %v", len(repositories)))
	for i := range repositories {
		if !repositories[i].MarkDeleted {
			repoList = append(repoList, repositories[i].Name)
		}
	}
	sort.Strings(repoList)
	buffer, err := json.Marshal(repoList)
	if err != nil {
		h.Logger.Error("serveListRepository, encode repositories info", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}

	_, err = w.Write(buffer)
	if err != nil {
		h.Logger.Error("serveListRepository, write repositories info", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) serveShowRepository(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepository(repository); err != nil {
		h.Logger.Error("serveRepository", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	h.Logger.Info("serveRepository", zap.String("repository", repository))
	logStreams, err := h.MetaClient.Measurements(repository, nil)
	if err != nil {
		h.Logger.Error("serveRepository, GetLogRepositoryByName", zap.Error(err))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sort.Strings(logStreams)
	buffer, err := json.Marshal(logStreams)
	if err != nil {
		h.Logger.Error("serveRepository, encode log repository info", zap.Error(err))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(buffer)
	if err != nil {
		h.Logger.Error("serveRepository, write log repository info", zap.Error(err))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) serveUpdateRepository(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepository(repository); err != nil {
		logger.GetLogger().Error("serveUpdateRepository", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	logger.GetLogger().Info("serveUpdateRepository", zap.String("repository", repository))

	h.writeHeader(w, http.StatusOK)
}

func (h *Handler) getDefaultSchemaForLog(opt *meta2.Options) (*meta2.ColStoreInfo, []*proto2.FieldSchema, *influxql.IndexRelation, *meta2.ShardKeyInfo) {
	colStoreInfo := meta2.NewColStoreInfo([]string{"time"}, []string{"time"}, nil, 0, "block")

	tags := map[string]int32{"tags": influx.Field_Type_String}
	fields := map[string]int32{"content": influx.Field_Type_String}
	schemaInfo := meta2.NewSchemaInfo(tags, fields)
	oid, _ := tsi.GetIndexIdByName("bloomfilter")
	indexR := &influxql.IndexRelation{
		Rid:        0,
		Oids:       []uint32{oid},
		IndexNames: []string{"bloomfilter"},
		IndexList: []*influxql.IndexList{
			&influxql.IndexList{
				IList: []string{"tags", "content"},
			},
		},
		IndexOptions: map[string][]*influxql.IndexOption{
			"tags": []*influxql.IndexOption{
				{Tokens: opt.TagsSplit, Tokenizers: "standard"},
			},
			"content": []*influxql.IndexOption{
				{Tokens: opt.SplitChar, Tokenizers: "standard"},
			},
		},
	}
	ski := &meta2.ShardKeyInfo{Type: "hash"}
	return colStoreInfo, schemaInfo, indexR, ski
}

func (h *Handler) serveCreateLogstream(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(":repository")
	logStream := r.URL.Query().Get(":logStream")
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		logger.GetLogger().Error("serveCreateLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	options := &meta2.Options{}
	options.InitDefault()
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(options); err != nil {
		logger.GetLogger().Error("serveCreateLogstream, decode CreateLogStreamOptions", zap.Error(err))
		if err != nil && err.Error() != "EOF" {
			h.httpErrorRsp(w, ErrorResponse("parse body error: "+err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
	}
	if err := validateLogstreamOptions(options); err != nil {
		logger.GetLogger().Error("serveCreateLogstream failed", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	logger.GetLogger().Info("serveCreateLogstream", zap.String("logStream", logStream), zap.String("repository", repository))

	// create retentionPolicy
	var duration int64 = options.Ttl * int64(time.Hour) * 24
	spec := &meta2.RetentionPolicySpec{Name: logStream, ShardGroupDuration: 24 * time.Hour, Duration: meta2.GetDuration(&duration)}
	if _, err := h.MetaClient.CreateRetentionPolicy(repository, spec, false); err != nil {
		logger.GetLogger().Error("create logStream failed", zap.String("name", logStream), zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	// crete measurement
	colStoreInfo, schemaInfo, indexRelation, ski := h.getDefaultSchemaForLog(options)
	if _, err := h.MetaClient.CreateMeasurement(repository, logStream, logStream, ski, indexRelation, config.COLUMNSTORE, colStoreInfo, schemaInfo, options); err != nil {
		logger.GetLogger().Error("create logStream failed", zap.String("name", logStream), zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}

	h.writeHeader(w, http.StatusOK)
}

func (h *Handler) serveDeleteLogstream(w http.ResponseWriter, r *http.Request, user meta2.User) {
	logStream := r.URL.Query().Get(":logStream")
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		logger.GetLogger().Error("serveDeleteLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	logger.GetLogger().Info("serveDeleteLogstream", zap.String("logStream", logStream), zap.String("repository", repository))
	if err := h.MetaClient.MarkRetentionPolicyDelete(repository, logStream); err != nil {
		logger.GetLogger().Error("serveDeleteLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	h.writeHeader(w, http.StatusOK)
}

func (h *Handler) serveListLogstream(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepository(repository); err != nil {
		h.Logger.Error("serveListLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	h.Logger.Info("serveListLogstream", zap.String("repository", repository))
	dbInfo, err := h.MetaClient.Database(repository)
	if err != nil {
		h.Logger.Error("serveRepository, serveListLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	logStreams := []string{}
	for _, v := range dbInfo.RetentionPolicies {
		if !v.MarkDeleted {
			logStreams = append(logStreams, v.Name)
		}
	}
	sort.Strings(logStreams)
	buffer, err := json.Marshal(logStreams)
	if err != nil {
		h.Logger.Error("serveRepository, encode log repository info", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(buffer)
	if err != nil {
		h.Logger.Error("serveListLogstream, write logStreams info", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) serveShowLogstream(w http.ResponseWriter, r *http.Request, user meta2.User) {
	logStream := r.URL.Query().Get(":logStream")
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		h.Logger.Error("serveLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	rpi, err := h.MetaClient.RetentionPolicy(repository, logStream)
	if err != nil {
		h.Logger.Error("serveLogstream GetLogStreamByName", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	if rpi == nil {
		h.Logger.Error("serveLogstream fail", zap.Error(ErrLogStreamInvalid))
		h.httpErrorRsp(w, ErrorResponse(ErrLogStreamInvalid.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	err = jsonEncoder.Encode(rpi)
	if err != nil {
		h.Logger.Error("serveLogstream encode logStream info", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(bf.Bytes())
	if err != nil {
		h.Logger.Error("serveLogstream write logStream info", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) serveUpdateLogstream(w http.ResponseWriter, r *http.Request, user meta2.User) {
	logStream := r.URL.Query().Get(":logStream")
	repository := r.URL.Query().Get(":repository")
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		logger.GetLogger().Error("serveUpdateLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	option := &meta2.Options{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(option); err != nil {
		logger.GetLogger().Error("serveUpdateLogstream, decode UpdateLogStreamOptions", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	if err := validateLogstreamOptions(option); err != nil {
		logger.GetLogger().Error("serveUpdateLogstream failed", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	logger.GetLogger().Info(fmt.Sprintf("serveUpdateLogstream, ttl: %d", option.Ttl), zap.String("logStream", logStream),
		zap.String("repository", repository))
	err := h.MetaClient.UpdateMeasurement(repository, logStream, logStream, option)
	if err != nil {
		logger.GetLogger().Error("serveUpdateLogstream, UpdateLogStream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	h.writeHeader(w, http.StatusOK)
}

type LogDataType uint8

const (
	JSON LogDataType = iota
	JSONV2

	TAGS    = "tags"
	Tag     = "tag"
	CONTENT = "content"
	TIME    = "time"
)

var (
	LogMax         = 1000
	byteBufferPool = bufferpool.NewByteBufferPool(1024*100, cpu.GetCpuNum(), bufferpool.MaxLocalCacheLen)
	parserPool     = &fastjson.ParserPool{}
	arenaPool      = &fastjson.ArenaPool{}
)

var schema = record.Schemas{
	record.Field{Type: influx.Field_Type_String, Name: CONTENT},
	record.Field{Type: influx.Field_Type_String, Name: TAGS},
	record.Field{Type: influx.Field_Type_Int, Name: TIME},
}

type LogWriteRequest struct {
	repository string
	logStream  string
	retry      bool
	dataType   LogDataType
	mapping    *JsonMapping
}

type JsonMapping struct {
	timestamp   string
	defaultType string
	content     map[string]bool
	tags        map[string]bool
}

func getRecordType(ty string) (LogDataType, error) {
	logDataType := JSON
	ty = strings.ToLower(ty)
	switch ty {
	case "json", "":
		logDataType = JSON
	default:
		return logDataType, errno.NewError(errno.InvalidLogDataType)
	}
	return logDataType, nil
}

func parseMapping(mapping string) (*JsonMapping, error) {
	jsonMapping := &JsonMapping{
		tags:    make(map[string]bool),
		content: make(map[string]bool),
	}
	p := parserPool.Get()
	defer parserPool.Put(p)
	v, err := p.Parse(mapping)
	if err != nil {
		return nil, err
	}
	timeKey := v.Get("timestamp")
	if timeKey == nil {
		return nil, errno.NewError(errno.InvalidMappingTimeKey)
	}
	timeBy, err := timeKey.StringBytes()
	if err != nil {
		return nil, errno.NewError(errno.InvalidMappingTimeKeyType)
	}
	if string(timeBy) == "" {
		return nil, errno.NewError(errno.InvalidMappingTimeKeyVal)
	}
	jsonMapping.timestamp = string(timeBy)

	defaultTypeKey := v.Get("default_type")
	if defaultTypeKey == nil {
		jsonMapping.defaultType = TAGS
	} else {
		by, err := defaultTypeKey.StringBytes()
		if err != nil {
			return nil, errno.NewError(errno.InvalidMappingDefaultType)
		}
		switch string(by) {
		case TAGS, "":
			jsonMapping.defaultType = TAGS
		case CONTENT:
			jsonMapping.defaultType = CONTENT
		default:
			return nil, errno.NewError(errno.InvalidMappingDefaultType)
		}
	}

	value := v.Get(CONTENT)
	if value == nil {
		return nil, errno.NewError(errno.InvalidMappingContentKeyMissing)
	}
	cKeys := v.GetArray(CONTENT)
	if len(cKeys) == 0 {
		return nil, errno.NewError(errno.InvalidMappingContentKeyType)
	}
	contentKeys := map[string]bool{}
	for i := range cKeys {
		by, err := cKeys[i].StringBytes()
		if err != nil {
			return nil, errno.NewError(errno.InvalidMappingContentKeySubType)
		}
		contentKeys[string(by)] = true
	}
	if len(contentKeys) == 0 {
		return nil, errno.NewError(errno.InvalidMappingContentKeyValMissing)
	}
	jsonMapping.content = contentKeys

	value = v.Get(TAGS)
	if value == nil {
		return jsonMapping, nil
	}
	if value.Type() != fastjson.TypeArray {
		return nil, errno.NewError(errno.InvalidMappingTagsKeyType)
	}
	tagsKeys, _ := value.Array()
	tags := map[string]bool{}
	for i := range tagsKeys {
		by, err := tagsKeys[i].StringBytes()
		if err != nil {
			return nil, errno.NewError(errno.InvalidMappingTagsKeySubType)
		}
		tags[string(by)] = true
	}
	jsonMapping.tags = tags

	return jsonMapping, nil
}

func appendRowAll(rows *record.Record, tags []byte, time int64, schemasNil []bool) {
	if len(tags) == 0 {
		rows.ColVals[0].AppendStringNull()
	} else {
		rows.ColVals[0].AppendByteSlice(tags)
	}
	rows.ColVals[1].AppendInteger(time)
	for i := 2; i < len(schemasNil); i++ {
		if schemasNil[i] {
			schemasNil[i] = false
		} else {
			appendNilToRecordColumn(rows, i, rows.Schema.Field(i).Type)
		}
	}
}

func appendNilToRecordColumn(rows *record.Record, colIndex int, colType int) {
	switch colType {
	case influx.Field_Type_String:
		rows.ColVals[colIndex].AppendStringNull()
	case influx.Field_Type_Float:
		rows.ColVals[colIndex].AppendFloatNull()
	case influx.Field_Type_Boolean:
		rows.ColVals[colIndex].AppendBooleanNull()
	default:
		rows.ColVals[colIndex].AppendStringNull()
	}
}

func appendRow(rows *record.Record, tags, content []byte, time int64) {
	rows.ColVals[0].AppendByteSlice(content)
	if len(tags) == 0 {
		rows.ColVals[1].AppendStringNull()
	} else {
		rows.ColVals[1].AppendByteSlice(tags)
	}
	rows.ColVals[rows.ColNums()-1].AppendInteger(time)
}

func appendFailRow(rows *record.Record, tags, content []byte) {
	rows.ColVals[0].AppendByteSlice(content)
	rows.ColVals[1].AppendByteSlice(tags)
	rows.ColVals[rows.ColNums()-1].AppendInteger(time.Now().UnixNano())
}

func appendTags(tags []byte, key []byte, vv *fastjson.Value) []byte {
	tags = append(tags, key...)
	tags = append(tags, ":"...)
	if vv.Type() == fastjson.TypeString {
		str, _ := vv.StringBytes()
		tags = append(tags, str...)
	} else {
		tags = append(tags, vv.String()...)
	}
	return tags
}

func parameterValidate(r *http.Request) error {
	compressType := map[string]bool{"": true, "gzip": true}
	if r.ContentLength > MaxRequestBodyLength {
		return errno.NewError(errno.InvalidRequestBodyLength)
	}
	if !compressType[r.Header.Get("x-log-compresstype")] {
		return errno.NewError(errno.InvalidXLogCompressType)
	}
	return nil
}

func (h *Handler) getLogWriteRequest(r *http.Request) (*LogWriteRequest, error) {
	req := &LogWriteRequest{}
	var err error
	repository := r.URL.Query().Get(":repository")
	logStream := r.URL.Query().Get(":logStream")
	if err = ValidateRepoAndLogStream(repository, logStream); err != nil {
		return nil, err
	}
	req.logStream = logStream
	req.repository = repository

	retry := r.FormValue("retry")
	if retry != "" {
		req.retry, err = strconv.ParseBool(retry)
		if err != nil {
			return nil, errno.NewError(errno.InvalidRetryPara)
		}
	}
	logDataType, err := getRecordType(r.FormValue("type"))
	if err != nil {
		return nil, err
	}

	if mapping := r.FormValue("mapping"); logDataType == JSON && mapping != "" {
		// mapping {"timestamp":"time", "content": ["http", "addr"], "tags": ["host"]}
		// origin {"time":123, "http":"127.0.0.1", "addr":"/tmp", "host":"localhost"}
		// store {"timestamp":123, "content": "http:127.0.0.1, addr:/tmp", "tags": "host:localhost"}
		req.mapping, err = parseMapping(mapping)
		if err != nil {
			return nil, err
		}
		logDataType = JSONV2
	}
	req.dataType = logDataType

	return req, nil
}

func (h *Handler) validateRetentionPolicy(repository, logStream string) (*meta2.RetentionPolicyInfo, error) {
	logInfo, err := h.MetaClient.RetentionPolicy(repository, logStream)
	if err != nil {
		return nil, err
	}
	if logInfo == nil {
		return nil, ErrLogStreamInvalid
	}
	if logInfo.MarkDeleted {
		return nil, ErrLogStreamDeleted
	}
	return logInfo, nil
}

func (h *Handler) parseJson(scanner *bufio.Scanner, req *LogWriteRequest, rows, failRows *record.Record,
	effectiveEarliestTime int64) int64 {
	tagsStr := "type:failLog"
	tagsExpiredStr := "type:expiredLog"
	if req.retry {
		tagsStr = tagsStr + string(TagsSplitterChar) + LogRetryTag
		tagsExpiredStr = tagsExpiredStr + string(TagsSplitterChar) + LogRetryTag
	}
	var totalLen int64
	p := parserPool.Get()
	defer parserPool.Put(p)
	for scanner.Scan() {
		bytes := scanner.Bytes()
		totalLen += int64(len(bytes)) + NewlineLen
		v, err := p.ParseBytes(bytes)
		if err != nil {
			h.Logger.Error("Unmarshal json fail", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}

		content := v.GetStringBytes(CONTENT)
		if content == nil {
			h.Logger.Error("get content key fail", zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}
		value := v.Get("timestamp")
		if value == nil {
			h.Logger.Error("get timestamp key fail", zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}
		unixTimestamp := v.GetInt64("timestamp")
		if unixTimestamp < UnixTimestampMinMs || unixTimestamp > UnixTimestampMaxMs {
			h.Logger.Error("timestamp wrong format", zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}
		if unixTimestamp < effectiveEarliestTime {
			appendFailRow(failRows, []byte(tagsExpiredStr), scanner.Bytes())
			continue
		}
		tag := v.GetStringBytes(TAGS)
		if req.retry {
			if len(tag) == 0 {
				tag = []byte(LogRetryTag)
			} else {
				tag = append(tag, TagsSplitterChar)
				tag = append(tag, LogRetryTag...)
			}
		}
		appendRow(rows, tag, content, unixTimestamp*1e6)
	}
	return totalLen
}

func (h *Handler) parseJsonV2(scanner *bufio.Scanner, req *LogWriteRequest, rows, failRows *record.Record,
	effectiveEarliestTime int64) int64 {
	tagsStr := "type:failLog"
	tagsExpiredStr := "type:expiredLog"
	if req.retry {
		tagsStr = tagsStr + string(TagsSplitterChar) + LogRetryTag
		tagsExpiredStr = tagsExpiredStr + string(TagsSplitterChar) + LogRetryTag
	}
	var totalLen int64
	p := parserPool.Get()
	defer parserPool.Put(p)

	schemas := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: TAGS},
		record.Field{Type: influx.Field_Type_Int, Name: TIME},
	}
	rows.Schema = schemas
	rows.ColVals = make([]record.ColVal, 2)
	fieldIndex := 2
	schemasMap := make(map[string]int)
	schemasNil := []bool{false, false}

	tags := byteBufferPool.Get()
	defer byteBufferPool.Put(tags)
	for scanner.Scan() {
		bytes := scanner.Bytes()
		totalLen += int64(len(bytes)) + NewlineLen
		v, err := p.ParseBytes(bytes)
		if err != nil {
			h.Logger.Error("Unmarshal json fail", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}
		ob, err := v.Object()
		if err != nil {
			h.Logger.Error("fastjson object get err", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}
		value := v.Get(req.mapping.timestamp)
		if value == nil {
			h.Logger.Error("timestamp json empty", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}
		unixTimestamp := v.GetInt64(req.mapping.timestamp)
		if unixTimestamp < UnixTimestampMinMs || unixTimestamp > UnixTimestampMaxMs {
			h.Logger.Error("timestamp wrong format", zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}
		if unixTimestamp < effectiveEarliestTime {
			appendFailRow(failRows, []byte(tagsExpiredStr), scanner.Bytes())
			continue
		}

		firstTags := true
		contentCnt := 0
		if req.mapping.defaultType == TAGS {
			ob.Visit(func(key []byte, vv *fastjson.Value) {
				if req.mapping.content[util.Bytes2str(key)] {
					col, ok := schemasMap[util.Bytes2str(key)]
					if !ok {
						schemasMap[string(key)] = fieldIndex
						schemasNil = append(schemasNil, true)
						rows.Schema = append(rows.Schema, record.Field{Type: fastJsonTypeToRecordType(vv.Type()), Name: string(key)})
						rows.ColVals = append(rows.ColVals, record.ColVal{})
						appendValueToRecordColumn(rows, fieldIndex, vv)
						fieldIndex++
					} else {
						appendValueToRecordColumn(rows, col, vv)
						schemasNil[col] = true
					}
					contentCnt++
				} else if string(key) != req.mapping.timestamp {
					if !firstTags {
						tags = append(tags, TagsSplitterChar)
					}
					tags = appendTags(tags, key, vv)
					firstTags = false
				}
			})
		} else {
			ob.Visit(func(key []byte, vv *fastjson.Value) {
				if req.mapping.tags[string(key)] {
					if !firstTags {
						tags = append(tags, TagsSplitterChar)
					}
					tags = appendTags(tags, key, vv)
					firstTags = false
				} else if string(key) != req.mapping.timestamp {
					contentCnt++
					col, ok := schemasMap[util.Bytes2str(key)]
					if !ok {
						schemasMap[string(key)] = fieldIndex
						schemasNil = append(schemasNil, true)
						rows.Schema = append(rows.Schema, record.Field{Type: fastJsonTypeToRecordType(vv.Type()), Name: string(key)})
						appendValueToRecordColumn(rows, fieldIndex, vv)
						fieldIndex++
					} else {
						appendValueToRecordColumn(rows, col, vv)
						schemasNil[col] = true
					}
				}
			})
		}

		if contentCnt == 0 {
			h.Logger.Error("content json empty", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", string(scanner.Bytes())))
			appendFailRow(failRows, []byte(tagsStr), scanner.Bytes())
			continue
		}

		if req.retry {
			if len(tags) == 0 {
				tags = []byte(LogRetryTag)
			} else {
				tags = append(tags, TagsSplitterChar)
				tags = append(tags, []byte(LogRetryTag)...)
			}
		}
		appendRowAll(rows, tags, unixTimestamp*1e6, schemasNil)
		tags = tags[:0]
	}
	return totalLen
}

func appendValueToRecordColumn(rows *record.Record, col int, vv *fastjson.Value) {
	switch vv.Type() {
	case fastjson.TypeString:
		sb, _ := vv.StringBytes()
		rows.ColVals[col].AppendByteSlice(sb)
	case fastjson.TypeNumber:
		f, _ := vv.Float64()
		rows.ColVals[col].AppendFloat(f)
	case fastjson.TypeTrue, fastjson.TypeFalse:
		b, _ := vv.Bool()
		rows.ColVals[col].AppendBoolean(b)
	default:
		rows.ColVals[col].AppendString(vv.String())
	}
}

func fastJsonTypeToRecordType(ty fastjson.Type) int {
	switch ty {
	case fastjson.TypeString:
		return influx.Field_Type_String
	case fastjson.TypeNumber:
		return influx.Field_Type_Float
	case fastjson.TypeTrue:
		return influx.Field_Type_Boolean
	case fastjson.TypeFalse:
		return influx.Field_Type_Boolean
	default:
		return influx.Field_Type_String
	}
}

func (h *Handler) IsWriteNode() bool {
	nodeId := meta.DefaultMetaClient.NodeID()
	dataNode, err := h.MetaClient.DataNode(nodeId)
	if err != nil {
		h.Logger.Error("this data node is not used for writing", zap.Error(err), zap.Uint64("nodeId:", nodeId))
		return false
	}
	if !meta2.IsNodeWriter(dataNode.Role) {
		h.Logger.Error("this data node is not used for writing", zap.String("nodeRole", dataNode.Role), zap.Uint64("nodeId:", nodeId))
		return false
	}
	return true
}

// serveWrite receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveRecord(w http.ResponseWriter, r *http.Request, user meta2.User) {
	atomic.AddInt64(&statistics.HandlerStat.WriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.WriteRequestBytesIn, r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.WriteRequestDuration, d)
	}(time.Now())
	h.requestTracker.Add(r, user)

	if !h.IsWriteNode() {
		h.Logger.Error("serveRecord checkNodeRole fail", zap.Error(ErrInvalidWriteNode))
		h.httpErrorRsp(w, ErrorResponse(ErrInvalidWriteNode.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	err := parameterValidate(r)
	if err != nil {
		h.Logger.Error("serveRecord parameterValidate fail", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	req, err := h.getLogWriteRequest(r)
	if err != nil {
		h.Logger.Error("serveRecord getLogWriteRequest fail", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	logInfo, err := h.validateRetentionPolicy(req.repository, req.logStream)
	if err != nil {
		h.Logger.Error("GetLogStreamByName fail", zap.Error(err), zap.String("repository", req.repository),
			zap.String("logStream", req.logStream))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	xLogCompressType := r.Header.Get("x-log-compresstype")
	var totalLen int64
	body := r.Body
	// Handle gzip decoding of the body
	if xLogCompressType == "gzip" {
		b, err := GetGzipReader(r.Body)
		if err != nil {
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			h.Logger.Error("write error:Handle gzip decoding of the body err", zap.Error(errno.NewError(errno.HttpBadRequest)))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
		defer PutGzipReader(b)
		body = b
	}
	bodyLengthString := r.Header.Get("body-length")
	bodyLengthInt64, _ := strconv.ParseInt(bodyLengthString, 10, 64)
	if bodyLengthInt64 != r.ContentLength && bodyLengthInt64 != 0 {
		h.Logger.Error("body-length  is not equal to request ContentLength", zap.Int64("body-length", bodyLengthInt64),
			zap.Int64("request ContentLength", r.ContentLength), zap.String("x-log-compresstype", xLogCompressType))
		h.httpErrorRsp(w, ErrorResponse("body-length  is not equal to request ContentLength", LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	scanner := bufio.NewScanner(body)
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)
	scanner.Split(bufio.ScanLines)

	var rows *record.Record
	failRows := record.NewRecord(schema, false)

	var effectiveEarliestTime int64
	if logInfo.Duration != 0 {
		effectiveEarliestTime = time.Now().UnixMilli() - logInfo.Duration.Milliseconds()
	}
	if req.dataType == JSON {
		rows = record.NewRecord(schema, false)
		totalLen = h.parseJson(scanner, req, rows, failRows, effectiveEarliestTime)
	} else {
		rows = &record.Record{}
		totalLen = h.parseJsonV2(scanner, req, rows, failRows, effectiveEarliestTime)
	}
	if bodyLengthInt64 != totalLen && bodyLengthInt64 != 0 {
		h.Logger.Error("body-length  is not equal to scanner totalLen", zap.Int64("body-length", bodyLengthInt64),
			zap.Int64("scanner totalLen", totalLen), zap.String("x-log-compresstype", xLogCompressType))
		h.httpErrorRsp(w, ErrorResponse("body-length  is not equal to scanner totalLen", LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	SortHelper := record.NewSortHelper()
	rows = SortHelper.Sort(rows)
	SortHelper.Release()

	err = h.RecordWriter.RetryWriteLogRecord(req.repository, req.logStream, req.logStream, rows)
	if err != nil {
		h.Logger.Error("serve records", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse("write log error", LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	if failRows.RowNums() > 0 {
		err = h.RecordWriter.RetryWriteLogRecord(req.repository, req.logStream, req.logStream, failRows)
		if err != nil {
			h.Logger.Error("serve records", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("write fail log error", LogReqErr), http.StatusBadRequest)
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	}
	addLogInsertStatistics(req.repository, req.logStream, totalLen)
}

func addLogInsertStatistics(repoName, logStreamName string, totalLen int64) {
	item := statistics.NewLogKeeperStatItem(repoName, logStreamName)
	statistics.NewLogKeeperStatistics().AddTotalWriteRequestCount(1)
	statistics.NewLogKeeperStatistics().AddTotalWriteRequestSize(totalLen)
	atomic.AddInt64(&item.WriteRequestCount, 1)
	atomic.AddInt64(&item.WriteRequestSize, totalLen)
	statistics.NewLogKeeperStatistics().Push(item)
}

func (h *Handler) serveUpload(w http.ResponseWriter, r *http.Request, user meta2.User) {
	atomic.AddInt64(&statistics.HandlerStat.WriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.WriteRequestBytesIn, r.ContentLength)
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.WriteRequestDuration, d)
	}(time.Now())
	h.requestTracker.Add(r, user)

	if !h.IsWriteNode() {
		h.Logger.Error("serveRecord checkNodeRole fail", zap.Error(ErrInvalidWriteNode))
		h.httpErrorRsp(w, ErrorResponse(ErrInvalidWriteNode.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	repository := r.URL.Query().Get(":repository")
	logStream := r.URL.Query().Get(":logStream")
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		h.Logger.Error("ValidateRepoAndLogStream fail", zap.Error(err), zap.String("repository", repository),
			zap.String("logStream", logStream))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	scanner := bufio.NewScanner(r.Body)
	scanBuf := byteBufferPool.Get()
	defer byteBufferPool.Put(scanBuf)
	scanner.Buffer(scanBuf, ScannerBufferSize)
	scanner.Split(bufio.ScanLines)
	logInfo, err := h.validateRetentionPolicy(repository, logStream)
	if err != nil {
		h.Logger.Error("GetLogStreamByName fail", zap.Error(err), zap.String("repository", repository),
			zap.String("logStream", logStream))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	dur := logInfo.ShardGroupDuration
	date := time.Now().Unix()
	tagsStr := "date:" + strconv.FormatInt(date, 10) + string(TagsSplitterChar) + "type:upload"
	tagsByte := []byte(tagsStr)

	var groupId int
	var groupIdTmp int
	timeStr := r.FormValue("timestamp")
	timeStart, _ := strconv.ParseInt(timeStr, 10, 64)
	rows := record.NewRecord(schema, false)
	rowCount := 0
	timeNow := time.Now().UnixNano()
	var t int64
	for scanner.Scan() {
		if timeStart > 0 {
			t = timeStart + time.Now().UnixNano() - timeNow
		} else {
			t = time.Now().UnixNano()
		}
		groupIdTmp = int(t / (dur.Nanoseconds()))
		if groupId == 0 {
			groupId = groupIdTmp
		} else if groupIdTmp != groupId {
			groupId = groupIdTmp
			err = h.RecordWriter.RetryWriteLogRecord(repository, logStream, logStream, rows)
			if err != nil {
				h.Logger.Error("serve upload", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse("upload log error", LogReqErr), http.StatusBadRequest)
				atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
				return
			}
			rows = record.NewRecord(schema, false)
			rowCount = 0
		} else if rowCount > LogMax {
			err = h.RecordWriter.RetryWriteLogRecord(repository, logStream, logStream, rows)
			if err != nil {
				h.Logger.Error("serve upload", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse("upload log error", LogReqErr), http.StatusBadRequest)
				atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
				return
			}
			rows = record.NewRecord(schema, false)
			rowCount = 0
		}
		appendRow(rows, tagsByte, scanner.Bytes(), t)
		rowCount++
	}
	if rows.RowNums() > 0 {
		err = h.RecordWriter.RetryWriteLogRecord(repository, logStream, logStream, rows)
		if err != nil {
			h.Logger.Error("serve upload", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("upload log error", LogReqErr), http.StatusBadRequest)
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	}
}

// Query parameter
const (
	EmptyValue = ""
	Reverse    = "reverse"
	TimeoutMs  = "timeout_ms"
	Explain    = "explain"
	IsTruncate = "is_truncate"
	From       = "from"
	To         = "to"
	Scroll     = "scroll"
	ScrollId   = "scroll_id"
	Limit      = "limit"
	Highlight  = "highlight"
	Sql        = "sql"
	Select     = "select"
	Query      = "query"
	Https      = "https"
	Http       = "Http"
)

// Err substring
const (
	ErrSyntax             = "syntax error"
	ErrParsingQuery       = "error parsing query"
	ErrShardGroupNotFound = "log shard group not found"
	ErrShardNotFound      = "shard not found"
)

// Query result fields
const (
	Timestamp  = "timestamp"
	Cursor     = "cursor"
	IsOverflow = "is_overflow"
)

func TransYaccSyntaxErr(errorInfo string) string {
	errorInfo = strings.Replace(errorInfo, "$end", "END", -1)
	errorInfo = strings.Replace(errorInfo, "$unk", "UNKNOWN TOKEN", -1)
	errorInfo = strings.Replace(errorInfo, "LPAREN", "LEFT PARENTHESIS", -1)
	errorInfo = strings.Replace(errorInfo, "RPAREN", "RIGHT PARENTHESIS", -1)
	errorInfo = strings.Replace(errorInfo, "IDENT", "IDENTIFIER", -1)
	errorInfo = strings.Replace(errorInfo, "BITWISE_OR", "PIPE OPERATOR", -1)
	return errorInfo
}

func (h *Handler) parseLogQuery(logP *logparser.Parser, r *http.Request, q *influxql.Query) (*influxql.Query, error, int) {
	if logP == nil {
		return nil, nil, 0
	}

	db := r.FormValue("db")
	logParser := logparser.NewYyParser(logP.GetScanner())
	logParser.ParseTokens()
	logCond, err := logParser.GetQuery()
	if err != nil {
		h.Logger.Error("query error! parsing query value", zap.Error(err), zap.String("db", db))
		errorInfo := TransYaccSyntaxErr(err.Error())
		return nil, fmt.Errorf("error parsing query: " + errorInfo), http.StatusBadRequest
	}
	var selectStmt *influxql.SelectStatement
	var ok bool
	if stmt, currOk := q.Statements[0].(*influxql.ExplainStatement); currOk {
		ok = true
		selectStmt = stmt.Statement
	} else {
		selectStmt, ok = q.Statements[0].(*influxql.SelectStatement)
	}
	if !ok {
		errMsgMark := "can not combine log parser statement with statement which is not select statement"
		errMsg := fmt.Sprintf("%s: %v, %v, %v", errMsgMark, zap.Error(err), zap.String("db", db), zap.Any("r", r))
		h.Logger.Error(errMsg)
		return nil, fmt.Errorf(errMsgMark), http.StatusBadRequest
	}

	// Filter condition
	logCondition := logCond.Statements[0].(*influxql.LogPipeStatement).Cond
	if logCondition != nil && selectStmt.Condition != nil {
		selectStmt.Condition = &influxql.BinaryExpr{Op: influxql.AND, LHS: selectStmt.Condition, RHS: logCondition}
	} else if logCondition != nil && selectStmt.Condition == nil {
		selectStmt.Condition = logCondition
	}
	// Unnest source list
	unnest := logCond.Statements[0].(*influxql.LogPipeStatement).Unnest
	if unnest != nil {
		selectStmt.Sources = append(selectStmt.Sources, unnest)
	}

	if selectStmt.Sources != nil {
		if mstStmt, ok := selectStmt.Sources[0].(*influxql.Measurement); ok {
			mstStmt.Database = r.URL.Query().Get(":repository")
		}
	}

	return logCond, nil, 0
}

func splitLogQueryString(s string) []string {
	if len(s) == 0 {
		return nil
	}

	res := make([]string, 0)
	inQuota := false
	var builder strings.Builder
	// split string by '|'
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			inQuota = !inQuota
			builder.WriteByte(s[i])
			continue
		}
		if inQuota {
			builder.WriteByte(s[i])
			continue
		}

		if s[i] == '|' {
			res = append(res, builder.String())
			builder.Reset()
			continue
		}
		builder.WriteByte(s[i])
	}

	if builder.Len() > 0 {
		res = append(res, builder.String())
	}

	return res
}

func removeMulAndSpace(s string) string {
	var res string
	tokens := splitLogQueryString(s)
	for i := range tokens {
		// remove " xxx | content:* | xxx "
		if strings.Contains(tokens[i], ":") {
			subTokens := strings.Split(tokens[i], ":")
			if len(subTokens) != 2 {
				res += tokens[i] + "|"
				continue
			}
			for j := range subTokens[1] {
				if subTokens[1][j] != '*' && subTokens[1][j] != ' ' {
					res += tokens[i] + "|"
					break
				}
			}
			continue
		}
		// remove " xxx | * | xxx "
		for j := range tokens[i] {
			if tokens[i][j] != '*' && tokens[i][j] != ' ' {
				res += tokens[i] + "|"
				break
			}
		}
	}
	if len(res) > 0 {
		return res[:len(res)-1]
	}
	return ""
}

func getLastPipeIndex(query string) int {
	lastPipeIndex := -1
	inQuota := false
	inEscape := false
	for i := 0; i < len(query); i++ {
		if query[i] == '\\' || inEscape {
			inEscape = !inEscape
			continue
		}
		if query[i] == '"' || query[i] == '\'' {
			inQuota = !inQuota
			continue
		}
		if inQuota {
			continue
		}
		if query[i] == '|' {
			lastPipeIndex = i
		}
	}
	return lastPipeIndex
}

func removePreSpace(s string) string {
	for i := range s {
		if s[i] != ' ' {
			return s[i:]
		}
	}
	return ""
}

func generateDefaultStatement() influxql.Statement {
	return &influxql.SelectStatement{
		Fields: influxql.Fields{
			{Expr: &influxql.Wildcard{
				Type: influxql.MUL,
			}},
		},
	}
}

func (h *Handler) getPplQuery(r *http.Request, pr io.Reader, sqlQuery *influxql.Query) (*influxql.Query, error, int) {
	logP := logparser.NewParser(pr)
	defer logP.Release()

	pplQuery, err, status := h.parseLogQuery(logP, r, sqlQuery)
	if err != nil {
		return nil, err, status
	}

	return pplQuery, nil, http.StatusOK
}

// rewriteStatementForLogStore is used to construct time filter, generate adaptive time buckets, and generate dims.
func (h *Handler) rewriteStatementForLogStore(selectStmt *influxql.SelectStatement, param *QueryParam, r *http.Request) error {
	var isIncQuery bool
	selectStmt.RewriteUnnestSource()
	selectStmt.Sources = influxql.Sources{&influxql.Measurement{Name: r.URL.Query().Get(":logStream"), Database: r.URL.Query().Get(":repository"), RetentionPolicy: r.URL.Query().Get(":logStream")}}
	if param != nil {
		isIncQuery = param.IncQuery
		selectStmt.Limit = param.Limit
		if len(selectStmt.SortFields) == 0 {
			selectStmt.SortFields = []*influxql.SortField{{Name: "time", Ascending: param.Ascending}}
		}
		timeCond := &influxql.BinaryExpr{
			LHS: &influxql.BinaryExpr{
				LHS: &influxql.VarRef{Val: "time", Type: influxql.Time},
				Op:  influxql.GTE,
				RHS: &influxql.IntegerLiteral{Val: param.TimeRange.start},
			},
			Op: influxql.AND,
			RHS: &influxql.BinaryExpr{
				LHS: &influxql.VarRef{Val: "time", Type: influxql.Time},
				Op:  influxql.LT,
				RHS: &influxql.IntegerLiteral{Val: param.TimeRange.end},
			},
		}
		if interval, err := selectStmt.GroupByInterval(); err == nil && interval == 0 && isIncQuery {
			if param.Ascending && param.TimeRange.end <= param.TimeRange.start {
				errMsg := fmt.Sprintf("The query start time and end time are invalid. ascending: %t, startTime:%d, endTime:%d",
					param.Ascending, param.TimeRange.start/1e6, param.TimeRange.end/1e6)
				h.Logger.Error(errMsg)
				return fmt.Errorf(errMsg)
			}
			groupByTimeInterval := logstore.GetAdaptiveTimeBucket(time.Unix(0, param.TimeRange.start), time.Unix(0, param.TimeRange.end), param.Ascending)
			selectStmt.SetTimeInterval(groupByTimeInterval)
			param.GroupBytInterval = groupByTimeInterval
		}
		if selectStmt.Condition == nil {
			selectStmt.Condition = timeCond
		} else {
			selectStmt.Condition = &influxql.BinaryExpr{
				LHS: selectStmt.Condition,
				Op:  influxql.AND,
				RHS: timeCond,
			}
		}
		selectStmt.Scroll = influxql.Scroll{
			Scroll_id: param.Scroll_id,
			Scroll:    param.Scroll,
			Timeout:   param.Timeout,
		}
	}
	if selectStmt.Dimensions == nil {
		selectStmt.Dimensions = influxql.Dimensions{
			&influxql.Dimension{Expr: &influxql.Wildcard{Type: influxql.MUL}},
		}
	}
	return nil
}

// syntax format: Ppl0 | Ppl1 | ... | SQL
// Ppl(Pipe Language) must come first and at least one ppl must be present. Sql must come last and can be omitted.
func getPplAndSqlFromQuery(query string) (string, string) {
	lastPipeIndex := getLastPipeIndex(query)
	if lastPipeIndex == -1 {
		return query, ""
	}
	if strings.HasPrefix(removePreSpace(strings.ToLower(query[lastPipeIndex+1:])), "select ") {
		sql := strings.TrimSpace(query[lastPipeIndex+1:])
		ppl := strings.TrimSpace(query[:lastPipeIndex])
		ppl = removeMulAndSpace(ppl)
		return ppl, sql
	}
	return query, ""
}

// reutrn pplQuery for highlight
func (h *Handler) getSqlAndPplQuery(r *http.Request, param *QueryParam, user meta2.User) (*influxql.Query, *influxql.Query, error, int) {
	qp := h.getQueryFromRequest(r, param, user)
	ppl, sql := getPplAndSqlFromQuery(qp)
	// sql parser
	var sqlQuery *influxql.Query
	var err error
	var status int
	if sql != "" {
		sqlQuery, err, status = h.getSqlQuery(r, strings.NewReader(sql))
		if err != nil {
			return nil, nil, err, status
		}
	} else {
		stmt := generateDefaultStatement()
		sqlQuery = &influxql.Query{Statements: influxql.Statements{stmt}}
	}

	// ppl parser
	var pplQuery *influxql.Query
	if ppl != "" {
		pplQuery, err, status = h.getPplQuery(r, strings.NewReader(ppl), sqlQuery)
		if err != nil {
			return nil, nil, err, status
		}
	}

	// rewrite for logstore
	if selectStmt, ok := sqlQuery.Statements[0].(*influxql.SelectStatement); ok {
		if err = h.rewriteStatementForLogStore(selectStmt, param, r); err != nil {
			return nil, nil, err, http.StatusBadRequest
		}
	}

	if param.Explain {
		sqlQuery = &influxql.Query{Statements: influxql.Statements{&influxql.ExplainStatement{
			Statement: sqlQuery.Statements[0].(*influxql.SelectStatement),
			Analyze:   true,
		}}}
	}

	return sqlQuery, pplQuery, nil, http.StatusOK
}

func (h *Handler) ValidateAndCheckLogStreamExists(repoName, streamName string) error {
	err := ValidateRepoAndLogStream(repoName, streamName)
	if err != nil {
		h.Logger.Error("validate repo and logStream fail", zap.Error(err), zap.String("repoName", repoName), zap.String("streamName", streamName))
		return err
	}
	db, err := h.MetaClient.Database(repoName)
	if err != nil {
		h.Logger.Error("repo not found", zap.Error(err), zap.String("repoName", repoName))
		return err
	}
	_, err = db.GetRetentionPolicy(streamName)
	if err != nil {
		h.Logger.Error("stream not found", zap.Error(err), zap.String("repoName", repoName), zap.String("streamName", streamName))
		return err
	}
	return nil
}

func (h *Handler) serveLogQuery(w http.ResponseWriter, r *http.Request, param *QueryParam, user meta2.User) (*Response, *influxql.Query, error, int) {
	atomic.AddInt64(&statistics.HandlerStat.QueryRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, 1)
	start := time.Now()
	defer func() {
		atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.QueryRequestDuration, time.Since(start).Nanoseconds())
	}()
	h.requestTracker.Add(r, user)

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	if syscontrol.DisableReads {
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return nil, nil, fmt.Errorf("disable read"), http.StatusForbidden
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)
	// new reader for sql statement
	q, pplQuery, err, status := h.getSqlAndPplQuery(r, param, user)
	if err != nil {
		return nil, nil, err, status
	}
	epoch := strings.TrimSpace(r.FormValue("epoch"))
	db := r.FormValue("db")
	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(db) {
		qDuration = statistics.NewSqlSlowQueryStatistics(db)
		defer func() {
			d := time.Since(start).Nanoseconds()
			//d := time.Now().Sub(start)
			if d > time.Second.Nanoseconds()*10 {
				qDuration.AddDuration("TotalDuration", d)
				statistics.AppendSqlQueryDuration(qDuration)
				h.Logger.Info("slow query", zap.Int64("duration", d), zap.String("db", qDuration.DB), zap.String("query", qDuration.Query))
			}
		}()
	}

	// Sanitize the request query params so it doesn't show up in the response logger.
	// Do this before anything else so a parsing error doesn't leak passwords.
	sanitize(r)

	// Check authorization.
	err = h.checkAuthorization(user, q, db)
	if err != nil {
		return nil, nil, fmt.Errorf("error authorizing query: " + err.Error()), http.StatusForbidden
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		return nil, nil, err, http.StatusBadRequest
	}
	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"
	opts := *query2.NewExecutionOptions(db, r.FormValue("rp"), nodeID, chunkSize, innerChunkSize, false, r.Method == "GET", true,
		atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1)
	if param != nil {
		opts.IncQuery, opts.QueryID, opts.IterID = param.IncQuery, param.QueryID, param.IterID
	}

	opts.Authorizer = h.getAuthorizer(user)
	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		done := make(chan struct{})

		opts.AbortCh = closing
		defer func() {
			close(done)
		}()
		go func() {
			select {
			case <-done:
			case <-r.Context().Done():
			}
			close(closing)
		}()
	}

	// Execute query
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	// If we are running in async mode, open a goroutine to drain the results
	// and return with a StatusNoContent.
	if async {
		go h.async(q, results)
		h.writeHeader(w, http.StatusNoContent)
		return nil, nil, err, http.StatusNoContent
	}

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	stmtID2Result := make(map[int]*query.Result)

	// pull all results from the channel
	rows := 0
	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		// Throws out errors during query execution
		if r.Err != nil {
			return nil, nil, r.Err, http.StatusNoContent
		}

		// if requested, convert result timestamps to epoch
		if epoch != "" {
			convertToEpoch(r, epoch)
		}

		// Write out result immediately if chunked.
		if chunked {
			n, _ := rw.WriteResponse(Response{
				Results: []*query.Result{r},
			})
			atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
			w.(http.Flusher).Flush()
			continue
		}

		rows = h.getResultRowsCnt(r, rows)
		if !h.updateStmtId2Result(r, stmtID2Result) {
			continue
		}

		// Drop out of this loop and do not process further results when we hit the row limit.
		if h.Config.MaxRowLimit > 0 && rows >= h.Config.MaxRowLimit {
			// If the result is marked as partial, remove that partial marking
			// here. While the series is partial and we would normally have
			// tried to return the rest in the next chunk, we are not using
			// chunking and are truncating the series so we don't want to
			// signal to the client that we plan on sending another JSON blob
			// with another result.  The series, on the other hand, still
			// returns partial true if it was truncated or had more data to
			// send in a future chunk.
			r.Partial = false
			break
		}
	}

	resp := h.getStmtResult(stmtID2Result)
	opts.IterID++
	// If it's not chunked we buffered everything in memory, so write it out
	if !chunked {
		if param == nil {
			n, _ := rw.WriteResponse(resp)
			atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
		} else {
			param.IterID = opts.IterID
			if !param.Explain && q.Statements != nil && len(q.Statements) > 0 && len(q.Statements[0].(*influxql.SelectStatement).UnnestSource) > 0 {
				if q.Statements[0].(*influxql.SelectStatement).Fields != nil {
					for _, v := range q.Statements[0].(*influxql.SelectStatement).Fields {
						if _, ok := v.Expr.(*influxql.Call); ok {
							return &resp, q, nil, http.StatusOK
						}
					}
				}
				return &resp, pplQuery, nil, http.StatusOK
			}
			return &resp, pplQuery, nil, http.StatusOK
		}
	}
	if !chunked {
		n, _ := rw.WriteResponse(resp)
		atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
	}

	return nil, nil, nil, http.StatusOK
}

func getQueryLogRequest(r *http.Request) (*QueryLogRequest, error) {
	var err error
	queryLogRequest := &QueryLogRequest{}
	_, err = getQueryQueryLogRequest(r, queryLogRequest)
	if err != nil {
		return nil, err
	}
	if r.FormValue(Reverse) == EmptyValue {
		queryLogRequest.Reverse = true
	} else {
		queryLogRequest.Reverse, err = strconv.ParseBool(r.FormValue(Reverse))
		if err != nil {
			return nil, errno.NewError(errno.ReverseValueIllegal)
		}
	}
	if r.FormValue(TimeoutMs) == EmptyValue {
		queryLogRequest.Timeout = DefaultLogQueryTimeout
	} else {
		queryLogRequest.Timeout, err = strconv.Atoi(r.FormValue(TimeoutMs))
		if err != nil {
			return nil, errno.NewError(errno.TimeoutMsValueIllegal)
		}
		if queryLogRequest.Timeout < MinTimeoutMs || queryLogRequest.Timeout > MaxTimeoutMs {
			return nil, errno.NewError(errno.TimeoutMsRangeIllegal, MinTimeoutMs, MaxTimeoutMs)
		}
	}

	queryLogRequest.Explain = r.FormValue(Explain) == "true"
	queryLogRequest.IsTruncate = r.FormValue(IsTruncate) == "true"

	queryLogRequest.From, err = strconv.ParseInt(r.FormValue(From), 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.FromValueIllegal)
	}
	queryLogRequest.To, err = strconv.ParseInt(r.FormValue(To), 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.ToValueIllegal)
	}
	if queryLogRequest.From > queryLogRequest.To {
		return nil, errno.NewError(errno.FromValueLargerThanTo)
	}
	if queryLogRequest.From < MinFromValue {
		return nil, errno.NewError(errno.FromValueLowerThanMin, MinFromValue)
	}
	if queryLogRequest.To > (MaxToValue / int64(1e6)) {
		return nil, errno.NewError(errno.ToValueLargerThanMax, MaxToValue/int64(1e6))
	}
	maxSecond := MaxToValue / int64(1e6)
	if maxSecond < queryLogRequest.To {
		queryLogRequest.To = maxSecond
	}
	if maxSecond < queryLogRequest.From {
		queryLogRequest.From = maxSecond
	}

	queryLogRequest.Scroll = r.FormValue(Scroll)
	queryLogRequest.Scroll_id = r.FormValue(ScrollId)
	if queryLogRequest.Scroll_id != EmptyValue && (len(queryLogRequest.Scroll_id) < MinScrollIdLen || len(queryLogRequest.Scroll_id) > MaxScrollIdLen) {
		return nil, errno.NewError(errno.ScrollIdRangeInvalid, MinScrollIdLen, MaxScrollIdLen)
	}
	_, err = getQueryCommonLogRequest(r, queryLogRequest)
	if err != nil {
		return nil, err
	}
	if !queryLogRequest.Sql {
		queryLogRequest.Query = removeLastSelectStr(queryLogRequest.Query)
	}

	return queryLogRequest, nil
}

func getQueryCommonLogRequest(r *http.Request, queryLogRequest *QueryLogRequest) (*QueryLogRequest, error) {
	var err error
	if r.FormValue(Limit) == EmptyValue {
		queryLogRequest.Limit = DefaultLogLimit
	} else {
		queryLogRequest.Limit, err = strconv.Atoi(r.FormValue(Limit))
		if err != nil {
			return nil, errno.NewError(errno.LimitValueIllegal)
		}
		if queryLogRequest.Limit > MaxLogLimit {
			return nil, errno.NewError(errno.LimitValueLargerThanMax, MaxLogLimit)
		}
		if queryLogRequest.Limit < MinLogLimit {
			return nil, errno.NewError(errno.LimitValueLowerThanMin, MinLogLimit)
		}
	}
	if r.FormValue(Highlight) == EmptyValue {
		queryLogRequest.Highlight = false
	} else {
		queryLogRequest.Highlight, err = strconv.ParseBool(r.FormValue(Highlight))
		if err != nil {
			return nil, errno.NewError(errno.HighlightValueIllegal)
		}
	}

	if r.FormValue(Sql) == EmptyValue {
		// no sql in default
		queryLogRequest.Sql = false
	} else {
		queryLogRequest.Sql, err = strconv.ParseBool(r.FormValue(Sql))
		if err != nil {
			return nil, errno.NewError(errno.SqlValueIllegal)
		}
	}

	return queryLogRequest, nil
}

func removeLastSelectStr(queryStr string) string {
	if queryStr == EmptyValue {
		return ""
	}
	queryStr = strings.TrimSpace(queryStr)
	index := strings.LastIndex(queryStr, "|")
	if index > 0 &&
		strings.HasPrefix(removePreSpace(strings.ToLower(queryStr[index+1:])), Select) {
		queryStr = queryStr[:index]
	}

	return queryStr
}

func getQueryQueryLogRequest(r *http.Request, queryLogRequest *QueryLogRequest) (*QueryLogRequest, error) {
	if len(r.FormValue(Query)) > MaxQueryLen {
		return nil, errno.NewError(errno.TooLongQuery, MaxQueryLen)
	}
	queryLogRequest.Query = r.FormValue(Query)
	return queryLogRequest, nil
}

func (h *Handler) serveQueryLogWhenErr(w http.ResponseWriter, err error, t time.Time, repository, logStream string) {
	var count int64
	var logs []map[string]interface{}
	if QuerySkippingError(err.Error()) {
		res := QueryLogResponse{Success: true, Code: "200", Message: "", Request_id: uuid.TimeUUID().String(),
			Count: count, Progress: "Complete", Logs: logs, Took_ms: time.Since(t).Milliseconds()}
		b, err := json.Marshal(res)
		if err != nil {
			h.Logger.Error("query log marshal res fail! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}

		h.writeHeader(w, http.StatusOK)
		addLogQueryStatistics(repository, logStream)
		w.Write(b)
		return
	}

	if strings.Contains(err.Error(), ErrSyntax) || strings.Contains(err.Error(), ErrParsingQuery) {
		h.Logger.Error("query log fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
	} else {
		h.Logger.Error("query log fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
	}
}

func (h *Handler) getQueryLogExplainResult(resp *Response, repository, logStream string, w http.ResponseWriter, t time.Time) {
	var count int64
	var logs []map[string]interface{}
	explain := ""
	for _, result := range resp.Results {
		for _, s := range result.Series {
			for _, v := range s.Values {
				explain += v[0].(string)
				explain += "\n"
			}
		}
	}
	res := QueryLogResponse{Success: true, Code: "200", Message: "", Request_id: uuid.TimeUUID().String(),
		Count: count, Progress: "Complete", Logs: logs, Took_ms: time.Since(t).Milliseconds(), Explain: explain}
	b, err := json.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	h.writeHeader(w, http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	w.Write(b)
}

func (h *Handler) setRecord(rec map[string]interface{}, field string, value interface{}, truncate bool) {
	if !truncate {
		rec[field] = value
		return
	}
	strVal, ok := value.(string)
	if !ok {
		rec[field] = value
		return
	}
	if len(strVal) > MaxRowLen {
		rec[IsOverflow] = true
		rec[field] = strVal[:MaxRowLen]
	} else {
		rec[field] = value
	}
}

func (h *Handler) getQueryLogResult(resp *Response, logCond *influxql.Query, para *QueryParam, repository, logStream string) (int64, []map[string]interface{}, error) {
	var count int64
	var logs []map[string]interface{}
	var unnest *influxql.Unnest
	var unnestField string
	if logCond != nil {
		unnest = logCond.Statements[0].(*influxql.LogPipeStatement).Unnest
	}
	var unnestFunc *immutable.UnnestMatchAll
	if unnest != nil {
		unnestExpr, ok := unnest.Expr.(*influxql.Call)
		if ok {
			return 0, nil, fmt.Errorf("the type of unnest error")
		}
		unnestField = unnestExpr.Args[1].(*influxql.VarRef).Val
		if unnestField == TAGS {
			unnestField = Tag
		}
		var err error
		unnestFunc, err = immutable.NewUnnestMatchAll(unnest)
		if err != nil {
			return 0, nil, err
		}
	}
	for i := range resp.Results {
		for _, s := range resp.Results[i].Series {
			for j := range s.Values {
				rec := map[string]interface{}{}
				recMore := map[string]interface{}{}
				hasMore := false
				rec[IsOverflow] = false
				for id, c := range s.Columns {
					if unnest != nil && c == unnestField {
						unnestResult := unnestFunc.Get(s.Values[j][id].(string))
						for k, v := range unnestResult {
							rec[k] = v
						}
					}
					switch c {
					case TIME:
						v, _ := s.Values[j][id].(time.Time)
						rec[Timestamp] = v.UnixMilli()
						nano := v.UnixNano()
						rec[Cursor] = base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(nano, 10) + "|" + s.Tags[Scroll] + "^^"))
					case TAGS:
						tags, _ := s.Values[j][id].(string)
						rec[TAGS] = strings.Split(tags, tokenizer.TAGS_SPLITTER)
					case CONTENT:
						h.setRecord(rec, CONTENT, s.Values[j][id], para.Truncate)
					default:
						hasMore = true
						h.setRecord(recMore, c, s.Values[j][id], para.Truncate)
					}
				}
				if hasMore {
					if content, ok := rec[CONTENT]; ok {
						recMore[CONTENT] = content
					}
					rec[CONTENT] = recMore
				}
				count += 1
				if para.Highlight {
					rec[Highlight] = h.getHighlightFragments(rec, logCond, tokenizer.VersionLatest)
				}
				logs = append(logs, rec)
			}
		}
	}
	return count, logs, nil
}

func getQueryAnaRequest(r *http.Request) (*QueryAggRequest, error) {
	var err error
	queryAggRequest := &QueryAggRequest{}
	_, err = getQueryParaAggRequest(r, queryAggRequest)
	if err != nil {
		return nil, err
	}
	if r.FormValue(TimeoutMs) == EmptyValue {
		queryAggRequest.Timeout = DefaultAggLogQueryTimeout
	} else {
		queryAggRequest.Timeout, err = strconv.Atoi(r.FormValue(TimeoutMs))
		if err != nil {
			return nil, errno.NewError(errno.TimeoutMsValueIllegal)
		}
		if queryAggRequest.Timeout < MinTimeoutMs || queryAggRequest.Timeout > MaxTimeoutMs {
			return nil, errno.NewError(errno.TimeoutMsRangeIllegal, MinTimeoutMs, MaxTimeoutMs)
		}
	}
	queryAggRequest.From, err = strconv.ParseInt(r.FormValue(From), 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.FromValueIllegal)
	}
	queryAggRequest.To, err = strconv.ParseInt(r.FormValue(To), 10, 64)
	if err != nil {
		return nil, errno.NewError(errno.ToValueIllegal)
	}
	if queryAggRequest.From > queryAggRequest.To {
		return nil, errno.NewError(errno.FromValueLargerThanTo)
	}
	if queryAggRequest.From < MinFromValue {
		return nil, errno.NewError(errno.FromValueLowerThanMin, MinFromValue)
	}
	if queryAggRequest.To > (MaxToValue / int64(1e6)) {
		return nil, errno.NewError(errno.ToValueLargerThanMax, MaxToValue/int64(1e6))
	}
	maxSecond := (MaxToValue / int64(1e6))
	if maxSecond < queryAggRequest.To {
		queryAggRequest.To = maxSecond
	}
	if maxSecond < queryAggRequest.From {
		queryAggRequest.From = maxSecond - 1
	}
	queryAggRequest.Scroll = r.FormValue(Scroll)
	queryAggRequest.Scroll_id = r.FormValue(ScrollId)
	if queryAggRequest.Scroll_id != EmptyValue && (len(queryAggRequest.Scroll_id) < MinScrollIdLen || len(queryAggRequest.Scroll_id) > MaxScrollIdLen) {
		return nil, errno.NewError(errno.ScrollIdRangeInvalid, MinScrollIdLen, MaxScrollIdLen)
	}
	return queryAggRequest, nil
}

type QueryAggRequest struct {
	Query     string `json:"query,omitempty"`
	Timeout   int    `json:"timeout_ms,omitempty"`
	From      int64  `json:"from,omitempty"`
	To        int64  `json:"to,omitempty"`
	Scroll    string `json:"scroll,omitempty"`
	Scroll_id string `json:"scroll_id,omitempty"`
	Sql       bool   `json:"sql,omitempty"`
	IncQuery  bool
	Explain   bool `json:"explain,omitempty"`
}

func getQueryParaAggRequest(r *http.Request, queryAggRequest *QueryAggRequest) (*QueryAggRequest, error) {
	if len(r.FormValue(Query)) > MaxQueryLen {
		return nil, errno.NewError(errno.TooLongQuery, MaxQueryLen)
	}
	queryAggRequest.Query = r.FormValue(Query)
	return queryAggRequest, nil
}

func getQueryAggRequest(r *http.Request) (*QueryAggRequest, error) {
	queryAggRequest, err := getQueryAnaRequest(r)
	if err != nil {
		return queryAggRequest, err
	}

	if r.FormValue(Sql) == EmptyValue {
		// no sql in default
		queryAggRequest.Sql = false
	} else {
		queryAggRequest.Sql, err = strconv.ParseBool(r.FormValue(Sql))
		if err != nil {
			return nil, errno.NewError(errno.SqlValueIllegal)
		}
	}

	if queryAggRequest.Sql == false {
		queryAggRequest.Query = removeLastSelectStr(queryAggRequest.Query)
	}
	if r.FormValue(Explain) == "true" {
		// no sql in default
		queryAggRequest.Explain = true
	} else {
		queryAggRequest.Explain = false
	}
	return queryAggRequest, nil
}

type QueryLogAggResponse struct {
	Success    bool         `json:"success,omitempty"`
	Code       string       `json:"code,omitempty"`
	Message    string       `json:"message,omitempty"`
	Request_id string       `json:"request_id,omitempty"`
	Count      int64        `json:"total_size"`
	Progress   string       `json:"progress,omitempty"`
	Histograms []Histograms `json:"histograms,omitempty"`
	Took_ms    int64        `json:"took_ms,omitempty"`
	Scroll_id  string       `json:"scroll_id,omitempty"`
	Explain    string       `json:"explain,omitempty"`
}

func GetMSByScrollID(id string) (int64, error) {
	scrollIDByte, err := base64.StdEncoding.DecodeString(id)
	if err != nil {
		return 0, err
	}
	id = string(scrollIDByte)
	arrFirst := strings.SplitN(id, "^", 3)
	if len(arrFirst) != 3 {
		return 0, errno.NewError(errno.WrongScrollId)
	}

	if arrFirst[0] == EmptyValue {
		currT, err := strconv.ParseInt(arrFirst[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return currT / 1e6, nil
	}

	arr := strings.Split(arrFirst[0], "|")
	n, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		return 0, err
	}
	if len(arr) != 4 && len(arr) != 5 {
		return 0, err
	}
	return n / 1e6, nil
}

type Histograms struct {
	From  int64 `json:"from"`
	To    int64 `json:"to"`
	Count int64 `json:"count"`
}

func GenZeroHistogram(opt query2.ProcessorOptions, start, end int64, ascending bool) []Histograms {
	startTime, _ := opt.Window(start)
	_, endTime := opt.Window(end)
	if !ascending {
		startTime, endTime = endTime, startTime
		start, end = end, start
	}
	interval := opt.Interval.Duration.Nanoseconds()
	numInterval := int64(math.Floor((float64(endTime - startTime)) / float64(interval)))
	hits := make([]Histograms, 0, numInterval)
	for i := int64(0); i < numInterval; i++ {
		hits = append(hits, Histograms{
			From:  (startTime + i*interval) / 1e6,
			To:    (startTime + (i+1)*interval) / 1e6,
			Count: 0,
		})
	}
	hits[0].From = start / 1e6
	hits[len(hits)-1].To = end / 1e6
	return hits
}

func initScrollIDAndReverseProxy(h *Handler, w http.ResponseWriter, r *http.Request, scrollID string) (string, bool) {
	var err error
	if scrollID == EmptyValue {
		scrollID = strconv.FormatInt(time.Now().UnixNano(), 10) + "-0"
	} else {
		if h.Config.HTTPSEnabled {
			r.URL.Scheme = Https
		} else {
			r.URL.Scheme = Http
		}

		splits := strings.Split(scrollID, "-")
		if len(splits) != 3 {
			err = errno.NewError(errno.ScrollIdIllegal)
			h.Logger.Error("error format scroll_id! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return scrollID, false
		}
		nodeId, err := strconv.ParseUint(splits[0], 10, 64)
		if err != nil {
			h.Logger.Error("error format scroll_id! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return scrollID, false
		}
		if nodeId != meta.DefaultMetaClient.NodeID() {
			nodes, err := h.MetaClient.DataNodes()
			if err != nil {
				h.Logger.Error("error get nodes! ", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
				return scrollID, false
			}

			var nodeInfo *meta2.DataNode
			for i := range nodes {
				if nodes[i].ID == nodeId {
					nodeInfo = &nodes[i]
				}
			}

			if nodeInfo != nil && nodeInfo.Status == serf.StatusAlive {
				targetHost := proxy.MergeHost(nodeInfo.Host, h.Config.BindAddress)
				reverseProxy, err := proxy.NewPool().Get(r.URL, targetHost)
				if err == nil {
					r.Header.Set(LogProxy, "true")
					reverseProxy.ServeHTTP(w, r)
					return scrollID, false
				} else {
					h.Logger.Error("get proxy fail! ", zap.Error(err), zap.String("targetHost", targetHost), zap.Uint64("nodeId", nodeId))
				}
			}

			h.Logger.Error("error nodeId in scroll_id! ", zap.Error(err), zap.Uint64("nodeId", nodeId))
			// reinit Scroll_id
			scrollID = strconv.FormatInt(time.Now().UnixNano(), 10) + "-0"
		} else {
			scrollID = scrollID[len(splits[0])+1:]
		}
	}
	return scrollID, true
}

func wrapIncAggLogQueryErr(err error) error {
	if isIncAggLogQueryRetryErr(err) {
		return errno.NewError(errno.FailedRetryInvalidCache)
	}
	return err
}

func isIncAggLogQueryRetryErr(err error) bool {
	return errno.Equal(err, errno.FailedGetNodeMaxIterNum) || errno.Equal(err, errno.FailedGetGlobalMaxIterNum) || errno.Equal(err, errno.FailedGetIncAggItem)
}

func IncQuerySkippingError(err error) bool {
	return errno.Equal(err, errno.FailedRetryInvalidCache)
}
