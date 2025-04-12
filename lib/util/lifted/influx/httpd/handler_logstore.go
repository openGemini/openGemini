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

package httpd

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/mux"
	"github.com/influxdata/influxdb/uuid"
	"github.com/openGemini/openGemini/lib/bufferpool"
	compression "github.com/openGemini/openGemini/lib/compress"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
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
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/valyala/fastjson"
	"go.uber.org/zap"
)

const (
	// LogReqErr default error
	LogReqErr = "CSSOP.00050001"
)

// bad req
var (
	ErrLogRepoEmpty         = errors.New("repository name should not be none")
	ErrLogStreamEmpty       = errors.New("logstream name should not be none")
	ErrLogStreamDeleted     = errors.New("logstream being deleted")
	ErrLogStreamInvalid     = errors.New("logstream invalid in retentionPolicy")
	ErrInvalidRepoName      = errors.New("invalid repository name")
	ErrInvalidLogStreamName = errors.New("invalid logstream name")
	ErrInvalidWriteNode     = errors.New("this data node is not used for writing")
)

const (
	MaxTtl               int64 = 3000
	PermanentSaveTtl     int64 = 3650
	ScannerBufferSize    int   = 10 * 1024 * 1024
	MaxContentLen        int   = 10 * 1024 * 1024
	MaxRequestBodyLength int64 = 100 * 1024 * 1024
	MaxLogTagsLen              = 1024 * 1024
	MaxUnixTimestampNs   int64 = 4102416000000000000
	MinUnixTimestampNs   int64 = 1e16
	NewlineLen           int64 = 1

	MaxRowLen                                = 3500
	DefaultAggLogQueryTimeout                = 500
	IncAggLogQueryRetryCount             int = 3
	DefaultMaxLogStoreAnalyzeResponseNum     = 100

	MaxSplitCharLen         int = 128
	UTCPrefix               int = len("UTC")
	MarshalFieldPunctuation int = len(`,"":`)
)

type measurementInfo struct {
	name            string
	database        string
	retentionPolicy string
}

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
	repository := mux.Vars(r)[Repository]
	if err := ValidateRepository(repository); err != nil {
		logger.GetLogger().Error("serveCreateRepository", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	options := &obs.ObsOptions{}
	dec := json2.NewDecoder(r.Body)
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
	repository := mux.Vars(r)[Repository]
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
	buffer, err := json2.Marshal(repoList)
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
	repository := mux.Vars(r)[Repository]
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
	buffer, err := json2.Marshal(logStreams)
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
	repository := mux.Vars(r)[Repository]
	if err := ValidateRepository(repository); err != nil {
		logger.GetLogger().Error("serveUpdateRepository", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	logger.GetLogger().Info("serveUpdateRepository", zap.String("repository", repository))

	h.writeHeader(w, http.StatusOK)
}

func (h *Handler) getDefaultSchemaForLog(opt *meta2.Options) (*meta2.ColStoreInfo, []*proto2.FieldSchema, *influxql.IndexRelation, *meta2.ShardKeyInfo, int32) {
	colStoreInfo := meta2.NewColStoreInfo([]string{"time"}, []string{"time"}, nil, 0, "block")
	indexR := &influxql.IndexRelation{
		Rid:        0,
		Oids:       []uint32{uint32(index.BloomFilterFullText)},
		IndexNames: []string{index.BloomFilterFullTextIndex},
		IndexList:  []*influxql.IndexList{{IList: []string{}}},
		IndexOptions: []*influxql.IndexOptions{{Options: []*influxql.IndexOption{
			{Tokens: opt.SplitChar, Tokenizers: "standard"},
		}}},
	}
	ski := &meta2.ShardKeyInfo{Type: "hash"}
	return colStoreInfo, nil, indexR, ski, 0
}

func (h *Handler) serveCreateLogstream(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := mux.Vars(r)[Repository]
	logStream := mux.Vars(r)[LogStream]
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		logger.GetLogger().Error("serveCreateLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	options := &meta2.Options{}
	options.InitDefault()
	dec := json2.NewDecoder(r.Body)
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
	colStoreInfo, schemaInfo, indexRelation, ski, numOfShards := h.getDefaultSchemaForLog(options)
	if _, err := h.MetaClient.CreateMeasurement(repository, logStream, logStream, ski, numOfShards, indexRelation, config.COLUMNSTORE, colStoreInfo, schemaInfo, options); err != nil {
		logger.GetLogger().Error("create logStream failed", zap.String("name", logStream), zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}

	h.writeHeader(w, http.StatusOK)
}

func (h *Handler) serveDeleteLogstream(w http.ResponseWriter, r *http.Request, user meta2.User) {
	logStream := mux.Vars(r)[LogStream]
	repository := mux.Vars(r)[Repository]
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
	repository := mux.Vars(r)[Repository]
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
	buffer, err := json2.Marshal(logStreams)
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
	logStream := mux.Vars(r)[LogStream]
	repository := mux.Vars(r)[Repository]
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
	jsonEncoder := json2.NewEncoder(bf)
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
	logStream := mux.Vars(r)[LogStream]
	repository := mux.Vars(r)[Repository]
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		logger.GetLogger().Error("serveUpdateLogstream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	option := &meta2.Options{}
	dec := json2.NewDecoder(r.Body)
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

type FailLogType uint8

const (
	JSON      LogDataType = 0
	JSONArray LogDataType = 1

	Tags          = "tags"
	Tag           = "tag"
	Content       = "content"
	Time          = "time"
	FailTag       = "__fail_tag__"
	FailLog       = "__fail_log__"
	RetryTag      = "__retry_tag__"
	FailLogTag    = "failLog"
	ExpiredLogTag = "expiredLog"
	BigLogTag     = "bigLog"
	MstSuffix     = "_0000"
)

const (
	ParseError FailLogType = iota
	TimestampError
	ObjectError
	ContentFieldError
	NoContentError
)

var (
	LogMax           = 1000
	byteBufferPool   = bufferpool.NewByteBufferPool(100*1024, cpu.GetCpuNum(), bufferpool.MaxLocalCacheLen)
	fieldBufferPool  = bufferpool.NewByteBufferPool(10*1024, cpu.GetCpuNum()*10, bufferpool.MaxLocalCacheLen)
	recordBufferPool = bufferpool.NewByteBufferPool(10*1024*1024, cpu.GetCpuNum(), bufferpool.MaxLocalCacheLen)
	parserPool       = &fastjson.ParserPool{}
)

var logSchema = record.Schemas{
	record.Field{Type: influx.Field_Type_Boolean, Name: RetryTag},
	record.Field{Type: influx.Field_Type_Int, Name: Time},
}

var failLogSchema = record.Schemas{
	record.Field{Type: influx.Field_Type_String, Name: FailLog},
	record.Field{Type: influx.Field_Type_String, Name: FailTag},
	record.Field{Type: influx.Field_Type_Boolean, Name: RetryTag},
	record.Field{Type: influx.Field_Type_Int, Name: Time},
}

var uploadSchema = record.Schemas{
	record.Field{Type: influx.Field_Type_String, Name: Content},
	record.Field{Type: influx.Field_Type_String, Name: Tags},
	record.Field{Type: influx.Field_Type_Int, Name: Time},
}

var (
	reservedFields = map[string]bool{
		FailTag:  true,
		FailLog:  true,
		RetryTag: true,
		Time:     true,
	}
	CompressType = map[string]bool{
		"":     true,
		"gzip": true,
	}
)

type LogWriteRequest struct {
	repository     string
	logStream      string
	failTag        string
	retry          bool
	timeMultiplier int64
	expiredTime    int64
	requestTime    int64
	minTime        int64
	maxTime        int64
	logTagString   *string
	dataType       LogDataType
	mapping        *JsonMapping
	printFailLog   *PrintFailLog
	logTags        [][]byte
	logTagsKey     map[string]bool
	logSchema      record.Schemas
	mstSchema      *meta2.CleanSchema
}

type JsonMapping struct {
	isConvertTime bool
	timeZone      int64
	timeFormat    string
	timestamp     string
	discardFields map[string]bool
}

func ReadAll(b []byte, r io.Reader) ([]byte, error) {
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}

func getRecordType(ty string) (LogDataType, error) {
	logDataType := JSON
	ty = strings.ToLower(ty)
	switch ty {
	case "json", "":
		logDataType = JSON
	case "jsonarray":
		logDataType = JSONArray
	default:
		return logDataType, errno.NewError(errno.InvalidLogDataType)
	}
	return logDataType, nil
}

func parseMapping(mapping string) (*JsonMapping, error) {
	jsonMapping := &JsonMapping{}
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
	timeKeyByte, err := timeKey.StringBytes()
	if err != nil {
		return nil, errno.NewError(errno.InvalidMappingTimeKeyType)
	}
	if string(timeKeyByte) == "" {
		return nil, errno.NewError(errno.InvalidMappingTimeKeyVal)
	}
	jsonMapping.timestamp = string(timeKeyByte)

	err = getTimeFormat(v, jsonMapping)
	if err != nil {
		return nil, err
	}

	jsonMapping.discardFields = make(map[string]bool)
	value := v.Get("discard_fields")
	if value == nil {
		return jsonMapping, nil
	}
	if value.Type() != fastjson.TypeArray {
		return nil, errno.NewError(errno.InvalidMappingDiscardKeyType)
	}
	discardKeys, _ := value.Array()
	for i := range discardKeys {
		b, err := discardKeys[i].StringBytes()
		if err != nil {
			return nil, errno.NewError(errno.InvalidMappingDiscardKeySubType)
		}
		jsonMapping.discardFields[string(b)] = true
	}
	if jsonMapping.discardFields[jsonMapping.timestamp] {
		return nil, errno.NewError(errno.InvalidMappingDiscardKeyVal)
	}

	return jsonMapping, nil
}

func getTimeFormat(v *fastjson.Value, jsonMapping *JsonMapping) error {
	timeFormat := v.Get("time_format")
	if timeFormat == nil {
		return nil
	}
	format, err := timeFormat.StringBytes()
	if err != nil || len(format) == 0 {
		return errno.NewError(errno.InvalidMappingTimeFormatVal)
	}
	jsonMapping.timeFormat = convertTimeFormat(string(format))
	jsonMapping.isConvertTime = true

	timeZone := v.Get("time_zone")
	if timeZone == nil {
		return errno.NewError(errno.InvalidMappingTimeZone)
	}
	zoneBy, err := timeZone.StringBytes()
	if len(zoneBy) < UTCPrefix+1 || err != nil {
		return errno.NewError(errno.InvalidMappingTimeZoneVal)
	}
	zoneStr := strings.TrimSpace(string(zoneBy[UTCPrefix:]))
	utc, err := strconv.Atoi(zoneStr)
	if err != nil {
		return errno.NewError(errno.InvalidMappingTimeZoneVal)
	}
	jsonMapping.timeZone = int64(utc)

	return nil
}

func convertTimeFormat(timeFormat string) string {
	switch timeFormat {
	case "yyyy-MM-dd HH:mm:ss":
		return "2006-01-02 15:04:05"
	case "yyyy-MM-dd HH:mm":
		return "2006-01-02 15:04"
	case "yyyy-MM-dd HH":
		return "2006-01-02 15"
	case "yy-MM-dd HH:mm:ss":
		return "06-01-02 15:04:05"
	case "yyyy-MM-ddTHH:mm:ssZ":
		return "2006-01-02T15:04:05Z"
	case "dd/MMM/yyyy:HH:mm:ss":
		return "02/Jan/2006:15:04:05"
	case "EEE MMM dd HH:mm:ss zzz yyyy":
		return "Mon Jan 02 15:04:05 2006"
	default:
		return timeFormat
	}
}

func getTimestamp(i interface{}, req *LogWriteRequest) (int64, error) {
	var unixTimestamp int64
	switch v := i.(type) {
	case *fastjson.Value:
		unixTimestamp = v.GetInt64(req.mapping.timestamp)
		if req.mapping.isConvertTime {
			timeByte := v.GetStringBytes(req.mapping.timestamp)
			timeUtc, err := time.Parse(req.mapping.timeFormat, util.Bytes2str(timeByte))
			if err != nil {
				return 0, err
			}
			unixTimestamp = timeUtc.UnixNano() - req.mapping.timeZone*time.Hour.Nanoseconds()
		} else {
			unixTimestamp = unixTimestamp * req.timeMultiplier
		}
	case map[string]interface{}:
		if req.mapping.isConvertTime {
			timeStr, _ := v[req.mapping.timestamp].(string)
			timeUtc, err := time.Parse(req.mapping.timeFormat, timeStr)
			if err != nil {
				return 0, errno.NewError(errno.ErrParseTimestamp)
			}
			unixTimestamp = timeUtc.UnixNano() - req.mapping.timeZone*time.Hour.Nanoseconds()
		} else {
			timeFloat, _ := v[req.mapping.timestamp].(float64)
			unixTimestamp = int64(timeFloat) * req.timeMultiplier
		}
	default:
		return 0, errno.NewError(errno.ErrParseTimestamp)
	}

	if unixTimestamp < MinUnixTimestampNs || unixTimestamp > MaxUnixTimestampNs {
		return 0, errno.NewError(errno.ErrParseTimestamp)
	}
	if unixTimestamp < req.expiredTime {
		req.failTag = ExpiredLogTag
		return 0, errno.NewError(errno.ErrParseTimestamp)
	}

	return unixTimestamp, nil
}

func Interface2str(i interface{}) string {
	switch v := i.(type) {
	case map[string]interface{}:
		str, err := sonic.MarshalString(i)
		if err != nil {
			break
		}
		return str
	case []byte:
		return util.Bytes2str(v)
	}

	return fmt.Sprintf("%s", i)
}

func parseLogTags(req *LogWriteRequest) (map[string][]byte, error) {
	if *req.logTagString == "" {
		return nil, nil
	}

	decodedStr, err := url.QueryUnescape(*req.logTagString)
	if err != nil {
		return nil, errno.NewError(errno.ErrLogTagsDecode, err)
	}
	*req.logTagString = decodedStr

	if len(*req.logTagString) > MaxLogTagsLen {
		return nil, errno.NewError(errno.InvalidLogTagsParmLength)
	}
	tagsMap := map[string][]byte{}
	p := parserPool.Get()
	defer parserPool.Put(p)
	v, err := p.Parse(*req.logTagString)
	if err != nil {
		return nil, errno.NewError(errno.ErrLogTagsJsonFormat, err)
	}
	ob, err := v.Object()
	if err != nil {
		return nil, errno.NewError(errno.ErrLogTagsJsonFormat, err)
	}
	var stopVisit bool
	ob.Visit(func(k []byte, v *fastjson.Value) {
		if stopVisit {
			return
		}
		key := string(k)
		if req.mapping.discardFields[key] {
		} else {
			err = checkTagFieldsType(key, req, tagsMap)
			if err != nil {
				stopVisit = true
				return
			}

			// Declare a slice to avoid references to the original data address
			var b []byte
			if v.Type() == fastjson.TypeString {
				stringBytes, _ := v.StringBytes()
				b = append(b, stringBytes...)
			} else {
				b = append(b, v.String()...)
			}
			if len(b) > 0 {
				tagsMap[key] = b
			}
		}
	})

	return tagsMap, err
}

func checkTagFieldsType(key string, req *LogWriteRequest, tagsMap map[string][]byte) error {
	var err error
	if reservedFields[key] || req.mapping.timestamp == key {
		return errno.NewError(errno.ErrReservedFieldDuplication, key)
	}
	if _, ok := tagsMap[key]; ok {
		return errno.NewError(errno.ErrTagFieldDuplication, key)
	}
	if existType, ok := req.mstSchema.GetTyp(key); ok {
		if existType != influx.Field_Type_String {
			return errno.NewError(errno.ErrTagFieldDataType, key, getInfluxDataType(existType))
		}
	}
	return err
}

func getInfluxDataType(srcType int32) string {
	switch srcType {
	case influx.Field_Type_String:
		return "string"
	case influx.Field_Type_Float:
		return "number"
	case influx.Field_Type_Boolean:
		return "bool"
	default:
		return "string"
	}
}

func getBufferSize(requestSize int) int {
	if requestSize > ScannerBufferSize {
		return requestSize
	} else {
		return ScannerBufferSize
	}
}

// Reserve the location for the log-tags field
func addLogTagsField(failRows *record.Record, logTagsMap map[string][]byte, req *LogWriteRequest) ([][]byte, map[string]bool) {
	logTagsSlice := make([][]byte, len(logTagsMap))
	logTagsKey := make(map[string]bool, len(logTagsMap))
	i := 0
	for key, value := range logTagsMap {
		failRows.Schema = append(failRows.Schema, record.Field{Type: influx.Field_Type_String, Name: key})
		failRows.ColVals = append(failRows.ColVals, record.ColVal{})

		logTagsSlice[i] = value
		logTagsKey[key] = true
		req.logSchema = append(req.logSchema, record.Field{Type: influx.Field_Type_String, Name: key})
		i++
	}

	return logTagsSlice, logTagsKey
}

func appendLogTags(rows *record.Record, req *LogWriteRequest) {
	if len(req.logTags) == 0 {
		return
	}
	lastTagLocation := logSchema.Len() + len(req.logTags)
	for i := logSchema.Len(); i < lastTagLocation; i++ {
		rows.ColVals[i].AppendByteSlice(req.logTags[i-logSchema.Len()])
	}
}

func appendFailRowsLogTags(failRows *record.Record, req *LogWriteRequest) {
	for i := failLogSchema.Len(); i < failRows.ColNums(); i++ {
		failRows.ColVals[i].AppendByteSlice(req.logTags[i-failLogSchema.Len()])
	}
}

type PrintFailLog struct {
	alreadyPrintParseError     bool
	alreadyPrintTimestampError bool
	alreadyPrintObjectError    bool
	alreadyPrintFieldError     bool
	alreadyPrintNoContentError bool
}

func getPrintFailLog() *PrintFailLog {
	return &PrintFailLog{}
}

type ParseField struct {
	fieldIndex int
	contentCnt int
	tagCnt     int
	rowCnt     int
	object     *fastjson.Object
	schemasNil []bool
	schemasMap map[string]int
}

func getParseField(tagNum int) *ParseField {
	return &ParseField{
		schemasNil: make([]bool, tagNum),
		fieldIndex: tagNum,
		tagCnt:     tagNum,
		schemasMap: make(map[string]int),
	}
}

func getMinMaxTime(req *LogWriteRequest, t int64) {
	if req.minTime == 0 && req.maxTime == 0 {
		req.minTime = t
		req.maxTime = t
		return
	}

	if t < req.minTime {
		req.minTime = t
	} else {
		if t > req.maxTime {
			req.maxTime = t
		}
	}
}

func appendRowAll(rows *record.Record, pf *ParseField, time int64) {
	rows.ColVals[1].AppendInteger(time)
	for i := pf.tagCnt; i < pf.fieldIndex; i++ {
		if pf.schemasNil[i] {
			pf.schemasNil[i] = false
		} else {
			appendNilToRecordColumn(rows, i, rows.Schema.Field(i).Type)
		}
	}
}

func appendNilToRecordColumn(rows *record.Record, colIndex, colType int) {
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
	appendValueOrNull(&rows.ColVals[0], content)
	appendValueOrNull(&rows.ColVals[1], tags)
	rows.ColVals[rows.ColNums()-1].AppendInteger(time)
}

func appendFailRow(failRows *record.Record, req *LogWriteRequest, val interface{}) {
	var err error
	content, ok := val.([]byte)
	if !ok {
		content, err = sonic.Marshal(val)
		if err != nil {
			content = util.Str2bytes(Interface2str(val))
		}
	}

	appendValueOrNull(&failRows.ColVals[0], content)
	failRows.ColVals[1].AppendStrings(req.failTag)
	failRows.ColVals[2].AppendBoolean(req.retry)
	failRows.ColVals[3].AppendInteger(req.requestTime)
	req.requestTime++
	req.failTag = FailLogTag
	appendFailRowsLogTags(failRows, req)
}

func appendBigLog(failRows *record.Record, req *LogWriteRequest, content []byte) {
	segmentNum := 1
	for i := 0; i < len(content); i += MaxContentLen {
		req.failTag = fmt.Sprintf("%s_%d", BigLogTag, segmentNum)
		if i+MaxContentLen > len(content) {
			appendFailRow(failRows, req, content[i:])
		} else {
			appendFailRow(failRows, req, content[i:i+MaxContentLen])
		}
		segmentNum++
	}
}

func parameterValidate(r *http.Request) error {
	if r.ContentLength > MaxRequestBodyLength {
		return errno.NewError(errno.InvalidRequestBodyLength)
	}
	if !CompressType[r.Header.Get("x-log-compresstype")] {
		return errno.NewError(errno.InvalidXLogCompressType)
	}
	return nil
}

func (h *Handler) getLogWriteRequest(r *http.Request) (*LogWriteRequest, error) {
	req := &LogWriteRequest{mstSchema: &meta2.CleanSchema{}}
	var err error
	repository := mux.Vars(r)[Repository]
	logStream := mux.Vars(r)[LogStream]
	if err = ValidateRepoAndLogStream(repository, logStream); err != nil {
		return nil, err
	}
	req.logStream = logStream
	req.repository = repository
	req.failTag = FailLogTag

	retry := r.FormValue("retry")
	if retry != "" {
		req.retry, err = strconv.ParseBool(retry)
		if err != nil {
			return nil, errno.NewError(errno.InvalidRetryPara)
		}
	}
	req.dataType, err = getRecordType(r.FormValue("type"))
	if err != nil {
		return nil, err
	}

	precision := r.URL.Query().Get("precision")
	switch precision {
	case "ns":
		req.timeMultiplier = 1
	case "us":
		req.timeMultiplier = 1e3
	case "ms":
		req.timeMultiplier = 1e6
	case "s":
		req.timeMultiplier = 1e9
	case "":
		req.timeMultiplier = 1e6
	default:
		return nil, errno.NewError(errno.InvalidPrecisionPara)
	}

	req.mapping = &JsonMapping{
		timestamp:     "time",
		discardFields: make(map[string]bool),
	}
	if mapping := r.FormValue("mapping"); mapping != "" {
		// mapping {"timestamp":"time"}
		req.mapping, err = parseMapping(mapping)
		if err != nil {
			return nil, err
		}
	}

	logTags := r.Header.Get("log-tags")
	req.logTagString = &logTags
	req.logSchema = append(req.logSchema, logSchema...)

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

func (h *Handler) parseJson(scanner *bufio.Scanner, req *LogWriteRequest, rows, failRows *record.Record) int64 {
	var totalLen int64
	p := parserPool.Get()
	defer parserPool.Put(p)
	pf := getParseField(len(req.logTags) + logSchema.Len())

	firstRow := true
	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) == 0 {
			continue
		}

		totalLen += int64(len(b)) + NewlineLen
		if len(b) > MaxContentLen {
			appendBigLog(failRows, req, scanner.Bytes())
			continue
		}

		v, err := p.ParseBytes(b)
		if err != nil {
			h.printFailLog(ParseError, req, scanner.Bytes(), err)
			appendFailRow(failRows, req, scanner.Bytes())
			continue
		}

		unixTimestamp, err := getTimestamp(v, req)
		if err != nil {
			if req.failTag != ExpiredLogTag {
				h.printFailLog(TimestampError, req, scanner.Bytes(), nil)
			}
			appendFailRow(failRows, req, scanner.Bytes())
			continue
		}

		pf.object, err = v.Object()
		if err != nil {
			h.printFailLog(ObjectError, req, scanner.Bytes(), err)
			appendFailRow(failRows, req, scanner.Bytes())
			continue
		}

		pf.contentCnt = 0
		if firstRow {
			err = parseFirstRowSchema(req, pf)
			if err != nil {
				pf = getParseField(len(req.logTags) + logSchema.Len())
				req.logSchema = req.logSchema[:len(req.logTags)+logSchema.Len()]
				appendFailRow(failRows, req, scanner.Bytes())
				continue
			}
			reuseRecordSchema(rows, req)
			firstRow = false
		}

		err = visitJsonField(req, rows, pf)
		if err != nil {
			h.printFailLog(ContentFieldError, req, scanner.Bytes(), err)
			clearFailRow(rows, pf.rowCnt+1)
			resetSchemaNil(pf.schemasNil)
			appendFailRow(failRows, req, scanner.Bytes())
			continue
		}

		if pf.contentCnt == 0 {
			h.printFailLog(NoContentError, req, scanner.Bytes(), err)
			appendFailRow(failRows, req, scanner.Bytes())
			continue
		}

		rows.ColVals[0].AppendBoolean(req.retry)
		appendLogTags(rows, req)
		appendRowAll(rows, pf, unixTimestamp)
		getMinMaxTime(req, unixTimestamp)
		pf.rowCnt++
	}
	swapTimeColumnToEnd(rows, failRows)

	return totalLen
}

func (h *Handler) parseJsonArray(body io.ReadCloser, req *LogWriteRequest, rows, failRows *record.Record) int64 {
	pf := getParseField(len(req.logTags) + logSchema.Len())
	rows.ReserveSchemaAndColVal(req.logSchema.Len())
	copy(rows.Schema, req.logSchema)

	var jsonArray []map[string]interface{}
	b := recordBufferPool.Get()
	b, err := ReadAll(b, body)
	defer recordBufferPool.Put(b)
	totalLen := int64(len(b))
	if err != nil {
		h.Logger.Error("read all json fail", zap.Error(err), zap.String(Repository, req.repository),
			zap.String(LogStream, req.logStream), zap.Any("read length", len(b)))
		appendFailRow(failRows, req, b)
		return totalLen
	}

	err = sonic.Unmarshal(b, &jsonArray)
	if err != nil {
		h.Logger.Error("sonic unmarshal json fail", zap.Error(err), zap.String(Repository, req.repository),
			zap.String(LogStream, req.logStream), zap.Any("json length", len(b)))
		appendFailRow(failRows, req, b)
		return totalLen
	}

	for _, jsonMap := range jsonArray {
		unixTimestamp, err := getTimestamp(jsonMap, req)
		if err != nil {
			if req.failTag != ExpiredLogTag {
				h.printFailLog(TimestampError, req, jsonMap, nil)
			}
			appendFailRow(failRows, req, jsonMap)
			continue
		}

		pf.contentCnt = 0
		err = visitJsonMap(jsonMap, req, rows, pf)
		if err != nil {
			h.printFailLog(ContentFieldError, req, jsonMap, err)
			clearFailRow(rows, pf.rowCnt+1)
			resetSchemaNil(pf.schemasNil)
			appendFailRow(failRows, req, jsonMap)
			continue
		}

		if pf.contentCnt == 0 {
			h.printFailLog(NoContentError, req, jsonMap, err)
			appendFailRow(failRows, req, jsonMap)
			continue
		}

		rows.ColVals[0].AppendBoolean(req.retry)
		appendLogTags(rows, req)
		appendRowAll(rows, pf, unixTimestamp)
		getMinMaxTime(req, unixTimestamp)
		pf.rowCnt++
	}
	swapTimeColumnToEnd(rows, failRows)

	return totalLen
}

func (h *Handler) printFailLog(failLogType FailLogType, req *LogWriteRequest, line interface{}, err error) {
	str := Interface2str(line)

	switch failLogType {
	case ParseError:
		if !req.printFailLog.alreadyPrintParseError {
			h.Logger.Error("Unmarshal json fail", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", str))
			req.printFailLog.alreadyPrintParseError = true
		}
	case TimestampError:
		if !req.printFailLog.alreadyPrintTimestampError {
			h.Logger.Error("the timestamp format is incorrect or missing", zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", str))
			req.printFailLog.alreadyPrintTimestampError = true
		}
	case ObjectError:
		if !req.printFailLog.alreadyPrintObjectError {
			h.Logger.Error("fastjson object get err", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", str))
			req.printFailLog.alreadyPrintObjectError = true
		}
	case ContentFieldError:
		if !req.printFailLog.alreadyPrintFieldError {
			h.Logger.Error("content field err", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", str))
			req.printFailLog.alreadyPrintFieldError = true
		}
	case NoContentError:
		if !req.printFailLog.alreadyPrintNoContentError {
			h.Logger.Error("content json empty", zap.Error(err), zap.String("repository", req.repository),
				zap.String("logstream", req.logStream), zap.String("line", str))
			req.printFailLog.alreadyPrintNoContentError = true
		}
	default:
		break
	}
}

func visitJsonField(req *LogWriteRequest, rows *record.Record, p *ParseField) error {
	var key string
	var err error
	var stopVisit bool
	p.object.Visit(func(k []byte, v *fastjson.Value) {
		if stopVisit {
			return
		}
		key = string(k)
		if req.mapping.discardFields[key] || key == req.mapping.timestamp {
		} else if reservedFields[key] {
			err = errno.NewError(errno.ErrReservedFieldDuplication, key)
			stopVisit = true
			return
		} else {
			col, ok := p.schemasMap[key]
			if !ok {
				if err = newFieldCheck(req, fastJsonTypeToRecordType(v.Type()), key); err != nil {
					stopVisit = true
					return
				}
				p.schemasMap[key] = p.fieldIndex
				p.schemasNil = append(p.schemasNil, true)
				rows.Schema = append(rows.Schema, record.Field{Type: fastJsonTypeToRecordType(v.Type()), Name: key})
				colsLen := len(rows.ColVals)
				if colsLen < cap(rows.ColVals) {
					rows.ColVals = rows.ColVals[:colsLen+1]
					rows.ColVals[colsLen].Init()
				} else {
					rows.ColVals = append(rows.ColVals, record.ColVal{})
				}
				fillNewField(rows, p)
				appendFastJsonValue(rows, p.fieldIndex, v)
				p.fieldIndex++
			} else {
				if err = existFieldCheck(rows, col, p.rowCnt, fastJsonTypeToRecordType(v.Type()), key); err != nil {
					stopVisit = true
					return
				}
				appendFastJsonValue(rows, col, v)
				p.schemasNil[col] = true
			}
			p.contentCnt++
		}
	})
	return err
}

func parseFirstRowSchema(req *LogWriteRequest, p *ParseField) error {
	var key string
	var err error
	var stopVisit bool
	// Traverse the first log field added to the logSchema
	p.object.Visit(func(k []byte, v *fastjson.Value) {
		if stopVisit {
			return
		}
		key = string(k)
		if req.mapping.discardFields[key] || key == req.mapping.timestamp {
		} else if reservedFields[key] {
			err = errno.NewError(errno.ErrReservedFieldDuplication, key)
			stopVisit = true
			return
		} else {
			_, ok := p.schemasMap[key]
			if !ok {
				if err = newFieldCheck(req, fastJsonTypeToRecordType(v.Type()), key); err != nil {
					stopVisit = true
					return
				}
				p.schemasMap[key] = p.fieldIndex
				p.schemasNil = append(p.schemasNil, true)
				req.logSchema = append(req.logSchema, record.Field{Type: fastJsonTypeToRecordType(v.Type()), Name: key})
				p.fieldIndex++
			} else {
				err = errno.NewError(errno.ErrFieldDuplication, key)
				stopVisit = true
				return
			}
		}
	})

	return err
}

func reuseRecordSchema(rows *record.Record, req *LogWriteRequest) {
	rows.Schema = rows.Schema[:cap(rows.Schema)]
	rows.ColVals = rows.ColVals[:cap(rows.ColVals)]
	recoverySchema := make(map[string]int, len(rows.Schema))
	if len(rows.Schema) == 0 {
		// If there is an empty record, add the schema and return it directly
		rows.ColVals = rows.ColVals[:0]
		rows.ReserveSchemaAndColVal(req.logSchema.Len())
		copy(rows.Schema, req.logSchema)
		return
	}

	// Verify the record's own Schema and ColVal
	if delta := len(rows.Schema) - len(rows.ColVals); delta > 0 {
		rows.ColVals = append(rows.ColVals, make([]record.ColVal, delta)...)
	}

	// Gets the fields in the record
	for i, field := range rows.Schema {
		if field.Name == "" {
			break
		}
		recoverySchema[field.Name] = i
	}

	// Keep the record field no less than the number of logSchema fields
	if delta := len(req.logSchema) - len(rows.Schema); delta > 0 {
		rows.Schema = append(rows.Schema, make([]record.Field, delta)...)
		rows.ColVals = append(rows.ColVals, make([]record.ColVal, delta)...)
	}

	var newFieldIndex []int
	// Adjust the order of the record field to keep it with logSchema
	for i, field := range req.logSchema {
		if idx, ok := recoverySchema[field.Name]; ok {
			exchangeField(idx, i, rows, recoverySchema)
		} else {
			newFieldIndex = append(newFieldIndex, i)
		}
	}
	// Record fields that do not exist are replaced with logSchema fields
	for _, idx := range newFieldIndex {
		rows.Schema[idx] = req.logSchema[idx]
	}

	// Leave valid fields in record
	rows.Schema = rows.Schema[:len(req.logSchema)]
	rows.ColVals = rows.ColVals[:len(req.logSchema)]
}

func exchangeField(src, dst int, rows *record.Record, rowsSchemaMap map[string]int) {
	if src != dst {
		rows.Schema[dst], rows.Schema[src] = rows.Schema[src], rows.Schema[dst]
		rows.ColVals[dst], rows.ColVals[src] = rows.ColVals[src], rows.ColVals[dst]
		// Update the location of an unfixed field
		rowsSchemaMap[rows.Schema[src].Name] = src
	}
}

func fillNewField(rows *record.Record, p *ParseField) {
	fieldType := rows.Schema.Field(p.fieldIndex).Type
	for i := 0; i < p.rowCnt; i++ {
		appendNilToRecordColumn(rows, p.fieldIndex, fieldType)
	}
}

func visitJsonMap(jsonMap map[string]interface{}, req *LogWriteRequest, rows *record.Record, p *ParseField) error {
	for key, v := range jsonMap {
		if req.mapping.discardFields[key] || key == req.mapping.timestamp {
		} else if reservedFields[key] {
			return errno.NewError(errno.ErrReservedFieldDuplication, key)
		} else {
			col, ok := p.schemasMap[key]
			if !ok {
				if err := newFieldCheck(req, getInfluxType(v), key); err != nil {
					return err
				}
				p.schemasMap[key] = p.fieldIndex
				p.schemasNil = append(p.schemasNil, true)
				rows.Schema = append(rows.Schema, record.Field{Type: getInfluxType(v), Name: key})
				colsLen := len(rows.ColVals)
				if colsLen < cap(rows.ColVals) {
					rows.ColVals = rows.ColVals[:colsLen+1]
					rows.ColVals[colsLen].Init()
				} else {
					rows.ColVals = append(rows.ColVals, record.ColVal{})
				}
				fillNewField(rows, p)
				appendInterfaceValue(rows, p.fieldIndex, v)
				p.fieldIndex++
			} else {
				if err := existFieldCheck(rows, col, p.rowCnt, getInfluxType(v), key); err != nil {
					return err
				}
				appendInterfaceValue(rows, col, v)
				p.schemasNil[col] = true
			}
			p.contentCnt++
		}
	}

	return nil
}

func resetSchemaNil(schemasNil []bool) {
	for i, ok := range schemasNil {
		if ok {
			schemasNil[i] = !ok
		}
	}
}

func clearFailRow(rows *record.Record, failRowNum int) {
	for i, col := range rows.ColVals {
		if col.Len < failRowNum {
			continue
		}
		switch rows.Schema.Field(i).Type {
		case influx.Field_Type_String:
			rows.ColVals[i].RemoveLastString()
		case influx.Field_Type_Float:
			rows.ColVals[i].RemoveLastFloat()
		case influx.Field_Type_Int:
			rows.ColVals[i].RemoveLastInteger()
		case influx.Field_Type_Boolean:
			rows.ColVals[i].RemoveLastBoolean()
		default:
			rows.ColVals[i].RemoveLastString()
		}
	}
}

func newFieldCheck(req *LogWriteRequest, dstType int, key string) error {
	if existType, ok := req.mstSchema.GetTyp(key); ok {
		if int(existType) != dstType {
			return errno.NewError(errno.ErrFieldDataType, key, getInfluxDataType(existType))
		}
	}
	if req.logTagsKey[key] {
		err := errno.NewError(errno.ErrTagFieldDuplication, key)
		return err
	}
	return nil
}

func existFieldCheck(rows *record.Record, col, rowNums, dstType int, key string) error {
	if rows.ColVals[col].Len > rowNums {
		err := errno.NewError(errno.ErrFieldDuplication, key)
		return err
	}
	if dstType != rows.Schema[col].Type {
		err := errno.NewError(errno.ErrFieldDataType, key, getInfluxDataType(int32(rows.Schema[col].Type)))
		return err
	}
	return nil
}

func swapTimeColumnToEnd(rows, failRows *record.Record) {
	if failRows.ColNums() > failLogSchema.Len() {
		failRows.ColVals[3], failRows.ColVals[failRows.ColNums()-1] = failRows.ColVals[failRows.ColNums()-1], failRows.ColVals[3]
		failRows.Schema[3], failRows.Schema[failRows.ColNums()-1] = failRows.Schema[failRows.ColNums()-1], failRows.Schema[3]
	}
	if rows.ColNums() > logSchema.Len() {
		rows.ColVals[1], rows.ColVals[rows.ColNums()-1] = rows.ColVals[rows.ColNums()-1], rows.ColVals[1]
		rows.Schema[1], rows.Schema[rows.ColNums()-1] = rows.Schema[rows.ColNums()-1], rows.Schema[1]
	}
}

func appendFastJsonValue(rows *record.Record, col int, v *fastjson.Value) {
	switch v.Type() {
	case fastjson.TypeString:
		sb, _ := v.StringBytes()
		appendValueOrNull(&rows.ColVals[col], sb)
	case fastjson.TypeNumber:
		f, _ := v.Float64()
		rows.ColVals[col].AppendFloat(f)
	case fastjson.TypeTrue, fastjson.TypeFalse:
		b, _ := v.Bool()
		rows.ColVals[col].AppendBoolean(b)
	case fastjson.TypeObject:
		fieldBuf := fieldBufferPool.Get()
		fieldBuf = v.MarshalTo(fieldBuf)
		appendValueOrNull(&rows.ColVals[col], fieldBuf)
		fieldBufferPool.Put(fieldBuf)
	default:
		appendValueOrNull(&rows.ColVals[col], v.String())
	}
}

func appendInterfaceValue(rows *record.Record, col int, i interface{}) {
	switch v := i.(type) {
	case string:
		appendValueOrNull(&rows.ColVals[col], v)
	case float64:
		rows.ColVals[col].AppendFloat(v)
	case bool:
		rows.ColVals[col].AppendBoolean(v)
	default:
		appendValueOrNull(&rows.ColVals[col], Interface2str(i))
	}
}

func appendValueOrNull(cv *record.ColVal, i interface{}) {
	switch v := i.(type) {
	case string:
		if len(v) > 0 {
			cv.AppendString(v)
			return
		}
	case []byte:
		if len(v) > 0 {
			cv.AppendByteSlice(v)
			return
		}
	}
	cv.AppendStringNull()
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

func getInfluxType(i interface{}) int {
	switch i.(type) {
	case string, map[string]interface{}:
		return influx.Field_Type_String
	case float64:
		return influx.Field_Type_Float
	case bool:
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
	req.mstSchema = logInfo.Measurements[req.logStream+MstSuffix].Schema
	logInfo.Measurements[req.logStream+MstSuffix].SchemaLock.RLock()
	logTagsMap, err := parseLogTags(req)
	logInfo.Measurements[req.logStream+MstSuffix].SchemaLock.RUnlock()
	if err != nil {
		h.Logger.Error("serveRecord parseLogTags fail", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	xLogCompressType := r.Header.Get("x-log-compresstype")
	var totalLen int64
	body := r.Body
	// Handle gzip decoding of the body
	if xLogCompressType == "gzip" {
		b, err := compression.GetGzipReader(r.Body)
		if err != nil {
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			h.Logger.Error("write error:Handle gzip decoding of the body err", zap.Error(errno.NewError(errno.HttpBadRequest)))
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
		defer compression.PutGzipReader(b)
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
	scanner.Buffer(scanBuf, getBufferSize(int(r.ContentLength)))
	scanner.Split(bufio.ScanLines)

	rows := record.LogStoreRecordPool.Get()
	failRows := record.GetRecordFromPool(record.LogStoreFailRecordPool, failLogSchema)
	req.logTags, req.logTagsKey = addLogTagsField(failRows, logTagsMap, req)

	req.requestTime = time.Now().UnixNano()
	if logInfo.Duration != 0 {
		req.expiredTime = req.requestTime - logInfo.Duration.Nanoseconds()
	}
	req.printFailLog = getPrintFailLog()
	if req.dataType == JSON {
		totalLen = h.parseJson(scanner, req, rows, failRows)
	} else {
		totalLen = h.parseJsonArray(body, req, rows, failRows)
	}

	if scanner.Err() != nil {
		h.Logger.Error("scanner internal error", zap.Error(scanner.Err()), zap.String("repo", req.repository),
			zap.String("logstream", req.logStream))
		h.httpErrorRsp(w, ErrorResponse("scanner internal error:"+scanner.Err().Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	if bodyLengthInt64 != totalLen && bodyLengthInt64 != 0 {
		h.Logger.Error("body-length  is not equal to scanner totalLen", zap.Int64("body-length", bodyLengthInt64),
			zap.Int64("scanner totalLen", totalLen), zap.String("x-log-compresstype", xLogCompressType))
		h.httpErrorRsp(w, ErrorResponse("body-length  is not equal to scanner totalLen", LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	bulk, failBulk := getBulkRecords(rows, failRows, req, totalLen, logInfo.ShardGroupDuration)
	if rows.RowNums() > 0 {
		err = h.RecordWriter.RetryWriteLogRecord(bulk)
		if err != nil {
			h.Logger.Error("serve records", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("write log error", LogReqErr), http.StatusBadRequest)
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	} else {
		record.LogStoreRecordPool.PutBigRecord(rows)
	}

	if failRows.RowNums() > 0 {
		err = h.RecordWriter.RetryWriteLogRecord(failBulk)
		if err != nil {
			h.Logger.Error("serve records", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("write fail log error", LogReqErr), http.StatusBadRequest)
			atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
			return
		}
	} else {
		record.LogStoreFailRecordPool.PutBigRecord(failRows)
	}

	addLogInsertStatistics(req.repository, req.logStream, totalLen)
}

func getBulkRecords(rows, failRows *record.Record, req *LogWriteRequest, totalLen int64, shardGroupDuration time.Duration) (*record.BulkRecords,
	*record.BulkRecords) {
	bulk := &record.BulkRecords{Repo: req.repository, Logstream: req.logStream, Rec: rows, MsgType: record.LogStoreRecord}
	failBulk := &record.BulkRecords{Repo: req.repository, Logstream: req.logStream, Rec: failRows, MsgType: record.LogStoreFailRecord}

	if rows.RowNums() > 0 {
		bulk.TotalLen = totalLen
		if req.minTime/shardGroupDuration.Nanoseconds() != req.maxTime/shardGroupDuration.Nanoseconds() {
			SortHelper := record.NewSortHelper()
			bulk.Rec = SortHelper.SortForColumnStore(rows, []record.PrimaryKey{{Key: "time", Type: influx.Field_Type_Int}}, false, 0)
			SortHelper.Release()
		}
	} else if failRows.RowNums() > 0 {
		failBulk.TotalLen = totalLen
	}

	return bulk, failBulk
}

func addLogInsertStatistics(repoName, logStreamName string, totalLen int64) {
	item := statistics.NewLogKeeperStatItem(repoName, logStreamName)
	statistics.NewLogKeeperStatistics().AddTotalWriteRequestCount(1)
	statistics.NewLogKeeperStatistics().AddTotalWriteRequestSize(totalLen)
	atomic.AddInt64(&item.WriteRequestCount, 1)
	atomic.AddInt64(&item.WriteRequestSize, totalLen)
	statistics.NewLogKeeperStatistics().Push(item)
}

func getLogTimestamp(t int64) int64 {
	if t%1000 != 0 {
		return t + 1
	}
	tmp := time.Now().UnixNano()
	if tmp > t+1 {
		return tmp
	}
	return t + 1
}

func (h *Handler) serveUpload(w http.ResponseWriter, r *http.Request, user meta2.User) {
	atomic.AddInt64(&statistics.HandlerStat.WriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.WriteRequestBytesIn, r.ContentLength)
	start := time.Now()
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		atomic.AddInt64(&statistics.HandlerStat.ActiveWriteRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.WriteRequestDuration, d)
	}(start)

	if !h.IsWriteNode() {
		h.Logger.Error("serveRecord checkNodeRole fail", zap.Error(ErrInvalidWriteNode))
		h.httpErrorRsp(w, ErrorResponse(ErrInvalidWriteNode.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	repository := mux.Vars(r)[Repository]
	logStream := mux.Vars(r)[LogStream]
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

	dur := logInfo.ShardGroupDuration.Nanoseconds()
	date := start.Unix()
	tagsStr := "date:" + strconv.FormatInt(date, 10) + tokenizer.TAGS_SPLITTER + "type:upload"
	tagsByte := []byte(tagsStr)

	timeStr := r.FormValue("timestamp")
	timeStart, _ := strconv.ParseInt(timeStr, 10, 64)
	rows := record.GetRecordFromPool(record.LogStoreRecordPool, uploadSchema)
	rowCount := 0
	baseTime := int64(0)
	curTime := start.UnixNano()
	if timeStart > 0 {
		baseTime = timeStart - curTime
	}
	t := curTime + baseTime
	groupId := int(t / dur)
	for scanner.Scan() {
		curTime = getLogTimestamp(curTime)
		t = curTime + baseTime
		groupIdTmp := int(t / dur)
		if groupIdTmp != groupId {
			groupId = groupIdTmp
			bulk := &record.BulkRecords{Repo: repository, Logstream: logStream, Rec: rows, MsgType: record.LogStoreRecord}
			err = h.RecordWriter.RetryWriteLogRecord(bulk)
			if err != nil {
				h.Logger.Error("serve upload", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse("upload log error", LogReqErr), http.StatusBadRequest)
				atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
				return
			}
			rows = record.GetRecordFromPool(record.LogStoreRecordPool, uploadSchema)
			rowCount = 0
		} else if rowCount > LogMax {
			bulk := &record.BulkRecords{Repo: repository, Logstream: logStream, Rec: rows, MsgType: record.LogStoreRecord}
			err = h.RecordWriter.RetryWriteLogRecord(bulk)
			if err != nil {
				h.Logger.Error("serve upload", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse("upload log error", LogReqErr), http.StatusBadRequest)
				atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
				return
			}
			rows = record.GetRecordFromPool(record.LogStoreRecordPool, uploadSchema)
			rowCount = 0
		}
		appendRow(rows, tagsByte, scanner.Bytes(), t)
		rowCount++
	}
	if rows.RowNums() > 0 {
		bulk := &record.BulkRecords{Repo: repository, Logstream: logStream, Rec: rows, MsgType: record.LogStoreRecord}
		err = h.RecordWriter.RetryWriteLogRecord(bulk)
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
	EmptyValue    = ""
	Reverse       = "reverse"
	TimeoutMs     = "timeout_ms"
	Explain       = "explain"
	IsTruncate    = "is_truncate"
	From          = "from"
	To            = "to"
	Scroll        = "scroll"
	ScrollId      = "scroll_id"
	Limit         = "limit"
	Highlight     = "highlight"
	JsonHighlight = "json"
	Sql           = "sql"
	Select        = "select"
	Query         = "query"
	Https         = "https"
	Http          = "Http"
)

// Err substring
const (
	ErrSyntax             = "syntax error"
	ErrParsingQuery       = "error parsing query"
	ErrNoFieldSelected    = "no field selected"
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

func (h *Handler) parseLogQuery(logP *logparser.Parser, info *measurementInfo, q *influxql.Query) (*influxql.Query, error, int) {
	if logP == nil {
		return nil, nil, 0
	}

	logParser := logparser.NewYyParser(logP.GetScanner())
	logParser.ParseTokens()
	logCond, err := logParser.GetQuery()
	if err != nil {
		h.Logger.Error("query error! parsing query value", zap.Error(err), zap.String("db", info.database))
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
		errMsg := fmt.Sprintf("%s: %v, %v", errMsgMark, zap.Error(err), zap.String("db", info.database))
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
			mstStmt.Database = info.database
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

func (h *Handler) getPplQuery(info *measurementInfo, pr io.Reader, sqlQuery *influxql.Query) (*influxql.Query, error, int) {
	logP := logparser.NewParser(pr)
	defer logP.Release()

	pplQuery, err, status := h.parseLogQuery(logP, info, sqlQuery)
	if err != nil {
		return nil, err, status
	}

	return pplQuery, nil, http.StatusOK
}

// rewriteStatementForLogStore is used to construct time filter, generate adaptive time buckets, and generate dims.
func (h *Handler) rewriteStatementForLogStore(selectStmt *influxql.SelectStatement, param *QueryParam, info *measurementInfo) error {
	var isIncQuery bool
	selectStmt.RewriteUnnestSource()
	selectStmt.Sources = influxql.Sources{&influxql.Measurement{Name: info.name, Database: info.database, RetentionPolicy: info.retentionPolicy}}
	if param != nil {
		isIncQuery = param.IncQuery
		selectStmt.Limit = param.Limit
		if len(selectStmt.SortFields) == 0 {
			if param.Limit > 0 {
				selectStmt.SortFields = []*influxql.SortField{{Name: "time", Ascending: param.Ascending}, {Name: influxql.ShardIDField, Ascending: param.Ascending}, {Name: record.SeqIDField, Ascending: param.Ascending}}
			} else {
				selectStmt.SortFields = []*influxql.SortField{{Name: "time", Ascending: param.Ascending}}
			}
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
			if param.isHistogram {
				groupByTimeInterval := logstore.GetAdaptiveTimeBucket(time.Unix(0, param.TimeRange.start), time.Unix(0, param.TimeRange.end), param.Ascending)
				selectStmt.SetTimeInterval(groupByTimeInterval)
				param.GroupBytInterval = groupByTimeInterval
			}
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
		ppl := removeMulAndSpace(query)
		return ppl, ""
	}
	if strings.HasPrefix(removePreSpace(strings.ToLower(query[lastPipeIndex+1:])), "select ") {
		sql := strings.TrimSpace(query[lastPipeIndex+1:])
		ppl := strings.TrimSpace(query[:lastPipeIndex])
		ppl = removeMulAndSpace(ppl)
		return ppl, sql
	}
	ppl := removeMulAndSpace(query)
	return ppl, ""
}

// reutrn pplQuery for highlight
func (h *Handler) getSqlAndPplQuery(r *http.Request, param *QueryParam, user meta2.User, info *measurementInfo) (*influxql.Query, *influxql.Query, error, int) {
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
		pplQuery, err, status = h.getPplQuery(info, strings.NewReader(ppl), sqlQuery)
		if err != nil {
			return nil, nil, err, status
		}
	}

	// rewrite for logstore
	if selectStmt, ok := sqlQuery.Statements[0].(*influxql.SelectStatement); ok {
		if err = h.rewriteStatementForLogStore(selectStmt, param, info); err != nil {
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

func (h *Handler) serveLogQuery(w http.ResponseWriter, r *http.Request, param *QueryParam, user meta2.User, info *measurementInfo) (*Response, *influxql.Query, *influxql.Query, int, error) {
	atomic.AddInt64(&statistics.HandlerStat.QueryRequests, 1)
	atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, 1)
	start := time.Now()
	defer func() {
		atomic.AddInt64(&statistics.HandlerStat.ActiveQueryRequests, -1)
		atomic.AddInt64(&statistics.HandlerStat.QueryRequestDuration, time.Since(start).Nanoseconds())
	}()

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	if syscontrol.DisableReads {
		h.Logger.Error("read is forbidden!", zap.Bool("DisableReads", syscontrol.DisableReads))
		return nil, nil, nil, http.StatusForbidden, fmt.Errorf("disable read")
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)
	// new reader for sql statement
	q, pplQuery, err, status := h.getSqlAndPplQuery(r, param, user, info)
	if err != nil {
		return nil, nil, nil, status, err
	}
	// If an error occurs during the query, an error must be returned to avoid error shielding.
	q.SetReturnErr(true)
	epoch := strings.TrimSpace(r.FormValue("epoch"))
	var qDuration *statistics.SQLSlowQueryStatistics
	if !isInternalDatabase(info.database) {
		qDuration = statistics.NewSqlSlowQueryStatistics(info.database)
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
	err = h.checkAuthorization(user, q, info.database)
	if err != nil {
		return nil, nil, nil, http.StatusForbidden, fmt.Errorf("error authorizing query: " + err.Error())
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		return nil, nil, nil, http.StatusBadRequest, err
	}
	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"
	opts := *query.NewExecutionOptions(info.database, r.FormValue("rp"), nodeID, chunkSize, innerChunkSize, false, r.Method == "GET", true,
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
		return nil, nil, nil, http.StatusNoContent, err
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
			return nil, nil, nil, http.StatusNoContent, r.Err
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
			return &resp, pplQuery, q, http.StatusOK, nil
		}
	}
	if !chunked {
		n, _ := rw.WriteResponse(resp)
		atomic.AddInt64(&statistics.HandlerStat.QueryRequestBytesTransmitted, int64(n))
	}

	return nil, nil, nil, http.StatusOK, nil
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
	queryLogRequest.Pretty = r.FormValue("pretty") == "true"

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
		b, err := json2.Marshal(res)
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

	if strings.Contains(err.Error(), ErrSyntax) || strings.Contains(err.Error(), ErrParsingQuery) ||
		strings.Contains(err.Error(), ErrNoFieldSelected) {
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
	b, err := json2.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	h.writeHeader(w, http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	w.Write(b)
}

func (h *Handler) setRecord(rec map[string]interface{}, field string, value interface{}, truncate bool) (isAdd bool) {
	isNeed := h.isNeedReply(field, value)
	if !isNeed {
		return false
	}

	if v, ok := value.(float64); ok {
		value = json.Number(strconv.FormatFloat(v, 'f', -1, 64))
	}
	if !truncate {
		rec[field] = value
		return true
	}
	strVal, ok := value.(string)
	if !ok {
		rec[field] = value
		return true
	}
	if len(strVal) > MaxRowLen {
		rec[IsOverflow] = true
		rec[field] = strVal[:MaxRowLen]
	} else {
		rec[field] = value
	}

	return true
}

func (h *Handler) isNeedReply(key string, i interface{}) (isNeed bool) {
	if i == nil {
		return false
	}

	if key == RetryTag {
		isRetry, _ := i.(bool)
		if !isRetry {
			return false
		}
	}
	return true
}

func (h *Handler) getQueryLogResult(resp *Response, logCond *influxql.Query, para *QueryParam, keysMap map[string]bool) (int64, []map[string]interface{}, error) {
	var count int64
	var logs []map[string]interface{}
	var unnest *influxql.Unnest

	// map[highlightWord]map[finiteField][]int{operator...}
	highlightWords := map[string]map[string][]int{}
	if logCond != nil {
		unnest = logCond.Statements[0].(*influxql.LogPipeStatement).Unnest
		expr := &(logCond.Statements[0].(*influxql.LogPipeStatement).Cond)
		highlightWords = getHighlightWords(expr, highlightWords)
	}
	for i := range resp.Results {
		for _, s := range resp.Results[i].Series {
			for j := range s.Values {
				rec := map[string]interface{}{}
				content := map[string]interface{}{}
				rec[IsOverflow] = false

				var fieldScopes []marshalFieldScope
				var jsonHighlight []*JsonHighlightFragment
				var currSeqID int64
				var currShardID int64
				var currT int64
				for id, c := range s.Columns {
					if c == record.SeqIDField {
						if s.Values[j][id] != nil {
							currSeqID = s.Values[j][id].(int64)
						}
						continue
					}
					if c == influxql.ShardIDField {
						if s.Values[j][id] != nil {
							currShardID = s.Values[j][id].(int64)
						}
						continue
					}
					switch c {
					case Time:
						v, _ := s.Values[j][id].(time.Time)
						rec[Timestamp] = v.UnixMilli()
						currT = v.UnixNano()
					default:
						if unnest != nil && unnest.IsUnnestField(c) {
							h.setRecord(rec, c, s.Values[j][id], para.Truncate)
							keysMap[c] = true
						} else {
							isAdd := h.setRecord(content, c, s.Values[j][id], para.Truncate)
							if !isAdd {
								continue
							}

							if para.Highlight && logCond != nil {
								fieldScopes = h.appendFieldScopes(fieldScopes, c, content[c])
							}

							keysMap[c] = true
							if para.Highlight && para.Pretty {
								jsonHighlight = append(jsonHighlight, getJsonHighlight(c, content[c], highlightWords))
							}
						}
					}
				}

				rec[Cursor] = base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(currT, 10) + "|" + strconv.FormatInt(currShardID, 10) + "|" + strconv.FormatInt(currSeqID, 10) + "^^"))
				rec[Content] = content
				count += 1
				if para.Highlight {
					rec[Highlight] = h.getHighlightFragments(rec, highlightWords, fieldScopes)
					if para.Pretty {
						rec[JsonHighlight] = map[string]interface{}{"segments": jsonHighlight}
					}
				}

				logs = append(logs, rec)
			}
		}
	}
	return count, logs, nil
}

type marshalFieldScope struct {
	key   string
	start int
	end   int
}

func (h *Handler) appendFieldScopes(fieldScopes []marshalFieldScope, k string, i interface{}) []marshalFieldScope {
	keyLen := len(k) + MarshalFieldPunctuation
	if len(fieldScopes) == 0 {
		fieldScopes = append(fieldScopes, marshalFieldScope{k, keyLen, keyLen + h.getMarshalFieldLen(i)})
	} else {
		lastFs := fieldScopes[len(fieldScopes)-1]
		fieldScopes = append(fieldScopes, marshalFieldScope{k, keyLen + lastFs.end, keyLen + lastFs.end + h.getMarshalFieldLen(i)})
	}
	return fieldScopes
}

func (h *Handler) getMarshalFieldLen(i interface{}) int {
	b, err := json2.Marshal(i)
	if err != nil {
		h.Logger.Error("highlight marshal field len error", zap.Any("value", i))
	}
	return len(b)
}

func (h *Handler) serveContextQueryLog(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := mux.Vars(r)[Repository]
	logStream := mux.Vars(r)[LogStream]
	if err := h.ValidateAndCheckLogStreamExists(repository, logStream); err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	t := time.Now()
	queryLogRequest, err := getQueryLogContextRequest(r)
	if err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	para := NewQueryPara(queryLogRequest)

	if err := para.parseScrollID(); err != nil {
		h.Logger.Error("query context log scan request Scroll_id error! ", zap.Error(err), zap.Any("r", para.Scroll_id))
		h.httpErrorRsp(w, ErrorResponse("cursor value is illegal", LogReqErr), http.StatusBadRequest)
		return
	}
	para.QueryID = para.Scroll_id
	sgsAll, err := h.MetaClient.GetShardGroupByTimeRange(repository, logStream, time.Unix(0, para.TimeRange.start), time.Unix(0, para.TimeRange.end))
	if err != nil {
		h.serveQueryLogWhenErr(w, err, t, repository, logStream)
		return
	}
	var count int64
	var logs []map[string]interface{}
	keysMap := map[string]bool{}
	sgs, err := h.MetaClient.GetShardGroupByTimeRange(repository, logStream, time.Unix(0, para.TimeRange.start), time.Unix(0, para.TimeRange.end))
	tm := time.Now()
	isFinish := true
	sgStartTime := int64(0)
	for j := 0; j < len(sgs); j++ {
		i := j
		if queryLogRequest.Reverse {
			i = len(sgs) - 1 - j
		}
		currTm := time.Now()
		currPara := para.deepCopy()
		if sgs[i].StartTime.UnixNano() > currPara.TimeRange.start {
			currPara.TimeRange.start = sgs[i].StartTime.UnixNano()
		}
		if sgs[i].EndTime.UnixNano() < currPara.TimeRange.end {
			currPara.TimeRange.end = sgs[i].EndTime.UnixNano()
		}
		resp, logCond, _, _, err := h.serveLogQuery(w, r, currPara, user, &measurementInfo{
			name:            logStream,
			database:        repository,
			retentionPolicy: logStream,
		})
		if err != nil {
			if QuerySkippingError(err.Error()) {
				continue
			}
			h.serveQueryLogWhenErr(w, err, t, repository, logStream)
			return
		}
		if para.Explain {
			h.getQueryLogExplainResult(resp, repository, logStream, w, t)
			return
		}
		currCount, currLog, err := h.getQueryLogResult(resp, logCond, para, keysMap)
		if err != nil {
			h.Logger.Error("query err ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
		for k, _ := range currLog {
			if content, ok := currLog[k]["content"]; ok {
				contentJson, err := json2.Marshal(content)
				if err != nil {
					h.Logger.Error("query log marshal res fail! ", zap.Error(err), zap.Any("r", r))
					h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
					return
				}
				currLog[k]["content"] = string(contentJson)
			}
		}
		count += currCount
		logs = append(logs, currLog...)
		if count >= int64(para.Limit) {
			logs = logs[0:para.Limit]
			count = int64(para.Limit)
			break
		}
		if int(time.Since(tm).Milliseconds()+time.Since(currTm).Milliseconds()) >= queryLogRequest.Timeout && j != len(sgs)-1 {
			isFinish = false
			if queryLogRequest.Reverse {
				sgStartTime = sgs[i].StartTime.UnixNano()
			} else {
				sgStartTime = sgs[i].EndTime.UnixNano()
			}
			break
		}
	}
	var scrollIDString string
	var progress string
	var completeProgress float64
	var cursorTime int64
	// read finished
	if isFinish && (len(logs) == 0 || len(logs) < para.Limit) {
		scrollIDString = ""
		progress = "Complete"
		completeProgress = 1
		if len(sgs) == 0 {
			if !para.Ascending {
				cursorTime = para.TimeRange.end / 1e6
			} else {
				cursorTime = para.TimeRange.start / 1e6
			}
		} else {
			if !para.Ascending {
				cursorTime = sgs[0].StartTime.UnixNano() / 1e6
			} else {
				cursorTime = sgs[len(sgs)-1].EndTime.UnixNano() / 1e6
			}
		}
	} else {
		startTime := sgsAll[0].StartTime.UnixNano()
		endTime := sgsAll[len(sgs)-1].EndTime.UnixNano()
		if !isFinish {
			scrollIDString = "^" + strconv.Itoa(int(sgStartTime)) + "^"
			scrollIDString = base64.StdEncoding.EncodeToString([]byte(scrollIDString))
			cursorTime = sgStartTime / int64(1e6)
			if !para.Ascending {
				completeProgress = float64(endTime-sgStartTime) / float64(endTime-startTime)
			} else {
				completeProgress = float64(sgStartTime-startTime) / float64(endTime-startTime)
			}
		} else { // read limit num
			scrollIDString = logs[len(logs)-1]["cursor"].(string)
			cursorTime, err = GetMSByScrollID(logs[len(logs)-1]["cursor"].(string))
			if err != nil {
				h.Logger.Error("query log marshal res fail! ", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
				return
			}
			if !para.Ascending {
				completeProgress = float64(sgsAll[len(sgs)-1].EndTime.UnixNano()-cursorTime*int64(1e6)) / float64(sgsAll[len(sgs)-1].EndTime.UnixNano()-sgsAll[0].StartTime.UnixNano())
			} else {
				completeProgress = float64(cursorTime*int64(1e6)-sgsAll[0].StartTime.UnixNano()) / float64(sgsAll[len(sgs)-1].EndTime.UnixNano()-sgsAll[0].StartTime.UnixNano())
			}
		}
		progress = "InComplete"
	}

	res := QueryLogResponse{Success: true, Code: "200", Message: "", Request_id: uuid.TimeUUID().String(),
		Count: count, Progress: progress, Logs: logs, Keys: getKeys(keysMap), Took_ms: time.Since(t).Milliseconds(), Scroll_id: scrollIDString, Complete_progress: completeProgress, Cursor_time: cursorTime}

	b, err := json2.Marshal(res)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.Header().Set("X-Content-Length", strconv.Itoa(len(b)))
	h.writeHeader(w, http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	w.Write(b)
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
		return 0, fmt.Errorf("wrong scroll_id")
	}
	if arrFirst[0] == "" {
		return 0, nil
	}
	arr := strings.Split(arrFirst[0], "|")
	if len(arr) == 3 {
		time, err := strconv.ParseInt(arr[0], 10, 64)
		if err != nil {
			return 0, err
		}
		return time / 1e6, nil
	} else {
		return 0, fmt.Errorf("Get wrong scroll_id")
	}
}

type Histograms struct {
	From  int64 `json:"from"`
	To    int64 `json:"to"`
	Count int64 `json:"count"`
}

func GenZeroHistogram(opt query.ProcessorOptions, start, end int64, ascending bool) []Histograms {
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

func (h *Handler) serveRecallData(w http.ResponseWriter, r *http.Request, user meta2.User) {
	logStream := mux.Vars(r)[LogStream]
	repository := mux.Vars(r)[Repository]
	if err := ValidateRepoAndLogStream(repository, logStream); err != nil {
		h.Logger.Error("serveRecallData", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}

	h.Logger.Info("serveRecallData", zap.String("logStream", logStream), zap.String("repository", repository))
	err := h.MetaClient.RevertRetentionPolicyDelete(repository, logStream)
	if err != nil {
		h.Logger.Error("serveRecallData, UpdateLogStream", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusInternalServerError)
		return
	}
	h.writeHeader(w, http.StatusOK)
}
