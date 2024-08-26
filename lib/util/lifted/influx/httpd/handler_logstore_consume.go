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
	"encoding/base64"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gorilla/mux"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/coordinator"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/consume"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	MaxConsumeChanSize = 24
	BlockSize          = 4
)

func (h *Handler) serveQueryLogByCursor(w http.ResponseWriter, r *http.Request, user meta2.User) {
}

func (h *Handler) getRequestInfo(r *http.Request) (*consume.ConsumeInfo, error) {
	repository := mux.Vars(r)[Repository]
	logStream := mux.Vars(r)[LogStream]
	if err := h.ValidateAndCheckLogStreamExists(repository, logStream); err != nil {
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return nil, err
	}
	db, err := h.MetaClient.Database(repository)
	if err != nil {
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return nil, err
	}
	queryLogRequest, err := consume.GetQueryConsumeLogsRequest(r)
	if err != nil {
		return nil, err
	}
	mst, err := h.MetaClient.Measurement(repository, logStream, logStream)
	if err != nil {
		return nil, err
	}
	rp, err := h.MetaClient.DBPtView(repository)
	if err != nil {
		return nil, err
	}
	return &consume.ConsumeInfo{
		Pt:              rp,
		MstName:         mst.Name,
		Option:          db.Options,
		QueryLogRequest: queryLogRequest,
		Repository:      repository,
		LogStream:       logStream,
		Mst:             mst,
	}, nil
}

func (h *Handler) getConsumeInfo(w http.ResponseWriter, r *http.Request, user meta2.User, t time.Time, info *measurementInfo) (*consume.ConsumeInfo, *immutable.FilterOptions, error) {
	consumeInfo, err := h.getRequestInfo(r)
	if err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return nil, nil, err
	}
	cursor, err := consume.GetConsumeCursor(consumeInfo.QueryLogRequest.FromCursor)
	if err != nil || cursor == nil {
		h.httpErrorRsp(w, ErrorResponse("wrong from_cursor", LogReqErr), http.StatusBadRequest)
		return nil, nil, err
	}
	endCursor, err := consume.GetConsumeCursor(consumeInfo.GetQueryLogRequest().EndCursor)
	if err != nil {
		h.httpErrorRsp(w, ErrorResponse("wrong end_cursor", LogReqErr), http.StatusBadRequest)
		return nil, nil, err
	}

	if endCursor != nil && len(endCursor.Tasks) != len(cursor.Tasks) {
		h.httpErrorRsp(w, ErrorResponse("wrong from_cursor or end_cursor", LogReqErr), http.StatusBadRequest)
		return nil, nil, err
	}

	para := NewQueryPara(consumeInfo.GetQueryLogRequest())
	consumeInfo.FromCursor = cursor
	consumeInfo.EndCursor = endCursor
	sqlQuery, pplQuery, err, _ := h.getSqlAndPplQuery(r, para, user, info)
	if err != nil {
		h.getErrConsumeResponse(consumeInfo, t, w, err)
		return nil, nil, err
	}
	if pplQuery != nil {
		consumeInfo.Cond = pplQuery.Statements[0].(*influxql.LogPipeStatement).Cond
	}
	var filterOpt *immutable.FilterOptions
	if consumeInfo.Cond != nil {
		mapShard, err := h.QueryExecutor.StatementExecutor.(*coordinator.StatementExecutor).ShardMapper.MapShards(sqlQuery.Statements[0].(*influxql.SelectStatement).Sources, influxql.TimeRange{}, query.SelectOptions{}, consumeInfo.Cond)
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return nil, nil, err
		}
		mapper := query.FieldMapper{FieldMapper: mapShard}
		statement, err := sqlQuery.Statements[0].(*influxql.SelectStatement).RewriteFields(mapper, true, false)
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return nil, nil, err
		}

		opt, err := query.NewProcessorOptionsStmt(statement, query.SelectOptions{})
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return nil, nil, err
		}
		opt.Sources = statement.Sources
		consumeInfo.ProcessOption = &opt
		querySchema := executor.NewQuerySchemaWithSources(statement.Fields, statement.Sources, statement.ColumnNames(), &opt, statement.SortFields)
		ctx, err := engine.GetCtx(querySchema)
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return nil, nil, err
		}
		ctx.SetSchema(consumeInfo.GetMst().GetRecordSchema())
		filterOpt = immutable.NewFilterOpts(consumeInfo.Cond, ctx.GetFilterOption(), &influx.PointTags{}, nil)
		ctx.GetFilterOption().CondFunctions, err = binaryfilterfunc.NewCondition(nil, consumeInfo.Cond, consumeInfo.GetMst().GetRecordSchema(), &opt)
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return nil, nil, err
		}
		consumeInfo.FilterBitmap = bitmap.NewFilterBitmap(ctx.GetFilterOption().CondFunctions.NumFilter())
	}
	var maxConsumeTime int64
	if consumeInfo.GetEndCursor() != nil && len(consumeInfo.GetEndCursor().Tasks) != 0 {
		maxConsumeTime = consumeInfo.GetEndCursor().Time
	} else {
		maxConsumeTime = math.MaxInt64
	}
	consumeInfo.Tr = util.TimeRange{Min: consumeInfo.GetFromCursor().Time, Max: maxConsumeTime}
	sgs, err := h.MetaClient.GetShardGroupByTimeRange(consumeInfo.GetRepository(), consumeInfo.GetLogstream(), time.Unix(0, consumeInfo.GetFromCursor().Time), time.Unix(0, maxConsumeTime))
	if err != nil {
		h.getErrConsumeResponse(consumeInfo, t, w, err)
		return nil, nil, err
	}
	consumeInfo.Sgs = sgs
	return consumeInfo, filterOpt, nil
}

func (h *Handler) serveConsumeLogs(w http.ResponseWriter, r *http.Request, user meta2.User) {
	t := time.Now()
	repository := mux.Vars(r)[Repository]
	logStream := mux.Vars(r)[LogStream]
	consumeInfo, filterOpt, err := h.getConsumeInfo(w, r, user, t, &measurementInfo{database: repository, name: logStream, retentionPolicy: logStream})
	if err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		return
	}
	if consumeInfo == nil {
		return
	}

	defer func() {
		if e := recover(); e != nil {
			errInfo := fmt.Sprintf("consume request error! repo: %s, logstream: %s, info:%s", consumeInfo.GetRepository(), consumeInfo.GetLogstream(), consumeInfo.GetQueryLogRequest().FromCursor)
			h.Logger.Error(errInfo, zap.Error(errno.NewError(errno.RecoverPanic, e)))
			h.httpErrorRsp(w, ErrorResponse("consume request error! ", LogReqErr), http.StatusBadRequest)
			return
		}
	}()

	if len(consumeInfo.GetFromCursor().Tasks) == 0 {
		err = h.updateCursorByPtNum(consumeInfo.GetFromCursor(), consumeInfo.GetEndCursor(), consumeInfo, consumeInfo.GetMstName(), consumeInfo.GetSgs(), consumeInfo.GetPt())
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return
		}
		if len(consumeInfo.GetFromCursor().Tasks) == 0 {
			h.getEmptyCursor(consumeInfo, w, t)
			return
		}
	}

	taskID, isNeedConsume, err := h.getConsumeCursor(w, consumeInfo, t)
	if err != nil {
		errInfo := fmt.Sprintf("consume request error! repo: %s, logstream: %s, info:%s", consumeInfo.GetRepository(), consumeInfo.GetLogstream(), consumeInfo.GetQueryLogRequest().FromCursor)
		h.Logger.Error(errInfo, zap.Error(errno.NewError(errno.RecoverPanic, err)))
		return
	}
	if !isNeedConsume {
		taskID = rand.Intn(len(consumeInfo.GetFromCursor().Tasks))
	}

	isLastShard := false
	if consumeInfo.GetEndCursor() != nil && len(consumeInfo.GetEndCursor().Tasks) != 0 && consumeInfo.GetEndCursor().Tasks[taskID].CurrTask.SgID == consumeInfo.GetEndCursor().Tasks[taskID].CurrTask.SgID {
		isLastShard = true
	}

	isEnd := false
	var size, metaIndexId int64
	var blockId uint64
	var data []map[string]interface{}
	currTask := consumeInfo.GetFromCursor().Tasks[taskID].CurrTask
	if currTask.RemotePath == "" {
		data = nil
		isEnd = true
	} else {
		localPath := path.Join(h.SQLConfig.Data.DataDir, currTask.RemotePath)
		obsPath := sparseindex.NewOBSFilterPath(localPath, currTask.RemotePath, consumeInfo.GetOption())
		reader, err := immutable.NewSegmentSequenceReader(obsPath, taskID, uint64(math.Ceil(float64(consumeInfo.GetQueryLogRequest().Count)/float64(BlockSize))), consumeInfo, consumeInfo.GetMst().GetRecordSchema(), filterOpt)
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return
		}
		data, isEnd, size, blockId, metaIndexId, err = reader.ConsumeDateByShard()
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return
		}
		consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.MetaIndexId = int(metaIndexId)
		consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.BlockID = blockId
		if len(data) != 0 {
			consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.Timestamp = data[len(data)-1]["time"].(int64)
		}
		reader.Close()
	}

	var isFinish, isUpdateCursor bool
	if isEnd && isLastShard {
		isFinish = true
	} else if isEnd {
		isUpdateCursor, err = h.updateCursorTask(consumeInfo.GetFromCursor().Tasks[taskID], consumeInfo)
		if err != nil {
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return
		}
	}

	if consumeInfo.GetEndCursor() == nil || len(consumeInfo.GetEndCursor().Tasks) == 0 {
		err = h.updateCursorByPtNum(consumeInfo.GetFromCursor(), consumeInfo.GetEndCursor(), consumeInfo, consumeInfo.GetMstName(), consumeInfo.GetSgs(), consumeInfo.GetPt())
		if err != nil {
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
	}
	if isFinish {
		consumeInfo.GetFromCursor().Tasks = append(consumeInfo.GetFromCursor().Tasks[:taskID], consumeInfo.GetFromCursor().Tasks[taskID+1:]...)
		if consumeInfo.GetEndCursor() != nil && len(consumeInfo.GetEndCursor().Tasks) != 0 {
			consumeInfo.GetEndCursor().Tasks = append(consumeInfo.GetEndCursor().Tasks[:taskID], consumeInfo.GetEndCursor().Tasks[taskID+1:]...)
		}
	}

	isComplete := consumeInfo.GetEndCursor() != nil && len(consumeInfo.GetEndCursor().Tasks) == 0 && len(consumeInfo.FromCursor.Tasks) == 0

	// todo: preTaskNeedConsume
	if len(data) != 0 && !isFinish && !isUpdateCursor {
		consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.Timestamp = data[len(data)-1]["time"].(int64)
	}

	cursorEncode, err := consumeInfo.GetFromCursor().EncodeToByte()
	if err != nil {
		h.Logger.Error("cursor encode is error ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse("cursor encode is error ", LogReqErr), http.StatusBadRequest)
		return
	}
	var endCursorEncode []byte
	if consumeInfo.GetEndCursor() != nil {
		endCursorEncode, err = consumeInfo.GetEndCursor().EncodeToByte()
		if err != nil {
			h.Logger.Error("cursor encode is error ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("cursor encode is error ", LogReqErr), http.StatusBadRequest)
			return
		}
	}

	if isComplete {
		cursorEncode = []byte{}
		endCursorEncode = []byte{}
	}
	maxLogTime := int64(0)
	if len(data) != 0 {
		maxLogTime = data[len(data)-1]["time"].(int64) / int64(1e6)
	}
	resp := consume.QueryConsumeLogsResponse{
		TotalCount: len(data),
		TotalSize:  size,
		Logs:       data,
		MaxLogTime: maxLogTime,
		IsComplete: isComplete,
		TookMs:     time.Since(t).Milliseconds(),
		FromCursor: base64.StdEncoding.EncodeToString(cursorEncode),
		EndCursor:  base64.StdEncoding.EncodeToString(endCursorEncode),
	}

	results, err := sonic.Marshal(&resp)
	if err != nil {
		h.Logger.Error("consume logs get results request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.Header().Set("X-Content-Length", strconv.Itoa(len(results)))
	w.WriteHeader(http.StatusOK)
	addLogQueryStatistics(consumeInfo.GetRepository(), consumeInfo.GetLogstream())
	_, err = w.Write(results)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
	}
	return
}

func (h *Handler) getConsumeCursor(w http.ResponseWriter, consumeInfo *consume.ConsumeInfo, t time.Time) (taskID int, isNeedConsume bool, err error) {
	var cursorT int64
	cursorT = math.MaxInt64
	for k, task := range consumeInfo.GetFromCursor().Tasks {
		if task.CurrTask == nil {
			h.httpErrorRsp(w, ErrorResponse("task is not illegal", LogReqErr), http.StatusBadRequest)
			return
		}
		if task.CurrTask.Timestamp >= cursorT {
			continue
		}
		if task.CurrTask != nil && task.CurrTask.RemotePath == "" {
			_, err = h.updateCursorTask(task, consumeInfo)
			h.getErrConsumeResponse(consumeInfo, t, w, err)
			return
		}
		needConsume, err := h.isShardNeedConsume(task.CurrTask, consumeInfo.GetOption())
		if err != nil {
			if strings.Contains(err.Error(), "obsClient.GetObjectMetadata") {
				h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusRequestedRangeNotSatisfiable)
				return 0, false, err
			}
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return 0, false, err
		}
		if needConsume {
			taskID = k
			cursorT = task.CurrTask.Timestamp
			isNeedConsume = true
		}
	}
	return
}

func (h *Handler) isShardNeedConsume(cursor *consume.ConsumeTask, option *obs.ObsOptions) (bool, error) {
	chunkCount, err := immutable.GetMetaIndexChunkCount(option, cursor.RemotePath)
	return chunkCount > int64(cursor.MetaIndexId), err
}

func (h *Handler) getEmptyCursor(consumeInfo *consume.ConsumeInfo, w http.ResponseWriter, t time.Time) {
	cursorEncode, err := consumeInfo.GetFromCursor().EncodeToByte()
	if err != nil {
		h.Logger.Error("cursor encode is error ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse("cursor encode is error ", LogReqErr), http.StatusBadRequest)
		return
	}
	var endCursorEncode []byte
	if consumeInfo.GetEndCursor() != nil {
		endCursorEncode, err = consumeInfo.GetEndCursor().EncodeToByte()
		if err != nil {
			h.Logger.Error("cursor encode is error ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("cursor encode is error ", LogReqErr), http.StatusBadRequest)
			return
		}
	}
	isComplete := consumeInfo.GetEndCursor() != nil && (consumeInfo.GetFromCursor().Tasks == nil || (len(consumeInfo.GetEndCursor().Tasks) == 0 && len(consumeInfo.GetFromCursor().Tasks) == 0))
	if isComplete {
		cursorEncode = []byte{}
		endCursorEncode = []byte{}
	}
	resp := consume.QueryConsumeLogsResponse{
		TotalCount: 0,
		TotalSize:  0,
		Logs:       nil,
		IsComplete: isComplete,
		TookMs:     time.Since(t).Milliseconds(),
		FromCursor: base64.StdEncoding.EncodeToString(cursorEncode),
		EndCursor:  base64.StdEncoding.EncodeToString(endCursorEncode),
	}

	results, err := sonic.Marshal(&resp)
	if err != nil {
		h.Logger.Error("consume logs get results request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	addLogQueryStatistics(consumeInfo.GetRepository(), consumeInfo.GetLogstream())
	_, err = w.Write(results)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
	}
	return
}

func (h *Handler) getErrConsumeResponse(consumeInfo *consume.ConsumeInfo, t time.Time, w http.ResponseWriter, err error) {
	if QuerySkippingError(err.Error()) {
		var resp *consume.QueryConsumeLogsResponse
		if consumeInfo.GetEndCursor() == nil {
			cursorEncode, err := consumeInfo.GetFromCursor().EncodeToByte()
			if err != nil {
				h.Logger.Error("cursor encode is error ", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse("cursor encode is error ", LogReqErr), http.StatusBadRequest)
				return
			}
			resp = &consume.QueryConsumeLogsResponse{
				TotalCount: 0,
				TotalSize:  0,
				Logs:       make([]map[string]interface{}, 0),
				IsComplete: false,
				TookMs:     time.Since(t).Milliseconds(),
				FromCursor: base64.StdEncoding.EncodeToString(cursorEncode),
			}
		} else {
			resp = &consume.QueryConsumeLogsResponse{
				TotalCount: 0,
				TotalSize:  0,
				Logs:       make([]map[string]interface{}, 0),
				IsComplete: true,
				TookMs:     time.Since(t).Milliseconds(),
			}
		}
		results, err := sonic.Marshal(&resp)
		if err != nil {
			h.Logger.Error("consume logs get results request error! ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Content-Length", strconv.Itoa(len(results)))
		w.WriteHeader(http.StatusOK)
		addLogQueryStatistics(consumeInfo.GetRepository(), consumeInfo.GetLogstream())
		_, err = w.Write(results)
		if err != nil {
			h.Logger.Error("query log marshal res fail! ", zap.Error(err))
		}
		return
	}
	h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
	return
}

func (h *Handler) updateCursorTask(task *consume.ConsumeSegmentTask, consumeInfo *consume.ConsumeInfo) (bool, error) {
	isUpdata := false
	for _, sg := range consumeInfo.GetSgs() {
		if sg.Deleted() || sg.StartTime.UnixNano() <= task.CurrTask.Timestamp {
			continue
		}
		isExist := false
		for _, shard := range sg.Shards {
			if int(shard.Owners[0]) == task.PtID {
				shardPath := obs.GetShardPath(shard.ID, shard.IndexID, shard.Owners[0], sg.StartTime, sg.EndTime, consumeInfo.GetRepository(), consumeInfo.GetLogstream())
				absoluteShardPath := path.Join(h.SQLConfig.Data.DataDir, shardPath)
				localPath := obs.GetBaseMstPath(absoluteShardPath, consumeInfo.GetMstName())
				_, err := os.Stat(localPath)
				if err != nil && os.IsNotExist(err) {
					continue
				}
				remotePath := path.Join(shardPath, consumeInfo.GetMstName())
				obsPath := sparseindex.NewOBSFilterPath(localPath, remotePath, consumeInfo.GetOption())
				tr := util.TimeRange{Min: MinFromValue, Max: MaxToValue}
				tr.Min = sg.StartTime.UnixNano()
				min, minId, err := immutable.GetCursorsBy(obsPath, tr, true)
				if err != nil {
					return isExist, err
				}
				if min == -1 {
					continue
				}

				task.PreTask = task.CurrTask
				task.CurrTask = &consume.ConsumeTask{MetaIndexId: min, BlockID: minId, Timestamp: sg.StartTime.UnixNano(), RemotePath: remotePath, SgID: sg.ID}
				isExist = true
				isUpdata = true
				break
			}
		}
		if isExist {
			break
		}
	}
	return isUpdata, nil
}

func (h *Handler) serveConsumeCursorTime(w http.ResponseWriter, r *http.Request, user meta2.User) {
	params := strings.Split(r.URL.RawQuery, "&")
	var cursorString string
	for _, param := range params {
		kv := strings.SplitN(param, "=", 2)
		if len(kv) == 2 && kv[0] == "cursor" {
			cursorString = kv[1]
		}
	}
	if cursorString == "" {
		h.httpErrorRsp(w, ErrorResponse("cursor is illegal", LogReqErr), http.StatusBadRequest)
		return
	}
	var err error
	cursorString, err = url.PathUnescape(cursorString)
	if err != nil {
		h.httpErrorRsp(w, ErrorResponse("cursor is illegal", LogReqErr), http.StatusBadRequest)
		return
	}
	cursor, err := consume.GetConsumeCursor(cursorString)

	if err != nil || cursor == nil {
		h.httpErrorRsp(w, ErrorResponse("cursor is illegal", LogReqErr), http.StatusBadRequest)
		return
	}
	var t, maxT int64
	if cursor.Reverse {
		t = 0
		maxT = math.MaxInt64
	} else {
		t = math.MaxInt64
		maxT = 0
	}
	for _, task := range cursor.Tasks {
		if cursor.Reverse {
			if task.CurrTask.Timestamp > t {
				t = task.CurrTask.Timestamp
			}
			if task.CurrTask.Timestamp < maxT {
				maxT = task.CurrTask.Timestamp
			}
			continue
		} else {
			if task.CurrTask.Timestamp < t {
				t = task.CurrTask.Timestamp
			}
			if task.CurrTask.Timestamp > maxT {
				maxT = task.CurrTask.Timestamp
			}
			continue
		}
	}
	results, err := json2.Marshal(consume.QueryConsumeCursorTimeResponse{CursorTime: t / int64(1e6), MaxCursorTime: maxT / int64(1e6)})
	if err != nil {
		h.Logger.Error("consume logs get cursor time request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(results)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
	}
	return
}

func (h *Handler) updateCursorByPtNum(fromCursor, endCursor *consume.ConsumeCursor, consumeInfo *consume.ConsumeInfo, mstName string, sgs []*meta2.ShardGroupInfo, pts meta2.DBPtInfos) error {
	for _, pt := range pts {
		if int(pt.PtId)%fromCursor.TaskNum == fromCursor.CursorID {
			isNew := true
			// search new pt to update
			for _, task := range fromCursor.Tasks {
				if task.PtID == int(pt.PtId) {
					isNew = false
				}
			}
			if isNew {
				for _, sg := range sgs {
					if sg.Deleted() || fromCursor.Time >= sg.EndTime.UnixNano() {
						continue
					}
					isExist, err := h.updateCursorByShard(fromCursor, consumeInfo.GetRepository(), consumeInfo.GetLogstream(), mstName, consumeInfo.GetOption(), sg, pt.PtId, false)
					if err != nil {
						return err
					}
					if isExist {
						break
					}
				}
			}
		}
	}
	if endCursor != nil {
		for _, pt := range pts {
			if int(pt.PtId)%endCursor.TaskNum != endCursor.CursorID {
				continue
			}
			isNew := true
			for _, task := range endCursor.Tasks {
				if task.PtID == int(pt.PtId) {
					isNew = false
				}
			}

			if isNew {
				for i := len(sgs) - 1; i >= 0; i-- {
					sg := sgs[i]
					if sg.Deleted() || endCursor.Time < sg.StartTime.UnixNano() {
						continue
					}
					isExist, err := h.updateCursorByShard(endCursor, consumeInfo.GetRepository(), consumeInfo.GetLogstream(), mstName, consumeInfo.GetOption(), sg, pt.PtId, true)
					if err != nil {
						return err
					}
					if isExist {
						break
					}
				}
			}
		}
		if len(endCursor.Tasks) != len(fromCursor.Tasks) {
			return fmt.Errorf("update cursor info failed")
		}
	}
	return nil
}

func (h *Handler) updateCursorByShard(cursor *consume.ConsumeCursor, repository string, logStream, mstName string, option *obs.ObsOptions, sg *meta2.ShardGroupInfo, ptId uint32, isAscending bool) (bool, error) {
	isExist := false
	for _, shard := range sg.Shards {
		if shard.MarkDelete {
			continue
		}
		if shard.Owners[0] == ptId {
			shardPath := obs.GetShardPath(shard.ID, shard.IndexID, shard.Owners[0], sg.StartTime, sg.EndTime, repository, logStream)
			absoluteShardPath := path.Join(h.SQLConfig.Data.DataDir, shardPath)
			localPath := obs.GetBaseMstPath(absoluteShardPath, mstName)
			_, err := os.Stat(localPath)
			if err != nil && os.IsNotExist(err) {
				continue
			}
			remotePath := path.Join(shardPath, mstName)
			obsPath := sparseindex.NewOBSFilterPath(localPath, remotePath, option)
			tr := util.TimeRange{Min: MinFromValue, Max: MaxToValue}
			if !isAscending {
				tr.Min = cursor.Time
			} else {
				tr.Max = cursor.Time
			}
			min, minId, err := immutable.GetCursorsBy(obsPath, tr, isAscending)
			if err != nil {
				return isExist, err
			}
			if min == -1 {
				continue
			}
			cursor.Tasks = append(cursor.Tasks, &consume.ConsumeSegmentTask{PtID: int(ptId), CurrTask: &consume.ConsumeTask{MetaIndexId: min, BlockID: minId, Timestamp: cursor.Time, RemotePath: remotePath, SgID: sg.ID}})
			isExist = true
			break
		}
	}
	return isExist, nil
}

func (h *Handler) serveGetConsumeCursors(w http.ResponseWriter, r *http.Request, user meta2.User) {
	repository := mux.Vars(r)[Repository]
	logStream := mux.Vars(r)[LogStream]
	if err := h.ValidateAndCheckLogStreamExists(repository, logStream); err != nil {
		h.Logger.Error("query log scan request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	db, err := h.MetaClient.Database(repository)
	if err != nil {
		h.Logger.Error("query log request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		atomic.AddInt64(&statistics.HandlerStat.Write400ErrRequests, 1)
		return
	}
	queryLogRequest, err := getQueryConsumeCursorsRequest(r)
	if err != nil {
		h.Logger.Error("consume get cursors request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	pts, err := h.MetaClient.DBPtView(repository)
	if err != nil {
		h.Logger.Error("consume get cursors request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}

	rpMark := make(map[uint32]int, len(pts))
	for _, rp := range pts {
		rpMark[rp.PtId] = 1
	}
	sgs, err := h.MetaClient.GetShardGroupByTimeRange(repository, logStream, time.Unix(0, queryLogRequest.From), time.Unix(0, queryLogRequest.End))
	if err != nil {
		h.Logger.Error("consume get cursors request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	cursorNum := queryLogRequest.TaskNum
	ptNum := len(pts)
	if ptNum < cursorNum {
		cursorNum = ptNum
	}
	cursors := make([]*consume.ConsumeCursor, cursorNum)
	for k := range cursors {
		cursors[k] = &consume.ConsumeCursor{
			Time:           queryLogRequest.From,
			Reverse:        false,
			CursorID:       k,
			TaskNum:        queryLogRequest.TaskNum,
			CurrTotalPtNum: ptNum,
			Tasks:          make([]*consume.ConsumeSegmentTask, 0, (ptNum+queryLogRequest.TaskNum-1)/queryLogRequest.TaskNum),
		}
	}

	mst, err := h.MetaClient.Measurement(repository, logStream, logStream)
	if err != nil {
		h.Logger.Error("consume get cursors request error! ", zap.Error(err), zap.Any("r", r))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}

	maxTaskChan := make(chan struct{}, MaxConsumeChanSize)
	errs := make([]error, 2*len(pts))
	defer close(maxTaskChan)
	var lock sync.RWMutex
	for ptK := range pts {
		maxTaskChan <- struct{}{}
		go func(key int, pts meta2.DBPtInfos) {
			pt := pts[key]
			taskID := int(pt.PtId) % queryLogRequest.TaskNum
			defer func() {
				<-maxTaskChan
			}()
			for _, sg := range sgs {
				if sg.Deleted() {
					continue
				}
				if queryLogRequest.From >= sg.EndTime.UnixNano() {
					continue
				}
				consumeInfo := &consume.ConsumeInfo{
					Option:     db.Options,
					Repository: repository,
					LogStream:  logStream,
					MstName:    mst.Name,
				}
				isExist, err := h.getConsumeCursors(sg, queryLogRequest, pt, cursors, taskID, &lock, true, consumeInfo)
				if err != nil {
					errs[key] = err
					break
				}
				if isExist {
					break
				}
			}
			return
		}(ptK, pts)
	}

	endCursors := make([]*consume.ConsumeCursor, queryLogRequest.TaskNum)
	if queryLogRequest.HasEnd {
		for k := range endCursors {
			endCursors[k] = &consume.ConsumeCursor{
				Reverse:        true,
				CursorID:       k,
				TaskNum:        queryLogRequest.TaskNum,
				Time:           queryLogRequest.End - 1,
				CurrTotalPtNum: ptNum,
				Tasks:          make([]*consume.ConsumeSegmentTask, 0, (ptNum+queryLogRequest.TaskNum-1)/queryLogRequest.TaskNum),
			}
		}
		for ptK := range pts {
			maxTaskChan <- struct{}{}
			go func(key int, pts meta2.DBPtInfos) {
				pt := pts[key]
				taskID := int(pt.PtId) % queryLogRequest.TaskNum
				defer func() {
					<-maxTaskChan
				}()
				for i := len(sgs) - 1; i >= 0; i-- {
					sg := sgs[i]
					if sg.Deleted() {
						continue
					}
					if queryLogRequest.End < sg.StartTime.UnixNano() {
						continue
					}
					consumeInfo := &consume.ConsumeInfo{
						Option:     db.Options,
						Repository: repository,
						LogStream:  logStream,
						MstName:    mst.Name,
					}
					isExist, err := h.getConsumeCursors(sg, queryLogRequest, pt, endCursors, taskID, &lock, false, consumeInfo)
					if err != nil {
						errs[key+len(pts)] = err
						break
					}
					if isExist {
						break
					}
				}
				return
			}(ptK, pts)
		}
	}

	for i := 0; i < MaxConsumeChanSize; i++ {
		maxTaskChan <- struct{}{}
	}

	for _, e := range errs {
		if e != nil {
			h.Logger.Error("get cursor err ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("get cursor err", LogReqErr), http.StatusBadRequest)
			return
		}
	}

	for k := range cursors {
		sort.Slice(cursors[k].Tasks, func(i, j int) bool {
			return cursors[k].Tasks[i].PtID < cursors[k].Tasks[j].PtID
		})
	}
	if queryLogRequest.HasEnd {
		for k := range endCursors {
			sort.Slice(endCursors[k].Tasks, func(i, j int) bool {
				return endCursors[k].Tasks[i].PtID < endCursors[k].Tasks[j].PtID
			})
		}
	}

	resps := make([]consume.QueryConsumeCursorsResponse, 0)

	for k, v := range cursors {
		curEncode, err := v.EncodeToByte()
		if err != nil {
			h.Logger.Error("cursor encode is error ", zap.Error(err))
			h.httpErrorRsp(w, ErrorResponse("cursor encode is error ", LogReqErr), http.StatusBadRequest)
			return
		}

		var endCurEncode []byte
		if endCursors != nil && len(endCursors) != 0 && queryLogRequest.HasEnd {
			endCurEncode, err = endCursors[k].EncodeToByte()
			if err != nil {
				h.Logger.Error("cursor encode is error ", zap.Error(err))
				h.httpErrorRsp(w, ErrorResponse("cursor encode is error ", LogReqErr), http.StatusBadRequest)
				return
			}
		}
		resps = append(resps, consume.QueryConsumeCursorsResponse{
			From: base64.StdEncoding.EncodeToString(curEncode),
			End:  base64.StdEncoding.EncodeToString(endCurEncode),
		})
	}
	results, err := json2.Marshal(resps)
	if err != nil {
		h.Logger.Error("consume get cursors request error! ", zap.Error(err))
		h.httpErrorRsp(w, ErrorResponse(err.Error(), LogReqErr), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	addLogQueryStatistics(repository, logStream)
	_, err = w.Write(results)
	if err != nil {
		h.Logger.Error("query log marshal res fail! ", zap.Error(err))
	}
}

func (h *Handler) getConsumeCursors(sg *meta2.ShardGroupInfo, queryLogRequest *consume.ConsumeCursorsRequest, pt meta2.PtInfo,
	cursors []*consume.ConsumeCursor, taskID int, lock *sync.RWMutex, isAscending bool, consumeInfo *consume.ConsumeInfo) (isExist bool, err error) {
	isExist = false
	for _, shard := range sg.Shards {
		if shard.MarkDelete {
			continue
		}
		if shard.Owners[0] == pt.PtId {
			shardPath := obs.GetShardPath(shard.ID, shard.IndexID, shard.Owners[0], sg.StartTime, sg.EndTime, consumeInfo.GetRepository(), consumeInfo.GetLogstream())
			absoluteShardPath := path.Join(h.SQLConfig.Data.DataDir, shardPath)
			localPath := obs.GetBaseMstPath(absoluteShardPath, consumeInfo.GetMstName())
			_, err := os.Stat(localPath)
			if err != nil && os.IsNotExist(err) {
				continue
			}
			remotePath := path.Join(shardPath, consumeInfo.GetMstName())
			obsPath := sparseindex.NewOBSFilterPath(localPath, remotePath, consumeInfo.GetOption())
			min, minId, err := immutable.GetCursorsBy(obsPath, util.TimeRange{Min: queryLogRequest.From, Max: queryLogRequest.End}, isAscending)
			if err != nil {
				return isExist, err
			}
			if min == -1 {
				continue
			}
			var currT int64
			if isAscending {
				currT = queryLogRequest.From
			} else {
				currT = queryLogRequest.End
			}
			lock.Lock()
			cursors[taskID].Tasks = append(cursors[taskID].Tasks, &consume.ConsumeSegmentTask{PtID: int(pt.PtId), CurrTask: &consume.ConsumeTask{MetaIndexId: min, BlockID: minId, Timestamp: currT, RemotePath: remotePath, SgID: sg.ID}})
			lock.Unlock()
			isExist = true
			break
		}
	}
	return isExist, nil
}

func getQueryLogContextRequest(r *http.Request) (*QueryLogRequest, error) {
	var err error
	queryLogRequest := &QueryLogRequest{}
	_, err = getQueryQueryLogRequest(r, queryLogRequest)
	if err != nil {
		return nil, err
	}
	direction := r.FormValue("direction")
	if direction == "backward" {
		queryLogRequest.Reverse = true
	} else if direction == "forward" {
		queryLogRequest.Reverse = false
	} else if direction == "" {
		queryLogRequest.Reverse = false
	} else {
		return nil, fmt.Errorf("direction value is illegal")
	}

	if r.FormValue("timeout_ms") == "" {
		queryLogRequest.Timeout = DefaultLogQueryTimeout
	} else {
		queryLogRequest.Timeout, err = strconv.Atoi(r.FormValue("timeout_ms"))
		if err != nil {
			return nil, fmt.Errorf("timeout_ms value is illegal")
		}
		if queryLogRequest.Timeout < MinTimeoutMs || queryLogRequest.Timeout > MaxTimeoutMs {
			return nil, fmt.Errorf("the valid range for timeout_ms is [%d, %d]", MinTimeoutMs, MaxTimeoutMs)
		}
	}

	queryLogRequest.IsTruncate = r.FormValue("is_truncate") == "true"
	queryLogRequest.Pretty = r.FormValue("pretty") == "true"

	queryLogRequest.From, err = strconv.ParseInt(r.FormValue("from"), 10, 64)
	if err != nil {
		queryLogRequest.From = 0
	}
	queryLogRequest.To, err = strconv.ParseInt(r.FormValue("to"), 10, 64)
	if err != nil {
		queryLogRequest.To = math.MaxInt64 / int64(1e6)
	}

	if queryLogRequest.From > queryLogRequest.To {
		return nil, fmt.Errorf("from value cannot be lower than to value")
	}
	if queryLogRequest.From < MinFromValue {
		return nil, fmt.Errorf("from value is lower than min value [%d]", MinFromValue)
	}
	if queryLogRequest.To > (MaxToValue / int64(1e6)) {
		return nil, fmt.Errorf("to value is larger than max value [%d]", MaxToValue/int64(1e6))
	}
	maxSecond := (MaxToValue / int64(1e6))
	if maxSecond < queryLogRequest.To {
		queryLogRequest.To = maxSecond
	}
	if maxSecond < queryLogRequest.From {
		queryLogRequest.From = maxSecond
	}
	queryLogRequest.Scroll_id = r.FormValue("cursor")
	if queryLogRequest.Scroll_id == "" {
		return nil, fmt.Errorf("no cursor")
	}
	if queryLogRequest.Scroll_id != "" && (len(queryLogRequest.Scroll_id) < MinScrollIdLen || len(queryLogRequest.Scroll_id) > MaxScrollIdLen) {
		return nil, fmt.Errorf("the valid range for cursor is [%d, %d]", MinScrollIdLen, MaxScrollIdLen)
	}
	_, err = getQueryCommonLogRequest(r, queryLogRequest)
	if err != nil {
		return nil, err
	}
	if queryLogRequest.Sql == false {
		queryLogRequest.Query = removeLastSelectStr(queryLogRequest.Query)
	}
	return queryLogRequest, nil
}

func (h *Handler) serveGetCursor(w http.ResponseWriter, r *http.Request, user meta2.User) {}

func (h *Handler) servePullLog(w http.ResponseWriter, r *http.Request, user meta2.User) {}

func getQueryConsumeCursorsRequest(r *http.Request) (*consume.ConsumeCursorsRequest, error) {
	var err error
	queryLogRequest := &consume.ConsumeCursorsRequest{}

	queryLogRequest.From, err = strconv.ParseInt(r.FormValue("from"), 10, 64)
	if err != nil {
		if r.FormValue("from") == "" {
			queryLogRequest.From = 0
		} else {
			return nil, fmt.Errorf("from value is illegal")
		}
	}
	queryLogRequest.End, err = strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		queryLogRequest.HasEnd = false
		queryLogRequest.End = MaxToValue / int64(1e6)
	} else {
		queryLogRequest.HasEnd = true
	}
	if queryLogRequest.From >= queryLogRequest.End {
		return nil, fmt.Errorf("from value cannot be lower than end value")
	}
	if queryLogRequest.From < MinFromValue {
		return nil, fmt.Errorf("from value is lower than min value [%d]", MinFromValue)
	}
	if queryLogRequest.End > (MaxToValue / int64(1e6)) {
		return nil, fmt.Errorf("end value is larger than max value [%d]", MaxToValue/int64(1e6))
	}
	maxSecond := (MaxToValue / int64(1e6))
	if maxSecond < queryLogRequest.End {
		queryLogRequest.End = maxSecond
	}
	if maxSecond < queryLogRequest.From {
		queryLogRequest.From = maxSecond
	}

	queryLogRequest.From = queryLogRequest.From * int64(1e6)
	queryLogRequest.End = queryLogRequest.End * int64(1e6)

	queryLogRequest.TaskNum, err = strconv.Atoi(r.FormValue("task_num"))
	if err != nil || queryLogRequest.TaskNum <= 0 || queryLogRequest.TaskNum > 10000 {
		return nil, fmt.Errorf("task_num value is illegal")
	}
	return queryLogRequest, nil
}
