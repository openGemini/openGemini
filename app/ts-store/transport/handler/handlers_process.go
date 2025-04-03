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

package handler

import (
	"errors"
	"fmt"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

func (h *Delete) Process() (codec.BinaryCodec, error) {
	h.rsp.Err = h.store.ExecuteDelete(h.req)
	return h.rsp, nil
}

func (h *GetShardSplitPoints) Process() (codec.BinaryCodec, error) {
	logger.GetLogger().Info("GetShardSplitPoints",
		zap.Any("db", h.req.GetDB()),
		zap.Any("pt", h.req.GetPtID()),
		zap.Any("sharedID", h.req.GetShardID()),
		zap.Any("idxes", h.req.GetIdxes()))

	h.rsp.Err = processDDL(nil, func(expr influxql.Expr, tr influxql.TimeRange) error {
		var err error
		h.rsp.SplitPoints, err = h.store.GetShardSplitPoints(h.req.GetDB(), h.req.GetPtID(), h.req.GetShardID(), h.req.GetIdxes())
		return err
	})

	return h.rsp, nil
}

func (h *SeriesCardinality) Process() (codec.BinaryCodec, error) {
	err := processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		var err error
		h.rsp.CardinalityInfos, err = h.store.SeriesCardinality(*h.req.Db, h.req.PtIDs, h.req.Measurements, expr, tr)
		return err
	})

	if err != nil {
		h.rsp.Err = errors.New(*err)
	}

	return h.rsp, nil
}

func (h *SeriesExactCardinality) Process() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		var err error
		h.rsp.Cardinality, err = h.store.SeriesExactCardinality(*h.req.Db, h.req.PtIDs, h.req.Measurements, expr, tr)
		return err
	})
	return h.rsp, nil
}

// processRough used to search series keys from index
// the query time range can match only the index time range.
func (h *SeriesKeys) processRough() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		var err error
		h.rsp.Series, err = h.store.SeriesKeys(*h.req.Db, h.req.PtIDs, h.req.Measurements, expr, tr)
		return err
	})

	return h.rsp, nil
}

// processExact used to search series keys from the chunk meta and index,
// the query time range can match the chunk meta time range.
func (h *SeriesKeys) processExact() (codec.BinaryCodec, error) {
	var plan netstorage.DDLBasePlans

	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		engine := h.store.GetEngine()

		ptIdRefSuc := make([]uint32, 0, len(h.req.PtIDs))
		defer func() {
			for _, ptId := range ptIdRefSuc {
				engine.DbPTUnref(*h.req.Db, ptId)
			}
		}()
		for _, ptId := range h.req.PtIDs {
			if err := engine.DbPTRef(*h.req.Db, ptId); err != nil {
				return err
			}
			ptIdRefSuc = append(ptIdRefSuc, ptId)
		}

		plan = engine.CreateDDLBasePlans(hybridqp.ShowSeries, *h.req.Db, ptIdRefSuc, &tr)
		mstKeys := make(map[string][][]byte, len(h.req.Measurements))
		for _, mst := range h.req.Measurements {
			mstKeys[mst] = [][]byte{}
		}
		seriesKeys, err := plan.Execute(mstKeys, expr, util.TimeRange{
			Min: tr.Min.UnixNano(),
			Max: tr.Max.UnixNano(),
		}, 0)
		series, ok := seriesKeys.([]string)
		if !ok {
			return fmt.Errorf("invalid series")
		}
		h.rsp.Series = series
		return err
	})

	return h.rsp, nil
}

func (h *SeriesKeys) Process() (codec.BinaryCodec, error) {
	if !h.req.GetExact() {
		return h.processRough()
	}
	return h.processExact()
}

func (h *ShowTagKeys) Process() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		var err error
		h.rsp.TagKeys, err = h.store.TagKeys(*h.req.Db, h.req.PtIDs, h.req.Measurements, expr, tr)
		return err
	})

	return h.rsp, nil
}

func (h *CreateDataBase) Process() (codec.BinaryCodec, error) {
	if err := createDir(h.store.GetPath(), h.req.GetDb(), h.req.GetPt(), h.req.GetRp()); err != nil {
		h.rsp.Err = proto.String(err.Error())
	}

	return h.rsp, nil
}

func (h *ShowTagValues) Process() (codec.BinaryCodec, error) {
	if !h.req.GetExact() {
		return h.processRough()
	}
	return h.processExact()
}

// processRough used to search tag values from the index,
// the query time range can match the index time range.
func (h *ShowTagValues) processRough() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		tagKeys := h.req.GetTagKeysBytes()
		if len(tagKeys) == 0 {
			return nil
		}

		tagValues, err := h.store.TagValues(*h.req.Db, h.req.PtIDs, tagKeys, expr, tr)
		h.rsp.SetTagValuesSlice(tagValues)

		return err
	})

	return h.rsp, nil
}

// processExact used to search tag values from the chunk meta and index,
// the query time range can match the chunk meta time range.
func (h *ShowTagValues) processExact() (codec.BinaryCodec, error) {
	var plan netstorage.DDLBasePlans
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		engine := h.store.GetEngine()

		ptIdRefSuc := make([]uint32, 0, len(h.req.PtIDs))
		defer func() {
			for _, ptId := range ptIdRefSuc {
				engine.DbPTUnref(*h.req.Db, ptId)
			}
		}()
		for _, ptId := range h.req.PtIDs {
			if err := engine.DbPTRef(*h.req.Db, ptId); err != nil {
				return err
			}
			ptIdRefSuc = append(ptIdRefSuc, ptId)
		}

		plan = engine.CreateDDLBasePlans(hybridqp.ShowTagValues, *h.req.Db, ptIdRefSuc, &tr)

		tagValues, err := plan.Execute(h.req.GetTagKeysBytes(), expr, util.TimeRange{
			Min: tr.Min.UnixNano(),
			Max: tr.Max.UnixNano(),
		}, int(h.req.GetLimit()))

		tablesTagSets, ok := tagValues.(netstorage.TablesTagSets)
		if !ok {
			return fmt.Errorf("invalid TablesTagSets")
		}
		h.rsp.SetTagValuesSlice(tablesTagSets)

		return err
	})

	return h.rsp, nil
}

func (h *ShowTagValuesCardinality) Process() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		var err error
		h.rsp.Cardinality, err = h.store.TagValuesCardinality(*h.req.Db, h.req.PtIDs, h.req.GetTagKeysBytes(), expr, tr)
		return err
	})

	return h.rsp, nil
}

func (h *ShowQueries) Process() (codec.BinaryCodec, error) {
	var queries []*netstorage.QueryExeInfo

	// get all query exe info from all query managers
	getAllQueries := func(manager *query.Manager) {
		queries = append(queries, manager.GetAll()...)
	}
	query.VisitManagers(getAllQueries)
	rsp := &netstorage.ShowQueriesResponse{}
	rsp.QueryExeInfos = queries

	return rsp, nil

}

func (h *KillQuery) Process() (codec.BinaryCodec, error) {
	qid := h.req.GetQueryID()
	var isExist bool
	var abortSuccess bool
	killQueryByIDFn := func(manager *query.Manager) {
		// qid is not in current manager, or it has been aborted successfully
		if len(manager.Get(qid)) == 0 || abortSuccess {
			return
		}
		isExist = true
		manager.Kill(qid)
		manager.Abort(qid)
		abortSuccess = true
	}
	query.VisitManagers(killQueryByIDFn)
	if !isExist {
		var errCode uint32 = errno.ErrQueryNotFound
		err := errno.NewError(errno.ErrQueryNotFound, qid)
		var errMsg = err.Error()
		h.rsp.ErrCode = &errCode
		h.rsp.ErrMsg = &errMsg
	}
	return h.rsp, nil
}

func processDDL(cond *string, processor func(expr influxql.Expr, timeRange influxql.TimeRange) error) *string {
	var err error
	var expr influxql.Expr
	var tr influxql.TimeRange

	if cond != nil {
		expr, tr, err = parseTagKeyCondition(*cond)
		if err != nil {
			return netstorage.MarshalError(err)
		}
	}

	if tr.Min.IsZero() {
		tr.Min = time.Unix(0, influxql.MinTime).UTC()
	}
	if tr.Max.IsZero() {
		tr.Max = time.Unix(0, influxql.MaxTime).UTC()
	}

	err = processor(expr, tr)
	return netstorage.MarshalError(err)
}

func createDir(dataPath, db string, pt uint32, rp string) error {
	// create database directory
	dbPath := dataPath + "/" + db
	if err := fileops.Mkdir(dbPath, 0750); err != nil {
		return err
	}

	// create pt directory
	ptPath := dbPath + "/" + fmt.Sprintf("%d", pt)
	if err := fileops.Mkdir(ptPath, 0750); err != nil {
		return err
	}

	// create rp directory
	rpPath := ptPath + "/" + rp
	if err := fileops.Mkdir(rpPath, 0750); err != nil {
		return err
	}

	return nil
}

func (h *RaftMessages) Process() (codec.BinaryCodec, error) {
	err := h.store.SendRaftMessage(h.req.Database, uint64(h.req.PtId), h.req.RaftMessage)
	if err != nil {
		h.rsp.ErrMsg = proto.String(err.Error())
	}
	return h.rsp, nil
}
