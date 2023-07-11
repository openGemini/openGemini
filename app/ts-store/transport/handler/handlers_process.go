/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package handler

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
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

	h.rsp.Err = processDDL(nil, func(expr influxql.Expr) error {
		var err error
		h.rsp.SplitPoints, err = h.store.GetShardSplitPoints(h.req.GetDB(), h.req.GetPtID(), h.req.GetShardID(), h.req.GetIdxes())
		return err
	})

	return h.rsp, nil
}

func (h *SeriesCardinality) Process() (codec.BinaryCodec, error) {
	err := processDDL(h.req.Condition, func(expr influxql.Expr) error {
		var err error
		h.rsp.CardinalityInfos, err = h.store.SeriesCardinality(*h.req.Db, h.req.PtIDs, h.req.Measurements, expr)
		return err
	})

	if err != nil {
		h.rsp.Err = errors.New(*err)
	}

	return h.rsp, nil
}

func (h *SeriesExactCardinality) Process() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr) error {
		var err error
		h.rsp.Cardinality, err = h.store.SeriesExactCardinality(*h.req.Db, h.req.PtIDs, h.req.Measurements, expr)
		return err
	})
	return h.rsp, nil
}

func (h *SeriesKeys) Process() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr) error {
		var err error
		h.rsp.Series, err = h.store.SeriesKeys(*h.req.Db, h.req.PtIDs, h.req.Measurements, expr)
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
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr) error {
		tagKeys := h.req.GetTagKeysBytes()
		if len(tagKeys) == 0 {
			return nil
		}

		tagValues, err := h.store.TagValues(*h.req.Db, h.req.PtIDs, tagKeys, expr)
		h.rsp.SetTagValuesSlice(tagValues)

		return err
	})

	return h.rsp, nil
}

func (h *ShowTagValuesCardinality) Process() (codec.BinaryCodec, error) {
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr) error {
		var err error
		h.rsp.Cardinality, err = h.store.TagValuesCardinality(*h.req.Db, h.req.PtIDs, h.req.GetTagKeysBytes(), expr)
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

func processDDL(cond *string, processor func(expr influxql.Expr) error) *string {
	var err error
	var expr influxql.Expr

	if cond != nil {
		expr, err = parseTagKeyCondition(*cond)
		if err != nil {
			return netstorage.MarshalError(err)
		}
	}

	err = processor(expr)
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
