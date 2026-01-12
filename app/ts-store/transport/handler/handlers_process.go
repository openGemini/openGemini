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
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	data "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	query2 "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	MaxChunkSize    = 500000
	ZeroTime        = 0
	CountTimeColIdx = 0
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
	var plan engine.DDLBasePlans

	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		engine := h.store.GetEngine()

		ptIdRefSuc := make([]uint32, 0, len(h.req.PtIDs))
		defer func() {
			for _, ptId := range ptIdRefSuc {
				engine.DbPTUnref(*h.req.Db, ptId, true)
			}
		}()
		for _, ptId := range h.req.PtIDs {
			if err := engine.DbPTRef(*h.req.Db, ptId, true); err != nil {
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
	var plan engine.DDLBasePlans
	h.rsp.Err = processDDL(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		engine := h.store.GetEngine()

		ptIdRefSuc := make([]uint32, 0, len(h.req.PtIDs))
		defer func() {
			for _, ptId := range ptIdRefSuc {
				engine.DbPTUnref(*h.req.Db, ptId, true)
			}
		}()
		for _, ptId := range h.req.PtIDs {
			if err := engine.DbPTRef(*h.req.Db, ptId, true); err != nil {
				return err
			}
			ptIdRefSuc = append(ptIdRefSuc, ptId)
		}

		plan = engine.CreateDDLBasePlans(hybridqp.ShowTagValues, *h.req.Db, ptIdRefSuc, &tr)

		tagValues, err := plan.Execute(h.req.GetTagKeysBytes(), expr, util.TimeRange{
			Min: tr.Min.UnixNano(),
			Max: tr.Max.UnixNano(),
		}, int(h.req.GetLimit()))

		tablesTagSets, ok := tagValues.(influxql.TablesTagSets)
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
	var queries []*msgservice.QueryExeInfo

	// get all query exe info from all query managers
	getAllQueries := func(manager *query.Manager) {
		queries = append(queries, manager.GetAll()...)
	}
	query.VisitManagers(getAllQueries)
	rsp := &msgservice.ShowQueriesResponse{}
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
		manager.Abort(qid, statistics.User)
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
			return msgservice.MarshalError(err)
		}
	}

	if tr.Min.IsZero() {
		tr.Min = time.Unix(0, influxql.MinTime).UTC()
	}
	if tr.Max.IsZero() {
		tr.Max = time.Unix(0, influxql.MaxTime).UTC()
	}

	err = processor(expr, tr)
	return msgservice.MarshalError(err)
}

func processDML(cond []*data.EqCond, processor func(expr influxql.Expr, timeRange influxql.TimeRange) error) *string {
	var tr influxql.TimeRange
	// or binary
	expr, err := ConvertOrCondIntoExpr(cond)
	if err != nil {
		return msgservice.MarshalError(err)
	}

	if tr.Min.IsZero() {
		tr.Min = time.Unix(0, influxql.MinTime).UTC()
	}
	if tr.Max.IsZero() {
		tr.Max = time.Unix(0, influxql.MaxTime).UTC()
	}

	err = processor(expr, tr)
	return msgservice.MarshalError(err)
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

func (h *RaftMessages) Release() {
	h.req.Release()
}

func (h *DropSeries) Process() (codec.BinaryCodec, error) {
	logger.GetLogger().Info("start deleting series", zap.Any("condition", h.req.Condition), zap.String("measurement", h.req.Measurements[0]))
	cond := h.req.Condition
	var expr influxql.Expr
	var tr influxql.TimeRange
	var err error

	if cond != nil {
		expr, tr, err = parseTagKeyCondition(*cond)
		if err != nil {
			h.rsp.Err = msgservice.MarshalError(err)
			return h.rsp, nil
		}
	}

	if tr.Min.IsZero() {
		tr.Min = time.Unix(0, influxql.MinTime).UTC()
	}
	if tr.Max.IsZero() {
		tr.Max = time.Unix(0, influxql.MaxTime).UTC()
	}
	t := tsi.TimeRange{}
	t.Min = tr.MinTimeNano()
	t.Max = tr.MaxTimeNano()

	storeEngine := h.store.GetEngine()
	dbName := h.req.GetDb()
	ptIdRefSuc := make(map[uint32]struct{}, len(h.req.PtIDs))
	defer func() {
		for ptId := range ptIdRefSuc {
			storeEngine.DbPTUnref(dbName, ptId, false)
		}
	}()

	for _, ptId := range h.req.PtIDs {
		if err = storeEngine.DbPTRef(dbName, ptId, false); err != nil {
			h.rsp.Err = msgservice.MarshalError(err)
			return h.rsp, nil
		}
		ptIdRefSuc[ptId] = struct{}{}
	}
	mstName := []byte(h.req.Measurements[0])

	for ptId := range ptIdRefSuc {
		if err = storeEngine.MarkDropSeries(dbName, ptId, mstName, expr, t); err != nil {
			h.rsp.Err = msgservice.MarshalError(err)
			return h.rsp, nil
		}
	}
	return h.rsp, err
}

func (h *ShowLastIndex) Process() (codec.BinaryCodec, error) {
	dbName := h.req.GetDatabase()
	var err error

	storeEngine := h.store.GetEngine()

	ptIdRefSuc := make(map[uint32]struct{}, len(h.req.PtIds))
	defer func() {
		for ptId := range ptIdRefSuc {
			storeEngine.DbPTUnref(dbName, ptId, true)
		}
	}()

	for _, ptId := range h.req.PtIds {
		if err = storeEngine.DbPTRef(dbName, ptId, true); err != nil {
			h.rsp.Err = err
			return h.rsp, nil
		}
		ptIdRefSuc[ptId] = struct{}{}
	}
	var lastIndex uint64
	for ptId := range ptIdRefSuc {
		if lastIndex, err = storeEngine.GetLastIndex(dbName, ptId); err != nil {
			h.rsp.Err = err
			return h.rsp, nil
		}
		h.rsp.Infos = append(h.rsp.Infos, meta.DbPtLastIndexInfo{
			Database:  dbName,
			PtId:      ptId,
			LastIndex: lastIndex,
		})
	}

	return h.rsp, err
}

func (h *SmarterQuery) Release() {
	h.rsp.Release()
}

// Process executes the query request, processes the data, and returns the result or an error.
// It handles both non-aggregate and aggregate queries, merging or aggregating data as needed.
func (h *SmarterQuery) Process() (codec.BinaryCodec, error) {
	// Process the query condition and execute the data processing logic
	h.rsp.Err = processDML(h.req.Condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		// Get the storage engine instance
		eg := h.store.GetEngine()

		// Initialize query options, schema, and operation configurations
		options, schema, ops, err := h.initOptions(expr)
		if err != nil {
			return err
		}

		pk, ok := eg.GetColStorePK(*h.req.Db, *h.req.RP, *h.req.Mst)
		if ok && options.IsOnlyCountTime() {
			if options.GetCondition() == nil {
				return h.RowCount(eg, options, schema, tr, false)
			}
			if h.IsOnlyPKFilter(pk) {
				return h.RowCount(eg, options, schema, tr, true)
			}
		}

		// Create iterators to consume data from the storage engine
		ident := util.NewMeasurementIdent(*h.req.Db, *h.req.RP)
		ident.SetName(*h.req.Mst)
		iterators, release := eg.CreateConsumeIterator(ident, h.req.PtIDs, options)
		if len(iterators) == 0 {
			return nil
		}
		defer release()

		// Calculate the total number of time series IDs across all iterators
		var sidCnt int
		for i := range iterators {
			sidCnt += iterators[i].SidCnt()
		}

		// Handle non-aggregate or single time series aggregate queries by merging data
		if len(ops) == 0 || sidCnt == 1 {
			return h.merge(iterators, sidCnt == 1)
		}

		// Handle multi time series aggregate queries by aggregating and merging data
		return h.aggMerge(iterators, options, schema, ops)
	})

	return h.rsp, nil
}

func (h *SmarterQuery) IsOnlyPKFilter(pk record.Schemas) bool {
	if pk.Len() == 0 {
		return false
	}
	for _, field := range h.req.Condition {
		if pk.FieldIndex(field.GetKey()) == -1 {
			return false
		}
	}
	return true
}

func (h *SmarterQuery) initOptions(condition influxql.Expr) (
	*query2.ProcessorOptions, record.Schemas, []*comm.CallOption, error,
) {
	options := &query2.ProcessorOptions{
		StartTime: h.req.GetStartTime(),
		EndTime:   h.req.GetEndTime(),
		Interval:  hybridqp.Interval{Duration: time.Duration(*h.req.Interval)},
		Condition: condition,
		ChunkSize: MaxChunkSize,
		FieldAux:  make([]influxql.VarRef, 0, len(h.req.Fields)),
		Exprs:     make([]influxql.Expr, 0, len(h.req.Fields)),
		Limit:     int(h.req.GetLimit()),
		Offset:    int(h.req.GetOffset()),
		HintType:  hybridqp.HintType(h.req.GetHintType()),
		SeriesKey: h.req.GetHintSeriesKeys(),
	}
	var schema record.Schemas
	var ops []*comm.CallOption
	for i := range h.req.Fields {
		fieldExpr := ConvertFieldIntoExpr(h.req.Fields[i])
		options.Exprs = append(options.Exprs, fieldExpr)
		switch field := fieldExpr.(type) {
		case *influxql.Call:
			varRef := field.Args[0].(*influxql.VarRef)
			options.FieldAux = append(options.FieldAux, *varRef)
			schema = append(schema, record.Field{Type: record.ToModelTypes(varRef.Type), Name: varRef.Val})
			ops = append(ops, &comm.CallOption{
				Call: field,
				Ref:  varRef,
			})
		case *influxql.VarRef:
			options.FieldAux = append(options.FieldAux, *field)
			schema = append(schema, record.Field{Type: record.ToModelTypes(field.Type), Name: field.Val})
		default:
			return nil, nil, nil, fmt.Errorf("invalid field type")
		}
	}
	// add a time field as the last column in the schema.
	schema = append(schema, record.Field{Type: influx.Field_Type_Int, Name: record.TimeField})
	return options, schema, ops, nil
}

func (h *SmarterQuery) RowCount(eg engine.Engine, options hybridqp.Options, schema record.Schemas, tr influxql.TimeRange, isOnlyPKFilter bool) error {
	var rowCount int64
	options.SetSources(influxql.Sources{&influxql.Measurement{Name: *h.req.Mst}})
	querySchema := executor.NewQuerySchemaWithOpt(options)
	for _, ptID := range h.req.PtIDs {
		shardIDs, err := eg.GetShardIDs(*h.req.Db, ptID, &tr)
		if err != nil {
			return err
		}
		cnt, err := eg.RowCount(*h.req.Db, ptID, shardIDs, querySchema, isOnlyPKFilter)
		if err != nil {
			return err
		}
		rowCount += cnt
	}
	rec := record.NewRecord(schema, false)
	rec.Column(CountTimeColIdx).AppendInteger(rowCount)
	rec.AppendTime(ZeroTime)
	h.rsp.SetResult(record.NewSmartQueryResult([]*record.ConsumeRecord{{Rec: rec}}, true))
	return nil
}

func (h *SmarterQuery) merge(iterators []record.Iterator, isSingleSeries bool) error {
	defer func() {
		// Release all iterators after processing
		for i := range iterators {
			iterators[i].Release()
		}
	}()
	var recs []*record.ConsumeRecord
	i := 0
	for i < len(iterators) {
		rec, err := iterators[i].Next()
		if err != nil {
			if err == io.EOF {
				i++
				continue
			}
			return err
		}
		if rec == nil {
			i++
			continue
		}
		recs = append(recs, rec)
	}
	if len(recs) == 0 {
		return nil
	}
	h.rsp.SetResult(record.NewSmartQueryResult(recs, isSingleSeries))
	return nil
}

func (h *SmarterQuery) aggMerge(iterators []record.Iterator, options hybridqp.Options, schema record.Schemas, ops []*comm.CallOption) error {
	var recs []*record.ConsumeRecord
	srcIter := comm.NewSourceIterator(iterators, options)
	querySchema := executor.NewQuerySchemaWithOpt(options)
	queryCtx := engine.NewKeyCursorContext(querySchema)
	aggIter := engine.NewAggTagSetCursor(querySchema, queryCtx, srcIter, false)
	aggIter.InitOps(ops, schema)
	defer aggIter.Close()
	for {
		rec, _, err := aggIter.Next()
		if err != nil {
			return err
		}
		if rec == nil {
			break
		}
		recs = append(recs, &record.ConsumeRecord{Rec: rec})
	}
	if len(recs) == 0 {
		return nil
	}
	h.rsp.SetResult(record.NewSmartQueryResult(recs, false))
	return nil
}

func ConvertFieldIntoExpr(field *data.Field) influxql.Expr {
	if field == nil {
		return nil
	}
	if len(*field.Call) > 0 {
		return &influxql.Call{
			Name: *field.Call,
			Args: []influxql.Expr{&influxql.VarRef{Val: *field.Val, Type: influxql.DataType(*field.Typ)}},
		}
	}
	return &influxql.VarRef{Val: *field.Val, Type: influxql.DataType(*field.Typ)}
}

func convertCondIntoLiteral(cond *data.EqCond) (*influxql.BinaryExpr, error) {
	if cond == nil {
		return nil, nil
	}
	expr := &influxql.BinaryExpr{
		Op:  influxql.Token(*cond.Op),
		LHS: &influxql.VarRef{Val: *cond.Key, Type: influxql.DataType(*cond.Typ)},
	}
	switch influxql.DataType(*(cond.Typ)) {
	case influxql.String, influxql.Tag:
		expr.RHS = &influxql.StringLiteral{Val: *cond.Val}
	case influxql.Float:
		val, err := strconv.ParseFloat(*cond.Val, 64)
		if err != nil {
			return nil, err
		}
		expr.RHS = &influxql.NumberLiteral{Val: val}
	case influxql.Integer:
		val, err := strconv.ParseInt(*cond.Val, 10, 64)
		if err != nil {
			return nil, err
		}
		expr.RHS = &influxql.IntegerLiteral{Val: val}
	case influxql.Boolean:
		val, err := strconv.ParseBool(*cond.Val)
		if err != nil {
			return nil, err
		}
		expr.RHS = &influxql.BooleanLiteral{Val: val}
	default:
	}
	return expr, nil
}

func ConvertOrCondIntoExpr(cond []*data.EqCond) (influxql.Expr, error) {
	if len(cond) == 0 {
		return nil, nil
	}
	expr, err := convertCondIntoLiteral(cond[0])
	if err != nil {
		return nil, err
	}
	if len(cond) == 1 {
		return expr, nil
	}

	for i := 1; i < len(cond); i++ {
		lhs, err := convertCondIntoLiteral(cond[i])
		if err != nil {
			return nil, err
		}
		expr = &influxql.BinaryExpr{
			Op:  influxql.OR,
			LHS: lhs,
			RHS: expr,
		}
	}
	return expr, nil
}

func (h *PullRPPTWriteStatus) Process() (codec.BinaryCodec, error) {
	db := h.req.GetDatabase()
	rp := h.req.GetRetentionPolicy()
	status, err := h.store.GetEngine().GetRPPTWriteStat(db, rp)
	if err != nil {
		h.rsp.Error = err
		return h.rsp, nil
	}
	h.rsp.StatusInfo = status
	return h.rsp, nil
}

func (h *GetTask) Process() (codec.BinaryCodec, error) {
	task, err := h.store.GetEngine().GetTask(h.req.Type, 0)
	if err != nil {
		h.rsp.SetError(err.Error())
		return h.rsp, nil
	}
	if task != nil {
		h.rsp.SetTask(task)
	}
	return h.rsp, nil
}

func (h *SendTaskResult) Process() (codec.BinaryCodec, error) {
	switch h.req.Type {
	case scheduler.CompactTask:
		if h.req.Info == nil {
			return nil, errors.New("CompactedFileInfo should not be empty")
		}
		info, ok := h.req.Info.(*immutable.CompactedFileInfo)
		if !ok {
			return nil, fmt.Errorf("wrong info type, exp *immutable.CompactedFileInfo,got %s", reflect.TypeOf(h.req.Info).String())
		}
		err := h.store.GetEngine().CompactFiles(h.req.Type, h.req.Uuid, info)
		if err != nil {
			h.rsp.SetError(err.Error())
		}
	default:
		return nil, errors.New("wrong task type")
	}

	return h.rsp, nil
}
