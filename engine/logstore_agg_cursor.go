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
package engine

import (
	"math"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/logstore"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	logpath "github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	logStoreAggCursorDuration = "logStore_agg_cursor_duration"
)

type LogStoreAggCursor struct {
	isCutSchema           bool
	isInit                bool
	version               uint32
	minTime               int64
	maxTime               int64
	tm                    time.Time
	ctx                   *idKeyCursorContext
	span                  *tracing.Span
	schema                *executor.QuerySchema
	recordSchema          record.Schemas
	realRecordSchema      record.Schemas
	ridIdx                map[int]struct{}
	metaInfo              []*logstore.MetaData
	dataReader            *logstore.DataReader
	option                *obs.ObsOptions
	logStoreSegmentPrefix string
}

func NewLogStoreAggCursor(option *obs.ObsOptions, path string, version uint32, ctx *idKeyCursorContext, span *tracing.Span, schema *executor.QuerySchema) (*LogStoreAggCursor, error) {
	l := &LogStoreAggCursor{
		option:                option,
		ctx:                   ctx,
		span:                  span,
		schema:                schema,
		logStoreSegmentPrefix: path,
		version:               version,
		realRecordSchema:      ctx.schema.Copy(),
	}
	return l, nil
}

func (s *LogStoreAggCursor) init() (int32, error) {
	iterNum, metaData, maxBlockID, err := s.getLogStoreAggCursorMetaBy()
	if err != nil {
		return 0, err
	}
	currIter := s.ctx.querySchema.GetOptions().GetIterId()
	if currIter >= iterNum {
		return iterNum, nil
	}
	metaDataNum := int32(len(metaData))
	offsets := make([]int64, 0, logpath.GetConstant(s.version).MetaDataBatchSize)
	length := make([]int64, 0, logpath.GetConstant(s.version).MetaDataBatchSize)
	minTime := s.ctx.tr.Min
	maxTime := s.ctx.tr.Max
	for i := currIter; i < metaDataNum; i += iterNum {
		if metaData[i].GetMaxTime() > maxTime {
			maxTime = metaData[i].GetMaxTime()
		}
		if metaData[i].GetMinTime() < minTime {
			minTime = metaData[i].GetMinTime()
		}
		offsets = append(offsets, metaData[i].GetContentBlockOffset())
		length = append(length, int64(metaData[i].GetContentBlockLength()))
	}
	var unnest *influxql.Unnest

	s.metaInfo = metaData
	s.dataReader, err = logstore.NewDataReader(s.option, s.logStoreSegmentPrefix, s.version, s.ctx.tr, s.ctx.querySchema.GetOptions(), offsets, length, s.ctx.schema.Copy(), currIter == 0, unnest, maxBlockID)
	if err != nil {
		return 0, err
	}
	s.dataReader.StartSpan(s.span)
	s.minTime, s.maxTime = minTime, maxTime
	return iterNum, nil
}

func (s *LogStoreAggCursor) getLogStoreAggCursorMetaBy() (int32, []*logstore.MetaData, int64, error) {
	var m *logstore.IndexReader
	var metaData []*logstore.MetaData
	var err error
	queryId := s.ctx.querySchema.GetOptions().GetLogQueryCurrId()
	currIter := s.ctx.querySchema.GetOptions().GetIterId()
	metaDataBatchSize := logpath.GetConstant(s.version).MetaDataBatchSize
	var iterNum int32
	maxBlockID := int64(-1)
	if currIter != 0 {
		a, ok := logstore.GetMetaData(queryId + "_" + s.logStoreSegmentPrefix)
		if a == nil || !ok {
			m, err = logstore.NewIndexReader(s.option, s.version, s.logStoreSegmentPrefix, s.ctx.tr, s.ctx.querySchema.GetOptions())
			if err != nil {
				logger.GetLogger().Error(err.Error(), zap.Error(errno.NewError(errno.FailedPutNodeMaxIterNum, queryId)))
				return 0, nil, maxBlockID, err
			}
			m.StartSpan(s.span)
			metaData, err = m.Get()
			m.Close()
			if err != nil {
				logger.GetLogger().Error(err.Error(), zap.Error(errno.NewError(errno.FailedPutNodeMaxIterNum, queryId)))
				return 0, nil, maxBlockID, err
			}
			maxBlockID = m.GetMaxBlockId()
			iterNum = int32(math.Ceil(float64(len(metaData)) / float64(metaDataBatchSize)))
			cache.PutNodeIterNum(queryId, iterNum)
			logstore.PutMetaData(queryId+"_"+s.logStoreSegmentPrefix, metaData)
		} else {
			metaData = a
			iterNum = int32(math.Ceil(float64(len(metaData)) / float64(metaDataBatchSize)))
		}
	} else {
		m, err = logstore.NewIndexReader(s.option, s.version, s.logStoreSegmentPrefix, s.ctx.tr, s.ctx.querySchema.GetOptions())
		if err != nil {
			logger.GetLogger().Error(err.Error(), zap.Error(errno.NewError(errno.FailedPutNodeMaxIterNum, queryId)))
			return 0, nil, maxBlockID, err
		}
		m.StartSpan(s.span)
		metaData, err = m.Get()
		m.Close()
		if err != nil {
			logger.GetLogger().Error(err.Error(), zap.Error(errno.NewError(errno.FailedPutNodeMaxIterNum, queryId)))
			return 0, nil, maxBlockID, err
		}
		maxBlockID = m.GetMaxBlockId()
		iterNum = int32(math.Ceil(float64(len(metaData)) / float64(metaDataBatchSize)))
		if iterNum == 0 {
			iterNum = 1
		}
		cache.PutNodeIterNum(queryId, iterNum)
		logstore.PutMetaData(queryId+"_"+s.logStoreSegmentPrefix, metaData)
	}
	return iterNum, metaData, maxBlockID, nil
}

func (s *LogStoreAggCursor) SetOps(ops []*comm.CallOption) {
	s.ctx.decs.SetOps(ops)
}

func (s *LogStoreAggCursor) SetSchema(schema record.Schemas) {
	s.recordSchema = schema
}

func (s *LogStoreAggCursor) SinkPlan(plan hybridqp.QueryNode) {
	var schema record.Schemas
	s.ridIdx = make(map[int]struct{})
	ops := plan.RowExprOptions()
	// field

	for i, field := range s.ctx.schema[:s.ctx.schema.Len()-1] {
		var seen bool
		for _, expr := range ops {
			if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Val == field.Name {
				schema = append(schema, record.Field{Name: expr.Ref.Val, Type: record.ToModelTypes(expr.Ref.Type)})
				seen = true
				break
			}
		}
		if !seen && field.Name != record.TimeField {
			s.ridIdx[i] = struct{}{}
		}
	}

	// time
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	s.recordSchema = schema
	if len(s.recordSchema) != len(s.ctx.schema) {
		s.isCutSchema = true
	}
}

func (s *LogStoreAggCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return nil, &seriesInfo{}, nil
}

func (s *LogStoreAggCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if !s.isInit {
		s.isInit = true
		currIter := s.ctx.querySchema.GetOptions().GetIterId()
		if iterNum, err := s.init(); err != nil {
			return nil, nil, err
		} else if currIter >= iterNum {
			return nil, nil, nil
		}
	}
	r, err := s.dataReader.Next()
	if r == nil || err != nil {
		if s.span != nil {
			s.span.Count(logStoreAggCursorDuration, int64(time.Since(s.tm)))
		}
		return nil, nil, err
	}
	r.Schema = s.recordSchema
	return r, &comm.FileInfo{MinTime: s.minTime, MaxTime: s.maxTime}, nil
}

func (s *LogStoreAggCursor) Name() string {
	return "LogStoreAggCursor"
}

func (s *LogStoreAggCursor) Close() error {
	if s.dataReader != nil {
		s.dataReader.Close()
	}
	return nil
}

func (s *LogStoreAggCursor) GetSchema() record.Schemas {
	if s.recordSchema == nil {
		s.recordSchema = s.ctx.schema
	}
	return s.recordSchema
}

func (s *LogStoreAggCursor) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.tm = time.Now()
	s.span = span
	s.span.CreateCounter(logStoreAggCursorDuration, "ns")
}

func (s *LogStoreAggCursor) EndSpan() {
}
