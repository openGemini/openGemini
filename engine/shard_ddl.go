// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/immutable/tsreader"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/index"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

func (s *shard) CreateDDLBasePlan(client metaclient.MetaClient, ddl hybridqp.DDLType) immutable.DDLBasePlan {
	idx := s.getMergeIndex()
	if idx == nil {
		return nil
	}

	return immutable.NewDDLBasePlan(s.immTables, idx, s.log, s, client, ddl)
}

func (s *shard) CreateConsumeIterator(mst string, opts *query.ProcessorOptions) record.Iterator {
	s.consumeWg.Add(1)
	defer s.consumeWg.Done()
	msInfo, ok := colstore.MstManagerIns().Get(s.ident.OwnerDb, s.ident.Policy, mst)
	if ok && msInfo != nil {
		return s.createCSConsumeIterator(msInfo, opts)
	}

	return s.createTSConsumeIterator(mst, opts)
}

func (s *shard) createTSConsumeIterator(mst string, opts *query.ProcessorOptions) record.Iterator {
	idx := s.getMergeIndex()
	if idx == nil {
		return nil
	}

	// When only time conditions are present or no conditions are present, the full-scan mode will be adopted.
	var seriesItr index.SeriesIDIterator
	var err error
	if opts.Condition != nil {
		seriesItr, err = idx.SearchSeriesIterator(nil, []byte(mst), opts)
		if err != nil {
			logger.GetLogger().Error("failed to search series", zap.Error(err))
			return nil
		}
		if seriesItr.Ids().Len() == 0 {
			return nil
		}
	}

	consumeOptions, err := tsreader.NewConsumeOptions(opts, seriesItr)
	if err != nil {
		logger.GetLogger().Error("failed to create consumeOptions", zap.Error(err))
		if seriesItr != nil {
			seriesItr.Close()
		}
		return nil
	}

	tr := util.TimeRange{
		Min: opts.GetStartTime(),
		Max: opts.GetEndTime(),
	}

	itr := tsreader.NewConsumeIterator(consumeOptions, s.stopConsume.C())
	itr.SetIndex(idx)

	order, unordered, _ := s.immTables.GetBothFilesRef(mst, true, tr, nil)
	if len(order) == 0 && len(unordered) == 0 {
		if seriesItr != nil {
			seriesItr.Close()
		}
		return nil
	}
	for _, f := range order {
		itr.AddReader(true, f)
	}
	for _, f := range unordered {
		itr.AddReader(false, f)
	}

	if !opts.HaveCall() {
		if opts.GetLimit()+opts.GetOffset() > 0 {
			return s.createTSLimitIterator(itr, opts)
		}
		return itr
	}
	return s.createTSAggIterator(itr, opts, consumeOptions)
}

func (s *shard) createTSLimitIterator(itr record.Iterator, opts *query.ProcessorOptions) record.Iterator {
	querySchema := executor.NewQuerySchema(nil, nil, opts, nil)
	limCursor := NewLimitCursor(querySchema, RecordCutNormal)
	limCursor.SetCursor(comm.NewWrapCursor(itr))
	iterator := comm.NewWrapIterator(limCursor, itr.SidCnt())
	return iterator
}

func (s *shard) createTSAggIterator(itr record.Iterator, opts *query.ProcessorOptions, consumeOptions *tsreader.ConsumeOptions) record.Iterator {
	exprOpt := make([]hybridqp.ExprOptions, 0, len(opts.Exprs))
	for i := 0; i < len(opts.Exprs); i++ {
		exprOpt = append(exprOpt, hybridqp.ExprOptions{
			Expr: opts.Exprs[i],
			Ref:  opts.FieldAux[i],
		})
	}
	schema := consumeOptions.Schema()
	querySchema := executor.NewQuerySchema(nil, nil, opts, nil)
	aggCursor := NewAggregateCursor(comm.NewWrapCursor(itr), querySchema, AggPool, false)
	aggCursor.SetSchema(schema, schema, exprOpt)
	iterator := comm.NewWrapIterator(aggCursor, itr.SidCnt())
	return iterator
}

func (s *shard) createCSConsumeIterator(mst *colstore.Measurement, opts *query.ProcessorOptions) record.Iterator {
	files, ok := s.immTables.GetCSFiles(mst.MeasurementInfo().Name)
	if !ok {
		return nil
	}

	var its []*immutable.CSFileIterator
	for _, f := range files.Copy() {
		it, err := immutable.NewCSFileIterator(f, mst.TCDuration(), mst.IsClusterPKIndex())
		if err != nil {
			logger.GetLogger().Error("failed to create cs file iterator", zap.Error(err))
			continue
		}

		its = append(its, it)
	}

	return immutable.NewCSConsumeIterator(its)
}

func (s *shard) DisableConsume() {
	s.stopConsume.Close()
	s.consumeWg.Wait()
}

func (s *shard) EnableConsume() {
	s.stopConsume.ReOpen()
}
