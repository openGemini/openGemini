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
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
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

	itr := tsreader.NewConsumeIterator(consumeOptions)
	itr.SetIndex(idx)

	tr := util.TimeRange{
		Min: opts.GetStartTime(),
		Max: opts.GetEndTime(),
	}
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
	return itr
}
