// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package coordinator

import (
	"errors"

	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type DropSeriesExecutor struct {
	logger *logger.Logger
	mc     meta.MetaClient
	me     IMetaExecutor
	store  netstorage.Storage
}

func NewDropSeriesExecutor(logger *logger.Logger, mc meta.MetaClient, me IMetaExecutor, store netstorage.Storage) *DropSeriesExecutor {
	return &DropSeriesExecutor{
		logger: logger,
		mc:     mc,
		me:     me,
		store:  store,
	}
}

func (e *DropSeriesExecutor) Execute(stmt *influxql.DropSeriesStatement, database string) error {
	if len(stmt.Sources) != 1 {
		return errors.New("there must be and can only be one table")
	}
	mis, err := e.mc.MatchMeasurements(database, stmt.Sources.Measurements())
	if err != nil {
		return err
	}
	if len(mis) == 0 {
		return errors.New("the current input table does not exist")
	}
	names := make([]string, 0, len(mis))
	for _, m := range mis {
		names = append(names, m.Name)
	}

	err = e.me.EachDBNodes(database, func(nodeID uint64, pts []uint32) error {
		err = e.store.DropSeries(nodeID, database, pts, names, stmt.Condition)
		return err
	})
	return err
}
