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

package netstorage

import (
	"context"
	"sort"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type NewEngineFun func(dataPath, walPath string, options EngineOptions, ctx *metaclient.LoadCtx) (Engine, error)

var engines = make(map[string]NewEngineFun)

func RegisterNewEngineFun(name string, fn NewEngineFun) {
	if _, ok := engines[name]; ok {
		return
	}
	engines[name] = fn
}

func RegisteredEngines() []string {
	a := make([]string, 0, len(engines))
	for k := range engines {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

func GetNewEngineFunction(entType string) NewEngineFun {
	fn, ok := engines[entType]
	if ok {
		return fn
	}
	return nil
}

type Engine interface {
	Open(ptIds []uint32, durationInfos map[uint64]*meta.ShardDurationInfo) error
	Close() error
	ForceFlush()
	SetReadOnly(readonly bool)

	DeleteShard(db string, ptId uint32, shardID uint64) error
	DeleteIndex(db string, pt uint32, shardID uint64) error
	ExpiredShards() []*meta.ShardIdentifier
	ExpiredIndexes() []*meta.IndexIdentifier

	FetchShardsNeedChangeStore() ([]*meta.ShardIdentifier, []*meta.ShardIdentifier)
	ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error

	CreateShard(db, rp string, ptId uint32, shardID uint64, timeRangeInfo *meta.ShardTimeRangeInfo) error
	WriteRows(db, rp string, ptId uint32, shardID uint64, points []influx.Row, binaryRows []byte) error
	CreateDBPT(db string, pt uint32)

	GetShardSplitPoints(db string, ptId uint32, shardID uint64, idxes []int64) ([]string, error)

	DeleteDatabase(db string, ptId uint32) error

	DropRetentionPolicy(db string, rp string, ptId uint32) error

	DropMeasurement(db string, rp string, name string, shardIds []uint64) error

	SeriesKeys(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) ([]string, error)
	SeriesCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) ([]meta.MeasurementCardinalityInfo, error)
	SeriesExactCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) (map[string]uint64, error)

	TagValues(db string, ptId []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (TablesTagSets, error)
	TagValuesCardinality(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (map[string]uint64, error)
	DropSeries(database string, sources []influxql.Source, ptId []uint32, condition influxql.Expr) (int, error)

	DbPTRef(db string, ptId uint32) error
	DbPTUnref(db string, ptId uint32)
	CreateLogicalPlan(ctx context.Context, db string, ptId uint32, shardID uint64, sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error)
	LogicalPlanCost(db string, ptId uint32, sources influxql.Sources, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error)

	UpdateShardDurationInfo(info *meta.ShardDurationInfo) error

	SysCtrl(req *SysCtrlRequest) error
	Statistics(buffer []byte) ([]byte, error)
}
