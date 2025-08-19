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

package writer

import (
	"errors"
	"slices"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type MetaClient interface {
	Database(name string) (*meta.DatabaseInfo, error)
	CreateShardGroup(db, rp string, tm time.Time, version uint32, eng config.EngineType) (*meta.ShardGroupInfo, error)
	DBPtView(db string) (meta.DBPtInfos, error)
	UpdateSchema(db string, rp string, mst string, schema []*proto.FieldSchema) error
	GetAliveShards(db string, sgi *meta.ShardGroupInfo, isRead bool) []int
	Measurement(db string, rpName string, mstName string) (*meta.MeasurementInfo, error)
	SimpleCreateMeasurement(db, rp, mst string, engineType config.EngineType) (*meta.MeasurementInfo, error)
}

type WriteContext struct {
	mapper *RecordMapper
	meta   *MetaManager
	err    *WriteError

	indexKey []byte
}

var writeContextPool = pool.NewDefaultUnionPool[WriteContext](func() *WriteContext {
	return &WriteContext{
		mapper: NewRecordMapper(),
		meta:   NewMetaManager(),
		err:    &WriteError{},
	}
})

func NewWriteContext(mc MetaClient, db, rp string) (*WriteContext, func()) {
	ctx := writeContextPool.Get()
	ctx.meta.Init(mc, db, rp)

	return ctx, func() {
		ctx.mapper.Reset()
		ctx.meta.Reset()
		ctx.err.Clean()
		writeContextPool.PutWithMemSize(ctx, 0)
	}
}

func (w WriteContext) Err() *WriteError {
	return w.err
}

func (w WriteContext) Meta() *MetaManager {
	return w.meta
}

type RecordMapper struct {
	alc *pool.Allocator[uint64, shelf.BlobGroup]
}

func NewRecordMapper() *RecordMapper {
	return &RecordMapper{
		alc: pool.NewAllocator[uint64, shelf.BlobGroup](),
	}
}

func (m *RecordMapper) Walk(call func(id uint64, group *shelf.BlobGroup)) {
	for id, group := range m.alc.ValMap() {
		call(id, group)
	}
}

func (m *RecordMapper) Reset() {
	m.alc.Reset()
}

func (m *RecordMapper) MapRecord(id uint64, mst string, indexKey []byte, fields *record.Record, rowIdx int) {
	group, ok := m.alc.MapAlloc(id)
	if !ok {
		group.Init(config.GetShelfMode().Concurrent)
	}
	group.GroupingRow(mst, indexKey, fields, rowIdx)
}

type WriteError struct {
	partialErr error
	dropped    int
}

func (e *WriteError) Assert(err error, dropped int) error {
	if err == nil {
		return nil
	}

	if errno.Equal(err, errno.InvalidMeasurement) {
		logger.GetLogger().Error("invalid measurement", zap.Error(err))
		e.partialErr = err
		e.dropped += dropped
		return nil
	}

	if errno.Equal(err, errno.ErrorTagArrayFormat, errno.WriteErrorArray, errno.SeriesLimited) {
		e.partialErr = err
		return nil
	}

	return err
}

func (e *WriteError) AddDropRowError(err error) {
	e.dropped++
	if e.partialErr == nil {
		e.partialErr = err
	}
}

func (e *WriteError) Error() error {
	if e.partialErr != nil {
		return msgservice.PartialWriteError{Reason: e.partialErr, Dropped: e.dropped}
	}

	return nil
}

func (e *WriteError) Clean() {
	e.partialErr = nil
	e.dropped = 0
}

func (e *WriteError) PartialErr() error {
	return e.partialErr
}

type MetaManager struct {
	mc      MetaClient
	db      string
	rp      string
	minTime int64

	shards      map[uint64]*meta.ShardInfo
	shardGroups []*meta.ShardGroupInfo
	sg          *meta.ShardGroupInfo
	di          *meta.DatabaseInfo
	mi          *meta.MeasurementInfo
	si          *meta.ShardKeyInfo
	aliveShards []int
	schema      []*proto.FieldSchema
}

func NewMetaManager() *MetaManager {
	return &MetaManager{
		shards: make(map[uint64]*meta.ShardInfo),
	}
}

func (m *MetaManager) Init(mc MetaClient, db, rp string) {
	m.mc = mc
	m.db = db
	m.rp = rp
}

func (m *MetaManager) Reset() {
	clear(m.shards)
	m.shardGroups = m.shardGroups[:0]
	m.sg = nil
	m.aliveShards = m.aliveShards[:0]
	m.schema = m.schema[:0]
}

func (m *MetaManager) Shards() map[uint64]*meta.ShardInfo {
	return m.shards
}

func (m *MetaManager) CheckDBRP() error {
	// check db and rp validation
	dbInfo, err := m.mc.Database(m.db)
	if err != nil {
		return err
	}
	m.di = dbInfo

	if m.rp == "" {
		m.rp = dbInfo.DefaultRetentionPolicy
	}

	rpInfo, err := dbInfo.GetRetentionPolicy(m.rp)
	if err != nil {
		return err
	}

	if rpInfo.Duration > 0 {
		m.minTime = int64(fasttime.UnixTimestamp()*1e9) - rpInfo.Duration.Nanoseconds()
	}
	return nil
}

func (m *MetaManager) CreateShardGroupIfNeeded(times []int64) error {
	var lastShardGroup *meta.ShardGroupInfo
	slices.Sort(times)

	for i := range times {
		tm := time.Unix(0, times[i])
		if lastShardGroup != nil && lastShardGroup.Contains(tm) {
			continue
		}

		sg, err := m.mc.CreateShardGroup(m.db, m.rp, tm, 0, config.TSSTORE)
		if err != nil {
			return err
		}
		lastShardGroup = sg
		m.shardGroups = append(m.shardGroups, sg)
	}

	return nil
}

func (m *MetaManager) resetActiveShardGroup(ts int64) bool {
	if m.sg != nil && len(m.shardGroups) == 1 {
		return true
	}

	tm := time.Unix(0, ts)
	if m.sg != nil && m.sg.Contains(tm) {
		return true
	}

	for i := range m.shardGroups {
		if m.shardGroups[i].Contains(tm) {
			m.sg = m.shardGroups[i]
			m.aliveShards = m.mc.GetAliveShards(m.db, m.sg, false)
			break
		}
	}
	return false
}

func (m *MetaManager) UpdateSchemaIfNeeded(rec *record.Record, mst *meta.MeasurementInfo, originName string) error {
	schemaToCreate := m.updateSchemaCheck(rec, mst, originName)

	if len(schemaToCreate) == 0 {
		return nil
	}

	start := time.Now()
	err := m.mc.UpdateSchema(m.db, m.rp, originName, schemaToCreate)
	statistics.NewHandler().WriteUpdateSchemaDuration.AddSinceNano(start)

	return err
}

func (m *MetaManager) updateSchemaCheck(rec *record.Record, mst *meta.MeasurementInfo, originName string) []*proto.FieldSchema {
	m.schema = m.schema[:0]
	schemaMap := mst.Schema

	var dropIndex []int
	for i := range rec.Len() - 1 {
		schema := &rec.Schema[i]
		typ, ok := schemaMap.GetTyp(schema.Name)
		if !ok {
			// new field or tag
			m.schema = appendField(m.schema, schema.Name, int32(schema.Type))
			continue
		}

		// type conflict
		if int(typ) != schema.Type {
			dropIndex = append(dropIndex, i)
			err := errno.NewError(errno.FieldTypeConflict, schema.Name, originName,
				influx.FieldTypeString(int32(schema.Type)),
				influx.FieldTypeString(typ)).SetModule(errno.ModuleWrite)
			logger.GetLogger().Error("field type conflict", zap.Error(err))
		}
	}

	if len(dropIndex) > 0 {
		record.DropColByIndex(rec, dropIndex)
	}
	return m.schema
}

func (m *MetaManager) CreateMeasurement(name string, engineType config.EngineType) (*meta.MeasurementInfo, error) {
	start := time.Now()
	defer func() {
		statistics.NewHandler().WriteCreateMstDuration.AddSinceNano(start)
	}()

	mst, err := m.mc.Measurement(m.db, m.rp, name)
	if errors.Is(err, meta.ErrMeasurementNotFound) {
		mst, err = m.mc.SimpleCreateMeasurement(m.db, m.rp, name, engineType)
	}

	return mst, err
}

func (m *MetaManager) GetShardKeyAndGroupInfo(sameMst bool, ts int64) (*meta.ShardKeyInfo, *meta.ShardGroupInfo, []int) {
	sameSg := m.resetActiveShardGroup(ts)

	// The si can be reused when both the same shardGroup and the same mst
	if sameSg && sameMst {
		return m.si, m.sg, m.aliveShards
	}

	// the priority of database-level settings is higher than that of table-level settings.
	if len(m.di.ShardKey.ShardKey) > 0 {
		m.si = &m.di.ShardKey
	} else if len(m.mi.ShardKeys) > 0 {
		m.si = m.mi.GetShardKey(m.sg.ID)
	}
	return m.si, m.sg, m.aliveShards
}

func (m *MetaManager) ResetMeasurementInfos(mst string) error {
	originName := influx.GetOriginMstName(mst)
	mi, err := m.mc.Measurement(m.db, m.rp, originName)
	if err != nil {
		return errno.NewError(errno.ErrMeasurementNotFound)
	}
	m.mi = mi
	// si need to be reset.The si of the previous table and the current table may differ
	m.si = nil
	return nil
}

func (m *MetaManager) ResetDatabaseInfo() error {
	dbInfo, err := m.mc.Database(m.db)
	if err != nil {
		return err
	}
	m.di = dbInfo
	return nil
}
