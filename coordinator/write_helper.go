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

package coordinator

import (
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const NotInShardDuration = -1

var tagLimit = 0
var fieldLimit = 0

func SetTagLimit(limit int) {
	if limit > 0 {
		tagLimit = limit
	}
}

func SetFieldLimit(limit int) {
	if limit > 0 {
		fieldLimit = limit
	}
}

func GetTagLimit() int {
	return tagLimit
}

func GetFieldLimit() int {
	return fieldLimit
}

var AsyncSchemaEndtimeUpdateEn bool
var SchemaEndtimeUpdateManager *AsyncSchemaEndtimeUpdateManager

const defaultSchemaEndtimeUpdateSize = 1024

type AsyncSchemaEndtimeUpdateManager struct {
	cmds       chan *proto2.UpdateSchemaCommand
	MetaClient PWMetaClient
	closing    chan struct{}
	size       int
	once       sync.Once
}

func NewAsyncSchemaEndtimeUpdateManager(size int) *AsyncSchemaEndtimeUpdateManager {
	if size <= 0 {
		size = defaultSchemaEndtimeUpdateSize
	}

	return &AsyncSchemaEndtimeUpdateManager{
		cmds:    make(chan *proto2.UpdateSchemaCommand, size),
		closing: make(chan struct{}),
		size:    size,
	}
}

func (m *AsyncSchemaEndtimeUpdateManager) start(i int) {
	logger.GetLogger().Info("AsyncSchemaEndtimeUpdateManager start", zap.Int("seq", i), zap.Int("size", m.size))
	for {
		select {
		case cmd, ok := <-m.cmds:
			if ok {
				if err := m.MetaClient.UpdateSchemaByCmd(cmd); err != nil {
					logger.GetLogger().Error("async update schema endtime fail", zap.Int("seq", i), zap.String("db", *cmd.Database),
						zap.String("rp", *cmd.RpName), zap.String("mst", *cmd.Measurement), zap.Error(err))
				}
			} else {
				logger.GetLogger().Info("AsyncSchemaEndtimeUpdateManager closed", zap.Int("seq", i))
				return
			}
		case <-m.closing:
			m.once.Do(func() {
				close(m.cmds)
			})
			logger.GetLogger().Info("AsyncSchemaEndtimeUpdateManager closed", zap.Int("seq", i))
			return
		}

	}
}

func (m *AsyncSchemaEndtimeUpdateManager) Stop() {
	close(m.closing)
}

func (m *AsyncSchemaEndtimeUpdateManager) Put(database string, retentionPolicy string, mst string, fieldToUpdateEndTime []*proto2.FieldSchema) {
	cmd := &proto2.UpdateSchemaCommand{
		Database:      proto.String(database),
		RpName:        proto.String(retentionPolicy),
		Measurement:   proto.String(mst),
		FieldToCreate: fieldToUpdateEndTime,
	}
	m.cmds <- cmd
}

func InitAsyncSchemaEndtimeUpdateEn(en bool, size int, concurrency int) {
	AsyncSchemaEndtimeUpdateEn = en
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > runtime.NumCPU() {
		concurrency = runtime.NumCPU()
	}
	logger.GetLogger().Info("InitAsyncSchemaEndtimeUpdateEn", zap.Int("concurrency", concurrency), zap.Bool("en", en), zap.Int("size", size))
	if en {
		SchemaEndtimeUpdateManager = NewAsyncSchemaEndtimeUpdateManager(size)
		for i := 0; i < concurrency; i++ {
			go SchemaEndtimeUpdateManager.start(i)
		}
	}
}

type writeHelper struct {
	pw         *PointsWriter
	sameSchema bool
	sameSg     bool
	sameMst    bool
	preSg      *meta2.ShardGroupInfo
	preMst     *meta2.MeasurementInfo

	mstPrimaryKeyRowMap map[string]map[string]struct{} //check if tags and fields of rows contain primary keys for column store
	pkLength            int                            // column store primary key length not included "time"
}

func newWriteHelper(pw *PointsWriter) *writeHelper {
	return &writeHelper{
		pw: pw,
	}
}

// The table names of the same batch of data may be the same.
// Caches table information in the previous row to accelerate table information query.
func (wh *writeHelper) createMeasurement(database, retentionPolicy, name string, skipPreCheck bool) (*meta2.MeasurementInfo, error) {
	if skipPreCheck {
		return createMeasurementBase(database, retentionPolicy, name, wh.pw.MetaClient, config.TSSTORE)
	}
	return createMeasurement(database, retentionPolicy, name, wh.pw.MetaClient, &wh.preMst, &wh.sameSchema, config.TSSTORE)
}

func (wh *writeHelper) sameMeasurement(name string) {
	if wh.preMst == nil {
		wh.sameMst = false
		return
	}
	wh.sameMst = wh.preMst.OriginName() == name
}

func (wh *writeHelper) GetSgEndTime(database, rp string, ts int64, engineType config.EngineType) (int64, error) {
	return wh.pw.MetaClient.GetSgEndTime(database, rp, time.Unix(0, ts), engineType)
}

func (wh *writeHelper) updateCleanSchemaTagsCheck(r *influx.Row, dropTagIndex *[]int, pkCount *int, originName string,
	sgEndTime int64, fieldToCreatePool []*proto2.FieldSchema, fieldToUpdateEndTime []*proto2.FieldSchema,
	cleanSchema *meta2.CleanSchema, mst *meta2.MeasurementInfo, err *error) ([]*proto2.FieldSchema, []*proto2.FieldSchema, bool, error) {
	for i, tag := range r.Tags {
		if tag.Key == "time" {
			*dropTagIndex = append(*dropTagIndex, i)
			*err = errno.NewError(errno.InvalidTagKey, originName)
			continue
		}
		if err := r.CheckDuplicateTag(i); err != nil {
			return fieldToUpdateEndTime, fieldToCreatePool, true, err
		}
		if schemaVal, ok := (*cleanSchema)[tag.Key]; !ok {
			fieldToCreatePool = appendField(fieldToCreatePool, tag.Key, influx.Field_Type_Tag)
			setLastFieldEndTime(meta2.TimeReserveHigh32(sgEndTime), fieldToCreatePool)
		} else {
			endTime := meta2.TimeReserveHigh32(sgEndTime)
			if schemaVal.EndTime < endTime {
				if AsyncSchemaEndtimeUpdateEn {
					fieldToUpdateEndTime = appendField(fieldToUpdateEndTime, tag.Key, influx.Field_Type_Tag)
					setLastFieldEndTime(endTime, fieldToUpdateEndTime)
				} else {
					fieldToCreatePool = appendField(fieldToCreatePool, tag.Key, influx.Field_Type_Tag)
					setLastFieldEndTime(endTime, fieldToCreatePool)
				}
			}
			if mst.EngineType == config.COLUMNSTORE {
				if schemaVal.Typ != influx.Field_Type_Tag {
					return fieldToUpdateEndTime, fieldToCreatePool, true, errno.NewError(errno.WritePointHasInvalidTag, tag.Key)
				}
				m := wh.mstPrimaryKeyRowMap[r.Name]
				if _, exist := m[tag.Key]; exist {
					(*pkCount)++
				}
			}
		}
	}
	return fieldToUpdateEndTime, fieldToCreatePool, false, nil
}

func (wh *writeHelper) updateCleanSchemaFieldsCheck(r *influx.Row, dropFieldIndex *[]int, pkCount *int, originName string,
	sgEndTime int64, fieldToCreatePool []*proto2.FieldSchema, fieldToUpdateEndTime []*proto2.FieldSchema, cleanSchema *meta2.CleanSchema, mst *meta2.MeasurementInfo,
	err *error) ([]*proto2.FieldSchema, []*proto2.FieldSchema, bool, error) {
	for i, field := range r.Fields {
		schemaVal, ok := (*cleanSchema)[field.Key]
		if ok {
			if int32(schemaVal.Typ) != field.Type {
				failpoint.Inject("skip-field-type-conflict", func(val failpoint.Value) {
					if strings2.EqualInterface(val, field.Key) {
						failpoint.Continue()
					}
				})
				if mst.EngineType == config.COLUMNSTORE && schemaVal.Typ == influx.Field_Type_Tag {
					return fieldToUpdateEndTime, fieldToCreatePool, true, errno.NewError(errno.WritePointHasInvalidField, field.Key)
				}
				*err = errno.NewError(errno.FieldTypeConflict, field.Key, originName, influx.FieldTypeString(field.Type),
					influx.FieldTypeString(int32(schemaVal.Typ))).SetModule(errno.ModuleWrite)
				*dropFieldIndex = append(*dropFieldIndex, i)
			}
			if mst.EngineType == config.COLUMNSTORE {
				m := wh.mstPrimaryKeyRowMap[r.Name]
				if _, exist := m[field.Key]; exist {
					(*pkCount)++
				}
			}
			endTime := meta2.TimeReserveHigh32(sgEndTime)
			if schemaVal.EndTime < endTime {
				if AsyncSchemaEndtimeUpdateEn {
					fieldToUpdateEndTime = appendField(fieldToUpdateEndTime, field.Key, field.Type)
					setLastFieldEndTime(endTime, fieldToUpdateEndTime)
				}
				fieldToCreatePool = appendField(fieldToCreatePool, field.Key, field.Type)
				setLastFieldEndTime(endTime, fieldToCreatePool)
			}
			continue
		}
		fieldToCreatePool = appendField(fieldToCreatePool, field.Key, field.Type)
		setLastFieldEndTime(meta2.TimeReserveHigh32(sgEndTime), fieldToCreatePool)
	}
	return fieldToUpdateEndTime, fieldToCreatePool, false, nil
}

// only use for SchemaCleanEn = true
func (wh *writeHelper) updateCleanSchemaCheck(database, rp string, r *influx.Row, mst *meta2.MeasurementInfo,
	originName string, fieldToCreatePool []*proto2.FieldSchema, sgEndTime int64,
	fieldToUpdateEndTime []*proto2.FieldSchema) ([]*proto2.FieldSchema, []*proto2.FieldSchema, bool, error) {
	schemaMap := mst.Schema
	var dropTagIndex []int
	var err error
	var subErr error
	var pkCount int
	var flag bool
	fieldToUpdateEndTime, fieldToCreatePool, flag, subErr = wh.updateCleanSchemaTagsCheck(r, &dropTagIndex, &pkCount,
		originName, sgEndTime, fieldToCreatePool, fieldToUpdateEndTime, schemaMap, mst, &err)
	if flag {
		return fieldToUpdateEndTime, fieldToCreatePool, flag, subErr
	}

	if len(dropTagIndex) > 0 {
		dropTagByIndex(r, dropTagIndex)
	}

	if tl := GetTagLimit(); tl > 0 && len(fieldToCreatePool) > 0 && mst.TagKeysTotal() > tl {
		return fieldToUpdateEndTime, fieldToCreatePool, true, errno.NewError(errno.TooManyTagKeys)
	}

	if tl := GetFieldLimit(); tl > 0 && len(fieldToCreatePool) > 0 && mst.FieldKeysTotal() > tl {
		return fieldToUpdateEndTime, fieldToCreatePool, true, errno.NewError(errno.TooManyFieldKeys)
	}

	// check field type is conflict or not
	var dropFieldIndex []int
	fieldToUpdateEndTime, fieldToCreatePool, flag, subErr = wh.updateCleanSchemaFieldsCheck(r, &dropFieldIndex, &pkCount,
		originName, sgEndTime, fieldToCreatePool, fieldToUpdateEndTime, schemaMap, mst, &err)
	if flag {
		return fieldToUpdateEndTime, fieldToCreatePool, flag, subErr
	}

	if mst.EngineType == config.COLUMNSTORE && pkCount != wh.pkLength {
		return fieldToUpdateEndTime, fieldToCreatePool, true, errno.NewError(errno.WritePointPrimaryKeyErr, originName,
			len(mst.ColStoreInfo.PrimaryKey), pkCount)
	}

	if len(dropFieldIndex) > 0 {
		dropFieldByIndex(r, dropFieldIndex)
		if len(r.Fields) == 0 {
			return fieldToUpdateEndTime, fieldToCreatePool, true, err
		}
	}
	return fieldToUpdateEndTime, fieldToCreatePool, false, err
}

func (wh *writeHelper) updateSchemaCheck(database, rp string, r *influx.Row, mst *meta2.MeasurementInfo,
	originName string, fieldToCreatePool []*proto2.FieldSchema) ([]*proto2.FieldSchema, bool, error) {
	schemaMap := mst.Schema
	var dropTagIndex []int
	var err error
	var pkCount int

	// check tag need to add or not
	for i, tag := range r.Tags {
		if tag.Key == "time" {
			dropTagIndex = append(dropTagIndex, i)
			err = errno.NewError(errno.InvalidTagKey, originName)
			continue
		}
		if err := r.CheckDuplicateTag(i); err != nil {
			return fieldToCreatePool, true, err
		}

		if _, ok := schemaMap.GetTyp(tag.Key); !ok {
			fieldToCreatePool = appendField(fieldToCreatePool, tag.Key, influx.Field_Type_Tag)
			continue
		}
		if mst.EngineType == config.COLUMNSTORE {
			v, _ := schemaMap.GetTyp(tag.Key)
			if v != influx.Field_Type_Tag {
				return fieldToCreatePool, true, errno.NewError(errno.WritePointHasInvalidTag, tag.Key)
			}
			m := wh.mstPrimaryKeyRowMap[r.Name]
			if _, exist := m[tag.Key]; exist {
				pkCount++
			}
		}
	}

	if len(dropTagIndex) > 0 {
		dropTagByIndex(r, dropTagIndex)
	}

	if tl := GetTagLimit(); tl > 0 && len(fieldToCreatePool) > 0 && mst.TagKeysTotal() > tl {
		return fieldToCreatePool, true, errno.NewError(errno.TooManyTagKeys)
	}

	if tl := GetFieldLimit(); tl > 0 && len(fieldToCreatePool) > 0 && mst.FieldKeysTotal() > tl {
		return fieldToCreatePool, true, errno.NewError(errno.TooManyFieldKeys)
	}

	// check field type is conflict or not
	var dropFieldIndex []int
	for i, field := range r.Fields {
		fieldType, ok := schemaMap.GetTyp(field.Key)
		if ok {
			if fieldType != field.Type {
				failpoint.Inject("skip-field-type-conflict", func(val failpoint.Value) {
					if strings2.EqualInterface(val, field.Key) {
						failpoint.Continue()
					}
				})
				if mst.EngineType == config.COLUMNSTORE && fieldType == influx.Field_Type_Tag {
					return fieldToCreatePool, true, errno.NewError(errno.WritePointHasInvalidField, field.Key)
				}
				err = errno.NewError(errno.FieldTypeConflict, field.Key, originName, influx.FieldTypeString(field.Type),
					influx.FieldTypeString(fieldType)).SetModule(errno.ModuleWrite)
				dropFieldIndex = append(dropFieldIndex, i)
			}
			if mst.EngineType == config.COLUMNSTORE {
				m := wh.mstPrimaryKeyRowMap[r.Name]
				if _, exist := m[field.Key]; exist {
					pkCount++
				}
			}
			continue
		}
		fieldToCreatePool = appendField(fieldToCreatePool, field.Key, field.Type)
	}

	if mst.EngineType == config.COLUMNSTORE && pkCount != wh.pkLength {
		return fieldToCreatePool, true, errno.NewError(errno.WritePointPrimaryKeyErr, originName, len(mst.ColStoreInfo.PrimaryKey), pkCount)
	}

	if len(dropFieldIndex) > 0 {
		dropFieldByIndex(r, dropFieldIndex)
		if len(r.Fields) == 0 {
			return fieldToCreatePool, true, err
		}
	}
	return fieldToCreatePool, false, err
}

func (wh *writeHelper) updateSchemaCheckBase(database, rp string, r *influx.Row, mst *meta2.MeasurementInfo,
	originName string, fieldToCreatePool []*proto2.FieldSchema, fieldToUpdateEndTime []*proto2.FieldSchema) ([]*proto2.FieldSchema,
	[]*proto2.FieldSchema, bool, error) {
	if meta2.SchemaCleanEn {
		// GetSgEndTime will Rlock metadata. Pay attention when the schemalock is Rlocked to avoid deadlock.
		sgEndTime, err := wh.GetSgEndTime(database, rp, r.Timestamp, mst.EngineType)
		if err != nil {
			return fieldToUpdateEndTime, fieldToCreatePool, true, err
		}
		mst.SchemaLock.RLock()
		defer mst.SchemaLock.RUnlock()
		return wh.updateCleanSchemaCheck(database, rp, r, mst, originName, fieldToCreatePool, sgEndTime, fieldToUpdateEndTime)
	}
	mst.SchemaLock.RLock()
	defer mst.SchemaLock.RUnlock()
	var isDropRow bool
	var err error
	fieldToCreatePool, isDropRow, err = wh.updateSchemaCheck(database, rp, r, mst, originName, fieldToCreatePool)
	return fieldToUpdateEndTime, fieldToCreatePool, isDropRow, err
}

func (wh *writeHelper) updateSchemaIfNeeded(database, rp string, r *influx.Row, mst *meta2.MeasurementInfo,
	originName string, fieldToCreatePool []*proto2.FieldSchema) ([]*proto2.FieldSchema, bool, error) {
	// update schema if needed
	var err error
	var isDropRow bool
	var fieldToUpdateEndTime []*proto2.FieldSchema
	if fieldToUpdateEndTime, fieldToCreatePool, isDropRow, err = wh.updateSchemaCheckBase(database, rp, r, mst, originName,
		fieldToCreatePool, fieldToUpdateEndTime); err != nil {
		if isDropRow {
			return fieldToCreatePool, isDropRow, err
		}
	}

	if len(fieldToCreatePool) > 0 {
		start := time.Now()
		if errInner := wh.pw.MetaClient.UpdateSchema(database, rp, originName, fieldToCreatePool); errInner != nil {
			return fieldToCreatePool, strings.Contains(errInner.Error(), "field type conflict"), errInner
		}
		statistics.NewHandler().WriteUpdateSchemaDuration.AddSinceNano(start)
		wh.sameSchema = false
	}
	if len(fieldToUpdateEndTime) > 0 {
		start := time.Now()
		SchemaEndtimeUpdateManager.Put(database, rp, originName, fieldToUpdateEndTime)
		statistics.NewHandler().WriteUpdateSchemaEndTimeDuration.Add(time.Since(start).Nanoseconds())
	}
	return fieldToCreatePool, false, err
}

func (wh *writeHelper) updatePrimaryKeyMapIfNeeded(primaryKey []string, originName string) {
	if wh.mstPrimaryKeyRowMap == nil {
		wh.mstPrimaryKeyRowMap = make(map[string]map[string]struct{})
	}

	_, exist := wh.mstPrimaryKeyRowMap[originName]
	if !exist {
		m := make(map[string]struct{})
		for i := range primaryKey {
			if primaryKey[i] == "time" {
				continue
			}
			m[primaryKey[i]] = struct{}{}
		}
		wh.mstPrimaryKeyRowMap[originName] = m
		wh.pkLength = len(m)
	}
}

func (wh *writeHelper) reset() {
	wh.preSg = nil
	wh.preMst = nil
	wh.sameSchema = false
	wh.sameSg = false
	wh.sameMst = false
	wh.mstPrimaryKeyRowMap = nil
	wh.pkLength = 0
}

// The time range of the same batch of data may be similar,
// Therefore, there is a high probability that the data is written to the same shard.
// Caches the shard information of the previous row of data to accelerate the query of shard information.
func (wh *writeHelper) createShardGroup(database, retentionPolicy string, ts time.Time, engineType config.EngineType) (*meta2.ShardGroupInfo, bool, error) {
	var version uint32
	if engineType == config.COLUMNSTORE {
		version = logstore.CurrentLogTokenizerVersion
	}
	return createShardGroup(database, retentionPolicy, wh.pw.MetaClient, &wh.preSg, ts, version, engineType)
}

func appendField(fields []*proto2.FieldSchema, name string, typ int32) []*proto2.FieldSchema {
	fields = reserveField(fields)
	fields[len(fields)-1].FieldName = proto.String(name)
	fields[len(fields)-1].FieldType = proto.Int32(typ)
	return fields
}

func setLastFieldEndTime(ts int32, fields []*proto2.FieldSchema) {
	fields[len(fields)-1].EndTime = proto.Int32(ts)
}

type recordWriterHelper struct {
	sameSchema        bool
	nodeId            uint64
	db                string
	rp                string
	preSgStartTime    time.Time
	preSgEndTime      time.Time
	metaClient        RWMetaClient
	preSg             *meta2.ShardGroupInfo
	preShard          *meta2.ShardInfo
	preMst            *meta2.MeasurementInfo
	preSchema         *[]record.Field
	preShardType      config.EngineType
	fieldToCreatePool []*proto2.FieldSchema
}

func newRecordWriterHelper(metaClient RWMetaClient, nodeId uint64) *recordWriterHelper {
	return &recordWriterHelper{
		metaClient: metaClient,
		nodeId:     nodeId,
	}
}

func (wh *recordWriterHelper) createShardGroupsByTimeRange(database, retentionPolicy string, start, end time.Time,
	version uint32, engineType config.EngineType) ([]*meta2.ShardGroupInfo, error) {
	rpi, err := wh.metaClient.RetentionPolicy(database, retentionPolicy)
	if err != nil {
		return nil, err
	}
	startTime := start.Truncate(rpi.ShardGroupDuration)
	num := end.Sub(startTime).Nanoseconds()/rpi.ShardGroupDuration.Nanoseconds() + 1
	sgis := make([]*meta2.ShardGroupInfo, 0, num)
	startTime = start
	for i := 0; i < int(num); i++ {
		sg := rpi.ShardGroupByTimestampAndEngineType(startTime, engineType)
		if sg == nil {
			sg, _, err = wh.createShardGroup(database, retentionPolicy, startTime, version, engineType)
			if err != nil {
				return nil, err
			}
		}
		sgis = append(sgis, sg)
		startTime = startTime.Add(rpi.ShardGroupDuration)
	}
	return sgis, nil
}

func (wh *recordWriterHelper) createShardGroup(database, retentionPolicy string, ts time.Time, version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, bool, error) {
	return createShardGroup(database, retentionPolicy, wh.metaClient, &wh.preSg, ts, version, engineType)
}

func (wh *recordWriterHelper) GetShardByTime(sg *meta2.ShardGroupInfo, db, rp string, ts time.Time, ptIdx int, engineType config.EngineType) (*meta2.ShardInfo, error) {
	if wh.db == db && wh.rp == rp && wh.preShard != nil && wh.preShardType == engineType {
		if ts.After(wh.preSgStartTime) && ts.Before(wh.preSgEndTime) {
			return wh.preShard, nil
		}
	}
	shard, err := wh.metaClient.GetShardInfoByTime(db, rp, ts, ptIdx, wh.nodeId, engineType)
	if err != nil {
		return nil, err
	}
	wh.db = db
	wh.rp = rp
	wh.preSgStartTime = sg.StartTime
	wh.preSgEndTime = sg.EndTime
	wh.preShard = shard
	wh.preShardType = engineType
	return shard, nil
}

func (wh *recordWriterHelper) createMeasurement(database, retentionPolicy, name string) (*meta2.MeasurementInfo, error) {
	return createMeasurement(database, retentionPolicy, name, wh.metaClient, &wh.preMst, &wh.sameSchema, config.COLUMNSTORE)
}

func (wh *recordWriterHelper) checkAndUpdateRecordSchema(db, rp, mst, originName string, rec *record.Record) (startTime, endTime int64, err error) {
	wh.fieldToCreatePool = wh.fieldToCreatePool[:0]
	// check the number of columns
	if rec.ColNums() <= 1 {
		err = errno.NewError(errno.ColumnStoreColNumErr, db, rp, mst)
		return
	}

	// check the mst info and schema
	if wh.preMst == nil || wh.preMst.Name != mst {
		err = errno.NewError(errno.ColumnStoreSchemaNullErr, db, rp, mst)
		return
	}

	// check the column store info and primary key
	if wh.preMst.ColStoreInfo == nil || len(wh.preMst.ColStoreInfo.PrimaryKey) == 0 {
		err = errno.NewError(errno.ColumnStorePrimaryKeyNullErr, db, rp, mst)
		return
	}
	for _, key := range wh.preMst.ColStoreInfo.PrimaryKey {
		if rec.Schema.FieldIndex(key) == -1 {
			err = errno.NewError(errno.ColumnStorePrimaryKeyLackErr, mst, key)
			return
		}
	}

	// check the time field
	colNum := rec.ColNums() - 1
	if nil == rec.Schema.Field(colNum) || rec.Schema.Field(colNum).Name != record.TimeField {
		err = errno.NewError(errno.ArrowRecordTimeFieldErr)
		return
	}
	timeCol := &rec.ColVals[colNum]

	// check the field name and type
	samePreSchema := wh.sameSchema && wh.preSchema != nil && len(*wh.preSchema) == int(rec.ColNums())
	if !samePreSchema {
		schema := make([]record.Field, 0, rec.ColNums())
		wh.preSchema = &schema
	}
	wh.preMst.SchemaLock.RLock()
	for i := 0; i < colNum; i++ {
		_, ok := wh.preMst.Schema.GetTyp(rec.Schema.Field(i).Name)
		if !ok {
			wh.fieldToCreatePool = appendField(wh.fieldToCreatePool, rec.Schema.Field(i).Name, int32(rec.Schema.Field(i).Type))
		}
		if !samePreSchema {
			*wh.preSchema = append(*wh.preSchema, *rec.Schema.Field(i))
		}
	}
	wh.preMst.SchemaLock.RUnlock()
	if len(timeCol.IntegerValues()) == 0 {
		err = errno.NewError(errno.ArrowRecordTimeFieldErr)
		return
	}
	startTime, endTime = timeCol.IntegerValues()[0], timeCol.IntegerValues()[timeCol.Len-1]
	if !samePreSchema {
		*wh.preSchema = append(*wh.preSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	}
	if len(wh.fieldToCreatePool) > 0 {
		if err = wh.metaClient.UpdateSchema(db, rp, originName, wh.fieldToCreatePool); err != nil {
			return
		}
	}
	return
}

func (wh *recordWriterHelper) checkAndUpdateSchema(db, rp, mst, originName string, rec arrow.Record) (startTime, endTime int64, r *record.Record, err error) {
	wh.fieldToCreatePool = wh.fieldToCreatePool[:0]
	// check the number of columns
	if rec.NumCols() <= 1 {
		err = errno.NewError(errno.ColumnStoreColNumErr, db, rp, mst)
		return
	}

	// check the mst info and schema
	if wh.preMst == nil || wh.preMst.Name != mst || wh.preMst.Schema.Len() == 0 {
		err = errno.NewError(errno.ColumnStoreSchemaNullErr, db, rp, mst)
		return
	}

	// check the column store info and primary key
	if wh.preMst.ColStoreInfo == nil || len(wh.preMst.ColStoreInfo.PrimaryKey) == 0 {
		err = errno.NewError(errno.ColumnStorePrimaryKeyNullErr, db, rp, mst)
		return
	}
	for _, key := range wh.preMst.ColStoreInfo.PrimaryKey {
		if !rec.Schema().HasField(key) {
			err = errno.NewError(errno.ColumnStorePrimaryKeyLackErr, mst, key)
			return
		}
	}

	// check the time field
	colNum := int(rec.NumCols() - 1)
	times, ok := rec.Column(colNum).(*array.Int64)
	if !ok || rec.ColumnName(colNum) != record.TimeField {
		err = errno.NewError(errno.ArrowRecordTimeFieldErr)
		return
	}

	// check the field name and type
	samePreSchema := wh.sameSchema && wh.preSchema != nil && len(*wh.preSchema) == int(rec.NumCols())
	if !samePreSchema {
		schema := make([]record.Field, 0, rec.NumCols())
		wh.preSchema = &schema
	}
	wh.preMst.SchemaLock.Lock()
	for i := 0; i < colNum; i++ {
		colType, ok := wh.preMst.Schema.GetTyp(rec.ColumnName(i))
		fieldType := record.ArrowTypeToNativeType(rec.Column(i).DataType())
		if !ok {
			if rec.Schema().HasMetadata() {
				if rec.Schema().Metadata().FindKey(rec.ColumnName(i)) != -1 {
					wh.fieldToCreatePool = appendField(wh.fieldToCreatePool, rec.ColumnName(i), int32(influx.Field_Type_Tag))
					continue
				}
			}
			wh.fieldToCreatePool = appendField(wh.fieldToCreatePool, rec.ColumnName(i), int32(fieldType))
		} else {
			if (colType == influx.Field_Type_Tag && fieldType != influx.Field_Type_String) ||
				(colType != influx.Field_Type_Tag && fieldType != int(colType)) {
				err = errno.NewError(errno.ColumnStoreFieldTypeErr, mst, rec.ColumnName(i), fieldType, colType)
				wh.preMst.SchemaLock.Unlock()
				return
			}
		}
		if !samePreSchema {
			*wh.preSchema = append(*wh.preSchema, record.Field{Name: rec.ColumnName(i), Type: fieldType})
		}
	}
	wh.preMst.SchemaLock.Unlock()
	startTime, endTime = times.Value(0), times.Value(int(rec.NumRows()-1))
	if !samePreSchema {
		*wh.preSchema = append(*wh.preSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	}
	if len(wh.fieldToCreatePool) > 0 {
		if err = wh.metaClient.UpdateSchema(db, rp, originName, wh.fieldToCreatePool); err != nil {
			return
		}
	}
	r = record.NewRecord(cutPreSchema(*wh.preSchema), false)
	return
}

func (wh *recordWriterHelper) reset() {
	wh.preSg = nil
	wh.preMst = nil
	wh.preShard = nil
	wh.preSgStartTime = time.Time{}
	wh.preSgEndTime = time.Time{}
	wh.preSchema = nil
	wh.sameSchema = false
	wh.fieldToCreatePool = wh.fieldToCreatePool[:0]
}

func SearchLowerBoundOfRec(rec *record.Record, sg *meta2.ShardGroupInfo, start int) int {
	if rec.RowNums()-start <= 0 || rec.Time(start) >= sg.EndTime.UnixNano() || rec.Time(rec.RowNums()-1) < sg.StartTime.UnixNano() {
		return NotInShardDuration
	}
	return sort.Search(rec.RowNums()-start, func(i int) bool {
		return rec.Time(i+start) >= sg.EndTime.UnixNano()
	})
}

var writeRecCtxPool sync.Pool

func getWriteRecCtx() *writeRecCtx {
	v := writeRecCtxPool.Get()
	if v == nil {
		return &writeRecCtx{}
	}
	return v.(*writeRecCtx)
}

func putWriteRecCtx(s *writeRecCtx) {
	s.Reset()
	writeRecCtxPool.Put(s)
}

// ShardMapping contains a mapping of shards to points.
type writeRecCtx struct {
	minTime int64
	db      *meta2.DatabaseInfo
	rp      *meta2.RetentionPolicyInfo
	ms      *meta2.MeasurementInfo
}

func (wc *writeRecCtx) checkDBRP(database, retentionPolicy string, metaClient RWMetaClient) (err error) {
	// check db and rp validation
	wc.db, err = metaClient.Database(database)
	if err != nil {
		return err
	}

	if retentionPolicy == "" {
		retentionPolicy = wc.db.DefaultRetentionPolicy
	}

	wc.rp, err = wc.db.GetRetentionPolicy(retentionPolicy)
	if err != nil {
		return err
	}

	if wc.rp.Duration > 0 {
		wc.minTime = int64(fasttime.UnixTimestamp()*1e9) - wc.rp.Duration.Nanoseconds()
	}
	return
}

func (wc *writeRecCtx) Reset() {
	wc.minTime = 0
	wc.db = nil
	wc.rp = nil
	wc.ms = nil
}

type ComMetaClient interface {
	Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
	CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation, engineType config.EngineType,
		colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error)
	CreateShardGroup(database, policy string, timestamp time.Time, version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, error)
}

func createMeasurement(database, retentionPolicy, name string, client ComMetaClient, preMst **meta2.MeasurementInfo, sameSchema *bool, engineType config.EngineType) (*meta2.MeasurementInfo, error) {
	// fast path, same table name
	if *preMst != nil && *sameSchema {
		if (*preMst).OriginName() == name {
			return *preMst, nil
		}
	}

	start := time.Now()
	defer func() {
		statistics.NewHandler().WriteCreateMstDuration.AddSinceNano(start)
	}()

	mst, err := createMeasurementBase(database, retentionPolicy, name, client, engineType)

	if err == nil {
		*preMst = mst
		*sameSchema = true
	}
	return mst, err
}

func createMeasurementBase(database, retentionPolicy, name string, client ComMetaClient, engineType config.EngineType) (*meta2.MeasurementInfo, error) {
	start := time.Now()
	defer func() {
		statistics.NewHandler().WriteCreateMstDuration.AddSinceNano(start)
	}()

	mst, err := client.Measurement(database, retentionPolicy, name)
	if err == meta2.ErrMeasurementNotFound {
		ski := &meta2.ShardKeyInfo{ShardKey: nil, Type: influxql.HASH}
		mst, err = client.CreateMeasurement(database, retentionPolicy, name, ski, 0, nil, engineType, nil, nil, nil)
	}
	return mst, err
}

func createShardGroup(database, retentionPolicy string, client ComMetaClient, preSg **meta2.ShardGroupInfo, ts time.Time,
	version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, bool, error) {
	// fast path, time is contained
	if *preSg != nil && (*preSg).Contains(ts) && (*preSg).EngineType == engineType {
		return *preSg, true, nil
	}

	sg, err := client.CreateShardGroup(database, retentionPolicy, ts, version, engineType)
	if err != nil {
		return sg, false, err
	}

	if sg == nil {
		return nil, false, errno.NewError(errno.WriteNoShardGroup)
	}

	*preSg = sg
	return sg, false, nil
}

func cutPreSchema(preSchema []record.Field) []record.Field {
	if !config.IsLogKeeper() {
		return preSchema
	}
	// cut seqId field
	schema := make([]record.Field, 0, len(preSchema)-1)
	for i := range preSchema {
		if preSchema[i].Name == record.SeqIDField {
			continue
		}
		schema = append(schema, preSchema[i])
	}
	return schema
}
