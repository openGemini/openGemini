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

package coordinator

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
)

const NotInShardDuration = -1

var tagLimit = 0

func SetTagLimit(limit int) {
	if limit > 0 {
		tagLimit = limit
	}
}

func GetTagLimit() int {
	return tagLimit
}

type writeHelper struct {
	pw         *PointsWriter
	sameSchema bool
	sameSg     bool
	preSg      *meta2.ShardGroupInfo
	preMst     *meta2.MeasurementInfo
}

func newWriteHelper(pw *PointsWriter) *writeHelper {
	return &writeHelper{
		pw: pw,
	}
}

// The table names of the same batch of data may be the same.
// Caches table information in the previous row to accelerate table information query.
func (wh *writeHelper) createMeasurement(database, retentionPolicy, name string) (*meta2.MeasurementInfo, error) {
	return createMeasurement(database, retentionPolicy, name, wh.pw.MetaClient, &wh.preMst, &wh.sameSchema, config.TSSTORE)
}

func (wh *writeHelper) updateSchemaIfNeeded(database, rp string, r *influx.Row, mst *meta2.MeasurementInfo,
	originName string, fieldToCreatePool []*proto2.FieldSchema) ([]*proto2.FieldSchema, bool, error) {
	// update schema if needed
	schemaMap := mst.Schema

	// check tag need to add or not
	for i, tag := range r.Tags {
		if tag.Key == "time" {
			return fieldToCreatePool, true, errno.NewError(errno.InvalidTagKey, tag.Key)
		}
		if err := r.CheckDuplicateTag(i); err != nil {
			return fieldToCreatePool, true, err
		}

		if _, ok := schemaMap[tag.Key]; !ok {
			fieldToCreatePool = appendField(fieldToCreatePool, tag.Key, influx.Field_Type_Tag)
		}
	}

	if tl := GetTagLimit(); tl > 0 && len(fieldToCreatePool) > 0 && mst.TagKeysTotal() > tl {
		return fieldToCreatePool, true, errno.NewError(errno.TooManyTagKeys)
	}

	// check field type is conflict or not
	var dropFieldIndex []int
	var err error
	for i, field := range r.Fields {
		fieldType, ok := schemaMap[field.Key]
		if ok {
			if fieldType != field.Type {
				failpoint.Inject("skip-field-type-conflict", func(val failpoint.Value) {
					if strings2.EqualInterface(val, field.Key) {
						failpoint.Continue()
					}
				})

				err = errno.NewError(errno.FieldTypeConflict, field.Key, originName, influx.FieldTypeString(field.Type),
					influx.FieldTypeString(fieldType)).SetModule(errno.ModuleWrite)
				dropFieldIndex = append(dropFieldIndex, i)
			}
			continue
		}

		fieldToCreatePool = appendField(fieldToCreatePool, field.Key, field.Type)
	}

	if len(dropFieldIndex) > 0 {
		dropFieldByIndex(r, dropFieldIndex)
		if len(r.Fields) == 0 {
			return fieldToCreatePool, true, err
		}
	}

	if len(fieldToCreatePool) > 0 {
		start := time.Now()
		if errInner := wh.pw.MetaClient.UpdateSchema(database, rp, originName, fieldToCreatePool); errInner != nil {
			return fieldToCreatePool, strings.Contains(errInner.Error(), "field type conflict"), errInner
		}
		atomic.AddInt64(&statistics.HandlerStat.WriteUpdateSchemaDuration, time.Since(start).Nanoseconds())
		wh.sameSchema = false
	}
	return fieldToCreatePool, false, err
}

func (wh *writeHelper) reset() {
	wh.preSg = nil
	wh.preMst = nil
	wh.sameSchema = false
	wh.sameSg = false
}

// The time range of the same batch of data may be similar,
// Therefore, there is a high probability that the data is written to the same shard.
// Caches the shard information of the previous row of data to accelerate the query of shard information.
func (wh *writeHelper) createShardGroup(database, retentionPolicy string, ts time.Time, engineType config.EngineType) (*meta2.ShardGroupInfo, bool, error) {
	return createShardGroup(database, retentionPolicy, wh.pw.MetaClient, &wh.preSg, ts, engineType)
}

func appendField(fields []*proto2.FieldSchema, name string, typ int32) []*proto2.FieldSchema {
	fields = reserveField(fields)
	fields[len(fields)-1].FieldName = proto.String(name)
	fields[len(fields)-1].FieldType = proto.Int32(typ)
	return fields
}

type recordWriterHelper struct {
	sameSchema   bool
	db           string
	rp           string
	nodeId       uint64
	groupId      int
	dur          time.Duration
	metaClient   RWMetaClient
	preSg        *meta2.ShardGroupInfo
	preShard     *meta2.ShardInfo
	preMst       *meta2.MeasurementInfo
	preShardType config.EngineType
}

func newRecordWriterHelper(metaClient RWMetaClient, nodeId uint64) *recordWriterHelper {
	return &recordWriterHelper{
		metaClient: metaClient,
		nodeId:     nodeId,
	}
}

func (wh *recordWriterHelper) createShardGroupsByTimeRange(database, retentionPolicy string, start, end time.Time, engineType config.EngineType) ([]*meta2.ShardGroupInfo, error) {
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
			sg, _, err = wh.createShardGroup(database, retentionPolicy, startTime, engineType)
			if err != nil {
				return nil, err
			}
		}
		sgis = append(sgis, sg)
		startTime = startTime.Add(rpi.ShardGroupDuration)
	}
	return sgis, nil
}

func (wh *recordWriterHelper) createShardGroup(database, retentionPolicy string, ts time.Time, engineType config.EngineType) (*meta2.ShardGroupInfo, bool, error) {
	return createShardGroup(database, retentionPolicy, wh.metaClient, &wh.preSg, ts, engineType)
}

func (wh *recordWriterHelper) GetShardByTime(db, rp string, ts time.Time, ptIdx int, engineType config.EngineType) (*meta2.ShardInfo, error) {
	if wh.db == db && wh.rp == rp && wh.dur != 0 && wh.preShard != nil && wh.preShardType == engineType {
		if curGroupId := int(ts.UnixNano() / wh.dur.Nanoseconds()); curGroupId == wh.groupId {
			return wh.preShard, nil
		}
	}
	shard, err := wh.metaClient.GetShardInfoByTime(db, rp, ts, ptIdx, wh.nodeId, engineType)
	if err != nil {
		return nil, err
	}
	rpInfo, err := wh.metaClient.RetentionPolicy(db, rp)
	if err != nil {
		return nil, err
	}
	wh.groupId = int(ts.UnixNano() / rpInfo.ShardGroupDuration.Nanoseconds())
	wh.db = db
	wh.rp = rp
	wh.dur = rpInfo.ShardGroupDuration
	wh.preShard = shard
	wh.preShardType = engineType
	return shard, nil
}

func (wh *recordWriterHelper) createMeasurement(database, retentionPolicy, name string) (*meta2.MeasurementInfo, error) {
	return createMeasurement(database, retentionPolicy, name, wh.metaClient, &wh.preMst, &wh.sameSchema, config.COLUMNSTORE)
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
	CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *meta2.IndexRelation, engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo) (*meta2.MeasurementInfo, error)
	CreateShardGroup(database, policy string, timestamp time.Time, engineType config.EngineType) (*meta2.ShardGroupInfo, error)
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
		atomic.AddInt64(&statistics.HandlerStat.WriteCreateMstDuration, time.Since(start).Nanoseconds())
	}()

	mst, err := client.Measurement(database, retentionPolicy, name)
	if err == meta2.ErrMeasurementNotFound {
		ski := &meta2.ShardKeyInfo{ShardKey: nil, Type: influxql.HASH}
		mst, err = client.CreateMeasurement(database, retentionPolicy, name, ski, nil, engineType, nil)
	}

	if err == nil {
		*preMst = mst
		*sameSchema = true
	}
	return mst, err
}

func createShardGroup(database, retentionPolicy string, client ComMetaClient, preSg **meta2.ShardGroupInfo, ts time.Time, engineType config.EngineType) (*meta2.ShardGroupInfo, bool, error) {
	// fast path, time is contained
	if *preSg != nil && (*preSg).Contains(ts) {
		return *preSg, true, nil
	}

	sg, err := client.CreateShardGroup(database, retentionPolicy, ts, engineType)
	if err != nil {
		return sg, false, err
	}

	if sg == nil {
		return nil, false, errno.NewError(errno.WriteNoShardGroup)
	}

	*preSg = sg
	return sg, false, nil
}
