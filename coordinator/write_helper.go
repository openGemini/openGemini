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
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
)

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
	// fast path, same table name
	if wh.preMst != nil && wh.sameSchema {
		if wh.preMst.OriginName() == name {
			return wh.preMst, nil
		}
	}

	start := time.Now()
	defer func() {
		atomic.AddInt64(&statistics.HandlerStat.WriteCreateMstDuration, time.Since(start).Nanoseconds())
	}()

	var mst *meta2.MeasurementInfo
	var err error
	mst, err = wh.pw.MetaClient.Measurement(database, retentionPolicy, name)
	if err == meta2.ErrMeasurementNotFound {
		ski := &meta2.ShardKeyInfo{ShardKey: nil, Type: influxql.HASH}
		mst, err = wh.pw.MetaClient.CreateMeasurement(database, retentionPolicy, name, ski, nil, config.TSSTORE, nil)
	}

	if err == nil {
		wh.preMst = mst
		wh.sameSchema = true
	}

	return mst, err
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
	// fast path, time is contained
	if wh.preSg != nil && wh.preSg.Contains(ts) {
		return wh.preSg, true, nil
	}

	var err error
	var sg *meta2.ShardGroupInfo

	sg, err = wh.pw.MetaClient.CreateShardGroup(database, retentionPolicy, ts, engineType)
	if err != nil {
		return sg, false, err
	}

	if sg == nil {
		return nil, false, errno.NewError(errno.WriteNoShardGroup)
	}

	wh.preSg = sg
	return sg, false, nil
}

func appendField(fields []*proto2.FieldSchema, name string, typ int32) []*proto2.FieldSchema {
	fields = reserveField(fields)
	fields[len(fields)-1].FieldName = proto.String(name)
	fields[len(fields)-1].FieldType = proto.Int32(typ)
	return fields
}
