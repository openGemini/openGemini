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
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
)

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
		mst, err = wh.pw.MetaClient.CreateMeasurement(database, retentionPolicy, name, ski, nil)
	}

	if err == nil {
		wh.preMst = mst
		wh.sameSchema = true
	}

	return mst, err
}

func (wh *writeHelper) getIndexGroupInfo(database, retentionPolicy string, timestamp time.Time) (*meta2.IndexGroupInfo, error) {
	var indexGroupInfo *meta2.IndexGroupInfo
	var err error
	indexGroupInfo, err = wh.pw.MetaClient.GetIndexGroupInfo(database, retentionPolicy, timestamp)
	if err != nil {
		return nil, err
	}
	return indexGroupInfo, err
}

func (wh *writeHelper) updateSchemaKeySetIfNeeded(database, rp string, r *influx.Row, idi *meta2.IndexGroupInfo, mst *meta2.MeasurementInfo,
	originName string, timestamp time.Time) error {
	SchemaKeySet := idi.SchemaKeySet
	var ids []uint64
	var fieldToCreatePool []*proto2.FieldSchema

	for _, tag := range r.Tags {
		keyInfo := mst.Schema[tag.Key]
		_, ok := SchemaKeySet[keyInfo.ID]
		if !ok {
			ids = append(ids, keyInfo.ID)
			fieldToCreatePool = append(fieldToCreatePool,
				&proto2.FieldSchema{FieldName: proto.String(tag.Key), FieldType: proto.Int32(influx.Field_Type_Tag)})
		}
	}

	for _, field := range r.Fields {
		keyInfo := mst.Schema[field.Key]
		_, ok := SchemaKeySet[keyInfo.ID]
		if !ok {
			ids = append(ids, keyInfo.ID)
			fieldToCreatePool = append(fieldToCreatePool,
				&proto2.FieldSchema{FieldName: proto.String(field.Key), FieldType: proto.Int32(field.Type)})
		}
	}

	if len(ids) == 0 {
		return nil
	}
	err := wh.pw.MetaClient.UpdateSchemaKeySet(database, rp, originName, ids, fieldToCreatePool, timestamp)
	return err

}

func (wh *writeHelper) updateSchemaIfNeeded(database, rp string, r *influx.Row, mst *meta2.MeasurementInfo,
	originName string, fieldToCreatePool []*proto2.FieldSchema) ([]*proto2.FieldSchema, bool, error) {
	// update schema if needed
	schemaMap := mst.Schema

	// check tag need to add or not
	for i, tag := range r.Tags {
		if tag.Key == "time" {
			return fieldToCreatePool, true, errors.New("tag key can't be 'time'")
		}
		if err := r.CheckDuplicateTag(i); err != nil {
			return fieldToCreatePool, true, err
		}

		_, ok := schemaMap[tag.Key]
		if !ok {
			fieldToCreatePool = reserveField(fieldToCreatePool)
			fieldToCreatePool[len(fieldToCreatePool)-1].FieldName = proto.String(tag.Key)
			fieldToCreatePool[len(fieldToCreatePool)-1].FieldType = proto.Int32(influx.Field_Type_Tag)
		}
	}

	// check field type is conflict or not
	var dropFieldIndex []int
	var err error
	for i, field := range r.Fields {
		fieldInfo, ok := schemaMap[field.Key]
		if ok {
			if fieldInfo.Type != field.Type {
				failpoint.Inject("skip-field-type-conflict", func(val failpoint.Value) {
					if strings2.EqualInterface(val, field.Key) {
						failpoint.Continue()
					}
				})

				err = errno.NewError(errno.FieldTypeConflict, field.Key, originName, influx.FieldTypeString(field.Type),
					influx.FieldTypeString(fieldInfo.Type)).SetModule(errno.ModuleWrite)
				dropFieldIndex = append(dropFieldIndex, i)
			}
			continue
		}

		fieldToCreatePool = reserveField(fieldToCreatePool)
		fieldToCreatePool[len(fieldToCreatePool)-1].FieldName = proto.String(field.Key)
		fieldToCreatePool[len(fieldToCreatePool)-1].FieldType = proto.Int32(field.Type)
	}
	if len(dropFieldIndex) > 0 {
		dropFieldByIndex(r, dropFieldIndex)
		if len(r.Fields) == 0 {
			return fieldToCreatePool, true, err
		}
	}

	if len(fieldToCreatePool) > 0 {
		if errInner := wh.pw.MetaClient.UpdateSchema(database, rp, originName, fieldToCreatePool); errInner != nil {
			if strings.Contains(errInner.Error(), "field type conflict") {
				return fieldToCreatePool, true, errInner
			}
			return fieldToCreatePool, false, errInner
		}
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
func (wh *writeHelper) createShardGroup(database, retentionPolicy string, ts time.Time) (*meta2.ShardGroupInfo, bool, error) {
	// fast path, time is contained
	if wh.preSg != nil && wh.preSg.Contains(ts) {
		return wh.preSg, true, nil
	}

	var err error
	var sg *meta2.ShardGroupInfo

	sg, err = wh.pw.MetaClient.CreateShardGroup(database, retentionPolicy, ts)
	if err != nil {
		return sg, false, err
	}

	if sg == nil {
		return nil, false, errno.NewError(errno.WriteNoShardGroup)
	}

	wh.preSg = sg
	return sg, false, nil
}
