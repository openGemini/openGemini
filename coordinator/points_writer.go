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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const MaxShardKey = 64 * 1024

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	timeout    time.Duration
	MetaClient interface {
		Database(name string) (di *meta2.DatabaseInfo, err error)
		RetentionPolicy(database, policy string) (*meta2.RetentionPolicyInfo, error)
		CreateShardGroup(database, policy string, timestamp time.Time) (*meta2.ShardGroupInfo, error)
		DBPtView(database string) (meta2.DBPtInfos, error)
		Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
		UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error
		CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *meta2.IndexRelation) (*meta2.MeasurementInfo, error)
		GetAliveShards(database string, sgi *meta2.ShardGroupInfo) []int
	}

	TSDBStore interface {
		WriteRows(nodeID uint64, database, rp string, pt uint32, shard uint64, rows *[]influx.Row, timeout time.Duration) error
	}

	logger *logger.Logger
}

// NewPointsWriter returns a new instance of PointsWriter for a node.
func NewPointsWriter(timeout time.Duration) *PointsWriter {
	return &PointsWriter{
		timeout: timeout,
		logger:  logger.NewLogger(errno.ModuleCoordinator),
	}
}

// ShardMapping contains a mapping of shards to points.
type injestionCtx struct {
	fieldToCreatePool []*proto2.FieldSchema
	rowsPool          sync.Pool
	shardRowMap       dictpool.Dict
	shardMap          dictpool.Dict
}

func (s *injestionCtx) getShardRowMap() *dictpool.Dict {
	return &s.shardRowMap
}

func (s *injestionCtx) getShardMap() *dictpool.Dict {
	return &s.shardMap
}

func (s *injestionCtx) getRowsPool() *[]influx.Row {
	v := s.rowsPool.Get()
	if v == nil {
		return &[]influx.Row{}
	}
	return v.(*[]influx.Row)
}

func (s *injestionCtx) putRowsPool(rp *[]influx.Row) {
	for _, r := range *rp {
		r.Reset()
	}
	*rp = (*rp)[:0]
	s.rowsPool.Put(rp)
}

func (s *injestionCtx) Reset() {
	s.fieldToCreatePool = s.fieldToCreatePool[:0]
	s.shardMap.Reset()
	s.shardRowMap.Reset()
}

func getInjestionCtx() *injestionCtx {
	v := injestionCtxPool.Get()
	if v == nil {
		return &injestionCtx{}
	}
	return v.(*injestionCtx)
}

func putInjestionCtx(s *injestionCtx) {
	s.Reset()
	injestionCtxPool.Put(s)
}

var injestionCtxPool sync.Pool

func reserveField(fieldToCreatePool []*proto2.FieldSchema) []*proto2.FieldSchema {
	if cap(fieldToCreatePool) > len(fieldToCreatePool) {
		fieldToCreatePool = fieldToCreatePool[:len(fieldToCreatePool)+1]
	} else {
		fieldToCreatePool = append(fieldToCreatePool, &proto2.FieldSchema{})
	}
	if fieldToCreatePool[len(fieldToCreatePool)-1] == nil {
		fieldToCreatePool[len(fieldToCreatePool)-1] = &proto2.FieldSchema{}
	}
	return fieldToCreatePool
}

func dropFieldByIndex(r *influx.Row, dropFieldIndex []int) {
	totalField := len(r.Fields)
	dropField := len(dropFieldIndex)
	if totalField == dropField {
		r.Fields = r.Fields[:0]
		return
	}

	remainField := make([]influx.Field, totalField-dropField)
	remainIdx, dropIdx, totalIdx := 0, 0, 0
	for totalIdx = 0; totalIdx < totalField && dropIdx < dropField; totalIdx++ {
		if totalIdx < dropFieldIndex[dropIdx] {
			remainField[remainIdx] = r.Fields[totalIdx]
			remainIdx++
		} else {
			dropIdx++
		}
	}

	for totalIdx < totalField {
		remainField[remainIdx] = r.Fields[totalIdx]
		remainIdx++
		totalIdx++
	}

	r.Fields = remainField
}

func checkFields(fields influx.Fields) error {
	for i := 0; i < len(fields); i++ {
		if i > 0 && fields[i-1].Key == fields[i].Key {
			failpoint.Inject("skip-duplicate-field-check", func(val failpoint.Value) {
				if strings2.EqualInterface(val, fields[i].Key) {
					failpoint.Continue()
				}
			})

			return errno.NewError(errno.DuplicateField, fields[i].Key)
		}
	}

	return nil
}

func (w *PointsWriter) updateSchemaIfNeeded(database, rp string, r *influx.Row, mst *meta2.MeasurementInfo, fieldToCreatePool []*proto2.FieldSchema) ([]*proto2.FieldSchema, bool, error) {
	// update schema if needed
	schemaMap := mst.Schema

	// check tag need to add or not
	for _, tag := range r.Tags {
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
		fieldType, ok := schemaMap[field.Key]
		if ok {
			if fieldType != field.Type {
				failpoint.Inject("skip-field-type-conflict", func(val failpoint.Value) {
					if strings2.EqualInterface(val, field.Key) {
						failpoint.Continue()
					}
				})

				err = errno.NewError(errno.FieldTypeConflict, field.Key, r.Name, influx.FieldTypeString(field.Type),
					influx.FieldTypeString(fieldType)).SetModule(errno.ModuleWrite)
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
		if errInner := w.MetaClient.UpdateSchema(database, rp, r.Name, fieldToCreatePool); errInner != nil {
			if strings.Contains(errInner.Error(), "field type conflict") {
				return fieldToCreatePool, true, errInner
			}
			return fieldToCreatePool, false, errInner
		}
	}
	return fieldToCreatePool, false, err
}

func (w *PointsWriter) WritePointRows(database, retentionPolicy string, rows []influx.Row) error {
	// check db and rp validation
	db, err := w.MetaClient.Database(database)
	if err != nil {
		return err
	}

	if retentionPolicy == "" {
		retentionPolicy = db.DefaultRetentionPolicy
	}

	rp, err := db.GetRetentionPolicy(retentionPolicy)
	if err != nil {
		return err
	}

	var min int64
	if rp.Duration > 0 {
		min = int64(fasttime.UnixTimestamp()*1e9) - rp.Duration.Nanoseconds()
	}

	ctx := getInjestionCtx()
	defer putInjestionCtx(ctx)

	shardrowmap := ctx.getShardRowMap()
	shardmap := ctx.getShardMap()
	isDropRow := false
	var partialErr error
	var dropped int

	//validate, map and push point to bach transport buffer
	for i := range rows {
		r := &rows[i]
		sort.Sort(r.Fields)

		if err := checkFields(r.Fields); err != nil {
			partialErr = err
			continue
		}

		//check point is between rp duration
		if r.Timestamp < min {
			errInfo := errno.NewError(errno.WritePointOutOfRP)
			w.logger.Error("write failed", zap.Error(errInfo))
			partialErr = errInfo
			dropped++
			continue
		}

		start := time.Now()
		mst, err := w.MetaClient.Measurement(database, retentionPolicy, r.Name)
		if err == meta2.ErrMeasurementNotFound {
			ski := &meta2.ShardKeyInfo{ShardKey: nil, Type: influxql.HASH}
			mst, err = w.MetaClient.CreateMeasurement(database, retentionPolicy, r.Name, ski, nil)
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		atomic.AddInt64(&statistics.HandlerStat.WriteCreateMstDuration, time.Since(start).Nanoseconds())
		start = time.Now()
		if ctx.fieldToCreatePool, isDropRow, err = w.updateSchemaIfNeeded(database, retentionPolicy, r, mst, ctx.fieldToCreatePool[:0]); err != nil {
			if strings.Contains(err.Error(), "field type conflict") {
				partialErr = err
				if isDropRow {
					dropped++
					continue
				}
			} else {
				return err
			}
		}
		atomic.AddInt64(&statistics.HandlerStat.WriteUpdateSchemaDuration, time.Since(start).Nanoseconds())
		start = time.Now()
		var shardgroup *meta2.ShardGroupInfo
		if shardgroup, err = w.MetaClient.CreateShardGroup(database, retentionPolicy, time.Unix(0, r.Timestamp)); err != nil {
			return err
		}
		atomic.AddInt64(&statistics.HandlerStat.WriteCreateSgDuration, time.Since(start).Nanoseconds())
		if shardgroup == nil {
			return errno.NewError(errno.WriteNoShardGroup)
		}
		start = time.Now()
		var shardKeyInfo *meta2.ShardKeyInfo
		if len(db.ShardKey.ShardKey) > 0 {
			shardKeyInfo = &db.ShardKey
		} else {
			shardKeyInfo = mst.GetShardKey(shardgroup.ID)
		}

		if shardKeyInfo == nil {
			return errno.NewError(errno.WriteNoShardKey)
		}

		if err = r.UnmarshalShardKeyByTag(shardKeyInfo.ShardKey); err != nil {
			if err != influx.ErrPointShouldHaveAllShardKey {
				return err
			}
			partialErr = err
			dropped++
			continue
		}

		if len(r.ShardKey) > MaxShardKey {
			partialErr = errno.NewError(errno.WritePointShardKeyTooLarge)
			w.logger.Error("write failed", zap.Error(partialErr))
			dropped++
			continue
		}

		atomic.AddInt64(&statistics.HandlerStat.WriteUnmarshalSkDuration, time.Since(start).Nanoseconds())
		var sh *meta2.ShardInfo
		aliveShardIdxes := w.MetaClient.GetAliveShards(database, shardgroup)
		if shardKeyInfo.Type == influxql.RANGE {
			sh = shardgroup.DestShard(bytesutil.ToUnsafeString(r.ShardKey))
		} else {
			sh = shardgroup.ShardFor(meta2.HashID(r.ShardKey), aliveShardIdxes)
		}

		if sh == nil {
			return errno.NewError(errno.WritePointMap2Shard)
		}

		id := strconv.FormatInt(int64(sh.ID), 10)
		shardmap.Set(id, sh)

		indexRelations := mst.GetIndexRelationIndexList()
		if len(indexRelations) != 0 {
			r.IndexOptions = r.IndexOptions[:0]
			columns := make(Columns, r.Tags.Len()+r.Fields.Len())
			index := 0
			for j := 0; j < r.Tags.Len(); j++ {
				columns[index] = r.Tags[j].Key
				index++
			}
			for j := 0; j < r.Fields.Len(); j++ {
				columns[index] = r.Fields[j].Key
				index++
			}

			for _, relation := range indexRelations {
				for _, indexList := range relation.IndexList {
					ok, index := selectArr(columns, indexList.IList)
					if ok {
						opt := influx.IndexOption{
							IndexList: index,
							Oid:       relation.Oid,
						}
						r.IndexOptions = append(r.IndexOptions, opt)
					}
				}
			}
		}

		if err = w.MapRowToShard(shardrowmap, ctx, id, r); err != nil {
			return err
		}
		atomic.AddInt64(&statistics.HandlerStat.FieldsWritten, int64(r.Fields.Len()))
	}

	errC := make(chan error, shardrowmap.Len())
	for _, mapp := range shardrowmap.D {
		shardId := mapp.Key

		sh, ok := shardmap.Get(shardId).(*meta2.ShardInfo)
		if !ok {
			return errno.NewError(errno.WriteMapMetaShardInfo)
		}

		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			return errno.NewError(errno.WriteMapMetaShardInfo)
		}
		go func(shard *meta2.ShardInfo, db, rp string, rs *[]influx.Row, ctx *injestionCtx) {
			err := w.writeRowToShard(sh, database, retentionPolicy, rows, ctx)
			errC <- err
		}(sh, database, retentionPolicy, rows, ctx)
	}

	for i := 0; i < shardrowmap.Len(); i++ {
		errShard := <-errC
		if errShard != nil {
			err = errShard
		}
	}
	if err != nil {
		return err
	}
	if dropped > 0 {
		return netstorage.PartialWriteError{Reason: partialErr, Dropped: dropped}
	}
	return partialErr
}

func (w *PointsWriter) MapRowToShard(shardrowmap *dictpool.Dict, ctx *injestionCtx, id string, r *influx.Row) error {
	if !shardrowmap.Has(id) {
		rp := ctx.getRowsPool()
		shardrowmap.Set(id, rp)
	}
	rowsPool := shardrowmap.Get(id)
	rp, ok := rowsPool.(*[]influx.Row)
	if !ok {
		return errors.New("shardrowmap error")
	}

	if cap(*rp) > len(*rp) {
		*rp = (*rp)[:len(*rp)+1]
	} else {
		*rp = append(*rp, influx.Row{})
	}
	rr := &(*rp)[len(*rp)-1]
	rr.Clone(r)
	return nil
}

// writeRowToShard writes row to a shard.
func (w *PointsWriter) writeRowToShard(shard *meta2.ShardInfo, database, retentionPolicy string, row *[]influx.Row, ctx *injestionCtx) error {
	ptView, err := w.MetaClient.DBPtView(database)
	if err != nil {
		return err
	}
	start := time.Now()
	for _, ptId := range shard.Owners {
		err = w.TSDBStore.WriteRows(ptView[ptId].Owner.NodeID, database, retentionPolicy, ptId, shard.ID, row, w.timeout)

		ctx.putRowsPool(row)
		if err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.HandlerStat.WriteStoresDuration, time.Since(start).Nanoseconds())
	return nil
}

// define to sync.Pool
type Columns []string

func (columns Columns) Less(i, j int) bool {
	return columns[i] < columns[j]
}

func (columns Columns) Len() int {
	return len(columns)
}

func (columns Columns) Swap(i, j int) {
	columns[i], columns[j] = columns[j], columns[i]
}

func (f *Columns) Reset() {}

func selectArr(arr1 []string, arr2 []string) (bool, []uint16) {
	var index []uint16
	m, n := len(arr1), len(arr2)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i, c1 := range arr1 {
		for j, c2 := range arr2 {
			if c1 == c2 {
				index = append(index, uint16(i))
				dp[i+1][j+1] = dp[i][j] + 1
			} else {
				dp[i+1][j+1] = max(dp[i][j+1], dp[i+1][j])
			}
		}
	}

	return dp[m][n] == n, index
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
