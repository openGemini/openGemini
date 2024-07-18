/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// ShardMapping contains a mapping of shards to points.
type injestionCtx struct {
	fieldToCreatePool []*proto.FieldSchema
	pRowsPool         sync.Pool
	shardRowMap       ShardRows

	srcStreamDstShardIdMap map[uint64]map[uint64]uint64
	mstShardIdRowMap       map[string]map[uint64]*[]*influx.Row

	streamInfos           []*meta.StreamInfo
	streamDBs             []*meta.DatabaseInfo
	streamMSTs            []*meta.MeasurementInfo
	streamShardKeyInfos   []*meta.ShardKeyInfo
	streamWriteHelpers    []*writeHelper
	streamAliveShardIdxes [][]int

	minTime         int64
	db              *meta.DatabaseInfo
	rp              *meta.RetentionPolicyInfo
	ms              *meta.MeasurementInfo
	shardKeyInfo    *meta.ShardKeyInfo
	writeHelper     *writeHelper
	aliveShardIdxes []int

	stream *Stream

	writeCtx []*netstorage.WriteContext
}

func (s *injestionCtx) getShardRow(id uint64) *ShardRow {
	idx := sort.Search(s.shardRowMap.Len(), func(i int) bool {
		return id <= s.shardRowMap[i].shardInfo.ID
	})

	if idx < s.shardRowMap.Len() && s.shardRowMap[idx].shardInfo.ID == id {
		return &s.shardRowMap[idx]
	}
	return nil
}

func (s *injestionCtx) setShardRow(shardInfo *meta.ShardInfo, row *influx.Row) {
	shardRows := s.getShardRow(shardInfo.ID)
	if shardRows == nil {
		// if the current length equals the capacity, append the new ShardRow
		if cap(s.shardRowMap) == len(s.shardRowMap) {
			s.shardRowMap = append(s.shardRowMap, ShardRow{shardInfo, []*influx.Row{row}})
		} else {
			// otherwise, increase the length of the slice and append the new ShardRow
			index := len(s.shardRowMap)
			s.shardRowMap = s.shardRowMap[:index+1]
			s.shardRowMap[index].shardInfo = shardInfo
			s.shardRowMap[index].rows = append(s.shardRowMap[index].rows[:0], row)
		}
		sort.Sort(s.shardRowMap)
	} else {
		shardRows.rows = append(shardRows.rows, row)
	}
}

func (s *injestionCtx) Reset() {
	s.fieldToCreatePool = s.fieldToCreatePool[:0]
	s.shardRowMap = s.shardRowMap[:0]
	s.writeCtx = s.writeCtx[:0]

	if s.srcStreamDstShardIdMap != nil {
		s.srcStreamDstShardIdMap = map[uint64]map[uint64]uint64{}
	}
	if s.mstShardIdRowMap != nil {
		s.mstShardIdRowMap = map[string]map[uint64]*[]*influx.Row{}
	}
	if s.stream != nil {
		s.stream.tasks = map[string]*streamTask{}
	}

	s.streamInfos = s.streamInfos[:0]
	s.streamDBs = s.streamDBs[:0]
	s.streamMSTs = s.streamMSTs[:0]
	for i := range s.streamAliveShardIdxes {
		s.streamAliveShardIdxes[i] = s.streamAliveShardIdxes[i][:0]
	}
	s.streamShardKeyInfos = s.streamShardKeyInfos[:0]

	for i := 0; i < len(s.streamWriteHelpers); i++ {
		s.streamWriteHelpers[i].reset()
	}
	if s.writeHelper != nil {
		s.writeHelper.reset()
	}
	s.db = nil
	s.rp = nil
	s.ms = nil
	s.minTime = 0
	s.aliveShardIdxes = s.aliveShardIdxes[:0]
	s.shardKeyInfo = nil
}

func (s *injestionCtx) initStreamDBs(length int) {
	if cap(s.streamDBs) < length {
		s.streamDBs = make([]*meta.DatabaseInfo, length)
	} else {
		s.streamDBs = s.streamDBs[:length]
	}
}

func (s *injestionCtx) initStreamMSTs(length int) {
	if cap(s.streamMSTs) < length {
		s.streamMSTs = make([]*meta.MeasurementInfo, length)
	} else {
		s.streamMSTs = s.streamMSTs[:length]
	}
}

// streamWriteHelpers not reset, PointsWriter stateless
func (s *injestionCtx) initStreamWriteHelpers(length int, w *PointsWriter) {
	if len(s.streamWriteHelpers) >= length {
		return
	}
	addLen := length - len(s.streamWriteHelpers)
	for i := 0; i < addLen; i++ {
		s.streamWriteHelpers = append(s.streamWriteHelpers, newWriteHelper(w))
	}
}

func (s *injestionCtx) initStreamAliveShardIdxes(length int) {
	if cap(s.streamAliveShardIdxes) < length {
		s.streamAliveShardIdxes = make([][]int, length)
	} else {
		s.streamAliveShardIdxes = s.streamAliveShardIdxes[:length]
	}
}

func (s *injestionCtx) initStreamShardKeyInfos(length int) {
	if cap(s.streamShardKeyInfos) < length {
		s.streamShardKeyInfos = make([]*meta.ShardKeyInfo, length)
	} else {
		s.streamShardKeyInfos = s.streamShardKeyInfos[:length]
	}
}

func (s *injestionCtx) checkDBRP(database, retentionPolicy string, w *PointsWriter) (err error) {
	// check db and rp validation
	s.db, err = w.MetaClient.Database(database)
	if err != nil {
		return err
	}

	if retentionPolicy == "" {
		retentionPolicy = s.db.DefaultRetentionPolicy
	}

	s.rp, err = s.db.GetRetentionPolicy(retentionPolicy)
	if err != nil {
		return err
	}

	if s.rp.Duration > 0 {
		s.minTime = int64(fasttime.UnixTimestamp()*1e9) - s.rp.Duration.Nanoseconds()
	}
	return
}

// initStreamVar init the var needed by the stream calculation
func (s *injestionCtx) initStreamVar(w *PointsWriter) (err error) {
	dstSis := s.getDstSis()
	streamLen := len(*dstSis)
	if s.stream == nil {
		s.stream = NewStream(w.TSDBStore, w.MetaClient, w.logger, w.timeout)
	}

	s.initStreamDBs(streamLen)
	s.initStreamMSTs(streamLen)
	s.initStreamWriteHelpers(streamLen, w)
	s.initStreamAliveShardIdxes(streamLen)
	s.initStreamShardKeyInfos(streamLen)

	streamDBS := s.getStreamDBs()
	streamMSTs := s.getStreamMSTs()
	streamWHs := s.getWriteHelpers()

	for i := 0; i < streamLen; i++ {
		(*streamDBS)[i], err = w.MetaClient.Database((*dstSis)[i].DesMst.Database)
		if err != nil {
			return
		}

		(*streamMSTs)[i], err = (*streamWHs)[i].createMeasurement((*dstSis)[i].DesMst.Database, (*dstSis)[i].DesMst.RetentionPolicy, (*dstSis)[i].DesMst.Name)
		if err != nil {
			return
		}
	}
	return
}

func (s *injestionCtx) getShardRowMap() ShardRows {
	return s.shardRowMap
}

func (s *injestionCtx) getMstShardIdRowMap() map[string]map[uint64]*[]*influx.Row {
	return s.mstShardIdRowMap
}

func (s *injestionCtx) getDstSis() *[]*meta.StreamInfo {
	return &s.streamInfos
}

func (s *injestionCtx) getWriteHelpers() *[]*writeHelper {
	return &s.streamWriteHelpers
}

func (s *injestionCtx) getWriteHelper(w *PointsWriter) *writeHelper {
	if s.writeHelper == nil {
		s.writeHelper = newWriteHelper(w)
	}
	return s.writeHelper
}

func (s *injestionCtx) getStreamDBs() *[]*meta.DatabaseInfo {
	return &s.streamDBs
}

func (s *injestionCtx) getStreamShardKeyInfos() []*meta.ShardKeyInfo {
	return s.streamShardKeyInfos
}

func (s *injestionCtx) getStreamMSTs() *[]*meta.MeasurementInfo {
	return &s.streamMSTs
}

func (s *injestionCtx) getStreamAliveShardIdxes() *[][]int {
	return &s.streamAliveShardIdxes
}

func (s *injestionCtx) getSrcStreamDstShardIdMap() map[uint64]map[uint64]uint64 {
	return s.srcStreamDstShardIdMap
}

func (s *injestionCtx) getPRowsPool() *[]*influx.Row {
	v := s.pRowsPool.Get()
	if v == nil {
		return &[]*influx.Row{}
	}
	return v.(*[]*influx.Row)
}

func (s *injestionCtx) allocWriteContext(shard *meta.ShardInfo, rs []*influx.Row) *netstorage.WriteContext {
	size := len(s.writeCtx)
	if cap(s.writeCtx) == len(s.writeCtx) {
		s.writeCtx = append(s.writeCtx, &netstorage.WriteContext{})
	}

	s.writeCtx = s.writeCtx[:size+1]
	ctx := s.writeCtx[size]
	if ctx == nil {
		ctx = &netstorage.WriteContext{}
		s.writeCtx[size] = ctx
	}

	ctx.StreamShards = ctx.StreamShards[:0]
	ctx.Shard = shard
	ctx.Rows = copyRows(rs, ctx.Rows[:0])
	return ctx
}

func copyRows(src []*influx.Row, dst []influx.Row) []influx.Row {
	if cap(dst) >= len(src) {
		dst = dst[:len(src)]
	} else {
		dst = make([]influx.Row, len(src))
	}

	for i := range dst {
		dst[i].Clone(src[i])
	}

	return dst
}
