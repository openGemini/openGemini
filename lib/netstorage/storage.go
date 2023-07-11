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
	"fmt"
	"time"

	numenc "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

var (
	debugEn        = true
	migrateTimeout = 5 * time.Second
)

const (
	PackageTypeFast = byte(2)
)

type Storage interface {
	Open() error
	Close() error

	WriteRows(nodeID uint64, database string, rp string, pt uint32, shard uint64, streamShardIdList []uint64, rows *[]influx.Row, timeout time.Duration) error
	DropShard(nodeID uint64, database, rpName string, dbPts []uint32, shardID uint64) error

	TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr) (TablesTagSets, error)
	TagValuesCardinality(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr) (map[string]uint64, error)

	ShowSeries(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr) ([]string, error)
	SeriesCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) ([]meta2.MeasurementCardinalityInfo, error)
	SeriesExactCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) (map[string]uint64, error)

	SendSysCtrlOnNode(nodID uint64, req SysCtrlRequest) (map[string]string, error)

	GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32,
		shardId uint64, idxes []int64) ([]string, error)
	DeleteDatabase(node *meta2.DataNode, database string, pt uint32) error
	DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, pt uint32) error
	DeleteMeasurement(node *meta2.DataNode, db string, rp string, name string, shardIds []uint64) error
	MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error

	GetQueriesOnNode(nodeID uint64) ([]*QueryExeInfo, error)
}

type NetStorage struct {
	metaClient meta.MetaClient
	log        *logger.Logger
}

func NewNetStorage(mcli meta.MetaClient) Storage {
	return &NetStorage{
		metaClient: mcli,
		log:        logger.NewLogger(errno.ModuleNetwork).With(zap.String("service", "netstorage")),
	}
}

func (s *NetStorage) Open() error {
	return nil
}

func (s *NetStorage) Close() error {
	return nil
}

func (s *NetStorage) GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32,
	shardId uint64, idxes []int64) ([]string, error) {

	req := &GetShardSplitPointsRequest{}
	req.DB = proto.String(database)
	req.PtID = proto.Uint32(pt)
	req.ShardID = proto.Uint64(shardId)
	req.Idxes = idxes

	v, err := s.ddlRequestWithNode(node, GetShardSplitPointsRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*GetShardSplitPointsResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.GetShardSplitPointsResponse", v)
	}

	return resp.SplitPoints, resp.Error()
}

func (s *NetStorage) HandleDeleteReq(node *meta2.DataNode, req *DeleteRequest) error {
	v, err := s.ddlRequestWithNode(node, DeleteRequestMessage, req)
	if err != nil {
		return err
	}

	resp, ok := v.(*DeleteResponse)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.DeleteResponse", v)
	}

	return resp.Err
}

func (s *NetStorage) DeleteMeasurement(node *meta2.DataNode, db string, rp string, name string, shardIds []uint64) error {
	deleteReq := &DeleteRequest{
		Type:        MeasurementDelete,
		Database:    db,
		ShardIds:    shardIds,
		Rp:          rp,
		Measurement: name,
	}
	return s.HandleDeleteReq(node, deleteReq)
}

func (s *NetStorage) DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, pt uint32) error {
	deleteReq := &DeleteRequest{
		Type:     RetentionPolicyDelete,
		Database: db,
		Rp:       rp,
		PtId:     pt,
	}

	return s.HandleDeleteReq(node, deleteReq)
}

func (s *NetStorage) DeleteDatabase(node *meta2.DataNode, database string, pt uint32) error {
	deleteReq := &DeleteRequest{
		Type:     DatabaseDelete,
		Database: database,
		PtId:     pt,
	}

	return s.HandleDeleteReq(node, deleteReq)
}

func (s *NetStorage) WriteRows(nodeID uint64, database string, rpName string, pt uint32, shard uint64, streamShardIdList []uint64, rows *[]influx.Row, timeout time.Duration) error {
	if len(*rows) == 0 {
		return nil
	}
	var err error

	// Determine the location of this shard and whether it still exists
	db, rp, sgi := s.metaClient.ShardOwner(shard)
	if sgi == nil {
		return nil
	}

	if db != database || rp != rpName {
		return fmt.Errorf("exp db: %v, rp: %v, but got: %v, %v", database, rpName, db, rp)
	}

	pBuf := bufferpool.GetPoints()
	defer func() {
		bufferpool.PutPoints(pBuf)
	}()

	pBuf = append(pBuf[:0], PackageTypeFast)
	// db
	pBuf = append(pBuf, uint8(len(db)))
	pBuf = append(pBuf, db...)
	// rp
	pBuf = append(pBuf, uint8(len(rp)))
	pBuf = append(pBuf, rp...)
	// ptid
	pBuf = numenc.MarshalUint32(pBuf, pt)
	pBuf = numenc.MarshalUint64(pBuf, shard)

	// streamShardIdList
	pBuf = numenc.MarshalUint32(pBuf, uint32(len(streamShardIdList)))
	pBuf = numenc.MarshalVarUint64s(pBuf, streamShardIdList)

	pBuf, err = influx.FastMarshalMultiRows(pBuf, *rows)
	if err != nil {
		return err
	}

	r := NewRequester(0, nil, s.metaClient)
	r.setToInsert()
	r.setTimeout(timeout)
	err = r.initWithNodeID(nodeID)
	if err != nil {
		return err
	}

	if len(streamShardIdList) > 0 {
		streamVars := make([]*StreamVar, len(*rows))
		for i := range *rows {
			streamVars[i] = &StreamVar{}
			streamVars[i].Only = (*rows)[i].StreamOnly
			streamVars[i].Id = (*rows)[i].StreamId
		}
		cb := &WriteStreamPointsCallback{}
		err = r.request(spdy.WriteStreamPointsRequest, NewWriteStreamPointsRequest(pBuf, streamVars), cb)
		if err != nil {
			return err
		}
		return nil

	}
	cb := &WritePointsCallback{}
	err = r.request(spdy.WritePointsRequest, NewWritePointsRequest(pBuf), cb)
	if err != nil {
		return err
	}
	return nil
}

func (s *NetStorage) ddlRequestWithNodeId(nodeID uint64, typ uint8, data codec.BinaryCodec) (interface{}, error) {
	r := NewRequester(typ, data, s.metaClient)
	err := r.initWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	return r.ddl()
}

func (s *NetStorage) ddlRequestWithNode(node *meta2.DataNode, typ uint8, data codec.BinaryCodec) (interface{}, error) {
	r := NewRequester(typ, data, s.metaClient)
	r.initWithNode(node)

	return r.ddl()
}

func (s *NetStorage) TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr) (TablesTagSets, error) {
	req := &ShowTagValuesRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	if cond != nil {
		req.Condition = proto.String(cond.String())
	}
	req.SetTagKeys(tagKeys)

	v, err := s.ddlRequestWithNodeId(nodeID, ShowTagValuesRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*ShowTagValuesResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.ShowTagValuesResponse", v)
	}

	s.log.Debug("[ShowTagValuesRequestMessage] request success", zap.Uint64("nodeID", nodeID))
	return resp.GetTagValuesSlice(), resp.Error()
}

func (s *NetStorage) TagValuesCardinality(nodeID uint64, db string, ptIDs []uint32,
	tagKeys map[string]map[string]struct{}, cond influxql.Expr) (map[string]uint64, error) {

	req := &ShowTagValuesRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	if cond != nil {
		req.Condition = proto.String(cond.String())
	}
	req.SetTagKeys(tagKeys)

	v, err := s.ddlRequestWithNodeId(nodeID, ShowTagValuesCardinalityRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*ShowTagValuesCardinalityResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.ShowTagValuesCardinalityResponse", v)
	}

	return resp.GetCardinality(), resp.Error()
}

func (s *NetStorage) SeriesCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) ([]meta2.MeasurementCardinalityInfo, error) {
	req := &SeriesKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = dbPts
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, SeriesCardinalityRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*SeriesCardinalityResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.SeriesCardinalityResponse", v)
	}

	if resp.Err != nil {
		errStr := resp.Err.Error()
		return resp.CardinalityInfos, NormalizeError(&errStr)
	}
	return resp.CardinalityInfos, nil
}

func (s *NetStorage) SeriesExactCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) (map[string]uint64, error) {
	req := &SeriesKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = dbPts
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, SeriesExactCardinalityRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*SeriesExactCardinalityResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.SeriesExactCardinalityResponse", v)
	}

	return resp.Cardinality, resp.Error()
}

func (s *NetStorage) ShowSeries(nodeID uint64, db string, ptIDs []uint32, measurements []string, condition influxql.Expr) ([]string, error) {
	req := &SeriesKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, SeriesKeysRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*SeriesKeysResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.SeriesKeysResponse", v)
	}

	return resp.Series, resp.Error()
}

func (s *NetStorage) DropShard(nodeID uint64, database, rpName string, dbPts []uint32, shardID uint64) error {
	return nil
}

func (s *NetStorage) SendSysCtrlOnNode(nodeID uint64, req SysCtrlRequest) (map[string]string, error) {
	r := NewRequester(0, nil, s.metaClient)
	err := r.initWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	v, err := r.sysCtrl(&req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*SysCtrlResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.SysCtrlResponse", v)
	}

	ret := map[string]string{r.node.TCPHost: "success"}
	if resp.Error() != nil {
		ret[r.node.TCPHost] = "failure"
	}

	return ret, nil
}

func (s *NetStorage) MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error {
	trans, err := transport.NewTransport(nodeID, spdy.PtRequest, cb)
	if err != nil {
		return err
	}
	trans.SetTimeout(migrateTimeout)
	if err := trans.Send(data); err != nil {
		return err
	}
	mcb := cb.(*MigratePtCallback)
	go func() {
		err := trans.Wait()
		if err != nil {
			mcb.fn(err)
		}
	}()
	return nil
}

func (s *NetStorage) GetQueriesOnNode(nodeID uint64) ([]*QueryExeInfo, error) {
	req := &ShowQueriesRequest{}
	v, err := s.ddlRequestWithNodeId(nodeID, ShowQueriesRequestMessage, req)
	if err != nil {
		return nil, err
	}
	resp, ok := v.(*ShowQueriesResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.ShowQueriesResponse", v)
	}
	return resp.QueryExeInfos, nil
}
