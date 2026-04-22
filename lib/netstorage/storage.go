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

package netstorage

import (
	"fmt"
	"time"

	numenc "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

var (
	migrateTimeout            = 5 * time.Second
	segregateTimeout          = 5 * time.Second
	transferLeadershipTimeout = 20 * time.Second
	sendClearEvent            = 5 * time.Second
)

const (
	PackageTypeFast = byte(2)
)

type Storage interface {
	WriteRows(ctx *WriteContext, nodeID uint64, pt uint32, database, rpName string, timeout time.Duration) error
	DropShard(nodeID uint64, database, rpName string, dbPts []uint32, shardID uint64) error

	TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr, limit int, exact bool) (influxql.TablesTagSets, error)
	TagValuesCardinality(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr) (map[string]uint64, error)

	ShowTagKeys(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr) ([]string, error)

	ShowSeries(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr, exact bool) ([]string, error)
	DropSeries(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr) error

	SeriesCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) ([]meta2.MeasurementCardinalityInfo, error)
	SeriesExactCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) (map[string]uint64, error)

	GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32, shardId uint64, idxes []int64) ([]string, error)
	DeleteDatabase(node *meta2.DataNode, database string, pt uint32) error
	DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, pt uint32) error
	DeleteMeasurement(node *meta2.DataNode, db string, rp string, name string, shardIds []uint64) error
	MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error

	GetQueriesOnNode(nodeID uint64) ([]*msgservice.QueryExeInfo, error)
	KillQueryOnNode(nodeID, queryID uint64) error
	SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error)

	TransferLeadership(database string, nodeId uint64, oldMasterPtId, newMasterPtId uint32) error
	SendClearEvents(nodeId uint64, data transport.Codec) error
}

type NetStorage struct {
	metaClient meta.MetaClient
	log        *logger.Logger
}

type WriteContext struct {
	Rows         []influx.Row
	Shard        *meta2.ShardInfo
	Buf          []byte
	StreamShards []uint64
}

func NewNetStorage(mcli meta.MetaClient) Storage {
	return &NetStorage{
		metaClient: mcli,
		log:        logger.NewLogger(errno.ModuleNetwork).With(zap.String("service", "netstorage")),
	}
}

func (s *NetStorage) Client() meta.MetaClient {
	return s.metaClient
}

func (s *NetStorage) GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32,
	shardId uint64, idxes []int64) ([]string, error) {

	req := &msgservice.GetShardSplitPointsRequest{}
	req.DB = proto.String(database)
	req.PtID = proto.Uint32(pt)
	req.ShardID = proto.Uint64(shardId)
	req.Idxes = idxes

	v, err := s.ddlRequestWithNode(node, msgservice.GetShardSplitPointsRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.GetShardSplitPointsResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.GetShardSplitPointsResponse", v)
	}

	return resp.SplitPoints, resp.Error()
}

func (s *NetStorage) handleDeleteReq(node *meta2.DataNode, req *msgservice.DeleteRequest) error {
	v, err := s.ddlRequestWithNode(node, msgservice.DeleteRequestMessage, req)
	if err != nil {
		return err
	}

	resp, ok := v.(*msgservice.DeleteResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.DeleteResponse", v)
	}

	return resp.Err
}

func (s *NetStorage) DeleteMeasurement(node *meta2.DataNode, db string, rp string, name string, shardIds []uint64) error {
	deleteReq := &msgservice.DeleteRequest{
		Type:        msgservice.MeasurementDelete,
		Database:    db,
		ShardIds:    shardIds,
		Rp:          rp,
		Measurement: name,
	}
	return s.handleDeleteReq(node, deleteReq)
}

func (s *NetStorage) DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, pt uint32) error {
	deleteReq := &msgservice.DeleteRequest{
		Type:     msgservice.RetentionPolicyDelete,
		Database: db,
		Rp:       rp,
		PtId:     pt,
	}

	return s.handleDeleteReq(node, deleteReq)
}

func (s *NetStorage) DeleteDatabase(node *meta2.DataNode, database string, pt uint32) error {
	deleteReq := &msgservice.DeleteRequest{
		Type:     msgservice.DatabaseDelete,
		Database: database,
		PtId:     pt,
	}

	return s.handleDeleteReq(node, deleteReq)
}

func (s *NetStorage) WriteRows(ctx *WriteContext, nodeID uint64, pt uint32, database, rp string, timeout time.Duration) error {
	rows := ctx.Rows
	if len(rows) == 0 {
		return nil
	}

	// Determine the location of this shard and whether it still exists
	db, rpName, sgi := s.metaClient.ShardOwner(ctx.Shard.ID)
	if sgi == nil {
		return fmt.Errorf("shard group not found, shardID: %d", ctx.Shard.ID)
	}

	if db != database || rpName != rp {
		return fmt.Errorf("exp db: %v, rp: %v, but got: %v, %v", database, rp, db, rpName)
	}

	pBuf, err := MarshalRows(ctx, db, rp, pt)
	if err != nil {
		return err
	}

	r := msgservice.NewRequester(0, nil, s.metaClient)
	r.SetToInsert()
	r.SetTimeout(timeout)

	err = r.InitWithNodeID(nodeID)
	if err != nil {
		return err
	}

	if len(ctx.StreamShards) > 0 {
		streamVars := make([]*msgservice.StreamVar, len(rows))
		for i := range rows {
			streamVars[i] = &msgservice.StreamVar{}
			streamVars[i].Only = rows[i].StreamOnly
			streamVars[i].Id = rows[i].StreamId
		}
		cb := &msgservice.WriteStreamPointsCallback{}
		err = r.Request(spdy.WriteStreamPointsRequest, msgservice.NewWriteStreamPointsRequest(pBuf, streamVars), cb)
		if err != nil {
			return err
		}
		return nil
	}
	cb := &msgservice.WritePointsCallback{}
	err = r.Request(spdy.WritePointsRequest, msgservice.NewWritePointsRequest(pBuf), cb)
	if err != nil {
		return err
	}
	return nil
}

func (s *NetStorage) ddlRequestWithNodeId(nodeID uint64, typ uint8, data codec.BinaryCodec) (interface{}, error) {
	r := msgservice.NewRequester(typ, data, s.metaClient)
	err := r.InitWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	return r.DDL()
}

func (s *NetStorage) ddlRequestWithNode(node *meta2.DataNode, typ uint8, data codec.BinaryCodec) (interface{}, error) {
	r := msgservice.NewRequester(typ, data, s.metaClient)
	r.InitWithNode(node)

	return r.DDL()
}

func (s *NetStorage) TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr, limit int, exact bool) (influxql.TablesTagSets, error) {
	req := &msgservice.ShowTagValuesRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Exact = &exact
	if cond != nil {
		req.Condition = proto.String(cond.String())
	}
	req.SetTagKeys(tagKeys)
	req.Limit = proto.Int(limit)

	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.ShowTagValuesRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.ShowTagValuesResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.ShowTagValuesResponse", v)
	}

	s.log.Debug("[ShowTagValuesRequestMessage] request success", zap.Uint64("nodeID", nodeID))
	return resp.GetTagValuesSlice(), resp.Error()
}

func (s *NetStorage) TagValuesCardinality(nodeID uint64, db string, ptIDs []uint32,
	tagKeys map[string]map[string]struct{}, cond influxql.Expr) (map[string]uint64, error) {

	req := &msgservice.ShowTagValuesRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	if cond != nil {
		req.Condition = proto.String(cond.String())
	}
	req.SetTagKeys(tagKeys)

	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.ShowTagValuesCardinalityRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.ShowTagValuesCardinalityResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.ShowTagValuesCardinalityResponse", v)
	}

	return resp.GetCardinality(), resp.Error()
}

func (s *NetStorage) SeriesCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) ([]meta2.MeasurementCardinalityInfo, error) {
	req := &msgservice.SeriesKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = dbPts
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.SeriesCardinalityRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.SeriesCardinalityResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.SeriesCardinalityResponse", v)
	}

	if resp.Err != nil {
		errStr := resp.Err.Error()
		return nil, msgservice.NormalizeError(&errStr)
	}
	return resp.CardinalityInfos, nil
}

func (s *NetStorage) SeriesExactCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) (map[string]uint64, error) {
	req := &msgservice.SeriesKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = dbPts
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.SeriesExactCardinalityRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.SeriesExactCardinalityResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.SeriesExactCardinalityResponse", v)
	}

	return resp.Cardinality, resp.Error()
}

func (s *NetStorage) ShowTagKeys(nodeID uint64, db string, ptIDs []uint32, measurements []string, condition influxql.Expr) ([]string, error) {
	req := &msgservice.ShowTagKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.ShowTagKeysRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.ShowTagKeysResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.ShowTagKeysResponse", v)
	}

	return resp.TagKeys, resp.Error()
}

func (s *NetStorage) DropSeries(nodeID uint64, db string, ptIDs []uint32, measurements []string, condition influxql.Expr) error {
	req := &msgservice.DropSeriesRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.DropSeriesRequestMessage, req)
	if err != nil {
		return err
	}

	resp, ok := v.(*msgservice.DropSeriesResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.DropSeriesResponse", v)
	}

	return resp.Error()
}

func (s *NetStorage) ShowSeries(nodeID uint64, db string, ptIDs []uint32, measurements []string, condition influxql.Expr, exact bool) ([]string, error) {
	req := &msgservice.SeriesKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Exact = &exact
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.SeriesKeysRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*msgservice.SeriesKeysResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.SeriesKeysResponse", v)
	}

	return resp.Series, resp.Error()
}

func (s *NetStorage) DropShard(nodeID uint64, database, rpName string, dbPts []uint32, shardID uint64) error {
	return nil
}

func (s *NetStorage) MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error {
	trans, err := transport.NewTransport(nodeID, spdy.PtRequest, cb)
	if err != nil {
		return err
	}
	trans.SetTimeout(migrateTimeout)
	if err = trans.Send(data); err != nil {
		return err
	}
	mcb := cb.(*msgservice.MigratePtCallback)
	go func() {
		if err := trans.Wait(); err != nil {
			mcb.CallFn(err)
		}
	}()
	return nil
}

func (s *NetStorage) SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error) {
	for i, nodeId := range nodeIDs {
		segregateNodeReq := msgservice.NewSegregateNodeRequest()
		segregateNodeReq.NodeId = &nodeId
		trans, err := transport.NewTransportByAddress(nodeId, address[i], spdy.SegregateNodeRequest, nil)
		if err != nil {
			return i, err
		}
		trans.SetTimeout(segregateTimeout)
		if err := trans.Send(segregateNodeReq); err != nil {
			return i, err
		}
		err = trans.Wait()
		if err != nil {
			return i, err
		}
	}
	return -1, nil
}

func (s *NetStorage) TransferLeadership(database string, nodeId uint64, oldMasterPtId, newMasterPtId uint32) error {
	transferLeadershipReq := msgservice.NewTransferLeadershipRequest()
	transferLeadershipReq.NodeId = &nodeId
	transferLeadershipReq.Database = &database
	transferLeadershipReq.PtId = &oldMasterPtId
	transferLeadershipReq.NewMasterPtId = &newMasterPtId
	trans, err := transport.NewTransport(nodeId, spdy.TransferLeadershipRequest, nil)
	if err != nil {
		return err
	}
	trans.SetTimeout(transferLeadershipTimeout)
	if err := trans.Send(transferLeadershipReq); err != nil {
		return err
	}
	err = trans.Wait()
	return err
}

func (s *NetStorage) SendClearEvents(nodeId uint64, data transport.Codec) error {
	s.log.Info("start send clear event", zap.Uint64("nodeId", nodeId))
	trans, err := transport.NewTransport(nodeId, spdy.SendClearEvent, nil)
	if err != nil {
		return err
	}
	trans.SetTimeout(sendClearEvent)
	if err := trans.Send(data); err != nil {
		return err
	}
	err = trans.Wait()
	return err
}

func MarshalRows(ctx *WriteContext, db, rp string, pt uint32) ([]byte, error) {
	pBuf := append(ctx.Buf[:0], PackageTypeFast)
	// db
	pBuf = append(pBuf, uint8(len(db)))
	pBuf = append(pBuf, db...)
	// rp
	pBuf = append(pBuf, uint8(len(rp)))
	pBuf = append(pBuf, rp...)
	// ptid
	pBuf = numenc.MarshalUint32(pBuf, pt)
	pBuf = numenc.MarshalUint64(pBuf, ctx.Shard.ID)

	// streamShardIdList
	pBuf = numenc.MarshalUint32(pBuf, uint32(len(ctx.StreamShards)))
	pBuf = numenc.MarshalVarUint64s(pBuf, ctx.StreamShards)

	var err error
	pBuf, err = influx.FastMarshalMultiRows(pBuf, ctx.Rows)
	if err != nil {
		return nil, err
	}
	ctx.Buf = pBuf
	return pBuf, err
}

func (s *NetStorage) GetQueriesOnNode(nodeID uint64) ([]*msgservice.QueryExeInfo, error) {
	req := &msgservice.ShowQueriesRequest{}
	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.ShowQueriesRequestMessage, req)
	if err != nil {
		return nil, err
	}
	resp, ok := v.(*msgservice.ShowQueriesResponse)
	if !ok {
		return nil, errno.NewInvalidTypeError("*msgservice.ShowQueriesResponse", v)
	}
	return resp.QueryExeInfos, nil
}

func (s *NetStorage) KillQueryOnNode(nodeID, queryID uint64) error {
	req := &msgservice.KillQueryRequest{}
	req.QueryID = proto.Uint64(queryID)
	v, err := s.ddlRequestWithNodeId(nodeID, msgservice.KillQueryRequestMessage, req)
	if err != nil {
		return err
	}
	resp, ok := v.(*msgservice.KillQueryResponse)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.KillQueryResponse", v)
	}
	return errno.NewError(errno.Errno(resp.GetErrCode()))
}

func PingNode(nodeID uint64, address string, timeout time.Duration) error {
	trans, err := transport.NewTransportByAddress(nodeID, address, spdy.PingRequest, nil)
	if err != nil {
		return err
	}
	trans.SetTimeout(timeout)
	if err = trans.Send(msgservice.NewPingRequest()); err != nil {
		return err
	}
	return trans.Wait()
}
