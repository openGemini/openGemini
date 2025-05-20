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
	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

var (
	migrateTimeout            = 5 * time.Second
	segregateTimeout          = 5 * time.Second
	transferLeadershipTimeout = 20 * time.Second
)

const (
	PackageTypeFast = byte(2)
)

type Storage interface {
	WriteRows(ctx *WriteContext, nodeID uint64, pt uint32, database, rpName string, timeout time.Duration) error
	DropShard(nodeID uint64, database, rpName string, dbPts []uint32, shardID uint64) error

	TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr, limit int, exact bool) (TablesTagSets, error)
	TagValuesCardinality(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr) (map[string]uint64, error)

	ShowTagKeys(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr) ([]string, error)

	ShowSeries(nodeID uint64, db string, ptId []uint32, measurements []string, condition influxql.Expr, exact bool) ([]string, error)
	SeriesCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) ([]meta2.MeasurementCardinalityInfo, error)
	SeriesExactCardinality(nodeID uint64, db string, dbPts []uint32, measurements []string, condition influxql.Expr) (map[string]uint64, error)

	SendQueryRequestOnNode(nodeID uint64, req SysCtrlRequest) (map[string]string, error)
	SendSysCtrlOnNode(nodID uint64, req SysCtrlRequest) (map[string]string, error)

	GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32, shardId uint64, idxes []int64) ([]string, error)
	DeleteDatabase(node *meta2.DataNode, database string, pt uint32) error
	DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, pt uint32) error
	DeleteMeasurement(node *meta2.DataNode, db string, rp string, name string, shardIds []uint64) error
	MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error

	GetQueriesOnNode(nodeID uint64) ([]*QueryExeInfo, error)
	KillQueryOnNode(nodeID, queryID uint64) error
	SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error)

	Ping(nodeID uint64, address string, timeout time.Duration) error
	TransferLeadership(database string, nodeId uint64, oldMasterPtId, newMasterPtId uint32) error

	SendRaftMessageToStorage
}

type SendRaftMessageToStorage interface {
	SendRaftMessages(nodeID uint64, database string, pt uint32, msgs raftpb.Message) error
	Client() meta.MetaClient
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

func (s *NetStorage) WriteRows(ctx *WriteContext, nodeID uint64, pt uint32, database, rpName string, timeout time.Duration) error {
	rows := ctx.Rows
	if len(rows) == 0 {
		return nil
	}

	// Determine the location of this shard and whether it still exists
	db, rp, sgi := s.metaClient.ShardOwner(ctx.Shard.ID)
	if sgi == nil {
		return nil
	}

	if db != database || rp != rpName {
		return fmt.Errorf("exp db: %v, rp: %v, but got: %v, %v", database, rpName, db, rp)
	}

	pBuf, err := MarshalRows(ctx, db, rp, pt)
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

	if len(ctx.StreamShards) > 0 {
		streamVars := make([]*StreamVar, len(rows))
		for i := range rows {
			streamVars[i] = &StreamVar{}
			streamVars[i].Only = rows[i].StreamOnly
			streamVars[i].Id = rows[i].StreamId
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

func (s *NetStorage) TagValues(nodeID uint64, db string, ptIDs []uint32, tagKeys map[string]map[string]struct{}, cond influxql.Expr, limit int, exact bool) (TablesTagSets, error) {
	req := &ShowTagValuesRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Exact = &exact
	if cond != nil {
		req.Condition = proto.String(cond.String())
	}
	req.SetTagKeys(tagKeys)
	req.Limit = proto.Int(limit)

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

func (s *NetStorage) ShowTagKeys(nodeID uint64, db string, ptIDs []uint32, measurements []string, condition influxql.Expr) ([]string, error) {
	req := &ShowTagKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Measurements = measurements
	if condition != nil {
		req.Condition = proto.String(condition.String())
	}

	v, err := s.ddlRequestWithNodeId(nodeID, ShowTagKeysRequestMessage, req)
	if err != nil {
		return nil, err
	}

	resp, ok := v.(*ShowTagKeysResponse)
	if !ok {
		return nil, executor.NewInvalidTypeError("*netstorage.ShowTagKeysResponse", v)
	}

	return resp.TagKeys, resp.Error()
}

func (s *NetStorage) ShowSeries(nodeID uint64, db string, ptIDs []uint32, measurements []string, condition influxql.Expr, exact bool) ([]string, error) {
	req := &SeriesKeysRequest{}
	req.Db = proto.String(db)
	req.PtIDs = ptIDs
	req.Exact = &exact
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

func (s *NetStorage) SendQueryRequestOnNode(nodeID uint64, req SysCtrlRequest) (map[string]string, error) {
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
	return resp.Result(), nil
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
	if err = trans.Send(data); err != nil {
		return err
	}
	mcb := cb.(*MigratePtCallback)
	go func() {
		if err := trans.Wait(); err != nil {
			mcb.fn(err)
		}
	}()
	return nil
}

func (s *NetStorage) SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error) {
	for i, nodeId := range nodeIDs {
		segregateNodeReq := NewSegregateNodeRequest()
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
	transferLeadershipReq := NewTransferLeadershipRequest()
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

func (s *NetStorage) KillQueryOnNode(nodeID, queryID uint64) error {
	req := &KillQueryRequest{}
	req.QueryID = proto.Uint64(queryID)
	v, err := s.ddlRequestWithNodeId(nodeID, KillQueryRequestMessage, req)
	if err != nil {
		return err
	}
	resp, ok := v.(*KillQueryResponse)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.KillQueryResponse", v)
	}
	return errno.NewError(errno.Errno(resp.GetErrCode()))
}

func (s *NetStorage) SendRaftMessages(nodeID uint64, database string, pt uint32, msgs raftpb.Message) error {
	req := &RaftMessagesRequest{}
	req.Database = database
	req.PtId = pt
	req.RaftMessage = msgs

	node, err := s.Client().DataNode(nodeID)
	if err != nil || node.Status != serf.StatusAlive {
		return nil
	}

	v, err := s.raftRequestWithNodeId(nodeID, RaftMessagesRequestMessage, req)
	if err != nil {
		return err
	}
	resp, ok := v.(*RaftMessagesResponse)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.RaftMsgResponse", v)
	}
	if resp.GetErrMsg() != "" {
		return errors.New(resp.GetErrMsg())
	}
	return nil
}

func (s *NetStorage) raftRequestWithNodeId(nodeID uint64, typ uint8, data codec.BinaryCodec) (interface{}, error) {
	r := NewRequester(typ, data, s.metaClient)
	r.setToInsert()
	r.setTimeout(config.RaftMsgTimeout)
	err := r.initWithNodeID(nodeID)
	if err != nil {
		return nil, err
	}

	return r.raftMsg()
}

func (s *NetStorage) Ping(nodeID uint64, address string, timeout time.Duration) error {
	trans, err := transport.NewTransportByAddress(nodeID, address, spdy.PingRequest, nil)
	if err != nil {
		return err
	}
	trans.SetTimeout(timeout)
	if err = trans.Send(NewPingRequest()); err != nil {
		return err
	}
	return trans.Wait()
}
