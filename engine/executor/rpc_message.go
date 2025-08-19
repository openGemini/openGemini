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

package executor

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/spdy/rpc"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/query/proto"
	"google.golang.org/protobuf/proto"
)

const IncQueryFinishLength = util.BooleanSizeBytes*2 + util.Int32SizeBytes + util.Int64SizeBytes

func NewRPCMessage(typ uint8) transport.Codec {
	var data transport.Codec

	switch typ {
	case ErrorMessage:
		data = &Error{}
	case ChunkResponseMessage:
		data = &ChunkImpl{}
	case AnalyzeResponseMessage:
		data = &AnalyzeResponse{}
	case QueryMessage:
		data = &RemoteQuery{}
	case FinishMessage:
		data = &Finish{}
	case IncQueryFinishMessage:
		data = &IncQueryFinish{}
	default:
		return nil
	}
	return data
}

type Error struct {
	errCode errno.Errno
	data    string
}

func NewErrorMessage(errCode errno.Errno, err string) *rpc.Message {
	return rpc.NewMessage(ErrorMessage, &Error{errCode: errCode, data: err})
}

func (e *Error) Marshal(buf []byte) ([]byte, error) {
	buf = encoding.MarshalUint16(buf, uint16(e.errCode))
	buf = append(buf, e.data...)
	return buf, nil
}

func (e *Error) Unmarshal(buf []byte) error {
	e.errCode = errno.Errno(encoding.UnmarshalUint16(buf[:2]))
	e.data = string(buf[2:])
	return nil
}

func (e *Error) Size() int {
	return 2 + len(e.data)
}

func (e *Error) Instance() transport.Codec {
	return &Error{}
}

type Finish struct {
	queryIndexState int32
}

func NewFinishMessage(queryIndexState int32) *rpc.Message {
	return rpc.NewMessage(FinishMessage, &Finish{queryIndexState: queryIndexState})
}

func (e *Finish) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendInt32(buf, e.queryIndexState)
	return buf, nil
}

func (e *Finish) Unmarshal(buf []byte) error {
	if len(buf) < util.Int32SizeBytes {
		return fmt.Errorf("invalid the finish length")
	}
	dec := codec.NewBinaryDecoder(buf)
	e.queryIndexState = dec.Int32()
	return nil
}

func (e *Finish) Size() int {
	return util.Int32SizeBytes
}

func (e *Finish) Instance() transport.Codec {
	return &Finish{}
}

type IncQueryFinish struct {
	isIncQuery bool
	getFailed  bool
	queryID    string
	iterMaxNum int32
	rowCount   int64
}

func NewIncQueryFinishMessage(isIncQuery, getFailed bool, queryID string, iterMaxNum int32, rowCount int64) *rpc.Message {
	return rpc.NewMessage(IncQueryFinishMessage, &IncQueryFinish{isIncQuery: isIncQuery, getFailed: getFailed, queryID: queryID, iterMaxNum: iterMaxNum, rowCount: rowCount})
}

func (e *IncQueryFinish) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBool(buf, e.isIncQuery)
	buf = codec.AppendBool(buf, e.getFailed)
	buf = codec.AppendString(buf, e.queryID)
	buf = codec.AppendInt32(buf, e.iterMaxNum)
	buf = codec.AppendInt64(buf, e.rowCount)
	return buf, nil
}

func (e *IncQueryFinish) Unmarshal(buf []byte) error {
	if len(buf) < IncQueryFinishLength {
		return fmt.Errorf("invalid the IncQueryFinish length")
	}
	dec := codec.NewBinaryDecoder(buf)
	e.isIncQuery = dec.Bool()
	e.getFailed = dec.Bool()
	e.queryID = dec.String()
	e.iterMaxNum = dec.Int32()
	e.rowCount = dec.Int64()
	return nil
}

func (e *IncQueryFinish) Size() int {
	return util.BooleanSizeBytes*2 + util.Int32SizeBytes + util.Int64SizeBytes + len(e.queryID) // isIncQuery + getFailed + iterMaxNum + rowCount + queryID
}

func (e *IncQueryFinish) Instance() transport.Codec {
	return &IncQueryFinish{}
}

type Abort struct {
	ClientID    uint64
	QueryID     uint64
	NoMarkCrash bool
}

func NewAbort(queryID uint64, clientID uint64, noMarkCrash bool) *Abort {
	return &Abort{ClientID: clientID, QueryID: queryID, NoMarkCrash: noMarkCrash}
}

func (e *Abort) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendUint64(buf, e.ClientID)
	buf = codec.AppendUint64(buf, e.QueryID)
	buf = codec.AppendBool(buf, e.NoMarkCrash)
	return buf, nil
}

func (e *Abort) Unmarshal(buf []byte) error {
	dec := codec.NewBinaryDecoder(buf)
	e.ClientID = dec.Uint64()
	e.QueryID = dec.Uint64()
	e.NoMarkCrash = dec.Bool()
	return nil
}

func (e *Abort) Size() int {
	return codec.SizeOfUint64()*2 + codec.SizeOfBool()
}

func (e *Abort) Instance() transport.Codec {
	return &Abort{}
}

type Crash struct {
	ClientID    uint64
	QueryID     uint64
	NoMarkCrash bool
}

func NewCrash(queryID uint64, clientID uint64, noMarkCrash bool) *Crash {
	return &Crash{ClientID: clientID, QueryID: queryID, NoMarkCrash: noMarkCrash}
}

func (c *Crash) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendUint64(buf, c.ClientID)
	buf = codec.AppendUint64(buf, c.QueryID)
	buf = codec.AppendBool(buf, c.NoMarkCrash)
	return buf, nil
}

func (c *Crash) Unmarshal(buf []byte) error {
	dec := codec.NewBinaryDecoder(buf)
	c.ClientID = dec.Uint64()
	c.QueryID = dec.Uint64()
	if !dec.End() {
		c.NoMarkCrash = dec.Bool()
	}
	return nil
}

func (c *Crash) Size() int {
	return codec.SizeOfUint64()*2 + codec.SizeOfBool()
}

func (c *Crash) Instance() transport.Codec {
	return &Crash{}
}

type AnalyzeResponse struct {
	trace *tracing.Trace
}

func NewAnalyzeResponse(trace *tracing.Trace) *rpc.Message {
	return rpc.NewMessage(AnalyzeResponseMessage, &AnalyzeResponse{
		trace: trace,
	})
}

func (a *AnalyzeResponse) Marshal(buf []byte) ([]byte, error) {
	b, err := a.trace.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf = append(buf, b...)
	return buf, nil
}

func (a *AnalyzeResponse) Unmarshal(buf []byte) error {
	a.trace, _ = tracing.NewTrace("root")
	return a.trace.UnmarshalBinary(buf)
}

func (a *AnalyzeResponse) Size() int {
	return 0
}

func (a AnalyzeResponse) Instance() transport.Codec {
	return &AnalyzeResponse{}
}

func NewChunkResponse(chunk Chunk) *rpc.Message {
	return rpc.NewMessage(ChunkResponseMessage, chunk)
}

type ShardInfo struct {
	ID      uint64
	Path    string // used for remote storage
	Version uint32 // identify data from different layouts
}

func MarshalShardInfos(shardInfos []ShardInfo) []*proto2.ShardInfo {
	ret := make([]*proto2.ShardInfo, 0, len(shardInfos))
	for _, shardInfo := range shardInfos {
		ret = append(ret, &proto2.ShardInfo{
			ID:      shardInfo.ID,
			Path:    shardInfo.Path,
			Version: shardInfo.Version,
		})
	}
	return ret
}

func UnmarshalShardInfos(shardInfos []*proto2.ShardInfo) []ShardInfo {
	ret := make([]ShardInfo, 0, len(shardInfos))
	for _, shardInfo := range shardInfos {
		ret = append(ret, ShardInfo{
			ID:      shardInfo.GetID(),
			Path:    shardInfo.GetPath(),
			Version: shardInfo.GetVersion(),
		})
	}
	return ret
}

type PtQuery struct {
	PtID       uint32
	ShardInfos []ShardInfo
}

func MarshalPtQuerys(ptQuerys []PtQuery) []*proto2.PtQuery {
	ret := make([]*proto2.PtQuery, 0, len(ptQuerys))
	for _, ptQuery := range ptQuerys {
		ret = append(ret, &proto2.PtQuery{
			PtID:       ptQuery.PtID,
			ShardInfos: MarshalShardInfos(ptQuery.ShardInfos),
		})
	}
	return ret
}

func UnmarshalPtQuerys(ptQuerys []*proto2.PtQuery) []PtQuery {
	ret := make([]PtQuery, 0, len(ptQuerys))
	for _, ptQuery := range ptQuerys {
		ret = append(ret, PtQuery{
			PtID:       ptQuery.GetPtID(),
			ShardInfos: UnmarshalShardInfos(ptQuery.GetShardInfos()),
		})
	}
	return ret
}

type RemoteQuery struct {
	Database string
	PtID     uint32 // for tsstore
	NodeID   uint64
	ShardIDs []uint64  // for tsstore
	PtQuerys []PtQuery // for csstore
	Opt      query.ProcessorOptions
	Analyze  bool
	Node     []byte
	MstInfos []*MultiMstInfo
}

type MultiMstInfo struct {
	ShardIds []uint64
	Opt      query.ProcessorOptions
}

func (c *RemoteQuery) Clone() *RemoteQuery {
	r := &RemoteQuery{}
	r.Database = c.Database
	r.PtID = c.PtID
	r.NodeID = c.NodeID
	r.PtQuerys = c.PtQuerys
	r.Analyze = c.Analyze
	r.Node = c.Node
	return r
}

func (c *RemoteQuery) BuildMstTraits() []*MultiMstReqs {
	if len(c.MstInfos) == 0 {
		return nil
	}
	res := make([]*MultiMstReqs, len(c.MstInfos))
	for i := range c.MstInfos {
		mmr := &MultiMstReqs{}
		rq := c.Clone()
		rq.ShardIDs = c.MstInfos[i].ShardIds
		rq.Opt = c.MstInfos[i].Opt
		mmr.reqs = append(mmr.reqs, rq)
		res[i] = mmr
	}
	return res
}

func (c *RemoteQuery) Marshal(buf []byte) ([]byte, error) {
	opt, err := c.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}
	rq := &proto2.RemoteQuery{
		Database:  c.Database,
		PtID:      c.PtID,
		ShardIDs:  c.ShardIDs,
		NodeID:    c.NodeID,
		Opt:       opt,
		Analyze:   c.Analyze,
		QueryNode: c.Node,
		PtQuerys:  MarshalPtQuerys(c.PtQuerys),
	}

	if err = c.MarshalMstInfos(rq); err != nil {
		return nil, err
	}

	msg, err := proto.Marshal(rq)
	ret := make([]byte, len(buf)+len(msg))
	copy(ret, buf)
	copy(ret[len(buf):], msg)
	return ret, err
}

func (c *RemoteQuery) MarshalMstInfos(rq *proto2.RemoteQuery) error {
	if len(c.MstInfos) == 0 {
		return nil
	}
	var err error
	mstInfos := make([]*proto2.MultiMstInfo, len(c.MstInfos))
	for i := range mstInfos {
		mstInfos[i] = &proto2.MultiMstInfo{}
		mstInfos[i].ShardIDs = c.MstInfos[i].ShardIds
		mstInfos[i].Opt, err = c.MstInfos[i].Opt.MarshalBinary()
		if err != nil {
			return err
		}
	}
	rq.MultiMstInfos = mstInfos
	return nil
}

func (c *RemoteQuery) Unmarshal(buf []byte) error {
	var pb proto2.RemoteQuery
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	c.Database = pb.GetDatabase()
	c.PtID = pb.GetPtID()
	c.ShardIDs = pb.GetShardIDs()
	c.NodeID = pb.GetNodeID()
	c.Analyze = pb.GetAnalyze()
	c.Node = pb.QueryNode
	c.PtQuerys = UnmarshalPtQuerys(pb.GetPtQuerys())
	if err := c.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}
	return c.UnmarshalMstInfos(&pb)
}

func (c *RemoteQuery) UnmarshalMstInfos(pb *proto2.RemoteQuery) error {
	if len(pb.GetMultiMstInfos()) == 0 {
		return nil
	}
	mstInfos := pb.GetMultiMstInfos()
	c.MstInfos = make([]*MultiMstInfo, len(mstInfos))
	for i := range c.MstInfos {
		c.MstInfos[i] = &MultiMstInfo{}
		c.MstInfos[i].ShardIds = mstInfos[i].GetShardIDs()
		if err := c.MstInfos[i].Opt.UnmarshalBinary(mstInfos[i].GetOpt()); err != nil {
			return err
		}
	}
	return nil
}

func (c *RemoteQuery) Size() int {
	return 0
}

func (c *RemoteQuery) Instance() transport.Codec {
	return &RemoteQuery{}
}

func (c *RemoteQuery) Empty() bool {
	return len(c.ShardIDs) == 0 && len(c.PtQuerys) == 0
}

func (c *RemoteQuery) HaveLocalMst() bool {
	return c.Opt.HaveLocalMst()
}

func (c *RemoteQuery) Len() int {
	if len(c.ShardIDs) != 0 {
		return len(c.ShardIDs)
	} else {
		return len(c.PtQuerys)
	}
}
