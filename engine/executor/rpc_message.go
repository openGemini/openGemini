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

package executor

import (
	"fmt"
	"reflect"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
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
}

func NewFinishMessage() *rpc.Message {
	return rpc.NewMessage(FinishMessage, &Finish{})
}

func (e *Finish) Marshal(buf []byte) ([]byte, error) {
	return buf, nil
}

func (e *Finish) Unmarshal(_ []byte) error {
	return nil
}

func (e *Finish) Size() int {
	return 0
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
	ClientID uint64
	QueryID  uint64
}

func NewAbort(queryID uint64, clientID uint64) *Abort {
	return &Abort{ClientID: clientID, QueryID: queryID}
}

func (e *Abort) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendUint64(buf, e.ClientID)
	buf = codec.AppendUint64(buf, e.QueryID)
	return buf, nil
}

func (e *Abort) Unmarshal(buf []byte) error {
	dec := codec.NewBinaryDecoder(buf)
	e.ClientID = dec.Uint64()
	e.QueryID = dec.Uint64()
	return nil
}

func (e *Abort) Size() int {
	return codec.SizeOfUint64() * 2
}

func (e *Abort) Instance() transport.Codec {
	return &Abort{}
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
}

func (c *RemoteQuery) Marshal(buf []byte) ([]byte, error) {
	opt, err := c.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}

	msg, err := proto.Marshal(&proto2.RemoteQuery{
		Database:  c.Database,
		PtID:      c.PtID,
		ShardIDs:  c.ShardIDs,
		NodeID:    c.NodeID,
		Opt:       opt,
		Analyze:   c.Analyze,
		QueryNode: c.Node,
		PtQuerys:  MarshalPtQuerys(c.PtQuerys),
	})

	ret := make([]byte, len(buf)+len(msg))
	copy(ret, buf)
	copy(ret[len(buf):], msg)
	return ret, err
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
	c.NodeID = pb.GetNodeID()
	c.Node = pb.QueryNode
	c.PtQuerys = UnmarshalPtQuerys(pb.GetPtQuerys())
	if err := c.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
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

func NewInvalidTypeError(exp string, data interface{}) error {
	return errno.NewError(errno.InvalidDataType, exp, reflect.TypeOf(data).String())
}
