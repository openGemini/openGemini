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

package meta

import (
	"sort"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
)

type StreamInfos []*StreamInfo

type StreamInfo struct {
	Name     string
	ID       uint64
	SrcMst   *StreamMeasurementInfo
	DesMst   *StreamMeasurementInfo
	Interval time.Duration
	Dims     []string
	Calls    []*StreamCall
	Delay    time.Duration
}

type StreamCall struct {
	Call  string
	Field string
	Alias string
}

type StreamMeasurementInfo struct {
	Name            string
	Database        string
	RetentionPolicy string
}

// func NewStreamInfo(stmt *influxql.CreateStreamStatement, selectStmt *influxql.SelectStatement) *StreamInfo {
func NewStreamInfo(name string, delay time.Duration, srcMstInfo *influxql.Measurement, desMstInfo *StreamMeasurementInfo, selectStmt *influxql.SelectStatement) *StreamInfo {
	info := &StreamInfo{
		Name:  name,
		Delay: delay,
	}
	info.SrcMst = &StreamMeasurementInfo{
		Name:            srcMstInfo.Name,
		Database:        srcMstInfo.Database,
		RetentionPolicy: srcMstInfo.RetentionPolicy,
	}
	info.DesMst = desMstInfo
	info.Calls = make([]*StreamCall, 0, len(selectStmt.Fields))
	for i := range selectStmt.Fields {
		f, ok := selectStmt.Fields[i].Expr.(*influxql.Call)
		if !ok {
			panic("should be call")
		}
		call := &StreamCall{
			Call:  f.Name,
			Alias: selectStmt.Fields[i].Alias,
			Field: f.Args[0].(*influxql.VarRef).Val,
		}
		if call.Alias == "" {
			call.Alias = f.Name + "_" + call.Field
		}
		info.Calls = append(info.Calls, call)
	}
	info.Dims = make([]string, 0, len(selectStmt.Dimensions))
	for i := range selectStmt.Dimensions {
		d, ok := selectStmt.Dimensions[i].Expr.(*influxql.VarRef)
		if !ok {
			continue
		}
		info.Dims = append(info.Dims, d.Val)
	}
	info.Interval, _ = selectStmt.GroupByInterval()
	sort.Strings(info.Dims)
	sort.Slice(info.Calls, func(i, j int) bool { return info.Calls[i].Field < info.Calls[j].Field })
	return info
}

func (s *StreamInfo) Marshal() *proto2.StreamInfo {
	pb := &proto2.StreamInfo{
		Name:     proto.String(s.Name),
		ID:       proto.Uint64(s.ID),
		Interval: proto.Int64(int64(s.Interval)),
		Delay:    proto.Int64(int64(s.Delay)),
		SrcMst:   s.SrcMst.marshal(),
		DesMst:   s.DesMst.marshal(),
	}
	if len(s.Dims) > 0 {
		pb.Dims = make([]string, 0, len(s.Dims))
		for i := range s.Dims {
			pb.Dims = append(pb.Dims, *proto.String(s.Dims[i]))
		}
	}
	if len(s.Calls) > 0 {
		pb.Calls = make([]*proto2.StreamCall, 0, len(s.Calls))
		for i := range s.Calls {
			pb.Calls = append(pb.Calls, s.Calls[i].marshal())
		}
	}
	return pb
}

func (s *StreamInfo) Unmarshal(pb *proto2.StreamInfo) {
	s.Name = pb.GetName()
	s.ID = pb.GetID()
	s.Interval = time.Duration(pb.GetInterval())
	s.Delay = time.Duration(pb.GetDelay())
	s.SrcMst = &StreamMeasurementInfo{}
	s.SrcMst.unmarshal(pb.SrcMst)
	s.DesMst = &StreamMeasurementInfo{}
	s.DesMst.unmarshal(pb.DesMst)
	s.Dims = pb.GetDims()
	if len(pb.Calls) > 0 {
		s.Calls = make([]*StreamCall, len(pb.Calls))
		for i := range s.Calls {
			s.Calls[i] = &StreamCall{}
			s.Calls[i].unmarshal(pb.Calls[i])
		}
	}
}

func (s StreamInfo) clone() *StreamInfo {
	other := &StreamInfo{
		Name:     s.Name,
		ID:       s.ID,
		Interval: s.Interval,
		Delay:    s.Delay,
	}
	other.SrcMst = s.SrcMst.Clone()
	other.DesMst = s.DesMst.Clone()
	other.Calls = make([]*StreamCall, len(s.Calls))
	for i := range other.Calls {
		other.Calls[i] = s.Calls[i].Clone()
	}
	other.Dims = make([]string, len(s.Dims))
	for i := range other.Dims {
		other.Dims[i] = s.Dims[i]
	}
	return other
}

func (s *StreamInfo) Dimensions() string {
	return strings.Join(s.Dims, ",")
}

func (s *StreamInfo) CallsName() string {
	var c []string
	for i := range s.Calls {
		c = append(c, s.Calls[i].String())
	}
	return strings.Join(c, ",")
}

func (s *StreamInfo) Equal(d *StreamInfo) bool {
	if s.Name != d.Name {
		return false
	}
	if s.Interval != d.Interval {
		return false
	}
	if s.Delay != d.Delay {
		return false
	}
	if len(s.Calls) != len(d.Calls) {
		return false
	}
	if len(s.Dims) != len(d.Dims) {
		return false
	}
	if !s.SrcMst.Equal(d.SrcMst) {
		return false
	}
	if !s.DesMst.Equal(d.DesMst) {
		return false
	}
	for i := range s.Dims {
		if s.Dims[i] != d.Dims[i] {
			return false
		}
	}
	return true
}

func (m *StreamMeasurementInfo) marshal() *proto2.StreamMeasurementInfo {
	pb := &proto2.StreamMeasurementInfo{
		Name:            proto.String(m.Name),
		Database:        proto.String(m.Database),
		RetentionPolicy: proto.String(m.RetentionPolicy),
	}
	return pb
}

func (m *StreamMeasurementInfo) unmarshal(pb *proto2.StreamMeasurementInfo) {
	m.Name = pb.GetName()
	m.Database = pb.GetDatabase()
	m.RetentionPolicy = pb.GetRetentionPolicy()
}

func (m StreamMeasurementInfo) Clone() *StreamMeasurementInfo {
	other := m
	return &other
}

func (m *StreamMeasurementInfo) Equal(s *StreamMeasurementInfo) bool {
	if m.Database != s.Database || m.RetentionPolicy != s.RetentionPolicy || m.Name != s.Name {
		return false
	}
	return true
}

func (c *StreamCall) marshal() *proto2.StreamCall {
	pb := &proto2.StreamCall{
		Call:  proto.String(c.Call),
		Alias: proto.String(c.Alias),
		Field: proto.String(c.Field),
	}
	return pb
}

func (c *StreamCall) unmarshal(pb *proto2.StreamCall) {
	c.Call = pb.GetCall()
	c.Alias = pb.GetAlias()
	c.Field = pb.GetField()
}

func (c *StreamCall) String() string {
	return c.Call + "_" + c.Field + "AS" + c.Alias
}

func (c StreamCall) Clone() *StreamCall {
	other := c
	return &other
}
