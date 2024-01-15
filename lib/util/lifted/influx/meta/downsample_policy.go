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

package meta

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	libStrings "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
)

var DownSampleSupportAgg = map[string]bool{"first": true, "last": true, "min": true, "max": true, "sum": true, "count": true, "mean": true}

type StoreDownSamplePolicy struct {
	Alive   bool
	Info    *DownSamplePolicyInfo
	Schemas [][]hybridqp.Catalog
}

type ShardDownSamplePolicyInfo struct {
	DbName                string
	RpName                string
	ShardId               uint64
	PtId                  uint32
	TaskID                uint64
	DownSamplePolicyLevel int
	Ident                 *ShardIdentifier
}

type DownSamplePolicyInfo struct {
	TaskID             uint64
	Calls              []*DownSampleOperators
	DownSamplePolicies []*DownSamplePolicy
	Duration           time.Duration
}

func (d *DownSamplePolicyInfo) GetCalls() map[int64][]string {
	calls := make(map[int64][]string)
	for _, item := range d.Calls {
		calls[item.DataType] = append(calls[item.DataType], item.RewriteOp()...)
	}

	for i := range calls {
		calls[i] = libStrings.UnionSlice(calls[i])
	}
	return calls
}

type DownSamplePolicyInfoWithDbRp struct {
	Info   *DownSamplePolicyInfo
	DbName string
	RpName string
}

type DownSamplePoliciesInfoWithDbRp struct {
	Infos []*DownSamplePolicyInfoWithDbRp
}

type RpMeasurementsFieldsInfo struct {
	MeasurementInfos []*MeasurementFieldsInfo
}

type MeasurementFieldsInfo struct {
	MstName    string
	TypeFields []*MeasurementTypeFields
}

type MeasurementTypeFields struct {
	Type   int64
	Fields []string
}

func (r *RpMeasurementsFieldsInfo) Marshal() *proto2.RpMeasurementsFieldsInfo {
	pb := &proto2.RpMeasurementsFieldsInfo{
		MeasurementInfos: make([]*proto2.MeasurementFieldsInfo, len(r.MeasurementInfos)),
	}
	for i := range r.MeasurementInfos {
		pb.MeasurementInfos[i] = r.MeasurementInfos[i].Marshal()
	}
	return pb
}

func (r *RpMeasurementsFieldsInfo) Unmarshal(pb *proto2.RpMeasurementsFieldsInfo) {
	infos := pb.GetMeasurementInfos()
	r.MeasurementInfos = make([]*MeasurementFieldsInfo, len(infos))
	for i := range infos {
		info := &MeasurementFieldsInfo{}
		info.Unmarshal(pb.MeasurementInfos[i])
		r.MeasurementInfos[i] = info
	}
}

func (r *RpMeasurementsFieldsInfo) MarshalBinary() ([]byte, error) {
	pb := r.Marshal()
	return proto.Marshal(pb)
}

func (r *RpMeasurementsFieldsInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.RpMeasurementsFieldsInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	r.Unmarshal(pb)
	return nil
}

func (m *MeasurementFieldsInfo) Marshal() *proto2.MeasurementFieldsInfo {
	pb := &proto2.MeasurementFieldsInfo{
		MstName:    proto.String(m.MstName),
		TypeFields: make([]*proto2.MeasurementTypeFields, len(m.TypeFields)),
	}
	for i := range m.TypeFields {
		pb.TypeFields[i] = m.TypeFields[i].marshal()
	}
	return pb
}

func (m *MeasurementFieldsInfo) Unmarshal(pb *proto2.MeasurementFieldsInfo) {
	m.MstName = pb.GetMstName()
	f := pb.GetTypeFields()
	m.TypeFields = make([]*MeasurementTypeFields, len(f))
	for i := range f {
		info := &MeasurementTypeFields{}
		info.unmarshal(f[i])
		m.TypeFields[i] = info
	}
}

func (m *MeasurementTypeFields) marshal() *proto2.MeasurementTypeFields {
	pb := &proto2.MeasurementTypeFields{
		Type:   proto.Int64(m.Type),
		Fields: make([]string, len(m.Fields)),
	}
	for i := range m.Fields {
		pb.Fields[i] = *proto.String(m.Fields[i])
	}
	return pb
}

func (m *MeasurementTypeFields) unmarshal(pb *proto2.MeasurementTypeFields) {
	m.Type = pb.GetType()
	m.Fields = pb.GetFields()
}

func (d *DownSamplePoliciesInfoWithDbRp) Marshal() *proto2.DownSamplePoliciesInfoWithDbRp {
	pb := &proto2.DownSamplePoliciesInfoWithDbRp{
		Infos: make([]*proto2.DownSamplePolicyInfoWithDbRp, len(d.Infos)),
	}
	for i, v := range d.Infos {
		pb.Infos[i] = v.Marshal()
	}
	return pb
}

func (d *DownSamplePoliciesInfoWithDbRp) Unmarshal(pb *proto2.DownSamplePoliciesInfoWithDbRp) {
	d.Infos = make([]*DownSamplePolicyInfoWithDbRp, len(pb.Infos))
	for i, v := range pb.Infos {
		d.Infos[i] = &DownSamplePolicyInfoWithDbRp{}
		d.Infos[i].Unmarshal(v)
	}
}

func (d *DownSamplePoliciesInfoWithDbRp) MarshalBinary() ([]byte, error) {
	pb := d.Marshal()
	return proto.Marshal(pb)
}

func (d *DownSamplePoliciesInfoWithDbRp) UnmarshalBinary(buf []byte) error {
	pb := &proto2.DownSamplePoliciesInfoWithDbRp{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	d.Unmarshal(pb)
	return nil
}

func (d *DownSamplePolicyInfoWithDbRp) Marshal() *proto2.DownSamplePolicyInfoWithDbRp {
	pb := &proto2.DownSamplePolicyInfoWithDbRp{
		DbName: proto.String(d.DbName),
		RpName: proto.String(d.RpName),
		Info:   d.Info.Marshal(),
	}
	return pb
}

func (d *DownSamplePolicyInfoWithDbRp) Unmarshal(pb *proto2.DownSamplePolicyInfoWithDbRp) {
	d.Info = &DownSamplePolicyInfo{}
	d.Info.Unmarshal(pb.GetInfo())
	d.DbName = pb.GetDbName()
	d.RpName = pb.GetRpName()
}

func (d *DownSamplePolicyInfo) Marshal() *proto2.DownSamplePolicyInfo {
	pb := &proto2.DownSamplePolicyInfo{
		Duration: GetInt64Duration(&d.Duration),
		TaskID:   proto.Uint64(d.TaskID),
	}

	if len(d.Calls) > 0 {
		pb.Calls = make([]*proto2.DownSampleOperators, len(d.Calls))
		for i, c := range d.Calls {
			pb.Calls[i] = c.marshal()
		}
	}

	if len(d.DownSamplePolicies) > 0 {
		pb.DownSamplePolicies = make([]*proto2.DownSamplePolicy, len(d.DownSamplePolicies))
		for i, d := range d.DownSamplePolicies {
			pb.DownSamplePolicies[i] = d.marshal()
		}
	}

	return pb
}

func (d *DownSamplePolicyInfo) IsNil() bool {
	return d.Calls == nil || d.DownSamplePolicies == nil
}

func (d *DownSamplePolicyInfo) Check(rpi *RetentionPolicyInfo) error {
	var err = errno.NewError(errno.DownSampleIntervalCheck)
	for i := 1; i < len(d.DownSamplePolicies); i++ {
		if d.DownSamplePolicies[i-1].SampleInterval >= d.DownSamplePolicies[i].SampleInterval {
			return err
		}
		if d.DownSamplePolicies[i-1].TimeInterval >= d.DownSamplePolicies[i].TimeInterval ||
			(d.DownSamplePolicies[i].TimeInterval)%(d.DownSamplePolicies[i-1].TimeInterval) != 0 {
			return err
		}
	}
	if d.Duration < time.Hour {
		d.Duration = time.Hour
	}
	if d.DownSamplePolicies[0].SampleInterval < rpi.ShardGroupDuration {
		return errors.New("sample interval must be greater than shard duration")
	}
	if d.DownSamplePolicies[len(d.DownSamplePolicies)-1].SampleInterval >= d.Duration {
		return errors.New("max sample interval time must be smaller than retention policy duration")
	}
	return nil
}

func (d *DownSamplePolicyInfo) GetTypes() []int64 {
	types := make([]int64, 0, 2)

	calls := d.Calls

	sort.Slice(calls, func(i, j int) bool {
		return calls[i].DataType < calls[j].DataType
	})
	for i, c := range calls {
		if i > 0 && calls[i].DataType == calls[i-1].DataType {
			continue
		}

		types = append(types, c.DataType)
	}
	return types
}

func (d *DownSamplePolicyInfo) Unmarshal(pb *proto2.DownSamplePolicyInfo) {
	for _, c := range pb.GetCalls() {
		call := &DownSampleOperators{}
		call.unmarshal(c)
		d.Calls = append(d.Calls, call)
	}
	for _, p := range pb.GetDownSamplePolicies() {
		policy := &DownSamplePolicy{}
		policy.unmarshal(p)
		d.DownSamplePolicies = append(d.DownSamplePolicies, policy)
	}
	d.Duration = time.Duration(pb.GetDuration())
	d.TaskID = pb.GetTaskID()
}

func (d *DownSamplePolicyInfo) MarshalBinary() ([]byte, error) {
	pb := d.Marshal()
	return proto.Marshal(pb)
}

func (d *DownSamplePolicyInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.DownSamplePolicyInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	d.Unmarshal(pb)
	return nil
}

func (d *DownSamplePolicyInfo) Equal(info *DownSamplePolicyInfo, checkID bool) bool {
	if d.TaskID != info.TaskID && checkID {
		return false
	}
	if len(d.Calls) != len(info.Calls) {
		return false
	}
	for i := range d.Calls {
		if !d.Calls[i].Equal(info.Calls[i]) {
			return false
		}
	}
	if len(d.DownSamplePolicies) != len(info.DownSamplePolicies) {
		return false
	}
	for i := range d.DownSamplePolicies {
		if !d.DownSamplePolicies[i].Equal(info.DownSamplePolicies[i]) {
			return false
		}
	}
	return d.Duration == info.Duration
}

func NewDownSamplePolicyInfo(Ops influxql.Fields, duration time.Duration, sampleIntervals []time.Duration, timeIntervals []time.Duration,
	waterMarks []time.Duration, rpi *RetentionPolicyInfo) (*DownSamplePolicyInfo, error) {
	d := &DownSamplePolicyInfo{
		Calls:    make([]*DownSampleOperators, len(Ops)),
		Duration: duration,
	}
	if len(sampleIntervals) != len(timeIntervals) {
		return nil, errno.NewError(errno.DownSampleIntervalLenCheck, "sampleIntervals", "timeIntervals")
	}
	d.DownSamplePolicies = make([]*DownSamplePolicy, len(sampleIntervals))
	for i := range sampleIntervals {
		d.DownSamplePolicies[i] = NewDownSamplePolicy(sampleIntervals[i], timeIntervals[i])
	}
	for i, ope := range Ops {
		c, ok := ope.Expr.(*influxql.Call)
		if !ok {
			return nil, errno.NewError(errno.DownSampleParaError, ope.String())
		}
		op, err := NewDownSampleOperators(c)
		if err != nil {
			return nil, err
		}
		d.Calls[i] = op
	}
	return d, d.Check(rpi)
}

func (d *DownSamplePolicyInfo) Calls2String() string {
	var s []string
	for _, c := range d.Calls {
		s = append(s, c.String())
	}
	return strings.Join(s, ",")
}

func (d *DownSamplePolicyInfo) TimeInterval2String() string {
	var s []string
	for _, t := range d.DownSamplePolicies {
		s = append(s, t.TimeInterval.String())
	}
	return strings.Join(s, ",")
}

func (d *DownSamplePolicyInfo) SampleInterval2String() string {
	var s []string
	for _, t := range d.DownSamplePolicies {
		s = append(s, t.SampleInterval.String())
	}
	return strings.Join(s, ",")
}

func (d *DownSamplePolicyInfo) WaterMark2String() string {
	var s []string
	for _, t := range d.DownSamplePolicies {
		s = append(s, t.WaterMark.String())
	}
	return strings.Join(s, ",")
}

type DownSamplePolicy struct {
	SampleInterval time.Duration
	TimeInterval   time.Duration
	WaterMark      time.Duration
}

func (d *DownSamplePolicy) Equal(dp *DownSamplePolicy) bool {
	if d.SampleInterval != dp.SampleInterval ||
		d.TimeInterval != dp.TimeInterval ||
		d.WaterMark != dp.WaterMark {
		return false
	}
	return true
}

type DownSampleOperators struct {
	AggOps   []string
	DataType int64
}

func (d *DownSampleOperators) String() string {
	s := make([]string, 0, len(d.AggOps))
	for _, op := range d.AggOps {
		s = append(s, op)
	}
	switch influxql.DataType(d.DataType) {
	case influxql.Integer:
		return "integer" + "{" + strings.Join(s, ",") + "}"
	case influxql.Boolean:
		return "boolean" + "{" + strings.Join(s, ",") + "}"
	case influxql.Float:
		return "float" + "{" + strings.Join(s, ",") + "}"
	case influxql.String:
		return "string" + "{" + strings.Join(s, ",") + "}"
	}
	return "unknown operators"
}

func (d *DownSampleOperators) Equal(op *DownSampleOperators) bool {
	if len(d.AggOps) != len(op.AggOps) {
		return false
	}
	for i := range d.AggOps {
		if d.AggOps[i] != op.AggOps[i] {
			return false
		}
	}
	if d.DataType != op.DataType {
		return false
	}
	return true
}

func (d *DownSampleOperators) RewriteOp() []string {
	var hasMean bool
	var hasSum, hasCount bool
	calls := make([]string, 0, len(d.AggOps))
	for i := range d.AggOps {
		if d.AggOps[i] == "mean" {
			hasMean = true
			continue
		}
		if d.AggOps[i] == "sum" {
			hasSum = true
		}
		if d.AggOps[i] == "count" {
			hasCount = true
		}
		calls = append(calls, d.AggOps[i])
	}
	if hasMean {
		if !hasCount {
			calls = append(calls, "count")
		}
		if !hasSum {
			calls = append(calls, "sum")
		}
	}
	return calls
}

func (d *DownSamplePolicy) marshal() *proto2.DownSamplePolicy {
	pb := &proto2.DownSamplePolicy{
		SampleInterval: proto.Int64(int64(d.SampleInterval)),
		TimeInterval:   proto.Int64(int64(d.TimeInterval)),
		WaterMark:      proto.Int64(int64(d.WaterMark)),
	}
	return pb
}

func (d *DownSamplePolicy) unmarshal(pb *proto2.DownSamplePolicy) {
	d.SampleInterval = time.Duration(pb.GetSampleInterval())
	d.TimeInterval = time.Duration(pb.GetTimeInterval())
	d.WaterMark = time.Duration(pb.GetWaterMark())
	return
}

func (d *DownSampleOperators) marshal() *proto2.DownSampleOperators {
	pb := &proto2.DownSampleOperators{
		AggOps:   d.AggOps,
		DataType: proto.Int64(d.DataType),
	}
	return pb
}

func (d *DownSampleOperators) unmarshal(pb *proto2.DownSampleOperators) {
	d.DataType = pb.GetDataType()
	d.AggOps = pb.GetAggOps()
}

func NewDownSamplePolicy(sampleInterval time.Duration, timeInterval time.Duration) *DownSamplePolicy {
	return &DownSamplePolicy{
		SampleInterval: sampleInterval,
		TimeInterval:   timeInterval,
	}
}

func NewDownSampleOperators(c *influxql.Call) (*DownSampleOperators, error) {
	d := &DownSampleOperators{
		AggOps: make([]string, len(c.Args)),
	}
	switch c.Name {
	case "integer":
		d.DataType = int64(influxql.Integer)
	case "float":
		d.DataType = int64(influxql.Float)
	case "boolean":
		d.DataType = int64(influxql.Boolean)
	case "string":
		d.DataType = int64(influxql.String)
	default:
		return nil, errno.NewError(errno.DownSampleUnExpectedDataType, c.Name)
	}
	if len(c.Args) == 0 {
		return nil, errno.NewError(errno.DownSampleAtLeastOneOpsForDataType, c.Name)
	}
	for i, a := range c.Args {
		v, ok := a.(*influxql.VarRef)
		if !ok || !DownSampleSupportAgg[v.Val] {
			return nil, errno.NewError(errno.DownSampleUnsupportedAggOp, a.String())
		}
		d.AggOps[i] = v.Val
	}
	return d, nil
}

type ShardDownSampleUpdateInfo struct {
	Ident         *ShardIdentifier
	DownSampleLvl int
}

type ShardDownSampleUpdateInfos struct {
	Infos []*ShardDownSampleUpdateInfo
}

func NewShardDownSampleUpdateInfo(ident *ShardIdentifier, downSampleLvl int) *ShardDownSampleUpdateInfo {
	return &ShardDownSampleUpdateInfo{
		Ident:         ident,
		DownSampleLvl: downSampleLvl,
	}
}

func (s *ShardDownSampleUpdateInfos) Marshal() *proto2.ShardDownSampleUpdateInfos {
	pb := &proto2.ShardDownSampleUpdateInfos{
		Infos: make([]*proto2.ShardDownSampleUpdateInfo, len(s.Infos)),
	}
	for i := range pb.Infos {
		pb.Infos[i] = s.Infos[i].marshal()
	}
	return pb
}

func (s *ShardDownSampleUpdateInfos) Unmarshal(pb *proto2.ShardDownSampleUpdateInfos) {
	infos := pb.GetInfos()
	s.Infos = make([]*ShardDownSampleUpdateInfo, len(infos))
	for i := range s.Infos {
		s.Infos[i] = &ShardDownSampleUpdateInfo{}
		s.Infos[i].unmarshal(infos[i])
	}
}

func (s *ShardDownSampleUpdateInfos) MarshalBinary() ([]byte, error) {
	pb := s.Marshal()
	return proto.Marshal(pb)
}

func (s *ShardDownSampleUpdateInfos) UnmarshalBinary(buf []byte) error {
	pb := &proto2.ShardDownSampleUpdateInfos{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	s.Unmarshal(pb)
	return nil
}

func (s *ShardDownSampleUpdateInfo) marshal() *proto2.ShardDownSampleUpdateInfo {
	pb := &proto2.ShardDownSampleUpdateInfo{
		Ident:         s.Ident.marshal(),
		DownSampleLvl: proto.Int64(int64(s.DownSampleLvl)),
	}
	return pb
}

func (s *ShardDownSampleUpdateInfo) unmarshal(pb *proto2.ShardDownSampleUpdateInfo) {
	s.DownSampleLvl = int(pb.GetDownSampleLvl())
	s.Ident = &ShardIdentifier{}
	s.Ident.unmarshal(pb.GetIdent())
}
