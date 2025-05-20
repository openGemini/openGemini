package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/data.go

2022.01.23 Change Databases struct from slice to map
Add PtView to represent distribution of data
Add the following method:
WalkDatabases
GetDurationInfos
DurationInfos
NewestShardGroup
Measurement
UpdateSchema
createIndexGroup
CreateMeasurement
AlterShardKey
CheckDataNodeAlive
GetTierOfShardGroup
createIndexGroupIfNeeded
expendDBPtView
DeleteIndexGroup
ValidShardKey
LoadDurationOrDefault
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	// DefaultRetentionPolicyReplicaN is the default value of RetentionPolicyInfo.ReplicaN.
	DefaultRetentionPolicyReplicaN = 1

	// DefaultRetentionPolicyDuration is the default value of RetentionPolicyInfo.Duration.
	DefaultRetentionPolicyDuration = time.Duration(0)

	// DefaultRetentionPolicyWarmDuration is the default value of RetentionPolicyInfo.WarmDuration.
	DefaultRetentionPolicyWarmDuration = time.Duration(0)

	// DefaultRetentionPolicyName is the default name for auto generated retention policies.
	DefaultRetentionPolicyName = "autogen"

	// MinRetentionPolicyDuration represents the minimum duration for a policy.
	MinRetentionPolicyDuration = time.Hour

	// MinRetentionPolicyWarmDuration represents the minimum warm duration for a policy.
	MinRetentionPolicyWarmDuration = time.Hour

	// QueryIDSpan is the default id range span.
	QueryIDSpan = 100000000 // 100 million
)

const (
	HASH          = "hash"
	RANGE         = "range"
	seperatorChar = "$"
	DATANODE      = "data"
	METANODE      = "meta"
	OPMAPLIMITCAP = 30
)

const (
	MarkDelete   int32 = 0 // this type for metricStore
	CancelDelete int32 = 1
)

var dropStreamFirstError = errors.New("stream task exists, drop it first")

type SQLHost string

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

// Metadata snapshot when incremental synchronization is used
type DataOps struct {
	ops           []string     // Snapshot dynamic part: incremental cmds after encoding
	index         uint64       // Snapshot index
	MaxCQChangeID uint64       // Snapshot static part: only MaxCQChangeID
	state         int          // Snapshot Type
	data          *proto2.Data // Full metadata used only for node startup
}

func NewDataOps(ops []string, MaxCQChangeID uint64, state int, index uint64) *DataOps {
	return &DataOps{
		ops:           ops,
		MaxCQChangeID: MaxCQChangeID,
		state:         state,
		index:         index,
	}
}

func NewDataOpsOfAllClear(state int, data *proto2.Data, index uint64) *DataOps {
	return &DataOps{
		state: state,
		data:  data,
		index: index,
	}
}

func (dps *DataOps) Marshal() []byte {
	var index int64 = int64(dps.index)
	var MaxCQChangeID int64 = int64(dps.MaxCQChangeID)
	var state int32 = int32(dps.state)
	pb := proto2.DataOps{Op: dps.ops, NewIndex: &index, MaxCQChangeID: &MaxCQChangeID, GetOpState: &state, Data: dps.data}
	opsBytes, err := proto.Marshal(&pb)
	if err != nil {
		DataLogger.Error("dataOps Marshal err", zap.String("pb string", pb.String()))
		return nil
	}
	return opsBytes
}

func (dps *DataOps) UnmarshalBinary(buf []byte) error {
	var pb proto2.DataOps
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	dps.Unmarshal(&pb)
	return nil
}

func (dps *DataOps) Unmarshal(pb *proto2.DataOps) {
	dps.index = uint64(*pb.NewIndex)
	dps.ops = pb.Op
	dps.state = int(*pb.GetOpState)
	dps.MaxCQChangeID = uint64(*pb.MaxCQChangeID)
	dps.data = pb.Data
}

func (dps *DataOps) Len() int {
	return len(dps.ops)
}

func (dps *DataOps) GetOps() []string {
	return dps.ops
}

func (dps *DataOps) GetData() *proto2.Data {
	return dps.data
}

func (dps *DataOps) GetState() int {
	return dps.state
}

func (dps *DataOps) GetIndex() uint64 {
	return dps.index
}

// Data represents the top level collection of all metadata.
type Data struct {
	Term         uint64 // associated raft term
	Index        uint64 // associated raft index
	ClusterID    uint64
	ClusterPtNum uint32 // default number is the total cpu number of 16 nodes.
	PtNumPerNode uint32
	NumOfShards  int32 // default number of shard for measurement created by `CREATE MEASUREMENT ... SHARDS AUTO`

	MetaNodes     []NodeInfo                // careful: metaNode.gossipAddr is not use.
	DataNodes     []DataNode                // data nodes
	SqlNodes      []DataNode                // sql nodes
	PtView        map[string]DBPtInfos      // PtView's key is dbname, value is PtInfo's slice.
	ReplicaGroups map[string][]ReplicaGroup // key is dbname, value is the replication group of the database

	Databases     map[string]*DatabaseInfo
	Streams       map[string]*StreamInfo
	Users         []UserInfo
	MigrateEvents map[string]*MigrateEventInfo

	// Query ID range segment allocated by all sql nodes
	QueryIDInit map[SQLHost]uint64 // {"127.0.0.1:8086": 0, "127.0.0.2:8086": 10w, "127.0.0.3:8086": 20w}, span is QueryIDSpan

	// adminUserExists provides a constant time mechanism for determining
	// if there is at least one admin GetUser.
	AdminUserExists    bool
	TakeOverEnabled    bool // set by syscontrol command
	BalancerEnabled    bool
	ExpandShardsEnable bool // set by config (not persistence)

	MaxNodeID                      uint64
	MaxShardGroupID                uint64
	MaxShardID                     uint64
	MaxMstID                       uint64
	MaxIndexGroupID                uint64
	MaxIndexID                     uint64
	MaxEventOpId                   uint64
	MaxDownSampleID                uint64
	MaxStreamID                    uint64
	MaxConnID                      uint64
	MaxSubscriptionID              uint64 // +1 for any changes to subscriptions
	MaxCQChangeID                  uint64 // +1 for any changes to continuous queries
	opsMapMu                       sync.RWMutex
	OpsMap                         map[uint64]*Op
	OpsMapMinIndex                 uint64
	OpsMapMaxIndex                 uint64
	OpsToMarshalIndex              uint64
	UpdateNodeTmpIndexCommandStart uint64 // start of all UpdateNodeTmpIndexCommand

	SQLite *SQLiteWrapper
}

var DataLogger *zap.Logger

type ReShardingInfo struct {
	Database     string
	Rp           string
	ShardGroupID uint64
	SplitTime    int64
	Bounds       []string
}

type Op struct {
	com         *proto2.Command
	nextOpIndex uint64
	cacheBytes  []byte
}

func NewOp(com *proto2.Command, nextOpIndex uint64, cacheBytes []byte) *Op {
	return &Op{
		com:         com,
		nextOpIndex: nextOpIndex,
		cacheBytes:  cacheBytes,
	}
}

func (data *Data) SetOps(o *Data) {
	if o != nil {
		data.OpsMap = o.OpsMap
		data.OpsMapMinIndex = o.OpsMapMinIndex
		data.OpsMapMaxIndex = o.OpsMapMaxIndex
		data.OpsToMarshalIndex = o.OpsToMarshalIndex
	}
}

func (data *Data) GetMinAliveNodeTmpIndex() uint64 {
	var ret uint64 = math.MaxUint64
	for _, sqlNode := range data.SqlNodes {
		if sqlNode.Index < ret && sqlNode.Status == serf.StatusAlive {
			ret = sqlNode.Index
		}
	}
	for _, dataNode := range data.DataNodes {
		if dataNode.Index < ret && dataNode.Status == serf.StatusAlive {
			ret = dataNode.Index
		}
	}
	return ret
}

func (data *Data) ClearOpsMapV2(minAliveNodeTmpIndex uint64) {
	data.opsMapMu.Lock()
	defer data.opsMapMu.Unlock()
	if len(data.OpsMap) <= OPMAPLIMITCAP {
		return
	}
	// keep [minAliveNodeTmpIndex:], remove [:minAliveNodeTmpIndex)
	if minAliveNodeTmpIndex == math.MaxUint64 || data.OpsMapMinIndex >= minAliveNodeTmpIndex {
		return
	}
	var tmpOp *Op
	var next uint64
	var start uint64
	for start = data.OpsMapMinIndex; start < minAliveNodeTmpIndex && start != 0; {
		tmpOp, _ = data.OpsMap[start]
		next = tmpOp.nextOpIndex
		delete(data.OpsMap, start)
		start = next
	}
	if next == 0 {
		data.OpsMapMinIndex = math.MaxUint64
		data.OpsMapMaxIndex = 0
		data.OpsToMarshalIndex = 0
	} else {
		data.OpsMapMinIndex = next
		if data.OpsToMarshalIndex < data.OpsMapMinIndex {
			data.OpsToMarshalIndex = data.OpsMapMinIndex
		}
	}
}

func (data *Data) UpdateOpsMapCacheBytes() error {
	data.opsMapMu.Lock()
	defer data.opsMapMu.Unlock()
	if len(data.OpsMap) == 0 || data.OpsToMarshalIndex == 0 {
		return nil
	}
	var err error
	for i := data.OpsToMarshalIndex; i <= data.OpsMapMaxIndex; {
		tmpOp, ok := data.OpsMap[i]
		if !ok {
			return errno.NewError(errno.OpsMapInValid, i)
		}
		tmpOp.cacheBytes, err = proto.Marshal(tmpOp.com)
		if err != nil {
			return errno.NewError(errno.OpMarshalErr, tmpOp.com.String())
		}
		i = data.OpsMap[i].nextOpIndex
		data.OpsToMarshalIndex = i
		if i == 0 {
			break
		}
	}
	return nil
}

var commandUseForClient map[proto2.Command_Type]struct{}

func init() {
	commandUseForClient = map[proto2.Command_Type]struct{}{
		proto2.Command_CreateDatabaseCommand:            {},
		proto2.Command_DropDatabaseCommand:              {},
		proto2.Command_CreateRetentionPolicyCommand:     {},
		proto2.Command_DropRetentionPolicyCommand:       {},
		proto2.Command_SetDefaultRetentionPolicyCommand: {},
		proto2.Command_UpdateRetentionPolicyCommand:     {},
		proto2.Command_CreateShardGroupCommand:          {},
		proto2.Command_DeleteShardGroupCommand:          {},
		proto2.Command_CreateSubscriptionCommand:        {},
		proto2.Command_DropSubscriptionCommand:          {},
		proto2.Command_CreateUserCommand:                {},
		proto2.Command_DropUserCommand:                  {},
		proto2.Command_UpdateUserCommand:                {},
		proto2.Command_SetPrivilegeCommand:              {},
		proto2.Command_SetAdminPrivilegeCommand:         {},
		proto2.Command_SetDataCommand:                   {},
		proto2.Command_CreateMetaNodeCommand:            {},
		proto2.Command_DeleteMetaNodeCommand:            {},
		proto2.Command_SetMetaNodeCommand:               {},
		proto2.Command_CreateDataNodeCommand:            {},
		proto2.Command_CreateSqlNodeCommand:             {},
		proto2.Command_DeleteDataNodeCommand:            {},
		proto2.Command_MarkDatabaseDeleteCommand:        {},
		proto2.Command_MarkRetentionPolicyDeleteCommand: {},
		proto2.Command_CreateMeasurementCommand:         {},
		proto2.Command_ReShardingCommand:                {},
		proto2.Command_UpdateSchemaCommand:              {},
		proto2.Command_AlterShardKeyCmd:                 {},
		proto2.Command_PruneGroupsCommand:               {},
		proto2.Command_MarkMeasurementDeleteCommand:     {},
		proto2.Command_DropMeasurementCommand:           {},
		proto2.Command_DeleteIndexGroupCommand:          {},
		proto2.Command_UpdateShardInfoTierCommand:       {},
		proto2.Command_UpdateNodeStatusCommand:          {},
		proto2.Command_UpdateSqlNodeStatusCommand:       {},
		proto2.Command_UpdatePtInfoCommand:              {},
		proto2.Command_CreateDownSamplePolicyCommand:    {},
		proto2.Command_DropDownSamplePolicyCommand:      {},
		proto2.Command_CreateDbPtViewCommand:            {},
		proto2.Command_UpdateShardDownSampleInfoCommand: {},
		proto2.Command_CreateStreamCommand:              {},
		proto2.Command_DropStreamCommand:                {},
		proto2.Command_ExpandGroupsCommand:              {},
		proto2.Command_UpdatePtVersionCommand:           {},
		proto2.Command_RegisterQueryIDOffsetCommand:     {},
		proto2.Command_CreateContinuousQueryCommand:     {},
		proto2.Command_ContinuousQueryReportCommand:     {},
		proto2.Command_DropContinuousQueryCommand:       {},
		proto2.Command_SetNodeSegregateStatusCommand:    {},
		proto2.Command_RemoveNodeCommand:                {},
		proto2.Command_UpdateReplicationCommand:         {},
		proto2.Command_UpdateMeasurementCommand:         {},
		proto2.Command_UpdateMetaNodeStatusCommand:      {},
	}
}

type GetOpsState int

const (
	AllClear GetOpsState = iota
	NoClear
)

func (data *Data) GetNextOp(index uint64) ([]byte, uint64, error) {
	if op, ok := data.OpsMap[index]; ok {
		if len(op.cacheBytes) > 0 {
			return op.cacheBytes, op.nextOpIndex, nil
		} else {
			str, err := proto.Marshal(op.com)
			if err != nil {
				DataLogger.Error("GetNextOp op marshal err", zap.Uint64("typ", uint64(*op.com.Type)))
				return nil, 0, errno.NewError(errno.OpMarshalErr)
			}
			return str, op.nextOpIndex, nil
		}
	} else {
		return nil, 0, errno.NewError(errno.OpsMapInValid)
	}
}

func (data *Data) GetOpsFromStartOp(startOpIndex uint64, ops *[]string) ([]string, GetOpsState) {
	for startOpIndex != 0 {
		opStr, next, err := data.GetNextOp(startOpIndex)
		if err != nil {
			DataLogger.Error("GetOpsFromStartOp getnextop err", zap.Uint64("index", startOpIndex))
			return nil, AllClear
		} else {
			*ops = append(*ops, string(opStr))
		}
		startOpIndex = next
	}
	return *ops, NoClear
}

func (data *Data) GetOps(oldIndex uint64) ([]string, GetOpsState) {
	if data.OpsMap == nil {
		return nil, AllClear
	}
	data.opsMapMu.RLock()
	defer data.opsMapMu.RUnlock()
	ops := make([]string, 0)
	// the oldIndex has been cleared
	if oldIndex < data.OpsMapMinIndex && data.OpsMapMinIndex != math.MaxUint64 {
		return ops, AllClear
	}
	// snapshot has no ops
	if oldIndex >= data.OpsMapMaxIndex {
		return ops, NoClear
	}

	var tmpOps *Op
	var ok bool
	if tmpOps, ok = data.OpsMap[oldIndex]; ok {
		return data.GetOpsFromStartOp(tmpOps.nextOpIndex, &ops)
	} else {
		// get start op
		var nextIndex uint64
		for i := oldIndex + 1; i <= data.OpsMapMaxIndex; i++ {
			opStr, next, err := data.GetNextOp(i)
			if err == nil {
				ops = append(ops, string(opStr))
				nextIndex = next
				break
			} else if errno.Equal(err, errno.OpMarshalErr) {
				return nil, AllClear
			} else {
				continue
			}
		}
		// get last ops
		return data.GetOpsFromStartOp(nextIndex, &ops)
	}
}

func (data *Data) AddCmdAsOpToOpMap(op proto2.Command, newIndex uint64) {
	if data.OpsMap == nil {
		return
	}
	if _, ok := commandUseForClient[*op.Type]; !ok {
		return
	}
	data.opsMapMu.Lock()
	defer data.opsMapMu.Unlock()
	if oldOp, ok := data.OpsMap[newIndex]; ok {
		DataLogger.Error("AddCmdAsOpToOpMap newIndex err", zap.String("oldOp.string", oldOp.com.String()), zap.Uint64("newIndex", newIndex))
		return
	}
	if len(data.OpsMap) > 0 {
		preOp := data.OpsMap[data.OpsMapMaxIndex]
		preOp.nextOpIndex = newIndex
	}
	data.OpsMap[newIndex] = &Op{com: &op}
	if newIndex < data.OpsMapMinIndex {
		data.OpsMapMinIndex = newIndex
	}
	if newIndex > data.OpsMapMaxIndex {
		data.OpsMapMaxIndex = newIndex
	}
	if data.OpsToMarshalIndex == 0 {
		data.OpsToMarshalIndex = newIndex
	}
}

func (data *Data) DBReplicaN(db string) int {
	replicaN := data.Databases[db].ReplicaN
	if replicaN == 0 {
		return 1
	}
	return replicaN
}

func (data *Data) GetReplicaN(db string) (int, bool) {
	dbInfo, ok := data.Databases[db]
	if !ok {
		return 0, false
	}
	if dbInfo.ReplicaN == 0 {
		return 1, true
	}
	return dbInfo.ReplicaN, true
}

func (data *Data) GetClusterPtNum() uint32 {
	return data.ClusterPtNum
}

func (data *Data) SetClusterPtNum(ptNum uint32) {
	data.ClusterPtNum = ptNum
}

func (data *Data) WalkDatabases(fn func(db *DatabaseInfo)) {
	for dbName := range data.Databases {
		fn(data.Databases[dbName])
	}
}

func (data *Data) WalkDatabasesOrderly(fn func(db *DatabaseInfo)) {
	dbNames := make([]string, 0, len(data.Databases))
	for dbName := range data.Databases {
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)
	for i := 0; i < len(dbNames); i++ {
		fn(data.Databases[dbNames[i]])
	}
}

func (data *Data) WalkDataNodes(fn func(node *DataNode)) {
	for i := range data.DataNodes {
		fn(&data.DataNodes[i])
	}
}

func (data *Data) WalkMetaNodes(fn func(node *NodeInfo)) {
	for i := range data.MetaNodes {
		fn(&data.MetaNodes[i])
	}
}

func (data *Data) WalkMigrateEvents(fn func(eventId string, info *MigrateEventInfo)) {
	for k, v := range data.MigrateEvents {
		fn(k, v)
	}
}

func (data *Data) IndexDurationInfos(dbPtIds map[string][]uint32) *IndexDurationResponse {
	r := &IndexDurationResponse{DataIndex: data.Index}
	data.WalkDatabases(func(db *DatabaseInfo) {
		if _, ok := dbPtIds[db.Name]; !ok {
			return
		}
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			rp.walkIndexGroups(func(ig *IndexGroupInfo) {
				ig.walkIndexes(func(ii *IndexInfo) {
					for i := range dbPtIds[db.Name] {
						if ii.Owners[0] == dbPtIds[db.Name][i] {
							durationInfo := IndexDurationInfo{}
							durationInfo.Ident = IndexBuilderIdentifier{}
							durationInfo.Ident.IndexID = ii.ID
							durationInfo.Ident.IndexGroupID = ig.ID
							durationInfo.Ident.OwnerDb = db.Name
							durationInfo.Ident.OwnerPt = dbPtIds[db.Name][i]
							durationInfo.Ident.Policy = rp.Name
							durationInfo.Ident.StartTime = ig.StartTime
							durationInfo.Ident.EndTime = ig.EndTime
							durationInfo.DurationInfo = DurationDescriptor{}
							durationInfo.DurationInfo.Duration = rp.Duration
							r.Durations = append(r.Durations, durationInfo)
							return
						}
					}
				})
			})
		})
	})
	return r
}

func (data *Data) DurationInfos(dbPtIds map[string][]uint32) *ShardDurationResponse {
	r := &ShardDurationResponse{DataIndex: data.Index}
	data.WalkDatabases(func(db *DatabaseInfo) {
		if _, ok := dbPtIds[db.Name]; !ok {
			return
		}
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			rp.WalkShardGroups(func(sg *ShardGroupInfo) {
				sg.walkShards(func(sh *ShardInfo) {
					for i := range dbPtIds[db.Name] {
						if sh.Owners[0] == dbPtIds[db.Name][i] {
							durationInfo := ShardDurationInfo{}
							durationInfo.Ident = ShardIdentifier{}
							durationInfo.Ident.ShardID = sh.ID
							durationInfo.Ident.ShardGroupID = sg.ID
							durationInfo.Ident.OwnerDb = db.Name
							durationInfo.Ident.OwnerPt = dbPtIds[db.Name][i]
							durationInfo.Ident.Policy = rp.Name
							durationInfo.Ident.DownSampleLevel = int(sh.DownSampleLevel)
							durationInfo.Ident.DownSampleID = sh.DownSampleID
							durationInfo.Ident.ReadOnly = sh.ReadOnly
							durationInfo.Ident.EngineType = uint32(sg.EngineType)
							durationInfo.Ident.StartTime = sg.StartTime
							durationInfo.Ident.EndTime = sg.EndTime
							durationInfo.DurationInfo = DurationDescriptor{}
							durationInfo.DurationInfo.Duration = rp.Duration
							durationInfo.DurationInfo.Tier = sh.Tier
							durationInfo.DurationInfo.TierDuration = rp.TierDuration(sh.Tier)
							r.Durations = append(r.Durations, durationInfo)
							return
						}
					}
				})
			})
		})
	})
	return r
}

func (data *Data) NewestShardGroup(database, retentionPolicy string) (sg *ShardGroupInfo) {
	rp, err := data.RetentionPolicy(database, retentionPolicy)
	if err != nil {
		return nil
	}

	var msti *MeasurementInfo
	for _, mst := range rp.Measurements {
		msti = mst
		break
	}
	if msti == nil || msti.ShardKeys == nil ||
		msti.ShardKeys[0].Type == HASH {
		return nil
	}

	sgLen := len(rp.ShardGroups)
	if sgLen == 0 {
		return nil
	}

	return &rp.ShardGroups[sgLen-1]
}

func (data *Data) GetMeasurementID(database, retentionPolicy, mst string) (uint64, error) {
	db, err := data.GetDatabase(database)
	if err != nil {
		return 0, err
	}
	rp, err := db.GetRetentionPolicy(retentionPolicy)
	if err != nil {
		return 0, err
	}

	msti := rp.Measurement(mst)
	if msti == nil {
		return 0, ErrMeasurementNotFound
	}

	return msti.ID, nil
}

func (data *Data) Measurement(database, retentionPolicy, mst string) (*MeasurementInfo, error) {
	db, err := data.GetDatabase(database)
	if err != nil {
		return nil, err
	}
	rp, err := db.GetRetentionPolicy(retentionPolicy)
	if err != nil {
		return nil, err
	}

	msti := rp.Measurement(mst)
	if msti == nil {
		return nil, ErrMeasurementNotFound
	}

	if msti.MarkDeleted {
		return nil, ErrMeasurementNotFound
	}
	if msti.ObsOptions == nil && db.Options != nil {
		msti.ObsOptions = db.Options
	}

	return msti, nil
}

func (data *Data) Measurements(database, retentionPolicy string) (*MeasurementsInfo, error) {
	db, err := data.GetDatabase(database)
	if err != nil {
		return nil, err
	}
	rp, err := db.GetRetentionPolicy(retentionPolicy)
	if err != nil {
		return nil, err
	}

	m := &MeasurementsInfo{}
	mstsSlice := make([]*MeasurementInfo, 0, len(rp.Measurements))
	rp.EachMeasurements(func(mst *MeasurementInfo) {
		if mst.ObsOptions == nil && db.Options != nil {
			mst.ObsOptions = db.Options
		}
		mstsSlice = append(mstsSlice, mst)
	})

	if len(mstsSlice) == 0 {
		return nil, ErrMeasurementsNotFound
	}

	m.MstsInfo = mstsSlice
	return m, nil
}

func (data *Data) UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
	msti, err := data.Measurement(database, retentionPolicy, mst)
	if err != nil {
		return err
	}
	msti.SchemaLock.Lock()
	defer msti.SchemaLock.Unlock()
	if msti.Schema == nil {
		newSchema := NewCleanSchema(0)
		msti.Schema = &newSchema
	}
	if SchemaCleanEn {
		cleanSchema := msti.Schema
		for i := range fieldToCreate {
			existVal, ok := (*cleanSchema)[fieldToCreate[i].GetFieldName()]
			if !ok {
				(*cleanSchema)[fieldToCreate[i].GetFieldName()] = SchemaVal{Typ: int8(fieldToCreate[i].GetFieldType()), EndTime: fieldToCreate[i].GetEndTime()}
				continue
			}
			if int32(existVal.Typ) != fieldToCreate[i].GetFieldType() {
				return ErrFieldTypeConflict
			}
			if existVal.EndTime < fieldToCreate[i].GetEndTime() {
				(*cleanSchema)[fieldToCreate[i].GetFieldName()] = SchemaVal{Typ: int8(fieldToCreate[i].GetFieldType()), EndTime: fieldToCreate[i].GetEndTime()}
			}
		}
	} else {
		normalSchema := msti.Schema
		for i := range fieldToCreate {
			existType, ok := (*normalSchema)[fieldToCreate[i].GetFieldName()]
			if !ok {
				msti.Schema.SetTyp(fieldToCreate[i].GetFieldName(), fieldToCreate[i].GetFieldType())
				continue
			}
			if int32(existType.Typ) != fieldToCreate[i].GetFieldType() {
				return ErrFieldTypeConflict
			}
		}
	}

	return nil
}

func (data *Data) ReSharding(info *ReShardingInfo) error {
	rp, err := data.RetentionPolicy(info.Database, info.Rp)
	if err != nil {
		return err
	}

	length := len(rp.ShardGroups)
	if length == 0 {
		return ErrShardGroupNotFound
	}

	if rp.ShardGroups[length-1].ID != info.ShardGroupID {
		return ErrShardGroupAlreadyReSharding(info.ShardGroupID)
	}

	startTime := time.Unix(0, info.SplitTime+1)
	data.createIndexGroup(info.Database, rp, startTime)
	DataLogger.Info("reSharding info", zap.Time("splitTime", time.Unix(0, info.SplitTime+1)), zap.Any("bounds", info.Bounds))
	err = data.CreateShardGroupWithBounds(info.Database, rp, startTime, info.Bounds, rp.ShardGroups[length-1].EngineType) // shard group id start from 1...
	return err
}

func (data *Data) createIndexGroup(db string, rp *RetentionPolicyInfo, startTime time.Time) {
	length := len(rp.ShardGroups)
	endTime := rp.ShardGroups[length-1].EndTime
	igLen := len(rp.IndexGroups)

	for i := igLen - 1; i >= 0; i-- {
		if rp.IndexGroups[i].Overlaps(startTime, endTime) {
			endTime = rp.IndexGroups[i].EndTime
			break
		}
	}

	igi := IndexGroupInfo{}
	data.MaxIndexGroupID++
	igi.ID = data.MaxIndexGroupID
	igi.StartTime = startTime
	igi.EndTime = endTime
	igi.Indexes = make([]IndexInfo, data.GetClusterPtNum())
	for i := range igi.Indexes {
		data.MaxIndexID++
		igi.Indexes[i] = IndexInfo{ID: data.MaxIndexID, Owners: []uint32{uint32(i)}}
	}
	rp.IndexGroups = append(rp.IndexGroups, igi)
	sort.Sort(IndexGroupInfos(rp.IndexGroups))
}

func (data *Data) CreateShardGroupWithBounds(db string, rp *RetentionPolicyInfo, startTime time.Time, bounds []string, engineType config.EngineType) error {
	// Create the shard group.
	data.MaxShardGroupID++
	sgi := ShardGroupInfo{}
	sgi.ID = data.MaxShardGroupID
	sgi.StartTime = startTime.UTC()
	lastSg := &rp.ShardGroups[len(rp.ShardGroups)-1]
	sgi.EndTime = lastSg.EndTime.UTC()
	sgi.EngineType = engineType

	igi := rp.IndexGroups[len(rp.IndexGroups)-1]
	shardN := len(bounds) + 1
	index := 0
	// Create shards on the group.
	sgi.Shards = make([]ShardInfo, shardN)
	for i := range sgi.Shards {
		data.MaxShardID++
		// FIXME shardTier should put in command
		sgi.Shards[i] = ShardInfo{ID: data.MaxShardID, Tier: lastSg.Shards[0].Tier}
		ptNum := data.GetClusterPtNum()
		for ptId := 0; ptId < int(ptNum); ptId++ {
			if ptId%shardN == i {
				sgi.Shards[i].Owners = append(sgi.Shards[i].Owners, uint32(ptId))
				sgi.Shards[i].IndexID = igi.Indexes[ptId].ID
				break
			}
		}

		if index != shardN-1 {
			sgi.Shards[i].Max = bounds[index]
		}

		if index > 0 {
			sgi.Shards[i].Min = bounds[index-1]
		}
		index++
	}

	rp.ShardGroups = append(rp.ShardGroups, sgi)
	sort.Sort(ShardGroupInfos(rp.ShardGroups))
	return nil
}

// createVersionMeasurement create new measurement
func (data *Data) createVersionMeasurement(db string, rp *RetentionPolicyInfo, shardKey *proto2.ShardKeyInfo, numOfShards int32,
	indexR *proto2.IndexRelation, ski *ShardKeyInfo, mst string, version uint32, engineType config.EngineType,
	colStoreInfo *ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *proto2.Options) error {
	sgLen := len(rp.ShardGroups)
	if sgLen == 0 {
		ski.ShardGroup = data.MaxShardGroupID + 1
	} else {
		ski.ShardGroup = 0
	}
	nameWithVer := influx.GetNameWithVersion(mst, version)

	msti := NewMeasurementInfo(nameWithVer, mst, engineType, data.MaxMstID)
	data.MaxMstID++
	if colStoreInfo != nil {
		msti.ColStoreInfo = colStoreInfo
	}

	if shardKey != nil {
		msti.ShardKeys = []ShardKeyInfo{*ski}
	}

	if numOfShards == -1 {
		numOfShards = data.NumOfShards
	}
	msti.InitNumOfShards = 0 // default
	maxShardNum := int32(data.ClusterPtNum)
	if numOfShards != 0 && ski.Type == HASH && numOfShards < maxShardNum { // only works for hash sharding.
		msti.InitNumOfShards = numOfShards
		msti.ShardIdexes = make(map[uint64][]int)
		shardGroups := rp.ShardGroups

		if len(shardGroups) > 0 {
			for _, shardGroup := range shardGroups {
				shards := shardGroup.Shards
				msti.ShardIdexes[shardGroup.ID] = mapShards(nameWithVer, shards, msti.InitNumOfShards)
			}
		}
	}

	if indexR != nil {
		msti.IndexRelation = *DecodeIndexRelation(indexR)
	}

	if options != nil {
		msti.Options = &Options{}
		msti.Options.Unmarshal(options)
	}

	if rp.Measurements == nil {
		rp.Measurements = make(map[string]*MeasurementInfo)
	}

	rp.MstVersions[mst] = MeasurementVer{nameWithVer, version}
	rp.Measurements[nameWithVer] = msti

	if len(schemaInfo) > 0 {
		return data.UpdateSchema(db, rp.Name, mst, schemaInfo)
	}
	return nil
}

func (data *Data) CreateMeasurement(database string, rpName string, mst string,
	shardKey *proto2.ShardKeyInfo, numOfShards int32, indexR *proto2.IndexRelation, engineType config.EngineType,
	colStoreInfo *proto2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *proto2.Options) error {
	rp, err := data.RetentionPolicy(database, rpName)
	if err != nil {
		return err
	}

	ski := &ShardKeyInfo{}
	if shardKey != nil {
		ski.unmarshal(shardKey)
		if err := rp.validMeasurementShardType(ski.Type, mst); err != nil {
			return err
		}
		if rp.ReplicaN > 1 && ski.Type == RANGE {
			return errno.NewError(errno.ConflictWithRep)
		}
	}

	var hInfo *ColStoreInfo
	if colStoreInfo != nil {
		if rp.ReplicaN > 1 {
			return errno.NewError(errno.ConflictWithRep)
		}
		hInfo = &ColStoreInfo{}
		hInfo.Unmarshal(colStoreInfo)
	}

	msti := rp.Measurement(mst)
	if msti == nil || msti.MarkDeleted {
		var ver uint32
		version, ok := rp.MstVersions[mst]
		if ok {
			ver = (version.Version + 1) & 0xffff
		}
		if len(rp.MstVersions) == 0 {
			rp.MstVersions = make(map[string]MeasurementVer)
		}
		return data.createVersionMeasurement(database, rp, shardKey, numOfShards, indexR, ski, mst, ver, engineType, hInfo, schemaInfo, options)
	}

	n := len(msti.ShardKeys)
	if n > 0 && ski.EqualsToAnother(&msti.ShardKeys[n-1]) {
		return nil
	}
	return ErrMeasurementExists
}

func (data *Data) AlterShardKey(database string, rpName string, mst string, shardKey *proto2.ShardKeyInfo) error {
	rp, err := data.RetentionPolicy(database, rpName)
	if err != nil {
		return err
	}

	msti := rp.Measurement(mst)
	if msti == nil || msti.MarkDeleted {
		return ErrMeasurementNotFound
	}

	shardKeyInfo := &msti.ShardKeys[len(msti.ShardKeys)-1]
	ski := &ShardKeyInfo{}
	if shardKey != nil {
		ski.unmarshal(shardKey)
	}

	if ski.Type != shardKeyInfo.Type {
		return ErrShardingTypeNotEqual(rpName, ski.Type, shardKeyInfo.Type)
	}

	if ski.EqualsToAnother(shardKeyInfo) {
		return nil
	}

	if err := rp.validMeasurementShardType(ski.Type, mst); err != nil {
		return err
	}
	ski.ShardGroup = data.MaxShardGroupID + 1
	// rp max shard group ID is less than last shardKey effective shard group ID means do not create new sg after last shardKey change
	// so that last shardKey is useless just overwrite
	if len(rp.ShardGroups) == 0 || rp.maxShardGroupID() < shardKeyInfo.ShardGroup {
		msti.ShardKeys[len(msti.ShardKeys)-1] = *ski
		return nil
	}

	msti.ShardKeys = append(msti.ShardKeys, *ski)
	return nil
}

func (data *Data) GetNodeIndex(nodeId uint64) (uint64, error) {
	for i, value := range data.DataNodes {
		if value.ID == nodeId {
			return uint64(i), nil
		}
	}

	DataLogger.Info("[Balancer] not found task ", zap.Uint64("nodeid", nodeId))
	return 0, errno.NewError(errno.DataNodeNotFound, nodeId)
}

func (data *Data) GetNodeIndexV2(nodeId uint64) (uint64, error) {
	for _, value := range data.DataNodes {
		if value.ID == nodeId {
			return value.Index, nil
		}
	}

	DataLogger.Error("[GetNodeIndexV2] not found task ", zap.Uint64("nodeid", nodeId))
	return 0, errno.NewError(errno.DataNodeNotFound, nodeId)
}

func (data *Data) GetSqlNodeIndex(nodeId uint64) (uint64, error) {
	for _, value := range data.SqlNodes {
		if value.ID == nodeId {
			return value.Index, nil
		}
	}

	DataLogger.Error("[GetSqlNodeIndex] not found task ", zap.Uint64("nodeid", nodeId))
	return 0, errno.NewError(errno.SqlNodeNotFound, nodeId)
}

func (data *Data) SetSqlNodeIndex(nodeId uint64, newIndex uint64) error {
	for i, value := range data.SqlNodes {
		if value.ID == nodeId {
			if newIndex > data.SqlNodes[i].Index {
				data.SqlNodes[i].Index = newIndex
				return nil
			} else if newIndex < data.SqlNodes[i].Index {
				DataLogger.Error("[SetSqlNodeIndex] newIndex err", zap.Uint64("newIndex", newIndex), zap.Uint64("data.SqlNodes[i].Index", data.SqlNodes[i].Index), zap.Uint64("nodeId", nodeId))
				return fmt.Errorf("SetSqlNodeIndex newIndex err")
			}
		}
	}
	DataLogger.Error("[SetSqlNodeIndex] not found task ", zap.Uint64("nodeid", nodeId))
	return fmt.Errorf("SetSqlNodeIndex node not find err")
}

func (data *Data) SetDataNodeIndex(nodeId uint64, newIndex uint64) error {
	for i, value := range data.DataNodes {
		if value.ID == nodeId {
			if newIndex > data.DataNodes[i].Index {
				data.DataNodes[i].Index = newIndex
				return nil
			} else if newIndex < data.DataNodes[i].Index {
				DataLogger.Error("[SetDataNodeIndex] newIndex err", zap.Uint64("newIndex", newIndex), zap.Uint64("data.DataNodes[i].Index", data.DataNodes[i].Index), zap.Uint64("nodeId", nodeId))
				return fmt.Errorf("SetDataNodeIndex newIndex err")
			}
		}
	}
	DataLogger.Error("[SetDataNodeIndex] not found task ", zap.Uint64("nodeid", nodeId))
	return fmt.Errorf("SetDataNodeIndex node not find err")
}

func (data *Data) CheckDataNodeAlive(nodeId uint64) error {
	nodeIndex, err := data.GetNodeIndex(nodeId)
	if err != nil {
		err = fmt.Errorf("dataNode %d not found", nodeId)
		return err
	}
	if data.DataNodes[nodeIndex].SegregateStatus != Normal {
		return fmt.Errorf("dataNode %d is segregated, segregatedStatus:%d", nodeId, data.DataNodes[nodeIndex].SegregateStatus)
	}
	status := data.DataNodes[nodeIndex].Status
	if status != serf.StatusAlive {
		return errno.NewError(errno.DataNoAlive, nodeId)
	} else {
		return nil
	}
}

func (data *Data) DataNodeAlive(id uint64) bool {
	for i := range data.DataNodes {
		if data.DataNodes[i].ID == id {
			return data.DataNodes[i].Status == serf.StatusAlive
		}
	}
	return false
}

// DataNode returns a node by id.
func (data *Data) DataNode(id uint64) *DataNode {
	for i := range data.DataNodes {
		if data.DataNodes[i].ID == id {
			return &data.DataNodes[i]
		}
	}
	return nil
}

// SqlNode returns a sql node by id.
func (data *Data) SqlNode(id uint64) *DataNode {
	for i := range data.SqlNodes {
		if data.SqlNodes[i].ID == id {
			return &data.SqlNodes[i]
		}
	}
	return nil
}

// MetaNode returns a meta node by id.
func (data *Data) MetaNode(id uint64) *NodeInfo {
	for i := range data.MetaNodes {
		if data.MetaNodes[i].ID == id {
			return &data.MetaNodes[i]
		}
	}
	return nil
}

func (data *Data) DataNodeByHttpHost(httpAddr string) *DataNode {
	for i := range data.DataNodes {
		if data.DataNodes[i].Host == httpAddr {
			return &data.DataNodes[i]
		}
	}
	return nil
}

func (data *Data) SqlNodeByHttpHost(httpAddr string) *DataNode {
	for i := range data.SqlNodes {
		if data.SqlNodes[i].TCPHost == httpAddr {
			return &data.SqlNodes[i]
		}
	}
	return nil
}

// DataNode returns a node by id.
func (data *Data) DataNodeByIp(nodeIp string) *DataNode {
	for i, n := range data.DataNodes {
		ip := strings.Split(n.TCPHost, ":")[0]
		if ip == nodeIp {
			return &data.DataNodes[i]
		}
	}
	return nil
}

func (data *Data) DataNodeIDs() []int {
	ids := make([]int, 0, len(data.DataNodes))
	for i := range data.DataNodes {
		ids = append(ids, int(data.DataNodes[i].ID))
	}
	sort.Ints(ids)
	return ids
}

// Change data node state and Data nodes view version.
func (data *Data) ClusterChangeState(nodeID uint64, newState serf.MemberStatus) bool {
	for i := range data.DataNodes {
		if data.DataNodes[i].ID == nodeID {
			data.DataNodes[i].Status = newState
			return true
		}
	}
	// ToDo: change database state for
	return false
}

// CreateDataNode adds a node to the metadata.
func (data *Data) CreateDataNode(host, tcpHost, role, az string) (uint64, error) {
	data.MaxConnID++
	// Ensure a node with the same host doesn't already exist.
	for i := range data.DataNodes {
		if data.DataNodes[i].TCPHost == tcpHost {
			data.DataNodes[i].ConnID = data.MaxConnID
			return data.DataNodes[i].ID, nil
		}
	}

	// If an existing meta node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.MetaNodes {
		if n.TCPHost == tcpHost {
			existingID = n.ID
			break
		}
	}

	// We didn't find an existing node, so assign it a new node ID
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	dn := DataNode{
		NodeInfo: NodeInfo{
			ID:      existingID,
			Host:    host,
			Role:    role,
			TCPHost: tcpHost},
		Az:     az,
		ConnID: data.MaxConnID}
	// Append new node.
	data.DataNodes = append(data.DataNodes, dn)

	sort.Sort(DataNodeInfos(data.DataNodes))
	data.initDataNodePtView(data.GetClusterPtNum())
	if role == NodeReader {
		return existingID, nil
	}
	for db := range data.PtView {
		data.expandDBPtView(db, data.GetClusterPtNum(), &dn)
	}
	if data.ExpandShardsEnable {
		data.ExpandGroups()
	}
	return existingID, nil
}

func (data *Data) CreateSqlNode(httpHost string, gossipAddr string) (uint64, error) {
	data.MaxConnID++
	// Ensure a node with the same host doesn't already exist.
	for i := range data.SqlNodes {
		if data.SqlNodes[i].TCPHost == httpHost {
			data.SqlNodes[i].ConnID = data.MaxConnID
			return data.SqlNodes[i].ID, nil
		}
	}

	// If an existing meta node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.MetaNodes {
		if n.TCPHost == httpHost {
			existingID = n.ID
			break
		}
	}

	// We didn't find an existing node, so assign it a new node ID
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	dn := DataNode{
		NodeInfo: NodeInfo{
			ID:         existingID,
			Role:       "",
			TCPHost:    httpHost,
			GossipAddr: gossipAddr,
		},
		ConnID: data.MaxConnID}
	// Append new sql node.
	data.SqlNodes = append(data.SqlNodes, dn)
	DataLogger.Info("CreateSqlNode success ", zap.Uint64("nodeid", dn.ID), zap.String("httpHost", httpHost), zap.String("gossipHost", gossipAddr))
	return existingID, nil
}

func (data *Data) updatePtStatus(db string, ptId uint32, nodeId uint64, status PtStatus) {
	ptNum := data.GetClusterPtNum()
	dbPtView, ok := data.PtView[db]
	if !ok {
		dbPtView = make([]PtInfo, ptNum)
	}

	if ptId >= uint32(len(dbPtView)) {
		newPtView := make([]PtInfo, ptNum)
		copy(newPtView, dbPtView)
		dbPtView = newPtView
	}
	pi := &dbPtView[ptId]
	pi.Owner.NodeID = nodeId
	pi.Status = status
	pi.PtId = ptId
	if pi.Ver == 0 {
		pi.Ver = 1
	}
	if status == Online && config.GetHaPolicy() == config.Replication {
		data.nextSubHealth(db, pi.RGID, dbPtView)
	}
	data.PtView[db] = dbPtView
}

func (data *Data) nextSubHealth(db string, rgId uint32, dbPtView DBPtInfos) {
	rg := data.GetRGOfPtFast(rgId, db)
	if rg != nil {
		rg.nextSubHealth(db, dbPtView)
	}
}

func (data *Data) markAllDbPtOffload() {
	for db := range data.PtView {
		for _, ptInfo := range data.PtView[db] {
			data.updatePtStatus(db, ptInfo.PtId, ptInfo.Owner.NodeID, Offline)
		}
	}
}

// setDataNode adds a data node with a pre-specified nodeID.
// this should only be used when the cluster is upgrading from 0.9 to 0.10
func (data *Data) SetDataNode(nodeID uint64, host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for i := range data.DataNodes {
		if data.DataNodes[i].Host == host {
			return ErrNodeExists
		}
	}

	// Append new node.
	data.DataNodes = append(data.DataNodes, DataNode{
		NodeInfo: NodeInfo{
			ID:      nodeID,
			Host:    host,
			TCPHost: tcpHost},
	})

	return nil
}

func (data *Data) DeleteDataNode(id uint64) error {
	return nil
}

// CreateMetaNode will add a new meta node to the metastore
func (data *Data) CreateMetaNode(httpAddr, rpcAddr, tcpAddr string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.MetaNodes {
		if n.Host == httpAddr {
			return nil
		}
	}

	// If an existing data node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpAddr {
			existingID = n.ID
			break
		}
	}

	// We didn't find and existing data node ID, so assign a new ID
	// to this meta node.
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.MetaNodes = append(data.MetaNodes, NodeInfo{
		ID:      existingID,
		Host:    httpAddr,
		RPCAddr: rpcAddr,
		TCPHost: tcpAddr,
	})

	sort.Sort(NodeInfos(data.MetaNodes))
	return nil
}

// SetMetaNode will update the information for the single meta
// node or create a new metanode. If there are more than 1 meta
// nodes already, an error will be returned
func (data *Data) SetMetaNode(httpAddr, rpcAddr, tcpAddr string) error {
	if len(data.MetaNodes) > 1 {
		return fmt.Errorf("can't set meta node when there are more than 1 in the metastore")
	}

	if len(data.MetaNodes) == 0 {
		return data.CreateMetaNode(httpAddr, rpcAddr, tcpAddr)
	}

	data.MetaNodes[0].Host = httpAddr
	data.MetaNodes[0].TCPHost = tcpAddr

	return nil
}

// DeleteMetaNode will remove the meta node from the store
func (data *Data) DeleteMetaNode(id uint64) error {
	// Node has to be larger than 0 to be real
	if id == 0 {
		return ErrNodeIDRequired
	}

	var nodes []NodeInfo
	for _, n := range data.MetaNodes {
		if n.ID == id {
			continue
		}
		nodes = append(nodes, n)
	}

	if len(nodes) == len(data.MetaNodes) {
		return ErrNodeNotFound
	}

	data.MetaNodes = nodes
	return nil
}

// Database returns PtInfo by the database name.
func (data *Data) DBPtView(name string) DBPtInfos {
	return data.PtView[name]
}

func (data *Data) DBRepGroups(name string) []ReplicaGroup {
	return data.ReplicaGroups[name]
}

func (data *Data) GetDatabase(name string) (*DatabaseInfo, error) {
	dbi := data.Database(name)
	if dbi == nil {
		return nil, errno.NewError(errno.DatabaseNotFound, name)
	}
	if dbi.MarkDeleted {
		return nil, errno.NewError(errno.DatabaseIsBeingDelete, name)
	}
	return dbi, nil
}

func (data *Data) Database(name string) *DatabaseInfo {
	return data.Databases[name]
}

// CloneDatabases returns a copy of the DatabaseInfo.
func (data *Data) CloneDatabases() map[string]*DatabaseInfo {
	if data.Databases == nil {
		return nil
	}
	dbs := make(map[string]*DatabaseInfo, len(data.Databases))
	for dbName := range data.Databases {
		dbs[dbName] = data.Databases[dbName].clone()
	}
	return dbs
}

func (data *Data) CloneStreams() map[string]*StreamInfo {
	if data.Streams == nil {
		return nil
	}
	strs := make(map[string]*StreamInfo, len(data.Streams))
	for name := range data.Streams {
		strs[name] = data.Streams[name].clone()
	}
	return strs
}

// CloneDataNodes returns a copy of the NodeInfo.
func (data *Data) CloneDataNodes() []DataNode {
	if data == nil || data.DataNodes == nil {
		return nil
	}
	nis := make([]DataNode, len(data.DataNodes))
	copy(nis, data.DataNodes)
	return nis
}

// CloneSqlNodes returns a copy of the NodeInfo.
func (data *Data) CloneSqlNodes() []DataNode {
	if data == nil || data.SqlNodes == nil {
		return nil
	}
	nis := make([]DataNode, len(data.SqlNodes))
	for i := range data.SqlNodes {
		nis[i] = data.SqlNodes[i]
	}
	return nis
}

// CloneMetaNodes returns a copy of the NodeInfo.
func (data *Data) CloneMetaNodes() []NodeInfo {
	if data.MetaNodes == nil {
		return nil
	}
	mns := make([]NodeInfo, len(data.MetaNodes))
	for i := range data.MetaNodes {
		mns[i] = data.MetaNodes[i].clone()
	}
	return mns
}

func (data *Data) CloneQueryIDInit() map[SQLHost]uint64 {
	if data.QueryIDInit == nil {
		return nil
	}
	cloneIdInit := make(map[SQLHost]uint64, len(data.QueryIDInit))
	for host := range data.QueryIDInit {
		cloneIdInit[host] = data.QueryIDInit[host]
	}
	return cloneIdInit
}

type assignPtFn func(data *Data, name string) error

var dbPtAssigner []assignPtFn

func init() {
	dbPtAssigner = make([]assignPtFn, config.PolicyEnd)
	dbPtAssigner[config.WriteAvailableFirst] = assignPtForWAF
	dbPtAssigner[config.SharedStorage] = assignPtForSS
	dbPtAssigner[config.Replication] = assignPtForWAF
}

func (data *Data) GetWriteNodeNum() uint32 {
	var writeNodeNum uint32
	for i := 0; i < len(data.DataNodes); i++ {
		if IsNodeWriter(data.DataNodes[i].Role) {
			writeNodeNum++
		}
	}
	return writeNodeNum
}

func (data *Data) GetWriteNode() []DataNode {
	var writeNodes []DataNode
	for i := 0; i < len(data.DataNodes); i++ {
		if IsNodeWriter(data.DataNodes[i].Role) {
			writeNodes = append(writeNodes, data.DataNodes[i])
		}
	}
	return writeNodes
}

func (data *Data) GetAliveWriteNode() []DataNode {
	var writeNodes []DataNode
	for i := 0; i < len(data.DataNodes); i++ {
		if IsNodeWriter(data.DataNodes[i].Role) &&
			data.DataNodes[i].Status == serf.StatusAlive &&
			data.DataNodes[i].SegregateStatus == Normal {
			writeNodes = append(writeNodes, data.DataNodes[i])
		}
	}
	return writeNodes
}

// assign pts to all datanodes, because move db pt is not permitted in write-available-first policy and replication policy
func assignPtForWAF(data *Data, name string) error {
	writeNodes := data.GetWriteNode()
	if len(writeNodes) == 0 {
		return errno.NewError(errno.DataNoAlive)
	}

	ptNum := data.GetClusterPtNum()
	for ptId := 0; ptId < int(ptNum); ptId++ {
		pos := ptId % len(writeNodes)
		data.updatePtStatus(name, uint32(ptId), writeNodes[pos].ID, Offline)
	}

	return nil
}

// assign pts to alive datanodes in shared-storage policy due to pts can balance by background balancer
func assignPtForSS(data *Data, name string) error {
	aliveDataNodes := data.GetAliveWriteNode()
	if len(aliveDataNodes) == 0 {
		return errno.NewError(errno.DataNoAlive)
	}
	ptNum := data.GetClusterPtNum()
	for ptId := 0; ptId < int(ptNum); ptId++ {
		pos := ptId % len(aliveDataNodes)
		data.updatePtStatus(name, uint32(ptId), aliveDataNodes[pos].ID, Offline)
	}
	return nil
}

func (data *Data) CreateDBPtView(name string) (bool, error) {
	if data.PtView == nil {
		data.PtView = make(map[string]DBPtInfos)
	}

	// pt view is already exist
	if data.PtView[name] != nil {
		return false, nil
	}
	return true, dbPtAssigner[config.GetHaPolicy()](data, name)
}

// CloneDatabases returns a copy of the DatabaseInfo.
func (data *Data) CloneDBPtView() map[string]DBPtInfos {
	if data.PtView == nil {
		return nil
	}
	dbPts := make(map[string]DBPtInfos, len(data.PtView))

	for db, ptView := range data.PtView {
		dbView := make([]PtInfo, len(ptView))
		copy(dbView, ptView)
		dbPts[db] = dbView
	}
	return dbPts
}

func (data *Data) initDataNodePtView(ptNum uint32) {
	newPtNum := data.PtNumPerNode * data.GetWriteNodeNum()
	if ptNum < newPtNum {
		data.SetClusterPtNum(newPtNum)
	}
}

func (data *Data) checkStoreReady() error {
	if data.GetClusterPtNum() == 0 {
		return ErrStorageNodeNotReady
	}
	return nil
}

func (data *Data) CheckCanCreateDatabase(name string) error {
	if name == "" {
		return ErrDatabaseNameRequired
	}

	if err := data.checkStoreReady(); err != nil {
		return err
	}

	databaseInfo := data.Database(name)
	if databaseInfo == nil {
		return nil
	}
	if databaseInfo.MarkDeleted {
		return fmt.Errorf("can't create same DB Name:%s when DB is being deleted", name)
	}
	return ErrDatabaseExists
}

// CreateDatabase creates a new database.
// It returns an error if name is blank or if a database with the same name already exists.
func (data *Data) CreateDatabase(dbName string, rpi *RetentionPolicyInfo, shardKey *proto2.ShardKeyInfo, enableTagArray bool, replicaN uint32,
	options *proto2.ObsOptions) error {
	err := data.CheckCanCreateDatabase(dbName)
	if err != nil {
		if err == ErrDatabaseExists {
			return nil
		}
		return err
	}

	dbi := NewDatabase(dbName)
	if rpi != nil {
		if err = data.CheckCanCreateRetentionPolicy(dbi, rpi, true); err != nil {
			return err
		}
		data.SetRetentionPolicy(dbi, rpi, true)
	}

	if shardKey != nil {
		ski := &ShardKeyInfo{}
		ski.unmarshal(shardKey)
		dbi.ShardKey = *ski
	}

	dbi.EnableTagArray = enableTagArray
	dbi.ReplicaN = int(replicaN)
	if options != nil {
		dbi.Options = UnmarshalObsOptions(options)
	}
	err = data.SetDatabase(dbi)
	return err
}

func (data *Data) SetDatabase(dbi *DatabaseInfo) error {
	if data.Databases == nil {
		data.Databases = make(map[string]*DatabaseInfo)
	}
	data.Databases[dbi.Name] = dbi
	return nil
}

func (data *Data) SetStream(info *StreamInfo) error {
	if data.Streams == nil {
		data.Streams = make(map[string]*StreamInfo)
	}
	if v := data.Streams[info.Name]; v != nil && !v.Equal(info) {
		return errno.NewError(errno.StreamHasExist)
	}
	info.ID = data.MaxStreamID
	data.MaxStreamID++
	data.Streams[info.Name] = info
	return nil
}

func (data *Data) GetAliveDataNodeNum() int {
	aliveNum := 0
	for _, dataNode := range data.DataNodes {
		if dataNode.Status == serf.StatusAlive {
			aliveNum++
		}
	}
	return aliveNum
}

func (data *Data) MarkDatabaseDelete(name string) error {
	dbi := data.Databases[name]
	if dbi == nil {
		return errno.NewError(errno.DatabaseNotFound, name)
	}
	if e := data.CheckStreamExistInDatabase(name); e != nil {
		return e
	}
	if dbi.MarkDeleted {
		return errno.NewError(errno.DatabaseIsBeingDelete, name)
	}
	if err := data.checkMigrateConflict(name); err != nil {
		return err
	}
	dbi.MarkDeleted = true
	return nil
}

// DropDatabase removes a database by name. It does not return an error
// if the database cannot be found.
func (data *Data) DropDatabase(name string) {
	delete(data.Databases, name)
	delete(data.ReplicaGroups, name)

	for i := range data.Users {
		delete(data.Users[i].Privileges, name)
	}

	if data.PtView != nil {
		delete(data.PtView, name)
	}
}

// RetentionPolicy returns a retention policy for a database by name.
func (data *Data) RetentionPolicy(database, name string) (*RetentionPolicyInfo, error) {
	di, err := data.GetDatabase(database)
	if err != nil {
		return nil, err
	}

	return di.GetRetentionPolicy(name)
}

func (data *Data) CheckCanCreateRetentionPolicy(dbi *DatabaseInfo, rpi *RetentionPolicyInfo, makeDefault bool) error {
	// Validate retention policy.
	if rpi == nil {
		return ErrRetentionPolicyRequired
	} else if rpi.Name == "" {
		return ErrRetentionPolicyNameRequired
	}

	if err := rpi.CheckSpecValid(); err != nil {
		return err
	}

	existRp := dbi.RetentionPolicy(rpi.Name)
	if existRp == nil {
		if dbi.ReplicaN != 0 && rpi.ReplicaN != dbi.ReplicaN {
			return ErrReplicaNConflict
		}
		return nil
	}

	if !existRp.EqualsAnotherRp(rpi) {
		return ErrRetentionPolicyConflict
	}

	if makeDefault && dbi.DefaultRetentionPolicy != rpi.Name {
		return ErrRetentionPolicyConflict
	}
	return ErrRetentionPolicyExists
}

func (data *Data) SetRetentionPolicy(dbi *DatabaseInfo, rpi *RetentionPolicyInfo, makeDefault bool) {
	if dbi.RetentionPolicies == nil {
		dbi.RetentionPolicies = make(map[string]*RetentionPolicyInfo)
	}
	dbi.RetentionPolicies[rpi.Name] = rpi
	if makeDefault {
		dbi.DefaultRetentionPolicy = rpi.Name
	}
}

func (data *Data) CreateRetentionPolicy(dbName string, rpi *RetentionPolicyInfo, makeDefault bool) error {
	dbi, err := data.GetDatabase(dbName)
	if err != nil {
		return err
	}
	err = data.CheckCanCreateRetentionPolicy(dbi, rpi, makeDefault)
	if err != nil {
		if err == ErrRetentionPolicyExists {
			return nil
		}
		return err
	}
	data.SetRetentionPolicy(dbi, rpi, makeDefault)
	return nil
}

// DropRetentionPolicy removes a retention policy from a database by name.
func (data *Data) DropRetentionPolicy(database, name string) error {
	// Find database.
	di, err := data.GetDatabase(database)
	if err != nil {
		return err
	}
	if di == nil {
		return nil
	}

	if di.RetentionPolicies == nil {
		return nil
	}
	delete(di.RetentionPolicies, name)

	return nil
}

func (data *Data) MarkRetentionPolicyDelete(database, name string) error {
	rp, err := data.RetentionPolicy(database, name)
	if err != nil {
		return err
	}
	if e := data.CheckStreamExistInRetention(database, name); e != nil {
		return e
	}
	if err = data.checkMigrateConflict(database); err != nil {
		return err
	}
	rp.MarkDeleted = true

	return nil
}

func (data *Data) MarkMeasurementDelete(database, policy, measurement string) error {
	mst, err := data.Measurement(database, policy, measurement)
	if err != nil {
		return err
	}
	if e := data.CheckStreamExistInMst(database, policy, measurement); e != nil {
		return e
	}
	if err = data.checkMigrateConflict(database); err != nil {
		return err
	}
	mst.MarkDeleted = true
	return nil
}

func (data *Data) DropMeasurement(database, policy, nameWithVer string) error {
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	}
	if rpi == nil {
		return nil
	}

	if rpi.Measurements == nil {
		return nil
	}
	for k, m := range rpi.Measurements {
		if k == nameWithVer && m.MarkDeleted {
			delete(rpi.Measurements, nameWithVer)
			break
		}
	}
	return nil
}

// SetDefaultRetentionPolicy sets the default retention policy for a database.
func (data *Data) SetDefaultRetentionPolicy(database, name string) error {
	di, err := data.GetDatabase(database)
	if err != nil {
		return err
	}

	if _, err = di.GetRetentionPolicy(name); err != nil {
		return err
	}
	di.DefaultRetentionPolicy = name

	return nil
}

func (data *Data) ShowShardsFromMst(db string, rp string, mst string) models.Rows {
	var rows models.Rows
	rpi, err := data.RetentionPolicy(db, rp)
	if rpi == nil || err != nil {
		return nil
	}
	mstInfo, err := rpi.GetMeasurement(mst)
	if err != nil {
		return nil
	}
	shardIdxes := mstInfo.ShardIdexes
	row := &models.Row{Columns: []string{"id", "database", "retention_policy", "measurement", "shard_group"}, Name: db + "." + rp + "." + mst}
	shardGroups := rpi.ShardGroups
	if shardIdxes == nil {
		for _, shardGroup := range shardGroups {
			if shardGroup.Deleted() {
				continue
			}
			shard := shardGroup.Shards
			for _, shardInfo := range shard {
				row.Values = append(row.Values, []interface{}{
					shardInfo.ID,
					db,
					rp,
					mst,
					shardGroup.ID,
				})
			}
		}
	} else {
		sgIDToShards := make(map[uint64][]ShardInfo)
		for _, shardGroup := range shardGroups {
			if shardGroup.Deleted() {
				continue
			}
			if _, ok := shardIdxes[shardGroup.ID]; ok {
				sgIDToShards[shardGroup.ID] = shardGroup.Shards
			}
		}
		for shardGroup, shardIdx := range shardIdxes {
			for _, shard := range shardIdx {
				if _, ok := sgIDToShards[shardGroup]; !ok {
					continue
				}
				row.Values = append(row.Values, []interface{}{
					sgIDToShards[shardGroup][shard].ID,
					db,
					rp,
					mst,
					shardGroup,
				})
			}
		}
	}

	rows = append(rows, row)
	return rows
}

func (data *Data) ShowShards() models.Rows {
	var rows models.Rows
	data.WalkDatabases(func(db *DatabaseInfo) {
		row := &models.Row{Columns: []string{"id", "database", "retention_policy", "shard_group", "start_time", "end_time", "expiry_time", "owners", "tier", "downSample_level"}, Name: db.Name}
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			rp.WalkShardGroups(func(sg *ShardGroupInfo) {
				if sg.Deleted() {
					return
				}
				sg.walkShards(func(sh *ShardInfo) {
					row.Values = append(row.Values, []interface{}{
						sh.ID,
						db.Name,
						rp.Name,
						sg.ID,
						sg.StartTime.UTC().Format(time.RFC3339),
						sg.EndTime.UTC().Format(time.RFC3339),
						sg.EndTime.Add(rp.Duration).UTC().Format(time.RFC3339),
						joinUint64(data.GetDbPtOwners(db.Name, sh.Owners)),
						TierToString(sh.Tier),
						sh.DownSampleLevel,
					})
				})
			})
		})
		rows = append(rows, row)
	})
	return rows
}

func joinUint64(a []uint64) string {
	var buf bytes.Buffer
	for i, x := range a {
		buf.WriteString(strconv.FormatUint(x, 10))
		if i < len(a)-1 {
			buf.WriteRune(',')
		}
	}
	return buf.String()
}

func (data *Data) ShowShardGroups() models.Rows {
	row := &models.Row{Columns: []string{"id", "database", "retention_policy", "start_time", "end_time", "expiry_time"}, Name: "shard groups"}
	data.WalkDatabases(func(db *DatabaseInfo) {
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			rp.WalkShardGroups(func(sg *ShardGroupInfo) {
				if sg.Deleted() {
					return
				}

				row.Values = append(row.Values, []interface{}{
					sg.ID,
					db.Name,
					rp.Name,
					sg.StartTime.UTC().Format(time.RFC3339),
					sg.EndTime.UTC().Format(time.RFC3339),
					sg.EndTime.Add(rp.Duration).UTC().Format(time.RFC3339),
				})
			})
		})
	})

	return []*models.Row{row}
}

func (data *Data) ShowSubscriptions() models.Rows {
	var rows models.Rows
	data.WalkDatabases(func(db *DatabaseInfo) {
		row := &models.Row{Columns: []string{"retention_policy", "name", "mode", "destinations"}, Name: db.Name}
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			for i := range rp.Subscriptions {
				row.Values = append(row.Values, []interface{}{rp.Name, rp.Subscriptions[i].Name, rp.Subscriptions[i].Mode, rp.Subscriptions[i].Destinations})
			}
		})
		if len(row.Values) > 0 {
			rows = append(rows, row)
		}
	})
	return rows
}

func (data *Data) ShowRetentionPolicies(database string) (models.Rows, error) {
	dbi := data.Database(database)
	if dbi == nil {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}

	row := &models.Row{Columns: []string{"name", "duration", "shardGroupDuration", "hot duration", "warm duration", "index duration", "replicaN", "default"}}
	dbi.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
		row.Values = append(row.Values, []interface{}{rp.Name, rp.Duration.String(), rp.ShardGroupDuration.String(),
			rp.HotDuration.String(), rp.WarmDuration.String(), rp.IndexGroupDuration.String(), rp.ReplicaN, dbi.DefaultRetentionPolicy == rp.Name})
	})

	sort.Slice(row.Values, func(i, j int) bool {
		return row.Values[i][0].(string) < row.Values[j][0].(string)
	})
	return []*models.Row{row}, nil
}

func (data *Data) ShowCluster(nodeType string, ID uint64) (*ShowClusterInfo, error) {
	clusterInfo := &ShowClusterInfo{}
	timestamp := time.Now().UTC().UnixNano()

	switch nodeType {
	case METANODE:
		data.WalkMetaNodes(func(node *NodeInfo) {
			if ID == 0 || node.ID == ID {
				clusterInfo.Nodes = append(clusterInfo.Nodes, NodeRow{timestamp, node.Status.String(), node.Host, node.ID, METANODE})
			}
		})
	case DATANODE:
		data.WalkDataNodes(func(node *DataNode) {
			if ID == 0 || node.ID == ID {
				clusterInfo.Nodes = append(clusterInfo.Nodes, NodeRow{timestamp, node.Status.String(), node.Host, node.ID, DATANODE})
			}
		})
	default:
		data.WalkMetaNodes(func(node *NodeInfo) {
			if ID == 0 || node.ID == ID {
				clusterInfo.Nodes = append(clusterInfo.Nodes, NodeRow{timestamp, node.Status.String(), node.Host, node.ID, METANODE})
			}
		})
		data.WalkDataNodes(func(node *DataNode) {
			if ID == 0 || node.ID == ID {
				clusterInfo.Nodes = append(clusterInfo.Nodes, NodeRow{timestamp, node.Status.String(), node.Host, node.ID, DATANODE})
			}
		})
	}
	if len(clusterInfo.Nodes) == 0 {
		return nil, errno.NewError(errno.InValidNodeID, ID)
	}

	data.WalkMigrateEvents(func(eventId string, info *MigrateEventInfo) {
		if ID == 0 || (nodeType != METANODE && (ID == info.src || ID == info.dest)) {
			eventTypeStr, currStateStr, preStateStr := getEventStr(EventType(info.eventType), info.currState, info.preState)
			clusterInfo.Events = append(clusterInfo.Events, EventRow{info.opId, eventTypeStr, info.pt.Db, info.pt.Pti.PtId, info.src, info.dest, currStateStr, preStateStr})
		}
	})

	return clusterInfo, nil
}

type AssignState int

const (
	Init         AssignState = 0
	StartAssign  AssignState = 8
	AssignFailed AssignState = 9
	Assigned     AssignState = 10
	Final        AssignState = 11
)

func getAssignEventStateStr(s AssignState) string {
	switch s {
	case Init:
		return "init"
	case StartAssign:
		return "startAssign"
	case Assigned:
		return "assigned"
	case AssignFailed:
		return "assignFailed"
	default:
		return "unknown"
	}
}

type EventType int

const (
	AssignType EventType = iota
	OffloadType
	MoveType
)

type MoveState int

var MoveStateStr []string = []string{"move_init", "move_preOffload", "move_rollbackPreoffload", "move_preAssign",
	"move_rollbackPreAssign", "move_offload", "move_offloadFailed", "move_offloaded", "move_assign", "move_assignFailed",
	"move_assigned", "move_final"}

const (
	MoveInit               MoveState = 0
	MovePreOffload         MoveState = 1
	MoveRollbackPreOffload MoveState = 2
	MovePreAssign          MoveState = 3
	MoveRollbackPreAssign  MoveState = 4 // rollback preAssign in store when preAssign failed
	MoveOffload            MoveState = 5 // if offload failed retry do not rollback preAssign
	MoveOffloadFailed      MoveState = 6
	MoveOffloaded          MoveState = 7
	MoveAssign             MoveState = 8
	MoveAssignFailed       MoveState = 9
	MoveAssigned           MoveState = 10
	MoveFinal              MoveState = 11
)

func (s MoveState) String() string {
	if int(s) < len(MoveStateStr) {
		return MoveStateStr[s]
	}
	return "unknown assign state"
}

func getEventStr(eventType EventType, currState, preState int) (string, string, string) {
	eventTypeStr := "unknown"
	currStateStr := ""
	preStateStr := ""
	switch eventType {
	case AssignType:
		eventTypeStr = "assign_event"
		currStateStr, preStateStr = getAssignEventStateStr(AssignState(currState)), getAssignEventStateStr(AssignState(preState))
	case OffloadType:
		eventTypeStr = "offload_event"
	case MoveType:
		eventTypeStr = "move_event"
		currStateStr, preStateStr = MoveState(currState).String(), MoveState(preState).String()
	}
	return eventTypeStr, currStateStr, preStateStr
}

func (data *Data) GetDbPtOwners(database string, ptIds []uint32) []uint64 {
	ownerIDs := make([]uint64, len(ptIds))
	if data.PtView[database] == nil {
		logger.GetLogger().Error("db pt not found in ptView", zap.String("db", database), zap.Uint32s("pt", ptIds))
		return nil
	}

	for i := range ptIds {
		nodeID := data.PtView[database][ptIds[i]].Owner.NodeID
		ownerIDs[i] = nodeID
	}
	return ownerIDs
}

func (data *Data) GetDbPtOwner(database string, ptId uint32) (uint64, error) {
	if data.PtView[database] == nil {
		return 0, fmt.Errorf("pt Owner found no ptViews ptId:%d; database:%s", ptId, database)
	}
	return data.PtView[database][ptId].Owner.NodeID, nil
}

// UpdateRetentionPolicy updates an existing retention policy.
func (data *Data) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error {
	// Find database.
	di, err := data.GetDatabase(database)
	if err != nil {
		return err
	}

	rpi, err := di.GetRetentionPolicy(name)
	if err != nil {
		return err
	}

	if rpi.HasDownSamplePolicy() && (rpu.Duration != nil && rpi.Duration != *rpu.Duration) {
		return errno.NewError(errno.DownSamplePolicyExists)
	}
	// Ensure new policy doesn't match an existing policy.
	err = di.checkUpdateRetentionPolicyName(name, rpu.Name)
	if err != nil {
		return err
	}

	checkRpi := &RetentionPolicyInfo{
		Duration:           *LoadDurationOrDefault(rpu.Duration, &rpi.Duration),
		ShardGroupDuration: *LoadDurationOrDefault(rpu.ShardGroupDuration, &rpi.ShardGroupDuration),
		IndexGroupDuration: *LoadDurationOrDefault(rpu.IndexGroupDuration, &rpi.IndexGroupDuration),
		HotDuration:        *LoadDurationOrDefault(rpu.HotDuration, &rpi.HotDuration),
		WarmDuration:       *LoadDurationOrDefault(rpu.WarmDuration, &rpi.WarmDuration),
	}
	err = checkRpi.CheckSpecValid()
	if err != nil {
		return err
	}

	if rpu.Name != nil {
		checkRpi.Name = *rpu.Name
	} else {
		checkRpi.Name = rpi.Name
	}

	rpi.updateWithOtherRetentionPolicy(checkRpi)

	if makeDefault {
		di.DefaultRetentionPolicy = rpi.Name
	}

	return nil
}

// DropShard removes a shard by ID.
//
// DropShard won't return an error if the shard can't be found, which
// allows the command to be re-run in the case that the meta store
// succeeds but a data node fails.
func (data *Data) DropShard(id uint64) {
	found := -1
	for dbidx, dbi := range data.Databases {
		for rpidx, rpi := range dbi.RetentionPolicies {
			for sgidx, sg := range rpi.ShardGroups {
				for sidx, s := range sg.Shards {
					if s.ID == id {
						found = sidx
						break
					}
				}

				if found > -1 {
					shards := sg.Shards
					data.Databases[dbidx].RetentionPolicies[rpidx].ShardGroups[sgidx].Shards = append(shards[:found], shards[found+1:]...)

					if len(shards) == 1 {
						// We just deleted the last shard in the shard group.
						data.Databases[dbidx].RetentionPolicies[rpidx].ShardGroups[sgidx].DeletedAt = time.Now()
					}
					return
				}
			}
		}
	}
}

// ShardGroups returns a list of all shard groups on a database and retention policy.
func (data *Data) ShardGroups(database, policy string) ([]ShardGroupInfo, error) {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (data *Data) ShardGroupsByTimeRange(database, policy string, tmin, tmax time.Time) ([]ShardGroupInfo, error) {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() || !g.Overlaps(tmin, tmax) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

func (data *Data) GetTierOfShardGroup(database, policy string, timestamp time.Time, defaultTier uint64, engineType config.EngineType) (*ShardGroupInfo, uint64, error) {
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, 0, err
	}

	sgi := rpi.ShardGroupByTimestampAndEngineType(timestamp, engineType)
	if sgi != nil {
		return sgi, 0, nil
	}

	startTime := timestamp.Truncate(rpi.ShardGroupDuration).UTC()
	endTime := startTime.Add(rpi.ShardGroupDuration).UTC()
	if endTime.After(time.Unix(0, models.MaxNanoTime)) {
		// Shard group range is [start, end) so add one to the max time.
		endTime = time.Unix(0, models.MaxNanoTime+1)
	}

	tier := defaultTier
	now := time.Now()
	if rpi.HotDuration > 0 {
		if endTime.Add(rpi.HotDuration).Before(now) {
			tier = util.Warm
		}
	}
	return nil, tier, nil
}

// ShardGroupByTimestampAndEngineType returns the shard group on a database and policy for a given timestamp and engine type.
func (data *Data) ShardGroupByTimestampAndEngineType(database, policy string, timestamp time.Time, engineType config.EngineType) (*ShardGroupInfo, error) {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, ErrRetentionPolicyNotFound(policy)
	}

	return rpi.ShardGroupByTimestampAndEngineType(timestamp, engineType), nil
}

func (data *Data) mapShardsToMst(database string, rpi *RetentionPolicyInfo, sgi *ShardGroupInfo) {
	var numOfShards int32
	for mstName, mstInfo := range rpi.Measurements {
		if mstInfo.InitNumOfShards == 0 { // Default
			continue
		} else {
			numOfShards = mstInfo.InitNumOfShards
		}
		if mstInfo.ShardIdexes != nil {
			mstInfo.ShardIdexes[sgi.ID] = mapShards(mstName, sgi.Shards, numOfShards)
		} else {
			DataLogger.Error("mapShardsToMst err", zap.String("mstName", mstName), zap.Int32("InitNumOfShards", mstInfo.InitNumOfShards))
		}
	}
}

// This method could be improved later.
func mapShards(mstName string, shards []ShardInfo, numOfShards int32) []int {
	rand.Seed(int64(HashID([]byte(mstName))))
	randomSlice := rand.Perm(int(len(shards)))
	shardsIdx := randomSlice[:numOfShards]
	sort.Ints(shardsIdx)
	return shardsIdx
}

func (data *Data) createShards(database string, sgi *ShardGroupInfo, igi *IndexGroupInfo,
	rpi *RetentionPolicyInfo, msti *MeasurementInfo, tier uint64) {
	// Determine shard count by node count divided by replication factor.
	// This will ensure nodes will get distributed across nodes evenly and
	// replicated the correct number of times.
	shardN := int(data.GetClusterPtNum())

	var lastSgi *ShardGroupInfo
	if len(msti.ShardKeys) > 0 && msti.ShardKeys[0].Type == RANGE {
		if len(rpi.ShardGroups) == 0 {
			sgi.Shards = make([]ShardInfo, 1)
		} else {
			lastSgi = &rpi.ShardGroups[len(rpi.ShardGroups)-1]
			sgi.Shards = make([]ShardInfo, len(lastSgi.Shards))
		}
	} else {
		sgi.Shards = make([]ShardInfo, shardN)
	}

	for i := range sgi.Shards {
		data.MaxShardID++
		sgi.Shards[i] = ShardInfo{ID: data.MaxShardID, Tier: tier}

		for ptId := 0; ptId < shardN; ptId++ {
			if ptId%shardN == i {
				sgi.Shards[i].Owners = append(sgi.Shards[i].Owners, uint32(ptId))
				sgi.Shards[i].IndexID = igi.Indexes[ptId].ID
			}
		}

		if lastSgi != nil {
			sgi.Shards[i].Min = lastSgi.Shards[i].Min
			sgi.Shards[i].Max = lastSgi.Shards[i].Max
		}
	}
}

func (data *Data) newShardGroup(rpi *RetentionPolicyInfo, timestamp time.Time, engineType config.EngineType, version uint32) *ShardGroupInfo {
	startTime := timestamp.Truncate(rpi.ShardGroupDuration)
	data.MaxShardGroupID++
	sgi := ShardGroupInfo{
		ID:         data.MaxShardGroupID,
		StartTime:  startTime.UTC(),
		EndTime:    startTime.Add(rpi.ShardGroupDuration).UTC(),
		EngineType: engineType,
		Version:    version,
	}
	if sgi.EndTime.After(time.Unix(0, models.MaxNanoTime)) {
		// Shard group range is [start, end) so add one to the max time.
		sgi.EndTime = time.Unix(0, models.MaxNanoTime+1)
	}
	return &sgi
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (data *Data) CreateShardGroup(database, policy string, timestamp time.Time, tier uint64, engineType config.EngineType, version uint32) error {
	if err := data.checkStoreReady(); err != nil {
		return err
	}

	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	}

	// Verify that shard group doesn't already exist for this timestamp.
	if rpi.ShardGroupByTimestampAndEngineType(timestamp, engineType) != nil {
		return nil
	}

	var msti *MeasurementInfo
	for _, mst := range rpi.Measurements {
		msti = mst
		break
	}

	if msti == nil {
		return fmt.Errorf("there is no measurement in database %s policy %s", database, policy)
	}

	//check index group contain this shard group
	ptNum := data.GetClusterPtNum()
	igi := data.createIndexGroupIfNeeded(rpi, timestamp, engineType, ptNum)

	// Create the shard group.
	sgi := data.newShardGroup(rpi, timestamp, engineType, version)

	// Create shards on the group.
	data.createShards(database, sgi, igi, rpi, msti, tier)

	// Map shards to measurements, only works for hash sharding.
	if msti.ShardKeys[0].Type == HASH {
		data.mapShardsToMst(database, rpi, sgi)
	}

	// Retention policy has a new shard group, so update the policy. Shard
	// Groups must be stored in sorted order, as other parts of the system
	// assume this to be the case.
	rpi.ShardGroups = append(rpi.ShardGroups, *sgi)
	sort.Sort(ShardGroupInfos(rpi.ShardGroups))

	return nil
}

func (data *Data) CreateIndexGroup(rpi *RetentionPolicyInfo, timestamp time.Time, engineType config.EngineType, ptNum uint32) *IndexGroupInfo {
	data.MaxIndexGroupID++
	igi := IndexGroupInfo{}
	igi.ID = data.MaxIndexGroupID
	igi.StartTime = timestamp.Truncate(rpi.IndexGroupDuration).UTC()
	igi.EndTime = igi.StartTime.Add(rpi.IndexGroupDuration).UTC()
	if igi.EndTime.After(time.Unix(0, models.MaxNanoTime)) {
		igi.EndTime = time.Unix(0, models.MaxNanoTime+1)
	}
	igi.EngineType = engineType
	igi.Indexes = make([]IndexInfo, ptNum)
	for i := range igi.Indexes {
		data.MaxIndexID++
		igi.Indexes[i] = IndexInfo{ID: data.MaxIndexID, Owners: []uint32{uint32(i)}}
	}
	rpi.IndexGroups = append(rpi.IndexGroups, igi)
	sort.Sort(IndexGroupInfos(rpi.IndexGroups))
	return &igi
}

func (data *Data) createIndexGroupIfNeeded(rpi *RetentionPolicyInfo, timestamp time.Time, engineType config.EngineType, ptNum uint32) *IndexGroupInfo {
	if len(rpi.IndexGroups) == 0 {
		return data.CreateIndexGroup(rpi, timestamp, engineType, ptNum)
	}

	var igIdx int
	for igIdx = len(rpi.IndexGroups) - 1; igIdx >= 0; igIdx-- {
		if rpi.IndexGroups[igIdx].EngineType == engineType && rpi.IndexGroups[igIdx].Contains(timestamp) {
			break
		}
	}
	if igIdx >= 0 && len(rpi.IndexGroups[igIdx].Indexes) >= int(ptNum) {
		return &rpi.IndexGroups[igIdx]
	}
	return data.CreateIndexGroup(rpi, timestamp, engineType, ptNum)
}

func (data *Data) expandDBPtView(database string, ptNum uint32, newNode *DataNode) {
	dbPtInfos := data.DBPtView(database)
	oldDBPtNums := uint32(len(dbPtInfos))
	if ptNum == oldDBPtNums {
		return
	} else if ptNum < oldDBPtNums {
		assert(false, fmt.Sprintf("expand dbPT db:%s from %d to %d", database, oldDBPtNums, ptNum))
	}

	replicaN := data.DBReplicaN(database)
	dataNodeNum := uint32(len(data.DataNodes))
	DataLogger.Info("expand db ptview", zap.String("db", database), zap.Uint32("from", oldDBPtNums), zap.Uint32("to", ptNum),
		zap.Int("replicaN", replicaN), zap.Uint32("total node num", dataNodeNum))

	for ptId := oldDBPtNums; ptId < ptNum; ptId++ {
		// set pt status offline, and set it online when assigned successfully
		data.updatePtStatus(database, ptId, newNode.ID, Offline)
	}

	if replicaN > 1 {
		data.chooseRG(database, newNode, replicaN)
	}
}

func (data *Data) chooseRG(database string, newNode *DataNode, replicaN int) {
	ChooseRGFns[repDisPolicy](data, database, newNode, replicaN)
}

func (data *Data) ExpandGroups() {
	data.WalkDatabasesOrderly(func(db *DatabaseInfo) {
		ptNum := data.GetClusterPtNum()
		db.WalkRetentionPolicyOrderly(func(rp *RetentionPolicyInfo) {
			if rp.shardingType() == RANGE {
				return
			}

			rp.walkIndexGroups(func(ig *IndexGroupInfo) {
				for i := len(ig.Indexes); i < int(ptNum); i++ {
					data.MaxIndexID++
					ig.Indexes = append(ig.Indexes, IndexInfo{
						ID:     data.MaxIndexID,
						Owners: []uint32{uint32(i)},
					})
				}
			})

			rp.WalkShardGroups(func(sg *ShardGroupInfo) {
				for i := len(sg.Shards); i < int(ptNum); i++ {
					igi := data.createIndexGroupIfNeeded(rp, sg.StartTime, sg.EngineType, ptNum)
					data.MaxShardID++
					sg.Shards = append(sg.Shards, ShardInfo{ID: data.MaxShardID, Owners: []uint32{uint32(i)}, IndexID: igi.Indexes[i].ID, Tier: sg.Shards[i-1].Tier})
				}
			})
		})
	})
}

func (data *Data) UpdatePtVersion(db string, ptId uint32) error {
	dbPtView, ok := data.PtView[db]
	if !ok {
		return errno.NewError(errno.DatabaseNotFound)
	}
	if ptId >= uint32(len(dbPtView)) || ptId != data.PtView[db][ptId].PtId {
		return errno.NewError(errno.PtNotFound)
	}
	data.PtView[db][ptId].Ver += 1
	return nil
}

func (data *Data) DeleteIndexGroup(database, policy string, id uint64) error {
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	}

	for i := range rpi.IndexGroups {
		if rpi.IndexGroups[i].ID == id {
			rpi.IndexGroups[i].DeletedAt = time.Now().UTC()
			break
		}
	}
	return nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (data *Data) DeleteShardGroup(database, policy string, id uint64, deletedAt int64, deleteType int32) error {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	}
	// Find shard group by ID and set its deletion timestamp.
	for i := range rpi.ShardGroups {
		if rpi.ShardGroups[i].ID == id {
			if deleteType == CancelDelete {
				// mark shardGroup as no deleted
				rpi.ShardGroups[i].DeletedAt = time.Time{}
			} else {
				// mark shardGroup as deleted
				if deletedAt != 0 {
					//When delay-deleted, the deletedAt time cannot be updated with the raft playback, so deletedAt is specified by the client.
					rpi.ShardGroups[i].DeletedAt = time.Unix(0, deletedAt) // logkeeper
				} else {
					rpi.ShardGroups[i].DeletedAt = time.Now().UTC() // metricStore
				}
			}
			break
		}
	}

	return nil
}

func (data *Data) PruneGroups(shardGroup bool, id uint64) error {
	if shardGroup {
		return data.pruneShardGroups(id)
	} else {
		return data.pruneIndexGroups(id)
	}
}

// remove all expired indexgroups.
func (data *Data) pruneIndexGroups(id uint64) error {
	data.WalkDatabases(func(db *DatabaseInfo) {
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			for idx := 0; idx < len(rp.IndexGroups); {
				if id >= rp.IndexGroups[idx].Indexes[0].ID && id <= rp.IndexGroups[idx].Indexes[len(rp.IndexGroups[idx].Indexes)-1].ID {
					pos := sort.Search(len(rp.IndexGroups[idx].Indexes), func(i int) bool {
						return rp.IndexGroups[idx].Indexes[i].ID >= id
					})
					rp.IndexGroups[idx].Indexes[pos].MarkDelete = true
				}
				if rp.IndexGroups[idx].canDelete() {
					rp.IndexGroups = append(rp.IndexGroups[:idx],
						rp.IndexGroups[idx+1:]...)
				} else {
					idx++
				}
			}
		})
	})

	return nil
}

// remove all expired shardgroups.
func (data *Data) pruneShardGroups(id uint64) error {
	data.WalkDatabases(func(db *DatabaseInfo) {
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			var endTime int64
			deleteSg := false
			for idx := 0; idx < len(rp.ShardGroups); {
				if id >= rp.ShardGroups[idx].Shards[0].ID && id <= rp.ShardGroups[idx].Shards[len(rp.ShardGroups[idx].Shards)-1].ID {
					pos := sort.Search(len(rp.ShardGroups[idx].Shards), func(i int) bool {
						return rp.ShardGroups[idx].Shards[i].ID >= id
					})
					rp.ShardGroups[idx].Shards[pos].MarkDelete = true
				}

				if !rp.ShardGroups[idx].DeletedAt.IsZero() && rp.ShardGroups[idx].canDelete() {
					for _, mstInfo := range rp.Measurements {
						if mstInfo.InitNumOfShards == 0 {
							continue
						}
						delete(mstInfo.ShardIdexes, rp.ShardGroups[idx].ID)
					}
					if rp.ShardGroups[idx].EndTime.UnixNano() > endTime {
						endTime = rp.ShardGroups[idx].EndTime.UnixNano()
					}
					rp.ShardGroups = append(rp.ShardGroups[:idx], rp.ShardGroups[idx+1:]...)
					deleteSg = true
				} else {
					idx++
				}
			}
			if SchemaCleanEn && deleteSg {
				data.SchemaClean(rp, endTime, db)
			}
		})
	})
	return nil
}

func (data *Data) SchemaClean(rp *RetentionPolicyInfo, sgEndTime int64, db *DatabaseInfo) {
	for _, msti := range rp.Measurements {
		leftSchema := msti.SchemaClean(sgEndTime)
		if msti.EngineType == config.TSSTORE && leftSchema == 0 {
			data.MarkMeasurementDelete(db.Name, rp.Name, msti.originName)
		}
	}
}

// CreateSubscription adds a named subscription to a database and retention policy.
func (data *Data) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	rpi, err := data.RetentionPolicy(database, rp)
	if err != nil {
		return err
	}

	// Ensure the name doesn't already exist.
	for i := range rpi.Subscriptions {
		if rpi.Subscriptions[i].Name == name {
			return ErrSubscriptionExists
		}
	}

	// Append new query.
	rpi.Subscriptions = append(rpi.Subscriptions, SubscriptionInfo{
		Name:         name,
		Mode:         mode,
		Destinations: destinations,
	})

	data.MaxSubscriptionID++
	return nil
}

// DropSubscription removes a subscription.
func (data *Data) DropSubscription(database, rp, name string) error {
	// Drop all subscriptions
	if database == "" {
		for _, db := range data.Databases {
			for _, rp := range db.RetentionPolicies {
				rp.Subscriptions = rp.Subscriptions[:0]
			}
		}
		data.MaxSubscriptionID++
		return nil
	}

	// Drop all subscriptions on the Specified db
	if name == "" {
		db, ok := data.Databases[database]
		if !ok {
			return ErrDatabaseNotExists
		}
		for _, rp := range db.RetentionPolicies {
			rp.Subscriptions = rp.Subscriptions[:0]
		}
		data.MaxSubscriptionID++
		return nil
	}

	// if rp is not specified, traverse the rps
	if rp == "" {
		db, ok := data.Databases[database]
		if !ok {
			return ErrDatabaseNotExists
		}
		for _, rpi := range db.RetentionPolicies {
			for i := range rpi.Subscriptions {
				if rpi.Subscriptions[i].Name == name {
					rpi.Subscriptions = append(rpi.Subscriptions[:i], rpi.Subscriptions[i+1:]...)
					data.MaxSubscriptionID++
					return nil
				}
			}
		}
	}

	rpi, err := data.RetentionPolicy(database, rp)
	if err != nil {
		return err
	}

	for i := range rpi.Subscriptions {
		if rpi.Subscriptions[i].Name == name {
			rpi.Subscriptions = append(rpi.Subscriptions[:i], rpi.Subscriptions[i+1:]...)
			data.MaxSubscriptionID++
			return nil
		}
	}
	return ErrSubscriptionNotFound
}

func (data *Data) GetUser(username string) *UserInfo {
	for i := range data.Users {
		if data.Users[i].Name == username {
			return &data.Users[i]
		}
	}
	return nil
}

// User returns a GetUser by username.
func (data *Data) User(username string) User {
	u := data.GetUser(username)
	if u == nil {
		// prevent non-nil interface with nil pointer
		return nil
	}
	return u
}

// CreateUser creates a new GetUser.
func (data *Data) CreateUser(name, hash string, admin, rwuser bool) error {
	// Ensure the GetUser doesn't already exist.
	if name == "" {
		return ErrUsernameRequired
	} else if data.User(name) != nil {
		return ErrUserExists
	}

	if admin && data.HasAdminUser() {
		return ErrUserForbidden
	}

	// Append new GetUser.
	data.Users = append(data.Users, UserInfo{
		Name:   name,
		Hash:   hash,
		Admin:  admin,
		Rwuser: rwuser,
	})

	// We know there is now at least one admin GetUser.
	if admin {
		data.AdminUserExists = true
	}

	return nil
}

// DropUser removes an existing GetUser by name.
func (data *Data) DropUser(name string) error {
	if u := data.GetUser(name); u != nil {
		if u.Admin {
			return ErrUserDropSelf
		}
	}

	for i := range data.Users {
		if data.Users[i].Name == name {
			data.Users = append(data.Users[:i], data.Users[i+1:]...)
			return nil
		}
	}

	return ErrUserNotFound
}

// UpdateUser updates the password hash of an existing GetUser.
func (data *Data) UpdateUser(name, hash string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			if data.Users[i].Hash == hash {
				return ErrPwdUsed
			}
			data.Users[i].Hash = hash
			return nil
		}
	}
	return ErrUserNotFound
}

// CloneUsers returns a copy of the GetUser infos.
func (data *Data) CloneUsers() []UserInfo {
	if len(data.Users) == 0 {
		return []UserInfo{}
	}
	users := make([]UserInfo, len(data.Users))
	for i := range data.Users {
		users[i] = data.Users[i].clone()
	}

	return users
}

// SetPrivilege sets a privilege for a GetUser on a database.
func (data *Data) SetPrivilege(name, database string, p originql.Privilege) error {
	ui := data.GetUser(name)
	if ui == nil {
		return ErrUserNotFound
	}

	_, err := data.GetDatabase(database)
	if err != nil {
		return err
	}

	if ui.Privileges == nil {
		ui.Privileges = make(map[string]originql.Privilege)
	}
	ui.Privileges[database] = p

	return nil
}

// SetAdminPrivilege sets the admin privilege for a GetUser.
func (data *Data) SetAdminPrivilege(name string, admin bool) error {
	ui := data.GetUser(name)
	if ui == nil {
		return ErrUserNotFound
	}

	return ErrGrantOrRevokeAdmin
}

// AdminUserExist returns true if an admin GetUser exists.
func (data *Data) AdminUserExist() bool {
	return data.AdminUserExists
}

// UserPrivileges gets the privileges for a GetUser.
func (data *Data) UserPrivileges(name string) (map[string]originql.Privilege, error) {
	ui := data.GetUser(name)
	if ui == nil {
		return nil, ErrUserNotFound
	}

	return ui.Privileges, nil
}

// UserPrivilege gets the privilege for a GetUser on a database.
func (data *Data) UserPrivilege(name, database string) (*originql.Privilege, error) {
	ui := data.GetUser(name)
	if ui == nil {
		return nil, ErrUserNotFound
	}

	for db, p := range ui.Privileges {
		if db == database {
			return &p, nil
		}
	}

	return originql.NewPrivilege(originql.NoPrivileges), nil
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := *data

	// Copy nodes.
	other.DataNodes = data.CloneDataNodes()
	other.MetaNodes = data.CloneMetaNodes()

	other.Databases = data.CloneDatabases()
	other.Streams = data.CloneStreams()
	other.Users = data.CloneUsers()
	other.PtView = data.CloneDBPtView()
	other.MigrateEvents = data.CloneMigrateEvents()

	other.QueryIDInit = data.CloneQueryIDInit()

	return &other
}

// Marshal serializes data to a protobuf representation.
func (data *Data) Marshal() *proto2.Data {
	pb := data.MarshalBase()
	pb.MigrateEvents = make([]*proto2.MigrateEventInfo, len(data.MigrateEvents))
	i := 0
	for eventStr := range data.MigrateEvents {
		pb.MigrateEvents[i] = data.MigrateEvents[eventStr].marshal()
		i++
	}
	return pb
}

func (data *Data) MarshalBase() *proto2.Data {
	pb := &proto2.Data{
		Term:         proto.Uint64(data.Term),
		Index:        proto.Uint64(data.Index),
		ClusterID:    proto.Uint64(data.ClusterID),
		ClusterPtNum: proto.Uint32(data.ClusterPtNum),

		MaxShardGroupID: proto.Uint64(data.MaxShardGroupID),
		MaxShardID:      proto.Uint64(data.MaxShardID),
		MaxMstID:        proto.Uint64(data.MaxMstID),
		MaxIndexGroupID: proto.Uint64(data.MaxIndexGroupID),
		MaxIndexID:      proto.Uint64(data.MaxIndexID),

		// Need this for reverse compatibility
		MaxNodeID: proto.Uint64(data.MaxNodeID),

		PtNumPerNode:    proto.Uint32(data.PtNumPerNode),
		NumOfShards:     proto.Int32(data.NumOfShards),
		MaxEventOpId:    proto.Uint64(data.MaxEventOpId),
		TakeOverEnabled: proto.Bool(data.TakeOverEnabled),
		BalancerEnabled: proto.Bool(data.BalancerEnabled),

		MaxDownSampleID:   proto.Uint64(data.MaxDownSampleID),
		MaxStreamID:       proto.Uint64(data.MaxStreamID),
		MaxConnId:         proto.Uint64(data.MaxConnID),
		MaxSubscriptionID: proto.Uint64(data.MaxSubscriptionID),
		MaxCQChangeID:     proto.Uint64(data.MaxCQChangeID),
	}
	if data.SQLite != nil {
		pb.IsSQLiteEnabled = proto.Bool(true)
	}

	pb.DataNodes = make([]*proto2.DataNode, len(data.DataNodes))
	for i := range data.DataNodes {
		pb.DataNodes[i] = data.DataNodes[i].marshal()
	}

	pb.MetaNodes = make([]*proto2.NodeInfo, len(data.MetaNodes))
	for i := range data.MetaNodes {
		pb.MetaNodes[i] = data.MetaNodes[i].marshal()
	}

	pb.SqlNodes = make([]*proto2.DataNode, len(data.SqlNodes))
	for i := range data.SqlNodes {
		pb.SqlNodes[i] = data.SqlNodes[i].marshal()
	}

	pb.PtView = make(map[string]*proto2.DBPtInfo, len(data.PtView))
	for key, dbView := range data.PtView {
		dbPi := &proto2.DBPtInfo{
			DbPt: make([]*proto2.PtInfo, len(dbView)),
		}
		for i := range dbView {
			dbPi.DbPt[i] = dbView[i].Marshal()
		}
		pb.PtView[key] = dbPi
	}

	pb.Databases = make([]*proto2.DatabaseInfo, len(data.Databases))
	i := 0
	for dbName := range data.Databases {
		pb.Databases[i] = data.Databases[dbName].marshal()
		i++
	}

	pb.Streams = make([]*proto2.StreamInfo, len(data.Streams))
	j := 0
	for si := range data.Streams {
		pb.Streams[j] = data.Streams[si].Marshal()
		j++
	}

	pb.Users = make([]*proto2.UserInfo, len(data.Users))
	for i := range data.Users {
		pb.Users[i] = data.Users[i].marshal()
	}

	pb.QueryIDInit = make(map[string]uint64, len(data.QueryIDInit))
	for host := range data.QueryIDInit {
		pb.QueryIDInit[string(host)] = data.QueryIDInit[host]
	}

	if len(data.ReplicaGroups) > 0 {
		pb.ReplicaGroups = make(map[string]*proto2.Replications, len(data.ReplicaGroups))
		for dbname, repls := range data.ReplicaGroups {
			replication := &proto2.Replications{
				Groups: make([]*proto2.ReplicaGroup, len(repls)),
			}
			for i = range repls {
				replication.Groups[i] = repls[i].marshal()
			}
			pb.ReplicaGroups[dbname] = replication
		}
	}
	return pb
}

func (data *Data) MarshalV2() *proto2.Data {
	return data.MarshalBase()
}

// unmarshal deserializes from a protobuf representation.
func (data *Data) Unmarshal(pb *proto2.Data) {
	data.Term = pb.GetTerm()
	data.Index = pb.GetIndex()
	data.UpdateNodeTmpIndexCommandStart = data.Index
	data.ClusterID = pb.GetClusterID()
	data.ClusterPtNum = pb.GetClusterPtNum()

	data.MaxNodeID = pb.GetMaxNodeID()
	data.MaxShardGroupID = pb.GetMaxShardGroupID()
	data.MaxShardID = pb.GetMaxShardID()
	data.MaxMstID = pb.GetMaxMstID()
	data.PtNumPerNode = pb.GetPtNumPerNode()
	data.NumOfShards = pb.GetNumOfShards()
	data.MaxIndexGroupID = pb.GetMaxIndexGroupID()
	data.MaxIndexID = pb.GetMaxIndexID()
	data.MaxEventOpId = pb.GetMaxEventOpId()
	data.TakeOverEnabled = pb.GetTakeOverEnabled()
	data.BalancerEnabled = pb.GetBalancerEnabled()
	data.MaxDownSampleID = pb.GetMaxDownSampleID()
	data.MaxStreamID = pb.GetMaxStreamID()
	data.MaxConnID = pb.GetMaxConnId()
	data.MaxSubscriptionID = pb.GetMaxSubscriptionID()
	data.MaxCQChangeID = pb.GetMaxCQChangeID()

	if pb.GetIsSQLiteEnabled() {
		data.SQLite = &SQLiteWrapper{}
	}

	data.DataNodes = make([]DataNode, len(pb.GetDataNodes()))
	for i, x := range pb.GetDataNodes() {
		data.DataNodes[i].unmarshal(x)
	}

	data.MetaNodes = make([]NodeInfo, len(pb.GetMetaNodes()))
	for i, x := range pb.GetMetaNodes() {
		data.MetaNodes[i].unmarshal(x)
	}

	data.SqlNodes = make([]DataNode, len(pb.GetSqlNodes()))
	for i, x := range pb.GetSqlNodes() {
		data.SqlNodes[i].unmarshal(x)
	}

	data.PtView = make(map[string]DBPtInfos, len(pb.GetPtView()))
	for key, dbPi := range pb.GetPtView() {
		dbView := make([]PtInfo, len(dbPi.DbPt))
		for i, x := range dbPi.GetDbPt() {
			dbView[i].unmarshal(x)
		}
		data.PtView[key] = dbView
	}

	data.Databases = make(map[string]*DatabaseInfo, len(pb.GetDatabases()))
	for _, x := range pb.GetDatabases() {
		dbi := &DatabaseInfo{}
		dbi.unmarshal(x)
		data.Databases[dbi.Name] = dbi
	}

	data.Streams = make(map[string]*StreamInfo, len(pb.GetStreams()))
	for _, s := range pb.GetStreams() {
		si := &StreamInfo{}
		si.Unmarshal(s)
		data.Streams[si.Name] = si
	}

	data.Users = make([]UserInfo, len(pb.GetUsers()))
	for i, x := range pb.GetUsers() {
		data.Users[i].unmarshal(x)
	}

	data.MigrateEvents = make(map[string]*MigrateEventInfo, len(pb.GetMigrateEvents()))
	for _, me := range pb.GetMigrateEvents() {
		mei := &MigrateEventInfo{}
		mei.unmarshal(me)
		data.MigrateEvents[mei.eventId] = mei
	}
	// Exhaustively determine if there is an admin GetUser. The marshalled cache
	// value may not be correct.
	data.AdminUserExists = data.HasAdminUser()

	data.QueryIDInit = make(map[SQLHost]uint64, len(pb.GetQueryIDInit()))
	for host := range pb.QueryIDInit {
		data.QueryIDInit[SQLHost(host)] = pb.QueryIDInit[host]
	}

	if len(pb.ReplicaGroups) == 0 {
		return
	}
	data.ReplicaGroups = make(map[string][]ReplicaGroup, len(pb.ReplicaGroups))
	for dbname, rgs := range pb.ReplicaGroups {
		data.ReplicaGroups[dbname] = make([]ReplicaGroup, len(rgs.Groups))
		for i := range rgs.Groups {
			data.ReplicaGroups[dbname][i].unmarshal(rgs.Groups[i])
		}
	}
}

// MarshalBinary encodes the metadata to a binary format.
func (data *Data) MarshalBinary() ([]byte, error) {
	return proto.Marshal(data.Marshal())
}

// UnmarshalBinary decodes the object from a binary format.
func (data *Data) UnmarshalBinary(buf []byte) error {
	var pb proto2.Data
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	data.Unmarshal(&pb)
	return nil
}

// HasAdminUser exhaustively checks for the presence of at least one admin
// GetUser.
func (data *Data) HasAdminUser() bool {
	for _, u := range data.Users {
		if u.Admin {
			return true
		}
	}
	return false
}

// ImportData imports selected data into the current metadata.
// if non-empty, backupDBName, restoreDBName, backupRPName, restoreRPName can be used to select DB metadata from other,
// and to assign a new name to the imported data.  Returns a map of shard ID's in the old metadata to new shard ID's
// in the new metadata, along with a list of new databases created, both of which can assist in the import of existing
// shard data during a database restore.
func (data *Data) ImportData(other Data, backupDBName, restoreDBName, backupRPName, restoreRPName string) (map[uint64]uint64, []string, error) {
	shardIDMap := make(map[uint64]uint64)
	if backupDBName != "" {
		dbName, err := data.importOneDB(other, backupDBName, restoreDBName, backupRPName, restoreRPName, shardIDMap)
		if err != nil {
			return nil, nil, err
		}

		return shardIDMap, []string{dbName}, nil
	}

	// if no backupDBName then we'll try to import all the DB's.  If one of them fails, we'll mark the whole
	// operation a failure and return an error.
	var newDBs []string
	for _, dbi := range other.Databases {
		if dbi.Name == "_internal" {
			continue
		}
		dbName, err := data.importOneDB(other, dbi.Name, "", "", "", shardIDMap)
		if err != nil {
			return nil, nil, err
		}
		newDBs = append(newDBs, dbName)
	}
	return shardIDMap, newDBs, nil
}

// importOneDB imports a single database/rp from an external metadata object, renaming them if new names are provided.
func (data *Data) importOneDB(other Data, backupDBName, restoreDBName, backupRPName, restoreRPName string, shardIDMap map[uint64]uint64) (string, error) {

	dbPtr := other.Database(backupDBName)
	if dbPtr == nil || dbPtr.MarkDeleted {
		return "", fmt.Errorf("imported metadata does not have datbase named %s", backupDBName)
	}

	if restoreDBName == "" {
		restoreDBName = backupDBName
	}

	if data.Database(restoreDBName) != nil {
		return "", errors.New("database already exists")
	}
	// change the names if we want/need to
	err := data.CreateDatabase(restoreDBName, nil, nil, false, 1, nil)
	if err != nil {
		return "", err
	}
	dbImport := data.Database(restoreDBName)

	if backupRPName != "" {
		rpPtr := dbPtr.RetentionPolicy(backupRPName)

		if rpPtr != nil && !rpPtr.MarkDeleted {
			rpImport := rpPtr.Clone()
			if restoreRPName == "" {
				restoreRPName = backupRPName
			}
			rpImport.Name = restoreRPName
			dbImport.RetentionPolicies = make(map[string]*RetentionPolicyInfo)
			dbImport.RetentionPolicies[rpImport.Name] = rpImport
			dbImport.DefaultRetentionPolicy = restoreRPName
		} else {
			return "", fmt.Errorf("retention Policy not found in meta backup: %s.%s", backupDBName, backupRPName)
		}

	} else { // import all RP's without renaming
		dbImport.DefaultRetentionPolicy = dbPtr.DefaultRetentionPolicy
		if dbPtr.RetentionPolicies != nil {
			dbImport.RetentionPolicies = make(map[string]*RetentionPolicyInfo)
			for i := range dbPtr.RetentionPolicies {
				dbImport.RetentionPolicies[i] = dbPtr.RetentionPolicies[i].Clone()
			}
		}

	}

	// renumber the shard groups and shards for the new retention policy(ies)
	for _, rpImport := range dbImport.RetentionPolicies {
		for j, sgImport := range rpImport.ShardGroups {
			data.MaxShardGroupID++
			rpImport.ShardGroups[j].ID = data.MaxShardGroupID
			for k := range sgImport.Shards {
				data.MaxShardID++
				shardIDMap[sgImport.Shards[k].ID] = data.MaxShardID
				sgImport.Shards[k].ID = data.MaxShardID
			}
		}
	}

	return restoreDBName, nil
}

func (data *Data) UpdateShardInfoTier(shardID uint64, shardTier uint64, dbName, rpName string) error {
	rpi, err := data.RetentionPolicy(dbName, rpName)
	if err != nil {
		return err
	}

	for i := range rpi.ShardGroups {
		for j := range rpi.ShardGroups[i].Shards {
			if rpi.ShardGroups[i].Shards[j].ID == shardID {
				rpi.ShardGroups[i].Shards[j].Tier = shardTier
				return nil
			}
		}
	}
	return fmt.Errorf("cannot find shard %d for rp %s on database %s", shardID, rpName, dbName)
}

func (data *Data) UpdateIndexInfoTier(indexID uint64, indexTier uint64, dbName, rpName string) error {
	rpi, err := data.RetentionPolicy(dbName, rpName)
	if err != nil {
		return err
	}

	for i := range rpi.IndexGroups {
		for j := range rpi.IndexGroups[i].Indexes {
			if rpi.IndexGroups[i].Indexes[j].ID == indexID {
				// todo: rpi.IndexGroups[i].Indexes[j].Tier = indexTier
				return nil
			}
		}
	}
	return fmt.Errorf("cannot find index %d for rp %s on database %s", indexID, rpName, dbName)
}

func (data *Data) UpdateNodeStatus(id uint64, status int32, lTime uint64, gossipPort string) error {
	// do not take over
	if !data.TakeOverEnabled {
		return nil
	}
	dn := data.DataNode(id)
	if dn == nil {
		return errno.NewError(errno.DataNodeNotFound, id)
	}

	if lTime < dn.LTime {
		DataLogger.Error("event is older", zap.Uint64("id", id), zap.Int32("status", status), zap.Uint64("ltime", lTime), zap.Uint64("dnLtime", dn.LTime))
		return errno.NewError(errno.OlderEvent)
	}

	updateStatus := serf.MemberStatus(status)
	// node cannot join into cluster after network faulty and no need to handle repeated event
	// in write-available-first policy, pt can not assign to other node, so make the node join cluster and do not kill it self
	if config.GetHaPolicy() == config.SharedStorage && updateStatus == serf.StatusAlive && dn.ConnID == dn.AliveConnID {
		return errno.NewError(errno.DataNodeSplitBrain)
	}
	dn.Status = updateStatus
	dn.LTime = lTime
	if updateStatus == serf.StatusAlive {
		dn.AliveConnID = dn.ConnID
	}
	if dn.GossipAddr == "" {
		host, _, _ := net.SplitHostPort(dn.Host)
		dn.GossipAddr = fmt.Sprintf("%s:%s", host, gossipPort)
	}

	data.updatePtViewStatus(id, Offline)
	return nil
}

func (data *Data) UpdateSqlNodeStatus(id uint64, status int32, lTime uint64, gossipPort string) error {
	dn := data.SqlNode(id)
	if dn == nil {
		return errno.NewError(errno.SqlNodeNotFound, id)
	}

	if lTime < dn.LTime {
		DataLogger.Error("event is older", zap.Uint64("id", id), zap.Int32("status", status), zap.Uint64("ltime", lTime), zap.Uint64("dnLtime", dn.LTime))
		return errno.NewError(errno.OlderEvent)
	}

	updateStatus := serf.MemberStatus(status)
	dn.Status = updateStatus
	dn.LTime = lTime
	if updateStatus == serf.StatusAlive {
		dn.AliveConnID = dn.ConnID
	}
	if dn.GossipAddr == "" {
		DataLogger.Error("UpdateSqlNodeStatus GossipAddr err", zap.String("dn.TCPHost", dn.TCPHost), zap.Uint64("id", id))
	}

	return nil
}

func (data *Data) UpdateMetaNodeStatus(id uint64, status int32, lTime uint64, gossipPort string) error {
	dn := data.MetaNode(id)
	if dn == nil {
		return errno.NewError(errno.MetaNodeNotFound, id, gossipPort)
	}

	if lTime < dn.LTime {
		DataLogger.Error("event is older", zap.Uint64("id", id), zap.Int32("status", status), zap.Uint64("ltime", lTime), zap.Uint64("dnLtime", dn.LTime))
		return errno.NewError(errno.OlderEvent)
	}

	updateStatus := serf.MemberStatus(status)
	dn.Status = updateStatus
	dn.LTime = lTime
	return nil
}

// return pts for the nid
func (data *Data) updatePtViewStatus(nid uint64, status PtStatus) {
	for db := range data.PtView {
		for i := range data.PtView[db] {
			if data.PtView[db][i].Owner.NodeID == nid {
				data.PtView[db][i].Status = status
				data.PtView[db][i].Ver += 1
				if status == Offline && config.GetHaPolicy() == config.Replication {
					data.nextHealth(db, data.PtView[db][i].RGID, data.PtView[db])
				}
			}
		}
	}

}

func (data *Data) nextHealth(db string, rgId uint32, dbPtView DBPtInfos) {
	rg := data.GetRGOfPtFast(rgId, db)
	replicasN, ok := data.GetReplicaN(db)
	if rg != nil && ok {
		rg.nextHealth(db, dbPtView, replicasN)
	}

}

type DbPtInfo struct {
	Db          string
	Pti         *PtInfo
	Shards      map[uint64]*ShardDurationInfo
	DBBriefInfo *DatabaseBriefInfo
}

func (pt *DbPtInfo) String() string {
	return fmt.Sprintf("%s%s%d", pt.Db, seperatorChar, pt.Pti.PtId)
}

func (pt *DbPtInfo) Marshal() *proto2.DbPt {
	pb := &proto2.DbPt{
		Db: proto.String(pt.Db),
		Pt: pt.Pti.Marshal(),
	}
	if len(pt.Shards) > 0 {
		pb.Shards = make(map[uint64]*proto2.ShardDurationInfo)
	}

	for sid := range pt.Shards {
		pb.Shards[sid] = pt.Shards[sid].marshal()
	}

	pb.DBBriefInfo = &proto2.DatabaseBriefInfo{
		Name:           proto.String(pt.Db),
		EnableTagArray: proto.Bool(pt.DBBriefInfo.EnableTagArray),
		Replicas:       proto.Int(pt.DBBriefInfo.Replicas),
	}
	return pb
}

func (pt *DbPtInfo) Unmarshal(pb *proto2.DbPt) {
	if pb.GetPt() != nil {
		pt.Pti = &PtInfo{}
		pt.Pti.unmarshal(pb.GetPt())
	}
	pt.Db = pb.GetDb()
	if len(pb.Shards) > 0 {
		pt.Shards = make(map[uint64]*ShardDurationInfo)
	}
	for sid := range pb.Shards {
		pt.Shards[sid] = &ShardDurationInfo{}
		pt.Shards[sid].unmarshal(pb.Shards[sid])
	}
	pt.DBBriefInfo = &DatabaseBriefInfo{}
	pt.DBBriefInfo.Name = pb.DBBriefInfo.GetName()
	pt.DBBriefInfo.EnableTagArray = pb.DBBriefInfo.GetEnableTagArray()
	pt.DBBriefInfo.Replicas = int(pb.DBBriefInfo.GetReplicas())
}

func (data *Data) GetShardDurationsByDbPt(db string, pt uint32) map[uint64]*ShardDurationInfo {
	dbi := data.Database(db)
	r := make(map[uint64]*ShardDurationInfo, 7)
	dbi.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
		if rp.MarkDeleted {
			return
		}
		rp.WalkShardGroups(func(sg *ShardGroupInfo) {
			// need remove shard directory in store
			if sg.Deleted() {
				return
			}
			if len(sg.Shards) > int(pt) {
				sh := sg.Shards[pt]
				durationInfo := &ShardDurationInfo{}
				durationInfo.Ident = ShardIdentifier{}
				durationInfo.Ident.ShardID = sh.ID
				durationInfo.Ident.ShardGroupID = sg.ID
				durationInfo.Ident.OwnerDb = db
				durationInfo.Ident.OwnerPt = pt
				durationInfo.Ident.Policy = rp.Name
				durationInfo.Ident.ShardType = rp.shardingType()
				durationInfo.Ident.DownSampleID = sh.DownSampleID
				durationInfo.Ident.DownSampleLevel = int(sh.DownSampleLevel)
				durationInfo.Ident.ReadOnly = sh.ReadOnly
				durationInfo.Ident.EngineType = uint32(sg.EngineType)
				durationInfo.DurationInfo = DurationDescriptor{}
				durationInfo.DurationInfo.Duration = rp.Duration
				durationInfo.DurationInfo.Tier = sh.Tier
				durationInfo.DurationInfo.TierDuration = rp.TierDuration(sh.Tier)
				r[sh.ID] = durationInfo
			}
		})
	})
	return r
}

// GetShardDurationsByDbPtForRetention is used to transfer shards that are not marked for deletion and delete them from the retention.
func (data *Data) GetShardDurationsByDbPtForRetention(db string, pt uint32) map[uint64]*ShardDurationInfo {
	dbi := data.Database(db)
	r := make(map[uint64]*ShardDurationInfo, 7)
	dbi.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
		if rp.MarkDeleted {
			return
		}
		rp.WalkShardGroups(func(sg *ShardGroupInfo) {
			if len(sg.Shards) > int(pt) {
				sh := sg.Shards[pt]
				// need remove shard directory in store
				if sh.MarkDelete {
					return
				}
				durationInfo := &ShardDurationInfo{}
				durationInfo.Ident = ShardIdentifier{}
				durationInfo.Ident.ShardID = sh.ID
				durationInfo.Ident.ShardGroupID = sg.ID
				durationInfo.Ident.OwnerDb = db
				durationInfo.Ident.OwnerPt = pt
				durationInfo.Ident.Policy = rp.Name
				durationInfo.Ident.ShardType = rp.shardingType()
				durationInfo.Ident.DownSampleID = sh.DownSampleID
				durationInfo.Ident.DownSampleLevel = int(sh.DownSampleLevel)
				durationInfo.Ident.ReadOnly = sh.ReadOnly
				durationInfo.Ident.EngineType = uint32(sg.EngineType)
				durationInfo.DurationInfo = DurationDescriptor{}
				durationInfo.DurationInfo.Duration = rp.Duration
				durationInfo.DurationInfo.Tier = sh.Tier
				durationInfo.DurationInfo.TierDuration = rp.TierDuration(sh.Tier)
				r[sh.ID] = durationInfo
			}
		})
	})
	return r
}

func (data *Data) GetFailedPtInfos(id uint64, status PtStatus) []*DbPtInfo {
	resPtInfos := make([]*DbPtInfo, 0, data.GetClusterPtNum())
	for db := range data.PtView {
		// do not get pt which db mark deleted
		if data.Databases[db] == nil || data.Database(db).MarkDeleted {
			continue
		}
		dbInfo := data.GetDBBriefInfo(db)
		dbInfo.Replicas = data.Databases[db].ReplicaN
		for i := range data.PtView[db] {
			if data.PtView[db][i].Owner.NodeID == id && data.PtView[db][i].Status == status {
				shards := data.GetShardDurationsByDbPtForRetention(db, data.PtView[db][i].PtId)
				pt := data.PtView[db][i]
				resPtInfos = append(resPtInfos, &DbPtInfo{Db: db, Pti: &pt, Shards: shards, DBBriefInfo: dbInfo})
			}
		}
	}
	return resPtInfos
}

func (data *Data) GetPtInfosByDbname(name string, enableTagArray bool, replicasN uint32) ([]*DbPtInfo, error) {
	resPtInfos := make([]*DbPtInfo, len(data.PtView[name]))
	if data.Database(name) != nil && data.Database(name).MarkDeleted {
		return nil, errno.NewError(errno.DatabaseIsBeingDelete)
	}
	dbBriefInfo := &DatabaseBriefInfo{
		Name:           name,
		EnableTagArray: enableTagArray,
		Replicas:       int(replicasN),
	}
	idx := 0
	for i := range data.PtView[name] {
		if data.PtView[name][i].Status == Offline {
			pt := data.PtView[name][i]
			if config.GetHaPolicy() == config.Replication && replicasN > 1 {
				if data.GetRGOfPtFast(pt.RGID, name).Status == UnFull {
					continue
				}
			}
			resPtInfos[idx] = &DbPtInfo{Db: name, Pti: &pt, DBBriefInfo: dbBriefInfo}
			idx++
		}
	}
	return resPtInfos[:idx], nil
}

func (data *Data) GetRGOfPtFast(rgId uint32, database string) *ReplicaGroup {
	rgs, ok := data.ReplicaGroups[database]
	if !ok {
		return nil
	}
	return &rgs[rgId]
}

func (data *Data) CreateMigrateEvent(e *proto2.MigrateEventInfo) error {
	if data.MigrateEvents == nil {
		data.MigrateEvents = make(map[string]*MigrateEventInfo)
	}
	if data.MigrateEvents[e.GetEventId()] != nil {
		if data.MigrateEvents[e.GetEventId()].src != e.GetSrc() ||
			data.MigrateEvents[e.GetEventId()].dest != e.GetDest() ||
			data.MigrateEvents[e.GetEventId()].currState != int(e.GetCurrState()) ||
			data.MigrateEvents[e.GetEventId()].preState != int(e.GetPreState()) {
			return errno.NewError(errno.PtEventIsAlreadyExist)
		}
		return nil
	}
	if err := data.checkDDLConflict(e); err != nil {
		return err
	}
	mei := &MigrateEventInfo{}
	mei.unmarshal(e)
	data.MaxEventOpId++
	mei.opId = data.MaxEventOpId
	data.MigrateEvents[mei.eventId] = mei
	return nil
}

func (data *Data) UpdateMigrateEvent(e *proto2.MigrateEventInfo) error {
	if data.MigrateEvents == nil || data.MigrateEvents[e.GetEventId()] == nil ||
		data.MigrateEvents[e.GetEventId()].opId != e.GetOpId() {
		return errno.NewError(errno.EventNotFound, e.GetEventId())
	}

	eventInfo := data.MigrateEvents[e.GetEventId()]
	eventInfo.currState = int(e.GetCurrState())
	eventInfo.preState = int(e.GetPreState())
	return nil
}

func (data *Data) RemoveEventInfo(eventId string) error {
	delete(data.MigrateEvents, eventId)
	return nil
}

func (data *Data) getNodeStatus(id uint64) (serf.MemberStatus, bool) {
	for _, node := range data.DataNodes {
		if node.ID == id {
			return node.Status, true
		}
	}
	return serf.StatusNone, false
}

func (data *Data) UpdatePtInfo(db string, info *proto2.PtInfo, ownerId uint64, status uint32) error {
	oldPtNum := len(data.PtView[db])
	if int(info.GetPtId()) >= oldPtNum {
		return errno.NewError(errno.PtNotFound)
	}

	curPtOwner := data.PtView[db][info.GetPtId()].Owner.NodeID
	// check ptInfo is not changed after get failed dbpts
	if curPtOwner != *(info.GetOwner().NodeID) ||
		data.PtView[db][info.GetPtId()].Status != PtStatus(info.GetStatus()) {
		return errno.NewError(errno.PtChanged)
	}
	if PtStatus(status) == Online {
		if nodeStatus, exist := data.getNodeStatus(ownerId); exist && nodeStatus != serf.StatusAlive {
			DataLogger.Info("UpdatePtInfo to online of a noAlive dataNode", zap.Uint64("nodeId", ownerId), zap.Int("nodeStatus", int(nodeStatus)), zap.String("db", db), zap.Uint32("ptId", info.GetPtId()))
			return nil
		}
	}
	data.updatePtStatus(db, info.GetPtId(), ownerId, PtStatus(status))
	return nil
}

func (data *Data) CloneMigrateEvents() map[string]*MigrateEventInfo {
	if data.MigrateEvents == nil {
		return nil
	}
	events := make(map[string]*MigrateEventInfo, len(data.MigrateEvents))
	for eventId := range data.MigrateEvents {
		events[eventId] = data.MigrateEvents[eventId].Clone()
	}
	return events
}

func (data *Data) CreateDownSamplePolicy(database, rpName string, info *DownSamplePolicyInfo) error {
	d := data.Database(database)
	id := data.MaxDownSampleID
	data.MaxDownSampleID++
	info.TaskID = id
	d.RetentionPolicies[rpName].DownSamplePolicyInfo = info
	d.RetentionPolicies[rpName].Duration = info.Duration
	return nil
}

func (data *Data) DropDownSamplePolicy(database, rpName string, dropAll bool) {
	if !dropAll {
		d := data.Database(database)
		d.RetentionPolicies[rpName].DownSamplePolicyInfo.DownSamplePolicies = nil
		d.RetentionPolicies[rpName].DownSamplePolicyInfo.Calls = nil
		return
	}
	for _, rpi := range data.Database(database).RetentionPolicies {
		rpi.DownSamplePolicyInfo = nil
	}
}

func (data *Data) ShowDownSamplePolicies(database string) (models.Rows, error) {
	dbi, err := data.GetDatabase(database)
	if err != nil {
		return nil, err
	}

	row := &models.Row{Columns: []string{"rpName", "field_operator", "duration", "sampleInterval", "timeInterval"}}
	dbi.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
		info := rp.DownSamplePolicyInfo
		if info == nil || info.IsNil() {
			return
		}
		row.Values = append(row.Values, []interface{}{rp.Name, info.Calls2String(), info.Duration.String(), info.SampleInterval2String(),
			info.TimeInterval2String()})
	})

	sort.Slice(row.Values, func(i, j int) bool {
		return row.Values[i][0].(string) < row.Values[j][0].(string)
	})
	return []*models.Row{row}, nil
}

// Marshal serializes data to a protobuf representation.
func (data *Data) MarshalUsers() *proto2.Data {
	pb := &proto2.Data{
		Term:         proto.Uint64(data.Term),
		Index:        proto.Uint64(data.Index),
		ClusterID:    proto.Uint64(data.ClusterID),
		ClusterPtNum: proto.Uint32(data.ClusterPtNum),

		MaxShardGroupID: proto.Uint64(data.MaxShardGroupID),
		MaxShardID:      proto.Uint64(data.MaxShardID),
		MaxIndexGroupID: proto.Uint64(data.MaxIndexGroupID),
		MaxIndexID:      proto.Uint64(data.MaxIndexID),

		// Need this for reverse compatibility
		MaxNodeID: proto.Uint64(data.MaxNodeID),

		PtNumPerNode:    proto.Uint32(data.PtNumPerNode),
		NumOfShards:     proto.Int32(data.NumOfShards),
		MaxEventOpId:    proto.Uint64(data.MaxEventOpId),
		TakeOverEnabled: proto.Bool(data.TakeOverEnabled),
	}
	pb.Users = make([]*proto2.UserInfo, len(data.Users))
	for i := range data.Users {
		pb.Users[i] = data.Users[i].marshal()
	}
	return pb
}

// MarshalBinary encodes the metadata to a binary format.
func (data *Data) MarshalBinaryUser() ([]byte, error) {
	return proto.Marshal(data.MarshalUsers())
}

func (data *Data) UpdateShardDownSampleInfo(ident *ShardIdentifier) error {
	database := data.Databases[ident.OwnerDb]
	if database == nil {
		return nil
	}
	rp := database.RetentionPolicies[ident.Policy]
	if rp == nil {
		return nil
	}
	shardGroups := rp.ShardGroups
	for i := range shardGroups {
		if inShardGroup(&shardGroups[i], ident.ShardID) {
			if int64(ident.DownSampleLevel) > shardGroups[i].Shard(ident.ShardID).DownSampleLevel {
				shardGroups[i].Shard(ident.ShardID).DownSampleLevel = int64(ident.DownSampleLevel)
			}
			shardGroups[i].Shard(ident.ShardID).ReadOnly = ident.ReadOnly
			shardGroups[i].Shard(ident.ShardID).DownSampleID = ident.DownSampleID
		}
	}
	return nil
}

func (data *Data) MarkTakeover(enable bool) {
	data.TakeOverEnabled = enable
}

func (data *Data) MarkBalancer(enable bool) {
	data.BalancerEnabled = enable
}

func (data *Data) CreateStream(info *StreamInfo) error {
	if info == nil {
		return nil
	}
	return data.SetStream(info)
}

func (data *Data) ShowStreams(database string, showAll bool) (models.Rows, error) {
	_, err := data.GetDatabase(database)
	if err != nil && !showAll {
		return nil, err
	}
	row := &models.Row{Columns: []string{"database", "retention", "measurement", "Name", "source measurement", "dimensions", "calls", "interval", "delay", "condition"}}
	for _, v := range data.Streams {
		if showAll || v.DesMst.Database == database {
			values := []interface{}{
				v.DesMst.Database,
				v.DesMst.RetentionPolicy,
				v.DesMst.Name,
				v.Name,
				v.SrcMst.Database + "." + v.SrcMst.RetentionPolicy + "." + v.SrcMst.Name,
				v.Dimensions(),
				v.CallsName(),
				v.Interval.String(),
				v.Delay.String(),
				v.Cond,
			}
			row.Values = append(row.Values, values)
		}
	}

	sort.Slice(row.Values, func(i, j int) bool {
		return row.Values[i][0].(string) < row.Values[j][0].(string)
	})
	return []*models.Row{row}, nil
}

func (data *Data) DropStream(name string) error {
	if _, ok := data.Streams[name]; !ok {
		return errno.NewError(errno.StreamNotFound)
	}
	delete(data.Streams, name)
	return nil
}

func (data *Data) CheckStreamExistInDatabase(database string) error {
	for _, v := range data.Streams {
		if v.SrcMst.Database == database || v.DesMst.Database == database {
			return dropStreamFirstError
		}
	}
	return nil
}

func (data *Data) CheckStreamExistInRetention(database, rp string) error {
	for _, v := range data.Streams {
		if (v.SrcMst.Database == database && v.SrcMst.RetentionPolicy == rp) || (v.DesMst.Database == database && v.DesMst.RetentionPolicy == rp) {
			return dropStreamFirstError
		}
	}
	return nil
}

func (data *Data) CheckStreamExistInMst(database, rp, mst string) error {
	for _, v := range data.Streams {
		if (v.SrcMst.Database == database && v.SrcMst.Name == mst && v.SrcMst.RetentionPolicy == rp) ||
			(v.DesMst.Database == database && v.DesMst.Name == mst && v.DesMst.RetentionPolicy == rp) {
			return dropStreamFirstError
		}
	}
	return nil
}

func (data *Data) checkMigrateConflict(database string) error {
	for i := 0; i < len(data.PtView[database]); i++ {
		eventId := fmt.Sprintf("%s%s%d", database, seperatorChar, i)
		if data.MigrateEvents[eventId] != nil {
			return errno.NewError(errno.ConflictWithEvent)
		}
	}
	return nil
}

func (data *Data) checkDDLConflict(e *proto2.MigrateEventInfo) error {
	if !e.GetCheckConflict() {
		return nil
	}
	dbi := data.Databases[e.Pti.GetDb()]
	if dbi == nil {
		return errno.NewError(errno.DatabaseNotFound)
	}
	if dbi.MarkDeleted {
		return errno.NewError(errno.DatabaseIsBeingDelete)
	}
	for rpName := range dbi.RetentionPolicies {
		rpi := dbi.RetentionPolicies[rpName]
		if rpi.MarkDeleted {
			return errno.NewError(errno.RpIsBeingDelete)
		}
		for mstIdx := range rpi.Measurements {
			if rpi.Measurements[mstIdx].MarkDeleted {
				return errno.NewError(errno.MstIsBeingDelete)
			}
		}
	}
	return nil
}

// RegisterQueryIDOffset register the mapping relationship between its host and query id offset for ts-sql
func (data *Data) RegisterQueryIDOffset(host SQLHost) error {
	if data.QueryIDInit == nil {
		data.QueryIDInit = make(map[SQLHost]uint64)
	}

	if _, ok := data.QueryIDInit[host]; ok {
		return nil
	}

	currentAssignedNum := len(data.QueryIDInit)
	newOffset := uint64(currentAssignedNum * QueryIDSpan)

	data.QueryIDInit[host] = newOffset

	return nil
}

func (data *Data) expandDBRG(db string) {
	if data.ReplicaGroups == nil {
		data.ReplicaGroups = make(map[string][]ReplicaGroup)
	}
	data.ReplicaGroups[db] = append(data.ReplicaGroups[db], make([]ReplicaGroup, 0)...)
}

func (data *Data) GetDBBriefInfo(name string) *DatabaseBriefInfo {
	dbBriefInfo := &DatabaseBriefInfo{
		Name:     name,
		Replicas: data.Databases[name].ReplicaN,
	}
	dbBriefInfo.EnableTagArray = data.Databases[name].EnableTagArray
	return dbBriefInfo
}

func (data *Data) GetReplicaGroup(db string, groupID uint32) *ReplicaGroup {
	rgs, ok := data.ReplicaGroups[db]
	if !ok {
		return nil
	}

	for i := range rgs {
		if rgs[i].ID == groupID {
			return &rgs[i]
		}
	}
	return nil
}

func (data *Data) GetPtInfo(name string, ptID uint32) *PtInfo {
	view, ok := data.PtView[name]
	if !ok {
		return nil
	}

	for i := range view {
		if view[i].PtId == ptID {
			return &view[i]
		}
	}
	return nil
}

func (data *Data) GetSegregateStatusByNodeId(nodeId uint64) uint64 {
	for _, dn := range data.DataNodes {
		if dn.ID == nodeId {
			return dn.SegregateStatus
		}
	}
	return Normal
}

func (data *Data) GetPtInfosByNodeId(id uint64) []*DbPtInfo {
	resPtInfos := make([]*DbPtInfo, 0, data.GetClusterPtNum())
	for db := range data.PtView {
		if data.Database(db) == nil {
			continue
		}
		dbInfo := data.GetDBBriefInfo(db)
		for i := range data.PtView[db] {
			if data.PtView[db][i].Owner.NodeID == id {
				shards := data.GetShardDurationsByDbPt(db, data.PtView[db][i].PtId)
				pt := data.PtView[db][i]
				resPtInfos = append(resPtInfos, &DbPtInfo{Db: db, Pti: &pt, Shards: shards, DBBriefInfo: dbInfo})
			}
		}
	}
	return resPtInfos
}

func (data *Data) GetNodeIdsByNodeLst(nodeLst []string) ([]uint64, []string, error) {
	nodeids := make([]uint64, 0, len(nodeLst))
	address := make([]string, 0, len(nodeLst))
	for _, host := range nodeLst {
		findNodeId := false
		for _, node := range data.DataNodes {
			nodeAddr := strings.Split(node.TCPHost, ":")[0]
			if host == nodeAddr {
				nodeids = append(nodeids, node.ID)
				address = append(address, node.TCPHost)
				findNodeId = true
				break
			}
		}
		if !findNodeId {
			return nil, nil, fmt.Errorf("some limit node ip is not correct: %s", host)
		}
	}
	return nodeids, address, nil
}

func (data *Data) GetNodeSegregateStatus(nodeIds []uint64) ([]uint64, error) {
	nodesSegregateStatus := make([]uint64, 0)
	for _, nodeId := range nodeIds {
		findNodeId := false
		for _, dn := range data.DataNodes {
			if dn.ID == nodeId {
				nodesSegregateStatus = append(nodesSegregateStatus, dn.SegregateStatus)
				findNodeId = true
				break
			}
		}
		if !findNodeId {
			return nil, fmt.Errorf("some node id is not find to get it's SegregateStatus: %d", nodeId)
		}
	}
	return nodesSegregateStatus, nil
}

func (data *Data) GetAllNodeSegregateStatus() []uint64 {
	nodesSegregateStatus := make([]uint64, 0, len(data.DataNodes))
	for _, dn := range data.DataNodes {
		nodesSegregateStatus = append(nodesSegregateStatus, dn.SegregateStatus)
	}
	return nodesSegregateStatus
}

func (data *Data) SetSegregateNodeStatus(status []uint64, nodeIds []uint64) {
	for i, id := range nodeIds {
		for idx, node := range data.DataNodes {
			if id == node.ID {
				data.DataNodes[idx].SegregateStatus = status[i]
				DataLogger.Info("set segregate dn status", zap.Uint64("node id", node.ID), zap.Uint64("flag", status[i]))
				break
			}
		}
	}
}

func (data *Data) GetNodeIDs() []uint64 {
	ids := make([]uint64, 0, len(data.DataNodes))
	for i := range data.DataNodes {
		ids = append(ids, data.DataNodes[i].ID)
	}
	return ids
}

func (data *Data) RemoveNode(nodeIds []uint64) {
	newDns := make([]DataNode, 0, len(data.DataNodes))
	for _, dn := range data.DataNodes {
		findNodeId := false
		for _, nodeId := range nodeIds {
			if dn.ID == nodeId {
				findNodeId = true
				break
			}
		}
		if !findNodeId {
			newDns = append(newDns, dn)
		}
	}
	data.DataNodes = newDns
}

// when masterPtId is offline, elect a new masterPtId
func (data *Data) UpdateReplication(database string, rgId, masterId uint32, peers []*proto2.Peer) (uint32, error) {
	rgs, ok := data.ReplicaGroups[database]
	if !ok {
		return 0, errno.NewError(errno.DatabaseNotFound, database)
	}
	rg := &rgs[rgId]
	oldMasterPtID := rg.MasterPtID
	rg.MasterPtID = masterId
	if len(peers) > 0 {
		rg.Peers = make([]Peer, len(peers))
		for i := range peers {
			rg.Peers[i].ID = peers[i].GetID()
			rg.Peers[i].PtRole = Role(peers[i].GetRole())
		}
	}
	return oldMasterPtID, nil
}

func (data *Data) CreateDBReplication(db string, replicaN uint32) error {
	if replicaN > 1 {
		return CreateDBRGFns[repDisPolicy](data, db, int(replicaN))
	}
	return nil
}

func (data *Data) UpdateMeasurement(db, rp, mst string, options *proto2.Options) error {
	rpi, err := data.RetentionPolicy(db, rp)
	if err != nil {
		return err
	}
	msti, err := rpi.GetMeasurement(mst)
	if err != nil {
		return err
	}

	if msti.Options != nil {
		msti.Options = &Options{}
		msti.Options.Unmarshal(options)
	}

	// perform upgrade compatibility judgment
	var newDuration int64
	ttl := options.GetTtl()
	if ttl >= int64(time.Hour)*24 {
		newDuration = ttl // old version value
	} else {
		newDuration = options.GetTtl() * int64(time.Hour) * 24 // new version value
	}
	if newDuration != *GetInt64Duration(&rpi.Duration) {
		rpi.Duration = *GetDuration(&newDuration)
	}
	return nil
}

func (data *Data) UpdateNodeTmpIndex(role int32, index uint64, nodeID uint64) error {
	if NodeType(role) == SQL {
		return data.SetSqlNodeIndex(nodeID, index)
	} else if NodeType(role) == STORE {
		return data.SetDataNodeIndex(nodeID, index)
	} else {
		DataLogger.Error("UpdateNodeTmpIndex err meta")
		return fmt.Errorf("UpdateNodeTmpIndex err meta")
	}
}

func (data *Data) GetNewRg(db string, rgId uint32, newMasterPtId uint32) (uint32, []Peer, error) {
	dbRgs, ok := data.ReplicaGroups[db]
	if !ok {
		return 0, nil, fmt.Errorf("no rg of db")
	}
	if rgId >= uint32(len(dbRgs)) {
		return 0, nil, fmt.Errorf("rgId > len(dbRgs), len:%v", len(dbRgs))
	}
	if newMasterPtId == dbRgs[rgId].MasterPtID {
		return 0, nil, fmt.Errorf("newMasterPtId == oldMasterPtId")
	}

	newPeers := make([]Peer, 0)
	newPeers = append(newPeers, Peer{ID: dbRgs[rgId].MasterPtID, PtRole: Slave})
	findNewMasterPtId := false
	for i := range dbRgs[rgId].Peers {
		if dbRgs[rgId].Peers[i].ID == newMasterPtId {
			findNewMasterPtId = true
		} else {
			newPeers = append(newPeers, Peer{ID: dbRgs[rgId].Peers[i].ID, PtRole: Slave})
		}
	}
	if !findNewMasterPtId || len(newPeers) != len(dbRgs[rgId].Peers) {
		return 0, nil, fmt.Errorf("newMasterPtId find err")
	}
	return newMasterPtId, newPeers, nil
}

func (data *Data) GetRgsOfNodeMasterPts(node DataNode) ([]*ReplicaGroup, []string) {
	rgs := make([]*ReplicaGroup, 0)
	dbs := make([]string, 0)
	for db, pts := range data.PtView {
		// 1.skip noRep pts
		dbInfo, ok := data.Databases[db]
		if !ok || dbInfo.ReplicaN <= 1 {
			continue
		}
		// 2.get full rgs those masterPts are node's pts
		for _, pt := range pts {
			if pt.Owner.NodeID == node.ID {
				rg := data.GetReplicaGroup(db, pt.RGID)
				if rg != nil && rg.Status != UnFull && rg.MasterPtID == pt.PtId {
					rgs = append(rgs, rg)
					dbs = append(dbs, db)
				}
			}
		}
	}
	return rgs, dbs
}

func (data *Data) DBRGN() int {
	rgn := 0
	for _, rgs := range data.ReplicaGroups {
		rgn += len(rgs)
	}
	return rgn
}

// MarshalTime converts t to nanoseconds since epoch. A zero time returns 0.
func MarshalTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// UnmarshalTime converts nanoseconds since epoch to time.
// A zero value returns a zero time.
func UnmarshalTime(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v).UTC()
}

func ValidShardKey(shardKeys []string) error {
	if len(shardKeys) == 0 {
		return nil
	}
	i := 0
	for i < len(shardKeys)-1 {
		if shardKeys[i] == "" {
			return ErrInvalidShardKey
		}
		if shardKeys[i] == shardKeys[i+1] {
			return ErrDuplicateShardKey
		}
		i++
	}
	if shardKeys[i] == "" {
		return ErrInvalidShardKey
	}
	return nil
}

func GetInt64Duration(duration *time.Duration) *int64 {
	if duration != nil {
		value := int64(*duration)
		return &value
	}
	return nil
}

func GetDuration(d *int64) *time.Duration {
	if d != nil {
		value := time.Duration(*d)
		return &value
	}
	return nil
}

func LoadDurationOrDefault(duration *time.Duration, existDuration *time.Duration) *time.Duration {
	if duration == nil {
		return existDuration
	}
	return duration
}

func inShardGroup(group *ShardGroupInfo, shardID uint64) bool {
	return shardID >= group.Shards[0].ID && shardID <= group.Shards[len(group.Shards)-1].ID
}
