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
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	// DefaultRetentionPolicyReplicaN is the default value of RetentionPolicyInfo.ReplicaN.
	DefaultRetentionPolicyReplicaN = 1

	// DefaultRetentionPolicyDuration is the default value of RetentionPolicyInfo.Duration.
	DefaultRetentionPolicyDuration = time.Duration(0)

	// DefaultRetentionPolicyName is the default name for auto generated retention policies.
	DefaultRetentionPolicyName = "autogen"

	// MinRetentionPolicyDuration represents the minimum duration for a policy.
	MinRetentionPolicyDuration = time.Hour
)

const (
	HASH          = "hash"
	RANGE         = "range"
	seperatorChar = "$"
)

var dropStreamFirstError = errors.New("stream task exists, drop it first")

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

// Data represents the top level collection of all metadata.
type Data struct {
	Term         uint64 // associated raft term
	Index        uint64 // associated raft index
	ClusterID    uint64
	ClusterPtNum uint32 // default number is the total cpu number of 16 nodes.
	PtNumPerNode uint32

	MetaNodes []NodeInfo
	DataNodes []DataNode           // data nodes
	PtView    map[string]DBPtInfos // PtView's key is dbname, value is PtInfo's slice.

	Databases     map[string]*DatabaseInfo
	Streams       map[string]*StreamInfo
	Users         []UserInfo
	MigrateEvents map[string]*MigrateEventInfo

	// adminUserExists provides a constant time mechanism for determining
	// if there is at least one admin GetUser.
	AdminUserExists    bool
	TakeOverEnabled    bool // set by syscontrol command
	BalancerEnabled    bool
	ExpandShardsEnable bool // set by config (not persistence)

	MaxNodeID       uint64
	MaxShardGroupID uint64
	MaxShardID      uint64
	MaxIndexGroupID uint64
	MaxIndexID      uint64
	MaxEventOpId    uint64
	MaxDownSampleID uint64
	MaxStreamID     uint64
	MaxConnID       uint64
}

var DataLogger *zap.Logger

type ReShardingInfo struct {
	Database     string
	Rp           string
	ShardGroupID uint64
	SplitTime    int64
	Bounds       []string
}

func (data *Data) WalkDatabases(fn func(db *DatabaseInfo)) {
	for dbName := range data.Databases {
		fn(data.Databases[dbName])
	}
}

func (data *Data) GetDurationInfos(dbPtIds map[string][]uint32) map[uint64]*ShardDurationInfo {
	r := make(map[uint64]*ShardDurationInfo)
	data.WalkDatabases(func(db *DatabaseInfo) {
		if db.MarkDeleted {
			return
		}
		if _, ok := dbPtIds[db.Name]; !ok {
			return
		}
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			if rp.MarkDeleted {
				return
			}
			rp.walkShardGroups(func(sg *ShardGroupInfo) {
				sg.walkShards(func(sh *ShardInfo) {
					for i := range dbPtIds[db.Name] {
						if sh.Owners[0] == dbPtIds[db.Name][i] {
							durationInfo := &ShardDurationInfo{}
							durationInfo.Ident = ShardIdentifier{}
							durationInfo.Ident.ShardID = sh.ID
							durationInfo.Ident.ShardGroupID = sg.ID
							durationInfo.Ident.OwnerDb = db.Name
							durationInfo.Ident.OwnerPt = dbPtIds[db.Name][i]
							durationInfo.Ident.Policy = rp.Name
							durationInfo.Ident.ShardType = rp.shardingType()
							durationInfo.Ident.DownSampleLevel = int(sh.DownSampleLevel)
							durationInfo.Ident.DownSampleID = sh.DownSampleID
							durationInfo.Ident.ReadOnly = sh.ReadOnly
							durationInfo.DurationInfo = DurationDescriptor{}
							durationInfo.DurationInfo.Duration = rp.Duration
							durationInfo.DurationInfo.Tier = sh.Tier
							durationInfo.DurationInfo.TierDuration = rp.TierDuration(sh.Tier)
							r[sh.ID] = durationInfo
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
			rp.walkShardGroups(func(sg *ShardGroupInfo) {
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

func (data *Data) Measurement(database, retentionPolicy, mst string) (*MeasurementInfo, error) {
	rp, err := data.RetentionPolicy(database, retentionPolicy)
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
	return msti, nil
}

func (data *Data) UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
	msti, err := data.Measurement(database, retentionPolicy, mst)
	if err != nil {
		return err
	}

	schema := make(map[string]int32)
	for field := range msti.Schema {
		schema[field] = msti.Schema[field]
	}

	for i := range fieldToCreate {
		existType, ok := schema[fieldToCreate[i].GetFieldName()]
		if !ok {
			schema[fieldToCreate[i].GetFieldName()] = fieldToCreate[i].GetFieldType()
			continue
		}
		if existType != fieldToCreate[i].GetFieldType() {
			return ErrFieldTypeConflict
		}
	}

	msti.Schema = schema
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
	data.createIndexGroup(rp, startTime)
	DataLogger.Info("reSharding info", zap.Time("splitTime", time.Unix(0, info.SplitTime+1)), zap.Any("bounds", info.Bounds))
	err = data.CreateShardGroupWithBounds(rp, startTime, info.Bounds)
	return err
}

func (data *Data) createIndexGroup(rp *RetentionPolicyInfo, startTime time.Time) {
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
	igi.Indexes = make([]IndexInfo, data.ClusterPtNum)
	for i := range igi.Indexes {
		data.MaxIndexID++
		igi.Indexes[i] = IndexInfo{ID: data.MaxIndexID, Owners: []uint32{uint32(i)}}
	}
	rp.IndexGroups = append(rp.IndexGroups, igi)
	sort.Sort(IndexGroupInfos(rp.IndexGroups))
}

func (data *Data) CreateShardGroupWithBounds(rp *RetentionPolicyInfo, startTime time.Time, bounds []string) error {
	// Create the shard group.
	data.MaxShardGroupID++
	sgi := ShardGroupInfo{}
	sgi.ID = data.MaxShardGroupID
	sgi.StartTime = startTime.UTC()
	lastSg := &rp.ShardGroups[len(rp.ShardGroups)-1]
	sgi.EndTime = lastSg.EndTime.UTC()

	igi := rp.IndexGroups[len(rp.IndexGroups)-1]
	shardN := len(bounds) + 1
	index := 0
	// Create shards on the group.
	sgi.Shards = make([]ShardInfo, shardN)
	for i := range sgi.Shards {
		data.MaxShardID++
		// FIXME shardTier should put in command
		sgi.Shards[i] = ShardInfo{ID: data.MaxShardID, Tier: lastSg.Shards[0].Tier}
		for ptId := 0; ptId < int(data.ClusterPtNum); ptId++ {
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
func (data *Data) createVersionMeasurement(rp *RetentionPolicyInfo, shardKey *proto2.ShardKeyInfo,
	indexR *proto2.IndexRelation, ski *ShardKeyInfo, mst string, version uint32) {
	sgLen := len(rp.ShardGroups)
	if sgLen == 0 {
		ski.ShardGroup = data.MaxShardGroupID + 1
	} else {
		ski.ShardGroup = 0
	}
	nameWithVer := influx.GetNameWithVersion(mst, version)

	msti := &MeasurementInfo{Name: nameWithVer, originName: mst}
	if shardKey != nil {
		msti.ShardKeys = []ShardKeyInfo{*ski}
	}

	if indexR != nil {
		newIndexR := IndexRelation{
			Rid:  indexR.GetRid(),
			Oids: indexR.GetOid(),
		}
		indexLists := indexR.GetIndexLists()
		newIndexR.IndexList = make([]*IndexList, len(indexLists))
		for i, iList := range indexLists {
			indexlist := make([]*IndexInfor, len(iList.GetIList()))
			for j, index := range iList.GetIList() {
				indexlist[j] = &IndexInfor{
					FieldName:  index.GetFieldName(),
					Tokens:     index.GetTokens(),
					Tokenizers: index.GetTokenizers(),
					IndexName:  index.GetIndexName()}
			}
			newIndexR.IndexList[i] = &IndexList{IList: indexlist}
		}
		msti.IndexRelation = newIndexR
	}

	if rp.Measurements == nil {
		rp.Measurements = make(map[string]*MeasurementInfo)
	}

	rp.MstVersions[mst] = version
	rp.Measurements[nameWithVer] = msti
}

func (data *Data) CreateMeasurement(database string, rpName string, mst string, shardKey *proto2.ShardKeyInfo, indexR *proto2.IndexRelation) error {
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
	}

	msti := rp.Measurement(mst)
	if msti == nil || msti.MarkDeleted {
		version, ok := rp.MstVersions[mst]
		if ok {
			version = (version + 1) & 0xffff
		}
		if len(rp.MstVersions) == 0 {
			rp.MstVersions = make(map[string]uint32)
		}
		rp.MstVersions[mst] = version
		data.createVersionMeasurement(rp, shardKey, indexR, ski, mst, version)
		return nil
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

func (data *Data) CheckDataNodeAlive(nodeId uint64) error {
	nodeIndex, err := data.GetNodeIndex(nodeId)
	if err != nil {
		err = fmt.Errorf("dataNode %d not found\n", nodeId)
		return err
	}

	status := data.DataNodes[nodeIndex].Status
	if status != serf.StatusAlive {
		return errno.NewError(errno.DataNoAlive, nodeId)
	} else {
		return nil
	}
}

func (data *Data) UpdateShardOwnerId(db, rp string, id, ownerId uint64) error {
	rpi, err := data.RetentionPolicy(db, rp)
	if err != nil {
		return err
	}
	for sgIdx := range rpi.ShardGroups {
		for shIdx := range rpi.ShardGroups[sgIdx].Shards {
			sh := &rpi.ShardGroups[sgIdx].Shards[shIdx]
			if sh.ID != id {
				continue
			}
			if len(sh.Owners) == 1 {
				sh.Owners[0] = uint32(ownerId)
				return nil
			}
		}
	}

	return fmt.Errorf("shard(%d) not found", id)
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

func (data *Data) DataNodeByHttpHost(httpAddr string) *DataNode {
	for i := range data.DataNodes {
		if data.DataNodes[i].Host == httpAddr {
			return &data.DataNodes[i]
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
func (data *Data) CreateDataNode(host, tcpHost string) (error, uint64) {
	data.MaxConnID++
	// Ensure a node with the same host doesn't already exist.
	for i := range data.DataNodes {
		if data.DataNodes[i].TCPHost == tcpHost {
			data.DataNodes[i].ConnID = data.MaxConnID
			return nil, data.DataNodes[i].ID
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
			TCPHost: tcpHost},
		ConnID: data.MaxConnID}
	if len(data.MetaNodes) == 1 {
		dn.Status = serf.StatusAlive
		dn.LTime = 1
		dn.AliveConnID = dn.ConnID
	}
	// Append new node.
	data.DataNodes = append(data.DataNodes, dn)

	sort.Sort(DataNodeInfos(data.DataNodes))
	data.initDataNodePtView()
	for db := range data.PtView {
		data.expendDBPtView(db, data.ClusterPtNum)
	}
	if data.ExpandShardsEnable {
		data.ExpandGroups()
	}
	return nil, existingID
}

func (data *Data) updatePtStatus(db string, ptId uint32, nodeId uint64, status PtStatus) {
	dbPtView, ok := data.PtView[db]
	if !ok {
		dbPtView = make([]PtInfo, data.ClusterPtNum)
	}

	if ptId >= uint32(len(dbPtView)) {
		newPtView := make([]PtInfo, data.ClusterPtNum)
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
	data.PtView[db] = dbPtView
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

func (data *Data) ShardsOfDBPT(name string, ptID uint32) map[string][]uint64 {
	rpShards := make(map[string][]uint64)

	for i := range data.Databases {
		if data.Databases[i].Name == name {
			for _, rp := range data.Databases[i].RetentionPolicies {
				if rp.MarkDeleted {
					continue
				}
				var shardIds []uint64
				for _, sg := range rp.ShardGroups {
					if sg.Deleted() {
						continue
					}
					for _, sh := range sg.Shards {
						if sh.Owners[0] == ptID {
							shardIds = append(shardIds, sh.ID)
						}
					}
				}
				if len(shardIds) != 0 {
					rpShards[rp.Name] = shardIds
				}
			}
		}
	}
	return rpShards
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
	for i := range data.DataNodes {
		nis[i] = data.DataNodes[i]
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

// assign db to all data nodes that have been joined.
func (data *Data) createDBPtView(name string) error {
	if data.PtView == nil {
		data.PtView = make(map[string]DBPtInfos)
	}

	for ptId := 0; ptId < int(data.ClusterPtNum); ptId++ {
		pos := ptId % len(data.DataNodes)
		if err := data.CheckDataNodeAlive(data.DataNodes[pos].ID); err != nil {
			data.updatePtStatus(name, uint32(ptId), data.DataNodes[pos].ID, Offline)
			continue
		}
		data.updatePtStatus(name, uint32(ptId), data.DataNodes[pos].ID, Online)
	}

	return nil
}

func (data *Data) CreateDBPtView(name string) error {
	if data.PtView == nil {
		data.PtView = make(map[string]DBPtInfos)
	}

	// pt view is already exist
	if data.PtView[name] != nil {
		return nil
	}
	var aliveDataNodeIds []uint64
	for i := range data.DataNodes {
		if data.DataNodes[i].Status == serf.StatusAlive {
			aliveDataNodeIds = append(aliveDataNodeIds, data.DataNodes[i].ID)
		}
	}

	if len(aliveDataNodeIds) == 0 {
		return errno.NewError(errno.DataNoAlive)
	}
	for ptId := 0; ptId < int(data.ClusterPtNum); ptId++ {
		pos := ptId % len(aliveDataNodeIds)
		data.updatePtStatus(name, uint32(ptId), aliveDataNodeIds[pos], Offline)
	}
	return nil
}

// CloneDatabases returns a copy of the DatabaseInfo.
func (data *Data) CloneDBPtView() map[string]DBPtInfos {
	if data.PtView == nil {
		return nil
	}
	dbPts := make(map[string]DBPtInfos, len(data.PtView))

	for db, ptView := range data.PtView {
		dbView := make([]PtInfo, len(ptView))
		for i := range ptView {
			dbView[i] = ptView[i]
		}
		dbPts[db] = dbView
	}
	return dbPts
}

func (data *Data) initDataNodePtView() {
	if data.ClusterPtNum < data.PtNumPerNode*uint32(len(data.DataNodes)) {
		data.ClusterPtNum = data.PtNumPerNode * uint32(len(data.DataNodes))
	}
}

func (data *Data) checkStoreReady() error {
	if data.ClusterPtNum == 0 {
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
func (data *Data) CreateDatabase(dbName string, rpi *RetentionPolicyInfo, shardKey *proto2.ShardKeyInfo) error {
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

	err = data.SetDatabase(dbi)
	return err
}

func (data *Data) SetDatabase(dbi *DatabaseInfo) error {
	var err error
	if !config.GetHaEnable() {
		err = data.createDBPtView(dbi.Name)
	}

	if err != nil {
		return err
	}

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
	} else if rpi.ReplicaN < 1 {
		return ErrReplicationFactorTooLow
	}

	if err := rpi.CheckSpecValid(); err != nil {
		return err
	}

	existRp := dbi.RetentionPolicy(rpi.Name)
	if existRp == nil {
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

func (data *Data) ShowShards() models.Rows {
	var rows models.Rows
	data.WalkDatabases(func(db *DatabaseInfo) {
		row := &models.Row{Columns: []string{"id", "database", "retention_policy", "shard_group", "start_time", "end_time", "expiry_time", "owners", "tier", "downSample_level"}, Name: db.Name}
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			rp.walkShardGroups(func(sg *ShardGroupInfo) {
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
			rp.walkShardGroups(func(sg *ShardGroupInfo) {
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

func (data *Data) GetDbPtOwners(database string, ptIds []uint32) []uint64 {
	ownerIDs := make([]uint64, len(ptIds))
	if data.PtView[database] == nil {
		DataLogger.Error("db pt not found in ptView", zap.String("db", database), zap.Uint32s("pt", ptIds))
		return nil
	}

	for i := range ptIds {
		nodeID := data.PtView[database][ptIds[i]].Owner.NodeID
		ownerIDs[i] = nodeID
	}
	return ownerIDs
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

func (data *Data) GetTierOfShardGroup(database, policy string, timestamp time.Time, defaultTier uint64) (*ShardGroupInfo, uint64, error) {
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, 0, err
	}

	sgi := rpi.ShardGroupByTimestamp(timestamp)
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

	if rpi.WarmDuration > 0 {
		if endTime.Add(rpi.WarmDuration).Before(now) {
			tier = util.Cold
		}
	}
	return nil, tier, nil
}

// ShardGroupByTimestamp returns the shard group on a database and policy for a given timestamp.
func (data *Data) ShardGroupByTimestamp(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, ErrRetentionPolicyNotFound(policy)
	}

	return rpi.ShardGroupByTimestamp(timestamp), nil
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (data *Data) CreateShardGroup(database, policy string, timestamp time.Time, tier uint64) error {
	if err := data.checkStoreReady(); err != nil {
		return err
	}

	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	}

	// Verify that shard group doesn't already exist for this timestamp.
	if rpi.ShardGroupByTimestamp(timestamp) != nil {
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

	// Require at least one replica but no more replicas than nodes.
	replicaN := rpi.ReplicaN
	if replicaN == 0 {
		replicaN = 1
	} else if replicaN > len(data.DataNodes) {
		replicaN = len(data.DataNodes)
	}

	// Determine shard count by node count divided by replication factor.
	// This will ensure nodes will get distributed across nodes evenly and
	// replicated the correct number of times.
	shardN := int(data.ClusterPtNum) / replicaN

	// Create the shard group.
	data.MaxShardGroupID++
	sgi := ShardGroupInfo{}
	sgi.ID = data.MaxShardGroupID
	sgi.StartTime = timestamp.Truncate(rpi.ShardGroupDuration).UTC()
	sgi.EndTime = sgi.StartTime.Add(rpi.ShardGroupDuration).UTC()
	if sgi.EndTime.After(time.Unix(0, models.MaxNanoTime)) {
		// Shard group range is [start, end) so add one to the max time.
		sgi.EndTime = time.Unix(0, models.MaxNanoTime+1)
	}

	// Create shards on the group.
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

	//check index group contain this shard group
	igi := data.createIndexGroupIfNeeded(rpi, timestamp)

	for i := range sgi.Shards {
		data.MaxShardID++
		sgi.Shards[i] = ShardInfo{ID: data.MaxShardID, Tier: tier}
		sgi.Shards[i].Owners = make([]uint32, 0, replicaN)
		for ptId := 0; ptId < int(data.ClusterPtNum); ptId++ {
			if ptId%shardN == i {
				for j := 0; j < replicaN; j++ {
					sgi.Shards[i].Owners = append(sgi.Shards[i].Owners, uint32(ptId+j))
					sgi.Shards[i].IndexID = igi.Indexes[ptId].ID
				}
			}
		}
		if lastSgi != nil {
			sgi.Shards[i].Min = lastSgi.Shards[i].Min
			sgi.Shards[i].Max = lastSgi.Shards[i].Max
		}
	}

	// Retention policy has a new shard group, so update the policy. Shard
	// Groups must be stored in sorted order, as other parts of the system
	// assume this to be the case.
	rpi.ShardGroups = append(rpi.ShardGroups, sgi)
	sort.Sort(ShardGroupInfos(rpi.ShardGroups))

	return nil
}

func (data *Data) CreateIndexGroup(rpi *RetentionPolicyInfo, timestamp time.Time) *IndexGroupInfo {
	data.MaxIndexGroupID++
	igi := IndexGroupInfo{}
	igi.ID = data.MaxIndexGroupID
	igi.StartTime = timestamp.Truncate(rpi.IndexGroupDuration).UTC()
	igi.EndTime = igi.StartTime.Add(rpi.IndexGroupDuration).UTC()
	if igi.EndTime.After(time.Unix(0, models.MaxNanoTime)) {
		igi.EndTime = time.Unix(0, models.MaxNanoTime+1)
	}
	igi.Indexes = make([]IndexInfo, data.ClusterPtNum)
	for i := range igi.Indexes {
		data.MaxIndexID++
		igi.Indexes[i] = IndexInfo{ID: data.MaxIndexID, Owners: []uint32{uint32(i)}}
	}
	rpi.IndexGroups = append(rpi.IndexGroups, igi)
	sort.Sort(IndexGroupInfos(rpi.IndexGroups))
	return &igi
}

func (data *Data) createIndexGroupIfNeeded(rpi *RetentionPolicyInfo, timestamp time.Time) *IndexGroupInfo {
	if len(rpi.IndexGroups) == 0 {
		return data.CreateIndexGroup(rpi, timestamp)
	}

	var igIdx int
	for igIdx = 0; igIdx < len(rpi.IndexGroups); igIdx++ {
		if rpi.IndexGroups[igIdx].Contains(timestamp) {
			break
		}
	}
	if igIdx < len(rpi.IndexGroups) && len(rpi.IndexGroups[igIdx].Indexes) >= int(data.ClusterPtNum) {
		return &rpi.IndexGroups[igIdx]
	}
	return data.CreateIndexGroup(rpi, timestamp)
}

func (data *Data) expendDBPtView(database string, ptNum uint32) {
	dbPtInfos := data.DBPtView(database)
	oldDBPtNums := uint32(len(dbPtInfos))
	if ptNum == oldDBPtNums {
		return
	} else if ptNum < oldDBPtNums {
		assert(false, fmt.Sprintf("expend dbPT db:%s from %d to %d", database, oldDBPtNums, ptNum))
	} else {
		DataLogger.Info("expend db ptview", zap.String("db", database), zap.Uint32("from", oldDBPtNums), zap.Uint32("to", ptNum))
	}

	for ptId := oldDBPtNums; ptId < ptNum; ptId++ {
		pos := ptId % uint32(len(data.DataNodes))
		if data.DataNodes[pos].Status == serf.StatusAlive {
			data.updatePtStatus(database, ptId, data.DataNodes[pos].ID, Online)
			continue
		}
		data.updatePtStatus(database, ptId, data.DataNodes[pos].ID, Offline)
	}
}

func (data *Data) ExpandGroups() {
	data.WalkDatabases(func(db *DatabaseInfo) {
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			if rp.shardingType() == RANGE {
				return
			}

			rp.walkIndexGroups(func(ig *IndexGroupInfo) {
				for i := len(ig.Indexes); i < int(data.ClusterPtNum); i++ {
					data.MaxIndexID++
					ig.Indexes = append(ig.Indexes, IndexInfo{
						ID:     data.MaxIndexID,
						Owners: []uint32{uint32(i)},
					})
				}
			})

			rp.walkShardGroups(func(sg *ShardGroupInfo) {
				for i := len(sg.Shards); i < int(data.ClusterPtNum); i++ {
					igi := data.createIndexGroupIfNeeded(rp, sg.StartTime)
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
func (data *Data) DeleteShardGroup(database, policy string, id uint64) error {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	}
	// Find shard group by ID and set its deletion timestamp.
	for i := range rpi.ShardGroups {
		if rpi.ShardGroups[i].ID == id {
			rpi.ShardGroups[i].DeletedAt = time.Now().UTC()
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
			for idx := 0; idx < len(rp.ShardGroups); {
				if id >= rp.ShardGroups[idx].Shards[0].ID && id <= rp.ShardGroups[idx].Shards[len(rp.ShardGroups[idx].Shards)-1].ID {
					pos := sort.Search(len(rp.ShardGroups[idx].Shards), func(i int) bool {
						return rp.ShardGroups[idx].Shards[i].ID >= id
					})
					rp.ShardGroups[idx].Shards[pos].MarkDelete = true
				}

				if !rp.ShardGroups[idx].DeletedAt.IsZero() && rp.ShardGroups[idx].canDelete() {
					rp.ShardGroups = append(rp.ShardGroups[:idx], rp.ShardGroups[idx+1:]...)
				} else {
					idx++
				}
			}
		})
	})
	return nil
}

// validateURL returns an error if the URL does not have a port or uses a scheme other than UDP or HTTP.
func validateURL(input string) error {
	u, err := url.Parse(input)
	if err != nil {
		return ErrInvalidSubscriptionURL(input)
	}

	if u.Scheme != "udp" && u.Scheme != "http" && u.Scheme != "https" {
		return ErrInvalidSubscriptionURL(input)
	}

	_, port, err := net.SplitHostPort(u.Host)
	if err != nil || port == "" {
		return ErrInvalidSubscriptionURL(input)
	}

	return nil
}

// CreateSubscription adds a named subscription to a database and retention policy.
func (data *Data) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	for _, d := range destinations {
		if err := validateURL(d); err != nil {
			return err
		}
	}

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

	return nil
}

// DropSubscription removes a subscription.
func (data *Data) DropSubscription(database, rp, name string) error {
	rpi, err := data.RetentionPolicy(database, rp)
	if err != nil {
		return err
	}

	for i := range rpi.Subscriptions {
		if rpi.Subscriptions[i].Name == name {
			rpi.Subscriptions = append(rpi.Subscriptions[:i], rpi.Subscriptions[i+1:]...)
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
func (data Data) AdminUserExist() bool {
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
	return &other
}

// Marshal serializes data to a protobuf representation.
func (data *Data) Marshal() *proto2.Data {
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
		MaxEventOpId:    proto.Uint64(data.MaxEventOpId),
		TakeOverEnabled: proto.Bool(data.TakeOverEnabled),
		BalancerEnabled: proto.Bool(data.BalancerEnabled),

		MaxDownSampleID: proto.Uint64(data.MaxDownSampleID),
		MaxStreamID:     proto.Uint64(data.MaxStreamID),
		MaxConnId:       proto.Uint64(data.MaxConnID),
	}

	pb.DataNodes = make([]*proto2.DataNode, len(data.DataNodes))
	for i := range data.DataNodes {
		pb.DataNodes[i] = data.DataNodes[i].marshal()
	}

	pb.MetaNodes = make([]*proto2.NodeInfo, len(data.MetaNodes))
	for i := range data.MetaNodes {
		pb.MetaNodes[i] = data.MetaNodes[i].marshal()
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

	pb.MigrateEvents = make([]*proto2.MigrateEventInfo, len(data.MigrateEvents))
	i = 0
	for eventStr := range data.MigrateEvents {
		pb.MigrateEvents[i] = data.MigrateEvents[eventStr].marshal()
		i++
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (data *Data) Unmarshal(pb *proto2.Data) {
	data.Term = pb.GetTerm()
	data.Index = pb.GetIndex()
	data.ClusterID = pb.GetClusterID()
	data.ClusterPtNum = pb.GetClusterPtNum()

	data.MaxNodeID = pb.GetMaxNodeID()
	data.MaxShardGroupID = pb.GetMaxShardGroupID()
	data.MaxShardID = pb.GetMaxShardID()
	data.PtNumPerNode = pb.GetPtNumPerNode()
	data.MaxIndexGroupID = pb.GetMaxIndexGroupID()
	data.MaxIndexID = pb.GetMaxIndexID()
	data.DataNodes = make([]DataNode, len(pb.GetDataNodes()))
	data.MaxEventOpId = pb.GetMaxEventOpId()
	data.TakeOverEnabled = pb.GetTakeOverEnabled()
	data.BalancerEnabled = pb.GetBalancerEnabled()
	data.MaxDownSampleID = pb.GetMaxDownSampleID()
	data.MaxStreamID = pb.GetMaxStreamID()
	data.MaxConnID = pb.GetMaxConnId()

	for i, x := range pb.GetDataNodes() {
		data.DataNodes[i].unmarshal(x)
	}

	data.MetaNodes = make([]NodeInfo, len(pb.GetMetaNodes()))
	for i, x := range pb.GetMetaNodes() {
		data.MetaNodes[i].unmarshal(x)
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

	if streams := pb.GetStreams(); len(streams) > 0 {
		data.Streams = make(map[string]*StreamInfo, len(streams))
		for _, s := range streams {
			si := &StreamInfo{}
			si.Unmarshal(s)
			data.Streams[si.Name] = si
		}
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

// MarshalToStore serializes data to a protobuf representation. Just for ts-store use
func (data *Data) MarshalToStore() *proto2.Data {
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
		MaxEventOpId:    proto.Uint64(data.MaxEventOpId),
		TakeOverEnabled: proto.Bool(data.TakeOverEnabled),
	}

	pb.Databases = make([]*proto2.DatabaseInfo, len(data.Databases))
	i := 0
	for dbName := range data.Databases {
		pb.Databases[i] = data.Databases[dbName].marshal()
		i++
	}

	pb.Users = make([]*proto2.UserInfo, len(data.Users))
	for i := range data.Users {
		pb.Users[i] = data.Users[i].marshal()
	}
	return pb
}

// MarshalBinaryToStore encodes the metadata to a binary format. Just for ts-store use
func (data *Data) MarshalBinaryToStore() ([]byte, error) {
	return proto.Marshal(data.MarshalToStore())
}

// TruncateShardGroups truncates any shard group that could contain timestamps beyond t.
func (data *Data) TruncateShardGroups(t time.Time) {
	data.WalkDatabases(func(db *DatabaseInfo) {
		db.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
			for k := range rp.ShardGroups {
				sgi := &rp.ShardGroups[k]

				if !t.Before(sgi.EndTime) || sgi.Deleted() || (sgi.Truncated() && sgi.TruncatedAt.Before(t)) {
					continue
				}

				if !t.After(sgi.StartTime) {
					// future shardgroup
					sgi.TruncatedAt = sgi.StartTime
				} else {
					sgi.TruncatedAt = t
				}
			}
		})
	})
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
	err := data.CreateDatabase(restoreDBName, nil, nil)
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

func (data *Data) UpdateNodeStatus(id uint64, status int32, lTime uint64, gossipAddr string) error {
	dn := data.DataNode(id)
	if dn == nil {
		return errno.NewError(errno.DataNodeNotFound, id)
	}

	if lTime < dn.LTime {
		DataLogger.Error("event is older", zap.Uint64("id", id), zap.Int32("status", status), zap.Uint64("ltime", lTime), zap.Uint64("dnLtime", dn.LTime))
		return errno.NewError(errno.OlderEvent)
	}

	updateStatus := serf.MemberStatus(status)
	// node cannot join into cluster after split-brain
	if config.GetHaEnable() && updateStatus == serf.StatusAlive && lTime == dn.LTime {
		return errno.NewError(errno.DataNodeSplitBrain)
	}
	dn.Status = updateStatus
	dn.LTime = lTime
	if updateStatus == serf.StatusAlive {
		dn.AliveConnID = dn.ConnID
	}
	if dn.GossipAddr == "" {
		host, _, _ := net.SplitHostPort(dn.Host)
		dn.GossipAddr = fmt.Sprintf("%s:%s", host, gossipAddr)
	}

	if config.GetHaEnable() || updateStatus == serf.StatusFailed {
		data.updatePtViewStatus(id, Offline)
		return nil
	}

	data.updatePtViewStatus(id, Online)
	return nil
}

// return pts for the nid
func (data *Data) updatePtViewStatus(nid uint64, status PtStatus) {
	for db := range data.PtView {
		for i := range data.PtView[db] {
			if data.PtView[db][i].Owner.NodeID == nid {
				data.PtView[db][i].Status = status
				data.PtView[db][i].Ver += 1
			}
		}
	}
}

type DbPtInfo struct {
	Db     string
	Pti    *PtInfo
	Shards map[uint64]*ShardDurationInfo
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
}

func (data *Data) GetShardDurationsByDbPt(db string, pt uint32) map[uint64]*ShardDurationInfo {
	dbi := data.Database(db)
	r := make(map[uint64]*ShardDurationInfo, 7)
	dbi.WalkRetentionPolicy(func(rp *RetentionPolicyInfo) {
		if rp.MarkDeleted {
			return
		}
		rp.walkShardGroups(func(sg *ShardGroupInfo) {
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
	resPtInfos := make([]*DbPtInfo, 0, data.ClusterPtNum)
	for db := range data.PtView {
		// do not get pt which db mark deleted
		if data.Databases[db] == nil || data.Database(db).MarkDeleted {
			continue
		}
		for i := range data.PtView[db] {
			if data.PtView[db][i].Owner.NodeID == id && data.PtView[db][i].Status == status {
				shards := data.GetShardDurationsByDbPt(db, data.PtView[db][i].PtId)
				pt := data.PtView[db][i]
				resPtInfos = append(resPtInfos, &DbPtInfo{Db: db, Pti: &pt, Shards: shards})
			}
		}
	}
	return resPtInfos
}

func (data *Data) GetPtInfosByDbname(name string) ([]*DbPtInfo, error) {
	resPtInfos := make([]*DbPtInfo, len(data.PtView[name]))
	if data.Database(name) != nil && data.Database(name).MarkDeleted {
		return nil, errno.NewError(errno.DatabaseIsBeingDelete)
	}
	idx := 0
	for i := range data.PtView[name] {
		if data.PtView[name][i].Status == Offline {
			pt := data.PtView[name][i]
			resPtInfos[idx] = &DbPtInfo{Db: name, Pti: &pt}
			idx++
		}
	}
	return resPtInfos[:idx], nil
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
			shardGroups[i].Shard(ident.ShardID).DownSampleLevel = int64(ident.DownSampleLevel)
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
	row := &models.Row{Columns: []string{"database", "retention", "measurement", "Name", "source measurement", "dimensions", "calls", "interval", "delay"}}
	for _, v := range data.Streams {
		if showAll || v.DesMst.Database == database {
			row.Values = append(row.Values, []interface{}{v.DesMst.Database, v.DesMst.RetentionPolicy, v.DesMst.Name, v.Name, v.SrcMst.Name + "." + v.SrcMst.RetentionPolicy + "." + v.SrcMst.Name,
				v.Dimensions(), v.CallsName(), v.Interval.String(), v.Delay.String()})
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

// ValidName checks to see if the given name can would be valid for DB/RP name
func ValidName(name string) bool {
	return validName(name, `,:;./\`)
}

func ValidMeasurementName(name string) bool {
	if name == "." || name == ".." {
		return false
	}

	return validName(name, `,:;/\`)
}

func validName(name string, charactersNotSupport string) bool {
	for _, r := range name {
		if !unicode.IsPrint(r) {
			return false
		}
	}

	return name != "" && !strings.ContainsAny(name, charactersNotSupport)
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
