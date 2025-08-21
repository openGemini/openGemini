// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package metaclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/sysinfo"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	tls            bool
	logger         *logger.Logger
	nodeID         uint64
	Clock          uint64
	ShardDurations map[uint64]*meta2.ShardDurationInfo
	DBBriefInfos   map[string]*meta2.DatabaseBriefInfo

	mu          sync.RWMutex
	metaServers []string
	closing     chan struct{}
	changed     chan chan struct{}
	cacheData   *meta2.Data

	// Authentication cache.
	auth        *Auth
	weakPwdPath string
	ShardTier   uint64

	replicaInfoManager *ReplicaInfoManager

	UseSnapshotV2       bool
	RetentionAutoCreate bool

	// send RPC message interface.
	SendRPCMessage
}

func (c *Client) SetExpandShardsEnable(en bool) {
	c.cacheData.ExpandShardsEnable = en
}

var applyFunc = map[proto2.Command_Type]func(c *Client, op *proto2.Command) error{
	proto2.Command_CreateDatabaseCommand:            applyCreateDatabase,
	proto2.Command_DropDatabaseCommand:              applyDropDatabase,
	proto2.Command_CreateRetentionPolicyCommand:     applyCreateRetentionPolicy,
	proto2.Command_DropRetentionPolicyCommand:       applyDropRetentionPolicy,
	proto2.Command_SetDefaultRetentionPolicyCommand: applySetDefaultRetentionPolicy,
	proto2.Command_UpdateRetentionPolicyCommand:     applyUpdateRetentionPolicy,
	proto2.Command_CreateShardGroupCommand:          applyCreateShardGroup,
	proto2.Command_DeleteShardGroupCommand:          applyDeleteShardGroup,
	proto2.Command_CreateSubscriptionCommand:        applyCreateSubscription,
	proto2.Command_DropSubscriptionCommand:          applyDropSubscription,
	proto2.Command_CreateUserCommand:                applyCreateUser,
	proto2.Command_DropUserCommand:                  applyDropUser,
	proto2.Command_UpdateUserCommand:                applyUpdateUser,
	proto2.Command_SetPrivilegeCommand:              applySetPrivilege,
	proto2.Command_SetAdminPrivilegeCommand:         applySetAdminPrivilege,
	proto2.Command_SetDataCommand:                   applySetData,
	proto2.Command_CreateMetaNodeCommand:            applyCreateMetaNode,
	proto2.Command_DeleteMetaNodeCommand:            applyDeleteMetaNode,
	proto2.Command_SetMetaNodeCommand:               applySetMetaNode,
	proto2.Command_CreateDataNodeCommand:            applyCreateDataNode,
	proto2.Command_CreateSqlNodeCommand:             applyCreateSqlNode,
	proto2.Command_DeleteDataNodeCommand:            applyDeleteDataNode,
	proto2.Command_MarkDatabaseDeleteCommand:        applyMarkDatabaseDelete,
	proto2.Command_MarkRetentionPolicyDeleteCommand: applyMarkRetentionPolicyDelete,
	proto2.Command_CreateMeasurementCommand:         applyCreateMeasurement,
	proto2.Command_ReShardingCommand:                applyReSharding,
	proto2.Command_UpdateSchemaCommand:              applyUpdateSchema,
	proto2.Command_AlterShardKeyCmd:                 applyAlterShardKey,
	proto2.Command_PruneGroupsCommand:               applyPruneGroups,
	proto2.Command_MarkMeasurementDeleteCommand:     applyMarkMeasurementDelete,
	proto2.Command_DropMeasurementCommand:           applyDropMeasurement,
	proto2.Command_DeleteIndexGroupCommand:          applyDeleteIndexGroup,
	proto2.Command_UpdateShardInfoTierCommand:       applyUpdateShardInfoTier,
	proto2.Command_UpdateIndexInfoTierCommand:       applyUpdateIndexInfoTier,
	proto2.Command_UpdateNodeStatusCommand:          applyUpdateNodeStatus,
	proto2.Command_UpdateSqlNodeStatusCommand:       applyUpdateSqlNodeStatus,
	proto2.Command_UpdatePtInfoCommand:              applyUpdatePtInfo,
	proto2.Command_CreateDownSamplePolicyCommand:    applyCreateDownSample,
	proto2.Command_DropDownSamplePolicyCommand:      applyDropDownSample,
	proto2.Command_CreateDbPtViewCommand:            applyCreateDbPtView,
	proto2.Command_UpdateShardDownSampleInfoCommand: applyUpdateShardDownSampleInfo,
	proto2.Command_CreateStreamCommand:              applyCreateStream,
	proto2.Command_DropStreamCommand:                applyDropStream,
	proto2.Command_ExpandGroupsCommand:              applyExpandGroups,
	proto2.Command_UpdatePtVersionCommand:           applyUpdatePtVersion,
	proto2.Command_RegisterQueryIDOffsetCommand:     applyRegisterQueryIDOffset,
	proto2.Command_CreateContinuousQueryCommand:     applyCreateContinuousQuery,
	proto2.Command_ContinuousQueryReportCommand:     applyContinuousQueryReport,
	proto2.Command_DropContinuousQueryCommand:       applyDropContinuousQuery,
	proto2.Command_SetNodeSegregateStatusCommand:    applySetNodeSegregateStatus,
	proto2.Command_RemoveNodeCommand:                applyRemoveNode,
	proto2.Command_UpdateReplicationCommand:         applyUpdateReplication,
	proto2.Command_UpdateMeasurementCommand:         applyUpdateMeasurement,
	proto2.Command_UpdateMetaNodeStatusCommand:      applyUpdateMetaNodeStatus,
	proto2.Command_ReplaceMergeShardsCommand:        applyReplaceMergeShardsCommand,
	proto2.Command_RecoverMetaData:                  applyRecoverMetaData,
}

// NewClient returns a new *Client.
func NewClient(weakPwdPath string, retentionAutoCreate bool, maxConcurrentWriteLimit int) *Client {
	cli := &Client{
		cacheData:          &meta2.Data{},
		closing:            make(chan struct{}),
		changed:            make(chan chan struct{}, maxConcurrentWriteLimit),
		weakPwdPath:        weakPwdPath,
		logger:             logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		replicaInfoManager: NewReplicaInfoManager(),
		SendRPCMessage:     &RPCMessageSender{},
	}
	cli.auth = NewAuth(cli.logger)
	cliOnce.Do(func() {
		DefaultMetaClient = cli
	})

	return cli
}

func (c *Client) EnableUseSnapshotV2(RetentionAutoCreate bool, ExpandShardsEnable bool) {
	c.UseSnapshotV2 = true
	c.RetentionAutoCreate = RetentionAutoCreate
	c.SetExpandShardsEnable(ExpandShardsEnable)
}

func cvtDataForAlgoVer(ver string) int {
	var rst int
	switch ver {
	case "ver01":
		rst = algoVer01
	case "ver02":
		rst = algoVer02
	case "ver03":
		rst = algoVer03
	default:
		rst = algoVer02
	}
	return rst
}

func (c *Client) SetHashAlgo(optHashAlgo string) {
	c.auth.optAlgoVer = cvtDataForAlgoVer(optHashAlgo)
}

// Open a connection to a meta service cluster.
func (c *Client) Open() error {
	if c.auth == nil {
		c.auth = NewAuth(c.logger)
	}
	if c.UseSnapshotV2 {
		meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
		err := c.retryUntilSnapshotV2(SQL, 0)
		if err != nil {
			// this will only be nil if the client has been closed,
			// so we can exit out
			c.logger.Error("client has been closed:", zap.String("err:", err.Error()))
			return err
		}
		go c.pollForUpdatesV2(SQL)
	} else {
		c.cacheData = c.retryUntilSnapshot(SQL, 0)
		go c.pollForUpdates(SQL)
	}

	return nil
}

func (c *Client) OpenAtStore() error {
	if c.UseSnapshotV2 {
		meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
		err := c.retryUntilSnapshotV2(STORE, 0)
		if err != nil {
			// this will only be nil if the client has been closed,
			// so we can exit out
			c.logger.Error("client has been closed:", zap.String("err:", err.Error()))
			return err
		}
		go c.pollForUpdatesV2(STORE)
	} else {
		c.cacheData = c.retryUntilSnapshot(STORE, 0)
		go c.pollForUpdates(STORE)
	}
	go c.verifyDataNodeStatus(time.Second * 10)
	return nil
}

// Close the meta service cluster connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}

	select {
	case <-c.closing:
		return nil
	default:
		close(c.closing)
	}

	return nil
}

// NodeID GetNodeID returns the client's node ID.
func (c *Client) NodeID() uint64 { return c.nodeID }

func (c *Client) SetNode(newNodeID uint64, newClock uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodeID = newNodeID
	c.Clock = newClock
}

func (c *Client) SetTier(tier string) error {
	c.ShardTier = meta2.StringToTier(strings.ToUpper(tier))
	if c.ShardTier == util.TierBegin {
		return fmt.Errorf("invalid tier %s", tier)
	}
	return nil
}

// SetMetaServers updates the meta servers on the client.
func (c *Client) SetMetaServers(a []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metaServers = a

	for i, server := range a {
		transport.NewMetaNodeManager().Add(uint64(i), server)
	}
}

// SetTLS sets whether the client should use TLS when connecting.
// This function is not safe for concurrent use.
func (c *Client) SetTLS(v bool) { c.tls = v }

// Ping will hit the ping endpoint for the metaservice and return nil if
// it returns 200. If checkAllMetaServers is set to true, it will hit the
// ping endpoint and tell it to verify the health of all metaservers in the
// cluster
func (c *Client) Ping(checkAllMetaServers bool) error {
	all := 0
	if checkAllMetaServers {
		all = 1
	}
	callback := &PingCallback{}
	msg := message.NewMetaMessage(message.PingRequestMessage, &message.PingRequest{All: all})
	err := c.SendRPCMsg(0, msg, callback)
	if err != nil {
		return err
	}
	return errors.New(string(callback.Leader))
}

// ClusterID returns the ID of the cluster it's connected to.
func (c *Client) ClusterID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.ClusterID
}

// DataNode returns a node by id.
func (c *Client) DataNode(id uint64) (*meta2.DataNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for i := range c.cacheData.DataNodes {
		if c.cacheData.DataNodes[i].ID == id {
			return &c.cacheData.DataNodes[i], nil
		}
	}
	return nil, meta2.ErrNodeNotFound
}

func (c *Client) AliveReadNodes() ([]meta2.DataNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var aliveReaders []meta2.DataNode
	var aliveDefault []meta2.DataNode
	var aliveWriters []meta2.DataNode
	for _, n := range c.cacheData.DataNodes {
		if n.Status != serf.StatusAlive {
			continue
		}
		if n.Role == meta2.NodeReader {
			aliveReaders = append(aliveReaders, n)
		} else if n.Role == meta2.NodeDefault {
			aliveDefault = append(aliveDefault, n)
		} else if n.Role == meta2.NodeWriter {
			aliveWriters = append(aliveWriters, n)
		}
	}

	if len(aliveReaders) != 0 {
		sort.Sort(meta2.DataNodeInfos(aliveReaders))
		return aliveReaders, nil
	}
	if len(aliveDefault) != 0 {
		sort.Sort(meta2.DataNodeInfos(aliveDefault))
		return aliveDefault, nil
	}
	if len(aliveWriters) != 0 {
		sort.Sort(meta2.DataNodeInfos(aliveWriters))
		return aliveWriters, nil
	}
	return nil, fmt.Errorf("there is no data nodes for querying")
}

// DataNodes returns the data nodes' info.
func (c *Client) DataNodes() ([]meta2.DataNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.DataNodes, nil
}

func (c *Client) GetAllMst(dbName string) []string {
	var mstName []string
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.cacheData.Databases[dbName]; !ok {
		return nil
	}

	for _, rp := range c.cacheData.Databases[dbName].RetentionPolicies {
		for mst := range rp.Measurements {
			mstName = append(mstName, mst)
		}
	}
	return mstName
}

// CreateDataNode will create a new data node in the metastore
func (c *Client) CreateDataNode(storageNodeInfo *StorageNodeInfo, role string) (uint64, uint64, uint64, error) {
	writeHost, queryHost, az, retryTime, retryNumber := storageNodeInfo.InsertAddr, storageNodeInfo.SelectAddr, storageNodeInfo.Az, storageNodeInfo.RetryTime, storageNodeInfo.RetryNumber
	var retryCount int
	currentServer := connectedServer
	for {
		// exit if we're closed
		select {
		case <-c.closing:
			return 0, 0, 0, meta2.ErrClientClosed
		default:
			// we're still open, continue on
		}
		if retryCount >= retryNumber {
			return 0, 0, 0, errors.New("data: retry number exceeds the limit")
		}
		c.mu.RLock()
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()

		node, err := c.getNode(currentServer, writeHost, queryHost, role, az)

		if err == nil && node.NodeId > 0 {
			c.nodeID = node.NodeId
			return c.nodeID, node.LTime, node.ConnId, nil
		}

		c.logger.Warn("get node failed", zap.Error(err), zap.String("writeHost", writeHost), zap.String("queryHost", queryHost))
		time.Sleep(retryTime)

		currentServer++
		retryCount++
	}
}

func (c *Client) CreateSqlNode(sqlNodeInfo *SqlNodeInfo) (uint64, uint64, uint64, error) {
	httpHost, gossipHost, retryTime, retryNumber := sqlNodeInfo.HttpAddr, sqlNodeInfo.GossipAddr, sqlNodeInfo.RetryTime, sqlNodeInfo.RetryNumber
	var retryCount int
	currentServer := connectedServer
	for {
		// exit if we're closed
		select {
		case <-c.closing:
			return 0, 0, 0, meta2.ErrClientClosed
		default:
			// we're still open, continue on
		}
		if retryCount >= retryNumber {
			return 0, 0, 0, errors.New("sql: retry number exceeds the limit")
		}
		c.mu.RLock()
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()

		node, err := c.getSqlNode(httpHost, gossipHost, currentServer)

		if err == nil && node.NodeId > 0 {
			c.nodeID = node.NodeId
			return c.nodeID, node.LTime, node.ConnId, nil
		}

		c.logger.Warn("get sql node failed", zap.Error(err))
		time.Sleep(retryTime)

		currentServer++
		retryCount++
	}
}

// DataNodeByHTTPHost returns the data node with the give http bind address
func (c *Client) DataNodeByHTTPHost(httpAddr string) (*meta2.DataNode, error) {
	nodes, err := c.DataNodes()
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n.Host == httpAddr {
			return &n, nil
		}
	}

	return nil, meta2.ErrNodeNotFound
}

// DataNodeByTCPHost returns the data node with the give http bind address
func (c *Client) DataNodeByTCPHost(tcpAddr string) (*meta2.DataNode, error) {
	nodes, err := c.DataNodes()
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n.TCPHost == tcpAddr {
			return &n, nil
		}
	}

	return nil, meta2.ErrNodeNotFound
}

// DeleteDataNode deletes a data node from the cluster.
func (c *Client) DeleteDataNode(id uint64) error {
	cmd := &proto2.DeleteDataNodeCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(proto2.Command_DeleteDataNodeCommand, proto2.E_DeleteDataNodeCommand_Command, cmd)
}

// MetaNodes returns the meta nodes' info.
func (c *Client) MetaNodes() ([]meta2.NodeInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.MetaNodes, nil
}

// SqlNodes returns the sql nodes' info.
func (c *Client) SqlNodes() ([]meta2.DataNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.SqlNodes, nil
}

// MetaNodeByAddr returns the meta node's info.
func (c *Client) MetaNodeByAddr(addr string) *meta2.NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, n := range c.cacheData.MetaNodes {
		if n.Host == addr {
			return &n
		}
	}
	return nil
}

func (c *Client) FieldKeys(database string, ms influxql.Measurements) (map[string]map[string]int32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	mis, err := c.matchMeasurements(database, ms)
	if err != nil {
		return nil, err
	}
	if len(mis) == 0 {
		return nil, nil
	}

	ret := make(map[string]map[string]int32, len(mis))
	for _, m := range mis {
		ret[m.OriginName()] = make(map[string]int32)
		m.FieldKeys(ret)
	}
	return ret, nil
}

func (c *Client) TagKeys(database string) map[string]set.Set[string] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbi := c.cacheData.Database(database)
	uniqueMap := make(map[string]set.Set[string])

	dbi.WalkRetentionPolicy(func(rp *meta2.RetentionPolicyInfo) {
		rp.EachMeasurements(func(mst *meta2.MeasurementInfo) {
			s := set.NewSet[string]()
			callback := func(k string, v int32) {
				if v == influx.Field_Type_Tag {
					s.Add(k)
				}
			}
			mst.Schema.RangeTypCall(callback)
			_, ok := uniqueMap[mst.Name]
			if ok {
				uniqueMap[mst.Name] = uniqueMap[mst.Name].Union(s)
				return
			}
			uniqueMap[mst.Name] = s
		})
	})

	return uniqueMap
}

func (c *Client) GetMeasurements(m *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
	var measurements []*meta2.MeasurementInfo
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbi, err := c.cacheData.GetDatabase(m.Database)
	if err != nil {
		return nil, err
	}

	rpi, err := dbi.GetRetentionPolicy(m.RetentionPolicy)
	if err != nil {
		return nil, err
	}

	if m.Regex != nil {
		rpi.EachMeasurements(func(msti *meta2.MeasurementInfo) {
			if m.Regex.Val.Match([]byte(influx.GetOriginMstName(msti.Name))) {
				measurements = append(measurements, msti)
			}
		})
		sort.Slice(measurements, func(i, j int) bool {
			return influx.GetOriginMstName(measurements[i].Name) < influx.GetOriginMstName(measurements[j].Name)
		})
	} else if m.Name == "" {
		for _, msti := range rpi.Measurements {
			measurements = append(measurements, msti)
		}
	} else {
		if m.MstType != influxql.TEMPORARY {
			msti, err := rpi.GetMeasurement(m.Name)
			if err != nil {
				return nil, err
			}
			measurements = append(measurements, msti)
		}
	}
	return measurements, nil
}

func (c *Client) RetryMeasurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	info, err := c.Measurement(database, rpName, mstName)
	if err == nil {
		return info, nil
	}
	return c.GetMeasurementInfoStore(database, rpName, mstName)
}

func (c *Client) Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.Measurement(database, rpName, mstName)
}

func (c *Client) GetMeasurementID(database string, rpName string, mstName string) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.GetMeasurementID(database, rpName, mstName)
}

func (c *Client) RetryGetMeasurementInfoStore(database string, rpName string, mstName string) ([]byte, error) {
	startTime := time.Now()
	currentServer := connectedServer
	var err error
	var info []byte
	for {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil, errors.New("GetMeasurementInfoStore fail")
		default:

		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()
		info, err = c.getMeasurementInfo(currentServer, database, rpName, mstName)
		if err == nil {
			break
		}

		if time.Since(startTime).Seconds() > float64(len(c.metaServers))*HttpReqTimeout.Seconds() {
			break
		}
		time.Sleep(errSleep)

		currentServer++
	}
	return info, nil
}

func (c *Client) getMeasurementInfo(currentServer int, database string, rpName string, mstName string) ([]byte, error) {
	callback := &GetMeasurementInfoCallback{}
	msg := message.NewMetaMessage(message.GetMeasurementInfoRequestMessage, &message.GetMeasurementInfoRequest{
		DbName: database, RpName: rpName, MstName: mstName})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		if !strings.Contains(err.Error(), "node is not the leader") {
			c.logger.Error("GetMeasurementInfoR SendRPCMsg fail", zap.Error(err))
		}
	}
	return callback.Data, nil
}

func (c *Client) GetMeasurementInfoStore(dbName string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	b, err := c.RetryGetMeasurementInfoStore(dbName, rpName, mstName)
	if err != nil {
		return nil, err
	}
	mst := &meta2.MeasurementInfo{}
	if err = mst.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return mst, nil
}

// Database returns info for the requested database.
func (c *Client) Database(name string) (*meta2.DatabaseInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.GetDatabase(name)
}

// returns obs options info for the requested database.
func (c *Client) DatabaseOption(name string) (*obs.ObsOptions, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbi := c.cacheData.Database(name)
	if dbi == nil {
		return nil, errno.NewError(errno.DatabaseNotFound, name)
	}
	return dbi.Options, nil
}

// Databases returns a list of all database infos.
func (c *Client) Databases() map[string]*meta2.DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.Databases
}

type RepConfWriteType uint32

const (
	NOREPDB RepConfWriteType = iota
	RAFTFORREPDB
	UNKOWN
)

func (c *Client) RaftEnabledForDB(name string) (RepConfWriteType, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dbInfo, err := c.cacheData.GetDatabase(name)
	if err != nil {
		return UNKOWN, errno.NewError(errno.DatabaseNotFound)
	}
	if dbInfo.ReplicaN <= 1 {
		return NOREPDB, nil
	}
	// ReplicaN: 3, 5, 7...
	if dbInfo.ReplicaN > 1 && dbInfo.ReplicaN%2 != 0 {
		return RAFTFORREPDB, nil
	}
	return UNKOWN, nil
}

func (c *Client) ShowShards(db string, rp string, mst string) models.Rows {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if db == "" && rp == "" && mst == "" {
		return c.cacheData.ShowShards()
	} else {
		return c.cacheData.ShowShardsFromMst(db, rp, mst)
	}
}

func (c *Client) ShowShardGroups() models.Rows {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.ShowShardGroups()
}

func (c *Client) ShowSubscriptions() models.Rows {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.ShowSubscriptions()
}

func (c *Client) ShowRetentionPolicies(database string) (models.Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.ShowRetentionPolicies(database)
}

func (c *Client) buildClusterRows(clusterInfo *meta2.ShowClusterInfo) models.Rows {
	srcNodeMap := make(map[uint64]interface{})
	eventRow := &models.Row{Columns: []string{"opId", "eventType", "db", "ptId", "srcNodeId", "dstNodeId", "currState", "preState"}}
	for _, event := range clusterInfo.Events {
		srcNodeMap[event.SrcNodeId] = nil
		eventRow.Values = append(eventRow.Values, []interface{}{event.OpId, event.EventType, event.Db, event.PtId, event.SrcNodeId, event.DstNodeId, event.CurrState, event.PreState})
	}

	var availability string
	nodeRow := &models.Row{Columns: []string{"time", "status", "hostname", "nodeID", "nodeType", "availability"}}
	for _, node := range clusterInfo.Nodes {
		availability = "available"
		if node.Status != "alive" {
			availability = "unavailable"
		}
		if _, ok := srcNodeMap[node.NodeID]; ok {
			availability = "unavailable"
		}
		if node.NodeType == "meta" {
			node.Status = "alive"
		}
		nodeRow.Values = append(nodeRow.Values, []interface{}{node.Timestamp, node.Status, node.HostName, node.NodeID, node.NodeType, availability})
	}
	return []*models.Row{nodeRow, eventRow}
}

func (c *Client) ShowCluster(nodeType string, ID uint64) (models.Rows, error) {
	val := &proto2.ShowClusterCommand{
		NodeType: proto.String(nodeType),
		ID:       proto.Uint64(ID),
	}

	t := proto2.Command_ShowClusterCommand
	cmd := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd, proto2.E_ShowClusterCommand_Command, val); err != nil {
		return nil, fmt.Errorf("setExtension err%v", err)
	}
	b, err := c.RetryShowCluster(cmd)
	if err != nil {
		return nil, err
	}

	clusterInfo := &meta2.ShowClusterInfo{}
	if err := clusterInfo.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return c.buildClusterRows(clusterInfo), err
}

func (c *Client) RetryShowCluster(cmd *proto2.Command) ([]byte, error) {
	startTime := time.Now()
	currentServer := connectedServer
	var err error
	var clusterMsg []byte
	for {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil, meta2.ErrClientClosed
		default:

		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()
		clusterMsg, err = c.showCluster(currentServer, cmd)
		if err == nil {
			break
		}

		if time.Since(startTime).Nanoseconds() > int64(len(c.metaServers))*HttpReqTimeout.Nanoseconds() {
			c.logger.Error("show cluster timeout", zap.String("cmd", cmd.String()), zap.Error(err))
			break
		}
		c.logger.Error("retry show cluster timeout", zap.String("cmd", cmd.String()), zap.Error(err))
		time.Sleep(errSleep)

		currentServer++
	}
	return clusterMsg, err
}

func (c *Client) showCluster(currentServer int, cmd *proto2.Command) ([]byte, error) {
	b, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	callback := &ShowClusterCallback{}
	msg := message.NewMetaMessage(message.ShowClusterRequestMessage, &message.ShowClusterRequest{Body: b})
	err = c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	return callback.Data, nil
}

func (c *Client) ShowClusterWithCondition(nodeType string, ID uint64) (models.Rows, error) {
	return c.ShowCluster(nodeType, ID)
}

func (c *Client) Schema(database string, retentionPolicy string, mst string) (fields map[string]int32,
	dimensions map[string]struct{}, err error) {
	fields = make(map[string]int32)
	dimensions = make(map[string]struct{})
	msti, err := c.Measurement(database, retentionPolicy, mst)
	if err != nil {
		return nil, nil, err
	}
	msti.SchemaLock.RLock()
	defer msti.SchemaLock.RUnlock()
	callback := func(k string, v int32) {
		if v == influx.Field_Type_Tag {
			dimensions[k] = struct{}{}
		} else {
			fields[k] = v
		}
	}
	msti.Schema.RangeTypCall(callback)
	return fields, dimensions, nil
}

func (c *Client) UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
	cmd := &proto2.UpdateSchemaCommand{
		Database:      proto.String(database),
		RpName:        proto.String(retentionPolicy),
		Measurement:   proto.String(mst),
		FieldToCreate: fieldToCreate,
	}

	return c.UpdateSchemaByCmd(cmd)
}

func (c *Client) UpdateSchemaByCmd(cmd *proto2.UpdateSchemaCommand) error {
	err := c.retryUntilExec(proto2.Command_UpdateSchemaCommand, proto2.E_UpdateSchemaCommand_Command, cmd)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) IsSQLiteEnabled() bool {
	return c.cacheData.SQLite != nil
}

func (c *Client) InsertFiles(fileInfos []meta2.FileInfo) error {
	size := len(fileInfos)
	if size == 0 {
		return nil
	}
	fileInfosProto := make([]*proto2.FileInfo, size)
	for i, file := range fileInfos {
		fileInfosProto[i] = file.Marshal()
	}
	cmd := &proto2.InsertFilesCommand{FileInfos: fileInfosProto}
	return c.retryUntilExec(proto2.Command_InsertFilesCommand, proto2.E_InsertFilesCommand_Command, cmd)
}

func (c *Client) CreateMeasurement(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo, NumOfShards int32, indexR *influxql.IndexRelation,
	engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error) {
	msti, err := c.Measurement(database, retentionPolicy, mst)
	if msti != nil {
		// check shardkey equal or not
		n := len(msti.ShardKeys)
		if n == 0 || !shardKey.EqualsToAnother(&msti.ShardKeys[n-1]) {
			return nil, meta2.ErrMeasurementExists
		}
		return msti, nil
	}
	if err != meta2.ErrMeasurementNotFound {
		return nil, err
	}

	if !meta2.ValidMeasurementName(mst) {
		return nil, errno.NewError(errno.InvalidMeasurement, mst)
	}

	cmd := &proto2.CreateMeasurementCommand{
		DBName:          proto.String(database),
		RpName:          proto.String(retentionPolicy),
		Name:            proto.String(mst),
		EngineType:      proto.Uint32(uint32(engineType)),
		InitNumOfShards: proto.Int32(NumOfShards),
	}

	if shardKey != nil {
		cmd.Ski = shardKey.Marshal()
	}

	if indexR != nil {
		if msti == nil {
			indexR.Rid = 0
		} else {
			indexR.Rid = 1
		}
		cmd.IR = meta2.EncodeIndexRelation(indexR)
	}

	if colStoreInfo != nil {
		cmd.ColStoreInfo = colStoreInfo.Marshal()
	}

	if len(schemaInfo) > 0 {
		cmd.SchemaInfo = schemaInfo
	}

	if options != nil {
		rpi, err := c.RetentionPolicy(database, retentionPolicy)
		if err != nil {
			return nil, err
		}
		if time.Duration(options.Ttl) > rpi.Duration {
			return nil, errno.NewError(errno.InvalidMeasurementTTL, time.Duration(options.Ttl).String(), rpi.Duration.String())
		}
		cmd.Options = options.Marshal()
	}

	err = c.retryUntilExec(proto2.Command_CreateMeasurementCommand, proto2.E_CreateMeasurementCommand_Command, cmd)
	if err != nil {
		return nil, err
	}
	return c.Measurement(database, retentionPolicy, mst)
}

func (c *Client) SimpleCreateMeasurement(db, rp, mst string, engineType config.EngineType) (*meta2.MeasurementInfo, error) {
	msti, err := c.Measurement(db, rp, mst)
	if msti != nil {
		return msti, nil
	}
	if !errors.Is(err, meta2.ErrMeasurementNotFound) {
		return nil, err
	}
	if !meta2.ValidMeasurementName(mst) {
		return nil, errno.NewError(errno.InvalidMeasurement, mst)
	}

	cmd := &proto2.CreateMeasurementCommand{
		DBName:     proto.String(db),
		RpName:     proto.String(rp),
		Name:       proto.String(mst),
		EngineType: proto.Uint32(uint32(engineType)),
	}
	shardKey := &meta2.ShardKeyInfo{
		ShardKey: nil,
		Type:     meta2.HASH,
	}
	cmd.Ski = shardKey.Marshal()

	err = c.retryUntilExec(proto2.Command_CreateMeasurementCommand, proto2.E_CreateMeasurementCommand_Command, cmd)
	if err != nil {
		return nil, err
	}
	return c.Measurement(db, rp, mst)
}

func (c *Client) AlterShardKey(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo) error {
	_, err := c.Measurement(database, retentionPolicy, mst)
	if err != nil {
		return err
	}

	cmd := &proto2.AlterShardKeyCmd{
		DBName: proto.String(database),
		RpName: proto.String(retentionPolicy),
		Name:   proto.String(mst),
		Ski:    shardKey.Marshal(),
	}

	return c.retryUntilExec(proto2.Command_AlterShardKeyCmd, proto2.E_AlterShardKeyCmd_Command, cmd)
}

// CreateDatabase creates a database or returns it if it already exists.
func (c *Client) CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta2.DatabaseInfo, error) {
	if strings.Count(name, "") > maxDbOrRpName {
		return nil, ErrNameTooLong
	}

	var err error
	replicaN, _, err = checkAndUpdateReplication(replicaN, nil)
	if err != nil {
		return nil, err
	}

	db, err := c.Database(name)
	if db != nil || !errno.Equal(err, errno.DatabaseNotFound) {
		return db, err
	}

	cmd := &proto2.CreateDatabaseCommand{
		Name:           proto.String(name),
		EnableTagArray: proto.Bool(enableTagArray),
		ReplicaNum:     proto.Uint32(replicaN),
	}

	if options != nil && options.Enabled {
		cmd.Options = meta2.MarshalObsOptions(options)
	}

	err = c.retryUntilExec(proto2.Command_CreateDatabaseCommand, proto2.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	return c.Database(name)
}

func checkAndUpdateReplication(dbReplicaN uint32, rpReplicaN *int) (uint32, *int, error) {
	oneReplication := 1
	if dbReplicaN == 0 && rpReplicaN == nil {
		// Default number of replication: 1
		dbReplicaN = 1
		rpReplicaN = &oneReplication
	} else if dbReplicaN == 0 && rpReplicaN != nil {
		dbReplicaN = uint32(*rpReplicaN)
		if dbReplicaN == 0 {
			dbReplicaN = 1
			rpReplicaN = &oneReplication
		}
	} else if dbReplicaN != 0 && rpReplicaN == nil {
		rpReplicaN = &oneReplication
		*rpReplicaN = int(dbReplicaN)
	}

	if dbReplicaN != uint32(*rpReplicaN) {
		return dbReplicaN, rpReplicaN, errno.NewError(errno.ReplicaNumberNotEqual)
	}
	if dbReplicaN%2 == 0 {
		return dbReplicaN, rpReplicaN, errno.NewError(errno.ReplicaNumberNotSupport)
	}
	if dbReplicaN > 1 && config.GetHaPolicy() != config.Replication {
		return dbReplicaN, rpReplicaN, errno.NewError(errno.ConflictWithRep)
	}
	return dbReplicaN, rpReplicaN, nil
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified
// retention policy.
//
// When creating a database with a retention policy, the retention policy will
// always be set to default. Therefore if the caller provides a retention policy
// that already exists on the database, but that retention policy is not the
// default one, an error will be returned.
//
// This call is only idempotent when the caller provides the exact same
// retention policy, and that retention policy is already the default for the
// database.
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, spec *meta2.RetentionPolicySpec, shardKey *meta2.ShardKeyInfo,
	enableTagArray bool, replicaN uint32) (*meta2.DatabaseInfo, error) {
	if spec == nil {
		return nil, errors.New("CreateDatabaseWithRetentionPolicy called with nil spec")
	}

	var err error
	replicaN, spec.ReplicaN, err = checkAndUpdateReplication(replicaN, spec.ReplicaN)
	if err != nil {
		return nil, err
	}

	rpi := spec.NewRetentionPolicyInfo()
	if err := rpi.CheckSpecValid(); err != nil {
		return nil, err
	}

	db, err := c.Database(name)
	if err != nil && !errno.Equal(err, errno.DatabaseNotFound) {
		return nil, err
	}

	if db != nil {
		if !db.ShardKey.EqualsToAnother(shardKey) {
			return nil, errno.NewError(errno.ShardKeyConflict)
		}
		if rp := db.RetentionPolicy(rpi.Name); rp != nil {
			if !rp.EqualsAnotherRp(rpi) {
				return nil, meta2.ErrRetentionPolicyConflict
			}
			return db, nil
		}
	}

	cmd := &proto2.CreateDatabaseCommand{
		Name:            proto.String(name),
		RetentionPolicy: rpi.Marshal(),
		EnableTagArray:  proto.Bool(enableTagArray),
		ReplicaNum:      proto.Uint32(replicaN),
	}

	if len(shardKey.ShardKey) > 0 {
		cmd.Ski = shardKey.Marshal()
	}

	err = c.retryUntilExec(proto2.Command_CreateDatabaseCommand, proto2.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	return c.Database(name)
}

func (c *Client) MarkMeasurementDelete(database, policy, measurement string) error {
	dbi, err := c.Database(database)
	if err != nil {
		return err
	}
	if dbi == nil {
		return nil
	}

	if policy == "" {
		//  rp is not provided, delete all the mst
		err := c.deleteAllRpMst(dbi, database, measurement)
		return err
	}
	rp, ok := dbi.RetentionPolicies[policy]
	if !ok {
		return fmt.Errorf("policy not exist")
	}

	msti := rp.Measurement(measurement)
	if msti == nil {
		c.logger.Error("measurement not found")
		return nil
	} else if msti.MarkDeleted {
		return nil
	}
	cmd := &proto2.MarkMeasurementDeleteCommand{
		Database:    proto.String(database),
		Policy:      proto.String(policy),
		Measurement: proto.String(measurement),
	}
	return c.retryUntilExec(proto2.Command_MarkMeasurementDeleteCommand, proto2.E_MarkMeasurementDeleteCommand_Command, cmd)
}

func (c *Client) deleteAllRpMst(dbi *meta2.DatabaseInfo, database, measurement string) error {
	var err error
	for _, rp := range dbi.RetentionPolicies {
		msti := rp.Measurement(measurement)
		if msti == nil {
			continue
		}
		if msti.MarkDeleted {
			return nil
		}
		cmd := &proto2.MarkMeasurementDeleteCommand{
			Database:    proto.String(database),
			Policy:      proto.String(rp.Name),
			Measurement: proto.String(measurement),
		}
		err = c.retryUntilExec(proto2.Command_MarkMeasurementDeleteCommand, proto2.E_MarkMeasurementDeleteCommand_Command, cmd)
		if err != nil {
			break
		}
	}
	return err
}

func (c *Client) MarkDatabaseDelete(name string) error {
	cmd := &proto2.MarkDatabaseDeleteCommand{
		Name: proto.String(name),
	}

	return c.retryUntilExec(proto2.Command_MarkDatabaseDeleteCommand, proto2.E_MarkDatabaseDeleteCommand_Command, cmd)
}

func (c *Client) MarkRetentionPolicyDelete(database, name string) error {
	cmd := &proto2.MarkRetentionPolicyDeleteCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
	}

	return c.retryUntilExec(proto2.Command_MarkRetentionPolicyDeleteCommand, proto2.E_MarkRetentionPolicyDeleteCommand_Command, cmd)
}

func (c *Client) RevertRetentionPolicyDelete(database, name string) error {
	rp, err := c.RetentionPolicy(database, name)
	if err != nil {
		return err
	}
	if rp == nil || rp.MarkDeleted {
		return meta2.ErrRetentionPolicyNotFound(name)
	}

	for i := range rp.ShardGroups {
		if !rp.ShardGroups[i].Deleted() {
			continue
		}
		err := c.DeleteShardGroup(database, name, rp.ShardGroups[i].ID, meta2.CancelDelete)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) GetExpiredShards() ([]meta2.ExpiredShardInfos, []meta2.ExpiredShardInfos) {
	t := time.Now().UTC()
	markDelSgInfos := []meta2.ExpiredShardInfos{}
	expiredShards := []meta2.ExpiredShardInfos{}

	dataBases := c.Databases()
	for dbName, db := range dataBases {
		if db.Options == nil || db.MarkDeleted {
			continue
		}
		obsOptions := db.Options
		dbPtInfos, err := c.DBPtView(dbName)
		if err != nil {
			continue
		}
		for rpName, rp := range db.RetentionPolicies {
			if rp.MarkDeleted {
				continue
			}
			for i := range rp.ShardGroups {
				// 1.mark the shardGroup deleted
				if !rp.ShardGroups[i].Deleted() {
					if rp.Duration != 0 && rp.ShardGroups[i].EndTime.Add(rp.Duration).Before(t) {
						markDelSgInfos = append(markDelSgInfos, meta2.ExpiredShardInfos{Database: dbName, Policy: rpName, ShardGroupId: rp.ShardGroups[i].ID})
					}
					continue
				}
				// 2. Deleted: get shardGroupID and shardPath that need to delete
				if rp.ShardGroups[i].DeletedAt.Add(RetentionDelayedTime).After(t) {
					continue
				}
				shardPaths := []string{}
				shardIds := []uint64{}
				for j := range rp.ShardGroups[i].Shards {
					if rp.ShardGroups[i].Shards[j].MarkDelete {
						continue
					}
					// For online shards, aging operations are performed by their owner node.
					ptId := rp.ShardGroups[i].Shards[j].Owners[0]
					if dbPtInfos[ptId].Owner.NodeID != c.nodeID && dbPtInfos[ptId].Status == meta2.Online {
						continue
					}
					logPath := obs.GetShardPath(
						rp.ShardGroups[i].Shards[j].ID,
						rp.ShardGroups[i].Shards[j].IndexID,
						rp.ShardGroups[i].Shards[j].Owners[0],
						rp.ShardGroups[i].StartTime,
						rp.ShardGroups[i].EndTime, db.Name, rp.Name)
					shardPaths = append(shardPaths, logPath)
					shardIds = append(shardIds, rp.ShardGroups[i].Shards[j].ID)
				}
				expiredShards = append(expiredShards, meta2.ExpiredShardInfos{Database: dbName, Policy: rpName, ShardGroupId: rp.ShardGroups[i].ID,
					ShardIds: shardIds, ShardPaths: shardPaths, ObsOpts: obsOptions})
			}
		}
	}
	return markDelSgInfos, expiredShards
}

func (c *Client) GetExpiredIndexes() []meta2.ExpiredIndexInfos {
	t := time.Now().UTC()
	expiredIndexes := []meta2.ExpiredIndexInfos{}

	dataBases := c.Databases()
	for dbName, db := range dataBases {
		if db.Options == nil || db.MarkDeleted {
			continue
		}
		dbPtInfos, err := c.DBPtView(dbName)
		if err != nil {
			continue
		}
		for rpName, rp := range db.RetentionPolicies {
			if rp.MarkDeleted {
				continue
			}
			for i := range rp.IndexGroups {
				if rp.Duration == 0 || rp.IndexGroups[i].EndTime.Add(rp.Duration+RetentionDelayedTime).After(t) {
					continue
				}
				indexIds := make([]uint64, 0, len(rp.IndexGroups[i].Indexes))
				for j := range rp.IndexGroups[i].Indexes {
					// For online shards, aging operations are performed by their owner node.
					ptId := rp.IndexGroups[i].Indexes[j].Owners[0]
					if dbPtInfos[ptId].Owner.NodeID != c.nodeID && dbPtInfos[ptId].Status == meta2.Online {
						continue
					}
					indexIds = append(indexIds, rp.IndexGroups[i].Indexes[j].ID)
				}
				expiredIndexes = append(expiredIndexes, meta2.ExpiredIndexInfos{Database: dbName, Policy: rpName, IndexGroupID: rp.IndexGroups[i].ID,
					IndexIDs: indexIds})
			}
		}
	}
	return expiredIndexes
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error) {

	if spec.Duration != nil && *spec.Duration < meta2.MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, meta2.ErrRetentionPolicyDurationTooLow
	}

	rpi := spec.NewRetentionPolicyInfo()
	if strings.Count(rpi.Name, "") > maxDbOrRpName {
		return nil, ErrNameTooLong
	}
	cmd := &proto2.CreateRetentionPolicyCommand{
		Database:        proto.String(database),
		RetentionPolicy: rpi.Marshal(),
		DefaultRP:       proto.Bool(makeDefault),
	}

	if err := c.retryUntilExec(proto2.Command_CreateRetentionPolicyCommand, proto2.E_CreateRetentionPolicyCommand_Command, cmd); err != nil {
		return nil, err
	}

	return c.RetentionPolicy(database, rpi.Name)
}

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *meta2.RetentionPolicyInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db := c.cacheData.Database(database)
	if db == nil || db.MarkDeleted {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}

	return db.RetentionPolicy(name), nil
}

func (c *Client) GetNodePtsMap(database string) (map[uint64][]uint32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.cacheData == nil || len(c.cacheData.PtView[database]) == 0 {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}
	nodePtMap := make(map[uint64][]uint32, len(c.cacheData.DataNodes))
	repGroups := c.cacheData.DBRepGroups(database)
	ptInfo := c.cacheData.DBPtView(database)
	repGroupN := uint32(len(repGroups))
	for i := range ptInfo {
		if config.GetHaPolicy() == config.WriteAvailableFirst && c.cacheData.PtView[database][i].Status == meta2.Offline {
			continue
		}
		if config.GetHaPolicy() == config.Replication && ptInfo[i].RGID < repGroupN && !repGroups[ptInfo[i].RGID].IsMasterPt(ptInfo[i].PtId) {
			continue
		}
		nodePtMap[ptInfo[i].Owner.NodeID] = append(nodePtMap[ptInfo[i].Owner.NodeID], ptInfo[i].PtId)
	}
	return nodePtMap, nil
}

func (c *Client) DBPtView(database string) (meta2.DBPtInfos, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pts := c.cacheData.DBPtView(database)
	if pts == nil {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}

	return pts, nil
}

func (c *Client) DBRepGroups(database string) []meta2.ReplicaGroup {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.DBRepGroups(database)
}

func (c *Client) GetReplicaN(database string) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db := c.cacheData.Database(database)
	if db == nil {
		return 0, errno.NewError(errno.DatabaseNotFound, database)
	}
	return db.ReplicaN, nil
}

// SetDefaultRetentionPolicy sets a database's default retention policy.
func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	cmd := &proto2.SetDefaultRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
	}

	return c.retryUntilExec(proto2.Command_SetDefaultRetentionPolicyCommand, proto2.E_SetDefaultRetentionPolicyCommand_Command, cmd)
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *meta2.RetentionPolicyUpdate, makeDefault bool) error {
	var newName *string
	if rpu.Name != nil {
		newName = rpu.Name
	}

	var replicaN *uint32
	if rpu.ReplicaN != nil {
		value := uint32(*rpu.ReplicaN)
		replicaN = &value
	}

	cmd := &proto2.UpdateRetentionPolicyCommand{
		Database:           proto.String(database),
		Name:               proto.String(name),
		NewName:            newName,
		Duration:           meta2.GetInt64Duration(rpu.Duration),
		ReplicaN:           replicaN,
		ShardGroupDuration: meta2.GetInt64Duration(rpu.ShardGroupDuration),
		MakeDefault:        proto.Bool(makeDefault),
		HotDuration:        meta2.GetInt64Duration(rpu.HotDuration),
		WarmDuration:       meta2.GetInt64Duration(rpu.WarmDuration),
		IndexGroupDuration: meta2.GetInt64Duration(rpu.IndexGroupDuration),
		IndexColdDuration:  meta2.GetInt64Duration(rpu.IndexColdDuration),
	}

	return c.retryUntilExec(proto2.Command_UpdateRetentionPolicyCommand, proto2.E_UpdateRetentionPolicyCommand_Command, cmd)
}

// IsLeader - should get rid of this
func (c *Client) IsLeader() bool {
	return false
}

// Users returns a slice of UserInfo representing the currently known users.
func (c *Client) Users() []meta2.UserInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	users := c.cacheData.Users

	if users == nil {
		return []meta2.UserInfo{}
	}
	return users
}

// User returns the user with the given name, or ErrUserNotFound.
func (c *Client) User(name string) (meta2.User, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, u := range c.cacheData.Users {
		if u.Name == name {
			return &u, nil
		}
	}

	return nil, meta2.ErrUserNotFound
}

// CreateUser adds a user with the given name and password and admin status.
func (c *Client) CreateUser(name, password string, admin, rwuser bool) (meta2.User, error) {
	// verify name length
	if err := c.isValidName(name); err != nil {
		return nil, err
	}
	// verify password
	if err := c.isValidPwd(password, name); err != nil {
		return nil, err
	}

	data := c.cacheData.Clone()

	// See if the user already exists.
	if u := data.GetUser(name); u != nil {
		if err := c.auth.CompareHashAndPlainPwd(u.Hash, password); err != nil || u.Admin != admin {
			return nil, meta2.ErrUserExists
		}
		return u, nil
	}

	// Forbidden create multi admin user
	if admin && data.HasAdminUser() {
		return nil, meta2.ErrUserForbidden
	}

	// Hash the password before serializing it.
	hash, err := c.auth.genHashPwdVal(password)
	if err != nil {
		return nil, err
	}

	if err := c.retryUntilExec(proto2.Command_CreateUserCommand, proto2.E_CreateUserCommand_Command,
		&proto2.CreateUserCommand{
			Name:   proto.String(name),
			Hash:   proto.String(hash),
			Admin:  proto.Bool(admin),
			RwUser: proto.Bool(rwuser),
		},
	); err != nil {
		return nil, err
	}
	return c.User(name)
}

// UpdateUser updates the password of an existing user.
func (c *Client) UpdateUser(name, password string) error {
	// verify password
	if err := c.isValidPwd(password, name); err != nil {
		return err
	}

	// Hash the password before serializing it.
	hash, err := c.auth.genHashPwdVal(password)
	if err != nil {
		return err
	}

	if u := c.cacheData.GetUser(name); u == nil {
		return meta2.ErrUserNotFound
	} else {
		if err = c.auth.CompareHashAndPlainPwd(u.Hash, password); err == nil {
			return meta2.ErrPwdUsed
		}
	}
	return c.retryUntilExec(proto2.Command_UpdateUserCommand, proto2.E_UpdateUserCommand_Command,
		&proto2.UpdateUserCommand{
			Name: proto.String(name),
			Hash: proto.String(hash),
		},
	)
}

func (c *Client) isValidName(user string) error {
	if len(user) < minUsernameLen || len(user) > maxUsernameLen {
		return errno.NewError(errno.InvalidUsernameLen, minUsernameLen, maxUsernameLen)
	}
	return nil
}

func (c *Client) isValidPwd(s, user string) error {
	if len(s) < minPasswordLen || len(s) > maxPasswordLen {
		return errno.NewError(errno.InvalidPwdLen, minPasswordLen, maxPasswordLen)
	}

	if ok, _ := c.isInWeakPwdDict(s); ok {
		return errno.NewError(errno.InvalidPwdComplex)
	}

	if c.isSimilarToUserName(s, user) {
		return errno.NewError(errno.InvalidPwdLooks)
	}

	if !c.isStrengthPwd(s) {
		return errno.NewError(errno.InvalidPwdComplex)
	}

	return nil
}

func (c *Client) isSimilarToUserName(s, user string) bool {
	if len(s) != len(user) {
		return false
	}

	//pwd same as user name
	if s == user {
		return true
	}

	//pwd same as reverse(user name)
	slen := len(s)
	for i := 0; i < slen; i++ {
		if s[i] != user[slen-1-i] {
			return false
		}
	}
	return true
}

func (c *Client) isInWeakPwdDict(s string) (bool, error) {
	filename := c.weakPwdPath
	content, err := os.ReadFile(path.Clean(filename))
	if err != nil {
		return false, err
	}
	dec := unicode.BOMOverride(transform.Nop)
	content, _, err = transform.Bytes(dec, content)
	if err != nil {
		return false, err
	}
	weakPwdDict := strings.Split(string(content), "\r\n")
	for _, v := range weakPwdDict {
		if v == s {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) isStrengthPwd(s string) bool {
	var specChars = "-~@$#%_^!*+=?"
	var lowerCh, upperCh, digitCh, specCh bool
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case 'a' <= ch && ch <= 'z':
			lowerCh = true
		case 'A' <= ch && ch <= 'Z':
			upperCh = true
		case '0' <= ch && ch <= '9':
			digitCh = true
		case strings.Contains(specChars, string(ch)):
			specCh = true
		default:
			c.logger.Info("verify pwd strength:", zap.Any("unsupported char", string(ch)))
			return false
		}
	}
	c.logger.Info("verify pwd strength:", zap.Any("lower", lowerCh), zap.Any("upper", upperCh),
		zap.Any("digit", digitCh), zap.Any("spec", specCh))
	return lowerCh && upperCh && digitCh && specCh
}

// DropUser removes the user with the given name.
func (c *Client) DropUser(name string) error {
	if u, err := c.User(name); err != nil {
		return err
	} else {
		if u.AuthorizeUnrestricted() {
			return meta2.ErrUserDropSelf
		}
	}
	return c.retryUntilExec(proto2.Command_DropUserCommand, proto2.E_DropUserCommand_Command,
		&proto2.DropUserCommand{
			Name: proto.String(name),
		},
	)
}

// SetPrivilege sets a privilege for the given user on the given database.
func (c *Client) SetPrivilege(username, database string, p originql.Privilege) error {
	return c.retryUntilExec(proto2.Command_SetPrivilegeCommand, proto2.E_SetPrivilegeCommand_Command,
		&proto2.SetPrivilegeCommand{
			Username:  proto.String(username),
			Database:  proto.String(database),
			Privilege: proto.Int32(int32(p)),
		},
	)
}

// SetAdminPrivilege sets or unsets admin privilege to the given username.
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	return c.retryUntilExec(proto2.Command_SetAdminPrivilegeCommand, proto2.E_SetAdminPrivilegeCommand_Command,
		&proto2.SetAdminPrivilegeCommand{
			Username: proto.String(username),
			Admin:    proto.Bool(admin),
		},
	)
}

// UserPrivileges returns the privileges for a user mapped by database name.
func (c *Client) UserPrivileges(username string) (map[string]originql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivileges(username)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// UserPrivilege returns the privilege for the given user on the given database.
func (c *Client) UserPrivilege(username, database string) (*originql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivilege(username, database)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// AdminUserExists returns true if any user has admin privilege.
func (c *Client) AdminUserExists() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.AdminUserExist()
}

// Authenticate returns a UserInfo if the username and password match an existing entry.
func (c *Client) Authenticate(username, password string) (u meta2.User, e error) {
	//verify user lock or not
	if c.auth.IsLockedUser(username) {
		return nil, meta2.ErrUserLocked
	}

	// Find user.
	c.mu.RLock()
	userInfo := c.cacheData.GetUser(username)
	c.mu.RUnlock()
	if userInfo == nil {
		return nil, meta2.ErrUserNotFound
	}

	err := c.auth.Authenticate(userInfo, password)
	return userInfo, err
}

// UserCount returns the number of users stored.
func (c *Client) UserCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.cacheData.Users)
}

// ShardIDs returns a list of all shard ids.
func (c *Client) ShardIDs() []uint64 {
	c.mu.RLock()

	var a []uint64
	for _, dbi := range c.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					a = append(a, si.ID)
				}
			}
		}
	}
	c.mu.RUnlock()
	slices.Sort(a)
	return a
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta2.ShardGroupInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Find retention policy.
	rpi, err := c.cacheData.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, meta2.ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]meta2.ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// DropShard deletes a shard by ID.
func (c *Client) DropShard(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data := c.cacheData.Clone()
	data.DropShard(id)
	return nil
}

func (c *Client) GetSgEndTime(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	db := c.cacheData.Database(database)
	if db == nil || db.MarkDeleted {
		return 0, errno.NewError(errno.DatabaseNotFound, database)
	}

	rpi := db.RetentionPolicy(rp)
	if rpi == nil {
		return 0, errno.NewError(errno.RpNotFound, rp)
	}
	sg := rpi.ShardGroupByTimestampAndEngineType(timestamp, engineType)
	if sg == nil {
		return 0, errno.NewError(errno.WriteNoShardGroup)
	}
	return sg.EndTime.UnixNano(), nil
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time, version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, error) {
	c.mu.RLock()
	sg, tier, err := c.cacheData.GetTierOfShardGroup(database, policy, timestamp, c.ShardTier, engineType)
	if err != nil {
		c.mu.RUnlock()
		return nil, err
	}
	if sg != nil {
		sgi := *sg // need to make a copy
		c.mu.RUnlock()
		return &sgi, nil
	}
	c.mu.RUnlock()

	cmd := &proto2.CreateShardGroupCommand{
		Database:   proto.String(database),
		Policy:     proto.String(policy),
		Timestamp:  proto.Int64(timestamp.UnixNano()),
		ShardTier:  proto.Uint64(tier),
		EngineType: proto.Uint32(uint32(engineType)),
		Version:    proto.Uint32(version),
	}

	if err := c.retryUntilExec(proto2.Command_CreateShardGroupCommand, proto2.E_CreateShardGroupCommand_Command, cmd); err != nil {
		return nil, err
	}

	rpi, err := c.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil || rpi.MarkDeleted {
		return nil, errors.New("retention policy deleted after shard group created")
	}
	c.mu.RLock() // ShardGroups of rpi may be changed when some shardgroups are deleted/pruned.
	defer c.mu.RUnlock()
	sgi := *(rpi.ShardGroupByTimestampAndEngineType(timestamp, engineType)) // need to make a copy

	return &sgi, nil
}

func (c *Client) GetShardInfoByTime(database, retentionPolicy string, t time.Time, ptIdx int, nodeId uint64, engineType config.EngineType) (*meta2.ShardInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rp, err := c.cacheData.RetentionPolicy(database, retentionPolicy)
	if err != nil {
		return nil, err
	}
	if rp.MarkDeleted {
		return nil, errno.NewError(errno.RpNotFound)
	}
	shardGroup := rp.ShardGroupByTimestampAndEngineType(t, engineType)
	if shardGroup == nil {
		return nil, errno.NewError(errno.ShardNotFound)
	}
	if shardGroup.Deleted() {
		return nil, errno.NewError(errno.ShardNotFound)
	}

	info := c.cacheData.PtView[database]
	if info == nil {
		return nil, fmt.Errorf("db %v in PtView not exist", database)
	}
	cnt, ptId := 0, uint32(math.MaxUint32)
	for i := range info {
		if info[i].Owner.NodeID == nodeId {
			if ptIdx == cnt {
				ptId = info[i].PtId
				cnt++
				break
			} else {
				cnt++
			}
		}
	}
	if cnt == 0 || ptId == math.MaxUint32 {
		return nil, errors.New("nodeId cannot find pt")
	}

	shard := shardGroup.Shards[ptId]
	return &shard, nil
}

func (c *Client) getAliveShardsForWAF(database string, sgi *meta2.ShardGroupInfo, read bool) []int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !read && config.IsHardWrite() {
		return c.getAliveShardsForHardWrite(database, sgi)
	}
	aliveShardIdxes := make([]int, 0, len(sgi.Shards))
	for i := range sgi.Shards {
		if c.cacheData.PtView[database][sgi.Shards[i].Owners[0]].Status == meta2.Online {
			aliveShardIdxes = append(aliveShardIdxes, i)
		}
	}
	return aliveShardIdxes
}

func (c *Client) getAliveShardsForHardWrite(database string, sgi *meta2.ShardGroupInfo) []int {
	aliveShardIdxes := make([]int, 0, len(sgi.Shards))
	for i := range sgi.Shards {
		aliveShardIdxes = append(aliveShardIdxes, i)
	}
	return aliveShardIdxes
}

func (c *Client) getAliveShardsForSSAndRep(database string, sgi *meta2.ShardGroupInfo) []int {
	replicaN, err := c.GetReplicaN(database)
	if replicaN == 0 || err != nil {
		replicaN = 1
	}
	if replicaN > 1 {
		return c.getAliveShardsForRepDB(database, sgi, replicaN)
	}

	c.mu.RLock()
	aliveShardIdxes := make([]int, 0, len(sgi.Shards)/replicaN)
	for i := range sgi.Shards {
		aliveShardIdxes = append(aliveShardIdxes, i)
	}
	c.mu.RUnlock()
	return aliveShardIdxes
}

func (c *Client) getAliveShardsForRepDB(database string, sgi *meta2.ShardGroupInfo, replicaN int) []int {
	repGroups := c.DBRepGroups(database)
	aliveShardIdxes := make([]int, 0, len(sgi.Shards)/replicaN)
	addedRGID := make(map[uint32]interface{}, 0)

	c.mu.RLock()
	ptView := c.cacheData.PtView[database]
	for i := range sgi.Shards {
		for _, ptId := range sgi.Shards[i].Owners {
			if repGroups[ptView[ptId].RGID].Status == meta2.Health && repGroups[ptView[ptId].RGID].IsMasterPt(ptId) {
				aliveShardIdxes = append(aliveShardIdxes, i)
				addedRGID[ptView[ptId].RGID] = nil
				break
			} else if repGroups[ptView[ptId].RGID].Status == meta2.SubHealth && ptView[ptId].Status == meta2.Online {
				if _, ok := addedRGID[ptView[ptId].RGID]; !ok {
					aliveShardIdxes = append(aliveShardIdxes, i)
					addedRGID[ptView[ptId].RGID] = nil
					break
				}
			}
		}
	}
	c.mu.RUnlock()
	return aliveShardIdxes
}

func (c *Client) GetNodePT(database string) []uint32 {
	ptIds := []uint32{}
	c.mu.RLock()
	ptView := c.cacheData.PtView[database]
	for _, ptInfo := range ptView {
		if ptInfo.Owner.NodeID == c.nodeID {
			ptIds = append(ptIds, ptInfo.PtId)
		}
	}
	c.mu.RUnlock()
	return ptIds
}

/*
used for map shards in select and write progress.
write progress shard for all shards in shared-storage and replication policy.
*/
func (c *Client) GetAliveShards(database string, sgi *meta2.ShardGroupInfo, isRead bool) []int {
	if config.GetHaPolicy() != config.WriteAvailableFirst {
		return c.getAliveShardsForSSAndRep(database, sgi)
	}
	return c.getAliveShardsForWAF(database, sgi, isRead)
}

func (c *Client) DeleteIndexGroup(database, policy string, id uint64) error {
	cmd := &proto2.DeleteIndexGroupCommand{
		Database:     proto.String(database),
		Policy:       proto.String(policy),
		IndexGroupID: proto.Uint64(id),
	}
	_, err := c.retryExec(proto2.Command_DeleteIndexGroupCommand, proto2.E_DeleteIndexGroupCommand_Command, cmd)
	return err
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64, deleteType int32) error {
	cmd := &proto2.DeleteShardGroupCommand{
		Database:     proto.String(database),
		Policy:       proto.String(policy),
		ShardGroupID: proto.Uint64(id),
		DeleteType:   proto.Int32(deleteType),
	}

	_, err := c.retryExec(proto2.Command_DeleteShardGroupCommand, proto2.E_DeleteShardGroupCommand_Command, cmd)
	return err
}

// When delay-deleted, the deletedAt time cannot be updated with the raft playback, so deletedAt is specified by the client.
func (c *Client) DelayDeleteShardGroup(database, policy string, id uint64, deletedAt time.Time, deleteType int32) error {
	cmd := &proto2.DeleteShardGroupCommand{
		Database:     proto.String(database),
		Policy:       proto.String(policy),
		ShardGroupID: proto.Uint64(id),
		DeletedAt:    proto.Int64(deletedAt.UnixNano()),
		DeleteType:   proto.Int32(deleteType),
	}

	_, err := c.retryExec(proto2.Command_DeleteShardGroupCommand, proto2.E_DeleteShardGroupCommand_Command, cmd)
	return err
}

// PyStore send command to PyMeta. NO need to waitForIndex.
func (c *Client) PruneGroupsCommand(shardGroup bool, id uint64) error {
	cmd := &proto2.PruneGroupsCommand{
		ShardGroup: proto.Bool(shardGroup),
		ID:         proto.Uint64(id),
	}
	_, err := c.retryExec(proto2.Command_PruneGroupsCommand, proto2.E_PruneGroupsCommand_Command, cmd)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error {
	cmd := &proto2.UpdateShardInfoTierCommand{
		ShardID: proto.Uint64(shardID),
		Tier:    proto.Uint64(tier),
		DbName:  proto.String(dbName),
		RpName:  proto.String(rpName),
	}
	_, err := c.retryExec(proto2.Command_UpdateShardInfoTierCommand, proto2.E_UpdateShardInfoTierCommand_Command, cmd)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) UpdateIndexInfoTier(indexID uint64, tier uint64, dbName, rpName string) error {
	cmd := &proto2.UpdateIndexInfoTierCommand{
		IndexID: proto.Uint64(indexID),
		Tier:    proto.Uint64(tier),
		DbName:  proto.String(dbName),
		RpName:  proto.String(rpName),
	}
	_, err := c.retryExec(proto2.Command_UpdateIndexInfoTierCommand, proto2.E_UpdateIndexInfoTierCommand_Command, cmd)
	if err != nil {
		return err
	}
	return nil
}

// ShardOwner returns the owning shard group info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, policy string, sgi *meta2.ShardGroupInfo) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for dbIdx := range c.cacheData.Databases {
		rps := c.cacheData.Databases[dbIdx].RetentionPolicies
		for rpIdx := range rps {
			for sgIdx := range rps[rpIdx].ShardGroups {
				sg := &rps[rpIdx].ShardGroups[sgIdx]
				if sg.Deleted() {
					continue
				}
				for shIdx := range sg.Shards {
					if sg.Shards[shIdx].ID == shardID {
						database = c.cacheData.Databases[dbIdx].Name
						policy = rps[rpIdx].Name
						sgi = sg
						return
					}
				}
			}
		}
	}
	return
}

// JoinMetaServer will add the passed in tcpAddr to the raft peers and add a MetaNode to
// the metastore
func (c *Client) JoinMetaServer(httpAddr, rpcAddr, tcpAddr string) (*meta2.NodeInfo, error) {
	node := &meta2.NodeInfo{
		Host:    httpAddr,
		RPCAddr: rpcAddr,
		TCPHost: tcpAddr,
	}
	b, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}

	currentServer := 0
	for {
		c.mu.RLock()
		if currentServer >= len(c.metaServers) {
			// We've tried every server, wait a second before
			// trying again
			time.Sleep(errSleep)
			currentServer = 0
		}
		c.mu.RUnlock()

		callback := &JoinCallback{NodeInfo: node}
		msg := message.NewMetaMessage(message.UpdateRequestMessage, &message.UpdateRequest{Body: b})
		err = c.SendRPCMsg(currentServer, msg, callback)
		if err != nil {
			currentServer++
			continue
		}
		// Successfully joined
		break
	}
	return node, nil
}

func (c *Client) CreateMetaNode(httpAddr, tcpAddr string) (*meta2.NodeInfo, error) {
	cmd := &proto2.CreateMetaNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}

	if err := c.retryUntilExec(proto2.Command_CreateMetaNodeCommand, proto2.E_CreateMetaNodeCommand_Command, cmd); err != nil {
		return nil, err
	}

	n := c.MetaNodeByAddr(httpAddr)
	if n == nil {
		return nil, errors.New("new meta node not found")
	}

	c.nodeID = n.ID

	return n, nil
}

func (c *Client) DeleteMetaNode(id uint64) error {
	cmd := &proto2.DeleteMetaNodeCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(proto2.Command_DeleteMetaNodeCommand, proto2.E_DeleteMetaNodeCommand_Command, cmd)
}

// validateURL returns an error if the URL does not have a port or uses a scheme other than HTTP.
func validateURL(input string) error {
	u, err := url.Parse(input)
	if err != nil {
		return errors.New("invalid url")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("invalid url")
	}

	_, port, err := net.SplitHostPort(u.Host)
	if err != nil || port == "" {
		return errors.New("invalid url")
	}

	return nil
}

func pingServer(server string) error {
	pingUrl := server + "/ping"
	client := http.Client{Timeout: time.Second}
	req, err := http.NewRequest("GET", pingUrl, nil)
	if err != nil {
		return err
	}
	_, err = client.Do(req)
	if err != nil {
		return err
	}
	return nil
}

// CreateSubscription creates a subscription against the given database and retention policy.
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	for _, destination := range destinations {
		if err := validateURL(destination); err != nil {
			return fmt.Errorf("invalid url %s", destination)
		}
		if err := pingServer(destination); err != nil {
			return fmt.Errorf("fail to ping %s", destination)
		}
	}
	return c.retryUntilExec(proto2.Command_CreateSubscriptionCommand, proto2.E_CreateSubscriptionCommand_Command,
		&proto2.CreateSubscriptionCommand{
			Database:        proto.String(database),
			RetentionPolicy: proto.String(rp),
			Name:            proto.String(name),
			Mode:            proto.String(mode),
			Destinations:    destinations,
		},
	)
}

// DropSubscription removes the named subscription from the given database and retention policy.
func (c *Client) DropSubscription(database, rp, name string) error {
	return c.retryUntilExec(proto2.Command_DropSubscriptionCommand, proto2.E_DropSubscriptionCommand_Command,
		&proto2.DropSubscriptionCommand{
			Database:        proto.String(database),
			RetentionPolicy: proto.String(rp),
			Name:            proto.String(name),
		},
	)
}

func (c *Client) GetMaxSubscriptionID() uint64 {
	return c.cacheData.MaxSubscriptionID
}

// WaitForDataChanged returns a channel that will get a stuct{} when
// the metastore data has changed.
func (c *Client) WaitForDataChanged() chan struct{} {
	ch := make(chan struct{})

	c.changed <- ch
	return ch
}

func (c *Client) index() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.Index
}

func (c *Client) LocalExec(index uint64, typ proto2.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	c.mu.RLock()
	if index <= c.cacheData.Index {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()
	opcmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(opcmd, desc, value); err != nil {
		panic(err)
	}
	if _, ok := applyFunc[typ]; ok {
		c.waitForIndex(index)
		return nil
	} else {
		c.logger.Info("cannot apply command: %x", zap.Int32("typ", int32(*opcmd.Type)))
	}
	return nil
}

// retryUntilExec will attempt the command on each of the metaservers until it either succeeds or
// hits the max number of tries
func (c *Client) retryUntilExec(typ proto2.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	index, err := c.retryExec(typ, desc, value)
	if err != nil {
		return err
	}
	if c.UseSnapshotV2 {
		return c.LocalExec(index, typ, desc, value)
	}
	c.waitForIndex(index)
	return nil
}

func (c *Client) retryExec(typ proto2.Command_Type, desc *proto.ExtensionDesc, value interface{}) (index uint64, err error) {
	// TODO do not use index to check cache data is newest
	tries := 0
	currentServer := connectedServer
	timeout := time.After(RetryExecTimeout)

	for {
		c.mu.RLock()
		// exit if we're closed
		select {
		case <-timeout:
			c.mu.RUnlock()
			return c.index(), meta2.ErrCommandTimeout
		case <-c.closing:
			c.mu.RUnlock()
			return c.index(), meta2.ErrClientClosed
		default:
			// we're still open, continue on
		}
		c.mu.RUnlock()

		// build the url to hit the redirect server or the next metaserver
		var server string

		c.mu.RLock()
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		server = c.metaServers[currentServer]
		c.mu.RUnlock()

		index, err = c.exec(currentServer, typ, desc, value, message.ExecuteRequestMessage)

		if err == nil {
			return index, nil
		}
		tries++
		currentServer++

		if strings.Contains(err.Error(), "node is not the leader") {
			continue
		}

		if _, ok := err.(errCommand); ok {
			return c.index(), err
		}
		c.logger.Info("retryUntilExec retry", zap.String("server", server), zap.Any("type", typ),
			zap.Int("tries", tries), zap.Error(err))
		time.Sleep(errSleep)
	}
}

func getMetaMsg(msgTy uint8, body []byte) *message.MetaMessage {
	switch msgTy {
	case message.ExecuteRequestMessage:
		return message.NewMetaMessage(msgTy, &message.ExecuteRequest{Body: body})
	case message.ReportRequestMessage:
		return message.NewMetaMessage(msgTy, &message.ReportRequest{Body: body})
	default:
		return nil
	}
}

func (c *Client) exec(currentServer int, typ proto2.Command_Type, desc *proto.ExtensionDesc, value interface{}, msgTy uint8) (index uint64, err error) {
	// Create command.
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return 0, err
	}

	callback := &ExecuteAndReportCallback{Typ: msgTy}
	msg := getMetaMsg(msgTy, b)
	err = c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return 0, err
	} else if callback.ErrCommand != nil {
		return 0, *callback.ErrCommand
	}
	return callback.Index, nil
}

func (c *Client) waitForIndex(idx uint64) {
	for {
		ch := c.WaitForDataChanged()
		c.mu.RLock()
		if c.cacheData.Index >= idx {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()
		<-ch
	}
}

// Role: sql/store/meta
func (c *Client) pollForUpdates(role Role) {
	for {
		data := c.retryUntilSnapshot(role, c.index())
		if data == nil {
			// this will only be nil if the client has been closed,
			// so we can exit out
			c.logger.Error("client has been closed")
			return
		}
		// update the data and notify of the change
		c.mu.Lock()
		idx := c.cacheData.Index
		if idx < data.Index {
			c.cacheData = data
			c.auth.UpdateAuthCache(data.Users)
			c.replicaInfoManager.Update(data, c.nodeID, role)
			for len(c.changed) > 0 {
				notifyC := <-c.changed
				close(notifyC)
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) pollForUpdatesV2(role Role) {
	for {
		preIndex := c.index()
		err := c.retryUntilSnapshotV2(role, preIndex)
		if err != nil {
			// this will only be nil if the client has been closed,
			// so we can exit out
			c.logger.Error("client has been closed:", zap.String("err:", err.Error()))
			return
		}
		// update the data and notify of the change
		c.mu.Lock()
		if preIndex < c.cacheData.Index {
			c.auth.UpdateAuthCache(c.cacheData.Users)
			c.replicaInfoManager.Update(c.cacheData, c.nodeID, role)
			for len(c.changed) > 0 {
				notifyC := <-c.changed
				close(notifyC)
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) getNode(currentServer int, writeHost, queryHost, role, az string) (*meta2.NodeStartInfo, error) {
	callback := &CreateNodeCallback{
		NodeStartInfo: &meta2.NodeStartInfo{},
	}
	msg := message.NewMetaMessage(message.CreateNodeRequestMessage, &message.CreateNodeRequest{WriteHost: writeHost, QueryHost: queryHost, Role: role, Az: az})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	return callback.NodeStartInfo, nil
}

func (c *Client) getSqlNode(httpHost string, gossipHost string, currentServer int) (*meta2.NodeStartInfo, error) {
	callback := &CreateSqlNodeCallback{
		NodeStartInfo: &meta2.NodeStartInfo{},
	}
	msg := message.NewMetaMessage(message.CreateSqlNodeRequestMessage, &message.CreateSqlNodeRequest{HttpHost: httpHost, GossipHost: gossipHost})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	return callback.NodeStartInfo, nil
}

func (c *Client) getShardInfo(currentServer int, cmd *proto2.Command) ([]byte, error) {
	b, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	callback := &GetShardInfoCallback{}
	msg := message.NewMetaMessage(message.GetShardInfoRequestMessage, &message.GetShardInfoRequest{Body: b})
	err = c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	return callback.Data, nil
}

func (c *Client) getSnapshot(role Role, currentServer int, index uint64) (*meta2.Data, error) {
	c.logger.Debug("getting snapshot from start")

	callback := &SnapshotCallback{}
	msg := message.NewMetaMessage(message.SnapshotRequestMessage, &message.SnapshotRequest{Role: int(role), Index: index})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	c.logger.Debug("getting snapshot from end")

	if len(callback.Data) == 0 {
		return nil, nil
	}
	stat := statistics.NewMetaStatistics()
	stat.AddSnapshotTotal(1)
	stat.AddSnapshotDataSize(int64(len(callback.Data)))

	start := time.Now()
	data := &meta2.Data{}
	if err = data.UnmarshalBinary(callback.Data); err != nil {
		return nil, err
	}
	stat.AddSnapshotUnmarshalDuration(time.Since(start).Milliseconds())

	return data, nil
}

func (c *Client) getDownSampleInfo(currentServer int) ([]byte, error) {
	callback := &GetDownSampleInfoCallback{}
	msg := message.NewMetaMessage(message.GetDownSampleInfoRequestMessage, &message.GetDownSampleInfoRequest{})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	return callback.Data, nil
}

func (c *Client) getRpMstInfos(currentServer int, dbName, rpName string, dataTypes []int64) ([]byte, error) {
	callback := &GetRpMstInfoCallback{}
	msg := message.NewMetaMessage(message.GetRpMstInfosRequestMessage, &message.GetRpMstInfosRequest{
		DbName:    dbName,
		RpName:    rpName,
		DataTypes: dataTypes,
	})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	return callback.Data, nil
}

// Peers returns the TCPHost addresses of all the metaservers
func (c *Client) Peers() []string {

	var peers Peers
	// query each server and keep track of who their peers are
	for currentServer := range c.metaServers {
		callback := &PeersCallback{}
		msg := message.NewMetaMessage(message.PeersRequestMessage, &message.PeersRequest{})
		err := c.SendRPCMsg(currentServer, msg, callback)
		if err != nil {
			continue
		}
		//// This meta-server might not be ready to answer, continue on
		p := callback.Peers
		peers = peers.Append(p...)
	}

	// Return the unique set of peer addresses
	return []string(peers.Unique())
}

var connectedServer int

func (c *Client) retryUntilSnapshot(role Role, idx uint64) *meta2.Data {
	currentServer := connectedServer
	for {
		// get the index to look from and the server to poll
		c.mu.RLock()
		// exit if we're closed
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil
		default:
			// we're still open, continue on
		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		server := c.metaServers[currentServer]
		c.mu.RUnlock()

		data, err := c.getSnapshot(role, currentServer, idx)

		if err == nil && data != nil {
			return data
		} else if err == nil && data == nil {
			continue
		}

		c.logger.Debug("failure getting snapshot from", zap.String("server", server), zap.Error(err))
		time.Sleep(errSleep)

		currentServer++
	}
}

func (c *Client) retryUntilSnapshotV2(role Role, idx uint64) error {
	currentServer := connectedServer
	for {
		// get the index to look from and the server to poll
		c.mu.RLock()
		// exit if we're closed
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil
		default:
			// we're still open, continue on
		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		server := c.metaServers[currentServer]
		c.mu.RUnlock()

		dataOps, err := c.getDataOps(role, currentServer, idx)

		if err == nil && dataOps == nil {
			continue
		} else if err == nil && dataOps.Len() == 0 {
			return nil
		} else if err == nil && dataOps.Len() != 0 {
			if err := c.applyDataOps(dataOps.GetOps(), dataOps.GetIndex(), dataOps.MaxCQChangeID); err != nil {
				return err
			}
			return nil
		}

		c.logger.Debug("failure getting snapshot from", zap.String("server", server), zap.Error(err))
		time.Sleep(errSleep)

		currentServer++
	}
}

func (c *Client) getDataOps(role Role, currentServer int, index uint64) (*meta2.DataOps, error) {
	c.logger.Debug("getting snapshot from start")

	callback := &SnapshotV2Callback{}
	msg := message.NewMetaMessage(message.SnapshotV2RequestMessage, &message.SnapshotV2Request{Role: int(role), Index: index, Id: c.nodeID})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return nil, err
	}
	c.logger.Debug("getting snapshot from end")

	if len(callback.Data) == 0 {
		return nil, nil
	}
	stat := statistics.NewMetaStatistics()
	stat.AddSnapshotTotal(1)
	stat.AddSnapshotDataSize(int64(len(callback.Data)))

	start := time.Now()
	dataOps := &meta2.DataOps{}
	if err = dataOps.UnmarshalBinary(callback.Data); err != nil {
		return nil, err
	} else {
		if dataOps.Len() == 0 && dataOps.GetState() == int(meta2.NoClear) {
			c.mu.Lock()
			c.cacheData.Index = dataOps.GetIndex()
			c.cacheData.MaxCQChangeID = dataOps.MaxCQChangeID
			c.mu.Unlock()
		} else if dataOps.Len() == 0 && dataOps.GetState() == int(meta2.AllClear) {
			data := &meta2.Data{ExpandShardsEnable: c.cacheData.ExpandShardsEnable}
			dataPb := dataOps.GetData()
			data.Unmarshal(dataPb)
			c.mu.Lock()
			c.cacheData = data
			c.mu.Unlock()
		}
	}
	stat.AddSnapshotUnmarshalDuration(time.Since(start).Milliseconds())
	return dataOps, nil
}

func (c *Client) applyDataOps(dataOps []string, index uint64, MaxCQChangeID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cacheData.Index = index
	c.cacheData.MaxCQChangeID = MaxCQChangeID
	for _, op := range dataOps {
		opcmd := proto2.Command{}
		if err := proto.Unmarshal(util.Str2bytes(op), &opcmd); err != nil {
			return err
		}
		if handler, ok := applyFunc[*opcmd.Type]; ok {
			if err := handler(c, &opcmd); err != nil {
				if errno.Equal(err, errno.ApplyFuncErr) {
					c.logger.Error("sql apply dataops err", zap.String("msg", err.Error()))
					continue
				}
				return err
			}
		} else {
			panic(fmt.Errorf("cannot apply command: %x", *opcmd.Type))
		}
	}
	return nil
}

const (
	autoCreateRetentionPolicyName       = "autogen"
	autoCreateRetentionPolicyPeriod     = 0
	autoCreateRetentionPolicyWarmPeriod = 0
)

func applyCreateDatabase(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateDatabaseCommand_Command)
	v, ok := ext.(*proto2.CreateDatabaseCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateDatabaseCommand", ext))
	}

	var rp *meta2.RetentionPolicyInfo
	rpi := v.GetRetentionPolicy()
	repN := v.GetReplicaNum()
	if repN == 0 {
		repN = 1
	}
	if rpi != nil {
		rp = &meta2.RetentionPolicyInfo{
			Name:               rpi.GetName(),
			ReplicaN:           int(rpi.GetReplicaN()),
			Duration:           time.Duration(rpi.GetDuration()),
			ShardGroupDuration: time.Duration(rpi.GetShardGroupDuration()),
			HotDuration:        time.Duration(rpi.GetHotDuration()),
			WarmDuration:       time.Duration(rpi.GetWarmDuration()),
			IndexColdDuration:  time.Duration(rpi.GetIndexColdDuration()),
			IndexGroupDuration: time.Duration(rpi.GetIndexGroupDuration()),
			ShardMergeDuration: time.Duration(rpi.GetShardMergeDuration())}
	} else if c.RetentionAutoCreate {
		// Create a retention policy.
		rp = meta2.NewRetentionPolicyInfo(autoCreateRetentionPolicyName)
		rp.ReplicaN = int(repN)
		rp.Duration = autoCreateRetentionPolicyPeriod
		rp.WarmDuration = autoCreateRetentionPolicyWarmPeriod
	}
	// 1.create db pt view
	val := &proto2.CreateDbPtViewCommand{
		DbName:     v.Name,
		ReplicaNum: v.ReplicaNum,
	}
	t := proto2.Command_CreateDbPtViewCommand
	command := &proto2.Command{Type: &t}
	if err := proto.SetExtension(command, proto2.E_CreateDbPtViewCommand_Command, val); err != nil {
		panic(err)
	}
	err := c.cacheData.CreateDatabase(v.GetName(), rp, v.GetSki(), v.GetEnableTagArray(), repN, v.GetOptions())
	if err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyCreateDatabase2", err.Error())
	}
	return nil
}

func applyDropDatabase(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropDatabaseCommand_Command)
	v, ok := ext.(*proto2.DropDatabaseCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a DropDatabaseCommand", ext))
	}
	dbi := c.cacheData.Database(v.GetName())
	if dbi == nil {
		return nil
	}
	c.cacheData.DropDatabase(v.GetName())

	return nil
}

func applyCreateRetentionPolicy(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyCreateRetentionPolicy(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyCreateRetentionPolicy", err.Error())
	}
	return nil
}

func applyRecoverMetaData(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyRecoverMetaData(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyRecoverMetaData", err.Error())
	}
	return nil
}

func applyDropRetentionPolicy(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyDropRetentionPolicy(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyDropRetentionPolicy", err.Error())
	}
	return nil
}

func applySetDefaultRetentionPolicy(c *Client, cmd *proto2.Command) error {
	return meta2.ApplySetDefaultRetentionPolicy(c.cacheData, cmd)
}

func applyUpdateRetentionPolicy(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateRetentionPolicy(c.cacheData, cmd)
}

func applyCreateShardGroup(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyCreateShardGroup(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyCreateShardGroup", err.Error())
	}
	return nil
}

func applyDeleteShardGroup(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDeleteShardGroup(c.cacheData, cmd)
}

func applyCreateSubscription(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyCreateSubscription(c.cacheData, cmd)
}

func applyDropSubscription(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDropSubscription(c.cacheData, cmd)
}

func applyCreateUser(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyCreateUser(c.cacheData, cmd)
}

func applyDropUser(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDropUser(c.cacheData, cmd)
}

func applyUpdateUser(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateUser(c.cacheData, cmd)
}

func applySetPrivilege(c *Client, cmd *proto2.Command) error {
	return meta2.ApplySetPrivilege(c.cacheData, cmd)
}

func applySetAdminPrivilege(c *Client, cmd *proto2.Command) error {
	return meta2.ApplySetAdminPrivilege(c.cacheData, cmd)
}

func applySetData(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_SetDataCommand_Command)
	v, ok := ext.(*proto2.SetDataCommand)
	if !ok {
		c.logger.Error("applySetData err")
	}
	// Overwrite data.
	temData := &meta2.Data{}
	temData.Unmarshal(v.GetData())
	c.cacheData = temData

	return nil
}

func applyCreateMetaNode(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyCreateMetaNode(c.cacheData, cmd)
}

func applySetMetaNode(c *Client, cmd *proto2.Command) error {
	return meta2.ApplySetMetaNode(c.cacheData, cmd)
}

func applyDeleteMetaNode(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDeleteMetaNode(c.cacheData, cmd)
}

func applyCreateDataNode(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyCreateDataNode(c.cacheData, cmd)
}

func applyDeleteDataNode(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDeleteDataNode(c.cacheData, cmd)
}

func applyMarkDatabaseDelete(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyMarkDatabaseDelete(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyMarkDatabaseDelete", err.Error())
	}
	return nil
}

func applyCreateSqlNode(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateSqlNodeCommand_Command)
	v, ok := ext.(*proto2.CreateSqlNodeCommand)
	if !ok {
		c.logger.Error("applyCreateSqlNode err")
	}
	sqlNode := c.cacheData.SqlNodeByHttpHost(v.GetHTTPAddr())
	if sqlNode != nil {
		c.cacheData.MaxConnID++
		sqlNode.ConnID = c.cacheData.MaxConnID
		return nil
	}

	_, err := c.cacheData.CreateSqlNode(v.GetHTTPAddr(), v.GetGossipAddr())
	if err != nil {
		c.logger.Error("applyCreateSqlNode err", zap.Error(err))
	} else {
		c.logger.Info("applyCreateSqlNode success")
	}
	return err
}

func applyMarkRetentionPolicyDelete(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyMarkRetentionPolicyDelete(c.cacheData, cmd)
}

func applyCreateMeasurement(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyCreateMeasurement(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyCreateMeasurement", err.Error())
	}
	return nil
}

func applyReSharding(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyReSharding(c.cacheData, cmd)
}

func applyUpdateSchema(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyUpdateSchema(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyUpdateSchema", err.Error())
	}
	return nil
}

func applyAlterShardKey(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyAlterShardKey(c.cacheData, cmd)
}

func applyPruneGroups(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyPruneGroups(c.cacheData, cmd)
}

func applyMarkMeasurementDelete(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyMarkMeasurementDelete(c.cacheData, cmd)
}

func applyDropMeasurement(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDropMeasurement(c.cacheData, cmd)
}

func applyDeleteIndexGroup(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDeleteIndexGroup(c.cacheData, cmd)
}

func applyUpdateShardInfoTier(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateShardInfoTier(c.cacheData, cmd)
}

func applyUpdateIndexInfoTier(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateIndexInfoTier(c.cacheData, cmd)
}

func applyUpdateNodeStatus(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateNodeStatus(c.cacheData, cmd)
}

func applyUpdatePtInfo(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdatePtInfo(c.cacheData, cmd)
}

func applyCreateDownSample(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyCreateDownSample(c.cacheData, cmd)
}

func applyDropDownSample(c *Client, cmd *proto2.Command) error {
	if c.cacheData.Databases != nil {
		return meta2.ApplyDropDownSample(c.cacheData, cmd)
	}
	return nil
}

func applyUpdateSqlNodeStatus(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateSqlNodeStatusCommand_Command)
	v, ok := ext.(*proto2.UpdateSqlNodeStatusCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateSqlNodeStatusCommand", ext))
	}
	return c.cacheData.UpdateSqlNodeStatus(v.GetID(), v.GetStatus(), v.GetLtime(), v.GetGossipAddr())
}

func applyUpdateMetaNodeStatus(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_UpdateMetaNodeStatusCommand_Command)
	v, ok := ext.(*proto2.UpdateMetaNodeStatusCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a UpdateMetaNodeStatusCommand", ext))
	}
	return c.cacheData.UpdateMetaNodeStatus(v.GetID(), v.GetStatus(), v.GetLtime(), v.GetGossipAddr())
}

func applyCreateDbPtView(c *Client, cmd *proto2.Command) error {
	if err := meta2.ApplyCreateDbPtViewCommand(c.cacheData, cmd); err != nil {
		return errno.NewError(errno.ApplyFuncErr, "applyCreateDbPtView", err.Error())
	}
	return nil
}

func applyUpdateShardDownSampleInfo(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateShardDownSampleInfo(c.cacheData, cmd)
}

func applyCreateStream(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyCreateStream(c.cacheData, cmd)
}

func applyDropStream(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyDropStream(c.cacheData, cmd)
}

func applyExpandGroups(c *Client, cmd *proto2.Command) error {
	c.cacheData.ExpandGroups()
	return nil
}

func applyUpdatePtVersion(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdatePtVersion(c.cacheData, cmd)
}

func applyRegisterQueryIDOffset(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyRegisterQueryIDOffset(c.cacheData, cmd)
}

func applyCreateContinuousQuery(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_CreateContinuousQueryCommand_Command)
	v, ok := ext.(*proto2.CreateContinuousQueryCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a CreateContinuousQueryCommand", ext))
	}

	if _, err := c.cacheData.CreateContinuousQueryBase(v.GetDatabase(), v.GetName(), v.GetQuery()); err != nil {
		return err
	}
	return nil
}

func applyContinuousQueryReport(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyContinuousQueryReport(c.cacheData, cmd)
}

func applyDropContinuousQuery(c *Client, cmd *proto2.Command) error {
	ext, _ := proto.GetExtension(cmd, proto2.E_DropContinuousQueryCommand_Command)
	v, ok := ext.(*proto2.DropContinuousQueryCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a DropContinuousQueryCommand", ext))
	}
	changed, err := c.cacheData.DropContinuousQueryBase(v.GetName(), v.GetDatabase())
	if err != nil || !changed {
		return err
	}

	return nil
}

func applySetNodeSegregateStatus(c *Client, cmd *proto2.Command) error {
	return meta2.ApplySetNodeSegregateStatus(c.cacheData, cmd)
}

func applyRemoveNode(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyRemoveNode(c.cacheData, cmd)
}

func applyUpdateReplication(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateReplication(c.cacheData, cmd, nil)
}

func applyUpdateMeasurement(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyUpdateMeasurement(c.cacheData, cmd)
}

func applyReplaceMergeShardsCommand(c *Client, cmd *proto2.Command) error {
	return meta2.ApplyReplaceMergeShards(c.cacheData, cmd)
}

func (c *Client) RetryDownSampleInfo() ([]byte, error) {
	startTime := time.Now()
	currentServer := connectedServer
	var err error
	var downSampleInfo []byte
	for {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil, meta2.ErrClientClosed
		default:
		}
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()
		downSampleInfo, err = c.getDownSampleInfo(currentServer)
		if err == nil {
			break
		}
		if time.Since(startTime).Seconds() > float64(len(c.metaServers))*HttpReqTimeout.Seconds() {
			break
		}
		time.Sleep(errSleep)
		currentServer++
	}
	return downSampleInfo, err
}

func (c *Client) RetryMstInfosInRp(dbName, rpName string, dataTypes []int64) ([]byte, error) {
	startTime := time.Now()
	currentServer := connectedServer
	var err error
	var mstInfos []byte
	for {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil, meta2.ErrClientClosed
		default:
		}
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()
		mstInfos, err = c.getRpMstInfos(currentServer, dbName, rpName, dataTypes)
		if err == nil {
			break
		}
		if time.Since(startTime).Seconds() > float64(len(c.metaServers))*HttpReqTimeout.Seconds() {
			break
		}
		time.Sleep(errSleep)
		currentServer++
	}
	return mstInfos, err
}

func (c *Client) RetryGetShardAuxInfo(cmd *proto2.Command) ([]byte, error) {
	startTime := time.Now()
	currentServer := connectedServer
	var err error
	var shardAuxInfo []byte
	for {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil, meta2.ErrClientClosed
		default:

		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()
		shardAuxInfo, err = c.getShardInfo(currentServer, cmd)
		if err == nil {
			break
		}
		if errno.Equal(err, errno.ShardMetaNotFound) {
			c.logger.Error("get shard info failed", zap.String("cmd", cmd.String()), zap.Error(err))
			break
		}

		if time.Since(startTime).Nanoseconds() > int64(len(c.metaServers))*HttpReqTimeout.Nanoseconds() {
			c.logger.Error("get shard info timeout", zap.String("cmd", cmd.String()), zap.Error(err))
			break
		}
		c.logger.Error("retry get shard info", zap.String("cmd", cmd.String()), zap.Error(err))
		time.Sleep(errSleep)

		currentServer++
	}
	return shardAuxInfo, err
}

func (c *Client) GetShardRangeInfo(db string, rp string, shardID uint64) (*meta2.ShardTimeRangeInfo, error) {
	val := &proto2.TimeRangeCommand{
		Database: proto.String(db),
		Policy:   proto.String(rp),
		ShardID:  proto.Uint64(shardID),
	}

	t := proto2.Command_TimeRangeCommand
	cmd := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd, proto2.E_TimeRangeCommand_Command, val); err != nil {
		panic(err)
	}
	b, err := c.RetryGetShardAuxInfo(cmd)
	if err != nil {
		return nil, err
	}

	rangeInfo := &meta2.ShardTimeRangeInfo{}
	if err := rangeInfo.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return rangeInfo, err
}

func (c *Client) GetShardDurationInfo(index uint64) (*meta2.ShardDurationResponse, error) {
	val := &proto2.ShardDurationCommand{Index: proto.Uint64(index), Pts: nil, NodeId: proto.Uint64(c.nodeID)}
	t := proto2.Command_ShardDurationCommand
	cmd := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd, proto2.E_ShardDurationCommand_Command, val); err != nil {
		panic(err)
	}
	b, err := c.RetryGetShardAuxInfo(cmd)
	if err != nil {
		return nil, err
	}

	durationInfo := &meta2.ShardDurationResponse{}
	if err := durationInfo.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return durationInfo, nil
}

func (c *Client) GetIndexDurationInfo(index uint64) (*meta2.IndexDurationResponse, error) {
	val := &proto2.IndexDurationCommand{Index: proto.Uint64(index), Pts: nil, NodeId: proto.Uint64(c.nodeID)}
	t := proto2.Command_IndexDurationCommand
	cmd := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd, proto2.E_IndexDurationCommand_Command, val); err != nil {
		panic(err)
	}
	b, err := c.RetryGetShardAuxInfo(cmd)
	if err != nil {
		return nil, err
	}

	durationInfo := &meta2.IndexDurationResponse{}
	if err := durationInfo.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return durationInfo, nil
}

func (c *Client) MetaServers() []string {
	return c.metaServers
}

func (c *Client) InitMetaClient(joinPeers []string, tlsEn bool, storageNodeInfo *StorageNodeInfo, sqlNodeInfo *SqlNodeInfo, role string, t Role) (uint64, uint64, uint64, error) {
	// It's the first time starting up and we need to either join
	// the cluster or initialize this node as the first member
	if len(joinPeers) == 0 {
		// start up a new single node cluster
		return 0, 0, 0, fmt.Errorf("invlude meta nodes host")
	}

	// join this node to dthe cluster
	c.SetMetaServers(joinPeers)
	c.SetTLS(tlsEn)

	var nid uint64
	var err error
	var clock uint64
	var connId uint64
	if storageNodeInfo != nil {
		nid, clock, connId, err = c.CreateDataNode(storageNodeInfo, role)
	} else if t == SQL && sqlNodeInfo != nil && c.UseSnapshotV2 {
		nid, clock, connId, err = c.CreateSqlNode(sqlNodeInfo)
	}
	config.SetNodeId(nid)
	return nid, clock, connId, err
}

func (c *Client) retryReport(typ proto2.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	tries := 0
	currentServer := connectedServer
	timeout := time.After(RetryReportTimeout)

	for {
		c.mu.RLock()
		// exit if we're closed
		select {
		case <-timeout:
			c.mu.RUnlock()
			return meta2.ErrCommandTimeout
		case <-c.closing:
			c.mu.RUnlock()
			return nil
		default:
			// we're still open, continue on
		}
		c.mu.RUnlock()

		// build the url to hit the redirect server or the next metaserver
		var server string
		c.mu.RLock()
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		server = c.metaServers[currentServer]
		c.mu.RUnlock()

		_, err := c.exec(currentServer, typ, desc, value, message.ReportRequestMessage)

		if err == nil {
			return nil
		}
		tries++
		currentServer++

		if strings.Contains(err.Error(), "node is not the leader") {
			continue
		}

		if _, ok := err.(errCommand); ok {
			return err
		}
		c.logger.Info("retryUntilExec retry", zap.String("server", server), zap.Any("type", typ),
			zap.Int("tries", tries), zap.Error(err))
		time.Sleep(errSleep)
	}
}

func (c *Client) ReportShardLoads(dbPTStats []*proto2.DBPtStatus) error {
	cmd := &proto2.ReportShardsLoadCommand{
		DBPTStat: dbPTStats,
	}
	return c.retryReport(proto2.Command_ReportShardsCommand, proto2.E_ReportShardsLoadCommand_Command, cmd)
}

func (c *Client) Measurements(database string, ms influxql.Measurements) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	mstMaps, err := c.matchMeasurements(database, ms)
	if err != nil {
		return nil, err
	}
	var measurements []string
	for _, mi := range mstMaps {
		measurements = append(measurements, mi.OriginName())
	}

	if len(measurements) == 0 {
		return measurements, nil
	}
	sort.Strings(measurements)

	l := len(measurements)
	for i := 1; i < l; i++ {
		if measurements[i] == measurements[i-1] {
			l--
			measurements = append(measurements[:i-1], measurements[i:]...)
			i--
		}
	}
	return measurements, nil
}

func (c *Client) MatchMeasurements(database string, ms influxql.Measurements) (map[string]*meta2.MeasurementInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.matchMeasurements(database, ms)
}

func (c *Client) matchMeasurements(database string, ms influxql.Measurements) (map[string]*meta2.MeasurementInfo, error) {
	dbi, err := c.cacheData.GetDatabase(database)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]*meta2.MeasurementInfo)
	dbi.WalkRetentionPolicy(func(rp *meta2.RetentionPolicyInfo) {
		rp.MatchMeasurements(ms, ret)
	})

	return ret, nil
}

func (c *Client) QueryTagKeys(database string, ms influxql.Measurements, cond influxql.Expr) (map[string]map[string]struct{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	mis, err := c.matchMeasurements(database, ms)
	if err != nil {
		return nil, err
	}
	if len(mis) == 0 {
		return nil, nil
	}

	// split the condition by time and tag filter
	cond, tr, err := influxql.SplitCondByTime(cond)
	if err != nil {
		return nil, err
	}

	// map[measurement name] map[tag key] struct{}
	ret := make(map[string]map[string]struct{}, len(mis))
	for _, m := range mis {
		if _, ok := ret[m.Name]; !ok {
			ret[m.Name] = make(map[string]struct{})
		}
		m.FilterTagKeys(cond, tr, ret)
	}
	return ret, nil
}

func (c *Client) NewDownSamplePolicy(database, name string, info *meta2.DownSamplePolicyInfo) error {
	cmd := &proto2.CreateDownSamplePolicyCommand{
		Database:             proto.String(database),
		Name:                 proto.String(name),
		DownSamplePolicyInfo: info.Marshal(),
	}
	err := c.retryUntilExec(proto2.Command_CreateDownSamplePolicyCommand, proto2.E_CreateDownSamplePolicyCommand_Command, cmd)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) DropDownSamplePolicy(database, name string, dropAll bool) error {
	cmd := &proto2.DropDownSamplePolicyCommand{
		Database: proto.String(database),
		RpName:   proto.String(name),
		DropAll:  proto.Bool(dropAll),
	}
	err := c.retryUntilExec(proto2.Command_DropDownSamplePolicyCommand, proto2.E_DropDownSamplePolicyCommand_Command, cmd)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) ShowDownSamplePolicies(database string) (models.Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.ShowDownSamplePolicies(database)
}

func (c *Client) GetDownSamplePolicies() (*meta2.DownSamplePoliciesInfoWithDbRp, error) {
	b, err := c.RetryDownSampleInfo()
	if err != nil {
		return nil, err
	}
	return c.unmarshalDownSamplePolicies(b)
}

func (c *Client) unmarshalDownSamplePolicies(b []byte) (*meta2.DownSamplePoliciesInfoWithDbRp, error) {
	DownSample := &meta2.DownSamplePoliciesInfoWithDbRp{}
	if err := DownSample.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return DownSample, nil
}

func (c *Client) GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta2.RpMeasurementsFieldsInfo, error) {
	b, err := c.RetryMstInfosInRp(dbName, rpName, dataTypes)
	if err != nil {
		return nil, err
	}
	return c.unmarshalMstInfoWithInRp(b)
}

func (c *Client) unmarshalMstInfoWithInRp(b []byte) (*meta2.RpMeasurementsFieldsInfo, error) {
	DownSample := &meta2.RpMeasurementsFieldsInfo{}
	if err := DownSample.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return DownSample, nil
}

func (c *Client) UpdateShardDownSampleInfo(Ident *meta2.ShardIdentifier) error {
	val := &proto2.UpdateShardDownSampleInfoCommand{
		Ident: Ident.Marshal(),
	}
	if _, err := c.retryExec(proto2.Command_UpdateShardDownSampleInfoCommand, proto2.E_UpdateShardDownSampleInfoCommand_Command, val); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetStreamInfos() map[string]*meta2.StreamInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.Streams
}

// GetDstStreamInfos get the stream info whose db and rip of the data are the same as the db and rp of the source table of the stream
// Note: make sure dstSis is initialized
func (c *Client) GetDstStreamInfos(db, rp string, dstSis *[]*meta2.StreamInfo) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.cacheData.Streams) == 0 {
		return false
	}
	i := 0
	*dstSis = (*dstSis)[:cap(*dstSis)]
	for _, si := range c.cacheData.Streams {
		if si.SrcMst.Database == db && si.SrcMst.RetentionPolicy == rp {
			if len(*dstSis) < i+1 {
				*dstSis = append(*dstSis, si)
			} else {
				(*dstSis)[i] = si
			}
			i++
		}
	}
	*dstSis = (*dstSis)[:i]
	return len(*dstSis) > 0
}

func (c *Client) UpdateStreamMstSchema(database string, retentionPolicy string, mst string, stmt *influxql.SelectStatement) error {
	if interval, err := stmt.GroupByInterval(); err == nil && interval == 0 && len(stmt.Dimensions) == 0 {
		return c.updateStreamMstSchemaNoGroupBy(database, retentionPolicy, mst, stmt)
	} else if err != nil {
		return err
	}

	fieldToCreate := make([]*proto2.FieldSchema, 0, len(stmt.Fields)+len(stmt.Dimensions))
	fields := stmt.Fields
	for i := range fields {
		f, ok := fields[i].Expr.(*influxql.Call)
		if !ok {
			return errors.New("unexpected call function")
		}
		if fields[i].Alias == "" {
			fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
				FieldName: proto.String(f.Name + "_" + f.Args[0].(*influxql.VarRef).Val),
			})
		} else {
			fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
				FieldName: proto.String(fields[i].Alias),
			})
		}
		valuer := influxql.TypeValuerEval{
			TypeMapper: DefaultTypeMapper,
		}
		t, e := valuer.EvalType(f, false)
		if e != nil {
			return e
		}
		switch t {
		case influxql.Float:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_Float)
		case influxql.Integer:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_Int)
		case influxql.Boolean:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_Int)
		case influxql.String:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_String)
		default:
			return errors.New("unexpected call type")
		}
	}
	dims := stmt.Dimensions
	for i := range dims {
		d, ok := dims[i].Expr.(*influxql.VarRef)
		if !ok {
			continue
		}
		fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
			FieldName: proto.String(d.Val),
			FieldType: proto.Int32(influx.Field_Type_Tag),
		})
	}
	return c.UpdateSchema(database, retentionPolicy, mst, fieldToCreate)
}

func (c *Client) updateStreamMstSchemaNoGroupBy(database, retentionPolicy, mst string, stmt *influxql.SelectStatement) error {
	fieldToCreate := make([]*proto2.FieldSchema, 0, len(stmt.Fields)+len(stmt.Dimensions))
	fields := stmt.Fields
	for i := range fields {
		f, ok := fields[i].Expr.(*influxql.VarRef)
		if !ok {
			return errors.New("the filter-only stream task can contain only var_ref(tag or field)")
		}

		// tag
		if f.Type == influxql.Tag {
			fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
				FieldName: proto.String(f.Val),
				FieldType: proto.Int32(influx.Field_Type_Tag),
			})
			continue
		}

		// field
		if f.Alias == "" {
			fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
				FieldName: proto.String(f.Val),
			})
		} else {
			fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
				FieldName: proto.String(f.Alias),
			})
		}
		switch f.Type {
		case influxql.Float:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_Float)
		case influxql.Integer:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_Int)
		case influxql.Boolean:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_Boolean)
		case influxql.String:
			fieldToCreate[i].FieldType = proto.Int32(influx.Field_Type_String)
		default:
			return errors.New("unexpected field type")
		}
	}
	return c.UpdateSchema(database, retentionPolicy, mst, fieldToCreate)
}

func (c *Client) CreateStreamPolicy(info *meta2.StreamInfo) error {
	cmd := &proto2.CreateStreamCommand{
		StreamInfo: info.Marshal(),
	}
	err := c.retryUntilExec(proto2.Command_CreateStreamCommand, proto2.E_CreateStreamCommand_Command, cmd)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) CreateStreamMeasurement(info *meta2.StreamInfo, src, dest *influxql.Measurement, stmt *influxql.SelectStatement) error {
	_, err := c.Measurement(dest.Database, dest.RetentionPolicy, dest.Name)
	if err == nil || err != meta2.ErrMeasurementNotFound {
		return err
	}

	srcInfo, err := c.Measurement(src.Database, src.RetentionPolicy, src.Name)
	if err != nil {
		return err
	}
	var schemaInfo []*proto2.FieldSchema
	var colStoreInfo *meta2.ColStoreInfo
	if srcInfo.EngineType == config.COLUMNSTORE {
		var compactType string
		if config.IsLogKeeper() {
			compactType = "block"
		}
		keys := append(info.Dims, "time")
		colStoreInfo = meta2.NewColStoreInfo(keys, keys, nil, 0, compactType)
		tags := map[string]int32{}
		for _, v := range info.Dims {
			tags[v] = influx.Field_Type_Tag
		}
		fields := map[string]int32{}
		for _, v := range info.Calls {
			fields[v.Alias] = influx.Field_Type_Float
		}
		schemaInfo = meta2.NewSchemaInfo(tags, fields)
	}
	var shardKeyInfo *meta2.ShardKeyInfo
	if len(srcInfo.ShardKeys) > 0 {
		shardKeyInfo = &srcInfo.ShardKeys[0]
	}
	_, err = c.CreateMeasurement(dest.Database, dest.RetentionPolicy, dest.Name, shardKeyInfo, srcInfo.InitNumOfShards, nil, srcInfo.EngineType, colStoreInfo, schemaInfo, nil)
	if err != nil {
		return err
	}
	return c.UpdateStreamMstSchema(dest.Database, dest.RetentionPolicy, dest.Name, stmt)
}

func (c *Client) ShowStreams(database string, showAll bool) (models.Rows, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.ShowStreams(database, showAll)
}

func (c *Client) DropStream(name string) error {
	cmd := &proto2.DropStreamCommand{
		Name: proto.String(name),
	}
	if err := c.retryUntilExec(proto2.Command_DropStreamCommand, proto2.E_DropStreamCommand_Command, cmd); err != nil {
		return nil
	}
	return nil
}

var VerifyNodeEn = true

/*
case :
1.store1 network partition
2.assign dbpt to store2
3.store1 network recovery
4.move dbpt to store1, store1 already has this dbpt
5.write rows to dbpt, store1 has no lease and panic
attention:
if always recover lease in assign dbpt no matter db pt is already on this node
flush may write older data to disk, compaction may choose file which has already deleted
stop flush and compaction , then clear memtable may solve this problem, but need replay new logs
*/
func (c *Client) verifyDataNodeStatus(t time.Duration) {
	if !VerifyNodeEn {
		return
	}
	tries := 0
	triesPt := 0
	begin := time.Now()

	var check = func() {
		err := c.retryVerifyDataNodeStatus()
		if err == nil {
			tries = 0
			triesPt = 0
			return
		}
		if errno.Equal(err, errno.PtNotFound) {
			if triesPt == 0 {
				begin = time.Now()
			}
			triesPt++
			checkTime := config.GetStoreConfig().RetryPtCheckTime

			if time.Since(begin) >= (time.Duration(checkTime) * time.Minute) {
				c.logger.Error("Verify retry for pt", zap.String("duration", time.Since(begin).String()), zap.Error(err))
				c.Suicide(err)
			}
			return
		}
		tries++
		c.logger.Error("Verify retry for node", zap.Int("tries", tries), zap.Error(err))
		if tries >= 3 {
			c.Suicide(err)
		}
	}
	check()
	util.TickerRun(t, c.closing, check, func() {})
}

func (c *Client) retryVerifyDataNodeStatus() error {
	startTime := time.Now()
	currentServer := connectedServer
	var err error
	for {
		c.mu.RLock()
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil
		default:
		}

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()
		err = c.verifyDataNodeStatusCmd(currentServer, c.NodeID())
		if err == nil {
			break
		}
		c.logger.Debug("verify datanode status failed", zap.Uint64("node id", c.NodeID()), zap.Error(err), zap.Duration("duration", time.Since(startTime)))
		if time.Since(startTime).Seconds() > float64(len(c.metaServers))*HttpReqTimeout.Seconds() {
			break
		}
		time.Sleep(errSleep)

		currentServer++
	}
	return err
}

func (c *Client) verifyDataNodeStatusCmd(currentServer int, nodeID uint64) error {
	callback := &VerifyDataNodeStatusCallback{}
	msg := message.NewMetaMessage(message.VerifyDataNodeStatusRequestMessage, &message.VerifyDataNodeStatusRequest{NodeID: nodeID})
	return c.SendRPCMsg(currentServer, msg, callback)
}

func (c *Client) Suicide(err error) {
	c.logger.Error("Suicide for fault data node", zap.Error(err))
	time.Sleep(errSleep)
	sysinfo.Suicide()
}

func (c *Client) TagArrayEnabled(db string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.cacheData.Databases[db]; !ok {
		return false
	}

	return c.cacheData.Databases[db].EnableTagArray
}

// RetryRegisterQueryIDOffset send a register rpc to ts-meta，request a query id offset
func (c *Client) RetryRegisterQueryIDOffset(host string) (uint64, error) {
	startTime := time.Now()
	currentServer := connectedServer
	var offset uint64
	var ok bool
	var err error

	for {
		c.mu.RLock()

		if offset, ok = c.cacheData.QueryIDInit[meta2.SQLHost(host)]; ok {
			c.logger.Info("current host has already registered in ts-meta")
			c.mu.RUnlock()
			return offset, nil
		}

		select {
		case <-c.closing:
			c.mu.RUnlock()
			return 0, meta2.ErrClientClosed
		default:
			// we're still open, continue on
		}
		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		c.mu.RUnlock()

		offset, err = c.registerOffset(currentServer, host)
		if err == nil {
			return offset, nil
		}

		currentServer++

		if strings.Contains(err.Error(), "node is not the leader") {
			continue
		}

		if time.Since(startTime).Seconds() > float64(len(c.metaServers))*HttpReqTimeout.Seconds() {
			return 0, errors.New("register query id offset timeout")
		}
		time.Sleep(errSleep)
	}
}

func (c *Client) registerOffset(currentServer int, host string) (uint64, error) {
	callback := &RegisterQueryIDOffsetCallback{}
	msg := message.NewMetaMessage(message.RegisterQueryIDOffsetRequestMessage, &message.RegisterQueryIDOffsetRequest{
		Host: host})
	err := c.SendRPCMsg(currentServer, msg, callback)
	if err != nil {
		return 0, err
	}
	return callback.Offset, nil
}

func (c *Client) ThermalShards(dbName string, start, end time.Duration) map[uint64]struct{} {
	startTime := time.Now()
	shards := make(map[uint64]struct{})
	var lt, rt time.Time

	if start != 0 {
		lt = time.Now().Add(-start).UTC()
	}
	if end != 0 {
		rt = time.Now().Add(end).UTC()
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	db, ok := c.cacheData.Databases[dbName]
	if !ok {
		return nil
	}

	for _, rp := range db.RetentionPolicies {
		if lt.IsZero() {
			lt = time.Now().Add(-rp.ShardGroupDuration).UTC()
		}
		if rt.IsZero() {
			rt = time.Now().Add(rp.ShardGroupDuration).UTC()
		}
		for i := 0; i < len(rp.ShardGroups); i++ {
			if rp.ShardGroups[i].Deleted() {
				continue
			}
			sg := &rp.ShardGroups[i]
			if sg.EndTime.Before(lt) || sg.EndTime.After(rt) {
				continue
			}
			for k := 0; k < len(sg.Shards); k++ {
				shards[sg.Shards[k].ID] = struct{}{}
			}
		}
	}
	c.logger.Info("thermal shards", zap.String("db", dbName), zap.Time("start", lt), zap.Time("end", rt),
		zap.Any("shards", shards), zap.Duration("duration", time.Since(startTime)))
	return shards
}

func (c *Client) UpdateMeasurement(db, rp, mst string, options *meta2.Options) error {
	_, err := c.Measurement(db, rp, mst)
	if err != nil {
		return err
	}
	cmd := &proto2.UpdateMeasurementCommand{
		Db:      proto.String(db),
		Rp:      proto.String(rp),
		Mst:     proto.String(mst),
		Options: options.Marshal(),
	}

	err = c.retryUntilExec(proto2.Command_UpdateMeasurementCommand, proto2.E_UpdateMeasurementCommand_Command, cmd)
	if err != nil {
		return err
	}

	return nil
}

// this function is used for UT testing
func (c *Client) SetCacheData(cacheData *meta2.Data) {
	c.cacheData = cacheData
}

func (c *Client) GetShardGroupByTimeRange(repoName, streamName string, min, max time.Time) ([]*meta2.ShardGroupInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	db, err := c.cacheData.GetDatabase(repoName)
	if err != nil {
		return nil, err
	}
	if db.MarkDeleted {
		return nil, nil
	}
	rp, err := db.GetRetentionPolicy(streamName)
	if err != nil {
		return nil, err
	}
	if rp.MarkDeleted {
		return nil, nil
	}
	sg := rp.ShardGroupsByTimeRange(min, max)

	return sg, nil
}

func (c *Client) IsMasterPt(ptId uint32, database string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbRgs, ok := c.cacheData.ReplicaGroups[database]
	if !ok {
		return false
	}
	for _, rg := range dbRgs {
		if rg.MasterPtID == ptId {
			return true
		}
	}
	return false
}

func (c *Client) ReplaceMergeShards(mergeShards meta2.MergeShards) error {
	cmd := &proto2.ReplaceMergeShardsCommand{
		Db:   proto.String(mergeShards.DbName),
		PtId: proto.Uint32(mergeShards.PtId),
		Rp:   proto.String(mergeShards.RpName),
	}
	cmd.ShardId = append(cmd.ShardId, mergeShards.ShardIds...)

	if err := c.retryUntilExec(proto2.Command_ReplaceMergeShardsCommand, proto2.E_ReplaceMergeShardsCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetTimeRange(db, rp string, sShardId, eShardId uint64) (*meta2.ShardTimeRangeInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbInfo, err := c.cacheData.GetDatabase(db)
	if err != nil {
		return nil, err
	}
	rpInfo, err := dbInfo.GetRetentionPolicy(rp)
	if err != nil {
		return nil, err
	}
	sShardTimeRangeInfo := rpInfo.ShardTimeRangeInfo(sShardId)
	if sShardTimeRangeInfo == nil {
		return nil, errno.NewError(errno.ShardMetaNotFound, sShardId)
	}
	eShardTimeRangeInfo := rpInfo.ShardTimeRangeInfo(eShardId)
	if eShardTimeRangeInfo == nil {
		return sShardTimeRangeInfo, nil
	}
	sShardTimeRangeInfo.TimeRange.EndTime = eShardTimeRangeInfo.TimeRange.EndTime
	return sShardTimeRangeInfo, nil
}

func (c *Client) GetNoClearShardId(shardId uint64, db string, shardGroupId uint64, policy string) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.cacheData.Databases[db]
	if !ok {
		return 0, errno.NewError(errno.DatabaseNotFound)
	}

	policyInfo, ok := info.RetentionPolicies[policy]
	if !ok {
		return 0, errno.NewError(errno.PtNotFound)
	}

	indexId, shardGroupInfo, err := getIndexId(shardId, policyInfo, shardGroupId)
	if err != nil {
		return 0, err
	}

	indexGroup, err := getIndexGroup(policyInfo, indexId)
	if err != nil {
		return 0, err
	}

	infos := indexGroup.ClearInfo
	if infos == nil {
		return 0, errors.New("clear info is nil")
	}

	noClearIndexId, err := getNoClearIndexId(infos, indexId)
	if err != nil {
		return 0, err
	}
	for i := range shardGroupInfo.Shards {
		if shardGroupInfo.Shards[i].IndexID == noClearIndexId {
			return shardGroupInfo.Shards[i].ID, nil
		}
	}
	return 0, errno.NewError(errno.DataIsOlder)
}

func (c *Client) GetNoClearIndexId(indexId uint64, db string, policy string) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.cacheData.Databases[db]
	if !ok {
		return 0, errno.NewError(errno.DatabaseNotFound)
	}

	policyInfo, ok := info.RetentionPolicies[policy]
	if !ok {
		return 0, errno.NewError(errno.PtNotFound)
	}

	indexGroup, err := getIndexGroup(policyInfo, indexId)
	if err != nil {
		return 0, err
	}

	infos := indexGroup.ClearInfo
	if infos == nil {
		return 0, errors.New("clear info is nil")
	}

	return getNoClearIndexId(infos, indexId)
}

func getNoClearIndexId(info *meta2.ReplicaClearInfo, indexId uint64) (uint64, error) {
	for j := range info.ClearPeers {
		if info.ClearPeers[j] == indexId {
			return info.NoClearIndexId, nil
		}
	}
	return 0, errors.New("NoClearIndexId not found")
}

func getIndexGroup(policyInfo *meta2.RetentionPolicyInfo, indexId uint64) (meta2.IndexGroupInfo, error) {
	for i := range policyInfo.IndexGroups {
		for j := range policyInfo.IndexGroups[i].Indexes {
			if policyInfo.IndexGroups[i].Indexes[j].ID == indexId {
				return policyInfo.IndexGroups[i], nil
			}
		}
	}
	return meta2.IndexGroupInfo{}, errors.New("index groups not found")
}

func getIndexId(shardId uint64, policyInfo *meta2.RetentionPolicyInfo, shardGroupId uint64) (uint64, meta2.ShardGroupInfo, error) {
	for i := range policyInfo.ShardGroups {
		if policyInfo.ShardGroups[i].ID == shardGroupId {
			shardInfo := policyInfo.ShardGroups[i].Shard(shardId)
			if shardInfo != nil {
				return shardInfo.IndexID, policyInfo.ShardGroups[i], nil
			}
		}
	}
	return 0, meta2.ShardGroupInfo{}, errors.New("index id not found")
}

func (c *Client) GetAllMstTTLInfo() map[string]map[string][]*meta2.MeasurementTTLTnfo {
	measurementTTLInfos := make(map[string]map[string][]*meta2.MeasurementTTLTnfo)
	dataBases := c.Databases()
	for dbName, db := range dataBases {
		if db == nil || db.MarkDeleted || db.RetentionPolicies == nil {
			continue
		}
		for rpName, rp := range db.RetentionPolicies {
			if rp == nil || rp.MarkDeleted || rp.Measurements == nil {
				continue
			}
			for _, mst := range rp.Measurements {
				if mst == nil || mst.MarkDeleted {
					continue
				}
				if measurementTTLInfos[dbName] == nil {
					measurementTTLInfos[dbName] = make(map[string][]*meta2.MeasurementTTLTnfo)
				}
				mstTTLInfo := &meta2.MeasurementTTLTnfo{
					Name:        mst.Name,
					OriginName:  mst.OriginName(),
					MarkDeleted: mst.MarkDeleted,
				}
				if mst.Options != nil {
					mstTTLInfo.TTL = mst.Options.Ttl
				}
				measurementTTLInfos[dbName][rpName] = append(measurementTTLInfos[dbName][rpName], mstTTLInfo)
			}
		}
	}
	return measurementTTLInfos
}

func (c *Client) GetMergeShardsList() []meta2.MergeShards {
	now := time.Now().UTC()
	c.mu.RLock()
	defer c.mu.RUnlock()
	mergeShardss := make([]meta2.MergeShards, 0)
	mergeShardGroup := make(map[uint32]map[int64][]meta2.ShardIdentifier) // <ptId, <mergeEndTime, [shard, shard...]>>
	for db := range c.cacheData.Databases {
		for rp, rpInfo := range c.cacheData.Databases[db].RetentionPolicies {
			// 1.get mergeShardGroup of each db.rp based on mergeDuration and shardGroupDuration
			mergeDuration := rpInfo.ShardMergeDuration
			if mergeDuration == 0 {
				continue
			}
			for _, sg := range c.cacheData.Databases[db].RetentionPolicies[rp].ShardGroups {
				mergeEndTime := sg.StartTime.Truncate(mergeDuration).UnixNano() + mergeDuration.Nanoseconds()
				for _, shard := range sg.Shards {
					_, ok := mergeShardGroup[shard.Owners[0]]
					if !ok {
						mergeShardGroup[shard.Owners[0]] = make(map[int64][]meta2.ShardIdentifier)
						mergeShardGroup[shard.Owners[0]][mergeEndTime] = make([]meta2.ShardIdentifier, 0)
					}
					if mergeEndTime+(sg.EndTime.UnixNano()-sg.StartTime.UnixNano()) < now.UnixNano() {
						mergeShardGroup[shard.Owners[0]][mergeEndTime] = append(mergeShardGroup[shard.Owners[0]][mergeEndTime],
							meta2.ShardIdentifier{ShardID: shard.ID, StartTime: sg.StartTime, EndTime: sg.EndTime, EngineType: uint32(sg.EngineType)})
					}
				}
			}

			// 2.add mergeShardGroup to mergeShardsList and return
			for ptId, mergeShards := range mergeShardGroup {
				for endTime, shards := range mergeShards {
					if len(shards) == 0 {
						continue
					}
					shardIds := make([]uint64, 0, len(shards))
					shardEndTimes := make([]int64, 0, len(shards))
					engineTypes := make([]config.EngineType, len(shards))
					for _, shard := range shards {
						shardIds = append(shardIds, shard.ShardID)
						shardEndTimes = append(shardEndTimes, shard.EndTime.UnixNano())
						engineTypes = append(engineTypes, config.EngineType(shard.EngineType))
					}
					mergeShardss = append(mergeShardss, meta2.MergeShards{
						DbName:        db,
						PtId:          ptId,
						RpName:        rp,
						ShardIds:      shardIds,
						ShardEndTimes: shardEndTimes,
						EngineType:    engineTypes,
					})
					mergeShards[endTime] = mergeShards[endTime][:0]
				}
			}
		}
	}
	return mergeShardss
}
