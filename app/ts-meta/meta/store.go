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
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"path"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	mclient "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	sp "github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	mproto "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

// Retention policy settings.
const (
	autoCreateRetentionPolicyName            = "autogen"
	autoCreateRetentionPolicyPeriod          = 0
	autoCreateRetentionPolicyWarmPeriod      = 0
	autoCreateRetentionPolicyIndexColdPeriod = 0

	reportTimeSpan = 10 * time.Second

	checkInterval = 500 * time.Millisecond

	updateCacheInterval = 100 * time.Millisecond

	pushInterval = 10 * time.Second

	clearCapacityStatMapInterval = 24 * time.Hour

	// Default sqlite database
	DefaultDatabase = "sqlite.db"
)

// Raft configuration.
const (
	raftListenerStartupTimeout = time.Second
	UpdateTmpIndexGap          = 20
)

const (
	DeleteDatabase = iota
	DeleteRp
	DeleteMeasurement
)

const categoryDatanode = "datanode"
const categoryMetadata = "metanode"
const categoryParam = "param"

var (
	CapacityStatMap       = &sync.Map{}
	statRetryTimes        = 3
	statConcurrency       = cpu.GetCpuNum() * 2
	statErrInfo           = "no such file"
	checkSegregateTimeout = 60 * time.Second
)

type CapacityStat struct {
	Capacity   int64
	UpdateTime time.Time
}

type ShardStat struct {
	id          uint64
	ownerPT     uint32
	shardSize   uint64
	seriesCount int32
	maxTime     int64
	reportTime  time.Time
}

func (ss ShardStat) String() string {
	return fmt.Sprintf("id %d, shardSize %d, ownerPT %d, seriesCount %d, maxTime %d, reportTime %+v\n", ss.id, ss.shardSize,
		ss.ownerPT, ss.seriesCount, ss.maxTime, ss.reportTime)
}

type dbInfo struct {
	rpMu         sync.RWMutex
	rpStatistics map[string]*rpInfo
	logger       *logger.Logger
	store        *Store
}

func (dbinfo *dbInfo) updateReportTime(ptId int) {
	dbinfo.rpMu.RLock()
	defer dbinfo.rpMu.RUnlock()
	for _, rpinfo := range dbinfo.rpStatistics {
		if len(rpinfo.shardStat) > ptId {
			rpinfo.shardStat[ptId].reportTime = time.Now()
		}
	}
}

func (dbinfo *dbInfo) getRpInfo(rpName string) *rpInfo {
	dbinfo.rpMu.Lock()
	defer dbinfo.rpMu.Unlock()
	rpinfo, ok := dbinfo.rpStatistics[rpName]
	if !ok {
		rpinfo = &rpInfo{}
		rpinfo.logger = dbinfo.logger
		rpinfo.store = dbinfo.store
		dbinfo.rpStatistics[rpName] = rpinfo
	}
	return rpinfo
}

type rpInfo struct {
	minShardID         uint64
	maxShardID         uint64
	reSharding         bool
	shardStat          []*ShardStat
	logger             *logger.Logger
	mu                 sync.RWMutex
	lastSeriesCount    int32
	lastRowCount       uint64
	currentSeriesCount int32
	currentRowCount    uint64
	store              *Store
}

func (rpi *rpInfo) getShardStat(id uint64) *ShardStat {
	idx := sort.Search(len(rpi.shardStat), func(i int) bool {
		return id <= rpi.shardStat[i].id
	})

	if idx < len(rpi.shardStat) && rpi.shardStat[idx].id == id {
		return rpi.shardStat[idx]
	}
	return nil
}

func (rpi *rpInfo) updateShardStat(shardId uint64, ptid uint32, shStat *mproto.ShardStatus) uint64 {
	rpi.mu.RLock()
	defer rpi.mu.RUnlock()
	shardStat := rpi.getShardStat(shardId)
	if shardStat == nil {
		if ptid <= uint32(rpi.maxShardID-rpi.minShardID) && ptid < uint32(len(rpi.shardStat)) {
			rpi.shardStat[ptid].reportTime = time.Now()
		}
		return 0
	}
	shardStat.reportTime = time.Now()
	shardStat.maxTime = shStat.GetMaxTime()
	shardStat.seriesCount = shStat.GetSeriesCount()
	shardStat.shardSize = shStat.GetShardSize()
	shardStat.ownerPT = ptid
	return shardStat.shardSize
}

func (rpi *rpInfo) createShardStat() {
	rpi.mu.Lock()
	defer rpi.mu.Unlock()
	shardsNum := rpi.maxShardID - rpi.minShardID + 1
	if len(rpi.shardStat) >= int(shardsNum) {
		rpi.shardStat = rpi.shardStat[:shardsNum]
	} else {
		num := len(rpi.shardStat)
		for cap(rpi.shardStat) > len(rpi.shardStat) {
			rpi.shardStat = rpi.shardStat[:len(rpi.shardStat)+1]
			if rpi.shardStat[len(rpi.shardStat)-1] == nil {
				rpi.shardStat[len(rpi.shardStat)-1] = &ShardStat{}
			}
			num++
			if num == int(shardsNum) {
				break
			}
		}

		for num < int(shardsNum) {
			rpi.logger.Info("create shard stat", zap.Int("num", num), zap.Uint64("shardsNum", shardsNum))
			rpi.shardStat = append(rpi.shardStat, &ShardStat{})
			num++
		}
	}

	idx := 0
	for id := rpi.minShardID; id <= rpi.maxShardID; id++ {
		rpi.shardStat[idx].id = id
		rpi.shardStat[idx].shardSize = 0
		rpi.shardStat[idx].seriesCount = 0
		rpi.shardStat[idx].ownerPT = math.MaxUint32
		rpi.shardStat[idx].reportTime = time.Now()
		rpi.shardStat[idx].maxTime = 0
		rpi.logger.Info("rpi shardStat", zap.Int("idx", idx), zap.Any("stat", rpi.shardStat[idx]))
		idx++
	}
	rpi.reSharding = false
}

func (rpi *rpInfo) checkReSharding(ptNum int, imbalanceThreshold float64) int64 {
	rpi.mu.Lock()
	defer rpi.mu.Unlock()
	if rpi.reSharding {
		return 0
	}

	var maxTime int64
	var maxLoad uint64
	var maxSeriesCount int32
	minReportTime := rpi.shardStat[0].reportTime
	maxReportTime := rpi.shardStat[0].reportTime
	for i := range rpi.shardStat {
		// haven't receive total load from store
		if rpi.shardStat[i].ownerPT == math.MaxUint32 {
			return 0
		}
		if rpi.shardStat[i].maxTime > maxTime {
			maxTime = rpi.shardStat[i].maxTime
		}

		if rpi.shardStat[i].reportTime.After(maxReportTime) {
			maxReportTime = rpi.shardStat[i].reportTime
		}

		if rpi.shardStat[i].reportTime.Before(minReportTime) {
			minReportTime = rpi.shardStat[i].reportTime
		}

		if maxSeriesCount < rpi.shardStat[i].seriesCount {
			maxSeriesCount = rpi.shardStat[i].seriesCount
		}
		rpi.currentSeriesCount += rpi.shardStat[i].seriesCount
		currSize := rpi.shardStat[i].shardSize
		rpi.currentRowCount += currSize
		if currSize > maxLoad {
			maxLoad = currSize
		}
		rpi.logger.Info("shard load", zap.Uint64("shardId", rpi.shardStat[i].id),
			zap.Uint64("load", rpi.shardStat[i].shardSize), zap.Int32("series count", rpi.shardStat[i].seriesCount),
			zap.Time("maxTime", time.Unix(0, maxTime)))
	}

	if maxReportTime.Sub(minReportTime) > reportTimeSpan {
		return 0
	}

	if len(rpi.shardStat) == ptNum {
		if rpi.currentSeriesCount < rpi.lastSeriesCount || rpi.currentRowCount < rpi.lastRowCount {
			return 0
		}
		if float64(maxLoad*uint64(len(rpi.shardStat))) < (1+imbalanceThreshold)*float64(rpi.currentRowCount) ||
			rpi.currentSeriesCount == maxSeriesCount*int32(len(rpi.shardStat)) {
			return 0
		}
	}

	rpi.reSharding = true
	return maxTime
}

func (rpi *rpInfo) getSplitVectorByRowCount(db string, shardNum int, data *meta.Data) []string {
	splitPoints := make([]string, shardNum-1)
	keyCount := int(rpi.currentRowCount)/shardNum + 1
	sum := 0
	index := 1
	hasErr := false
	wg := sync.WaitGroup{}
	splitIdx := 0
	for i := range rpi.shardStat {
		lastSum := sum
		sum += int(rpi.shardStat[i].shardSize)
		if sum < index*keyCount {
			continue
		}

		var idxes []int64
		for sum >= index*keyCount {
			idx := index*keyCount - lastSum
			idxes = append(idxes, int64(idx))
			index++
			if index == shardNum {
				break
			}
		}
		wg.Add(1)
		go func(ptID uint32, shardID uint64, idxes []int64, startIdx int) {
			defer wg.Done()
			node := data.DataNode(data.PtView[db][ptID].Owner.NodeID)
			splits, err := rpi.store.NetStore.GetShardSplitPoints(node, db, ptID, shardID, idxes)
			if err != nil {
				rpi.logger.Warn("get split points failed", zap.Error(err))
				hasErr = true
			}

			for j := range splits {
				splitPoints[startIdx+j] = splits[j]
			}
		}(rpi.shardStat[i].ownerPT, rpi.shardStat[i].id, idxes, splitIdx)

		splitIdx += len(idxes)
		if index == shardNum {
			break
		}
	}

	wg.Wait()
	if hasErr {
		return nil
	}

	return splitPoints
}

func (rpi *rpInfo) resetReSharding(err error) {
	rpi.mu.Lock()
	defer rpi.mu.Unlock()
	if err == nil {
		rpi.lastRowCount = rpi.currentRowCount
		rpi.lastSeriesCount = rpi.currentSeriesCount
	}
	rpi.currentSeriesCount = 0
	rpi.currentRowCount = 0
	rpi.reSharding = false
}

func (rpi *rpInfo) String() string {
	return fmt.Sprintf("minShardID %d, maxShardID %d, reSharding %t, shardStats %+v", rpi.minShardID,
		rpi.maxShardID, rpi.reSharding, rpi.shardStat)
}

type RaftInterface interface {
	State() raft.RaftState
	Peers() ([]string, error)
	Close() error
	IsLeader() bool
	IsCandidate() bool
	Leader() string
	Apply(b []byte) error
	AddServer(addr string) error
	ShowDebugInfo(witch string) ([]byte, error)
	UserSnapshot() error
	LeadershipTransfer() error
}

type Store struct {
	mu          sync.RWMutex
	segregateMu sync.Mutex
	closing     chan struct{}

	config *config.Meta

	data        *meta.Data
	dataRecover *meta.DataRecover
	dataChanged chan struct{}
	path        string
	opened      bool
	Logger      *logger.Logger
	raft        RaftInterface
	notifyCh    chan bool
	wg          sync.WaitGroup
	deleteWg    sync.WaitGroup
	stepDown    chan struct{}
	cm          *ClusterManager

	httpAddr string
	rpcAddr  string
	raftAddr string

	Node     *mclient.Node
	NetStore interface {
		GetShardSplitPoints(node *meta.DataNode, database string, pt uint32, shardId uint64, idxes []int64) ([]string, error)
		DeleteDatabase(node *meta.DataNode, database string, pt uint32) error
		DeleteRetentionPolicy(node *meta.DataNode, db string, rp string, pt uint32) error
		DeleteMeasurement(node *meta.DataNode, db string, rp string, name string, shardIds []uint64) error
		MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error
		SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error)
		TransferLeadership(database string, nodeId uint64, oldMasterPtId, newMasterPtId uint32) error
		SendClearEvents(nodeId uint64, data transport.Codec) error
	}

	statMu       sync.RWMutex
	dbStatistics map[string]*dbInfo
	client       *mclient.Client

	cacheMu          sync.RWMutex
	cacheData        *meta.Data
	cacheDataBytes   []byte
	cacheDataChanged chan struct{}

	// for continuous query
	cqLock            sync.RWMutex            // lock for all cq related items
	cqNames           []string                // sorted cq names. Restore from data unmarshal.
	heartbeatInfoList *list.List              // the latest heartbeat information for each ts-sql
	cqLease           map[string]*cqLeaseInfo // sql host to cq lease.
	sqlHosts          []string                // sorted hostname ["127.0.0.1:8086", "127.0.0.2:8086", "127.0.0.3:8086"]
	UseIncSyncData    bool
	GossipConfig      *config.Gossip
}

// NewStore will create a new metaStore with the passed in config
func NewStore(c *config.Meta, httpAddr, rpcAddr, raftAddr string) *Store {
	s := Store{
		data: &meta.Data{
			Index:                          1,
			PtNumPerNode:                   c.PtNumPerNode,
			TakeOverEnabled:                true,
			BalancerEnabled:                true,
			NumOfShards:                    c.NumOfShards,
			UpdateNodeTmpIndexCommandStart: 1,
		},
		cacheData:        &meta.Data{},
		closing:          make(chan struct{}),
		dataChanged:      make(chan struct{}),
		cacheDataChanged: make(chan struct{}),
		path:             c.Dir,
		config:           c,
		httpAddr:         httpAddr,
		rpcAddr:          rpcAddr,
		raftAddr:         raftAddr,
		dbStatistics:     make(map[string]*dbInfo),
		notifyCh:         make(chan bool, 1),

		// for continuous query
		heartbeatInfoList: list.New(),
		cqLease:           make(map[string]*cqLeaseInfo),
	}
	if c.UseIncSyncData {
		s.data.OpsMap = make(map[uint64]*meta.Op)
		s.data.OpsMapMinIndex = math.MaxUint64
		s.data.OpsMapMaxIndex = 0
		s.UseIncSyncData = true
	}
	meta.InitSchemaCleanEn(c.SchemaCleanEn)
	config.MetaEventHandleEn = c.MetaEventHandleEn
	return &s
}

func (s *Store) checkLeaderChanged() {
	defer s.wg.Done()
	pushTimer := time.NewTimer(pushInterval)
	defer pushTimer.Stop()
	lastState := s.raft.State()
	stat.NewMetaStatCollector().Push(&stat.MetaRaftStatItem{Status: int64(lastState),
		NodeID: strconv.FormatUint(s.Node.ID, 10)})
	for {
		select {
		case v := <-s.notifyCh:
			stat.NewMetaStatistics().AddLeaderSwitchTotal(1)
			s.Logger.Info("accept from notifyCh ", zap.Bool("v", v))
			if v {
				s.stepDown = make(chan struct{})
				if globalService.clusterManager != nil {
					s.Logger.Info("accept from notifyCh start msm , cluster manager", zap.Bool("v", v))
					globalService.msm.Start()
					globalService.balanceManager.Start()
					globalService.clusterManager.Start()
					globalService.masterPtBalanceManager.Start()
				}

				s.deleteWg.Add(3)
				go s.checkDelete(DeleteDatabase)
				go s.checkDelete(DeleteRp)
				go s.checkDelete(DeleteMeasurement)
				go s.RepairPT(time.Minute)
				continue
			}

			stat.NewMetaStatCollector().Clear(stat.TypeMetaStatItem)
			close(s.stepDown)
			if globalService.clusterManager != nil {
				s.Logger.Info("accept from notifyCh stop msm , cluster manager", zap.Bool("v", v))
				globalService.clusterManager.Stop()
				globalService.balanceManager.Stop()
				globalService.masterPtBalanceManager.Stop()
				globalService.msm.Stop()
			}
			s.deleteWg.Wait()
		case <-s.closing:
			return
		case <-pushTimer.C:
			currentState := s.raft.State()
			if currentState != lastState {
				stat.NewMetaStatCollector().Push(&stat.MetaRaftStatItem{Status: int64(currentState),
					NodeID: strconv.FormatUint(s.Node.ID, 10)})
				lastState = currentState
			}
			pushTimer.Reset(pushInterval)
		}
	}
}

func (s *Store) SetClusterManager(cm *ClusterManager) {
	s.Logger.Info("set cluster manager")
	s.cm = cm
}

func (s *Store) GetClusterManager() *ClusterManager {
	return s.cm
}

// open opens and initializes the raft Store.
func (s *Store) Open(raftln net.Listener) error {
	s.Logger.Info("Using data ", zap.String("dir", s.path), zap.Uint64("split threshold",
		s.config.SplitRowThreshold), zap.Float64("imbalance factor", s.config.ImbalanceFactor))

	c := s.makeClient()
	s.client = c
	peers := s.connectFull(c)
	if err := s.setOpen(); err != nil {
		return err
	}

	var err error

	tmp := s.data.NumOfShards
	if len(peers) < len(s.config.BindPeers) && len(s.config.BindPeers) > 0 {
		s.Logger.Info("Using config.BindPeers open raft", zap.Any("BindPeers", s.config.BindPeers))
		peers = s.config.BindPeers
	}
	err = s.newRaftWrapper(raftln, peers)
	if err != nil {
		return err
	}
	// set NumOfShards based on latest configuration
	s.data.NumOfShards = tmp

	if s.config.SQLiteEnabled {
		s.data.SQLite, err = meta.NewSQLiteWrapper(s.config.Dir + "/" + DefaultDatabase + "?cache=shared")
		if err != nil {
			return err
		}
	}

	if err = s.waitForLeader(); err != nil {
		return err
	}

	if err = s.joinMetaServer(c); err != nil {
		return err
	}

	s.wg.Add(5)
	if s.config.UseIncSyncData {
		go s.serveSnapshotV2()
		s.wg.Add(1)
		go s.ClearOpsMap()
	} else {
		go s.serveSnapshot()
	}
	go s.checkLeaderChanged()
	go s.detectSqlNodeOffline()
	go s.pushCapacityStats()
	go s.clearCapacityStatMap()

	return nil
}

func (s *Store) CloneDatabases() map[string]*meta.DatabaseInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cacheData.CloneDatabases()
}

func (s *Store) updateMetaNode() {
	sp.IsMeta = true
	sp.IsLeader = s.IsLeader()
}

func (s *Store) pushCapacityStats() {
	interval := time.NewTicker(5 * time.Minute)
	defer func() {
		if err := recover(); err != nil {
			s.Logger.Error("pushCapacityStats panic", zap.String("store raise stack:", string(debug.Stack())),
				zap.Error(errno.NewError(errno.RecoverPanic, err)))
		}
		s.wg.Done()
		interval.Stop()
	}()

	s.updateMetaNode()
	for {
		select {
		case <-s.closing:
			return
		case <-interval.C:
			s.updateMetaNode()
			if sp.IsLeader {
				s.UpLoadCapacityStat()
			}
		}
	}
}

func (s *Store) clearCapacityStatMap() {
	defer func() {
		if err := recover(); err != nil {
			s.Logger.Error("clearCapacityStatMap panic", zap.String("store raise stack:", string(debug.Stack())),
				zap.Error(errno.NewError(errno.RecoverPanic, err)))
		}
		s.wg.Done()
	}()

	for {
		select {
		case <-s.closing:
			return
		case <-time.After(clearCapacityStatMapInterval):
			if s.IsLeader() {
				s.ClearCapStatMap()
			}
		}
	}
}

func (s *Store) ClearCapStatMap() {
	timeNow := time.Now()
	CapacityStatMap.Range(func(key, value any) bool {
		if timeNow.Sub(value.(*CapacityStat).UpdateTime) > clearCapacityStatMapInterval {
			CapacityStatMap.Delete(key)
		}
		return true
	})
}

func (s *Store) UpLoadCapacityStat() {
	defer func() {
		if err := recover(); err != nil {
			s.Logger.Error("UpLoadCapacityStat panic", zap.String("store raise stack:", string(debug.Stack())),
				zap.Error(errno.NewError(errno.RecoverPanic, err)))
		}
	}()

	c := make(chan struct{}, statConcurrency)
	defer close(c)
	dbMap := s.CloneDatabases()

	for dbName, db := range dbMap {
		if db.Options == nil || db.MarkDeleted {
			continue
		}
		for rpName, rp := range db.RetentionPolicies {
			if rpName == autoCreateRetentionPolicyName || rp.MarkDeleted {
				continue
			}
			item := stat.NewLogKeeperStatItem(dbName, rpName)
			wg := &sync.WaitGroup{}
			for _, sg := range rp.ShardGroups {
				if sg.Deleted() {
					continue
				}
				for _, shard := range sg.Shards {
					if shard.MarkDelete {
						continue
					}
					wg.Add(1)
					c <- struct{}{}

					shardPath := obs.GetShardPath(shard.ID, shard.IndexID, shard.Owners[0], sg.StartTime, sg.EndTime, dbName, rpName)
					logPath := path.Join(s.config.DataDir, shardPath, getMst(rp), immutable.CapacityBinFile)

					go func(logPath string, shardID uint64) {
						defer func() {
							wg.Done()
							<-c
						}()

						CollectCapacityStat(shardID, logPath, item)
					}(logPath, shard.ID)
				}
			}

			wg.Wait()
			if item.ObsStoreDataSize == 0 {
				continue
			}
			stat.NewLogKeeperStatistics().Push(item)
		}
	}
}

func getMst(rp *meta.RetentionPolicyInfo) string {
	for key := range rp.Measurements {
		return key
	}
	return ""
}

func CollectCapacityStat(shardID uint64, logPath string, item *stat.LogKeeperStatItem) {
	for t := 0; t < statRetryTimes; t++ {
		shardCap, err := coordinator.LoadCapacity(logPath, t)
		if err != nil {
			if isBreak := ParseLoadErr(t, shardID, err, logPath, item); isBreak {
				break
			}
		} else {
			if isRetry := UpdateCapacityStatMap(t, shardCap, shardID, item); isRetry {
				continue
			}
			atomic.AddInt64(&item.ObsStoreDataSize, shardCap)
			break
		}
	}
}

func ParseLoadErr(retryTimes int, shardID uint64, err error, logPath string, item *stat.LogKeeperStatItem) (isBreak bool) {
	if strings.Contains(err.Error(), statErrInfo) {
		return true
	} else if retryTimes == statRetryTimes-1 {
		v, ok := CapacityStatMap.Load(shardID)
		if ok {
			atomic.AddInt64(&item.ObsStoreDataSize, v.(*CapacityStat).Capacity)
		}
		logger.GetLogger().Error("read mst cap fail", zap.String("logPath", logPath), zap.Error(err))
	}
	return false
}

func UpdateCapacityStatMap(retryTimes int, shardCap int64, shardID uint64, item *stat.LogKeeperStatItem) (isRetry bool) {
	capStat := &CapacityStat{shardCap, item.Begin}
	lastCapStat, loaded := CapacityStatMap.LoadOrStore(shardID, capStat)
	if loaded {
		lastStat, ok := lastCapStat.(*CapacityStat)
		if !ok {
			CapacityStatMap.Delete(shardID)
			CapacityStatMap.Store(shardID, capStat)
			return false
		}
		if lastStat.Capacity <= shardCap {
			CapacityStatMap.Store(shardID, capStat)
		} else {
			if retryTimes == statRetryTimes-1 {
				atomic.AddInt64(&item.ObsStoreDataSize, lastStat.Capacity)
			}
			return true
		}
	}
	return false
}

func (s *Store) joinMetaServer(c *mclient.Client) error {
	addr := s.config.CombineDomain(s.httpAddr)
	rpcAddr := s.config.CombineDomain(s.rpcAddr)
	raftAddr := s.config.CombineDomain(s.raftAddr)

	if len(s.config.JoinPeers) == 1 {
		for {
			err := s.setMetaNode(addr, rpcAddr, raftAddr)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		s.Node.ID = s.GetFirstMetaNodeId()
		s.Node.Clock = s.GetFirstMetaNodeClock()
		return nil
	}

	n, err := c.JoinMetaServer(addr, rpcAddr, raftAddr)
	if err != nil {
		return err
	}
	s.Node.ID = n.ID
	s.Node.Clock = n.LTime
	return nil
}

func (s *Store) makeClient() *mclient.Client {
	c := mclient.NewClient(s.config.Dir, s.config.RetentionAutoCreate, s.config.MaxConcurrentWriteLimit)
	if s.config.UseIncSyncData {
		c.EnableUseSnapshotV2(s.config.RetentionAutoCreate, s.config.ExpandShardsEnable)
	}
	c.SetMetaServers(s.config.JoinPeers)
	c.SetTLS(s.config.HTTPSEnabled)
	return c
}

// fix: modify full to upper subFull
func (s *Store) connectFull(c *mclient.Client) []string {
	var peers []string
	raftAddr := s.config.CombineDomain(s.raftAddr)

	for {
		peers = c.Peers()
		if !mclient.Peers(peers).Contains(raftAddr) {
			peers = append(peers, raftAddr)
		}

		if len(peers) >= len(s.config.JoinPeers) {
			s.Logger.Info("connectFull done", zap.Any("peers", peers), zap.Any("config.JoinPeers", s.config.JoinPeers))
			break
		}

		if len(peers) > len(s.config.BindPeers)/2 && len(s.config.BindPeers) > 0 {
			s.Logger.Info("connectSubFull done", zap.Any("peers", peers), zap.Any("config.BindPeers", s.config.BindPeers))
			break
		}

		s.Logger.Info("Waiting for join peers",
			zap.Int("join num.", len(s.config.JoinPeers)-len(peers)), zap.Strings("Have", peers),
			zap.Strings("Asking nodes", s.config.JoinPeers))
		time.Sleep(time.Second)
	}
	var netStore = netstorage.NewNetStorage(c)
	s.NetStore = netStore
	return peers
}

func (s *Store) setOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if Store has already been opened.
	if s.opened {
		return meta.ErrStoreOpen
	}
	s.opened = true
	return nil
}

// getData is used to get the Data in the Store
func (s *Store) GetData() *meta.Data {
	s.mu.RLock()

	data := s.data
	s.mu.RUnlock()
	return data
}

func (s *Store) GetMarshalData(parts []string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := s.data

	if len(parts) == 0 {
		return json.Marshal(data)
	}
	vo := reflect.ValueOf(data)
	to := reflect.TypeOf(data)
	if vo.Kind() == reflect.Ptr {
		to = to.Elem()
		vo = vo.Elem()
	}
	var newData = make(map[string]interface{})
	for i := 0; i < to.NumField(); i++ {
		fieldName := to.Field(i).Name
		if !slices.Contains(parts, fieldName) {
			continue
		}
		if !vo.IsValid() {
			continue
		}
		if !vo.Field(i).IsValid() {
			continue
		}
		newData[fieldName] = vo.Field(i).Interface()
	}
	return json.Marshal(newData)
}

// setData is used for ut test
func (s *Store) SetData(data *meta.Data) {
	s.mu.RLock()
	s.data = data
	s.mu.RUnlock()
}

// peers returns the raft peers known to this Store
func (s *Store) peers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft == nil {
		return []string{s.config.CombineDomain(s.raftAddr)}
	}
	peers, err := s.raft.Peers()
	if err != nil || len(peers) == 0 {
		return []string{s.config.CombineDomain(s.raftAddr)}
	}
	return peers
}

func (s *Store) newRaftWrapper(ln net.Listener, peers []string) error {
	raftInstance, err := newRaftWrapper(s, ln, peers)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.raft = raftInstance
	s.mu.Unlock()
	return nil
}

func (s *Store) updateCacheData() {
	var err error
	s.mu.RLock()
	dataPb := s.data.Marshal()
	s.mu.RUnlock()
	s.cacheMu.Lock()
	s.cacheData.Unmarshal(dataPb)
	s.cacheDataBytes, err = proto.Marshal(dataPb)
	if err != nil {
		logger.GetLogger().Warn("fail to update cache data", zap.Error(err))
	}
	s.cacheMu.Unlock()
	close(s.cacheDataChanged)
	s.cacheDataChanged = make(chan struct{})
	// save metadata statistics
	s.saveMetadataStatistics()
}

func (s *Store) UpdateCacheDataV2() {
	s.mu.RLock()
	tmp := s.data
	s.mu.RUnlock()
	s.cacheMu.Lock()
	s.cacheData = tmp
	var err error
	if err = s.UpdateOpsMapCacheBytes(); err != nil {
		logger.GetLogger().Error("fail to updateV2 cache data", zap.Error(err))
	}
	s.cacheMu.Unlock()
	// save metadata statistics
	s.saveMetadataStatistics()
}

func (s *Store) UpdateOpsMapCacheBytes() error {
	return s.data.UpdateOpsMapCacheBytes()
}

func (s *Store) saveMetadataStatistics() {
	if stat.MetadataInstance.HaveMetadata() {
		// reduce the reporting frequency to almost 10s.
		return
	}
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	for _, datanode := range s.cacheData.DataNodes {
		stat.MetadataInstance.SaveMetadataNodes(categoryDatanode, datanode.Host, datanode.ID, int64(datanode.Status))
	}
	for _, metanode := range s.cacheData.MetaNodes {
		stat.MetadataInstance.SaveMetadataNodes(categoryMetadata, metanode.Host, metanode.ID, int64(metanode.Status))
	}
	stat.MetadataInstance.SaveMetadataParam(categoryParam, s.cacheData.TakeOverEnabled, s.cacheData.BalancerEnabled, int64(s.cacheData.PtNumPerNode))
}

func (s *Store) serveSnapshot() {
	defer s.wg.Done()
	checkTime := time.After(updateCacheInterval)
	for {
		select {
		case <-s.dataChanged:
			if s.index() > s.cacheIndex() {
				s.updateCacheData()
			}
		case <-s.closing:
			return
		case <-checkTime:
			if s.index() > s.cacheIndex() {
				s.updateCacheData()
			}
			checkTime = time.After(updateCacheInterval)
		}
	}
}

func (s *Store) ClearOpsMap() {
	defer s.wg.Done()
	ticker := time.NewTicker(updateCacheInterval * 10)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.mu.RLock()
			minAliveNodeTmpIndex := s.data.GetMinAliveNodeTmpIndex()
			s.mu.RUnlock()
			s.data.ClearOpsMapV2(minAliveNodeTmpIndex)
		}
	}
}

func (s *Store) serveSnapshotV2() {
	defer s.wg.Done()
	checkTime := time.After(updateCacheInterval)
	for {
		select {
		case _, ok := <-s.dataChanged:
			if ok {
				logger.GetLogger().Error("serveSnapshotV2 dataChanged err")
			}
			if s.index() > s.cacheIndex() {
				s.UpdateCacheDataV2()
			}
		case <-s.closing:
			return
		case <-checkTime:
			s.UpdateCacheDataV2()
			checkTime = time.After(updateCacheInterval)
		}
	}
}

func (s *Store) getNodeShardsMap(db string, rp *meta.RetentionPolicyInfo, mst string) map[uint64][]uint64 {
	nodeShardsMap := make(map[uint64][]uint64)
	for sgIdx := range rp.ShardGroups {
		for shIdx := range rp.ShardGroups[sgIdx].Shards {
			originName := influx.GetOriginMstName(mst)
			if !rp.ShardGroups[sgIdx].Shards[shIdx].ContainPrefix(originName) {
				continue
			}
			if pts, ok := s.cacheData.PtView[db]; ok {
				for _, ptId := range rp.ShardGroups[sgIdx].Shards[shIdx].Owners {
					if int(ptId) < len(pts) {
						nodeId := pts[ptId].Owner.NodeID
						nodeShardsMap[nodeId] = append(nodeShardsMap[nodeId], rp.ShardGroups[sgIdx].Shards[shIdx].ID)
					}
				}
			}
		}
	}
	return nodeShardsMap
}

func (s *Store) deleteMeasurement(db string, rp *meta.RetentionPolicyInfo, mst string) error {
	s.mu.RLock()
	s.cacheMu.RLock()
	nodeShardsMap := s.getNodeShardsMap(db, rp, mst)
	s.mu.RUnlock()
	errChan := make(chan error)
	n := 0
	for nodeId, shardIds := range nodeShardsMap {
		n++
		node := s.cacheData.DataNode(nodeId)
		go func(node *meta.DataNode, db string, shardIds []uint64) {
			errChan <- s.NetStore.DeleteMeasurement(node, db, rp.Name, mst, shardIds)
		}(node, db, shardIds)
	}
	s.cacheMu.RUnlock()

	var err error
	for i := 0; i < n; i++ {
		errC := <-errChan
		if errC != nil {
			err = errC
		}
	}
	close(errChan)
	if err != nil {
		return err
	}

	return s.deleteMeasurementMetaData(db, rp.Name, mst)
}

// deleteDbDir deletes data/db dir and wal/db dir
func (s *Store) deleteDbDir(db string, obsOpt *obs.ObsOptions) error {
	dataDbPath := path.Join(s.config.DataDir, config.DataDirectory, db)
	lock := fileops.FileLockOption("")
	if obsOpt != nil {
		if err := fileops.DeleteObsPath(path.Join(config.DataDirectory, db), obsOpt); err != nil {
			return err
		}
	}
	if err := fileops.RemoveAll(dataDbPath, lock); err != nil {
		return err
	}
	walPtPath := path.Join(s.config.WalDir, config.WalDirectory, db)
	if err := fileops.RemoveAll(walPtPath, lock); err != nil {
		return err
	}
	return nil
}

// deletePtDir deletes data/db/pt dir and wal/db/pt dir
func (s *Store) deletePtDir(db string, pt uint32, obsOpt *obs.ObsOptions) error {
	ptId := strconv.Itoa(int(pt))
	dataPtPath := path.Join(s.config.DataDir, config.DataDirectory, db, ptId)
	lock := fileops.FileLockOption("")
	if obsOpt != nil {
		if err := fileops.DeleteObsPath(path.Join(config.DataDirectory, db, ptId), obsOpt); err != nil {
			return err
		}
	}
	if err := fileops.RemoveAll(dataPtPath, lock); err != nil {
		return err
	}
	walPtPath := path.Join(s.config.WalDir, config.WalDirectory, db, ptId)
	if err := fileops.RemoveAll(walPtPath, lock); err != nil {
		return err
	}
	return nil
}

// deleteRpDir deletes data/db/pt/rp dir and wal/db/pt/rp dir
func (s *Store) deleteRpDir(db, rp string, pt uint32, obsOpt *obs.ObsOptions) error {
	ptId := strconv.Itoa(int(pt))
	dataPtPath := path.Join(s.config.DataDir, config.DataDirectory, db, ptId, rp)
	lock := fileops.FileLockOption("")
	if obsOpt != nil {
		if err := fileops.DeleteObsPath(path.Join(config.DataDirectory, db, ptId, rp), obsOpt); err != nil {
			return err
		}
	}
	if err := fileops.RemoveAll(dataPtPath, lock); err != nil {
		return err
	}
	walPtPath := path.Join(s.config.WalDir, config.WalDirectory, db, ptId)
	if err := fileops.RemoveAll(walPtPath, lock); err != nil {
		return err
	}
	return nil
}

func (s *Store) deleteRetentionPolicy(db string, rp string) error {
	s.cacheMu.RLock()
	ptInfos := s.cacheData.PtView[db]
	dbInfo, ok := s.cacheData.Databases[db]
	if !ok {
		s.cacheMu.RUnlock()
		return nil
	}

	errChan := make(chan error)
	n := 0
	for i := range ptInfos {
		node := s.cacheData.DataNode(ptInfos[i].Owner.NodeID)
		if node == nil {
			continue
		}
		// in shared-storage case and the node is not alive status, remove pt dir directly
		if config.IsLogKeeper() && node.Status != serf.StatusAlive {
			if err := s.deleteRpDir(db, rp, ptInfos[i].PtId, dbInfo.Options); err != nil {
				s.cacheMu.RUnlock()
				return err
			}
			continue
		}

		n++
		go func(node *meta.DataNode, db string, rp string, pt uint32) {
			errChan <- s.NetStore.DeleteRetentionPolicy(node, db, rp, pt)
		}(node, db, rp, ptInfos[i].PtId)
	}
	s.cacheMu.RUnlock()

	var err error
	for i := 0; i < n; i++ {
		errC := <-errChan
		if errC != nil {
			err = errC
		}
	}
	close(errChan)
	if err != nil {
		return err
	}

	return s.deleteRpMetadata(db, rp)
}

func (s *Store) checkDelete(deleteType int) {
	defer s.deleteWg.Done()
	var index uint64 = 0
	errChan := make(chan error)
	n := 0
	for {
		select {
		case <-s.closing:
			return
		case <-s.stepDown:
			return
		default:
			s.cacheMu.RLock()
			if index < s.cacheData.Index {
				index = s.cacheData.Index
			}
			s.cacheMu.RUnlock()
		}

		n = 0
		s.cacheMu.RLock()
		if deleteType == DeleteDatabase {
			for dbName := range s.cacheData.Databases {
				if s.cacheData.Databases[dbName].MarkDeleted {
					n++
					go func(dbName string) {
						errChan <- s.deleteDatabase(dbName)
					}(dbName)
				}
			}
		} else if deleteType == DeleteRp {
			for dbName := range s.cacheData.Databases {
				if s.cacheData.Databases[dbName].MarkDeleted {
					continue
				}
				for rpName := range s.cacheData.Databases[dbName].RetentionPolicies {
					if s.cacheData.Databases[dbName].RetentionPolicies[rpName].MarkDeleted {
						n++
						go func(db string, rp string) {
							errChan <- s.deleteRetentionPolicy(db, rp)
						}(dbName, rpName)
					}
				}
			}
		} else if deleteType == DeleteMeasurement {
			s.cacheData.WalkDatabases(func(db *meta.DatabaseInfo) {
				db.WalkRetentionPolicy(func(rp *meta.RetentionPolicyInfo) {
					for mstIdx := range rp.Measurements {
						if rp.Measurements[mstIdx].MarkDeleted {
							n++
							go func(db string, rp *meta.RetentionPolicyInfo, mst string) {
								errChan <- s.deleteMeasurement(db, rp, mst)
							}(db.Name, rp, rp.Measurements[mstIdx].Name)
						}
					}
				})
			})
		}
		s.cacheMu.RUnlock()
		for i := 0; i < n; i++ {
			err := <-errChan
			if err != nil {
				s.Logger.Warn("drop failed", zap.Error(err), zap.Int("delete type", deleteType))
			}
		}

		time.Sleep(checkInterval)
	}
}

func (s *Store) deleteDatabase(dbName string) error {
	s.cacheMu.RLock()
	ptInfos := s.cacheData.PtView[dbName]
	dbInfo, ok := s.cacheData.Databases[dbName]
	if !ok {
		s.cacheMu.RUnlock()
		return nil
	}

	errChan := make(chan error)
	n := 0
	for i := range ptInfos {
		node := s.cacheData.DataNode(ptInfos[i].Owner.NodeID)
		if node == nil {
			continue
		}

		// in shared-storage case and the node is not alive status, remove pt dir directly
		// case: 1.drop database 2.store1 failed 3.database only dropped until store1 startup
		if (config.GetHaPolicy() == config.SharedStorage || config.IsLogKeeper()) && node.Status != serf.StatusAlive {
			if err := s.deletePtDir(dbName, ptInfos[i].PtId, dbInfo.Options); err != nil {
				s.cacheMu.RUnlock()
				return err
			}
			continue
		}

		n++
		go func(node *meta.DataNode, dbName string, ptId uint32) {
			errChan <- s.NetStore.DeleteDatabase(node, dbName, ptId)
		}(node, dbName, ptInfos[i].PtId)
	}
	s.cacheMu.RUnlock()

	var err error
	for i := 0; i < n; i++ {
		errC := <-errChan
		if errC != nil {
			err = errC
		}
	}
	close(errChan)
	if err != nil {
		return err
	}

	// after deleting PT, delete the empty directory of DB
	if config.IsLogKeeper() {
		if err := s.deleteDbDir(dbName, dbInfo.Options); err != nil {
			s.Logger.Warn("delete the empty directory of DB failed", zap.String("db", dbName), zap.Error(err))
		}
	}

	return s.deleteDatabaseMetadata(dbName)
}

func (s *Store) deleteDatabaseMetadata(database string) error {
	val := &mproto.DropDatabaseCommand{
		Name: proto.String(database),
	}
	t := mproto.Command_DropDatabaseCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_DropDatabaseCommand_Command, val); err != nil {
		return err
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) deleteRpMetadata(database string, policy string) error {
	val := &mproto.DropRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     proto.String(policy),
	}
	t := mproto.Command_DropRetentionPolicyCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_DropRetentionPolicyCommand_Command, val); err != nil {
		return err
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) deleteMeasurementMetaData(database, policy, measurement string) error {
	val := &mproto.DropMeasurementCommand{
		Database:    proto.String(database),
		Policy:      proto.String(policy),
		Measurement: proto.String(measurement),
	}
	t := mproto.Command_DropMeasurementCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_DropMeasurementCommand_Command, val); err != nil {
		return err
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) ApplyCmd(cmd *mproto.Command) error {
	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	if err := s.apply(b); err != nil {
		return err
	}
	return nil
}

func (s *Store) GetClose() chan struct{} {
	return s.closing
}
func (s *Store) Close() error {
	return s.close()
}
func (s *Store) close() error {
	select {
	case <-s.closing:
		return nil
	default:
		close(s.closing)
	}
	// wait checkLeaderChanged goroutine stopped to avoid cluster manager start and stop concurrently
	s.deleteWg.Wait()
	s.wg.Wait()

	if s.cm != nil {
		s.cm.Close()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var errSQLite, errRaft error
	if s.config.SQLiteEnabled && s.data.SQLite != nil {
		errSQLite = s.data.SQLite.Close()
	}
	if s.raft != nil {
		errRaft = s.raft.Close()
	}
	if errSQLite != nil || errRaft != nil {
		return fmt.Errorf("%s", errSQLite.Error()+errRaft.Error())
	}
	return nil
}

func (s *Store) getSnapshot(role mclient.Role) []byte {
	return s.getSnapshotBySql()
}

func (s *Store) getSnapshotBySql() []byte {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	return s.cacheDataBytes
}

func (s *Store) UpdateNodeTmpIndex(role mclient.Role, index uint64, nodeId uint64) {
	val := &mproto.UpdateNodeTmpIndexCommand{
		Role:   proto.Int32(int32(role)),
		Index:  proto.Uint64(index),
		NodeId: proto.Uint64(nodeId),
	}

	t := mproto.Command_UpdateNodeTmpIndexCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdateNodeTmpIndexCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		logger.GetLogger().Error("UpdateNodeTmpIndex fail", zap.Error(err))
		return
	}
}

func (s *Store) GetNodeTmpIndex(role mclient.Role, nodeId uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if role == mclient.SQL {
		return s.data.GetSqlNodeIndex(nodeId)
	} else if role == mclient.STORE {
		return s.data.GetNodeIndexV2(nodeId)
	}
	return 0, fmt.Errorf("metanode has not tmpIndex")
}

func (s *Store) TryUpdateNodeTmpIndex(role mclient.Role, index uint64, nodeId uint64) {
	tmpIndex, err := s.GetNodeTmpIndex(role, nodeId)
	if err != nil {
		logger.GetLogger().Error("getSnapshotV2 UpdateNodeTmpIndex err", zap.String("err", err.Error()), zap.Uint64("node", nodeId), zap.Int64("role", int64(role)), zap.Uint64("index", index))
		return
	}
	if tmpIndex < index && (index-tmpIndex) >= UpdateTmpIndexGap {
		go s.UpdateNodeTmpIndex(role, index, nodeId)
	}
}

func (s *Store) getSnapshotV2(role mclient.Role, OldIndex uint64, nodeId uint64) []byte {
	s.mu.RLock()
	if OldIndex == s.data.Index {
		s.mu.RUnlock()
		return nil
	} else if OldIndex > s.data.Index {
		logger.GetLogger().Error("getSnapshotV2 oldIndex > data.index", zap.Uint64("node", nodeId), zap.Int64("role", int64(role)), zap.Uint64("oldIndex", OldIndex), zap.Uint64("data.index", s.data.Index))
		s.mu.RUnlock()
		return nil
	}
	if OldIndex >= s.data.UpdateNodeTmpIndexCommandStart {
		s.mu.RUnlock()
		return nil
	}
	defer s.TryUpdateNodeTmpIndex(role, OldIndex, nodeId)
	ops, state := s.data.GetOps(OldIndex)
	if len(ops) > 0 {
		dataOps := meta.NewDataOps(ops, s.data.MaxCQChangeID, int(state), s.data.Index)
		s.mu.RUnlock()
		buf := dataOps.Marshal()
		stat.NewMetaStatistics().AddGetFromOpsMapTotal(1)
		stat.NewMetaStatistics().AddGetFromOpsMapLenTotal(int64(dataOps.Len()))
		logger.GetLogger().Debug("serveSnapshotV2Op ok", zap.Uint64("oldindex", OldIndex), zap.Uint64("newindex", s.index()), zap.Int("opNum", len(dataOps.GetOps())), zap.Int64("len", int64(len(buf))))
		return buf
	} else if state == meta.AllClear {
		dataPb := s.data.MarshalV2()
		s.mu.RUnlock()
		dataOps := meta.NewDataOpsOfAllClear(int(state), dataPb, *dataPb.Index)
		buf := dataOps.Marshal()
		stat.NewMetaStatistics().AddGetFromDataMarshalTotal(1)
		stat.NewMetaStatistics().AddGetFromDataMarshalLenTotal(int64(len(buf)))
		logger.GetLogger().Debug("serveSnapshotV2Data ok", zap.Uint64("oldindex", OldIndex), zap.Uint64("newindex", s.index()), zap.Int64("len", int64(len(buf))))
		return buf
	} else {
		dataOps := meta.NewDataOps(nil, s.data.MaxCQChangeID, int(state), s.data.Index)
		s.mu.RUnlock()
		buf := dataOps.Marshal()
		stat.NewMetaStatistics().AddGetFromOpsMapTotal(1)
		stat.NewMetaStatistics().AddGetFromOpsMapLenTotal(int64(dataOps.Len()))
		logger.GetLogger().Debug("serveSnapshotV2CQ2 ok", zap.Uint64("oldindex", OldIndex), zap.Uint64("newindex", s.index()), zap.Int64("len", int64(len(buf))))
		return buf
	}
}

// afterIndex returns a channel that will be closed to signal
// the caller when an updated snapshot is available.
func (s *Store) afterIndex(index uint64) <-chan struct{} {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	if index < s.cacheData.Index {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return s.cacheDataChanged
}

func (s *Store) waitForLeader() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return errors.New("closing")
		case <-ticker.C:
			if s.leader() != "" {
				return nil
			}
		}
	}
}

// IsLeader returns true if the Store is currently the leader.
func (s *Store) IsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft == nil {
		return false
	}
	return s.raft.IsLeader()
}

// leader returns what the Store thinks is the current leader. An empty
// string indicates no leader exists.
func (s *Store) leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft == nil {
		return ""
	}
	return s.raft.Leader()
}

// leaderHTTP returns the http address what the Store thinks is the current leader. An empty
// string indicates no leader exists.
func (s *Store) leaderHTTP() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft == nil {
		return ""
	}

	leader := s.raft.Leader()
	if leader == "" {
		return leader
	}

	var addr string
	for _, node := range s.data.MetaNodes {
		if leader == node.TCPHost {
			addr = node.Host
			break
		}
	}
	return addr
}

// otherMetaServersHTTP will return the HTTP bind addresses of the other
// meta servers in the cluster
func (s *Store) otherMetaServersHTTP() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var a []string
	for i := range s.data.MetaNodes {
		if s.data.MetaNodes[i].TCPHost != s.raftAddr {
			a = append(a, s.data.MetaNodes[i].Host)
		}
	}
	return a
}

// index returns the current Store index.
func (s *Store) index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Index
}

func (s *Store) cacheIndex() uint64 {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	return s.cacheData.Index
}

// apply applies a command to raft.
func (s *Store) apply(b []byte) error {
	stat.NewMetaStatistics().AddStoreApplyTotal(1)
	if s.raft == nil {
		return errno.NewError(errno.RaftIsNotOpen)
	}
	return s.raft.Apply(b)
}

func (s *Store) Join(n *meta.NodeInfo) (*meta.NodeInfo, error) {
	if s.raft == nil {
		return nil, ErrRaftNotOpen
	}
	if err := s.raft.AddServer(n.TCPHost); err != nil {
		return nil, err
	}

	if err := s.createMetaNode(n.Host, n.RPCAddr, n.TCPHost); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, node := range s.data.MetaNodes {
		if node.TCPHost == n.TCPHost && node.Host == n.Host {
			return &node, nil
		}
	}
	return nil, meta.ErrNodeNotFound
}

// createMetaNode is used by the join command to create the metanode int
// the metaStore
func (s *Store) createMetaNode(httpAddr, rpcAddr, raftAddr string) error {
	val := &mproto.CreateMetaNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		RPCAddr:  proto.String(rpcAddr),
		TCPAddr:  proto.String(raftAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}
	t := mproto.Command_CreateMetaNodeCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_CreateMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	return s.ApplyCmd(cmd)
}

// setMetaNode is used when the raft group has only a single peer. It will
// either create a metanode or update the information for the one metanode
// that is there. It's used because hostnames can change
func (s *Store) setMetaNode(addr, rpcAddr, raftAddr string) error {
	val := &mproto.SetMetaNodeCommand{
		HTTPAddr: proto.String(addr),
		RPCAddr:  proto.String(rpcAddr),
		TCPAddr:  proto.String(raftAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}
	t := mproto.Command_SetMetaNodeCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_SetMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	return s.ApplyCmd(cmd)
}

func (s *Store) GetFirstMetaNodeId() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.data.MetaNodes) == 1 {
		return s.data.MetaNodes[0].ID
	}
	return 0
}

func (s *Store) GetFirstMetaNodeClock() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.data.MetaNodes) == 1 {
		return s.data.MetaNodes[0].LTime
	}
	return 0
}

func (s *Store) showDebugInfo(witch string) ([]byte, error) {
	if strings.Contains(witch, "raft") {
		if s.raft == nil {
			return nil, ErrRaftNotOpen
		}
		return s.raft.ShowDebugInfo(witch)
	}

	if strings.Contains(witch, "meta") {
		switch witch {
		case "meta-version":
			s := map[string]int{
				"version": s.config.Version,
			}

			return json.Marshal(s)
		default:
			return nil, fmt.Errorf("not support debug type")
		}
	}

	s.Logger.Error("not support debug type")
	return nil, fmt.Errorf("not support debug type")
}

func (s *Store) userSnapshot(version uint32) error {
	if version != uint32(s.config.Version) {
		s.Logger.Error("userSnapshot error version input ", zap.Uint32("userVersion", version), zap.Int("version", s.config.Version))
		return fmt.Errorf("error version")
	}
	return s.raft.UserSnapshot()
}
func (s *Store) MeteRecover() {

	// recover data
	if s.config.MetaRecover {
		s.dataRecover.RecoverMeta(filepath.Join(s.config.DataDir, "data"), s.data)
	}
	err := s.userSnapshot(0)
	if err != nil {
		logger.GetLogger().Error("meta recover err", zap.Error(err))
	}
}

func (s *Store) reSharding(db string, rp string, sgId uint64, splitTime int64, shardBounds []string) error {
	val := &mproto.ReShardingCommand{
		Database:     proto.String(db),
		RpName:       proto.String(rp),
		ShardGroupID: proto.Uint64(sgId),
		SplitTime:    proto.Int64(splitTime),
		ShardBounds:  shardBounds}
	t := mproto.Command_ReShardingCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_ReShardingCommand_Command, val); err != nil {
		return err
	}

	return s.ApplyCmd(cmd)
}

type reShardingRes struct {
	rp  string
	err error
}

func (s *Store) getDbInfo(db string) *dbInfo {
	s.statMu.Lock()
	defer s.statMu.Unlock()
	dbinfo, ok := s.dbStatistics[db]
	if !ok {
		dbinfo = &dbInfo{rpStatistics: make(map[string]*rpInfo)}
		dbinfo.logger = s.Logger
		dbinfo.store = s
		s.dbStatistics[db] = dbinfo
	}
	return dbinfo
}

func (s *Store) UpdateLoad(b []byte) error {
	if !s.IsLeader() {
		return raft.ErrNotLeader
	}
	var cmd mproto.Command
	if err := proto.Unmarshal(b, &cmd); err != nil {
		panic(fmt.Errorf("cannot marshal command: %x", b))
	}

	if cmd.GetType() != mproto.Command_ReportShardsCommand {
		return fmt.Errorf("err command type")
	}

	ext, _ := proto.GetExtension(&cmd, mproto.E_ReportShardsLoadCommand_Command)
	v := ext.(*mproto.ReportShardsLoadCommand)
	res := make(chan *reShardingRes)
	reShardingNum := 0

	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	for i := range v.GetDBPTStat() {
		db := v.GetDBPTStat()[i].GetDB()
		dbinfo := s.getDbInfo(db)

		if len(v.GetDBPTStat()[i].GetRpStats()) == 0 {
			ptId := v.GetDBPTStat()[i].GetPtID()
			dbinfo.updateReportTime(int(ptId))
			continue
		}

		for j := range v.GetDBPTStat()[i].GetRpStats() {
			rpStat := v.GetDBPTStat()[i].GetRpStats()[j]
			rpinfo := dbinfo.getRpInfo(rpStat.GetRpName())
			sgInfo := s.cacheData.NewestShardGroup(db, rpStat.GetRpName())
			if sgInfo == nil || sgInfo.Shards == nil {
				continue
			}
			minShardID, maxShardID := sgInfo.Shards[0].ID, sgInfo.Shards[len(sgInfo.Shards)-1].ID
			// new shard group
			if minShardID != rpinfo.minShardID {
				rpinfo.minShardID, rpinfo.maxShardID = minShardID, maxShardID
				rpinfo.createShardStat()
			}

			shardId := rpStat.GetShardStats().GetShardID()
			shardSize := rpinfo.updateShardStat(shardId, v.GetDBPTStat()[i].GetPtID(), rpStat.GetShardStats())
			if shardSize > s.config.SplitRowThreshold {
				reShardingNum++
				go func(rpinfo *rpInfo, db, rp string, sgId uint64) {
					ptNum := len(s.cacheData.PtView[db])
					var err error
					defer func() {
						rpinfo.resetReSharding(err)
					}()
					splitTime := rpinfo.checkReSharding(ptNum, s.config.ImbalanceFactor)
					if splitTime == 0 {
						err = fmt.Errorf("does not satisfy split condition")
						res <- &reShardingRes{rp: rp, err: nil}
						return
					}

					shardNum := int(math.Min(math.Ceil(float64(rpinfo.currentRowCount)/float64(s.config.SplitRowThreshold)), float64(ptNum)))
					splitPoints := rpinfo.getSplitVectorByRowCount(db, shardNum, s.cacheData)
					if len(splitPoints) == 0 {
						err = fmt.Errorf("get split point failed")
						res <- &reShardingRes{rp: rp, err: err}
						return
					}
					err = s.reSharding(db, rp, sgId, splitTime, splitPoints)
					res <- &reShardingRes{rp: rp, err: err}
				}(rpinfo, db, rpStat.GetRpName(), sgInfo.ID)
			}
		}
	}

	for i := 0; i < reShardingNum; i++ {
		r := <-res
		if r.err != nil {
			s.Logger.Warn("reSharding failed,", zap.Error(r.err))
		}
	}
	close(res)
	return nil
}

func (s *Store) createDataNode(writeHost, queryHost, role, az string) ([]byte, error) {
	val := &mproto.CreateDataNodeCommand{
		HTTPAddr: proto.String(writeHost),
		TCPAddr:  proto.String(queryHost),
		Role:     proto.String(role),
		Az:       proto.String(az),
	}
	logger.GetLogger().Info("create data node start")
	t := mproto.Command_CreateDataNodeCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_CreateDataNodeCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		logger.GetLogger().Error("create data node fail", zap.Error(err))
		return nil, err
	}
	logger.GetLogger().Info("create data node finish")
	nodeStartInfo := meta.NodeStartInfo{}
	s.mu.RLock()
	dn := s.data.DataNodeByHttpHost(writeHost)
	logger.GetLogger().Info("create data node", zap.Uint64("id", dn.ID), zap.Uint64("lTime", dn.LTime))
	nodeStartInfo.NodeId = dn.ID
	nodeStartInfo.LTime = dn.LTime
	nodeStartInfo.ConnId = dn.ConnID
	status := dn.Status
	s.mu.RUnlock()
	// todo it is not approtiate to judge as single node, please modify it later
	if len(s.config.JoinPeers) == 1 {
		s.cm.eventCh <- serf.MemberEvent{
			Type:      serf.EventMemberJoin,
			EventTime: serf.LamportTime(nodeStartInfo.LTime + 1),
			Members: []serf.Member{
				serf.Member{Name: strconv.FormatUint(nodeStartInfo.NodeId, 10),
					Tags:   map[string]string{"role": "store"},
					Status: serf.StatusAlive},
			}}
	}
	// register the store node to SPDY, support send message from meta to store.
	transport.NewNodeManager().Add(dn.ID, dn.TCPHost)
	stat.NewMetaStatCollector().Push(&stat.MetaStatItem{Status: int64(status),
		LTime: int64(nodeStartInfo.LTime), NodeID: strconv.FormatUint(dn.ID, 10), Host: dn.Host})
	return nodeStartInfo.MarshalBinary()
}

func (s *Store) CreateSqlNode(httpHost string, gossipAddr string) ([]byte, error) {
	val := &mproto.CreateSqlNodeCommand{
		HTTPAddr:   proto.String(httpHost),
		GossipAddr: proto.String(gossipAddr),
	}

	t := mproto.Command_CreateSqlNodeCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_CreateSqlNodeCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		logger.GetLogger().Error("create sql data node fail", zap.Error(err))
		return nil, err
	}

	nodeStartInfo := meta.NodeStartInfo{}
	s.mu.RLock()
	dn := s.data.SqlNodeByHttpHost(httpHost)
	logger.GetLogger().Info("create sql data node success", zap.Uint64("id", dn.ID), zap.Uint64("lTime", dn.LTime))
	nodeStartInfo.NodeId = dn.ID
	nodeStartInfo.LTime = dn.LTime
	nodeStartInfo.ConnId = dn.ConnID
	s.mu.RUnlock()
	// todo it is not approtiate to judge as single node, please modify it later
	if len(s.config.JoinPeers) == 1 {
		s.cm.eventCh <- serf.MemberEvent{
			Type:      serf.EventMemberJoin,
			EventTime: serf.LamportTime(nodeStartInfo.LTime + 1),
			Members: []serf.Member{
				serf.Member{Name: strconv.FormatUint(nodeStartInfo.NodeId, 10),
					Tags:   map[string]string{"role": "sql"},
					Status: serf.StatusAlive},
			}}
	}
	// register the store node to SPDY, support send message from meta to store. sql not use
	// transport.NewNodeManager().Add(dn.ID, dn.TCPHost)
	// stat.NewMetaStatCollector().Push(&stat.MetaStatItem{Status: int64(status),
	//LTime: int64(nodeStartInfo.LTime), NodeID: strconv.FormatUint(dn.ID, 10), Host: dn.Host})
	return nodeStartInfo.MarshalBinary()
}

func (s *Store) getShardAuxInfo(body []byte) ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	var cmd mproto.Command
	if err := proto.Unmarshal(body, &cmd); err != nil {
		return nil, err
	}

	var b []byte
	var err error
	switch cmd.GetType() {
	case mproto.Command_TimeRangeCommand:
		if s.UseIncSyncData {
			b, err = s.getTimeRangeV2(&cmd)
		} else {
			b, err = s.getTimeRange(&cmd)
		}
	case mproto.Command_ShardDurationCommand:
		if s.UseIncSyncData {
			b, err = s.getDurationInfoV2(&cmd)
		} else {
			b, err = s.getDurationInfo(&cmd)
		}
	case mproto.Command_IndexDurationCommand:
		b, err = s.GetIndexDurationInfo(&cmd)
	default:
		err = fmt.Errorf("non suportted getAuxShardInfo cmd")
	}
	return b, err
}

func (s *Store) GetUserInfo() ([]byte, error) {
	b, err := s.cacheData.MarshalBinaryUser()
	return b, err
}

func (s *Store) getStreamInfo() ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	streams := &mproto.StreamInfos{
		Infos: make([]*mproto.StreamInfo, len(s.cacheData.Streams)),
	}
	j := 0
	for si := range s.cacheData.Streams {
		streams.Infos[j] = s.cacheData.Streams[si].Marshal()
		j++
	}

	return proto.Marshal(streams)
}

func (s *Store) getMeasurementInfo(dbName, rpName, mstName string) ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	mst, err := s.cacheData.Measurement(dbName, rpName, mstName)
	if err != nil {
		return nil, err
	}
	return mst.MarshalBinary()
}

func (s *Store) getMeasurementsInfo(dbName, rpName string) ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	msts, err := s.cacheData.Measurements(dbName, rpName)
	if err != nil {
		return nil, err
	}

	return msts.MarshalBinary()
}

func (s *Store) getTimeRange(cmd *mproto.Command) ([]byte, error) {
	ext, _ := proto.GetExtension(cmd, mproto.E_TimeRangeCommand_Command)
	v := ext.(*mproto.TimeRangeCommand)
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	dbInfo, err := s.cacheData.GetDatabase(v.GetDatabase())
	if err != nil {
		return nil, err
	}
	rpInfo, err := dbInfo.GetRetentionPolicy(v.GetPolicy())
	if err != nil {
		return nil, err
	}
	shardTimeRangeInfo := rpInfo.TimeRangeInfo(v.GetShardID())
	if shardTimeRangeInfo == nil {
		return nil, errno.NewError(errno.ShardMetaNotFound, v.GetShardID())
	}

	return shardTimeRangeInfo.MarshalBinary()
}

func (s *Store) getTimeRangeV2(cmd *mproto.Command) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ext, _ := proto.GetExtension(cmd, mproto.E_TimeRangeCommand_Command)
	v, ok := ext.(*mproto.TimeRangeCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a TimeRangeCommand", ext))
	}
	dbInfo, err := s.data.GetDatabase(v.GetDatabase())
	if err != nil {
		return nil, err
	}
	rpInfo, err := dbInfo.GetRetentionPolicy(v.GetPolicy())
	if err != nil {
		return nil, err
	}
	shardTimeRangeInfo := rpInfo.TimeRangeInfo(v.GetShardID())
	if shardTimeRangeInfo == nil {
		return nil, errno.NewError(errno.ShardMetaNotFound, v.GetShardID())
	}

	return shardTimeRangeInfo.MarshalBinary()
}

func (s *Store) getDurationInfo(cmd *mproto.Command) ([]byte, error) {
	ext, _ := proto.GetExtension(cmd, mproto.E_ShardDurationCommand_Command)
	v := ext.(*mproto.ShardDurationCommand)
	s.mu.RLock()
	dbPtIds := s.getDbPtsByNodeId(v.GetNodeId())
	s.mu.RUnlock()
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	if s.cacheData.Index < v.GetIndex() {
		return nil, errno.NewError(errno.DataIsOlder)
	}
	durationRes := s.cacheData.DurationInfos(dbPtIds)
	return durationRes.MarshalBinary()
}

func (s *Store) getDurationInfoV2(cmd *mproto.Command) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ext, err := proto.GetExtension(cmd, mproto.E_ShardDurationCommand_Command)
	if err != nil {
		return nil, fmt.Errorf("%s is not a E_ShardDurationCommand_Command", ext)
	}
	v, ok := ext.(*mproto.ShardDurationCommand)
	if !ok {
		return nil, fmt.Errorf("%s is not a ShardDurationCommand", ext)
	}
	dbPtIds := s.getDbPtsByNodeId(v.GetNodeId())
	if s.data.Index < v.GetIndex() {
		return nil, errno.NewError(errno.DataIsOlder)
	}
	durationRes := s.data.DurationInfos(dbPtIds)
	return durationRes.MarshalBinary()
}

func (s *Store) GetIndexDurationInfo(cmd *mproto.Command) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ext, err := proto.GetExtension(cmd, mproto.E_IndexDurationCommand_Command)
	if err != nil {
		return nil, fmt.Errorf("%s is not a E_IndexDurationCommand_Command", ext)
	}
	v, ok := ext.(*mproto.IndexDurationCommand)
	if !ok {
		return nil, fmt.Errorf("%s is not a IndexDurationCommand", ext)
	}
	dbPtIds := s.getDbPtsByNodeId(v.GetNodeId())
	if s.data.Index < v.GetIndex() {
		return nil, errno.NewError(errno.DataIsOlder)
	}
	durationRes := s.data.IndexDurationInfos(dbPtIds)
	return durationRes.MarshalBinary()
}

func (s *Store) GetDownSampleInfo() ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	d := &meta.DownSamplePoliciesInfoWithDbRp{
		Infos: make([]*meta.DownSamplePolicyInfoWithDbRp, 0),
	}
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	for database, dbInfo := range s.cacheData.Databases {
		for rpName, rpinfo := range dbInfo.RetentionPolicies {
			if rpinfo.DownSamplePolicyInfo == nil || rpinfo.DownSamplePolicyInfo.IsNil() {
				continue
			}
			d.Infos = append(d.Infos, &meta.DownSamplePolicyInfoWithDbRp{
				DbName: database,
				RpName: rpName,
				Info:   rpinfo.DownSamplePolicyInfo,
			})
		}
	}
	return d.MarshalBinary()
}
func (s *Store) GetRpMstInfos(db, rp string, dataTypes []int64) ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	if s.cacheData.Database(db) == nil {
		return nil, errno.NewError(errno.DatabaseNotFound, db)
	}
	rpInfos := s.cacheData.Database(db).RetentionPolicies[rp]
	if rpInfos == nil {
		return nil, errno.NewError(errno.RpNotFound, rp)
	}
	return TransMeasurementInfos2Bytes(dataTypes, rpInfos)
}

func (s *Store) updateNodeStatus(id uint64, status int32, lTime uint64, gossipPort string) error {
	val := &mproto.UpdateNodeStatusCommand{
		ID:         proto.Uint64(id),
		Status:     proto.Int32(status),
		Ltime:      proto.Uint64(lTime),
		GossipAddr: proto.String(gossipPort)}
	t := mproto.Command_UpdateNodeStatusCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdateNodeStatusCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		return err
	}
	s.mu.RLock()
	dn := *(s.data.DataNode(id))
	s.mu.RUnlock()
	stat.NewMetaStatCollector().Push(&stat.MetaStatItem{Status: int64(dn.Status), LTime: int64(dn.LTime),
		NodeID: strconv.FormatUint(dn.ID, 10), Host: dn.Host})
	return nil
}

func (s *Store) UpdateSqlNodeStatus(id uint64, status int32, lTime uint64, gossipPort string) error {
	val := &mproto.UpdateSqlNodeStatusCommand{
		ID:         proto.Uint64(id),
		Status:     proto.Int32(status),
		Ltime:      proto.Uint64(lTime),
		GossipAddr: proto.String(gossipPort)}
	t := mproto.Command_UpdateSqlNodeStatusCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdateSqlNodeStatusCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) UpdateMetaNodeStatus(id uint64, status int32, lTime uint64, gossipPort string) error {
	val := &mproto.UpdateMetaNodeStatusCommand{
		ID:         proto.Uint64(id),
		Status:     proto.Int32(status),
		Ltime:      proto.Uint64(lTime),
		GossipAddr: proto.String(gossipPort)}
	t := mproto.Command_UpdateMetaNodeStatusCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdateMetaNodeStatusCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) updateReplication(database string, rgId uint32, masterId uint32, peers []meta.Peer) error {
	mPeers := make([]*mproto.Peer, len(peers))
	for i := range peers {
		role := uint32(peers[i].PtRole)
		mPeers[i] = &mproto.Peer{
			ID:   &peers[i].ID,
			Role: &role,
		}
	}

	val := &mproto.UpdateReplicationCommand{
		Database:   proto.String(database),
		RepGroupId: proto.Uint32(rgId),
		MasterId:   proto.Uint32(masterId),
		Peers:      mPeers}
	t := mproto.Command_UpdateReplicationCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdateReplicationCommand_Command, val); err != nil {
		panic(err)
	}

	return s.ApplyCmd(cmd)
}

func (s *Store) dataNodes() meta.DataNodeInfos {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.CloneDataNodes()
}

func (s *Store) sqlNodes() meta.DataNodeInfos {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.CloneSqlNodes()
}

func (s *Store) metaNodes() []meta.NodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.CloneMetaNodes()
}

func (s *Store) GetGossipConfig() *config.Gossip {
	return s.GossipConfig
}

func (s *Store) removeEvent(eventId string) error {
	val := &mproto.RemoveEventCommand{
		EventId: proto.String(eventId),
	}

	t := mproto.Command_RemoveEventCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_RemoveEventCommand_Command, val); err != nil {
		panic(err)
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) createMigrateEvent(e MigrateEvent) error {
	val := &mproto.CreateEventCommand{
		EventInfo: e.marshalEvent(),
	}
	t := mproto.Command_CreateEventCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_CreateEventCommand_Command, val); err != nil {
		panic(err)
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) updateMigrateEvent(e MigrateEvent) error {
	val := &mproto.UpdateEventCommand{
		EventInfo: e.marshalEvent(),
	}
	t := mproto.Command_UpdateEventCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdateEventCommand_Command, val); err != nil {
		panic(err)
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) updatePtInfo(db string, ptInfo *meta.PtInfo, ownerNode uint64, status meta.PtStatus) error {
	val := &mproto.UpdatePtInfoCommand{
		Db:     proto.String(db),
		Pt:     ptInfo.Marshal(),
		Status: proto.Uint32(uint32(status)),
	}

	if ownerNode > 0 {
		val.OwnerNode = proto.Uint64(ownerNode)
	}
	t := mproto.Command_UpdatePtInfoCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdatePtInfoCommand_Command, val); err != nil {
		panic(err)
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) refreshDbPt(dbPt *meta.DbPtInfo) {
	s.mu.RLock()
	dbPt.Pti.Status = s.data.PtView[dbPt.Db][dbPt.Pti.PtId].Status
	dbPt.Pti.Owner = s.data.PtView[dbPt.Db][dbPt.Pti.PtId].Owner
	s.mu.RUnlock()
}

func (s *Store) getEventOpId(e MigrateEvent) uint64 {
	var opId uint64
	s.mu.RLock()
	mei := s.data.MigrateEvents[e.getEventId()]
	if mei != nil {
		opId = mei.GetOpId()
	}
	s.mu.RUnlock()
	return opId
}

func (s *Store) getEvents() []*meta.MigrateEventInfo {
	s.mu.RLock()
	events := make([]*meta.MigrateEventInfo, 0, len(s.data.MigrateEvents))
	for _, e := range s.data.MigrateEvents {
		events = append(events, e.Clone())
	}
	s.mu.RUnlock()
	return events
}

func (s *Store) getFailedDbPts(ownerNode uint64, status meta.PtStatus) []*meta.DbPtInfo {
	s.mu.RLock()
	ptInfos := s.data.GetFailedPtInfos(ownerNode, status)
	s.mu.RUnlock()
	return ptInfos
}

func (s *Store) getDbPtNumPerAliveNode() *map[uint64]uint32 {
	nodePtNumMap := make(map[uint64]uint32)
	s.mu.RLock()
	for _, dataNode := range s.data.DataNodes {
		if dataNode.SegregateStatus == meta.Normal && dataNode.Status == serf.StatusAlive && dataNode.AliveConnID == dataNode.ConnID {
			nodePtNumMap[dataNode.ID] = 0
		}
	}
	for db := range s.data.PtView {
		for _, ptInfo := range s.data.PtView[db] {
			if _, ok := nodePtNumMap[ptInfo.Owner.NodeID]; ok {
				nodePtNumMap[ptInfo.Owner.NodeID]++
			}
		}
	}
	s.mu.RUnlock()
	return &nodePtNumMap
}

func (s *Store) shouldTakeOver() bool {
	return s.data.TakeOverEnabled
}

func (s *Store) getDbPtsByDbname(db string, enableTagArray bool, replicasN uint32) ([]*meta.DbPtInfo, error) {
	s.mu.RLock()
	ptInfos, err := s.data.GetPtInfosByDbname(db, enableTagArray, replicasN)
	s.mu.RUnlock()
	return ptInfos, err
}

func (s *Store) getDbPtsByDbnameV2(db string) []*meta.DbPtInfo {
	s.mu.RLock()
	ptInfos := s.data.GetPtInfosByDbnameV2(db)
	s.mu.RUnlock()
	return ptInfos
}

func TransMeasurementInfos2Bytes(dataTypes []int64, rpInfos *meta.RetentionPolicyInfo) ([]byte, error) {
	d := &meta.RpMeasurementsFieldsInfo{
		MeasurementInfos: make([]*meta.MeasurementFieldsInfo, 0),
	}
	msts := rpInfos.Measurements
	for mst, mstInfo := range msts {
		info := &meta.MeasurementFieldsInfo{
			MstName: mst,
		}
		info.TypeFields = mstInfo.FindMstInfos(dataTypes)
		d.MeasurementInfos = append(d.MeasurementInfos, info)
	}
	return d.MarshalBinary()
}

func (s *Store) ExpandGroups() error {
	val := &mproto.ExpandGroupsCommand{}
	t := mproto.Command_ExpandGroupsCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_ExpandGroupsCommand_Command, val); err != nil {
		return err
	}

	return s.ApplyCmd(cmd)
}

func (s *Store) updatePtVersion(db string, ptId uint32) error {
	val := &mproto.UpdatePtVersionCommand{
		Db: proto.String(db),
		Pt: proto.Uint32(ptId),
	}
	t := mproto.Command_UpdatePtVersionCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_UpdatePtVersionCommand_Command, val); err != nil {
		panic(err)
	}
	return s.ApplyCmd(cmd)
}

func (s *Store) getPtVersion(db string, ptId uint32) uint64 {
	s.mu.RLock()
	ver := s.data.PtView[db][ptId].Ver
	s.mu.RUnlock()
	return ver
}

func (s *Store) getPtStatus(db string, ptId uint32) (meta.PtStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.data.PtView[db]; !ok {
		return meta.Offline, errno.NewError(errno.PtNotFound)
	}
	status := s.data.PtView[db][ptId].Status
	return status, nil
}

func (s *Store) getReplicationGroup(db string) []meta.ReplicaGroup {
	s.mu.RLock()
	repGroups := s.data.ReplicaGroups[db]
	s.mu.RUnlock()
	return repGroups
}

func (s *Store) getDBPtInfos(db string) meta.DBPtInfos {
	s.mu.RLock()
	ptInfos := s.data.PtView[db]
	s.mu.RUnlock()
	return ptInfos
}

// locked by the caller
func (s *Store) getDbPtsByNodeId(nodeId uint64) map[string][]uint32 {
	dbPtIds := make(map[string][]uint32)
	for db, ptInfos := range s.data.PtView {
		for i := range ptInfos {
			if ptInfos[i].Owner.NodeID != nodeId {
				continue
			}
			if _, ok := dbPtIds[db]; !ok {
				dbPtIds[db] = make([]uint32, 0, len(ptInfos))
			}
			dbPtIds[db] = append(dbPtIds[db], ptInfos[i].PtId)
		}
	}
	return dbPtIds
}

func (s *Store) getDBBriefInfo(dbName string) ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	d := &meta.DatabaseBriefInfo{
		Name: dbName,
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.data.Databases[dbName]; !ok {
		return nil, errno.NewError(errno.DatabaseNotFound)
	}

	d.EnableTagArray = s.data.Databases[dbName].EnableTagArray
	d.Replicas = s.data.Databases[dbName].ReplicaN
	return d.Marshal()
}

func (s *Store) getDataNodeAliveConnId(nodeId uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node := s.data.DataNode(nodeId)
	if node == nil {
		return 0, errno.NewError(errno.DataNodeNotFound)
	}
	return node.AliveConnID, nil
}

func (s *Store) registerQueryIDOffset(host meta.SQLHost) (uint64, error) {
	val := &mproto.RegisterQueryIDOffsetCommand{
		Host: proto.String(string(host)),
	}
	t := mproto.Command_RegisterQueryIDOffsetCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_RegisterQueryIDOffsetCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		return 0, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, ok := s.data.QueryIDInit[host]
	if !ok {
		return 0, fmt.Errorf("register query id failed, host: %s", host)
	}
	return offset, nil
}

func (s *Store) leadershipTransfer() error {
	if s.raft == nil {
		return errors.New("raft state not create")
	}
	return s.raft.LeadershipTransfer()
}

// cmd sample: limit|192.168.0.11,192.168.0.12 or unlimit|xxxx  or delete|xxxx
func (s *Store) SpecialCtlData(cmd string) error {
	s.segregateMu.Lock()
	defer s.segregateMu.Unlock()
	if !s.IsLeader() {
		return errno.NewError(errno.MetaIsNotLeader)
	}
	infos := strings.Split(cmd, "|")
	if len(infos) != 2 {
		return errors.New("invalid command")
	}

	cmdKey := strings.ToLower(infos[0])
	nodeLst := strings.Split(infos[1], ",")
	switch cmdKey {
	case "limit":
		nodeIds, address, err := s.data.GetNodeIdsByNodeLst(nodeLst)
		if err != nil {
			return err
		}
		return s.segregateNode(nodeIds, address, false)
	case "unlimit":
		return s.cancelSegregateNode(nodeLst)
	case "delete":
		nodeIds, address, err := s.data.GetNodeIdsByNodeLst(nodeLst)
		if err != nil {
			return err
		}
		return s.segregateNode(nodeIds, address, true)
	default:
		return fmt.Errorf("unknown command: %s", cmdKey)
	}
}

func (s *Store) segregateNodeSimple(nodeIds []uint64, remove bool) error {
	// 1.set Node SegregateStatus to Segregated or remove node
	preSegregateStatus, err := s.data.GetNodeSegregateStatus(nodeIds)
	if err != nil {
		return err
	}
	if !remove {
		segregatedStatus := make([]uint64, len(nodeIds))
		for i := range segregatedStatus {
			segregatedStatus[i] = meta.Segregated
		}
		if err := s.SetSegregateNodeStatus(segregatedStatus, nodeIds); err != nil {
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		s.Logger.Info("segregateNode finish", zap.Uint64s("node ids", nodeIds))
	} else {
		if err := s.RemoveNode(nodeIds); err != nil {
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		s.Logger.Info("removeNode finish", zap.Uint64s("node ids", nodeIds))
	}
	return nil
}

func (s *Store) setNodeToSegregating(nodeIds []uint64) ([]uint64, error) {
	preSegregateStatus, err := s.data.GetNodeSegregateStatus(nodeIds)
	if err != nil {
		return preSegregateStatus, err
	}
	segregatingStatus := make([]uint64, len(nodeIds))
	for i := range segregatingStatus {
		segregatingStatus[i] = meta.Segregating
	}
	err = s.SetSegregateNodeStatus(segregatingStatus, nodeIds)
	if err != nil {
		if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
			err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
		}
		return preSegregateStatus, err
	}
	return preSegregateStatus, nil
}

func (s *Store) forceNodeToTakeOverPts(nodeIds []uint64, preSegregateStatus []uint64) error {
	for _, nodeId := range nodeIds {
		dbptInfos := s.data.GetPtInfosByNodeId(nodeId)
		nodePtNumMap := globalService.store.getDbPtNumPerAliveNode()
		if len(*nodePtNumMap) == 0 {
			err := fmt.Errorf("no alive node to takeover segregate node pts")
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		targetId, err := globalService.clusterManager.getTakeOverNode(globalService.clusterManager, nodeId, nodePtNumMap, true)
		if err != nil {
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		if targetId == nodeId {
			err := fmt.Errorf("no alive node to takeover segregate node pts")
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		aliveConnId, err := globalService.store.getDataNodeAliveConnId(targetId)
		if err != nil {
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		for _, dbptInfo := range dbptInfos {
			err = globalService.balanceManager.forceMoveDbPt(dbptInfo, nodeId, targetId, aliveConnId)
			if err != nil {
				if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
					err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
				}
				return err
			}
		}
	}
	return nil
}

func (s *Store) WaitNodeTakeOverDone(nodeIds []uint64, address []string, preSegregateStatus []uint64) error {
	failNodeLoc, err := globalService.store.NetStore.SendSegregateNodeCmds(nodeIds, address)
	if err != nil {
		if err1 := s.SetSegregateNodeStatus(preSegregateStatus[failNodeLoc:], nodeIds[failNodeLoc:]); err1 != nil {
			err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			return err
		}
		segregatedStatus := make([]uint64, failNodeLoc)
		for i := range segregatedStatus {
			segregatedStatus[i] = meta.Segregated
		}
		err := fmt.Errorf("first fail node loc: %d when %v", failNodeLoc, err.Error())
		if err1 := s.SetSegregateNodeStatus(segregatedStatus, nodeIds[0:failNodeLoc]); err1 != nil {
			err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
		}
		return err
	}
	return nil
}

func (s *Store) setNodeToSegregatedOrDelete(nodeIds []uint64, preSegregateStatus []uint64, delete bool) error {
	if !delete {
		segregatedStatus := make([]uint64, len(nodeIds))
		for i := range segregatedStatus {
			segregatedStatus[i] = meta.Segregated
		}
		if err := s.SetSegregateNodeStatus(segregatedStatus, nodeIds); err != nil {
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		s.Logger.Info("segregateNode finish", zap.Uint64s("node ids", nodeIds))
	} else {
		if err := s.RemoveNode(nodeIds); err != nil {
			if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
				err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
			}
			return err
		}
		s.Logger.Info("removeNode finish", zap.Uint64s("node ids", nodeIds))
	}
	return nil
}

func (s *Store) segregateNode(nodeIds []uint64, address []string, delete bool) error {
	if config.GetHaPolicy() != config.SharedStorage {
		return s.segregateNodeSimple(nodeIds, delete)
	}
	// 1.set Node SegregateStatus to Segregating
	preSegregateStatus, err := s.setNodeToSegregating(nodeIds)
	if err != nil {
		return err
	}
	// 2.wait migration tasks of node done
	if err := s.checkSegregateNodeTaskDone(nodeIds); err != nil {
		if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
			err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
		}
		return err
	}
	s.Logger.Info("segregateNode wait migrations done", zap.Uint64s("node ids", nodeIds))
	// 3.force move dbpts of node to other node
	if err := s.forceNodeToTakeOverPts(nodeIds, preSegregateStatus); err != nil {
		return err
	}
	// 4.send Segregate cmds to ts-store and wait ts-store Segregate cmds all done
	if err := s.WaitNodeTakeOverDone(nodeIds, address, preSegregateStatus); err != nil {
		return err
	}
	// 5.wait force assign tasks done
	if err := s.checkSegregateNodeTaskDone(nodeIds); err != nil {
		if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
			err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
		}
		return err
	}
	s.Logger.Info("segregateNode wait force takeover done", zap.Uint64s("node ids", nodeIds))
	// 6.set Node SegregateStatus to Segregated or remove node
	if err := s.setNodeToSegregatedOrDelete(nodeIds, preSegregateStatus, delete); err != nil {
		return err
	}
	// 7.success return to client
	return nil
}

func (s *Store) cancelSegregateNode(nodeLst []string) error {
	// 1.get nodeIds by nodeLst
	var nodeIds []uint64
	var err error
	if len(nodeLst) == 1 && strings.ToLower(nodeLst[0]) == "all" {
		// unlimit|all
		nodeIds = s.data.GetNodeIDs()
	} else {
		nodeIds, _, err = s.data.GetNodeIdsByNodeLst(nodeLst)
		if err != nil {
			return err
		}
	}
	// 2.set Node SegregateStatus to Normal
	preSegregateStatus, err := s.data.GetNodeSegregateStatus(nodeIds)
	if err != nil {
		return err
	}
	NormalStatus := make([]uint64, len(nodeIds))
	for i := range NormalStatus {
		NormalStatus[i] = meta.Normal
	}
	err = s.SetSegregateNodeStatus(NormalStatus, nodeIds)
	if err != nil {
		if err1 := s.SetSegregateNodeStatus(preSegregateStatus, nodeIds); err1 != nil {
			err = fmt.Errorf("set preSegregateStatus fail %v when %v", err1.Error(), err.Error())
		}
		return err
	}
	s.Logger.Info("cancel segregateNode finish", zap.Uint64s("node ids", nodeIds))
	return nil
}

func (s *Store) RemoveNode(nodeIds []uint64) error {
	val := &mproto.RemoveNodeCommand{
		NodeIds: nodeIds,
	}
	t := mproto.Command_RemoveNodeCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_RemoveNodeCommand_Command, val); err != nil {
		panic(err)
	}
	err := s.ApplyCmd(cmd)
	if err != nil {
		return err
	}
	return nil
}

// The check is performed every 500 ms. The check times out after 60s.
func (s *Store) checkSegregateNodeTaskDone(nodeIds []uint64) error {
	timer := time.NewTimer(checkSegregateTimeout)
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("segregate timeout because of too many pt migrations or pt migrations error")
		default:
			if !globalService.msm.CheckNodeEventExsit(nodeIds) {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (s *Store) SetSegregateNodeStatus(status []uint64, nodeIds []uint64) error {
	val := &mproto.SetNodeSegregateStatusCommand{
		Status:  status,
		NodeIds: nodeIds,
	}
	t := mproto.Command_SetNodeSegregateStatusCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_SetNodeSegregateStatusCommand_Command, val); err != nil {
		panic(err)
	}
	err := s.ApplyCmd(cmd)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) verifyDataNodeStatus(nodeID uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeIndex, err := s.data.GetNodeIndex(nodeID)
	if err != nil {
		return err
	}
	dataNode := s.data.DataNodes[nodeIndex]
	// The data node has not joined into the cluster
	if dataNode.AliveConnID != dataNode.ConnID {
		return nil
	}
	if dataNode.Status == serf.StatusFailed {
		return errno.NewError(errno.DataNoAlive, nodeID)
	}

	return s.PtCheck(nodeID, s.data.PtView)
}

func (s *Store) PtCheck(nodeID uint64, pts map[string]meta.DBPtInfos) error {
	for _, value := range pts {
		for _, pt := range value {
			owner := pt.Owner
			status := pt.Status
			if owner.NodeID == nodeID && status != meta.Online {
				return errno.NewError(errno.PtNotFound, nodeID)
			}
		}
	}
	return nil
}

func (s *Store) ModifyRepDBMasterPt(db string, rgId uint32, newMasterPtId uint32) error {
	if config.GetHaPolicy() != config.Replication {
		return fmt.Errorf("ha-policy is not replication")
	}
	s.mu.RLock()
	newMasterId, newPeers, err := s.data.GetNewRg(db, rgId, newMasterPtId)
	s.mu.RUnlock()
	if err != nil {
		s.Logger.Error("ModifyRepDBMasterPt err", zap.String("db", db), zap.Uint32("rgId", rgId), zap.Uint32("newMasterPtId", newMasterPtId), zap.Error(err))
		return err
	}
	return globalService.store.updateReplication(db, rgId, newMasterId, newPeers)
}

func (s *Store) RecoverMetaData(databases []string, metaData []byte, nodeMap map[uint64]uint64) error {
	return globalService.store.recoverMetaData(databases, metaData, nodeMap)
}

func (s *Store) recoverMetaData(databases []string, metaData []byte, nodeMap map[uint64]uint64) error {
	val := &mproto.RecoverMetaDataCommand{
		Databases: databases,
		MetaData:  metaData,
		NodeMap:   nodeMap,
	}
	t := mproto.Command_RecoverMetaData
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_RecoverMetaDataCommand_Command, val); err != nil {
		return err
	}

	return s.ApplyCmd(cmd)
}

func (s *Store) TransferLeadershipEnd(database string, rgId, oldMasterId, newMasterId uint32, err error) {
	if err != nil {
		s.Logger.Error("end transferLeadership in ts-meta fail", zap.String("db", database), zap.Uint32("rgId", rgId),
			zap.Uint32("oldPtId", oldMasterId), zap.Uint32("newPtId", newMasterId), zap.Error(err))
	} else {
		s.Logger.Info("end transferLeadership in ts-meta success", zap.String("db", database), zap.Uint32("rgId", rgId),
			zap.Uint32("oldPtId", oldMasterId), zap.Uint32("newPtId", newMasterId))
	}
}

func (s *Store) TransferLeadership(database string, rgId, oldMasterId, newMasterId uint32) {
	if oldMasterId == newMasterId {
		return
	}
	var err error
	var nodeId uint64
	s.Logger.Info("start transferLeadership in ts-meta", zap.String("db", database), zap.Uint32("rgId", rgId),
		zap.Uint32("oldPtId", oldMasterId), zap.Uint32("newPtId", newMasterId))

	s.mu.RLock()
	nodeId, err = s.data.GetDbPtOwner(database, oldMasterId)
	s.mu.RUnlock()
	if err != nil {
		s.TransferLeadershipEnd(database, rgId, oldMasterId, newMasterId, err)
		return
	}
	err = s.NetStore.TransferLeadership(database, nodeId, oldMasterId, newMasterId)
	s.TransferLeadershipEnd(database, rgId, oldMasterId, newMasterId, err)
}

func (s *Store) ShowCluster(body []byte) ([]byte, error) {
	if !s.IsLeader() {
		return nil, raft.ErrNotLeader
	}
	var cmd mproto.Command
	if err := proto.Unmarshal(body, &cmd); err != nil {
		return nil, err
	}

	ext, _ := proto.GetExtension(&cmd, mproto.E_ShowClusterCommand_Command)
	v, ok := ext.(*mproto.ShowClusterCommand)
	if !ok {
		panic(fmt.Errorf("%s is not a ShowClusterCommand", ext))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	clusterInfo, err := s.data.ShowCluster(v.GetNodeType(), v.GetID())
	if err != nil {
		return nil, err
	}
	return clusterInfo.MarshalBinary()
}

func (s *Store) GetFailedDbPtsForRep(ownerNode uint64, status meta.PtStatus) (map[string][]*meta.DbPtInfo, []*meta.DbPtInfo) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resPtInfos := make([]*meta.DbPtInfo, 0, s.data.GetClusterPtNum())
	rgPtInfoMap := make(map[string][]*meta.DbPtInfo)
	for db := range s.data.PtView {
		// do not get pt which db mark deleted
		if s.data.Databases[db] == nil || s.data.Database(db).MarkDeleted {
			continue
		}
		dbInfo := s.data.GetDBBriefInfo(db)
		dbInfo.Replicas = s.data.Databases[db].ReplicaN
		if dbInfo.Replicas <= 1 {
			for i := range s.data.PtView[db] {
				if s.data.PtView[db][i].Owner.NodeID == ownerNode && s.data.PtView[db][i].Status == status {
					shards := s.data.GetShardDurationsByDbPtForRetention(db, s.data.PtView[db][i].PtId)
					pt := s.data.PtView[db][i]
					resPtInfos = append(resPtInfos, &meta.DbPtInfo{Db: db, Pti: &pt, Shards: shards, DBBriefInfo: dbInfo})
				}
			}
		} else {
			rgs := make(map[uint32]*meta.ReplicaGroup)
			for i := range s.data.PtView[db] {
				if s.data.PtView[db][i].Owner.NodeID == ownerNode {
					rgId := s.data.PtView[db][i].RGID
					rg := s.data.GetRGOfPtFast(rgId, db)
					if s.data.PtView[db][i].Status != status || rg == nil || rg.Status == meta.UnFull {
						if rg == nil {
							s.Logger.Info("The rep assign condition is not met,rg is nil", zap.Uint32("status", uint32(s.data.PtView[db][i].Status)))
						} else {
							s.Logger.Info("The rep assign condition is not met", zap.Uint32("status", uint32(s.data.PtView[db][i].Status)), zap.Uint8("rg.status", uint8(rg.Status)))
						}
						continue
					}
					rgs[rgId] = rg
				}
			}
			for i := range s.data.PtView[db] {
				pt := s.data.PtView[db][i]
				for j := range rgs {
					rg := rgs[j]
					key := db + strconv.FormatUint(uint64(rg.ID), 10)
					if rg.MasterPtID == pt.PtId {
						ptInfos := rgPtInfoMap[key]
						shards := s.data.GetShardDurationsByDbPtForRetention(db, s.data.PtView[db][i].PtId)
						ptInfos = append(ptInfos, &meta.DbPtInfo{Db: db, Pti: &pt, Shards: shards, DBBriefInfo: dbInfo})
						rgPtInfoMap[key] = ptInfos
						continue
					}
					for k := range rg.Peers {
						if rg.Peers[k].ID == pt.PtId {
							ptInfos := rgPtInfoMap[key]
							shards := s.data.GetShardDurationsByDbPtForRetention(db, s.data.PtView[db][i].PtId)
							ptInfos = append(ptInfos, &meta.DbPtInfo{Db: db, Pti: &pt, Shards: shards, DBBriefInfo: dbInfo})
							rgPtInfoMap[key] = ptInfos
						}
					}
				}
			}
		}
	}
	return rgPtInfoMap, resPtInfos
}
