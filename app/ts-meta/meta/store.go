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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	mclient "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/rand"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	mproto "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"go.uber.org/zap"
)

// Retention policy settings.
const (
	autoCreateRetentionPolicyName   = "autogen"
	autoCreateRetentionPolicyPeriod = 0

	// maxAutoCreatedRetentionPolicyReplicaN is the maximum replication factor that will
	// be set for auto-created retention policies.
	maxAutoCreatedRetentionPolicyReplicaN = 1

	reportTimeSpan = 10 * time.Second

	checkInterval = 500 * time.Millisecond

	updateCacheInterval = 100 * time.Millisecond

	pushInterval = 10 * time.Second
)

// Raft configuration.
const (
	raftListenerStartupTimeout = time.Second
)

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

type Store struct {
	mu      sync.RWMutex
	closing chan struct{}

	config *config.Meta

	data        *meta.Data
	dataChanged chan struct{}
	path        string
	opened      bool
	Logger      *logger.Logger
	raft        *raftWrapper
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
	}

	statMu       sync.RWMutex
	dbStatistics map[string]*dbInfo

	cacheMu          sync.RWMutex
	cacheData        *meta.Data
	cacheDataBytes   []byte
	cacheDataChanged chan struct{}
}

// NewStore will create a new metaStore with the passed in config
func NewStore(c *config.Meta, httpAddr, rpcAddr, raftAddr string) *Store {
	s := Store{
		data: &meta.Data{
			Index:        1,
			PtNumPerNode: meta.DefaultPtNumPerNode,
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
	}

	return &s
}

func (s *Store) checkLeaderChanged() {
	defer s.wg.Done()
	pushTimer := time.NewTimer(pushInterval)
	defer pushTimer.Stop()
	lastState := s.raft.raft.State()
	stat.NewMetaStatCollector().Push(&stat.MetaRaftStatItem{Status: int64(lastState),
		NodeID: strconv.FormatUint(s.Node.ID, 10)})
	for {
		select {
		case v := <-s.notifyCh:
			stat.NewMetaStatistics().AddLeaderSwitchTotal(1)
			if v {
				s.stepDown = make(chan struct{})
				if s.cm != nil {
					s.cm.Start()
				}

				s.deleteWg.Add(3)
				go s.checkDatabaseDelete()
				go s.checkRpDelete()
				go s.checkMeasurementDelete()
				continue
			}

			stat.NewMetaStatCollector().Clear(stat.TypeMetaStatItem)
			close(s.stepDown)
			if s.cm != nil {
				s.cm.Stop()
			}
			s.deleteWg.Wait()
		case <-s.closing:
			return
		case <-pushTimer.C:
			currentState := s.raft.raft.State()
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
	peers := s.connectFull(c)
	if err := s.setOpen(); err != nil {
		return err
	}

	err := s.newRaftWrapper(raftln, peers)
	if err != nil {
		return err
	}

	if err = s.waitForLeader(); err != nil {
		return err
	}

	if err = s.joinMetaServer(c); err != nil {
		return err
	}

	s.wg.Add(2)
	go s.serveSnapshot()
	go s.checkLeaderChanged()

	return nil
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
	c.SetMetaServers(s.config.JoinPeers)
	c.SetTLS(s.config.HTTPSEnabled)
	return c
}

func (s *Store) connectFull(c *mclient.Client) []string {
	var peers []string
	raftAddr := s.config.CombineDomain(s.raftAddr)

	for {
		peers = c.Peers()
		if !mclient.Peers(peers).Contains(raftAddr) {
			peers = append(peers, raftAddr)
		}

		if len(peers) >= len(s.config.JoinPeers) {
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

//getData is used to get the Data in the Store
func (s *Store) GetData() *meta.Data {
	s.mu.RLock()
	data := s.data
	s.mu.RUnlock()
	return data
}

// peers returns the raft peers known to this Store
func (s *Store) peers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft == nil {
		return []string{s.config.CombineDomain(s.raftAddr)}
	}
	peers, _ := s.raft.peers()
	if len(peers) == 0 {
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
func (s *Store) checkMeasurementDelete() {
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
		s.cacheData.WalkDatabases(func(db *meta.DatabaseInfo) {
			db.WalkRetentionPolicy(func(rp *meta.RetentionPolicyInfo) {
				for mstIdx := range rp.Measurements {
					if rp.Measurements[mstIdx].MarkDeleted {
						n++
						go func(db string, rp *meta.RetentionPolicyInfo, mst string) {
							errChan <- s.deleteMeasurement(db, rp, mst)
						}(db.Name, rp.Clone(), rp.Measurements[mstIdx].Name)
					}
				}
			})
		})
		s.cacheMu.RUnlock()
		for i := 0; i < n; i++ {
			err := <-errChan
			if err != nil {
				s.Logger.Warn("delete measurement failed", zap.Error(err))
			}
		}

		time.Sleep(checkInterval)
	}
}

func (s *Store) deleteMeasurement(db string, rp *meta.RetentionPolicyInfo, mst string) error {
	s.cacheMu.RLock()
	nodeShardsMap := make(map[uint64][]uint64)
	for sgIdx := range rp.ShardGroups {
		for shIdx := range rp.ShardGroups[sgIdx].Shards {
			if rp.ShardGroups[sgIdx].Shards[shIdx].ContainPrefix(mst) {
				ptId := rp.ShardGroups[sgIdx].Shards[shIdx].Owners[0]
				nodeId := s.cacheData.PtView[db][ptId].Owner.NodeID
				nodeShardsMap[nodeId] = append(nodeShardsMap[nodeId], rp.ShardGroups[sgIdx].Shards[shIdx].ID)
			}
		}
	}

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
	for i := 0; i < n; i++ {
		err := <-errChan
		if err != nil {
			return err
		}
	}
	close(errChan)

	return s.deleteMeasurementMetaData(db, rp.Name, mst)
}

func (s *Store) checkRpDelete() {
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
		s.cacheMu.RUnlock()

		for i := 0; i < n; i++ {
			err := <-errChan
			if err != nil {
				s.Logger.Warn("drop retentionPolicy failed", zap.Error(err))
			}
		}

		time.Sleep(checkInterval)
	}
}

func (s *Store) deleteRetentionPolicy(db string, rp string) error {
	s.cacheMu.RLock()
	ptInfos := s.cacheData.PtView[db]

	errChan := make(chan error)
	n := 0
	for i := range ptInfos {
		node := s.cacheData.DataNode(ptInfos[i].Owner.NodeID)
		if node == nil {
			continue
		}
		n++
		go func(node *meta.DataNode, db string, rp string, pt uint32) {
			errChan <- s.NetStore.DeleteRetentionPolicy(node, db, rp, pt)
		}(node, db, rp, ptInfos[i].PtId)
	}
	s.cacheMu.RUnlock()
	for i := 0; i < n; i++ {
		err := <-errChan
		if err != nil {
			return err
		}
	}
	close(errChan)

	return s.deleteRpMetadata(db, rp)
}

func (s *Store) checkDatabaseDelete() {
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
		for dbName := range s.cacheData.Databases {
			if s.cacheData.Databases[dbName].MarkDeleted {
				n++
				go func(dbName string) {
					errChan <- s.deleteDatabase(dbName)
				}(dbName)
			}
		}
		s.cacheMu.RUnlock()
		for i := 0; i < n; i++ {
			err := <-errChan
			if err != nil {
				s.Logger.Warn("drop database failed", zap.Error(err))
			}
		}

		time.Sleep(checkInterval)
	}
}

func (s *Store) deleteDatabase(dbName string) error {
	s.cacheMu.RLock()
	ptInfos := s.cacheData.PtView[dbName]

	errChan := make(chan error)
	n := 0
	for i := range ptInfos {
		node := s.cacheData.DataNode(ptInfos[i].Owner.NodeID)
		if node == nil {
			continue
		}
		n++
		go func(node *meta.DataNode, dbName string, ptId uint32) {
			errChan <- s.NetStore.DeleteDatabase(node, dbName, ptId)
		}(node, dbName, ptInfos[i].PtId)
	}
	s.cacheMu.RUnlock()

	for i := 0; i < n; i++ {
		err := <-errChan
		if err != nil {
			return err
		}
	}
	close(errChan)

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
	return s.raft.close()
}

func (s *Store) getSnapshot() []byte {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	return s.cacheDataBytes
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
	return s.raft.isLeader()
}

// isCandidate returns true if the Store is currently the Candidate.
func (s *Store) isCandidate() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.raft.isCandidate()
}

// leader returns what the Store thinks is the current leader. An empty
// string indicates no leader exists.
func (s *Store) leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.raft.leader()
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
	return s.raft.apply(b)
}

func (s *Store) Join(n *meta.NodeInfo) (*meta.NodeInfo, error) {
	if err := s.raft.addServer(n.TCPHost); err != nil {
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

func (s *Store) showDebugInfo(witch string) ([]byte, error) {
	if strings.Contains(witch, "raft") {
		return s.raft.showDebugInfo(witch)
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

func (s *Store) createDataNode(writeHost, queryHost string) ([]byte, error) {
	val := &mproto.CreateDataNodeCommand{
		HTTPAddr: proto.String(writeHost),
		TCPAddr:  proto.String(queryHost),
	}

	t := mproto.Command_CreateDataNodeCommand
	cmd := &mproto.Command{Type: &t}
	if err := proto.SetExtension(cmd, mproto.E_CreateDataNodeCommand_Command, val); err != nil {
		panic(err)
	}

	err := s.ApplyCmd(cmd)
	if err != nil {
		return nil, err
	}

	nodeStartInfo := meta.NodeStartInfo{}
	s.cacheMu.RLock()
	dn := s.cacheData.DataNodeByHttpHost(writeHost)
	if dn == nil {
		s.cacheMu.RUnlock()
		s.updateCacheData()
		s.cacheMu.RLock()
		dn = s.cacheData.DataNodeByHttpHost(writeHost)
	}
	nodeStartInfo.NodeId = dn.ID
	nodeStartInfo.LTime = uint64(dn.LTime)
	nodeStartInfo.PtIds = s.cacheData.GetPtsByNodeId(nodeStartInfo.NodeId)
	nodeStartInfo.ShardDurationInfos = s.cacheData.GetDurationInfos(nodeStartInfo.PtIds)
	s.cacheMu.RUnlock()
	stat.NewMetaStatCollector().Push(&stat.MetaStatItem{Status: int64(dn.Status),
		LTime: int64(dn.LTime), NodeID: strconv.FormatUint(dn.ID, 10), Host: dn.Host})
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
		b, err = s.getTimeRange(&cmd)
	case mproto.Command_ShardDurationCommand:
		b, err = s.getDurationInfo(&cmd)
	default:
		err = fmt.Errorf("non suportted getAuxShardInfo cmd")
	}
	return b, err
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

func (s *Store) getDurationInfo(cmd *mproto.Command) ([]byte, error) {
	ext, _ := proto.GetExtension(cmd, mproto.E_ShardDurationCommand_Command)
	v := ext.(*mproto.ShardDurationCommand)
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	if s.cacheData.Index < v.GetIndex() {
		return nil, errno.NewError(errno.DataIsOlder)
	}

	durationRes := s.cacheData.DurationInfos(v.GetPts())
	return durationRes.MarshalBinary()
}

func (s *Store) updateNodeStatus(id uint64, status int32, lTime uint64, gossipAddr string) error {
	val := &mproto.UpdateNodeStatusCommand{
		ID:         proto.Uint64(id),
		Status:     proto.Int32(status),
		Ltime:      proto.Uint64(lTime),
		GossipAddr: proto.String(gossipAddr)}
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

func (s *Store) dataNodes() meta.DataNodeInfos {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.CloneDataNodes()
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
