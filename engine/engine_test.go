// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/record"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockMetaClient4Drop struct {
	metaclient.MetaClient
}

func (m *mockMetaClient4Drop) DatabaseOption(database string) (*obs.ObsOptions, error) {
	return nil, nil
}

type shardMock struct {
	rp      string
	id      uint64
	expired bool
}

var DefaultEngineOption netstorage.EngineOptions
var globalTime = influxql.TimeRange{
	Min: time.Unix(0, influxql.MinTime).UTC(),
	Max: time.Unix(0, influxql.MaxTime).UTC(),
}

func init() {
	DefaultEngineOption = netstorage.NewEngineOptions()
	DefaultEngineOption.WriteColdDuration = time.Second * 5000
	DefaultEngineOption.ShardMutableSizeLimit = 30 * 1024 * 1024
	DefaultEngineOption.NodeMutableSizeLimit = 1e9
	DefaultEngineOption.MaxWriteHangTime = time.Second
	DefaultEngineOption.MemDataReadEnabled = true
	DefaultEngineOption.WalSyncInterval = 100 * time.Millisecond
	DefaultEngineOption.WalEnabled = true
	DefaultEngineOption.WalReplayParallel = false
	DefaultEngineOption.WalReplayAsync = false
	DefaultEngineOption.DownSampleWriteDrop = true
	openShardsLimit = limiter.NewFixed(cpu.GetCpuNum())
	replayWalLimit = limiter.NewFixed(cpu.GetCpuNum())
}

func newMockDBPartitions() map[string]map[uint64]map[uint64]shardMock {
	DBPartitions := make(map[string]map[uint64]map[uint64]shardMock)

	shardMap1 := make(map[uint64]shardMock)
	shardMap1[1] = shardMock{rp: "rp1", id: 1, expired: true}
	shardMap1[2] = shardMock{rp: "rp1", id: 2, expired: false}
	shardMap1[3] = shardMock{rp: "rp2", id: 3, expired: false}
	shardMap1[4] = shardMock{rp: "rp3", id: 4, expired: true}
	shardMap1[5] = shardMock{rp: "rp2", id: 5, expired: false}
	shardMap1[6] = shardMock{rp: "rp1", id: 6, expired: false}

	shardMap2 := make(map[uint64]shardMock)
	shardMap2[7] = shardMock{rp: "rp1", id: 7, expired: true}
	shardMap2[8] = shardMock{rp: "rp3", id: 8, expired: false}
	shardMap2[9] = shardMock{rp: "rp2", id: 9, expired: false}
	shardMap2[10] = shardMock{rp: "rp4", id: 10, expired: true}
	shardMap2[11] = shardMock{rp: "rp2", id: 11, expired: false}
	shardMap2[12] = shardMock{rp: "rp4", id: 12, expired: false}
	shardMap2[13] = shardMock{rp: "rp4", id: 13, expired: true}
	shardMap2[14] = shardMock{rp: "rp2", id: 14, expired: false}
	shardMap2[15] = shardMock{rp: "rp4", id: 15, expired: true}

	ptMap := make(map[uint64]map[uint64]shardMock)
	ptMap[0] = shardMap1
	ptMap[1] = shardMap2

	DBPartitions["foo"] = ptMap

	return DBPartitions
}

func TestEngine_ExpiredShards(t *testing.T) {
	DBPartitions := newMockDBPartitions()
	res := make(map[string]map[string][]uint64)
	for db, ptMap := range DBPartitions {
		rpShardIDs := make(map[string][]uint64)

		// find expired shard from all PTs.
		for _, DBPTInfo := range ptMap {
			for sid := range DBPTInfo {
				if DBPTInfo[sid].expired {
					rpShardIDs[DBPTInfo[sid].rp] = append(rpShardIDs[DBPTInfo[sid].rp], sid)
				}
			}
		}

		for _, shardIDs := range rpShardIDs {
			for i := 0; i < len(shardIDs); i++ {
				sort.Slice(shardIDs, func(i, j int) bool {
					return shardIDs[i] < shardIDs[j]
				})
			}
		}

		if len(rpShardIDs) != 0 {
			res[db] = rpShardIDs
		}
	}

	for db := range res {
		for rp := range res[db] {
			switch rp {
			case "rp1":
				expected := []uint64{1, 7}
				assert(reflect.DeepEqual(res[db][rp], expected), fmt.Sprintf("expected %v, got %v", expected, res[db][rp]))
			case "rp3":
				expected := []uint64{4}
				assert(reflect.DeepEqual(res[db][rp], expected), fmt.Sprintf("expected %v, got %v", expected, res[db][rp]))
			case "rp4":
				expected := []uint64{10, 13, 15}
				assert(reflect.DeepEqual(res[db][rp], expected), fmt.Sprintf("expected %v, got %v", expected, res[db][rp]))
			default:
				t.Fatal(fmt.Sprintf("not expect rp:%s", rp))
			}
		}
	}
}

var dPath = "data_engine/"

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

func initEngine(dir string) (*Engine, error) {
	dataPath := filepath.Join(dir, dPath)
	eng := &Engine{
		closed:        interruptsignal.NewInterruptSignal(),
		dataPath:      dataPath + "/data",
		DBPartitions:  make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:    make(map[string]string),
		droppingRP:    make(map[string]string),
		droppingMst:   make(map[string]string),
		migratingDbPT: make(map[string]map[uint32]struct{}),
	}
	eng.log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	eng.engOpt.ShardMutableSizeLimit = 30 * 1024 * 1024
	eng.engOpt.NodeMutableSizeLimit = 1e9
	eng.engOpt.MaxWriteHangTime = time.Second

	loadCtx := getLoadCtx()
	eng.loadCtx = loadCtx
	eng.CreateDBPT(defaultDb, defaultPtId, false)
	eng.DBPartitions[defaultDb][defaultPtId].logger = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	shardTimeRange := getTimeRangeInfo()

	// init instance
	stat.StoreTaskInstance = stat.NewStoreTaskDuration(false)
	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
	}
	err := eng.CreateShard(defaultDb, defaultRp, defaultPtId, defaultShardId, shardTimeRange, msInfo)
	if err != nil {
		panic(err)
	}
	eng.metaClient = &mockMetaClient4Drop{}

	return eng, nil
}

func getTimeRangeInfo() *meta.ShardTimeRangeInfo {
	tr := meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1999-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z")}
	shardDuration := getShardDurationInfo(defaultShardId)
	timeRange := &meta.ShardTimeRangeInfo{
		TimeRange: tr,
		OwnerIndex: meta.IndexDescriptor{
			IndexID:      defaultShardId,
			IndexGroupID: defaultShGroupId,
			TimeRange:    tr,
		},
		ShardDuration: shardDuration,
	}
	return timeRange
}

func getShardDurationInfo(shId uint64) *meta.ShardDurationInfo {
	shardDuration := &meta.ShardDurationInfo{
		Ident: meta.ShardIdentifier{
			ShardID:      shId,
			ShardGroupID: defaultShGroupId,
			Policy:       defaultRp,
			OwnerDb:      defaultDb,
			OwnerPt:      defaultPtId},
		DurationInfo: meta.DurationDescriptor{
			Tier: util.Hot, TierDuration: time.Hour, Duration: time.Hour,
		},
	}
	return shardDuration
}

func getLoadCtx() *metaclient.LoadCtx {
	loadCtx := &metaclient.LoadCtx{}
	loadCtx.LoadCh = make(chan *metaclient.DBPTCtx, 6)
	go func(loadCtx *metaclient.LoadCtx) {
		for {
			select {
			case _ = <-loadCtx.LoadCh:
			}
		}
	}(loadCtx)
	return loadCtx
}

func initEngine1(dir string, engineType config.EngineType) (*Engine, error) {
	dataPath := filepath.Join(dir, dPath)
	eng := &Engine{
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath + "/data",
		walPath:      dataPath + "/wal",
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
		fileInfos:    nil,
	}
	eng.log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())

	loadCtx := getLoadCtx()
	lockPath := filepath.Join(eng.dataPath, "LOCK")
	dbPTInfo := NewDBPTInfo("db0", 0, eng.dataPath, eng.walPath, loadCtx, eng.fileInfos)
	dbPTInfo.lockPath = &lockPath
	dbPTInfo.logger = eng.log
	eng.addDBPTInfo(dbPTInfo)
	reportLoadFrequency = time.Millisecond // 1ms
	dbPTInfo.enableReportShardLoad()

	indexPath := path.Join(dbPTInfo.path, "rp0", config.IndexFileDirectory,
		"659_946252800000000000_946857600000000000", "mergeset")
	_ = fileops.MkdirAll(indexPath, 0755)

	//indexPath := path.Join(dbPTInfo.path, "rp0", IndexFileDirectory, "1_1648544460000000000_1648548120000000000")

	dbPTInfo.OpenIndexes(0, "rp0", config.TSSTORE)

	indexBuilder := dbPTInfo.indexBuilder[659]
	shardIdent := &meta.ShardIdentifier{ShardID: 1, ShardGroupID: 1, Policy: "rp0", OwnerDb: "db0", OwnerPt: 0}
	shardDuration := &meta.DurationDescriptor{Tier: util.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1999-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z")}
	shard := NewShard(eng.dataPath, eng.walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption, engineType, nil)
	shard.indexBuilder = indexBuilder
	//shard.wal.logger = eng.log
	//shard.wal.traceLogger = eng.log
	err := shard.OpenAndEnable(nil)
	if err != nil {
		_ = shard.Close()
		return nil, err
	}
	dbPTInfo.shards[shard.ident.ShardID] = shard
	return eng, nil
}

func initEngineWithColdShard(dir string, engineType config.EngineType) (*Engine, error) {
	dataPath := filepath.Join(dir, dPath)
	eng := &Engine{
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath + "/data",
		walPath:      dataPath + "/wal",
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
		fileInfos:    nil,
	}
	eng.log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())

	loadCtx := getLoadCtx()
	lockPath := filepath.Join(eng.dataPath, "LOCK")
	dbPTInfo := NewDBPTInfo("db0", 0, eng.dataPath, eng.walPath, loadCtx, eng.fileInfos)
	dbPTInfo.lockPath = &lockPath
	dbPTInfo.logger = eng.log
	eng.addDBPTInfo(dbPTInfo)
	reportLoadFrequency = time.Millisecond // 1ms
	dbPTInfo.enableReportShardLoad()

	indexPath := path.Join(dbPTInfo.path, "rp0", config.IndexFileDirectory,
		"659_946252800000000000_946857600000000000", "mergeset")
	_ = fileops.MkdirAll(indexPath, 0755)

	//indexPath := path.Join(dbPTInfo.path, "rp0", IndexFileDirectory, "1_1648544460000000000_1648548120000000000")

	dbPTInfo.OpenIndexes(0, "rp0", config.TSSTORE)

	indexBuilder := dbPTInfo.indexBuilder[659]
	shardIdent := &meta.ShardIdentifier{ShardID: 1, ShardGroupID: 1, Policy: "rp0", OwnerDb: "db0", OwnerPt: 0}
	shardDuration := &meta.DurationDescriptor{Tier: util.Cold, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1999-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z")}
	shard := NewShard(eng.dataPath, eng.walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption, engineType, nil)
	shard.indexBuilder = indexBuilder
	//shard.wal.logger = eng.log
	//shard.wal.traceLogger = eng.log
	err := shard.OpenAndEnable(nil)
	if err != nil {
		_ = shard.Close()
		return nil, err
	}
	dbPTInfo.shards[shard.ident.ShardID] = shard
	return eng, nil
}

func initEngineWithNoLockPath(dir string, engineType config.EngineType) (*Engine, error) {
	dataPath := filepath.Join(dir, dPath)
	eng := &Engine{
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath + "/data",
		walPath:      dataPath + "/wal",
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
		fileInfos:    nil,
	}
	eng.log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())

	loadCtx := getLoadCtx()
	lockPath := ""
	dbPTInfo := NewDBPTInfo("db0", 0, eng.dataPath, eng.walPath, loadCtx, eng.fileInfos)
	dbPTInfo.lockPath = &lockPath
	dbPTInfo.logger = eng.log
	eng.addDBPTInfo(dbPTInfo)
	reportLoadFrequency = time.Millisecond // 1ms
	dbPTInfo.enableReportShardLoad()

	indexPath := path.Join(dbPTInfo.path, "rp0", config.IndexFileDirectory,
		"659_946252800000000000_946857600000000000", "mergeset")
	_ = fileops.MkdirAll(indexPath, 0755)

	//indexPath := path.Join(dbPTInfo.path, "rp0", IndexFileDirectory, "1_1648544460000000000_1648548120000000000")

	dbPTInfo.OpenIndexes(0, "rp0", config.TSSTORE)

	indexBuilder := dbPTInfo.indexBuilder[659]
	shardIdent := &meta.ShardIdentifier{ShardID: 1, ShardGroupID: 1, Policy: "rp0", OwnerDb: "db0", OwnerPt: 0}
	shardDuration := &meta.DurationDescriptor{Tier: util.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1999-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z")}
	shard := NewShard(eng.dataPath, eng.walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption, engineType, nil)
	shard.indexBuilder = indexBuilder
	//shard.wal.logger = eng.log
	//shard.wal.traceLogger = eng.log
	err := shard.OpenAndEnable(nil)
	if err != nil {
		_ = shard.Close()
		return nil, err
	}
	dbPTInfo.shards[shard.ident.ShardID] = shard
	return eng, nil
}

func Test_Engine_DropDatabase(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// db not found
	err = eng.DeleteDatabase("db011", defaultPtId)
	assert(err == nil, "error is not nil")
	assert(len(eng.DBPartitions) > 0, "db pt should exist")

	// really drop db
	err = eng.DeleteDatabase("db0", defaultPtId)
	assert(err == nil, "error is not nil")
	assert(len(eng.DBPartitions) == 0, "db pt should not exist")
}

func Test_Engine_DropDatabaseConcurrent(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	res := make(chan error)
	n := 0
	n++
	go func(db string) {
		res <- eng.DeleteDatabase(db, defaultPtId)
	}("db0")

	n++
	go func(db string) {
		res <- eng.DeleteDatabase(db, defaultPtId)
	}("db0")

	for i := 0; i < n; i++ {
		err = <-res
		if err != nil {
			t.Fatal(err)
		}
	}
	close(res)

	if len(eng.DBPartitions) != 0 {
		t.Fatalf("partition not deleted n %d, db0 pt 0 %v ", n, eng.DBPartitions)
	}
}

func Test_Engine_DropDatabaseNoPt(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	err = eng.DeleteDatabase(defaultDb, 2)
	assert(err == nil, "error is not nil")
	assert(len(eng.DBPartitions) > 0, "db pt should exist")
}

func Test_Engine_DropDatabaseFail(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	require.NoError(t, err)
	defer eng.Close()

	dbPT := eng.getDBPTInfo(defaultDb, defaultPtId)
	dbPT.exeCount = 1

	DeleteDatabaseTimeout = time.Second / 10
	defer func() {
		DeleteDatabaseTimeout = time.Second * 15
	}()

	err = eng.DeleteDatabase(defaultDb, defaultPtId)
	assert(err != nil, "error is nil")
	assert(len(eng.DBPartitions) > 0, "db pt should exist")
}

func TestEngine_DropRetentionPolicy_SkipLog(t *testing.T) {
	dir := t.TempDir()
	config.SetProductType(config.LogKeeperService)
	defer config.SetProductType("")
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	res := make(chan error)
	n := 0
	n++
	go func(db, rp string) {
		res <- eng.DropRetentionPolicy(db, rp, defaultPtId)
	}("db0", "rp0")

	for i := 0; i < n; i++ {
		err = <-res
		if err != nil {
			t.Fatal(err)
		}
	}
	close(res)
}

func TestEngine_DropRetentionPolicy(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	res := make(chan error)
	n := 0
	n++
	go func(db, rp string) {
		res <- eng.DropRetentionPolicy(db, rp, defaultPtId)
	}("db0", "rp0")

	for i := 0; i < n; i++ {
		err = <-res
		if err != nil {
			t.Fatal(err)
		}
	}
	close(res)
	if len(eng.DBPartitions) != 1 {
		t.Fatalf("drop retention policy got num of dbPT %d, exp 1", len(eng.DBPartitions))
	}
	dbPTInfo := eng.DBPartitions["db0"][defaultPtId]
	if len(dbPTInfo.shards) != 0 {
		t.Fatalf("drop retention policy got shard num %d, exp 0", len(dbPTInfo.shards))
	}
	if len(dbPTInfo.indexBuilder) != 0 {
		t.Fatalf("drop retention policy got index num %d, exp 0", len(dbPTInfo.indexBuilder))
	}
	if len(dbPTInfo.newestRpShard) != 0 {
		t.Fatalf("drop retention policy got newestRpShard %d, exp 0", len(dbPTInfo.newestRpShard))
	}

	delete(eng.DBPartitions["db0"], defaultPtId)
	require.Equal(t, true, errno.Equal(eng.DropRetentionPolicy("db0", "rp0", defaultPtId), errno.PtNotFound))
}

func TestEngine_DropRetentionPolicyErrorRP(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	res := make(chan error)
	n := 0
	n++
	go func(db, rp string) {
		res <- eng.DropRetentionPolicy(db, rp, defaultPtId)
	}("db0", "rpx")

	for i := 0; i < n; i++ {
		err = <-res
		if err != nil {
			t.Fatal(err)
		}
	}
	close(res)
}

func TestEngine_DeleteIndex(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	res := make(chan error)
	n := 0
	n++
	dbPTInfo := eng.DBPartitions["db0"][defaultPtId]
	go func(db string, ptId uint32, indexID uint64) {
		res <- eng.DeleteIndex(db, ptId, indexID)
	}("db0", dbPTInfo.id, 1)

	for i := 0; i < n; i++ {
		err = <-res
		if err != nil {
			t.Fatal(err)
		}
	}
	close(res)

	if dbPTInfo.indexBuilder[1] != nil {
		t.Fatalf("partition index not deleted n %d, db0 pt 0 %v ", n, eng.DBPartitions)
	}
}

func TestEngine_Statistics_Engine(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var bufferPool = bufferpool.NewByteBufferPool(0, cpu.GetCpuNum(), bufferpool.MaxLocalCacheLen)
	buf := bufferPool.Get()
	eng.Statistics(buf)
}

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func TestEngine_OpenLimitShardError(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, dPath)
	eng := &Engine{
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath + "/data",
		walPath:      dataPath + "/wal",
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
		loadCtx:      getLoadCtx(),
		engOpt:       DefaultEngineOption,
	}
	eng.log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	eng.engOpt.ShardMutableSizeLimit = 30 * 1024 * 1024
	eng.engOpt.NodeMutableSizeLimit = 1e9
	eng.engOpt.MaxWriteHangTime = time.Second

	getTimeRangeInfoByShard := func(shId uint64) *meta.ShardTimeRangeInfo {
		tr := meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1999-01-01T01:00:00Z"),
			EndTime: mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z")}
		timeRange := &meta.ShardTimeRangeInfo{
			TimeRange: tr,
			OwnerIndex: meta.IndexDescriptor{
				IndexID:      shId,
				IndexGroupID: defaultShGroupId,
				TimeRange:    tr,
			},
			ShardDuration: getShardDurationInfo(shId),
		}
		return timeRange
	}

	// step1: engine create shard, shard will open automatically
	eng.CreateDBPT(defaultDb, defaultPtId, false)
	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
		Schema:     &meta.CleanSchema{},
	}
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 1, getTimeRangeInfoByShard(1), msInfo))
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 2, getTimeRangeInfoByShard(2), msInfo)) // load fail
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 3, getTimeRangeInfoByShard(3), msInfo))

	// step2: write data for three shards
	rows, _, _ := GenDataRecord([]string{"mst"}, 1, 1000, time.Second, time.Now(), true, true, false)
	sh1 := eng.DBPartitions[defaultDb][defaultPtId].Shard(1).(*shard)
	require.NoError(t, writeData(sh1, rows, false))
	sh2 := eng.DBPartitions[defaultDb][defaultPtId].Shard(2).(*shard)
	require.NoError(t, writeData(sh2, rows, false))
	sh3 := eng.DBPartitions[defaultDb][defaultPtId].Shard(3).(*shard)
	require.NoError(t, writeData(sh3, rows, false))

	//sh2IndexLock := filepath.Join(filepath.Dir(sh2.WalPath()), "shard_key_index", "flock.lock")
	sh2WalFile := "1.wal"
	//sh3IndexLock := filepath.Join(filepath.Dir(sh3.WalPath()), "shard_key_index", "flock.lock")

	// step2: engine close
	require.NoError(t, eng.Close())

	// step3: engine reOpen
	shardDurationInfo := map[uint64]*meta.ShardDurationInfo{
		1: getShardDurationInfo(1),
		2: getShardDurationInfo(2),
		3: getShardDurationInfo(3),
	}

	failpoints := []struct {
		failPath string
		inTerms  string
		expect   func(err error) error
	}{
		{
			failPath: "github.com/openGemini/openGemini/engine/mock-replay-wal-error",
			inTerms:  fmt.Sprintf(`return("%s")`, sh2WalFile), // only shard2 fail
			expect: func(err error) error {
				if err != nil && err.Error() == fmt.Sprintf("%s", sh2WalFile) {
					return nil
				}
				return fmt.Errorf("unexpected error:%s", err)
			},
		},
	}
	dbBriefInfos := make(map[string]*meta.DatabaseBriefInfo)
	dbInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	dbBriefInfos[defaultDb] = dbInfo
	for _, fp := range failpoints {
		require.NoError(t, failpoint.Enable(fp.failPath, fp.inTerms))

		err := eng.Open(shardDurationInfo, dbBriefInfos, mockMetaClient())
		if err = fp.expect(err); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, eng.Close())
		require.NoError(t, failpoint.Disable(fp.failPath))
	}
}

func TestEngine_handleTagKeys(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	keysMap := make(map[string]map[string]struct{}, 0)
	keysMap["mst"] = make(map[string]struct{})
	key := []byte("mst,tag1=key1,tag2=key2,tag3=key3")
	eng.handleTagKeys(key, keysMap, "mst")
	for _, subMap := range keysMap {
		for k := range subMap {
			if k == "tag1" {
				continue
			} else if k == "tag2" {
				continue
			} else if k == "tag3" {
				continue
			} else {
				t.Fatal(errors.New("result wrong"))
			}
		}
	}
}

func TestEngine_TagKeys(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	dbInfo := eng.DBPartitions["db0"][0]
	idx := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	idx.DebugFlush()

	keys, err := eng.TagKeys("db0", []uint32{0xff}, [][]byte{[]byte(msNames[0])}, nil, globalTime)
	assert(len(keys) == 0, "Tag keys expect 0")
	require.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	// measurement not exist
	keys, err = eng.TagKeys("db0", []uint32{0}, [][]byte{[]byte("not_exist_measurement")}, nil, globalTime)
	assert(len(keys) == 0, "tag keys expect 0")
	assert(err == nil, "err should be nil")

	// no intersection of time
	keys, err = eng.TagKeys("db0", []uint32{0}, [][]byte{[]byte("cpu")}, nil, influxql.TimeRange{
		Min: time.Now().Add(7 * 24 * time.Hour),
		Max: time.Now().Add(8 * 24 * time.Hour),
	})
	assert(len(keys) == 0, "tag keys expect 0")
	assert(err == nil, "err should be nil")

	// normal
	keys, err = eng.TagKeys("db0", []uint32{0}, [][]byte{[]byte("cpu")}, nil, globalTime)
	assert(len(keys) == 1, "tag keys expect 1 (cpu,tagkey1,tagkey2,tagkey3,tagkey4)")
	assert(err == nil, "err should be nil")
}

func TestEngine_SeriesKeys(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	dbInfo := eng.DBPartitions["db0"][0]
	idx := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	idx.DebugFlush()

	// ignore pt not found
	keys, err := eng.SeriesKeys("db0", []uint32{0xff}, [][]byte{[]byte(msNames[0])}, nil, globalTime)
	assert(len(keys) == 0, "series keys expect 0")
	require.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	// measurement not exist
	keys, err = eng.SeriesKeys("db0", []uint32{0}, [][]byte{[]byte("not_exist_measurement")}, nil, globalTime)
	assert(len(keys) == 0, "series keys expect 0")
	assert(err == nil, "err should be nil")

	// no intersection of time
	keys, err = eng.SeriesKeys("db0", []uint32{0}, [][]byte{[]byte("cpu")}, nil, influxql.TimeRange{
		Min: time.Now().Add(7 * 24 * time.Hour),
		Max: time.Now().Add(8 * 24 * time.Hour),
	})
	assert(len(keys) == 0, "series keys expect 0")
	assert(err == nil, "err should be nil")
}

func TestEngine_SeriesCardinality(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	dbInfo := eng.DBPartitions["db0"][0]
	idx := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	idx.DebugFlush()

	// ignore pt not found
	mcis, err := eng.SeriesCardinality("db0", []uint32{0xff}, [][]byte{[]byte(msNames[0])}, nil, globalTime)
	assert(len(mcis) == 0, "seriesCardinality expect 0")
	require.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	// measurement not exist
	mcis, err = eng.SeriesCardinality("db0", []uint32{0}, [][]byte{[]byte("not_exist_measurement")}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	assert(len(mcis) == 0, "seriesCardinality expect 0")
	assert(err == nil, "err is nil")

	mcis, err = eng.SeriesCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0])}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assert(len(mcis) == 1, "seriesCardinality expect 1")
	assert(len(mcis[0].CardinalityInfos) == 1, "series cardinality res should only be 1")
	assert(mcis[0].CardinalityInfos[0].Cardinality == 10, "series count should be 10")

	condition := influxql.MustParseExpr(`tagkey1=tagvalue1_1`)
	mcis, err = eng.SeriesCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0])}, condition, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	assert(len(mcis) == 1, "seriesCardinality expect 1")
	assert(len(mcis[0].CardinalityInfos) == 1, "series cardinality res should only be 1")
	assert(mcis[0].CardinalityInfos[0].Cardinality == 10, "series count should be 1")

	// no intersection of time & no condition
	mcis, err = eng.SeriesCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0])}, nil, influxql.TimeRange{
		Min: time.Now().Add(7 * 24 * time.Hour),
		Max: time.Now().Add(8 * 24 * time.Hour),
	})
	assert(len(mcis) == 0, "seriesCardinality expect 0")
	assert(err == nil, "no error expected")

	// no intersection of time & has condition
	mcis, err = eng.SeriesCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0])}, condition, influxql.TimeRange{
		Min: time.Now().Add(7 * 24 * time.Hour),
		Max: time.Now().Add(8 * 24 * time.Hour),
	})
	assert(len(mcis) == 0, "seriesCardinality expect 0")
	assert(err == nil, "no error expected")
}

func TestEngine_TagValues(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	dbInfo := eng.DBPartitions["db0"][0]
	idx, ok := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	if !ok {
		t.Fatal()
	}
	idx.DebugFlush()

	// ignore pt not exist
	tagsets, err := eng.TagValues("db0", []uint32{0xff}, map[string][][]byte{
		msNames[0]: {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	require.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	// measurement not found
	tagsets, err = eng.TagValues("db0", []uint32{0}, map[string][][]byte{
		"invalid_measurement": {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	require.Equal(t, err, nil)

	tagsets, err = eng.TagValues("db0", []uint32{0}, map[string][][]byte{
		msNames[0]: {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 1, len(tagsets))
	require.Equal(t, 10, len(tagsets[0].Values))

	// No intersection of time
	tagsets, err = eng.TagValues("db0", []uint32{0}, map[string][][]byte{
		msNames[0]: {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Now().Add(7 * 24 * time.Hour),
		Max: time.Now().Add(8 * 24 * time.Hour),
	})
	require.Equal(t, nil, err)
	require.Equal(t, 0, len(tagsets))
}

func TestEngine_TagValuesDisorder(t *testing.T) {
	dir := t.TempDir()
	fmt.Println(dir)
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	eng.ForceFlush()
	dbInfo := eng.DBPartitions["db0"][0]
	idx := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	idx.DebugFlush()

	// normal
	plan := eng.CreateShowTagValuesPlan("db0", []uint32{0}, &influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime),
		Max: time.Unix(0, influxql.MaxTime),
	})
	tagsets, err := plan.Execute(
		map[string][][]byte{msNames[0]: {[]byte("tagkey1")}},
		nil,
		util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
		0)
	fmt.Printf("%+v", tagsets)
	require.Equal(t, err, nil)
	require.Equal(t, 1, len(tagsets))
	require.Equal(t, 10, len(tagsets[0].Values))

	// no dbpt
	plan = eng.CreateShowTagValuesPlan("db0", []uint32{0xff}, &influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime),
		Max: time.Unix(0, influxql.MaxTime),
	})
	tagsets, err = plan.Execute(
		map[string][][]byte{msNames[0]: {[]byte("tagkey1")}},
		nil,
		util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
		0)
	fmt.Printf("%+v", tagsets)
	require.Equal(t, err, nil)

	// reach limit
	plan = eng.CreateShowTagValuesPlan("db0", []uint32{0}, &influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime),
		Max: time.Unix(0, influxql.MaxTime),
	})
	tagsets, err = plan.Execute(
		map[string][][]byte{msNames[0]: {[]byte("tagkey1")}},
		nil,
		util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
		3)
	fmt.Printf("%+v", tagsets)
	require.Equal(t, err, nil)
	require.Equal(t, 1, len(tagsets))
	require.Equal(t, 3, len(tagsets[0].Values))
}

func TestEngine_TagValuesCardinality(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	dbInfo := eng.DBPartitions["db0"][0]
	idx, ok := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	if !ok {
		t.Fatal()
	}
	idx.DebugFlush()

	// ignore pt not exist
	tagsets, err := eng.TagValuesCardinality("db0", []uint32{0xff}, map[string][][]byte{
		msNames[0]: {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	require.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	// measurement not found
	tagsets, err = eng.TagValuesCardinality("db0", []uint32{0}, map[string][][]byte{
		"invalid_measurement": {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	require.Equal(t, err, nil)

	tagsets, err = eng.TagValuesCardinality("db0", []uint32{0}, map[string][][]byte{
		msNames[0]: {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Unix(0, influxql.MinTime).UTC(),
		Max: time.Unix(0, influxql.MaxTime).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, len(msNames), len(tagsets))
	require.Equal(t, uint64(10), tagsets[msNames[0]])

	// No intersection of time
	tagsets, err = eng.TagValuesCardinality("db0", []uint32{0}, map[string][][]byte{
		msNames[0]: {[]byte("tagkey1")},
	}, nil, influxql.TimeRange{
		Min: time.Now().Add(7 * 24 * time.Hour),
		Max: time.Now().Add(8 * 24 * time.Hour),
	})
	require.Equal(t, nil, err)
	require.Equal(t, uint64(0), tagsets["cpu"])
}

func Test_Engine_DropMeasurement(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	log = eng.log
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	err = eng.DropMeasurement("db1", "rp1", "mst1", []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEngine_UpdateShardDurationInfo(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	info := &meta.ShardDurationInfo{Ident: meta.ShardIdentifier{ShardID: 0, OwnerDb: defaultDb, OwnerPt: defaultPtId}}
	assert2.Equal(t, nil, eng.UpdateShardDurationInfo(info))
}

func TestGetShard(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var db, rp = "db0", "auto"
	var ptID uint32 = 100
	var shardID = defaultShardId
	var tr = getTimeRangeInfo()

	eng.CreateDBPT(db, ptID, false)
	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
	}
	require.NoError(t, eng.CreateShard(db, rp, ptID, shardID, tr, msInfo))
	sh1, err := eng.GetShard(db, ptID, shardID)
	require.NoError(t, err)
	require.NotEmpty(t, sh1)

	sh2, err := eng.GetShard(db, ptID+1, shardID)
	require.NoError(t, err)
	require.Empty(t, sh2)

	level := eng.GetShardDownSampleLevel(db, ptID, shardID+1)
	require.Equal(t, 0, level)
}

func TestEngine_OpenShardGetDBBriefInfoError(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, dPath)
	eng := &Engine{
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath + "/data",
		walPath:      dataPath + "/wal",
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
		loadCtx:      getLoadCtx(),
		engOpt:       DefaultEngineOption,
	}
	eng.log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	eng.engOpt.ShardMutableSizeLimit = 30 * 1024 * 1024
	eng.engOpt.NodeMutableSizeLimit = 1e9
	eng.engOpt.MaxWriteHangTime = time.Second

	getTimeRangeInfoByShard := func(shId uint64) *meta.ShardTimeRangeInfo {
		tr := meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1999-01-01T01:00:00Z"),
			EndTime: mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z")}
		timeRange := &meta.ShardTimeRangeInfo{
			TimeRange: tr,
			OwnerIndex: meta.IndexDescriptor{
				IndexID:      shId,
				IndexGroupID: defaultShGroupId,
				TimeRange:    tr,
			},
			ShardDuration: getShardDurationInfo(shId),
		}
		return timeRange
	}

	// step1: engine create shard, shard will open automatically
	eng.CreateDBPT(defaultDb, defaultPtId, false)
	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
	}
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 1, getTimeRangeInfoByShard(1), msInfo))
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 2, getTimeRangeInfoByShard(2), msInfo)) // load fail
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 3, getTimeRangeInfoByShard(3), msInfo))

	// step2: write data for three shards
	rows, _, _ := GenDataRecord([]string{"mst"}, 1, 1000, time.Second, time.Now(), true, true, false)
	sh1 := eng.DBPartitions[defaultDb][defaultPtId].Shard(1).(*shard)
	require.NoError(t, writeData(sh1, rows, false))
	sh2 := eng.DBPartitions[defaultDb][defaultPtId].Shard(2).(*shard)
	require.NoError(t, writeData(sh2, rows, false))
	sh3 := eng.DBPartitions[defaultDb][defaultPtId].Shard(3).(*shard)
	require.NoError(t, writeData(sh3, rows, false))

	//sh2IndexLock := filepath.Join(filepath.Dir(sh2.WalPath()), "shard_key_index", "flock.lock")
	sh2WalFile := "1.wal"
	//sh3IndexLock := filepath.Join(filepath.Dir(sh3.WalPath()), "shard_key_index", "flock.lock")

	// step2: engine close
	require.NoError(t, eng.Close())

	// step3: engine reOpen
	shardDurationInfo := map[uint64]*meta.ShardDurationInfo{
		1: getShardDurationInfo(1),
		2: getShardDurationInfo(2),
		3: getShardDurationInfo(3),
	}

	failpoints := []struct {
		failPath string
		inTerms  string
		expect   func(err error) error
	}{
		{
			failPath: "github.com/openGemini/openGemini/engine/mock-replay-wal-error",
			inTerms:  fmt.Sprintf(`return("%s")`, sh2WalFile), // only shard2 fail
			expect: func(err error) error {
				if err != nil && err.Error() == sh2WalFile {
					return nil
				}
				return fmt.Errorf("unexpected error:%s", err)
			},
		},
	}
	dbBriefInfos := make(map[string]*meta.DatabaseBriefInfo)
	dbInfo := &meta.DatabaseBriefInfo{
		Name:           "db1",
		EnableTagArray: false,
	}
	dbBriefInfos["db1"] = dbInfo
	for _, fp := range failpoints {
		require.NoError(t, failpoint.Enable(fp.failPath, fp.inTerms))

		err := eng.Open(shardDurationInfo, dbBriefInfos, mockMetaClient())
		if err = fp.expect(err); err != nil {
			if !strings.Contains(err.Error(), "database not found") {
				t.Fatal(err)
			}
		}
		require.NoError(t, eng.Close())
		require.NoError(t, failpoint.Disable(fp.failPath))
	}
}

func TestEngine_StatisticsOps(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames1 := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames1, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}

	msNames2 := []string{"mem"}
	rows, _, _ = GenDataRecord(msNames2, 20, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	dbInfo := eng.DBPartitions["db0"][0]
	idx := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	idx.DebugFlush()

	m := mockMetaClient()
	eng.metaClient = m
	stats := eng.StatisticsOps()
	expectStats := 2
	require.Equal(t, expectStats, len(stats))

	var expectSeriesNum int64
	stats = eng.StatisticsOps()
	expectSeriesNum = 30
	require.Equal(t, expectSeriesNum, stats[0].Values["numSeries"])

}

func TestUpdateShardDurationInfo(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	shardDuration := getShardDurationInfo(1)
	shardDuration.DurationInfo.Tier = util.Warm
	shardDuration.Ident.OwnerPt = 0
	err = eng.UpdateShardDurationInfo(shardDuration)
	require.NoError(t, err)
	sh, err := eng.GetShard(defaultDb, 0, 1)
	assert2.NoError(t, err)
	require.Equal(t, sh.GetDuration().Tier, uint64(util.Warm))
}

func TestSkipIndex(t *testing.T) {
	dir := t.TempDir()
	config.SetProductType(config.LogKeeperService)
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	shardDuration := getShardDurationInfo(1)
	shardDuration.DurationInfo.Tier = util.Warm
	shardDuration.Ident.OwnerPt = 0
	err = eng.UpdateShardDurationInfo(shardDuration)
	require.NoError(t, err)
	sh, err := eng.GetShard(defaultDb, 0, 1)
	assert2.NoError(t, err)
	require.Equal(t, sh.GetDuration().Tier, uint64(util.Hot))
	config.SetProductType("")
}

func TestEngine_SeriesLimited(t *testing.T) {
	testDir := t.TempDir()

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	require.NoError(t, err)

	defer sh.Close()
	defer sh.indexBuilder.Close()

	sh.initSeriesLimiter(10)
	rows, _, _ := GenDataRecord([]string{"mst"}, 20, 1, 1, time.Now(), false, true, false)
	err = writeData(sh, rows, true)
	require.NoError(t, err)

	rows, _, _ = GenDataRecord([]string{"mst"}, 30, 1, 1, time.Now(), false, true, false)
	err = writeData(sh, rows, true)
	require.EqualError(t, err, errno.NewError(errno.SeriesLimited, defaultDb, 10, 20).Error())

	sh.indexBuilder.EnableTagArray = true
	rows, _, _ = GenDataRecord([]string{"mst"}, 40, 1, 1, time.Now(), false, true, false)
	rows[0].Tags[0].Value = "[a,b,c]"
	rows[0].UnmarshalIndexKeys(nil)
	err = writeData(sh, rows, true)
	require.EqualError(t, err, errno.NewError(errno.SeriesLimited, defaultDb, 10, 20).Error())
}

func TestEngine_RowCount(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	var fields influxql.Fields
	var names []string
	opt := query.ProcessorOptions{}
	fields = append(fields, &influxql.Field{Expr: &influxql.VarRef{Val: "f1", Type: influxql.Integer}})
	names = append(names, "f1")
	schema := executor.NewQuerySchema(fields, names, &opt, nil)
	m := &influxql.Measurement{Name: "students"}
	schema.AddTable(m, schema.MakeRefs())
	count, err := eng.RowCount("db0", 0, []uint64{1}, schema)
	assert2.Equal(t, count, int64(0))
}

func TestEngine_RowCount_ShardLockPath(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngineWithNoLockPath(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
}

type mockShard struct {
	shard
}

func (ms *mockShard) IsOpened() bool {
	return ms.opened
}

func (ms *mockShard) OpenAndEnable(client metaclient.MetaClient) error {
	return nil
}

func (ms *mockShard) CreateShowTagValuesPlan(client metaclient.MetaClient) immutable.ShowTagValuesPlan {
	return &MockImmutableShowTagValuesPlan{}
}

func (ms *mockShard) Intersect(tr *influxql.TimeRange) bool {
	return true
}

func Test_openShardLazy(t *testing.T) {
	eng := &Engine{
		log: logger.NewLogger(errno.ModuleUnknown),
	}
	sh1 := &mockShard{
		shard: shard{
			opened: true,
		},
	}
	// case1: opened
	err := eng.openShardLazy(sh1)
	require.NoError(t, err)

	// case2: opened
	sh2 := &mockShard{
		shard: shard{
			opened:   false,
			dataPath: "test_path",
			ident:    &meta.ShardIdentifier{OwnerPt: 1},
		},
	}
	err = eng.openShardLazy(sh2)
	require.NoError(t, err)
}

func TestEngine_ExpiredCacheIndexes(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExpiredCacheIndexes()
}

func TestEngine_ClearIndexCache(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	dbPTInfo := eng.DBPartitions["db0"][defaultPtId]
	var okIndexID uint64 = 1
	err = eng.ClearIndexCache("db0", dbPTInfo.id, okIndexID)
	require.NoError(t, err)

	err = eng.ClearIndexCache("db0", 0, 1)
	require.Error(t, err)

	var errorIndexID uint64 = 0
	err = eng.ClearIndexCache("db0", dbPTInfo.id, errorIndexID)
	require.Error(t, err)
}

func TestStoreHierarchicalStorage(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions["db0"][0].shards[1]
	sh.GetDuration().Tier = util.Warm
	syscontrol.SetWriteColdShardEnabled(true)
	ok := eng.HierarchicalStorage("db0", 0, 1)
	require.Equal(t, ok, true)

	shard, _ := eng.getShard("db0", 0, 1)
	shard.DisableHierarchicalStorage()
	syscontrol.SetWriteColdShardEnabled(false)
	defer shard.SetEnableHierarchicalStorage()
}

func TestStoreHierarchicalStorage_CanNotDoShardMove(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions["db0"][0].shards[1]
	sh.GetDuration().Tier = util.Warm

	msNames := []string{"cpu"}
	tm := time.Now()
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil, nil); err != nil {
		t.Fatal(err)
	}

	shard, _ := eng.getShard("db0", 0, 1)
	shard.ForceFlush()

	tm = time.Now()
	rows2, _, _ := GenDataRecord(msNames, 10, 200, time.Second*10, tm, false, true, false)
	if err := eng.WriteRows("db0", "rp0", 0, 1, rows2, nil, nil); err != nil {
		t.Fatal(err)
	}
	shard.ForceFlush()
	time.Sleep(1 * time.Second)
	syscontrol.SetWriteColdShardEnabled(true)
	ok := eng.HierarchicalStorage("db0", 0, 1)
	syscontrol.SetWriteColdShardEnabled(false)
	require.Equal(t, ok, false)
}

func TestStoreHierarchicalStorage_DBPTNotFound(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions["db0"][0].shards[1]
	sh.GetDuration().Tier = util.Warm

	var errPtId uint32
	errPtId = 1
	syscontrol.SetWriteColdShardEnabled(true)
	ok := eng.HierarchicalStorage("db0", errPtId, 1)
	syscontrol.SetWriteColdShardEnabled(false)
	require.Equal(t, ok, false)
}

func TestStoreHierarchicalStorage_ShardNotFound(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions["db0"][0].shards[1]
	sh.GetDuration().Tier = util.Warm

	var errShardId uint64
	errShardId = 0
	syscontrol.SetWriteColdShardEnabled(true)
	ok := eng.HierarchicalStorage("db0", 0, errShardId)
	syscontrol.SetWriteColdShardEnabled(false)
	require.Equal(t, ok, false)
}

func TestStoreHierarchicalStorage_ShardIsCold(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngineWithColdShard(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	syscontrol.SetWriteColdShardEnabled(true)
	ok := eng.HierarchicalStorage("db0", 0, 1)
	syscontrol.SetWriteColdShardEnabled(false)
	require.Equal(t, ok, true)
}

func TestStoreHierarchicalStorage_MoveStop(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions["db0"][0].shards[1]
	sh.GetDuration().Tier = util.Warm

	sh.DisableHierarchicalStorage()
	defer sh.SetEnableHierarchicalStorage()
	syscontrol.SetWriteColdShardEnabled(true)
	ok := eng.HierarchicalStorage("db0", 0, 1)
	syscontrol.SetWriteColdShardEnabled(false)
	require.Equal(t, ok, false)
}

func TestStoreHierarchicalStorage_DoingOff(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.DBPartitions["db0"][0].doingOff = true
	syscontrol.SetWriteColdShardEnabled(true)
	ok := eng.HierarchicalStorage("db0", 0, 1)
	syscontrol.SetWriteColdShardEnabled(false)
	require.Equal(t, ok, false)
}

func TestStoreHierarchicalStorage_FetchShardsNeedChangeStore(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions["db0"][0].shards[1]
	sh.GetDuration().Tier = util.Cold

	eng.FetchShardsNeedChangeStore()
}

func TestInstantVectorCursorFunc(t *testing.T) {
	c := &InstantVectorCursor{}
	assert2.Equal(t, c.Name(), "instant_vector_cursor")
	inSchema, outSchema := []record.Field{{Name: "f1", Type: influx.Field_Type_Int}}, []record.Field{{Name: "f2", Type: influx.Field_Type_Int}}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "f1", Type: influx.Field_Type_Int},
			Ref:  influxql.VarRef{Val: "f21", Type: influx.Field_Type_Int},
		},
	}
	_, err := newPromSampleProcessor(inSchema, outSchema, exprOpt)
	assert2.True(t, strings.Contains(err.Error(), "input and output schemas are not aligned for prom sample cursor"))
	exprOpt = []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "f1", Type: influx.Field_Type_Int},
			Ref:  influxql.VarRef{Val: "f2", Type: influx.Field_Type_Int},
		},
	}
	_, err = newPromSampleProcessor(inSchema, outSchema, exprOpt)
	assert2.True(t, strings.Contains(err.Error(), "unsupported data type for prom sample cursor"))

	f := func(rec *record.Record, startSample, endSample, step, lookUpDelta int64, expected []uint16, opt *query.ProcessorOptions) {
		promCursor := &InstantVectorCursor{startSample: startSample, endSample: endSample, step: step, lookUpDelta: lookUpDelta}
		querySchema := executor.NewQuerySchema(nil, nil, opt, nil)
		promCursor.aggregateCursor = aggregateCursor{schema: querySchema}
		promCursor.computeIntervalIndex(rec)
		assert2.Equal(t, expected, promCursor.intervalIndex)
	}
	rec1 := record.NewRecord([]record.Field{{Name: record.TimeField, Type: influx.Field_Type_Int}}, false)
	rec1.ColVals[0].AppendIntegers(1, 2, 3, 4, 5, 6)

	rec2 := record.NewRecord([]record.Field{{Name: record.TimeField, Type: influx.Field_Type_Int}}, false)
	rec2.ColVals[0].AppendIntegers(1, 6)

	// sample time: {6}
	t.Run("1", func(t *testing.T) {
		expected := []uint16{0, 6}
		opt := &query.ProcessorOptions{}
		f(rec1, int64(1), int64(6), int64(0), int64(5), expected, opt)
	})

	// sample time: {1, 3, 5}
	t.Run("2", func(t *testing.T) {
		expected := []uint16{0, 1, 0, 3, 1, 5}
		opt := &query.ProcessorOptions{Interval: hybridqp.Interval{Duration: 2 * time.Nanosecond}}
		f(rec1, int64(1), int64(5), int64(2), int64(3), expected, opt)
	})

	// sample time: {1, 3, 5}
	t.Run("3", func(t *testing.T) {
		expected := []uint16{0, 1, 0, 1, 1, 1}
		opt := &query.ProcessorOptions{Interval: hybridqp.Interval{Duration: 2 * time.Nanosecond}}
		f(rec2, int64(1), int64(5), int64(2), int64(3), expected, opt)
	})

	// sample time: {1, 4}
	t.Run("4", func(t *testing.T) {
		expected := []uint16{0, 1, 1, 4}
		opt := &query.ProcessorOptions{Interval: hybridqp.Interval{Duration: 3 * time.Nanosecond}}
		f(rec1, int64(1), int64(5), int64(3), int64(2), expected, opt)
	})
}

func TestRangeVectorCursorFunc(t *testing.T) {
	ic := &seriesCursor{}
	opt := &query.ProcessorOptions{Step: 0}
	schema := executor.NewQuerySchema(nil, nil, opt, nil)
	pool := record.NewRecordPool(record.AggPool)
	c := NewRangeVectorCursor(ic, schema, pool, util.TimeRange{})
	rec := record.NewRecord([]record.Field{{Name: "time", Type: influx.Field_Type_Int}}, false)
	c.getIntervalIndex(rec)
	assert2.Equal(t, c.Name(), "range_vector_cursor")
	assert2.Equal(t, c.intervalIndex, []uint16{0, 0})
}

func TestIteratorInit(t *testing.T) {
	s := &shard{}
	opt := &query.ProcessorOptions{}
	opt.SetPromQuery(true)
	opt.Step = time.Second
	schema := executor.NewQuerySchema(nil, nil, opt, nil)
	schema.Visit(&influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "cpu", Type: influxql.Integer}}})
	ctx := &idKeyCursorContext{memTables: &mutable.MemTables{}, readers: &immutable.MmsReaders{}, querySchema: schema}
	tagSet := &tsi.TagSetInfo{
		IDs:        []uint64{1, 2},
		Filters:    []influxql.Expr{nil, nil},
		TagsVec:    []influx.PointTags{nil, nil},
		SeriesKeys: [][]byte{{'a'}, {'b'}},
	}
	start, step := 1, 1
	notAggOnSeriesFunc := func(m map[string]*influxql.Call) bool {
		for _, call := range m {
			if executor.NotAggOnSeries[call.Name] {
				return true
			}
		}
		return false
	}
	// instant vector selector
	itr, _ := s.iteratorInit(ctx, nil, schema, tagSet, start, step, false, notAggOnSeriesFunc)
	_, ok := itr.(*InstantVectorCursor)
	if !ok {
		t.Fatal("invalid the KeyCursor")
	}

	// range vector selector
	schema.Options().(*query.ProcessorOptions).Range = time.Minute
	itr, _ = s.iteratorInit(ctx, nil, schema, tagSet, start, step, false, notAggOnSeriesFunc)
	_, ok = itr.(*RangeVectorCursor)
	if !ok {
		t.Fatal("invalid the KeyCursor")
	}

	// query with agg and limit
	schema.Options().(*query.ProcessorOptions).Range = 0
	opt.SetPromQuery(false)
	_, err := itrsInitWithLimit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// range_query with agg and limit
	opt.SetPromQuery(true)
	_, err = itrsInitWithLimit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// query with agg
	opt.SetPromQuery(false)
	_, err = itrsInit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// range_query with agg
	opt.SetPromQuery(true)
	_, err = itrsInit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// range vector selector with agg and limit
	schema.Options().(*query.ProcessorOptions).Range = time.Minute
	_, err = itrsInitWithLimit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// range vector selector with agg
	_, err = itrsInit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// range_query with non agg
	schema = executor.NewQuerySchema(nil, nil, opt, nil)
	_, err = itrsInit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// range vector selector with non agg
	schema = executor.NewQuerySchema(nil, nil, opt, nil)
	_, err = itrsInit(ctx, nil, schema, tagSet, start, step, false, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegisterColdShard(t *testing.T) {
	dir := t.TempDir()
	config.GetStoreConfig().SkipRegisterColdShard = true
	eng, err := initEngineWithColdShard(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions["db0"][0].shards[1]
	sh.GetDuration().Tier = util.Cold
	compWorker.RegisterShard(sh.(*shard))
	_, ok := compWorker.sources[sh.GetID()]
	require.Equal(t, ok, false)
}

func TestEngineDeleteShard(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sh := eng.DBPartitions[defaultDb][defaultPtId].shards[defaultShardId]
	sh.SetObsOption(&obs.ObsOptions{})
	err = eng.DeleteShard(defaultDb, defaultPtId, defaultShardId)
	// ak、sk、endPoint、bucket is empty, deletedShard should return err
	assert2.NotNil(t, err)
}

func TestEngine_uploadFileInfos(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := mockMetaClient()
	eng.metaClient = m
	eng.fileInfos = make(chan []immutable.FileInfoExtend, 1)
	defer eng.Close()
	type args struct {
		fileInfos []immutable.FileInfoExtend
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test1",
			args: args{
				fileInfos: []immutable.FileInfoExtend{
					{
						FileInfo: meta.FileInfo{
							MstID:         1,
							ShardID:       1,
							Sequence:      1,
							Level:         0,
							Merge:         0,
							Extent:        0,
							CreatedAt:     1,
							DeletedAt:     0,
							MinTime:       1,
							MaxTime:       100,
							RowCount:      11,
							FileSizeBytes: 1011,
						},
						Name: "mst1_0000",
					},
				},
			},
		},
		{
			name: "test2",
			args: args{
				fileInfos: make([]immutable.FileInfoExtend, MaxFileInfoSize+1),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := eng
			e.fileInfos <- tt.args.fileInfos
			go func() {
				e.uploadFileInfos()
			}()
			time.Sleep(DefaultUploadFrequence)
			e.closed.Close()
		})
	}
}

type MockImmutableShowTagValuesPlan struct {
	immutable.ShowTagValuesPlan
}

func (p *MockImmutableShowTagValuesPlan) Execute(dst map[string]*immutable.TagSets, tagKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) error {
	tagSets := immutable.NewTagSets()
	tagSets.Add("k1", "v1")
	tagSets.Add("k2", "v2")
	tagSets.Add("k3", "v3")
	dst["mst"] = tagSets
	return nil
}

func TestShowTagValuesPlan_AddPlan(t *testing.T) {
	plan := &ShowTagValuesPlan{}
	plan.AddPlan(&MockImmutableShowTagValuesPlan{})
	plan.AddPlan(&MockImmutableShowTagValuesPlan{})
	if len(plan.plans) != 2 {
		t.Fatalf("ShowTagValuesPlan AddPlan failed, expected: %d, actual: %d", 2, len(plan.plans))
	}
}

func TestShowTagValuesPlan_Execute(t *testing.T) {
	plan := &ShowTagValuesPlan{
		plans: []immutable.ShowTagValuesPlan{
			&MockImmutableShowTagValuesPlan{},
		},
	}
	tagSets, err := plan.Execute(make(map[string][][]byte), &influxql.VarRef{}, util.TimeRange{}, 10)
	if err != nil {
		return
	}

	want := []netstorage.TableTagSets{
		{
			Name: "mst",
			Values: netstorage.TagSets{
				{Key: "k1", Value: "v1"},
				{Key: "k2", Value: "v2"},
				{Key: "k3", Value: "v3"},
			},
		},
	}
	if len(want) != len(tagSets) {
		t.Fatalf("ShowTagValuesPlan Execute failed, expected %+v, actual: %+v", want, tagSets)
	}
	if want[0].Name != tagSets[0].Name {
		t.Fatalf("ShowTagValuesPlan Execute failed, expected %+v, actual: %+v", want, tagSets)
	}
	sort.Slice(tagSets[0].Values, func(i, j int) bool {
		return tagSets[0].Values[i].Key < tagSets[0].Values[j].Key
	})
	for i, value := range want[0].Values {
		got := tagSets[0].Values[i]
		if value.Key != got.Key || value.Value != got.Value {
			t.Fatalf("ShowTagValuesPlan Execute failed, expected %+v, actual: %+v", want, tagSets)
		}
	}
}

func TestEngine_CreateShowTagValuesPlan(t *testing.T) {
	mockEngine := &Engine{
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"testdb": {
				1: &DBPTInfo{
					shards: map[uint64]Shard{
						11: &mockShard{},
						12: &mockShard{},
						13: &mockShard{},
						14: &mockShard{},
					},
				},
			},
		},
	}
	got := mockEngine.CreateShowTagValuesPlan("testdb", []uint32{1}, &influxql.TimeRange{})
	plan, ok := got.(*ShowTagValuesPlan)
	if !ok {
		t.Fatalf("CreateShowTagValuesPlan failed: %v", got)
	}
	want := 4
	if len(plan.plans) != want {
		t.Fatalf("CreateShowTagValuesPlan failed, actual: %v, expected: %v", len(plan.plans), want)
	}
}

func TestWriteToRaft(t *testing.T) {
	mockProposeC := make(chan []byte, 1)
	defer close(mockProposeC)
	mockEngine := &Engine{
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"db": {
				1: &DBPTInfo{
					shards: map[uint64]Shard{
						11: &mockShard{},
						12: &mockShard{},
						13: &mockShard{},
						14: &mockShard{},
					},
					proposeC: mockProposeC,
					node:     &raftconn.RaftNode{DataCommittedC: make(map[uint64]chan error), Identity: "db_1"},
				},
			},
		},
	}
	exp := "\x00\x00\x00\x00\x04db_1\x00\x00\x00\x00\x00\x00\x00\x01test"
	same := false
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dw := <-mockProposeC
		mockEngine.DBPartitions["db"][1].node.RetCommittedDataC(&raftlog.DataWrapper{ProposeId: 1, Identity: "db_1"}, nil)
		if strings.Compare(exp, string(dw)) == 0 {
			same = true
		}
	}()
	mockEngine.WriteToRaft("db", "autogen", 1, []byte("test"))
	wg.Wait()
	assert2.Equal(t, same, true)
}

func TestEngine_WriteToRaft(t *testing.T) {
	proposeC := make(chan []byte)
	mockEngine := &Engine{
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"testdb": {
				1: &DBPTInfo{
					proposeC: proposeC,
					node:     &raftconn.RaftNode{DataCommittedC: make(map[uint64]chan error), Identity: "testdb_1"},
				},
			},
		},
	}
	tail := []byte{'a', 'b', 'c'}
	exp := "\x00\x00\x00\x00\btestdb_1\x00\x00\x00\x00\x00\x00\x00\x01abc"
	same := false
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reTail := <-proposeC
		mockEngine.DBPartitions["testdb"][1].node.RetCommittedDataC(&raftlog.DataWrapper{ProposeId: 1, Identity: "testdb_1"}, nil)
		if strings.Compare(exp, string(reTail)) == 0 {
			same = true
		}
	}()
	mockEngine.WriteToRaft("testdb", "", 1, tail)
	wg.Wait()
	assert2.Equal(t, same, true)
}

func TestEngine_WriteToRaft_UsedProposeId_Err(t *testing.T) {
	proposeC := make(chan []byte)
	mockEngine := &Engine{
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"testdb": {
				1: &DBPTInfo{
					proposeC: proposeC,
					node:     &raftconn.RaftNode{DataCommittedC: map[uint64]chan error{1: make(chan error)}, Identity: "testdb_1"},
				},
			},
		},
	}

	tail := []byte{'a', 'b', 'c'}
	err := mockEngine.WriteToRaft("testdb", "", 1, tail)
	assert2.Equal(t, true, errno.Equal(err, errno.UsedProposeId))
}

func TestEngine_WriteToRaft_Committed_Err(t *testing.T) {
	proposeC := make(chan []byte)
	mockEngine := &Engine{
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"testdb": {
				1: &DBPTInfo{
					proposeC: proposeC,
					node:     &raftconn.RaftNode{DataCommittedC: make(map[uint64]chan error), Identity: "testdb_1"},
				},
			},
		},
	}
	tail := []byte{'a', 'b', 'c'}
	exp := "\x00\x00\x00\x00\btestdb_1\x00\x00\x00\x00\x00\x00\x00\x01abc"
	same := false
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reTail := <-proposeC
		mockEngine.DBPartitions["testdb"][1].node.RetCommittedDataC(&raftlog.DataWrapper{ProposeId: 1, Identity: "testdb_1"}, fmt.Errorf("mem write err"))
		if strings.Compare(exp, string(reTail)) == 0 {
			same = true
		}
	}()
	err := mockEngine.WriteToRaft("testdb", "", 1, tail)
	wg.Wait()
	assert2.Equal(t, same, true)
	assert2.Equal(t, err.Error(), "mem write err")
}
