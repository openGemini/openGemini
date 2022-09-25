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

package engine

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/pingcap/failpoint"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type shardMock struct {
	rp      string
	id      uint64
	expired bool
}

var defaultEngineOption netstorage.EngineOptions

func init() {
	log = logger.New(os.Stderr)
	defaultEngineOption = netstorage.NewEngineOptions()
	defaultEngineOption.WriteColdDuration = time.Second * 5000
	defaultEngineOption.ShardMutableSizeLimit = 30 * 1024 * 1024
	defaultEngineOption.NodeMutableSizeLimit = 1e9
	defaultEngineOption.MaxWriteHangTime = time.Second
	defaultEngineOption.WalSyncInterval = 100 * time.Millisecond
	defaultEngineOption.WalEnabled = true
	defaultEngineOption.WalReplayParallel = false
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
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath + "/data",
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
	}
	eng.log = logger.New(os.Stderr)
	eng.engOpt.ShardMutableSizeLimit = 30 * 1024 * 1024
	eng.engOpt.NodeMutableSizeLimit = 1e9
	eng.engOpt.MaxWriteHangTime = time.Second

	loadCtx := getLoadCtx()
	eng.loadCtx = loadCtx
	eng.CreateDBPT(defaultDb, defaultPtId)
	eng.DBPartitions[defaultDb][defaultPtId].logger = logger.New(os.Stderr)
	shardTimeRange := getTimeRangeInfo()
	err := eng.CreateShard(defaultDb, defaultRp, defaultPtId, defaultShardId, shardTimeRange)
	if err != nil {
		panic(err)
	}

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
			Tier: meta.Hot, TierDuration: time.Hour, Duration: time.Hour,
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

func initEngine1(dir string) (*Engine, error) {
	dataPath := filepath.Join(dir, dPath)
	eng := &Engine{
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath + "/data",
		walPath:      dataPath + "/wal",
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
	}
	eng.log = logger.New(os.Stderr)
	log = logger.New(os.Stderr)

	loadCtx := getLoadCtx()
	dbPTInfo := NewDBPTInfo("db0", 0, eng.dataPath, eng.walPath, loadCtx)
	dbPTInfo.logger = eng.log
	eng.addDBPTInfo(dbPTInfo)
	reportLoadFrequency = time.Millisecond // 1ms
	dbPTInfo.enableReportShardLoad()

	indexPath := path.Join(dbPTInfo.path, "rp0", IndexFileDirectory,
		"659_946252800000000000_946857600000000000", "mergeset")
	_ = fileops.MkdirAll(indexPath, 0755)

	//indexPath := path.Join(dbPTInfo.path, "rp0", IndexFileDirectory, "1_1648544460000000000_1648548120000000000")

	dbPTInfo.OpenIndexes("rp0")

	indexBuilder := dbPTInfo.indexBuilder[659]
	shardIdent := &meta.ShardIdentifier{ShardID: 1, ShardGroupID: 1, Policy: "rp0", OwnerDb: "db0", OwnerPt: 0}
	shardDuration := &meta.DurationDescriptor{Tier: meta.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1999-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z")}
	shard := NewShard(eng.dataPath, eng.walPath, shardIdent, indexBuilder, shardDuration, tr, defaultEngineOption)
	//shard.wal.logger = eng.log
	//shard.wal.traceLogger = eng.log
	err := shard.Open()
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
	log = eng.log
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

	var bufferPool = bufferpool.NewByteBufferPool(0)
	buf := bufferPool.Get()
	eng.Statistics(buf)
}

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func TestEngine_OpenLimitShardError(t *testing.T) {
	t.Skip()
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
		engOpt:       defaultEngineOption,
	}
	eng.log = logger.New(os.Stderr)
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
	eng.CreateDBPT(defaultDb, defaultPtId)
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 1, getTimeRangeInfoByShard(1)))
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 2, getTimeRangeInfoByShard(2))) // load fail
	require.NoError(t, eng.CreateShard(defaultDb, defaultRp, defaultPtId, 3, getTimeRangeInfoByShard(3)))

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
	for _, fp := range failpoints {
		require.NoError(t, failpoint.Enable(fp.failPath, fp.inTerms))

		err := eng.Open([]uint32{defaultPtId}, shardDurationInfo)
		if err = fp.expect(err); err != nil {
			t.Fatal(err)
		}
		require.NoError(t, eng.Close())
		require.NoError(t, failpoint.Disable(fp.failPath))
	}
}

func TestEngine_SeriesCardinality(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 200, time.Second, tm, false, true, false)

	if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil); err != nil {
		t.Fatal(err)
	}
	dbInfo := eng.DBPartitions["db0"][0]
	idx := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	idx.DebugFlush()

	mcis, err := eng.SeriesCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0])}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assert(len(mcis) == 1, "seriesCardinality expect 1")
	assert(len(mcis[0].CardinalityInfos) == 1, "series cardinality res should only be 1")
	assert(mcis[0].CardinalityInfos[0].Cardinality == 10, "series count should be 10")

	condition := influxql.MustParseExpr(`tagkey1=tagvalue1_1`)
	mcis, err = eng.SeriesCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0])}, condition)
	assert(len(mcis) == 1, "seriesCardinality expect 1")
	assert(len(mcis[0].CardinalityInfos) == 1, "series cardinality res should only be 1")
	assert(mcis[0].CardinalityInfos[0].Cardinality == 10, "series count should be 1")
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
