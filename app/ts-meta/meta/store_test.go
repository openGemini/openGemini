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

package meta_test

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/app/ts-meta/meta"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	logger2 "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var dbPTMu = sync.RWMutex{}
var dbPTStat = make(map[string]map[uint32]*MockDBPTStatistics)
var reportInterval = time.Second

// fixme: Occasional failure
func _TestUpdateLoad(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.InitStore(dir, "127.0.0.1")
	defer func() {
		ms.Close()
	}()
	if err != nil {
		t.Fatal(err)
	}
	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err := ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	db := "benchmark"
	rp := "autogen"
	mst := "cpu"
	shardKey := "hostname"
	shardType := "range"
	cmd = meta.GenerateCreateDatabaseCmd(db)
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateCreateMeasurementCmd(db, rp, mst, []string{shardKey}, shardType)
	if err = ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	ms.GetStore().NetStore = NewMockNetStorage()

	for i := 0; i < len(ms.GetStore().GetData().PtView[db]); i++ {
		go MockReportLoad(ms, db, uint32(i))
	}

	if err = generateData(ms, db, rp, mst, 100, 10, 10, ""); err != nil {
		t.Fatal(err)
	}

	// check if split happened
	data := ms.GetStore().GetData()
	if got, exp := len(data.Database(db).RetentionPolicy(rp).ShardGroups), 2; got != exp {
		t.Fatal(fmt.Errorf("shard should split, got num shardgroup %d, exp %d", got, exp))
	}

	// check max shard num and split point
	shards := data.Database(db).RetentionPolicy(rp).ShardGroups[1].Shards
	if got, exp := len(shards), len(data.PtView[db]); got != exp {
		t.Fatal(fmt.Errorf("max shard num got %d exp %d", got, exp))
	}

	splitPoint := shards[0].Max

	// check new data balance
	s0Count := dbPTStat[db][0].rpStatistics[rp].shardStatistics[shards[0].ID].shardRowCount
	s1Count := dbPTStat[db][1].rpStatistics[rp].shardStatistics[shards[1].ID].shardRowCount
	var maxCount uint64
	if s0Count < s1Count {
		maxCount = s1Count
	}
	avgCount := (s0Count + s1Count) / 2
	if float64(maxCount/avgCount) > 1+ms.GetConfig().ImbalanceFactor {
		t.Fatal(fmt.Errorf("got imbalance result shard row count is %d %d", s0Count, s1Count))
	}

	logger2.GetLogger().Info("generate data grater than split point")
	// generate data grater than splitPoint and then split happened
	if err = generateData(ms, db, rp, mst, 1000, 1, 10, splitPoint); err != nil {
		t.Fatal(err)
	}

	data = ms.GetStore().GetData()
	shardgroups := data.Database(db).RetentionPolicy(rp).ShardGroups
	shards = shardgroups[len(shardgroups)-1].Shards
	if got, exp := len(shards), len(data.PtView[db]); got != exp {
		t.Fatal(fmt.Errorf("max shard num got %d exp %d", got, exp))
	}

	logger2.GetLogger().Info("generate data all series")
	// generate new data contain all series
	errChan := make(chan error)
	go func() {
		errChan <- generateData(ms, db, rp, mst, 100, 10, 10, "")
	}()

	go func() {
		errChan <- generateData(ms, db, rp, mst, 1000, 1, 10, splitPoint)
	}()
	for i := 0; i < 2; i++ {
		err := <-errChan
		if err != nil {
			t.Fatal(err)
		}
	}

	// check new data balance
	s0Count = dbPTStat[db][0].rpStatistics[rp].shardStatistics[shards[0].ID].shardRowCount
	s1Count = dbPTStat[db][1].rpStatistics[rp].shardStatistics[shards[1].ID].shardRowCount
	if s0Count < s1Count {
		maxCount = s1Count
	}
	avgCount = (s0Count + s1Count) / 2
	if float64(maxCount/avgCount) > 1+ms.GetConfig().ImbalanceFactor {
		t.Fatal(fmt.Errorf("got imbalance result shard row count is %d %d", s0Count, s1Count))
	}
}

type MockDBPTStatistics struct {
	db           string
	ptId         uint32
	rpStatistics map[string]*MockRpStatistics
}

type MockRpStatistics struct {
	rp              string
	shardStatistics map[uint64]*MockShardStatistics
}

type MockShardStatistics struct {
	shardKeyValue []string
	distShardKey  map[string]int
	shardRowCount uint64
	maxTime       int64
	mu            sync.RWMutex
}

type MockStore interface {
	GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32,
		shardId uint64, idxes []int64) ([]string, error)
	DeleteDatabase(node *meta2.DataNode, database string, ptId uint32) error
	DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, ptId uint32) error
	DeleteMeasurement(node *meta2.DataNode, db string, rp, name string, shardIds []uint64) error
	MigratePt(uint64, transport.Codec, transport.Callback) error
	SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error)
}

type MockNetStorage struct {
}

func (s *MockNetStorage) GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32,
	shardId uint64, idxes []int64) ([]string, error) {
	shardStat := dbPTStat[database][pt].rpStatistics["autogen"].shardStatistics[shardId]
	var splitPoints []string
	count := 0
	j := 0
	for i := range shardStat.shardKeyValue {
		count += shardStat.distShardKey[shardStat.shardKeyValue[i]]
		if int64(count) >= idxes[j] {
			splitPoints = append(splitPoints, shardStat.shardKeyValue[i])
			j++
			if j >= len(idxes) {
				break
			}
		}
	}

	return splitPoints, nil
}

func (s *MockNetStorage) DeleteDatabase(node *meta2.DataNode, database string, ptId uint32) error {
	return nil
}

func (s *MockNetStorage) DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, ptId uint32) error {
	return nil
}

func (s *MockNetStorage) DeleteMeasurement(node *meta2.DataNode, db, rp, name string, shardIds []uint64) error {
	return nil
}

func (s *MockNetStorage) MigratePt(uint64, transport.Codec, transport.Callback) error {
	return nil
}

func (s *MockNetStorage) SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error) {
	return 0, nil
}

func NewMockNetStorage() MockStore {
	return &MockNetStorage{}
}

func generateData(ms *meta.MetaService, db, rp, mst string, shardKeyN int, valueN int, timeN int, splitPoint string) error {
	errChan := make(chan error)
	for i := 0; i < shardKeyN; i++ {
		go func(idx int) {
			var err error
			for pointCountIdx := 0; pointCountIdx < timeN; pointCountIdx++ {
				timestamp := time.Now()
				msti, err := ms.GetStore().GetData().Measurement(db, rp, mst)
				if err != nil {
					errChan <- err
					return
				}
				// create shard group
				sg, _ := ms.GetStore().GetData().ShardGroupByTimestampAndEngineType(db, rp, timestamp, config.TSSTORE)
				if sg == nil {
					cmd := meta.GenerateCreateShardGroupCmd(db, rp, timestamp, config.TSSTORE)
					err := ms.GetStore().ApplyCmd(cmd)
					if err != nil {
						logger2.GetLogger().Error("create shard group failed", zap.Error(err))
						errChan <- err
						return
					}
				}
				sg, err = ms.GetStore().GetData().ShardGroupByTimestampAndEngineType(db, rp, timestamp, config.TSSTORE)
				if err != nil {
					logger2.GetLogger().Error("get shard group failed", zap.Error(err))
					errChan <- err
					return
				}
				shardKey := msti.GetShardKey(sg.ID)
				if shardKey == nil {
					errChan <- fmt.Errorf("point should have shardKey")
					return
				}
				tagValue := "host_" + strconv.Itoa(idx)
				shardKeyAndValue := shardKey.ShardKey[0] + "=" + tagValue
				if strings.Compare(shardKeyAndValue, splitPoint) < 0 {
					continue
				}
				sh := sg.DestShard(shardKeyAndValue)
				dbPTMu.Lock()
				if dbPTStat[db] == nil {
					dbPTStat[db] = make(map[uint32]*MockDBPTStatistics)
				}

				if dbPTStat[db][sh.Owners[0]] == nil {
					dbPTStat[db][sh.Owners[0]] = &MockDBPTStatistics{db: db, ptId: sh.Owners[0], rpStatistics: make(map[string]*MockRpStatistics)}
				}

				dbptStat := dbPTStat[db][sh.Owners[0]]
				if dbptStat.rpStatistics[rp] == nil {
					dbptStat.rpStatistics[rp] = &MockRpStatistics{rp: rp, shardStatistics: make(map[uint64]*MockShardStatistics)}
				}
				rpStat := dbptStat.rpStatistics[rp]
				if rpStat.shardStatistics[sh.ID] == nil {
					rpStat.shardStatistics[sh.ID] = &MockShardStatistics{}
					rpStat.shardStatistics[sh.ID].distShardKey = make(map[string]int)
				}
				shStat := rpStat.shardStatistics[sh.ID]
				dbPTMu.Unlock()

				shStat.mu.Lock()
				index := sort.SearchStrings(shStat.shardKeyValue, shardKeyAndValue)

				if shStat.shardKeyValue == nil {
					shStat.shardKeyValue = []string{shardKeyAndValue}
				} else if index == len(shStat.shardKeyValue) {
					shStat.shardKeyValue = append(shStat.shardKeyValue, shardKeyAndValue)
				} else if shStat.shardKeyValue[index] != shardKeyAndValue {
					shStat.shardKeyValue = append(shStat.shardKeyValue[:index],
						append([]string{shardKeyAndValue}, shStat.shardKeyValue[index:]...)...)
				}

				shStat.shardRowCount = shStat.shardRowCount + uint64(valueN)
				shStat.distShardKey[shardKeyAndValue] = shStat.distShardKey[shardKeyAndValue] + valueN
				if timestamp.UnixNano() > shStat.maxTime {
					shStat.maxTime = timestamp.UnixNano()
				}
				shStat.mu.Unlock()
				time.Sleep(reportInterval)
			}
			errChan <- err

		}(i)
	}

	for i := 0; i < shardKeyN; i++ {
		err := <-errChan
		if err != nil {
			return err
		}
	}
	return nil
}

func MockReportLoad(ms *meta.MetaService, db string, ptId uint32) {
	t := time.NewTicker(reportInterval)
	for {
		select {
		case <-t.C:
			dbPTMu.RLock()
			if dbPTStat[db] == nil || dbPTStat[db][ptId] == nil || dbPTStat[db][ptId].rpStatistics == nil {
				dbPTMu.RUnlock()
				continue
			}
			var loads []*proto2.DBPtStatus
			load := &proto2.DBPtStatus{
				DB:   proto.String(db),
				PtID: proto.Uint32(ptId),
			}
			for rp := range dbPTStat[db][ptId].rpStatistics {
				rpStat := &proto2.RpShardStatus{RpName: proto.String(rp)}
				shStat := dbPTStat[db][ptId].rpStatistics[rp].shardStatistics
				for shardID := range shStat {
					dbPTStat[db][ptId].rpStatistics[rp].shardStatistics[shardID].mu.RLock()
					rpStat.ShardStats = &proto2.ShardStatus{
						ShardID:     proto.Uint64(shardID),
						ShardSize:   proto.Uint64(shStat[shardID].shardRowCount),
						SeriesCount: proto.Int(len(shStat[shardID].shardKeyValue)),
						MaxTime:     proto.Int64(shStat[shardID].maxTime),
					}
					dbPTStat[db][ptId].rpStatistics[rp].shardStatistics[shardID].mu.RUnlock()
				}
				load.RpStats = []*proto2.RpShardStatus{rpStat}
			}
			loads = append(loads, load)
			dbPTMu.RUnlock()
			val := &proto2.ReportShardsLoadCommand{DBPTStat: loads}
			t1 := proto2.Command_ReportShardsCommand
			cmd := &proto2.Command{Type: &t1}
			if err := proto.SetExtension(cmd, proto2.E_ReportShardsLoadCommand_Command, val); err != nil {
				panic(err)
			}
			b, err := proto.Marshal(cmd)
			if err != nil {
				logger2.GetLogger().Warn("marsh report cmd failed", zap.Error(err))
				continue
			}
			err = ms.GetStore().UpdateLoad(b)
		case <-ms.GetStore().GetClose():
			return
		}
	}
}

func TestConcurrentApply(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	PrintMemUsage()
	BatchApplyCmd(mms, 100, 60)
	PrintMemUsage()
}

func BatchApplyCmd(mms *meta.MockMetaService, goroutineNum int, dbNumPerRoutine int) {
	errChan := make(chan error)
	dbPrefix := "foo"
	rp := "autogen"
	mst := "cpu"
	startTime := time.Now()
	for i := 0; i < goroutineNum; i++ {
		go func(idx int) {
			commonDbName := dbPrefix + fmt.Sprint(idx)
			var err error
			for j := 0; j < dbNumPerRoutine; j++ {
				dbName := commonDbName + "-" + fmt.Sprint(j)
				err = mms.GetStore().ApplyCmd(meta.GenerateCreateDatabaseCmd(dbName))
				if err != nil {
					errChan <- err
				}
				err = mms.GetStore().ApplyCmd(meta.GenerateCreateMeasurementCmd(dbName, rp, mst, nil, "hash"))
				if err != nil {
					errChan <- err
				}
				err = mms.GetStore().ApplyCmd(meta.GenerateCreateShardGroupCmd(dbName, rp, time.Now(), config.TSSTORE))
				if err != nil {
					errChan <- err
				}
			}
			errChan <- err
		}(i)
	}

	for i := 0; i < goroutineNum; i++ {
		err := <-errChan
		if err != nil {
			fmt.Printf("apply command failed due to %v\n", err)
		}
	}
	fmt.Printf("batch apply cost %v\n", time.Since(startTime))
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %.2f MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %.2f MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %.2f MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) float32 {
	return float32(b) / 1024. / 1024.
}

func shardInfoMsgHandler(cmd *proto2.Command, mms *meta.MockMetaService) error {
	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	msg := message.NewMetaMessage(message.GetShardInfoRequestMessage, &message.GetShardInfoRequest{Body: b})
	h := meta.New(msg.Type())
	err = h.SetRequestMsg(msg.Data())
	if err != nil {
		return err
	}
	h.InitHandler(mms.GetStore(), mms.GetConfig(), nil)
	if err != nil {
		return err
	}
	resp, err := h.Process()
	if err != nil {
		return err
	}
	res := resp.(*message.GetShardInfoResponse)
	if res.Err != "" {
		return fmt.Errorf(res.Err)
	}
	return nil
}

func TestGetShardInfo_Process(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "db0"
	rp := "autogen"
	mst := "mst0"
	mms.GetStore().NetStore = meta.NewMockNetStorage()
	err = mms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmd(db), mms.GetConfig())
	if err != nil {
		t.Fatal(err)
	}
	err = mms.GetStore().ApplyCmd(meta.GenerateCreateMeasurementCmd(db, rp, mst, nil, "hash"))
	if err != nil {
		t.Fatal(err)
	}
	err = mms.GetStore().ApplyCmd(meta.GenerateCreateShardGroupCmd(db, rp, time.Now(), config.TSSTORE))
	if err != nil {
		t.Fatal(err)
	}

	cmd = meta.GenerateGetShardRangeInfoCmd(db, rp, 2)
	err = shardInfoMsgHandler(cmd, mms)
	assert.True(t, err.Error() == errno.NewError(errno.ShardMetaNotFound, 2).Error(),
		"actual err ", err.Error(), ";expect err ", errno.NewError(errno.ShardMetaNotFound, 2).Error())

	cmd = meta.GenerateShardDurationCmd(14, []uint32{0}, mms.GetService().Node.ID)
	err = shardInfoMsgHandler(cmd, mms)
	assert.True(t, err.Error() == errno.NewError(errno.DataIsOlder).Error(),
		"actual err ", err.Error(), ";expect err ", errno.NewError(errno.DataIsOlder, mms.GetStore().GetData().Index, 12).Error())

	cmd = meta.GenerateShardDurationCmd(0, []uint32{0}, mms.GetStore().GetData().PtView["db0"][0].Owner.NodeID)
	err = shardInfoMsgHandler(cmd, mms)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreDeleteDatabase(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "test"
	mms.GetStore().NetStore = meta.NewMockNetStorage()
	err = mms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmd(db), mms.GetConfig())
	if err != nil {
		t.Fatal(err)
	}

	cmd = meta.GenerateMarkDatabaseDelete(db)
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	netStore := meta.NewMockNetStorage()
	mms.GetStore().NetStore = netStore

	dbi := mms.GetStore().GetData().Database(db)
	assert.Equal(t, dbi.MarkDeleted, true)

	for {
		dbi = mms.GetStore().GetData().Database(db)
		if dbi == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	netStore.DeleteDatabaseFn = func(node *meta2.DataNode, database string, ptId uint32) error {
		return errno.NewError(errno.NoConnectionAvailable)
	}
	if err = mms.GetStore().ApplyCmd(meta.GenerateMarkDatabaseDelete(db)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	dbi = mms.GetStore().GetData().Database(db)
	assert.Equal(t, true, dbi != nil)
}

func TestStoreDeleteRetentionPolicy(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	dbName := "test"
	rpName := "autogen"
	mms.GetStore().NetStore = meta.NewMockNetStorage()
	err = mms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	if err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmd(dbName), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}

	rp, err := mms.GetStore().GetData().RetentionPolicy(dbName, rpName)
	assert.Equal(t, true, rp != nil)
	netStore := meta.NewMockNetStorage()
	mms.GetStore().NetStore = netStore

	cmd = meta.GenerateMarkRpDelete(dbName, rpName)
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	rp = mms.GetStore().GetData().Database(dbName).RetentionPolicy(rpName)
	assert.Equal(t, true, rp.MarkDeleted)
	for {
		rp = mms.GetStore().GetData().Database(dbName).RetentionPolicy(rpName)
		if rp == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	netStore.DeleteRetentionPolicyFn = func(node *meta2.DataNode, db string, rp string, ptId uint32) error {
		return errno.NewError(errno.NoConnectionAvailable)
	}
	dbName = "test1"
	if err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmd(dbName), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	if err = mms.GetStore().ApplyCmd(meta.GenerateMarkRpDelete(dbName, rpName)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	rp = mms.GetStore().GetData().Database(dbName).RetentionPolicy(rpName)
	assert.Equal(t, true, rp != nil)
}

func TestStoreDeleteMeasurement(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	db := "db0"
	rp := "autogen"
	mst := "mst0"
	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	netStore := meta.NewMockNetStorage()
	mms.GetStore().NetStore = netStore
	err = mms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	if err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmd(db), mms.GetConfig()); err != nil {
		t.Fatal(err)
	}

	if err = mms.GetStore().ApplyCmd(meta.GenerateCreateMeasurementCmd(db, rp, mst, nil, "hash")); err != nil {
		t.Fatal(err)
	}

	if err = mms.GetStore().ApplyCmd(meta.GenerateCreateShardGroupCmd(db, rp, time.Now(), config.TSSTORE)); err != nil {
		t.Fatal(err)
	}

	if err = mms.GetStore().ApplyCmd(meta.GenerateMarkMeasurementDeleteCmd(db, rp, mst)); err != nil {
		t.Fatal(err)
	}

	msti := mms.GetStore().GetData().Database(db).RetentionPolicy(rp).Measurement(mst)
	assert.Equal(t, true, msti.MarkDeleted)

	for {
		msti = mms.GetStore().GetData().Database(db).RetentionPolicy(rp).Measurement(mst)
		if msti == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	netStore.DeleteMeasurementFn = func(node *meta2.DataNode, db string, rp, name string, shardIds []uint64) error {
		return errno.NewError(errno.NoConnectionAvailable)
	}
	if err = mms.GetStore().ApplyCmd(meta.GenerateCreateMeasurementCmd(db, rp, mst, nil, "hash")); err != nil {
		t.Fatal(err)
	}
	if err = mms.GetStore().ApplyCmd(meta.GenerateMarkMeasurementDeleteCmd(db, rp, mst)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	msti = mms.GetStore().GetData().Database(db).RetentionPolicy(rp).Measurement(mst)
	assert.Equal(t, true, msti != nil)
}

func TestDownSampleCommands(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.NewMockMetaService(dir, "127.0.0.1")

	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ms.Close()
	}()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err := ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "db0"
	rp := "autogen"

	duration := 720 * time.Hour
	sampleIntervals := []time.Duration{24 * time.Hour, 168 * time.Hour}
	timeIntervals := []time.Duration{time.Minute, 15 * time.Minute}
	calls := []*meta2.DownSampleOperators{
		{
			AggOps:   []string{"min,first"},
			DataType: int64(influxql.Float),
		},
		{
			AggOps:   []string{"sum", "count"},
			DataType: int64(influxql.Integer),
		},
	}
	ms.GetStore().NetStore = meta.NewMockNetStorage()
	err = ms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	if err = meta.ProcessExecuteRequest(ms.GetStore(), meta.GenerateCreateDatabaseCmd(db), ms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = ms.GetStore().ApplyCmd(meta.GenerateCreateDownSampleCmd(db, rp, duration, sampleIntervals, timeIntervals, calls)); err != nil {
		t.Fatal(err)
	}
	if err = ms.GetStore().ApplyCmd(meta.GenerateDropDownSampleCmd(db, rp, false)); err != nil {
		t.Fatal(err)
	}
	_, _ = ms.GetStore().GetDownSampleInfo()
	_, _ = ms.GetStore().GetRpMstInfos(db, rp, []int64{1})
	ident := &meta2.ShardIdentifier{
		ShardID: 1,
		OwnerDb: "db0",
		Policy:  "autogen",
	}
	if err := ms.GetStore().ApplyCmd(meta.GenerateUpdateShardDownSampleInfoCmd(ident)); err != nil {
		t.Fatal(err)
	}

	rpInfos := &meta2.RetentionPolicyInfo{
		Measurements: map[string]*meta2.MeasurementInfo{"mst_0000": {
			Name:   "mst_0000",
			Schema: map[string]int32{"age": influx.Field_Type_Int},
		}},
		MstVersions: map[string]meta2.MeasurementVer{
			"mst": {NameWithVersion: "mst_0000", Version: 0},
		},
	}
	dataTypes := []int64{influx.Field_Type_Int}
	_, _ = meta.TransMeasurementInfos2Bytes(dataTypes, rpInfos)
}

func TestStreamCommands(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.NewMockMetaService(dir, "127.0.0.1")

	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ms.Close()
	}()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err := ms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "db0"
	rp := "autogen"
	srcMst := "mst0"
	desMst := "mst1"
	name := "test"
	dims := []string{"tag1"}
	ms.GetStore().NetStore = meta.NewMockNetStorage()
	if err = meta.ProcessExecuteRequest(ms.GetStore(), meta.GenerateCreateDatabaseCmd(db), ms.GetConfig()); err != nil {
		t.Fatal(err)
	}
	calls := []*meta2.StreamCall{
		{
			Call:  "first",
			Field: "age",
			Alias: "first_age",
		},
		{
			Call:  "first",
			Field: "score",
			Alias: "first_score",
		},
		{
			Call:  "first",
			Field: "name",
			Alias: "first_name",
		},
		{
			Call:  "first",
			Field: "alive",
			Alias: "first_alive",
		},
	}
	if err = ms.GetStore().ApplyCmd(meta.GenerateCreateStreamCmd(db, rp, srcMst, desMst, name, calls, dims, 0, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = ms.GetStore().ApplyCmd(meta.GenerateDropStreamCmd(name)); err != nil {
		t.Fatal(err)
	}
}

func TestStoreExpandGroups(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.NewMockMetaService(dir, "127.0.0.1")

	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ms.Close()
	}()

	if err = ms.GetStore().ExpandGroups(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateDataNodeRepeat(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	mms.GetStore().GetData().MetaNodes = append(mms.GetStore().GetData().MetaNodes, *new(meta2.NodeInfo))
	cmd = meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
}

func TestStore_registerQueryIDOffset(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	cmd := meta.GenerateRegisterQueryIDOffsetCmd("127.0.0.1:8086")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateRegisterQueryIDOffsetCmd("127.0.0.1:8087")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateRegisterQueryIDOffsetCmd("127.0.0.1:8088")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionPolicyAutoCreate(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "test"
	mms.GetStore().NetStore = meta.NewMockNetStorage()
	err = mms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}
	err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmdWithDefaultRep(db, 1), mms.GetConfig())
	if err != nil {
		t.Fatal(err)
	}
}

func createUpdateReplicationCmd(db string, rgId, masterId uint32, peers []meta2.Peer, rgStatus uint32) *proto2.Command {
	mPeers := make([]*proto2.Peer, len(peers))
	for i := range peers {
		role := uint32(peers[i].PtRole)
		mPeers[i] = &proto2.Peer{
			ID:   &peers[i].ID,
			Role: &role,
		}
	}

	val := &proto2.UpdateReplicationCommand{
		Database:   proto.String(db),
		RepGroupId: proto.Uint32(rgId),
		MasterId:   proto.Uint32(masterId),
		Peers:      mPeers,
		RgStatus:   proto.Uint32(rgStatus),
	}

	t1 := proto2.Command_UpdateReplicationCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_UpdateReplicationCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func TestUpdateReplicationCommand(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "test"
	mms.GetStore().NetStore = meta.NewMockNetStorage()
	err = mms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}

	config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmdWithDefaultRep(db, 2), mms.GetConfig())
	if err != nil {
		t.Fatal(err)
	}

	// abnormal branch, database does not exist
	cmd = createUpdateReplicationCmd("db not exist", 0, 0, nil, uint32(meta2.SubHealth))
	err = mms.GetStore().ApplyCmd(cmd)
	require.Error(t, errno.NewError(errno.DatabaseNotFound, "db not exist"))

	// normal branch, database exist
	cmd = createUpdateReplicationCmd(db, 0, 0, nil, uint32(meta2.SubHealth))
	err = mms.GetStore().ApplyCmd(cmd)
	require.Equal(t, nil, err)
}

func createUpdateRetentionPolicyCmd(db, rp string, rgId, masterId uint32, peers []meta2.Peer, rgStatus uint32) *proto2.Command {
	mPeers := make([]*proto2.Peer, len(peers))
	for i := range peers {
		role := uint32(peers[i].PtRole)
		mPeers[i] = &proto2.Peer{
			ID:   &peers[i].ID,
			Role: &role,
		}
	}

	var i int64
	val := &proto2.UpdateRetentionPolicyCommand{
		Database:     proto.String(db),
		Name:         proto.String(rp),
		NewName:      proto.String(rp),
		WarmDuration: &i,
		MakeDefault:  proto.Bool(true),
	}

	t1 := proto2.Command_UpdateRetentionPolicyCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_UpdateRetentionPolicyCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func TestUpdateRetentionPolicyCommand(t *testing.T) {
	dir := t.TempDir()
	mms, err := meta.NewMockMetaService(dir, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()

	cmd := meta.GenerateCreateDataNodeCmd("127.0.0.1:8400", "127.0.0.1:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}
	cmd = meta.GenerateCreateDataNodeCmd("127.0.0.2:8400", "127.0.0.2:8401")
	if err = mms.GetStore().ApplyCmd(cmd); err != nil {
		t.Fatal(err)
	}

	db := "test"
	mms.GetStore().NetStore = meta.NewMockNetStorage()
	err = mms.GetStore().GetData().UpdateNodeStatus(2, int32(serf.StatusAlive), 1, "127.0.0.1:8011")
	if err != nil {
		t.Fatal(err)
	}

	config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	err = meta.ProcessExecuteRequest(mms.GetStore(), meta.GenerateCreateDatabaseCmdWithDefaultRep(db, 2), mms.GetConfig())
	if err != nil {
		t.Fatal(err)
	}

	// abnormal branch, database does not exist
	cmd = createUpdateReplicationCmd("db not exist", 0, 0, nil, uint32(meta2.SubHealth))
	err = mms.GetStore().ApplyCmd(cmd)
	require.Error(t, errno.NewError(errno.DatabaseNotFound, "db not exist"))

	// normal branch, database exist
	cmd = createUpdateRetentionPolicyCmd(db, "autogen", 0, 0, nil, uint32(meta2.SubHealth))
	err = mms.GetStore().ApplyCmd(cmd)
	require.Equal(t, nil, err)
}
