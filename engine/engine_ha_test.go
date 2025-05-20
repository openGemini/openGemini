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

package engine

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_Engine_Assign_404PT(t *testing.T) {
	dir := t.TempDir()
	e, err := initEngine(dir)
	defer e.Close()
	err = e.Assign(0, 1, defaultDb, defaultPtId, 1, nil, nil, mockMetaClient(), nil)
	require.NoError(t, err)
}

func Test_Engine_Assign_ObsErr(t *testing.T) {
	dir := t.TempDir()
	e, err := initEngine(dir)
	defer e.Close()
	err = e.Assign(0, 1, defaultDb, defaultPtId, 1, nil, nil, &mockMetaClient4Obs{}, nil)
	require.NoError(t, err)
}

func Test_Engine_Assign_success(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine(dir)
	eng.Close()
	delete(eng.DBPartitions, defaultDb)
	log = eng.log
	if err != nil {
		t.Fatal(err)
	}
	shardDurationInfo := map[uint64]*meta.ShardDurationInfo{
		defaultShardId: getShardDurationInfo(defaultShardId),
	}
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.Assign(0, 1, defaultDb, defaultPtId, 1, shardDurationInfo, dbBriefInfo, mockMetaClient(), nil)
	require.NoError(t, err)
	eng.Close()
}

// TestPreOffLoadPts001 test parallel scene about PrepareOffLoadPts and DropDatabase.
func TestPreOffLoadPts001(t *testing.T) {
	dir := t.TempDir()
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		fmt.Println("pre offload start")
		err := eng.PreOffload(0, defaultDb, defaultPtId)
		if err != nil && (!errno.Equal(err, errno.DBPTClosed) && !errno.Equal(err, errno.PtNotFound)) {
			t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
		}
		fmt.Println("pre offload end")
		wg.Done()
	}()
	go func() {
		fmt.Println("begin delete db")
		err := eng.DeleteDatabase(defaultDb, defaultPtId)
		if err != nil {
			t.Error("expect delete database success", zap.Error(err))
		}
		wg.Done()
	}()
	wg.Wait()
}

// TestPreOffLoadPts002 test function status after prepare off load.
func TestPreOffLoadPts002(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.PreOffload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, should prepare off load success", zap.Error(err))
	}
	sh, ok := eng.DBPartitions[defaultDb][defaultPtId].shards[defaultShardId].(*shard)
	if ok {
		if reflect.DeepEqual(true, sh.immTables.CompactionEnabled()) {
			t.Fatal("shard compact enable should stoped")
		}
		if !reflect.DeepEqual(uint64(1), sh.immTables.GetFileSeq()) {
			t.Fatal("shardsMaxFileNum map content in migrate instance is wrong")
		}
	}
}

// TestPreOffLoadPts003 test PreOffload function. reentrant scene of pt not exist.
func TestPreOffLoadPts003(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	pt, err := eng.getPartition(defaultDb, defaultPtId, false)
	if err != nil {
		t.Error("PreOffLoadPt but Pt not found", zap.Error(err))
	}
	delete(eng.DBPartitions[defaultDb], defaultPtId)
	err = eng.PreOffload(0, defaultDb, defaultPtId)
	eng.DBPartitions[defaultDb][defaultPtId] = pt
	require.NoError(t, err)
}

// TestPreOffLoadPts004 test PreOffload function. shard expiring scene.
func TestPreOffLoadPts004(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	pt, err := eng.getPartition(defaultDb, defaultPtId, false)
	if err != nil {
		t.Error("PreOffLoadPt but Pt not found", zap.Error(err))
	}
	pt.pendingShardDeletes[defaultShardId] = struct{}{}
	err = eng.PreOffload(0, defaultDb, defaultPtId)
	require.Error(t, err)
}

// TestOffloadPts001 test parallel scene about OffloadPts and DropDatabase.
func TestOffloadPts001(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := eng.Offload(0, defaultDb, defaultPtId)
		if err != nil && (!errno.Equal(err, errno.DBPTClosed) && !errno.Equal(err, errno.PtNotFound)) {
			t.Error("OffLoadPts failed, and err isn't dbPt close", zap.Error(err))
		}
	}()
	go func() {
		defer wg.Done()
		err := eng.DeleteDatabase(defaultDb, defaultPtId)
		ok := err == nil || errno.Equal(err, errno.DBPTClosed) || err.Error() == meta.ErrConflictWithIo.Error()
		require.True(t, ok, "expect delete database success", zap.Error(err))
	}()
	wg.Wait()
}

// TestOffloadPts002 test OffloadPts function. 1. delete Pt success.2.receive closed signal.
func TestOffloadPts002(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		dbPtInfo := eng.DBPartitions[defaultDb][defaultPtId]
		select {
		case <-dbPtInfo.unload:
			wg.Done()
			return
		case <-time.After(2 * time.Second):
			wg.Done()
			t.Error("dbPtInfo should get closed signal")
			return
		}
	}()

	err := eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)
	wg.Wait()
}

// TestOffloadPts003 test OffloadPts function. PreOffLoadPts and then OffloadPts.
func TestOffloadPts003(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.PreOffload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, should prepare off load success", zap.Error(err))
	}

	sh, ok := eng.DBPartitions[defaultDb][defaultPtId].shards[defaultShardId].(*shard)
	if ok {
		if reflect.DeepEqual(true, sh.immTables.CompactionEnabled()) {
			t.Fatal("shard compact enable should stoped")
		}
		if !reflect.DeepEqual(uint64(1), sh.immTables.GetFileSeq()) {
			t.Fatal("shardsMaxFileNum map content in migrate instance is wrong")
		}
	}
	err = eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)
}

func TestOffloadPts004(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)

	err = eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)
}

// TestPrepareLoadPts001 test PrepareLoadPts, 1. off load DB PT, then 2.PrepareLoadPts. check shard in the process.
func TestPrepareLoadPts001(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)

	err := eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)

	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.PreAssign(0, defaultDb, defaultPtId, durationInfos, dbBriefInfo, mockMetaClient())
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, false, defaultDb, defaultPtId, true)
}

// TestPrepareLoadPts002 test PrepareLoadPts, don't have problem when force flushing or eng.close after PrepareLoadPts
// 1. off load DB PT, then 2.PrepareLoadPts. 3.eng.ForceFlush.
func TestPrepareLoadPts002(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.Offload(0, defaultDb, defaultPtId)
	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.PreAssign(0, defaultDb, defaultPtId, durationInfos, dbBriefInfo, mockMetaClient())
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, false, defaultDb, defaultPtId, true)

	eng.ForceFlush()
}

func TestPrepareLoadPts003(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.Offload(0, defaultDb, defaultPtId)
	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.PreAssign(0, defaultDb, defaultPtId, durationInfos, dbBriefInfo, mockMetaClient())
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, false, defaultDb, defaultPtId, true)

	eng.ForceFlush()
}

func TestPrepareLoadPts004(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.Offload(0, defaultDb, defaultPtId)
	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.PreAssign(0, defaultDb, defaultPtId, durationInfos, dbBriefInfo, mockMetaClient())
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, false, defaultDb, defaultPtId, true)
	err = eng.PreAssign(0, defaultDb, defaultPtId, durationInfos, dbBriefInfo, mockMetaClient())
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	dbPt, _ := eng.getPartition(defaultDb, defaultPtId, false)
	if err = eng.offloadDbPT(dbPt); err != nil {
		t.Error("Rollback preload failed", zap.Error(err))
	}
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	eng.ForceFlush()
}

func Test_PreAssign_Obs(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)

	err := eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)

	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.PreAssign(0, defaultDb, defaultPtId, durationInfos, dbBriefInfo, &mockMetaClient4Obs{})
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, false, defaultDb, defaultPtId, true)
}

// TestLoadPts001 test LoadPts. 1. off load DB PT, then 2.loadPts. check shard in the process.
func TestLoadPts001(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)

	err := eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)

	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.Assign(0, 1, defaultDb, defaultPtId, 0, durationInfos, dbBriefInfo, mockMetaClient(), nil)
	if err != nil {
		t.Error("LoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)
}

// TestLoadPts002 test LoadPts, PrepareLoadPts and LoadPts, check shard content is correct in this process.
// 1. off load DB PT, then 2.PrepareLoadPts and LoadPts.
func TestLoadPts002(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)

	err := eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, false, defaultDb, defaultPtId, false)

	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err = eng.PreAssign(0, defaultDb, defaultPtId, durationInfos, dbBriefInfo, mockMetaClient())
	if err != nil {
		t.Error("PrepareLoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, false, defaultDb, defaultPtId, true)
	err = eng.Assign(0, 1, defaultDb, defaultPtId, 0, durationInfos, dbBriefInfo, mockMetaClient(), nil)
	if err != nil {
		t.Error("LoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)
}

func TestLoadPts003(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)

	durationInfos := getDurationInfo(defaultShardId)
	dbBriefInfo := &meta.DatabaseBriefInfo{
		Name:           defaultDb,
		EnableTagArray: false,
	}
	err := eng.Assign(0, 1, defaultDb, defaultPtId, 0, durationInfos, dbBriefInfo, mockMetaClient(), nil)
	if err != nil {
		t.Error("LoadPts failed", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)
}

func TestRollPreOffload001(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.PreOffload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, should prepare off load success", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, false)
	err = eng.RollbackPreOffload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Fatal("RollbackPrepareOffload failed", zap.Error(err))
	}
	sh, ok := eng.DBPartitions[defaultDb][defaultPtId].shards[defaultShardId].(*shard)
	if ok {
		if !reflect.DeepEqual(uint64(1), sh.immTables.GetFileSeq()) {
			t.Fatal("shardsMaxFileNum map content in migrate instance is wrong")
		}
	}
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, true)
}

func TestRollPreOffload002(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()

	err := eng.PreOffload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, should prepare off load success", zap.Error(err))
	}
	checkShard(t, eng, 1, defaultShardId, true, defaultDb, defaultPtId, false)

	err = eng.Offload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Error("PrepareOffLoadPts failed, and err isn't dbPt close", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, true, defaultDb, defaultPtId, false)

	err = eng.RollbackPreOffload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Fatal("RollbackPrepareOffload failed", zap.Error(err))
	}
	checkShard(t, eng, 0, defaultShardId, true, defaultDb, defaultPtId, true)
}

func TestPreOffloadDoShardMove(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()
	eng.DBPartitions[defaultDb][defaultPtId].doingShardMoveN = 1
	err := eng.PreOffload(0, defaultDb, defaultPtId)
	if !errno.Equal(err, errno.PtIsDoingSomeShardMove) {
		t.Fatal("TestPreOffloadDoShardMove err")
	}
}

func TestRollbackPreOffloadDoShardMove(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	eng := getEngineBeforeTest(t, dir)
	defer eng.Close()
	eng.DBPartitions[defaultDb][defaultPtId].doingShardMoveN = 1
	err := eng.RollbackPreOffload(0, defaultDb, defaultPtId)
	if err != nil {
		t.Fatal("TestRollbackPreOffloadDoShardMove err")
	}
}

func checkShard(t *testing.T, e *Engine, shNum int, shardId uint64, hasIndex bool, db string, ptId uint32, able bool) {
	hasDBPT := shNum != 0
	if _, ok := e.DBPartitions[db]; ok != hasDBPT {
		t.Error("should have this db", zap.String("dbName", db))
	}
	if !hasDBPT {
		return
	}
	if _, ok := e.DBPartitions[db][ptId]; !ok {
		t.Error("should have this pt", zap.Uint32("ptId", ptId))
	}
	if shNum == 0 {
		return
	}
	if !reflect.DeepEqual(shNum, len(e.DBPartitions[db][ptId].shards)) {
		t.Error("the number of shards in db pt isn't expect", zap.Int("expect", shNum),
			zap.Int("fact", len(e.DBPartitions[db][ptId].shards)))
	}
	if _, ok := e.DBPartitions[db][ptId].shards[shardId]; !ok {
		t.Error("should have this shard", zap.Uint64("ptId", shardId))
	}
	sh, ok := e.DBPartitions[db][ptId].shards[shardId].(*shard)
	if ok {
		has := sh.indexBuilder != nil
		if has != hasIndex {
			t.Error("expect sh's indexBuilder", zap.Bool("expect", hasIndex), zap.Bool("fact", has))
		}
		if sh.immTables.CompactionEnabled() != able {
			t.Error("compaction status is wrong", zap.Bool("expect", able))
		}
		if sh.immTables.MergeEnabled() != able {
			t.Error("merge status is wrong", zap.Bool("expect", able))
		}
	}
}

func getDurationInfo(shId uint64) map[uint64]*meta.ShardDurationInfo {
	durationInfos := make(map[uint64]*meta.ShardDurationInfo)
	durationInfos[shId] = getShardDurationInfo(shId)
	return durationInfos
}

func getEngineBeforeTest(t *testing.T, dir string) *Engine {
	eng, err := initEngine(dir)
	log = eng.log
	if err != nil {
		t.Fatal(err)
	}
	msNames := []string{"cpu"}
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 20, time.Second, tm, false, true, false)
	if err := eng.WriteRows(defaultDb, defaultRp, defaultPtId, defaultShardId, rows, nil, nil); err != nil {
		t.Fatal(err)
	}
	eng.ForceFlush()
	return eng
}
