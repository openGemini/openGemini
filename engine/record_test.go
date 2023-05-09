package engine

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

var defaultString = "abcdefghijklmnopqrstuvwxyz"

const xxx = "111"
const defaultRp1 = "rp0"
const defaultDb1 = "db0"
const defaultShGroupId1 = uint64(1)
const defaultShardId1 = uint64(1)
const defaultPtId1 = uint32(1)
const defaultChunkSize1 = 1000
const defaultMeasurementName1 = "cpu"

func init() {

}

func TestXxx(t *testing.T) {

	pathName := "/tmp/openGemini"
	dataPath := pathName + "/data"
	walPath := pathName + "/wal"
	lockPath := filepath.Join(dataPath, "LOCK")
	indexPath := filepath.Join(pathName, defaultDb, "/index/data")
	ident := &meta.IndexIdentifier{OwnerDb: defaultDb1, OwnerPt: defaultPtId1, Policy: defaultRp1}
	ident.Index = &meta.IndexDescriptor{IndexID: 1, IndexGroupID: 2, TimeRange: meta.TimeRangeInfo{}}
	ltime := uint64(time.Now().Unix())
	opts := new(tsi.Options).
		Ident(ident).
		Path(indexPath).
		IndexType(tsi.MergeSet).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)

	indexBuilder := tsi.NewIndexBuilder(opts)
	primaryIndex, err := tsi.NewIndex(opts)
	if err != nil {
		fmt.Errorf(err.Error())
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	indexRelation, err := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
	indexBuilder.Relations[uint32(tsi.MergeSet)] = indexRelation
	err = indexBuilder.Open()
	if err != nil {
		fmt.Errorf(err.Error())
	}
	shardDuration := &meta.DurationDescriptor{Tier: util.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1970-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2099-01-01T01:00:00Z")}
	shardIdent := &meta.ShardIdentifier{ShardID: defaultShardId, ShardGroupID: 1, OwnerDb: defaultDb1, OwnerPt: defaultPtId1, Policy: defaultRp1}
	sh := NewShard(dataPath, walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption)
	sh.SetIndexBuilder(indexBuilder)
	if err := sh.OpenAndEnable(nil); err != nil {
		_ = sh.Close()
		fmt.Errorf(err.Error())
	}
	index, err := tsi.NewMergeSetIndex(opts)
	fmt.Errorf(index.Path())
}

// func Testxxxx(t *testing.T) {
// 	testDir := t.TempDir()

// 	// step1: clean env
// 	_ = os.RemoveAll(testDir)

// 	// step2: create shard
// 	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// not flush data to snapshot
// 	sh.SetWriteColdDuration(1000 * time.Second)
// 	sh.SetMutableSizeLimit(10000000000)

// 	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
// 	rows, startTime, endTime := GenDataRecord(50, 10, time.Now(), true, true, false, map[string]interface{}{
// 		"agentSN": defaultString, "bn": defaultString, "chunkdid": defaultString, "client_ip": defaultString, "cmpt": defaultString, "collection": defaultString, "dbname": defaultString, "errcode": defaultString, "metrictype": defaultString, "mk": defaultString, "ns": defaultString, "opt": defaultString, "request_method": defaultString, "rgn": defaultString, "schema_name": defaultString, "shard_ip": defaultString, "svc": defaultString,
// 	}, map[string]interface{}{
// 		"field4_float": int64(1),
// 	})
// 	err = writeData(sh, rows, false)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }
