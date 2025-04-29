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

package shelf_test

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func initConfig(n int) func() {
	conf := &config.GetStoreConfig().ShelfMode
	conf.Enabled = true
	conf.SeriesHashFactor = 2
	conf.Concurrent = n
	conf.TSSPConvertConcurrent = max(1, n/4)
	conf.ReliabilityLevel = config.ReliabilityLevelHigh

	shelf.Open()
	return func() {
		conf.ReliabilityLevel = config.ReliabilityLevelMedium
		conf.Enabled = false
	}
}

func TestRunner(t *testing.T) {
	defer initConfig(2)()
	shardID := uint64(10)
	shardIDNotExits := uint64(100)

	var runner = newNilRunner()
	runner.RegisterShard(shardID, nil)
	runner.UnregisterShard(shardID)
	runner.ForceFlush(shardID)
	runner.Close()

	runner = newRunner(shardID, t.TempDir())
	require.NoError(t, writeRows(runner, shardID))
	require.NotEmpty(t, writeRows(runner, shardIDNotExits)) // shard not exists

	runner.RegisterShard(shardID, nil)
	runner.ForceFlush(shardID)
	runner.UnregisterShard(shardID)
	runner.UnregisterShard(shardIDNotExits) // shard not exists

	runner.Close()
}

func TestReadRecord(t *testing.T) {
	defer initConfig(2)()

	shardID := uint64(10)
	runner := newRunner(shardID, t.TempDir())
	defer runner.Close()

	require.NoError(t, writeRows(runner, shardID))

	tr := &util.TimeRange{Max: math.MaxInt64}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "field_float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := shelf.ReadRecord(shardID, "foo_0", 10, tr, schema, true)
	require.True(t, rec.RowNums() > 0)

	rec = shelf.ReadRecord(shardID, "mst_not_exists", 10, tr, schema, true)
	require.True(t, rec == nil || rec.RowNums() == 0)

	// series not exists
	rec = shelf.ReadRecord(shardID, "foo_0", 100, tr, schema, true)
	require.True(t, rec == nil || rec.RowNums() == 0)
}

func TestAllocTSSPConvertSource(t *testing.T) {
	cpuNum := 16
	defer initConfig(cpuNum)()

	var releases []func()
	for i := range 10 {
		ok, release := shelf.AllocTSSPConvertSource()
		require.Equal(t, i < cpuNum/4, ok)
		if ok {
			releases = append(releases, release)
		}
	}

	for _, release := range releases {
		release()
	}
}

func TestBlobGroup(t *testing.T) {
	bg, release := shelf.NewBlobGroup(1)
	defer release()

	bg.GroupingRow("mst", []byte{0, 0}, buildRecord(1, 1), 0)
	ok := false
	bg.Walk(func(blob *shelf.Blob) {
		ok = true
	})
	require.True(t, ok)
}

func newNilRunner() *shelf.Runner {
	return nil
}

func newRunner(shardID uint64, dir string) *shelf.Runner {
	lock := ""
	config.GetStoreConfig().WALDir = dir

	ident := &meta.ShardIdentifier{
		Policy:  "default",
		OwnerDb: "db0",
		OwnerPt: 1,
	}
	shardInfo := shelf.NewShardInfo(ident, dir, &lock, &MockTbStore{}, &EmptyIndex{sidCache: 10, sidCreate: 100})

	runner := shelf.NewRunner()
	runner.RegisterShard(shardID, shardInfo)
	return runner
}

func newShard(shardID uint64, dir string) (*shelf.Shard, *EmptyIndex, *MockTbStore) {
	lock := ""
	config.GetStoreConfig().WALDir = dir
	ident := &meta.ShardIdentifier{
		ShardID: shardID,
		Policy:  "default",
		OwnerDb: "db0",
		OwnerPt: 1,
	}
	store := &MockTbStore{}
	idx := &EmptyIndex{sidCache: 10, sidCreate: 100}
	shardInfo := shelf.NewShardInfo(ident, dir, &lock, store, idx)

	return shelf.NewShard(0, shardInfo, 1), idx, store
}

func writeRows(runner *shelf.Runner, shardID uint64) error {
	group := buildRowGroups()

	return runner.ScheduleGroup(shardID, group)
}

func buildRowGroups() *shelf.BlobGroup {
	group, _ := shelf.NewBlobGroup(shelf.NewRunner().Size())

	now := time.Now().UnixNano()
	now -= now % 1e9
	for i := 0; i < 100; i++ {
		row := buildRow(now+int64(1000-i), fmt.Sprintf("foo_%d", i%5), i)
		rec := &record.Record{}
		_ = record.AppendRowToRecord(rec, row)
		group.GroupingRow(row.Name, row.IndexKey, rec, 0)
	}

	return group
}

func buildRow(ts int64, name string, i int) *influx.Row {
	row := &influx.Row{
		Timestamp: ts,
		Name:      name,
		Tags: []influx.Tag{
			{
				Key:   "tag1",
				Value: fmt.Sprintf("value%d", i),
			},
			{
				Key:   "tag2",
				Value: fmt.Sprintf("value%d", i),
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      "field_float",
				Type:     influx.Field_Type_Float,
				NumValue: float64(i),
			},
			influx.Field{
				Key:      "field_int",
				Type:     influx.Field_Type_Int,
				NumValue: float64(i),
			},
			influx.Field{
				Key:      "field_string",
				Type:     influx.Field_Type_String,
				StrValue: fmt.Sprintf("value%d", i),
			},
		},
	}
	row.UnmarshalIndexKeys(nil)
	return row
}

func buildRecord(n int, value int) *record.Record {
	var types = []int{influx.Field_Type_String, influx.Field_Type_Int, influx.Field_Type_Float}
	rec := &record.Record{}
	rec.ReserveSchemaAndColVal(n + 1)
	for i := range n {
		schema := &rec.Schema[i]
		schema.Name = fmt.Sprintf("foo_%d", i)
		schema.Type = types[i%len(types)]

		col := &rec.ColVals[i]
		switch schema.Type {
		case influx.Field_Type_String:
			col.AppendString("foo_00000001")
		case influx.Field_Type_Int:
			col.AppendInteger(int64(value * 1234))
		case influx.Field_Type_Float:
			col.AppendFloat(float64(value) * 77.23)
		}
	}

	rec.Schema[n].Name = "time"
	rec.Schema[n].Type = influx.Field_Type_Int
	rec.TimeColumn().AppendInteger(int64(value * 100007))
	sort.Sort(rec)
	return rec
}

type EmptyIndex struct {
	sidCache  uint64
	sidCreate uint64
}

func (i *EmptyIndex) GetSeriesIdBySeriesKeyFromCache([]byte) (uint64, error) {
	return i.sidCache, nil
}

func (i *EmptyIndex) CreateIndexIfNotExistsBySeries([]byte, []byte, influx.PointTags) (uint64, error) {
	return i.sidCreate, nil
}

type MockTbStore struct {
	immutable.TablesStore

	files []immutable.TSSPFile
	seq   uint64
}

func (s *MockTbStore) Sequencer() *immutable.Sequencer {
	return immutable.NewSequencer()
}

func (s *MockTbStore) GetTableFileNum(string, bool) int {
	return 0
}

func (s *MockTbStore) AddBothTSSPFiles(flushed *bool, name string, orderFiles []immutable.TSSPFile, unorderFiles []immutable.TSSPFile) {
	s.files = append(s.files, orderFiles...)
	s.files = append(s.files, unorderFiles...)
	return
}

func (s *MockTbStore) NextSequence() uint64 {
	s.seq++
	return s.seq
}

func (s *MockTbStore) GetObsOption() *obs.ObsOptions {
	return &obs.ObsOptions{}
}

func (s *MockTbStore) GetShardID() uint64 {
	return 10
}

func itrTSSPFile(f immutable.TSSPFile, hook func(sid uint64, rec *record.Record)) {
	fi := immutable.NewFileIterator(f, immutable.CLog)
	itr := immutable.NewChunkIterator(fi)

	for {
		if !itr.Next() {
			break
		}

		sid := itr.GetSeriesID()
		if sid == 0 {
			panic("series ID is zero")
		}
		rec := itr.GetRecord()
		record.CheckRecord(rec)

		hook(sid, rec)
	}
}
func TestIsUniqueSorted(t *testing.T) {

	var list []int
	list = append(list, 1, 2, 3, 4)
	sorted := shelf.IsUniqueSorted(list)
	require.Equal(t, true, sorted)

	list = append(list, 3, 2, 1)
	sorted = shelf.IsUniqueSorted(list)
	require.Equal(t, false, sorted)

}
