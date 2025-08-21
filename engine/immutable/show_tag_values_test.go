// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/openGemini/openGemini/engine/clearevent"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	stats "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestTagValuesIteratorHandler_NextChunkMeta(t *testing.T) {
	cm := buildChunkMeta()

	testErr := errors.New("test err")
	for _, testcase := range []struct {
		Name      string
		Idx       *MockIndexMergeSet
		Expr      influxql.Expr
		TimeRange *util.TimeRange
		callback  func([]byte)

		err error
	}{
		{
			Name: "normal",
			Idx: &MockIndexMergeSet{
				func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
					return nil
				},
				func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesBytes)) error {
					return nil
				},
				func(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
					return [][]byte{}, nil
				},
				func(tsid uint64) bool {
					return false
				},
			},
			Expr: &influxql.VarRef{},
			TimeRange: &util.TimeRange{
				Min: 0,
				Max: 100,
			},

			err: nil,
		},
		{
			Name: "not overlaps",
			Idx: &MockIndexMergeSet{
				func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
					return nil
				},
				func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesBytes)) error {
					return nil
				},
				func(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
					return [][]byte{}, nil
				},
				func(tsid uint64) bool {
					return false
				},
			},
			Expr: &influxql.VarRef{},
			TimeRange: &util.TimeRange{
				Min: 99,
				Max: 199,
			},

			err: nil,
		},
		{
			Name: "GetSeries error",
			Idx: &MockIndexMergeSet{
				func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
					return testErr
				},
				func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesBytes)) error {
					return nil
				},
				func(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
					return [][]byte{}, nil
				},
				func(tsid uint64) bool {
					return false
				},
			},
			Expr: &influxql.VarRef{},
			TimeRange: &util.TimeRange{
				Min: 0,
				Max: 100,
			},

			err: testErr,
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			handler := NewTagValuesIteratorHandler(testcase.Idx, testcase.Expr, testcase.TimeRange, 0)
			err := handler.NextChunkMeta(cm)
			if !errors.Is(err, testcase.err) {
				t.Fatalf("TagValuesIteratorHandler.NextChunkMeta error: %v, expected: %v", err, testcase.err)
			}
		})
	}
}

func TestTagValuesIteratorHandler_Limited(t *testing.T) {
	for _, testcase := range []struct {
		Name  string
		Limit int
		Sets  TagSets

		Want bool
	}{
		{
			Name:  "Limited true 1",
			Limit: 10,
			Sets:  TagSets{totalCount: 10},
		},
		{
			Name:  "Limited true 2",
			Limit: 10,
			Sets:  TagSets{totalCount: 13},
		},
		{
			Name:  "Limited false 1",
			Limit: 0,
			Sets:  TagSets{totalCount: 10},
		},
		{
			Name:  "Limited false 2",
			Limit: 10,
			Sets:  TagSets{totalCount: 7},
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			handler := NewTagValuesIteratorHandler(&MockIndexMergeSet{}, &influxql.VarRef{}, &util.TimeRange{}, 0)
			got := handler.Limited()
			if got != testcase.Want {
				t.Fatalf("Limited is not equal, actual: %v, expected: %v", got, testcase.Want)
			}
		})
	}
}

func TestTagValuesIteratorHandler_Init(t *testing.T) {
	handler := NewTagValuesIteratorHandler(&MockIndexMergeSet{}, &influxql.VarRef{}, &util.TimeRange{}, 0)
	tagSets := &TagSets{}
	keys := make([][]byte, 0)
	err := handler.Init(map[string]interface{}{
		InitParamKeyDst:         tagSets,
		InitParamKeyKeys:        keys,
		InitParamKeyMeasurement: "test_measurement",
	})
	if err != nil {
		t.Fatalf("TagValuesIteratorHandler Init failed, error: %v", err)
	}
}

func TestShowTagValuesPlan_Execute(t *testing.T) {
	idx := &MockIndexMergeSet{}
	mockStore := &MockTableStore{
		GetBothFilesRefFn: func(measurement string, hasTimeFilter bool, tr util.TimeRange, flushed *bool) ([]TSSPFile, []TSSPFile, bool) {
			return []TSSPFile{MocTsspFile{}}, []TSSPFile{MocTsspFile{}}, false
		},
	}
	openShardFailErr := errors.New("openShardFailErr")
	var IteratorRunFlag bool

	for _, testcase := range []struct {
		Name    string
		Dst     map[string]TagSets
		Itr     SequenceIterator
		Handler SequenceIteratorHandler
		Sh      EngineShard
		Err     error
	}{
		{
			Name: "normal",
			Dst:  map[string]TagSets{},
			Itr: &MockSequenceIterator{
				RunFn:      func() error { return nil },
				AddFilesFn: func(files []TSSPFile) {},
				ReleaseFn:  func() {},
			},
			Handler: &MockSequenceIteratorHandler{
				InitFn: func(m map[string]interface{}) error { return nil },
			},
			Sh:  &MockEngineShard{IsOpenedFn: func() bool { return true }},
			Err: nil,
		},
		{
			Name: "reach limit all 1",
			Dst:  map[string]TagSets{},
			Itr: &MockSequenceIterator{
				RunFn:      func() error { return io.EOF },
				AddFilesFn: func(files []TSSPFile) {},
				ReleaseFn:  func() {},
			},
			Handler: &MockSequenceIteratorHandler{
				InitFn: func(m map[string]interface{}) error { return nil },
			},
			Sh:  &MockEngineShard{IsOpenedFn: func() bool { return true }},
			Err: io.EOF,
		},
		{
			Name: "reach limit all 2",
			Dst: map[string]TagSets{
				"mst1": {totalCount: 3},
			},
			Itr: &MockSequenceIterator{
				RunFn:      func() error { return io.EOF },
				AddFilesFn: func(files []TSSPFile) {},
				ReleaseFn:  func() {},
			},
			Handler: &MockSequenceIteratorHandler{
				InitFn: func(m map[string]interface{}) error { return nil },
			},
			Sh:  &MockEngineShard{IsOpenedFn: func() bool { return true }},
			Err: io.EOF,
		},
		{
			Name: "reach limit part 1",
			Dst:  map[string]TagSets{},
			Itr: &MockSequenceIterator{
				RunFn: func() error {
					if IteratorRunFlag {
						return nil
					} else {
						IteratorRunFlag = true
						return io.EOF
					}
				},
				AddFilesFn: func(files []TSSPFile) {},
				ReleaseFn:  func() {},
			},
			Handler: &MockSequenceIteratorHandler{
				InitFn: func(m map[string]interface{}) error { return nil },
			},
			Sh:  &MockEngineShard{IsOpenedFn: func() bool { return true }},
			Err: nil,
		},
		{
			Name: "reach limit part 2",
			Dst: map[string]TagSets{
				"mst1": {totalCount: 3},
			},
			Itr: &MockSequenceIterator{
				RunFn: func() error {
					if IteratorRunFlag {
						return nil
					} else {
						IteratorRunFlag = true
						return io.EOF
					}
				},
				AddFilesFn: func(files []TSSPFile) {},
				ReleaseFn:  func() {},
			},
			Handler: &MockSequenceIteratorHandler{
				InitFn: func(m map[string]interface{}) error { return nil },
			},
			Sh:  &MockEngineShard{IsOpenedFn: func() bool { return true }},
			Err: nil,
		},
		{
			Name: "normal and open shard",
			Dst:  map[string]TagSets{},
			Itr: &MockSequenceIterator{
				RunFn:      func() error { return nil },
				AddFilesFn: func(files []TSSPFile) {},
				ReleaseFn:  func() {},
			},
			Handler: &MockSequenceIteratorHandler{
				InitFn: func(m map[string]interface{}) error { return nil },
			},
			Sh: &MockEngineShard{
				IsOpenedFn:      func() bool { return false },
				OpenAndEnableFn: func(client metaclient.MetaClient) error { return nil },
				GetDataPathFn:   func() string { return "" },
				GetIdentFn:      func() *meta.ShardIdentifier { return &meta.ShardIdentifier{} },
			},
			Err: nil,
		},
		{
			Name: "normal and open shard failed",
			Dst:  map[string]TagSets{},
			Itr: &MockSequenceIterator{
				RunFn:      func() error { return nil },
				AddFilesFn: func(files []TSSPFile) {},
				ReleaseFn:  func() {},
			},
			Handler: &MockSequenceIteratorHandler{
				InitFn: func(m map[string]interface{}) error { return nil },
			},
			Sh: &MockEngineShard{
				IsOpenedFn:      func() bool { return false },
				OpenAndEnableFn: func(client metaclient.MetaClient) error { return openShardFailErr },
				GetDataPathFn:   func() string { return "" },
				GetIdentFn:      func() *meta.ShardIdentifier { return &meta.ShardIdentifier{} },
			},
			Err: openShardFailErr,
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			plan := &ddlBasePlan{
				table:  mockStore,
				idx:    idx,
				shard:  testcase.Sh,
				logger: Logger.NewLogger(errno.ModuleUnknown),
			}
			plan.handler = testcase.Handler
			plan.itr = testcase.Itr
			plan.fn = NewTagSets
			plan.fv = NewTagValuesIteratorHandler

			dst := make(map[string]DDLRespData)
			tagKeys := map[string][][]byte{
				"mst1": [][]byte{[]byte("tagKey")},
				"mst2": [][]byte{[]byte("tagKey")},
				"mst3": [][]byte{[]byte("tagKey")},
			}
			timeRange := util.TimeRange{Min: 100, Max: 200}
			err := plan.Execute(dst, tagKeys, &influxql.VarRef{}, timeRange, 3)
			if !errors.Is(err, testcase.Err) {
				t.Fatalf("ShowTagValuesPlan Execute failed, error: %v, want: %v", err, testcase.Err)
			}
		})
	}
}

type MockEngineShard struct {
	IsOpenedFn      func() bool
	OpenAndEnableFn func(client metaclient.MetaClient) error
	GetDataPathFn   func() string
	GetIdentFn      func() *meta.ShardIdentifier
}

func (s *MockEngineShard) IsOpened() bool {
	return s.IsOpenedFn()
}

func (s *MockEngineShard) OpenAndEnable(client metaclient.MetaClient) error {
	return s.OpenAndEnableFn(client)
}

func (s *MockEngineShard) GetDataPath() string {
	return s.GetDataPathFn()
}

func (s *MockEngineShard) GetIdent() *meta.ShardIdentifier {
	return s.GetIdentFn()
}

type MockIndexMergeSet struct {
	GetSeriesFn        func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error
	GetSeriesBytesFn   func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesBytes)) error
	SearchSeriesKeysFn func(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error)
	HasDeletedTSIDFn   func(tsid uint64) bool
}

func (idx *MockIndexMergeSet) HasDeletedTSID(tsid uint64) bool {
	if tsid == 1 {
		return true
	}
	return false
}

func (idx *MockIndexMergeSet) GetSeries(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
	return idx.GetSeriesFn(sid, buf, condition, callback)
}

func (idx *MockIndexMergeSet) GetSeriesBytes(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesBytes)) error {
	return idx.GetSeriesBytesFn(sid, buf, condition, callback)
}

func (idx *MockIndexMergeSet) SearchSeriesKeys(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
	return idx.SearchSeriesKeysFn(series, name, condition)
}

type MockSequenceIterator struct {
	SetChunkMetasReaderFn func(reader SequenceIteratorChunkMetaReader)
	ReleaseFn             func()
	AddFilesFn            func(files []TSSPFile)
	StopFn                func()
	RunFn                 func() error
	BufferFn              func() *pool.Buffer
}

func (itr *MockSequenceIterator) SetChunkMetasReader(reader SequenceIteratorChunkMetaReader) {
	itr.SetChunkMetasReaderFn(reader)
}
func (itr *MockSequenceIterator) Release() {
	itr.ReleaseFn()
}
func (itr *MockSequenceIterator) AddFiles(files []TSSPFile) {
	itr.AddFilesFn(files)
}
func (itr *MockSequenceIterator) Stop() {
	itr.StopFn()
}
func (itr *MockSequenceIterator) Run() error {
	return itr.RunFn()
}
func (itr *MockSequenceIterator) Buffer() *pool.Buffer {
	return itr.BufferFn()
}

type MockTableStore struct {
	TablesStore
	SetOpIdFn                func(shardId uint64, opId uint64)
	OpenFn                   func() (int64, error)
	CloseFn                  func() error
	AddTableFn               func(ms *MsBuilder, isOrder bool, tmp bool)
	AddTSSPFilesFn           func(name string, isOrder bool, f ...TSSPFile)
	AddBothTSSPFilesFn       func(flushed *bool, name string, orderFiles []TSSPFile, unorderFiles []TSSPFile)
	AddPKFileFn              func(name, file string, rec *record.Record, mark fragment.IndexFragment, tcLocation int8)
	GetPKFileFn              func(mstName string, file string) (pkInfo *colstore.PKInfo, ok bool)
	FreeAllMemReaderFn       func()
	ReplaceFilesFn           func(name string, oldFiles, newFiles []TSSPFile, isOrder bool) error
	GetBothFilesRefFn        func(measurement string, hasTimeFilter bool, tr util.TimeRange, flushed *bool) ([]TSSPFile, []TSSPFile, bool)
	ReplaceDownSampleFilesFn func(mstNames []string, originFiles [][]TSSPFile, newFiles [][]TSSPFile, isOrder bool, callBack func()) error
	NextSequenceFn           func() uint64
	SequencerFn              func() *Sequencer
	GetTSSPFilesFn           func(mm string, isOrder bool) (*TSSPFiles, bool)
	GetCSFilesFn             func(mm string) (*TSSPFiles, bool)
	CopyCSFilesFn            func(mm string) []TSSPFile
	TierFn                   func() uint64
	SetTierFn                func(tier uint64)
	FileFn                   func(name string, namePath string, isOrder bool) TSSPFile
	CompactDoneFn            func(seq []string)
	CompactionEnableFn       func()
	CompactionDisableFn      func()
	MergeEnableFn            func()
	MergeDisableFn           func()
	CompactionEnabledFn      func() bool
	MergeEnabledFn           func() bool
	IsOutOfOrderFilesExistFn func() bool
	MergeOutOfOrderFn        func(shId uint64, full bool, force bool) error
	LevelCompactFn           func(level uint16, shid uint64) error
	FullCompactFn            func(shid uint64) error
	SetAddFuncFn             func(addFunc func(int64))
	GetLastFlushTimeBySidFn  func(measurement string, sid uint64) int64
	GetRowCountsBySidFn      func(measurement string, sid uint64) (int64, error)
	AddRowCountsBySidFn      func(measurement string, sid uint64, rowCounts int64)
	GetOutOfOrderFileNumFn   func() int
	GetMstFileStatFn         func() *stats.FileStat
	DropMeasurementFn        func(ctx context.Context, name string) error
	GetFileSeqFn             func() uint64
	DisableCompAndMergeFn    func()
	EnableCompAndMergeFn     func()
	FreeSequencerFn          func() bool
	SetImmTableTypeFn        func(engineType config.EngineType)
	SetMstInfoFn             func(name string, mstInfo *meta.MeasurementInfo)
	SetAccumulateMetaIndexFn func(name string, aMetaIndex *AccumulateMetaIndex)
	GetMstInfoFn             func(name string) (*meta.MeasurementInfo, bool)
	SeriesTotalFn            func() uint64
	SetLockPathFn            func(lock *string)
	FullyCompactedFn         func() bool
	SetObsOptionFn           func(option *obs.ObsOptions)
	GetObsOptionFn           func() *obs.ObsOptions
	GetShardIDFn             func() uint64
	SetIndexMergeSetFn       func(idx IndexMergeSet)
}

func (s *MockTableStore) SetOpId(shardId uint64, opId uint64) {
	s.SetOpIdFn(shardId, opId)
}
func (s *MockTableStore) Open(clearevent.NoClearShardAndIndexSupplier) (int64, error) {
	return s.OpenFn()
}
func (s *MockTableStore) Close() error {
	return s.CloseFn()
}
func (s *MockTableStore) AddTable(ms *MsBuilder, isOrder bool, tmp bool) {
	s.AddTableFn(ms, isOrder, tmp)
}
func (s *MockTableStore) AddTSSPFiles(name string, isOrder bool, f ...TSSPFile) {
	s.AddTSSPFilesFn(name, isOrder, f...)
}
func (s *MockTableStore) AddBothTSSPFiles(flushed *bool, name string, orderFiles []TSSPFile, unorderFiles []TSSPFile) {
	s.AddBothTSSPFilesFn(flushed, name, orderFiles, unorderFiles)
}
func (s *MockTableStore) AddPKFile(name, file string, rec *record.Record, mark fragment.IndexFragment, tcLocation int8) {
	s.AddPKFileFn(name, file, rec, mark, tcLocation)
}
func (s *MockTableStore) GetPKFile(mstName string, file string) (pkInfo *colstore.PKInfo, ok bool) {
	return s.GetPKFileFn(mstName, file)
}
func (s *MockTableStore) FreeAllMemReader() {
	s.FreeAllMemReaderFn()
}
func (s *MockTableStore) ReplaceFiles(name string, oldFiles, newFiles []TSSPFile, isOrder bool) error {
	return s.ReplaceFilesFn(name, oldFiles, newFiles, isOrder)
}
func (s *MockTableStore) GetBothFilesRef(measurement string, hasTimeFilter bool, tr util.TimeRange, flushed *bool) ([]TSSPFile, []TSSPFile, bool) {
	return s.GetBothFilesRefFn(measurement, hasTimeFilter, tr, flushed)
}
func (s *MockTableStore) ReplaceDownSampleFiles(mstNames []string, originFiles [][]TSSPFile, newFiles [][]TSSPFile, isOrder bool, callBack func()) error {
	return s.ReplaceDownSampleFilesFn(mstNames, originFiles, newFiles, isOrder, callBack)
}
func (s *MockTableStore) NextSequence() uint64 {
	return s.NextSequenceFn()
}
func (s *MockTableStore) Sequencer() *Sequencer {
	return s.SequencerFn()
}
func (s *MockTableStore) GetTSSPFiles(mm string, isOrder bool) (*TSSPFiles, bool) {
	return s.GetTSSPFilesFn(mm, isOrder)
}
func (s *MockTableStore) GetCSFiles(mm string) (*TSSPFiles, bool) {
	return s.GetCSFilesFn(mm)
}
func (s *MockTableStore) CopyCSFiles(mm string) []TSSPFile {
	return s.CopyCSFilesFn(mm)
}
func (s *MockTableStore) Tier() uint64 {
	return s.TierFn()
}
func (s *MockTableStore) SetTier(tier uint64) {
	s.SetTierFn(tier)
}
func (s *MockTableStore) File(name string, namePath string, isOrder bool) TSSPFile {
	return s.FileFn(name, namePath, isOrder)
}
func (s *MockTableStore) CompactDone(seq []string) {
	s.CompactDoneFn(seq)
}
func (s *MockTableStore) CompactionEnable() {
	s.CompactionEnableFn()
}
func (s *MockTableStore) CompactionDisable() {
	s.CompactionDisableFn()
}
func (s *MockTableStore) MergeEnable() {
	s.MergeEnableFn()
}
func (s *MockTableStore) MergeDisable() {
	s.MergeDisableFn()
}
func (s *MockTableStore) CompactionEnabled() bool {
	return s.CompactionEnabledFn()
}
func (s *MockTableStore) MergeEnabled() bool {
	return s.MergeEnabledFn()
}
func (s *MockTableStore) IsOutOfOrderFilesExist() bool {
	return s.IsOutOfOrderFilesExistFn()
}
func (s *MockTableStore) MergeOutOfOrder(shId uint64, full bool, force bool) error {
	return s.MergeOutOfOrderFn(shId, full, force)
}
func (s *MockTableStore) LevelCompact(level uint16, shid uint64) error {
	return s.LevelCompactFn(level, shid)
}
func (s *MockTableStore) FullCompact(shid uint64) error {
	return s.FullCompactFn(shid)
}
func (s *MockTableStore) SetAddFunc(addFunc func(int64)) {
	s.SetAddFuncFn(addFunc)
}
func (s *MockTableStore) GetLastFlushTimeBySid(measurement string, sid uint64) int64 {
	return s.GetLastFlushTimeBySidFn(measurement, sid)
}
func (s *MockTableStore) GetRowCountsBySid(measurement string, sid uint64) (int64, error) {
	return s.GetRowCountsBySidFn(measurement, sid)
}
func (s *MockTableStore) AddRowCountsBySid(measurement string, sid uint64, rowCounts int64) {
	s.AddRowCountsBySidFn(measurement, sid, rowCounts)
}
func (s *MockTableStore) GetOutOfOrderFileNum() int {
	return s.GetOutOfOrderFileNumFn()
}
func (s *MockTableStore) GetMstFileStat() *stats.FileStat {
	return s.GetMstFileStatFn()
}
func (s *MockTableStore) DropMeasurement(ctx context.Context, name string) error {
	return s.DropMeasurementFn(ctx, name)
}
func (s *MockTableStore) GetFileSeq() uint64 {
	return s.GetFileSeqFn()
}
func (s *MockTableStore) DisableCompAndMerge() {
	s.DisableCompAndMergeFn()
}
func (s *MockTableStore) EnableCompAndMerge() {
	s.EnableCompAndMergeFn()
}
func (s *MockTableStore) FreeSequencer() bool {
	return s.FreeSequencerFn()
}
func (s *MockTableStore) SetImmTableType(engineType config.EngineType) {
	s.SetImmTableTypeFn(engineType)
}
func (s *MockTableStore) SetMstInfo(name string, mstInfo *meta.MeasurementInfo) {
	s.SetMstInfoFn(name, mstInfo)
}
func (s *MockTableStore) SetAccumulateMetaIndex(name string, aMetaIndex *AccumulateMetaIndex) {
	s.SetAccumulateMetaIndexFn(name, aMetaIndex)
}
func (s *MockTableStore) GetMstInfo(name string) (*meta.MeasurementInfo, bool) {
	return s.GetMstInfoFn(name)
}
func (s *MockTableStore) SeriesTotal() uint64 {
	return s.SeriesTotalFn()
}
func (s *MockTableStore) SetLockPath(lock *string) {
	s.SetLockPathFn(lock)
}
func (s *MockTableStore) FullyCompacted() bool {
	return s.FullyCompactedFn()
}

func (s *MockTableStore) SetObsOption(option *obs.ObsOptions) {
	s.SetObsOptionFn(option)
}
func (s *MockTableStore) GetObsOption() *obs.ObsOptions {
	return s.GetObsOptionFn()
}
func (s *MockTableStore) GetShardID() uint64 {
	return s.GetShardIDFn()
}

func (s *MockTableStore) SetIndexMergeSet(idx IndexMergeSet) {
	s.SetIndexMergeSetFn(idx)
}
