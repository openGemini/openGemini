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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestSeriesKeyIteratorHandler_NextChunkMeta(t *testing.T) {
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
					return nil
				},
				func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesBytes)) error {
					return testErr
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
			handler := NewSeriesKeysIteratorHandler(testcase.Idx, testcase.Expr, testcase.TimeRange, 0)
			err := handler.NextChunkMeta(cm)
			if !errors.Is(err, testcase.err) {
				t.Fatalf("SeriesKeyIteratorHandler.NextChunkMeta error: %v, expected: %v", err, testcase.err)
			}
		})
	}
}

func TestSeriesKeyIteratorHandler_Limited(t *testing.T) {
	for _, testcase := range []struct {
		Name  string
		Limit int
		Sets  SeriesKeys

		Want bool
	}{
		{
			Name:  "Limited true 1",
			Limit: 10,
			Sets:  SeriesKeys{totalCount: 10},
		},
		{
			Name:  "Limited true 2",
			Limit: 10,
			Sets:  SeriesKeys{totalCount: 13},
		},
		{
			Name:  "Limited false 1",
			Limit: 0,
			Sets:  SeriesKeys{totalCount: 10},
		},
		{
			Name:  "Limited false 2",
			Limit: 10,
			Sets:  SeriesKeys{totalCount: 7},
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			handler := NewSeriesKeysIteratorHandler(&MockIndexMergeSet{}, &influxql.VarRef{}, &util.TimeRange{}, 0)
			got := handler.Limited()
			if got != testcase.Want {
				t.Fatalf("Limited is not equal, actual: %v, expected: %v", got, testcase.Want)
			}
		})
	}
}

func TestSeriesKeyIteratorHandler_Init(t *testing.T) {
	handler := NewSeriesKeysIteratorHandler(&MockIndexMergeSet{}, &influxql.VarRef{}, &util.TimeRange{}, 0)
	seriesKeys := &SeriesKeys{}
	err := handler.Init(map[string]interface{}{
		InitParamKeyDst:         seriesKeys,
		InitParamKeyMeasurement: "test_measurement",
	})
	if err != nil {
		t.Fatalf("SeriesKeysIteratorHandler Init failed, error: %v", err)
	}

	err = handler.Init(map[string]interface{}{
		InitParamKeyMeasurement: "test_measurement",
	})
	require.True(t, strings.Contains(err.Error(), fmt.Sprintf("SeriesKeysIteratorHandler init param invalid")))

	err = handler.Init(map[string]interface{}{
		InitParamKeyDst:         &TagSets{},
		InitParamKeyMeasurement: "test_measurement",
	})
	require.True(t, strings.Contains(err.Error(), fmt.Sprintf("SeriesKeysIteratorHandler init param invalid")))

	err = handler.Init(map[string]interface{}{
		InitParamKeyDst: seriesKeys,
	})
	require.True(t, strings.Contains(err.Error(), fmt.Sprintf("SeriesKeysIteratorHandler init param invalid")))

	err = handler.Init(map[string]interface{}{
		InitParamKeyDst:         seriesKeys,
		InitParamKeyMeasurement: 1,
	})
	require.True(t, strings.Contains(err.Error(), fmt.Sprintf("SeriesKeysIteratorHandler init param invalid")))
}

func TestSeriesKeyPlan_Execute(t *testing.T) {
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
		Dst     map[string]SeriesKeys
		Itr     SequenceIterator
		Handler SequenceIteratorHandler
		Sh      EngineShard
		Err     error
	}{
		{
			Name: "normal",
			Dst:  map[string]SeriesKeys{},
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
			Dst:  map[string]SeriesKeys{},
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
			Dst: map[string]SeriesKeys{
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
			Dst:  map[string]SeriesKeys{},
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
			Dst: map[string]SeriesKeys{
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
			Dst:  map[string]SeriesKeys{},
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
			Dst:  map[string]SeriesKeys{},
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
			plan.fn = NewSeriesKeys
			plan.fv = NewSeriesKeysIteratorHandler

			dst := make(map[string]DDLRespData)
			tagKeys := map[string][][]byte{
				"mst1": [][]byte{},
				"mst2": [][]byte{},
				"mst3": [][]byte{},
			}
			timeRange := util.TimeRange{Min: 100, Max: 200}
			err := plan.Execute(dst, tagKeys, &influxql.VarRef{}, timeRange, 3)
			if !errors.Is(err, testcase.Err) {
				t.Fatalf("ShowSeriesPlan Execute failed, error: %v, want: %v", err, testcase.Err)
			}
		})
	}
}
