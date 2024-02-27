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

package tsi

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
	assert1 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.SeriesParallelismRes, 0, 0)
}

func getTestIndexAndBuilder(path string, engineType config.EngineType) (Index, *IndexBuilder) {
	ltime := uint64(time.Now().Unix())
	lockPath := ""
	indexIdent := &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 2,
		IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}
	opts := new(Options).
		Path(path).
		Ident(indexIdent).
		IndexType(index.MergeSet).
		EngineType(engineType).
		StartTime(time.Now()).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)

	indexBuilder := NewIndexBuilder(opts)

	primaryIndex, err := NewIndex(opts)
	if err != nil {
		panic(err)
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	indexRelation, err := NewIndexRelation(opts, primaryIndex, indexBuilder)
	if err != nil {
		panic(err)
	}
	indexBuilder.Relations[uint32(index.MergeSet)] = indexRelation

	config.NewLogger("test")
	err = indexBuilder.Open()
	if err != nil {
		panic(err)
	}

	return primaryIndex, indexBuilder
}

func getTextIndexAndBuilder(path string) (Index, *IndexBuilder) {
	ltime := uint64(time.Now().Unix())
	lockPath := ""
	indexIdent := &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 2,
		IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}
	opts := new(Options).
		Path(path).
		Ident(indexIdent).
		IndexType(index.Text).
		StartTime(time.Now()).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)

	indexBuilder := NewIndexBuilder(opts)

	opt := new(Options).
		Path(path).
		IndexType(index.MergeSet).
		EngineType(config.TSSTORE).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)

	primaryIndex, err := NewIndex(opt)
	if err != nil {
		panic(err)
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	textIndexRelation, err := NewIndexRelation(opts, primaryIndex, indexBuilder)
	invertedIndexRelation, err := NewIndexRelation(opt, primaryIndex, indexBuilder)

	indexBuilder.Relations[uint32(index.Text)] = textIndexRelation
	indexBuilder.Relations[uint32(index.MergeSet)] = invertedIndexRelation

	err = indexBuilder.Open()

	return primaryIndex, indexBuilder
}

func getFieldIndexAndBuilder(path string) (Index, *IndexBuilder) {
	lockPath := ""
	indexIdent := &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 3, IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}
	ltime := uint64(time.Now().Unix())
	opts := new(Options).
		Path(path).
		IndexType(index.Field).
		StartTime(time.Now()).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		Ident(indexIdent).
		Lock(&lockPath).
		LogicalClock(1).
		SequenceId(&ltime)

	indexBuilder := NewIndexBuilder(opts)

	indexIdent = &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 1, IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}
	opt := new(Options).
		Path(path).
		IndexType(index.MergeSet).
		EngineType(config.TSSTORE).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		Ident(indexIdent).
		Lock(&lockPath)
	primaryIndex, err := NewIndex(opt)
	if err != nil {
		panic(err)
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	fieldIndexRelation, err := NewIndexRelation(opts, primaryIndex, indexBuilder)
	if err != nil {
		panic(err)
	}
	invertedIndexRelation, err := NewIndexRelation(opt, primaryIndex, indexBuilder)
	if err != nil {
		panic(err)
	}

	indexBuilder.Relations[uint32(index.Field)] = fieldIndexRelation
	indexBuilder.Relations[uint32(index.MergeSet)] = invertedIndexRelation

	err = indexBuilder.Open()
	if err != nil {
		panic(err)
	}

	return primaryIndex, indexBuilder
}

func TestSearchSeries_Relation(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name []byte, opts influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		dst := make([][]byte, 1)
		dst, err := idx.SearchSeries(dst[:0], name, opts, tr)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(dst, func(i, j int) bool {
			return string(dst[i]) < string(dst[j])
		})
		for i := 0; i < len(dst); i++ {
			assert.Equal(t, string(dst[i]), expectedSeriesKeys[i])
		}
		for _, key := range dst {
			influx.PutBytesBuffer(key)
		}
	}

	t.Run("NoCond", func(t *testing.T) {
		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("EQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1'`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
		})
	})

	t.Run("NEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1!='value1'`), defaultTR, []string{
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("EQEmpty", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1=''`), defaultTR, []string{})
	})

	t.Run("NEQEmpty", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1!=''`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("AND", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value11') AND (tk2='value22')`), defaultTR, []string{
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("OR", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value1') OR (tk3='value33')`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("RegEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1=~/val.*1/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk1=~/val.*11/`), defaultTR, []string{
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk1=~/(val.*e1|val.*11)/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("RegNEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1!~/val.*11/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
		})
	})

	t.Run("UnlimitedTR", func(t *testing.T) {
		f([]byte("mn-1"), nil, TimeRange{Min: math.MinInt64, Max: math.MaxInt64}, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("ExistFieldKey", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("RegNEQ2", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1!~/[val]/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})
}

func TestSearchSeriesKeys_Relation(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name []byte, opts influxql.Expr, expectedSeriesKeys map[string]struct{}) {
		dst := make([][]byte, 1)
		dst, err := idx.SearchSeriesKeys(dst[:0], name, opts)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, len(expectedSeriesKeys), len(dst))
		for i := 0; i < len(dst); i++ {
			_, ok := expectedSeriesKeys[string(dst[i])]
			assert.Equal(t, ok, true)
		}
	}

	t.Run("NoCond", func(t *testing.T) {
		f([]byte("mn-1"), nil, map[string]struct{}{
			"mn-1,tk1=value1,tk2=value2,tk3=value3":    {},
			"mn-1,tk1=value1,tk2=value22,tk3=value3":   {},
			"mn-1,tk1=value11,tk2=value2,tk3=value33":  {},
			"mn-1,tk1=value11,tk2=value22,tk3=value3":  {},
			"mn-1,tk1=value11,tk2=value22,tk3=value33": {},
		})
	})
}

func TestDropMeasurement_Relation(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name []byte, opts influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		dst := make([][]byte, 1)
		name = append(name, []byte("_0000")...)
		dst, err := idx.SearchSeries(dst[:0], name, opts, tr)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(dst, func(i, j int) bool {
			return string(dst[i]) < string(dst[j])
		})
		for i := 0; i < len(dst); i++ {
			assert.Equal(t, string(dst[i]), expectedSeriesKeys[i])
		}
		for _, key := range dst {
			influx.PutBytesBuffer(key)
		}
	}

	f([]byte("mn-1"), nil, defaultTR, []string{
		"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
		"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
		"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
		"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
		"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
	})

	t.Run("IndexReopenAndQuery", func(t *testing.T) {
		if err := idxBuilder.Close(); err != nil {
			t.Fatal(err)
		}
		if err := idxBuilder.Open(); err != nil {
			t.Fatal(err)
		}
		f([]byte("mn-1"), nil, defaultTR, nil)
	})

	t.Run("AddNewIndexAndQuery", func(t *testing.T) {
		CreateIndexByBuild(idxBuilder, idx)
		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})
}

func TestDeleteTSIDs_Relation(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name []byte, opts influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		dst := make([][]byte, 1)
		name = append(name, []byte("_0000")...)
		dst, err := idx.SearchSeries(dst[:0], name, opts, tr)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(dst, func(i, j int) bool {
			return string(dst[i]) < string(dst[j])
		})
		for i := 0; i < len(dst); i++ {
			assert.Equal(t, string(dst[i]), expectedSeriesKeys[i])
		}

		for _, key := range dst {
			influx.PutBytesBuffer(key)
		}
	}

	t.Run("NormalQuery", func(t *testing.T) {
		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("DeleteByEQCond", func(t *testing.T) {
		if err := idx.DeleteTSIDs([]byte("mn-1_0000"), MustParseExpr(`tk1='value1'`), defaultTR); err != nil {
			t.Fatal(err)
		}

		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})

		if err := idx.DeleteTSIDs([]byte("mn-1_0000"), MustParseExpr(`tk2='value2'`), defaultTR); err != nil {
			t.Fatal(err)
		}

		f([]byte("mn-1_0000"), nil, defaultTR, []string{
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("DeleteByBigTR", func(t *testing.T) {
		err := idx.DeleteTSIDs([]byte("mn-1_0000"), MustParseExpr(`tk2='value2'`), TimeRange{time.Now().Add(-41 * 24 * time.Hour).UnixNano(), time.Now().UnixNano()})
		assert.Equal(t, strings.Contains(err.Error(), "too much dates"), true)
	})

	t.Run("DeleteWithoutCond", func(t *testing.T) {
		if err := idx.DeleteTSIDs([]byte("mn-1_0000"), nil, defaultTR); err != nil {
			t.Fatal(err)
		}

		f([]byte("mn-1"), nil, defaultTR, nil)
	})
}

func TestSearchTagValues_Relation(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name []byte, tagKeys [][]byte, condition influxql.Expr, expectedTagValues [][]string) {
		//name = append(name, []byte("_0000")...)
		tagValues, err := idx.SearchTagValues(name, tagKeys, condition)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, len(expectedTagValues), len(tagValues))

		for i := 0; i < len(tagValues); i++ {
			require.Equal(t, len(expectedTagValues[i]), len(tagValues[i]))
			sort.Strings(tagValues[i])
			sort.Strings(expectedTagValues[i])
			for j := 0; j < len(tagValues[i]); j++ {
				assert.Equal(t, tagValues[i][j], expectedTagValues[i][j])
			}
		}
	}

	t.Run("SingleKeyWithoutCond", func(t *testing.T) {
		f([]byte("mn-1"), [][]byte{[]byte("tk1")}, nil, [][]string{{
			"value1",
			"value11",
		}})

		f([]byte("mn-1"), [][]byte{[]byte("tk2")}, nil, [][]string{{
			"value2",
			"value22",
		}})

		f([]byte("mn-1"), [][]byte{[]byte("tk3")}, nil, [][]string{{
			"value3",
			"value33",
		}})
	})

	t.Run("SingleKeyWithCond", func(t *testing.T) {
		f([]byte("mn-1"), [][]byte{[]byte("tk1")}, MustParseExpr(`tk3="value33"`), [][]string{{
			"value11",
		}})

		f([]byte("mn-1"), [][]byte{[]byte("tk2")}, MustParseExpr(`tk3="value33"`), [][]string{{
			"value2",
			"value22",
		}})

		f([]byte("mn-1"), [][]byte{[]byte("tk3")}, MustParseExpr(`tk1="value1"`), [][]string{{
			"value3",
		}})
	})

	t.Run("MultiKeysWithCond", func(t *testing.T) {
		f([]byte("mn-1"), [][]byte{[]byte("tk1"), []byte("tk2")}, MustParseExpr(`tk3="value33"`), [][]string{
			{
				"value11",
			},
			{
				"value2",
				"value22",
			},
		})

		f([]byte("mn-1"), [][]byte{[]byte("tk3"), []byte("tk2")}, MustParseExpr(`tk1="value1"`), [][]string{
			{
				"value3",
			},
			{
				"value2",
				"value22",
			},
		})
	})
}

func TestSeriesCardinality_Relation(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name []byte, condition influxql.Expr, expectCardinality uint64) {
		count, err := idx.SeriesCardinality(name, condition, defaultTR)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, count, expectCardinality)
	}

	t.Run("cardinality from measurement", func(t *testing.T) {
		f([]byte("mn-1"), nil, 5)
	})

	t.Run("cardinality with condition", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr("tk1=value1"), 2)
	})
}

func TestSearchTagValuesCardinality_Relation(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name, tagKey []byte, expectCardinality uint64) {
		count, err := idx.SearchTagValuesCardinality(name, tagKey)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, count, expectCardinality)
	}

	t.Run("NormalQuery", func(t *testing.T) {
		f([]byte("mn-1"), []byte("tk1"), 2)
		f([]byte("mn-1"), []byte("tk2"), 2)
		f([]byte("mn-1"), []byte("tk3"), 2)
	})

	t.Run("DeleteByEQ", func(t *testing.T) {
		if err := idx.DeleteTSIDs([]byte("mn-1"), MustParseExpr(`tk1='value1'`), defaultTR); err != nil {
			t.Fatal(err)
		}
		f([]byte("mn-1"), []byte("tk1"), 1)
	})
}

func TestWriteTextData(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getTextIndexAndBuilder(path)
	defer idxBuilder.Close()
	mmPoints := &dictpool.Dict{}
	if err := CreateIndexByRows(idxBuilder, mmPoints); err != nil {
		t.Fatal(err)
	}

	_, _, err := idxBuilder.Scan(nil, nil, &query.ProcessorOptions{}, resourceallocator.DefaultSeriesAllocateFunc)
	require.Nil(t, err)
}

func TestWriteFieldIndex(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getFieldIndexAndBuilder(path)

	SeriesKeys := []string{
		"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
	}

	opt := influx.IndexOption{
		IndexList: []uint16{0},
		Oid:       uint32(index.Field),
	}
	mmPoints := genRowsByOpt(opt, SeriesKeys)
	err := CreateIndexByRows(idxBuilder, mmPoints)
	if !assert1.NotNil(t, err) {
		t.Fatal()
	}

	opt.IndexList = []uint16{0, 3}
	mmPoints = genRowsByOpt(opt, SeriesKeys)
	err = CreateIndexByRows(idxBuilder, mmPoints)
	if !assert1.NotNil(t, err) {
		t.Fatal()
	}

	opt.IndexList = []uint16{3}
	mmPoints = genRowsByOpt(opt, SeriesKeys)
	err = CreateIndexByRows(idxBuilder, mmPoints)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	if err = idxBuilder.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFieldIndexSearch(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getFieldIndexAndBuilder(path)
	defer idxBuilder.Close()

	SeriesKeys := []string{
		"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
	}

	opt := influx.IndexOption{
		IndexList: []uint16{3},
		Oid:       uint32(index.Field),
	}
	mmPoints := genRowsByOpt(opt, SeriesKeys)
	if err := CreateIndexByRows(idxBuilder, mmPoints); err != nil {
		t.Fatal(err)
	}

	// No group by and without filter.
	nameWithVer := "mn-1_0000"
	result, _, err := idxBuilder.Scan(nil, []byte(nameWithVer), &query.ProcessorOptions{Name: nameWithVer}, resourceallocator.DefaultSeriesAllocateFunc)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	tagSets, ok := result.(GroupSeries)
	if !ok {
		t.Fatal()
	}
	if !assert1.Equal(t, 1, len(tagSets)) {
		t.Fatal()
	}
	if !assert1.Equal(t, 5, len(tagSets[0].SeriesKeys)) {
		t.Fatal()
	}

	sort.Strings(SeriesKeys)
	sort.Sort(tagSets[0])
	for i, key := range SeriesKeys {
		if !assert1.True(t, strings.Contains(string(tagSets[0].SeriesKeys[i]), key)) {
			t.Fatal(string(tagSets[0].SeriesKeys[i]), key)
		}
	}

	// No group by and with field filter.
	result, _, err = idxBuilder.Scan(nil, []byte(nameWithVer),
		&query.ProcessorOptions{
			Name:      "mn-1_0000",
			Condition: MustParseExpr(`field_str0='value-1'`)}, resourceallocator.DefaultSeriesAllocateFunc)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	tagSets, ok = result.(GroupSeries)
	if !ok {
		t.Fatal()
	}
	if !assert1.Equal(t, 1, len(tagSets)) {
		t.Fatal()
	}
	if !assert1.True(t, strings.Contains(string(tagSets[0].SeriesKeys[0]), "field_str0\x00value-1")) {
		t.Fatal()
	}

	// Group by field.
	result, _, err = idxBuilder.Scan(nil, []byte(nameWithVer),
		&query.ProcessorOptions{
			Name:       "mn-1_0000",
			Dimensions: []string{"field_str0"}}, resourceallocator.DefaultSeriesAllocateFunc)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	tagSets, ok = result.(GroupSeries)
	if !ok {
		t.Fatal()
	}
	if !assert1.Equal(t, len(SeriesKeys), len(tagSets)) {
		t.Fatal()
	}
	for i, tagSet := range tagSets {
		if !assert1.True(t, strings.Contains(string(tagSet.key), fmt.Sprintf("field_str0\x00value-%d", i))) {
			t.Fatal()
		}
	}
}

func TestPartialFieldIndexSearch(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getFieldIndexAndBuilder(path)
	defer idxBuilder.Close()

	SeriesKeys := []string{
		"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
	}

	opt := influx.IndexOption{
		IndexList: []uint16{3},
		Oid:       uint32(index.Field),
	}
	mmPoints := genPartialRowsByOpt(opt, SeriesKeys)
	if err := CreateIndexByRows(idxBuilder, mmPoints); err != nil {
		t.Fatal(err)
	}

	// No group by and without filter.
	nameWithVer := "mn-1_0000"
	result, _, err := idxBuilder.Scan(nil, []byte(nameWithVer), &query.ProcessorOptions{Name: nameWithVer}, resourceallocator.DefaultSeriesAllocateFunc)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	tagSets, ok := result.(GroupSeries)
	if !ok {
		t.Fatal()
	}
	if !assert1.Equal(t, 1, len(tagSets)) {
		t.Fatal()
	}
	if !assert1.Equal(t, 5, len(tagSets[0].SeriesKeys)) {
		t.Fatal()
	}
}

func genRowsByOpt(opt influx.IndexOption, seriesKeys []string) *dictpool.Dict {
	pts := make([]influx.Row, 0, len(seriesKeys))

	indexOptions := make([]influx.IndexOption, 0)
	indexOptions = append(indexOptions, opt)
	for i, key := range seriesKeys {
		pt := influx.Row{
			IndexOptions: indexOptions,
			Fields: []influx.Field{
				{
					Key:      "field_str0",
					Type:     influx.Field_Type_String,
					StrValue: fmt.Sprintf("value-%d", i),
				},
			},
		}
		s := strings.Split(key, ",")
		strs := strings.Split(s[1], string(byte(0)))
		pt.Name = s[0]
		pt.Tags = make(influx.PointTags, len(strs)/2)
		for i := 0; i < len(strs); i += 2 {
			pt.Tags[i/2].Key = strs[i]
			pt.Tags[i/2].Value = strs[i+1]
		}
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}

	mmPoints := &dictpool.Dict{}
	mmPoints.Set("mn-1_0000", &pts)
	return mmPoints
}

func genPartialRowsByOpt(opt influx.IndexOption, seriesKeys []string) *dictpool.Dict {
	pts := make([]influx.Row, 0, len(seriesKeys))

	indexOptions := make([]influx.IndexOption, 0)
	indexOptions = append(indexOptions, opt)

	tmpOpt := influx.IndexOption{
		IndexList: []uint16{0},
		Oid:       uint32(index.MergeSet),
	}
	tmpIndexOptions := make([]influx.IndexOption, 0)
	tmpIndexOptions = append(tmpIndexOptions, tmpOpt)
	for i, key := range seriesKeys {
		pt := influx.Row{
			Fields: []influx.Field{
				{
					Key:      "field_str0",
					Type:     influx.Field_Type_String,
					StrValue: fmt.Sprintf("value-%d", i),
				},
			},
		}
		if i == 0 {
			pt.IndexOptions = tmpIndexOptions
		} else {
			pt.IndexOptions = indexOptions
		}
		s := strings.Split(key, ",")
		strs := strings.Split(s[1], string(byte(0)))
		pt.Name = s[0]
		pt.Tags = make(influx.PointTags, len(strs)/2)
		for i := 0; i < len(strs); i += 2 {
			pt.Tags[i/2].Key = strs[i]
			pt.Tags[i/2].Value = strs[i+1]
		}
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}

	mmPoints := &dictpool.Dict{}
	mmPoints.Set("mn-1_0000", &pts)
	return mmPoints
}

func CreateIndexByBuild(iBuilder *IndexBuilder, idx Index) {
	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=value3",
		"mn-1,tk1=value11,tk2=value22,tk3=value33",
		"mn-1,tk1=value1,tk2=value22,tk3=value3",
		"mn-1,tk1=value11,tk2=value2,tk3=value33",
		"mn-1,tk1=value11,tk2=value22,tk3=value3",
	}
	pts := make([]influx.Row, 0, len(keys))
	for _, key := range keys {
		pt := influx.Row{}
		strs := strings.Split(key, ",")
		pt.Name = strs[0]
		pt.Tags = make(influx.PointTags, len(strs)-1)
		for i, str := range strs[1:] {
			kv := strings.Split(str, "=")
			pt.Tags[i].Key = kv[0]
			pt.Tags[i].Value = kv[1]
		}
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}

	mmPoints := &dictpool.Dict{}
	mmPoints.Set("mn-1", &pts)
	if err := iBuilder.CreateIndexIfNotExists(mmPoints, true); err != nil {
		panic(err)
	}

	for mmIndex := range mmPoints.D {
		rows, ok := mmPoints.D[mmIndex].Value.(*[]influx.Row)
		if !ok {
			panic("create index failed due to map mmPoints")
		}

		for rowIdx := range *rows {
			if (*rows)[rowIdx].SeriesId == 0 {
				panic("create index failed")
			}
		}
	}

	iBuilder.Close()
	iBuilder.Open()
}

func CreateIndexByRows(iBuilder *IndexBuilder, mmPoints *dictpool.Dict) error {

	if err := iBuilder.CreateIndexIfNotExists(mmPoints, true); err != nil {
		return err
	}

	for mmIndex := range mmPoints.D {
		rows, ok := mmPoints.D[mmIndex].Value.(*[]influx.Row)
		if !ok {
			return fmt.Errorf("create index failed due to map mmPoints")
		}

		for rowIdx := range *rows {
			if (*rows)[rowIdx].SeriesId == 0 {
				return fmt.Errorf("create index failed")
			}
		}
	}

	if err := iBuilder.Close(); err != nil {
		return err
	}
	if err := iBuilder.Open(); err != nil {
		return err
	}
	return nil
}

func BenchmarkCreateIndex1(b *testing.B) {
	path := b.TempDir()
	_, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()

	type IndexItem struct {
		name      []byte
		key       []byte
		shardKey  []byte
		tags      []influx.Tag
		shardID   uint64
		timestamp int64
		mmPoints  *dictpool.Dict
	}
	n := 1000000
	items := make([]*IndexItem, n)

	for i := 0; i < n; i++ {
		item := new(IndexItem)
		name := "mn-" + strconv.Itoa(i%100)
		tags := make([]influx.Tag, 10)
		var key string
		key += name
		for k := 0; k < 10; k++ {
			tags[k] = influx.Tag{
				Key:   "key-" + strconv.Itoa(k),
				Value: "value-" + strconv.Itoa(k*1000+rand.Intn(1000)),
			}
			key += "," + tags[k].Key + "=" + tags[k].Value + ","
		}
		key_b := []byte(key[:len(key)-1])
		shardID := uint64(i % 100)
		shardKey_b := []byte(tags[0].Key + "=" + tags[0].Value + "," + tags[1].Key + "=" + tags[1].Value)
		timestamp := time.Now().UnixNano()

		item.shardID = shardID
		item.shardKey = shardKey_b
		item.timestamp = timestamp
		item.key = key_b
		item.tags = tags
		item.name = []byte(name)
		pt := influx.Row{}
		pt.IndexKey = item.key
		pt.SeriesId = 0
		pt.Name = name
		pt.Tags = tags
		item.mmPoints = &dictpool.Dict{}
		item.mmPoints.Set(name+"_0000", &[]influx.Row{pt})
		items[i] = item
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		item := items[i%n]
		idxBuilder.CreateIndexIfNotExists(item.mmPoints, true)
	}
	b.StopTimer()
}

func GetIndexKey(serieskey string) []byte {
	pt := influx.Row{}
	strs := strings.Split(serieskey, ",")
	pt.Name = strs[0]
	pt.Tags = make(influx.PointTags, len(strs)-1)
	for i, str := range strs[1:] {
		kv := strings.Split(str, "=")
		pt.Tags[i].Key = kv[0]
		pt.Tags[i].Value = kv[1]
	}
	sort.Sort(&pt.Tags)
	pt.Timestamp = time.Now().UnixNano()
	pt.UnmarshalIndexKeys(nil)
	return pt.IndexKey
}

func getIndexAndBuilder(path string) (Index, *IndexBuilder) {
	lockPath := ""
	indexIdent := &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 3, IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}
	ltime := uint64(time.Now().Unix())
	opts := new(Options).
		Path(path).
		IndexType(index.Field).
		StartTime(time.Now()).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		Ident(indexIdent).
		Lock(&lockPath).
		LogicalClock(1).
		SequenceId(&ltime)

	indexBuilder := NewIndexBuilder(opts)

	indexIdent = &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 1, IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}
	opt := new(Options).
		Path(path).
		IndexType(index.MergeSet).
		EngineType(config.TSSTORE).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		Ident(indexIdent).
		Lock(&lockPath)
	primaryIndex, err := NewIndex(opt)
	if err != nil {
		panic(err)
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	invertedIndexRelation, err := NewIndexRelation(opt, primaryIndex, indexBuilder)
	if err != nil {
		panic(err)
	}

	indexBuilder.Relations[uint32(index.MergeSet)] = invertedIndexRelation

	err = indexBuilder.Open()
	if err != nil {
		panic(err)
	}

	return primaryIndex, indexBuilder
}

func TestCreateFieldIndex(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getIndexAndBuilder(path)
	defer idxBuilder.Close()

	SeriesKeys := []string{
		"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
		"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
	}

	opt := influx.IndexOption{
		IndexList: []uint16{3},
		Oid:       uint32(index.Field),
	}
	mmPoints := genRowsByOpt(opt, SeriesKeys)
	if err := CreateIndexByRows(idxBuilder, mmPoints); err != nil {
		t.Fatal(err)
	}

	idxBuilder.Flush()

	// No group by and without filter.
	nameWithVer := "mn-1_0000"
	result, _, err := idxBuilder.Scan(nil, []byte(nameWithVer), &query.ProcessorOptions{Name: nameWithVer}, resourceallocator.DefaultSeriesAllocateFunc)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	tagSets, ok := result.(GroupSeries)
	if !ok {
		t.Fatal()
	}
	if !assert1.Equal(t, 1, len(tagSets)) {
		t.Fatal()
	}
	if !assert1.Equal(t, 5, len(tagSets[0].SeriesKeys)) {
		t.Fatal()
	}

	sort.Strings(SeriesKeys)
	sort.Sort(tagSets[0])
	for i, key := range SeriesKeys {
		if !assert1.True(t, strings.Contains(string(tagSets[0].SeriesKeys[i]), key)) {
			t.Fatal(string(tagSets[0].SeriesKeys[i]), key)
		}
	}

	// No group by and with field filter.
	result, _, err = idxBuilder.Scan(nil, []byte(nameWithVer),
		&query.ProcessorOptions{
			Name:      "mn-1_0000",
			Condition: MustParseExpr(`field_str0='value-1'`)}, resourceallocator.DefaultSeriesAllocateFunc)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	tagSets, ok = result.(GroupSeries)
	if !ok {
		t.Fatal()
	}
	if !assert1.Equal(t, 1, len(tagSets)) {
		t.Fatal()
	}
	if !assert1.True(t, strings.Contains(string(tagSets[0].SeriesKeys[0]), "field_str0\x00value-1")) {
		t.Fatal()
	}

	// Group by field.
	result, _, err = idxBuilder.Scan(nil, []byte(nameWithVer),
		&query.ProcessorOptions{
			Name:       "mn-1_0000",
			Dimensions: []string{"field_str0"}}, resourceallocator.DefaultSeriesAllocateFunc)
	if !assert1.Nil(t, err) {
		t.Fatal()
	}
	tagSets, ok = result.(GroupSeries)
	if !ok {
		t.Fatal()
	}
	if !assert1.Equal(t, len(SeriesKeys), len(tagSets)) {
		t.Fatal()
	}
	for i, tagSet := range tagSets {
		if !assert1.True(t, strings.Contains(string(tagSet.key), fmt.Sprintf("field_str0\x00value-%d", i))) {
			t.Fatal()
		}
	}
}

func TestCreateFieldIndexError1(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getFieldIndexAndBuilder(path)
	defer idxBuilder.Close()
	for i := range idxBuilder.Relations {
		if i == int(index.Field) {
			break
		}
	}

	opts := new(Options).
		Path(path).
		IndexType(index.Field).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour)

	err := idxBuilder.initRelation(uint32(index.Field), opts, nil)
	if err != nil {
		t.Fatal()
	}
}

func TestCreateFieldIndexError2(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getIndexAndBuilder(path)
	defer idxBuilder.Close()

	opts := new(Options).
		Path(path).
		IndexType(index.Field + 1).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour)

	err := idxBuilder.initRelation(uint32(index.Field), opts, nil)
	if err == nil {
		t.Fatal()
	}
}

func TestMergeSetFlush(t *testing.T) {
	path := t.TempDir()
	opts := new(Options).
		Path(path).
		IndexType(index.Field)

	fi, err := NewFieldIndex(opts)
	if err != nil {
		t.Fatal()
	}

	MergeSetFlush(fi)
}

func TestDeleteIndex(t *testing.T) {
	path := t.TempDir()
	_, idxBuilder := getIndexAndBuilder(path)
	defer idxBuilder.Close()

	err := idxBuilder.Delete([]byte("mn-1"), nil, defaultTR)
	if err != nil {
		t.Fatal()
	}
}

func TestMergeSetCacheClear(t *testing.T) {
	path := t.TempDir()
	opts := new(Options).
		Path(path).
		IndexType(index.Field)

	mIndex, err := NewMergeSetIndex(opts)
	if err != nil {
		t.Fatal()
	}
	mem := 1 * 1024 * 1024
	mIndex.cache = newIndexCache(mem/32, mem/32, mem/16, mem/128, path, false)

	err = MergeSetCacheClear(mIndex)
	if err != nil {
		t.Fatal()
	}

	fi, err := NewFieldIndex(opts)
	if err != nil {
		t.Fatal()
	}
	err = MergeSetCacheClear(fi)
	if err == nil {
		t.Fatal()
	}
}

func TestFieldIndexCacheClear(t *testing.T) {
	path := t.TempDir()
	opts := new(Options).
		Path(path).
		IndexType(index.Field)

	fIndex, err := NewFieldIndex(opts)
	if err != nil {
		t.Fatal()
	}

	FieldCacheClear(fIndex)
}

func TestTextIndexCacheClear(t *testing.T) {
	path := t.TempDir()
	opts := new(Options).
		Path(path).
		IndexType(index.Field)

	tIndex, err := NewTextIndex(opts)
	if err != nil {
		t.Fatal()
	}

	TextCacheClear(tIndex)
}
