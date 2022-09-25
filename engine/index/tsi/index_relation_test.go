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
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func getTestIndexAndBuilder() (Index, *IndexBuilder) {
	opts := new(Options).
		Path(testIndexPath + "index-" + fmt.Sprintf("%d", time.Now().UnixNano())).
		IndexType(MergeSet).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour)

	indexBuilder := NewIndexBuilder(opts)
	indexBuilder.Relations = make(map[uint32]*IndexRelation)

	primaryIndex, err := NewIndex(opts)
	if err != nil {
		panic(err)
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	indexRelation, err := NewIndexRelation(opts, primaryIndex, indexBuilder)
	indexBuilder.Relations[uint32(MergeSet)] = indexRelation

	err = indexBuilder.Open()

	return primaryIndex, indexBuilder
}

func getTextIndexAndBuilder() (Index, *IndexBuilder) {
	opts := new(Options).
		Path(testIndexPath + "index-" + fmt.Sprintf("%d", time.Now().UnixNano())).
		IndexType(Text).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour)

	indexBuilder := NewIndexBuilder(opts)
	indexBuilder.Relations = make(map[uint32]*IndexRelation)

	opt := new(Options).
		Path(testIndexPath + "index-" + fmt.Sprintf("%d", time.Now().UnixNano())).
		IndexType(MergeSet).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour)
	primaryIndex, err := NewIndex(opt)
	if err != nil {
		panic(err)
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	textIndexRelation, err := NewIndexRelation(opts, primaryIndex, indexBuilder)
	invertedIndexRelation, err := NewIndexRelation(opt, primaryIndex, indexBuilder)

	indexBuilder.Relations[uint32(Text)] = textIndexRelation
	indexBuilder.Relations[uint32(MergeSet)] = invertedIndexRelation

	err = indexBuilder.Open()

	return primaryIndex, indexBuilder
}

func TestSearchSeries_Relation(t *testing.T) {
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
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
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
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
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
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

	f([]byte("mn-1"), nil, defaultTR, []string{
		"mn-1,tk1=value1,tk2=value2,tk3=value3",
		"mn-1,tk1=value1,tk2=value22,tk3=value3",
		"mn-1,tk1=value11,tk2=value2,tk3=value33",
		"mn-1,tk1=value11,tk2=value22,tk3=value3",
		"mn-1,tk1=value11,tk2=value22,tk3=value33",
	})

	t.Run("DropAndQuery", func(t *testing.T) {
		if err := idxBuilder.DropMeasurement([]byte("mn-1")); err != nil {
			t.Fatal(err)
		}
		f([]byte("mn-1"), nil, defaultTR, nil)
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
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})
}

func TestDeleteTSIDs_Relation(t *testing.T) {
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
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

	t.Run("NormalQuery", func(t *testing.T) {
		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("DeleteByEQCond", func(t *testing.T) {
		if err := idx.DeleteTSIDs([]byte("mn-1"), MustParseExpr(`tk1='value1'`), defaultTR); err != nil {
			t.Fatal(err)
		}

		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})

		if err := idx.DeleteTSIDs([]byte("mn-1"), MustParseExpr(`tk2='value2'`), defaultTR); err != nil {
			t.Fatal(err)
		}

		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("DeleteByBigTR", func(t *testing.T) {
		err := idx.DeleteTSIDs([]byte("mn-1"), MustParseExpr(`tk2='value2'`), TimeRange{time.Now().Add(-41 * 24 * time.Hour).UnixNano(), time.Now().UnixNano()})
		assert.Equal(t, strings.Contains(err.Error(), "too much dates"), true)
	})

	t.Run("DeleteWithoutCond", func(t *testing.T) {
		if err := idx.DeleteTSIDs([]byte("mn-1"), nil, defaultTR); err != nil {
			t.Fatal(err)
		}

		f([]byte("mn-1"), nil, defaultTR, nil)
	})
}

func TestSearchTagValues_Relation(t *testing.T) {
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
	CreateIndexByBuild(idxBuilder, idx)

	f := func(name []byte, tagKeys [][]byte, condition influxql.Expr, expectedTagValues [][]string) {
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

func TestSearchAllSeriesKeys_Relation(t *testing.T) {
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
	CreateIndexByBuild(idxBuilder, idx)

	f := func(expectCardinality uint64) {
		keys, err := idx.SearchAllSeriesKeys()
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, expectCardinality, uint64(len(keys)))
	}

	t.Run("NormalQuery", func(t *testing.T) {
		f(5)
	})

	t.Run("DeleteTSIDsByEQ", func(t *testing.T) {
		if err := idx.DeleteTSIDs([]byte("mn-1"), MustParseExpr(`tk1='value1'`), defaultTR); err != nil {
			t.Fatal(err)
		}
		f(3)
	})

	t.Run("DropMeasurement", func(t *testing.T) {
		if err := idxBuilder.DropMeasurement([]byte("mn-1")); err != nil {
			t.Fatal(err)
		}
		f(0)
	})
}

func TestSeriesCardinality_Relation(t *testing.T) {
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
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
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
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

func TestSearchAllTagValues_Relation(t *testing.T) {
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)
	CreateIndexByBuild(idxBuilder, idx)

	f := func(tagKey string, expectTagValues map[string]map[string]struct{}) {
		tagValues, err := idx.SearchAllTagValues([]byte(tagKey))
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, len(expectTagValues), len(tagValues))
		for name, etvs := range expectTagValues {
			atvs, ok := tagValues[name]
			if !ok {
				t.Fatalf("expected measurement '%s' not found", name)
			}
			for tv := range etvs {
				if _, ok := atvs[tv]; !ok {
					t.Fatalf("expected tag value '%s' not found", tv)
				}
			}
		}
	}

	t.Run("NormalQuery", func(t *testing.T) {
		f("tk1", map[string]map[string]struct{}{
			"mn-1": {
				"value1":  {},
				"value11": {},
			},
		})

		f("tk2", map[string]map[string]struct{}{
			"mn-1": {
				"value2":  {},
				"value22": {},
			},
		})

		f("tk3", map[string]map[string]struct{}{
			"mn-1": {
				"value3":  {},
				"value33": {},
			},
		})
	})
}

func TestWriteTextData(t *testing.T) {
	idx, idxBuilder := getTextIndexAndBuilder()
	defer clear(idx)
	CreateTextIndexByBuild(idxBuilder, idx)
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
	if err := iBuilder.CreateIndexIfNotExists(mmPoints); err != nil {
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

func CreateTextIndexByBuild(iBuilder *IndexBuilder, idx Index) {
	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=value3",
		"mn-1,tk1=value11,tk2=value22,tk3=value33",
		"mn-1,tk1=value1,tk2=value22,tk3=value3",
		"mn-1,tk1=value11,tk2=value2,tk3=value33",
		"mn-1,tk1=value11,tk2=value22,tk3=value3",
	}
	pts := make([]influx.Row, 0, len(keys))
	indexList := make([]uint16, 0)
	indexList = append(indexList, 0)
	opt := influx.IndexOption{
		IndexList: indexList,
		Oid:       uint32(Text),
	}
	indexOptions := make([]influx.IndexOption, 0)
	indexOptions = append(indexOptions, opt)
	for _, key := range keys {
		pt := influx.Row{
			IndexOptions: indexOptions,
		}
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
	if err := iBuilder.CreateIndexIfNotExists(mmPoints); err != nil {
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

	if err := iBuilder.CreateIndexIfPrimaryKeyExists(mmPoints, true); err != nil {
		panic(err)
	}

	iBuilder.Close()
	iBuilder.Open()
}

func BenchmarkCreateIndex1(b *testing.B) {
	idx, idxBuilder := getTestIndexAndBuilder()
	defer clear(idx)

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
		item.mmPoints.Set(name, &[]influx.Row{pt})
		items[i] = item
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		item := items[i%n]
		idxBuilder.CreateIndexIfNotExists(item.mmPoints)
	}
	b.StopTimer()
}
