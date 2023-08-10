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
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/vm/uint64set"
	"github.com/stretchr/testify/require"
)

var (
	duration  = time.Hour
	endTime   = time.Now().Add(duration)
	defaultTR = TimeRange{Min: time.Now().UnixNano(), Max: time.Now().UnixNano()}
)

var (
	fieldMap = map[string]influxql.DataType{
		"field_float1": influxql.Float,
		"field_str0":   influxql.String,
	}
)

func TestSearchSeries(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

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

	t.Run("NoCond", func(t *testing.T) {
		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("EQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1'`), defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
		})
	})

	t.Run("NEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1!='value1'`), defaultTR, []string{
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("AND", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value11') AND (tk2='value22')`), defaultTR, []string{
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("OR", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value1') OR (tk3='value33')`), defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("RegEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1=~/val.*1/`), defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk1=~/val.*11/`), defaultTR, []string{
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk1=~/(val.*e1|val.*11)/`), defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk1=~/.*/`), defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("RegNEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1!~/val.*11/`), defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
		})

		f([]byte("mn-1"), MustParseExpr(`tk1!~/.*/`), defaultTR, []string{})
	})

	t.Run("UnlimitedTR", func(t *testing.T) {
		f([]byte("mn-1"), nil, TimeRange{Min: math.MinInt64, Max: math.MaxInt64}, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("ExistFieldKey", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})
}

func TestSeriesByExprIterator(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
	}

	f := func(name []byte, expr influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		index := idx.(*MergeSetIndex)
		is := index.getIndexSearch()
		defer index.putIndexSearch(is)

		name = append(name, []byte("_0000")...)

		var tsids *uint64set.Set
		iterator, err := is.seriesByExprIterator(name, expr, &tsids, false)
		if err != nil {
			t.Fatal(err)
		}

		ids := iterator.Ids().AppendTo(nil)
		assert.Equal(t, len(ids), len(expectedSeriesKeys))

		keys := make([]string, 0, len(ids))
		for _, id := range ids {
			key, err := index.searchSeriesKey(nil, id)
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, string(influx.Parse2SeriesKey(key, nil, true)))
		}
		sort.Strings(keys)

		for i := 0; i < len(keys); i++ {
			assert.Equal(t, keys[i], expectedSeriesKeys[i])
		}
	}

	// tag AND field
	opt.Condition = MustParseExpr(`tk1='value11' AND field_float1>1.0`)
	t.Run("tag AND field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// field AND tag
	opt.Condition = MustParseExpr(`field_float1>1.0 AND tk1='value11'`)
	t.Run("field AND tag", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// field AND field
	opt.Condition = MustParseExpr(`field_float1>1.0 AND field_float1>0'`)
	t.Run("field AND field", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// tag AND parent field
	opt.Condition = MustParseExpr(`tk1='value11' AND ((field_float1))>1.0`)
	t.Run("tag AND parent field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// parent field AND tag
	opt.Condition = MustParseExpr(`((field_float1))>1.0 AND tk1='value11'`)
	t.Run("parent field AND tag", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// parent field AND parent field
	opt.Condition = MustParseExpr(`((field_float1>1.0)) AND ((field_float1>0))'`)
	t.Run("parent field AND parent field", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// tag OR field
	opt.Condition = MustParseExpr(`tk1='value11' OR field_float1>1.0`)
	t.Run("tag OR field", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// field OR tag
	opt.Condition = MustParseExpr(`field_float1>1.0 OR tk1='value11'`)
	t.Run("field OR tag", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// field OR field
	opt.Condition = MustParseExpr(`field_float1>1.0 OR field_float1<0.5`)
	t.Run("field OR field", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// tag OR parent field
	opt.Condition = MustParseExpr(`tk1='value11' OR ((field_float1>1.0))`)
	t.Run("tag OR parent field", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// parent field OR tag
	opt.Condition = MustParseExpr(`((field_float1>1.0)) OR tk1='value11'`)
	t.Run("parent field OR tag", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})

	// parent field OR parent field
	opt.Condition = MustParseExpr(`((field_float1>1.0)) OR ((field_float1<0.5))`)
	t.Run("parent field OR parent field", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk1='value1' or field_float1>1.0`), defaultTR, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value3",
			"mn-1_0000,tk1\x00value11\x00tk2\x00value22\x00tk3\x00value33",
		})
	})
}

func TestSearchSeriesWithOpts(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx, []string{
		"mn-1,tk1=value1",
		"mn-1,tk1=value1,tk2=value2,tk3=value3",
	}...)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		name = append(name, []byte("_0000")...)
		_, span := tracing.NewTrace("root")
		if e := resourceallocator.InitResAllocator(1000, 0, 1, 0, resourceallocator.SeriesParallelismRes, time.Second, 1); e != nil {
			t.Fatal(e)
		}
		groups, _, err := idx.SearchSeriesWithOpts(span, name, opt, resourceallocator.DefaultSeriesAllocateFunc, nil)
		if err != nil {
			t.Fatal(err)
		}
		keys := make([]string, 0)
		for _, group := range groups {
			for _, key := range group.SeriesKeys {
				keys = append(keys, string(key))
			}
		}
		sort.Strings(keys)
		sort.Strings(expectedSeriesKeys)
		assert.Equal(t, len(keys), len(expectedSeriesKeys))
		for i := 0; i < len(keys); i++ {
			assert.Equal(t, keys[i], expectedSeriesKeys[i])
		}
	}

	t.Run("single_series_search", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value1'`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1_0000,tk1\x00value1",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		})

		// Test singleSeries query with the same condition above
		r := influx.Row{Name: "mn-1_0000", Tags: influx.PointTags{
			influx.Tag{
				Key:   "tk1",
				Value: "value1",
			},
		}}
		opt.SeriesKey = r.UnmarshalIndexKeys(nil)
		opt.HintType = hybridqp.FullSeriesQuery
		f([]byte("mn-1"), opt, []string{
			"mn-1_0000,tk1\x00value1",
		})

		// Test condition with or field filter
		opt.Condition = MustParseExpr(`tk1='value1' OR field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1_0000,tk1\x00value1",
		})
	})

	t.Run("double_quoted_tag_values", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1="value1"`),
		}
		f([]byte("mn-1"), opt, nil)

		opt.Condition = MustParseExpr(`tk1="tk2"`)
		f([]byte("mn-1"), opt, []string{
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		})

		opt.Condition = MustParseExpr(`tk1!="tk2"`)
		f([]byte("mn-1"), opt, []string{
			"mn-1_0000,tk1\x00value1",
		})
	})

	t.Run("regex", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1=~/.*/`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1_0000,tk1\x00value1",
			"mn-1_0000,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		})

		opt.Condition = MustParseExpr(`tk1!~/.*/`)
		f([]byte("mn-1"), opt, nil)
	})
}

func TestSearchSeriesWithLimit(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx, []string{
		"mn-1,tk1=value1,tk2=k2",
		"mn-1,tk1=value2,tk2=k2",
		"mn-1,tk1=value3,tk2=k2",
		"mn-1,tk1=value4,tk2=k2",
		"mn-1,tk1=value5,tk2=k2",
	}...)

	run := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		name = append(name, []byte("_0000")...)
		_, span := tracing.NewTrace("root")
		groups, _, err := idx.SearchSeriesWithOpts(span, name, opt, func(num int64) error {
			return nil
		}, nil)
		require.NoError(t, err)

		keys := make([]string, 0)
		for _, group := range groups {
			for _, key := range group.SeriesKeys {
				keys = append(keys, string(key))
			}
		}
		sort.Strings(keys)
		sort.Strings(expectedSeriesKeys)
		require.Equal(t, len(expectedSeriesKeys), len(keys))
		for i := 0; i < len(keys); i++ {
			require.Equal(t, keys[i], expectedSeriesKeys[i])
		}
	}

	syscontrol.SetQuerySeriesLimit(2)
	defer syscontrol.SetQuerySeriesLimit(0)
	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
		Condition: MustParseExpr(`tk2='k2'`),
	}
	run([]byte("mn-1"), opt, []string{
		"mn-1_0000,tk1\x00value1\x00tk2\x00k2",
		"mn-1_0000,tk1\x00value2\x00tk2\x00k2",
	})
}

func TestSearchSeriesWithoutLimit(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx, []string{
		"mn-1,tk1=value1,tk2=k2",
		"mn-1,tk1=value2,tk2=k2",
		"mn-1,tk1=value3,tk2=k2",
		"mn-1,tk1=value4,tk2=k2",
		"mn-1,tk1=value5,tk2=k2",
	}...)

	run := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		name = append(name, []byte("_0000")...)
		_, span := tracing.NewTrace("root")
		_, _, err := idx.SearchSeriesWithOpts(span, name, opt, func(num int64) error {
			return nil
		}, nil)
		if err == nil {
			t.Error("expect error")
		}
	}

	syscontrol.SetQuerySeriesLimit(2)
	syscontrol.SetQueryEnabledWhenExceedSeries(false)
	defer func() {
		syscontrol.SetQuerySeriesLimit(0)
		syscontrol.SetQueryEnabledWhenExceedSeries(true)
	}()
	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
		Condition: MustParseExpr(`tk2='k2'`),
	}
	run([]byte("mn-1"), opt, nil)
}

func TestSearchSeriesKeys(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

	f := func(name []byte, opts influxql.Expr, expectedSeriesKeys map[string]struct{}) {
		dst := make([][]byte, 1)
		name = append(name, []byte("_0000")...)
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
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3":    {},
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3":   {},
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33":  {},
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3":  {},
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33": {},
		})
	})
}

func TestDropMeasurement(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

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
		"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
		"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
		"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
		"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
		"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
	})

	t.Run("IndexReopenAndQuery", func(t *testing.T) {
		if err := idx.Close(); err != nil {
			t.Fatal(err)
		}
		if err := idx.Open(); err != nil {
			t.Fatal(err)
		}
		f([]byte("mn-1"), nil, defaultTR, nil)
	})

	t.Run("AddNewIndexAndQuery", func(t *testing.T) {
		CreateIndexByPts(idx)
		f([]byte("mn-1"), nil, defaultTR, []string{
			"mn-1_0000,tk1=value1,tk2=value2,tk3=value3",
			"mn-1_0000,tk1=value1,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value2,tk3=value33",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value3",
			"mn-1_0000,tk1=value11,tk2=value22,tk3=value33",
		})
	})
}

func TestDeleteTSIDs(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

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

		f([]byte("mn-1"), nil, defaultTR, []string{
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

func TestSearchTagValues(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

	f := func(name []byte, tagKeys [][]byte, condition influxql.Expr, expectedTagValues [][]string) {
		name = append(name, []byte("_0000")...)
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

func TestSearchTagValuesForLabelStore(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.COLUMNSTORE)
	defer idxBuilder.Close()
	mergeSetIndex := idx.(*MergeSetIndex)
	csIndexImpl := mergeSetIndex.StorageIndex.(*CsIndexImpl)
	mergeSetIndex = generateIndexByPts(csIndexImpl, mergeSetIndex)

	f := func(name []byte, tagKeys [][]byte, condition influxql.Expr, expectedTagValues [][]string) {
		name = append(name, []byte("_0000")...)
		tagValues, err := mergeSetIndex.SearchTagValues(name, tagKeys, condition)
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

	t.Run("SingleKey", func(t *testing.T) {
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

	t.Run("MultiKeys", func(t *testing.T) {
		f([]byte("mn-1"), [][]byte{[]byte("tk1"), []byte("tk2")}, nil, [][]string{
			{
				"value1",
				"value11",
			},
			{
				"value2",
				"value22",
			},
		})

		f([]byte("mn-1"), [][]byte{[]byte("tk3"), []byte("tk2")}, nil, [][]string{
			{
				"value3",
				"value33",
			},
			{
				"value2",
				"value22",
			},
		})

		f([]byte("mn-1"), [][]byte{[]byte("tk3"), []byte("tk2"), []byte("tk1")}, nil, [][]string{
			{
				"value3",
				"value33",
			},
			{
				"value2",
				"value22",
			},
			{
				"value1",
				"value11",
			},
		})
	})
}

func generateIndexByPts(csIndexImpl *CsIndexImpl, idx *MergeSetIndex, keys ...string) *MergeSetIndex {
	if keys == nil {
		keys = []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
		}
	}

	pts := make([]influx.Row, 0, len(keys))
	for _, key := range keys {
		pt := influx.Row{}
		strs := strings.Split(key, ",")
		pt.Name = strs[0] + "_0000"
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
	mmPoints.Set("mn-1_0000", &pts)

	for mmIndex := range mmPoints.D {
		rows, ok := mmPoints.D[mmIndex].Value.(*[]influx.Row)
		if !ok {
			panic("create index failed due to map mmPoints")
		}

		for rowIdx := range *rows {
			err := csIndexImpl.CreateIndexIfNotExistsByRow(idx, &(*rows)[rowIdx])
			if err != nil {
				panic("create label store index failed ")
			}
		}
	}
	idx.Close()
	idx.Open()
	return idx
}

func TestSeriesCardinality(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

	f := func(name []byte, condition influxql.Expr, expectCardinality uint64) {
		name = append(name, []byte("_0000")...)
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

func TestSearchTagValuesCardinality(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer idxBuilder.Close()
	CreateIndexByPts(idx)

	f := func(name, tagKey []byte, expectCardinality uint64) {
		name = append(name, []byte("_0000")...)
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
		if err := idx.DeleteTSIDs([]byte("mn-1_0000"), MustParseExpr(`tk1='value1'`), defaultTR); err != nil {
			t.Fatal(err)
		}
		f([]byte("mn-1"), []byte("tk1"), 1)
	})
}

func CreateIndexByPts(idx Index, keys ...string) {
	if keys == nil {
		keys = []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
			"mn-1,tk1=value1,tk2=value22,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value22,tk3=value3",
		}
	}

	pts := make([]influx.Row, 0, len(keys))
	for _, key := range keys {
		pt := influx.Row{}
		strs := strings.Split(key, ",")
		pt.Name = strs[0] + "_0000"
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
	mmPoints.Set("mn-1_0000", &pts)
	if err := idx.CreateIndexIfNotExists(mmPoints); err != nil {
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

	idx.Close()
	idx.Open()
}

func BenchmarkGenerateUUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateUUID()
	}
}

func BenchmarkParallelGenerateUUID(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			GenerateUUID()
		}
	})
}

func BenchmarkCreateIndexIfNotExists(b *testing.B) {
	path := b.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
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
		item.mmPoints.Set(name, &[]influx.Row{pt})
		items[i] = item
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		item := items[i%n]
		idx.CreateIndexIfNotExists(item.mmPoints)
	}
	b.StopTimer()
}

// MustParseExpr parses an expression. Panic on error.
func MustParseExpr(s string) influxql.Expr {
	p := influxql.NewParser(strings.NewReader(s))
	defer p.Release()
	expr, err := p.ParseExpr()
	if err != nil {
		panic(err)
	}
	influxql.WalkFunc(expr, func(n influxql.Node) {
		ref, ok := n.(*influxql.VarRef)
		if !ok {
			return
		}
		ty, ok := fieldMap[ref.Val]
		if ok {
			ref.Type = ty
		} else {
			ref.Type = influxql.Tag
		}
	})
	return expr
}

func TestSortTagsets(t *testing.T) {
	var tagset TagSetInfo
	var tag2 []byte
	tag2 = append(tag2, "tag2"...)
	var tag1 []byte
	tag1 = append(tag1, "tag1"...)
	tagset.Append(2, tag2, nil, nil, nil)
	tagset.Append(1, tag1, nil, nil, nil)
	opt := query.ProcessorOptions{
		Limit:    1,
		HintType: hybridqp.ExactStatisticQuery,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	tagset.Sort(schema)
}

func TestGetIndexOidByName(t *testing.T) {
	_, err := GetIndexIdByName("field")
	if err != nil {
		t.Fatal(err)
	}
	_, err = GetIndexIdByName("FIELD")
	if err != nil {
		t.Fatal(err)
	}
}
