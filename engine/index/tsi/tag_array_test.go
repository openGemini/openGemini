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

package tsi

import (
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
	"github.com/savsgio/dictpool"
	"github.com/stretchr/testify/require"
)

func init() {
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ShardsParallelismRes, 0, 0)
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.SeriesParallelismRes, 0, 0)
}

func unmarshalTag(tag *influx.Tag, s string, noEscapeChars bool) error {
	tag.Reset()
	n := nextUnescapedChar(s, '=', noEscapeChars, true, false)
	if n < 0 {
		return errno.NewError(errno.WriteMissTagValue, s)
	}
	tag.Key = unescapeTagValue(s[:n], noEscapeChars)
	tag.Value = unescapeTagValue(s[n+1:], noEscapeChars)
	return nil
}

func unescapeTagValue(s string, noEscapeChars bool) string {
	if noEscapeChars {
		// Fast path - no escape chars.
		return s
	}
	n := strings.IndexByte(s, '\\')
	if n < 0 {
		return s
	}

	// Slow path. Remove escape chars.
	dst := make([]byte, 0, len(s))
	for {
		dst = append(dst, s[:n]...)
		s = s[n+1:]
		if len(s) == 0 {
			return string(append(dst, '\\'))
		}
		ch := s[0]
		if ch != ' ' && ch != ',' && ch != '=' && ch != '\\' {
			dst = append(dst, '\\')
		}
		dst = append(dst, ch)
		s = s[1:]
		n = strings.IndexByte(s, '\\')
		if n < 0 {
			return string(append(dst, s...))
		}
	}
}

func unmarshalTags(dst []influx.Tag, s string, noEscapeChars bool) ([]influx.Tag, error) {
	for {
		if cap(dst) > len(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, influx.Tag{})
		}
		tag := &dst[len(dst)-1]
		n := nextUnescapedChar(s, ',', noEscapeChars, true, true)
		if n < 0 {
			if err := unmarshalTag(tag, s, noEscapeChars); err != nil {
				return dst[:len(dst)-1], err
			}
			if len(tag.Key) == 0 || len(tag.Value) == 0 {
				// Skip empty tag
				dst = dst[:len(dst)-1]
			}
			return dst, nil
		}
		if err := unmarshalTag(tag, s[:n], noEscapeChars); err != nil {
			return dst[:len(dst)-1], err
		}
		s = s[n+1:]
		if len(tag.Key) == 0 || len(tag.Value) == 0 {
			// Skip empty tag
			dst = dst[:len(dst)-1]
		}
	}
}

func nextUnescapedChar(s string, ch byte, noEscapeChars, enableTagArray, tagParse bool) int {
	if noEscapeChars {
		// eg,tk1=value1,tk2=[value2,value22],tk3=value3
		if enableTagArray && tagParse {
			return nextUnescapedCharForTagArray(s, ch)
		}
		return strings.IndexByte(s, ch)
	}

	sOrig := s
again:
	// eg,tk1=value1,tk2=[value2,value22],tk3=value3
	var n int
	if enableTagArray && tagParse {
		n = nextUnescapedCharForTagArray(s, ch)
	} else {
		n = strings.IndexByte(s, ch)
	}

	if n < 0 {
		return -1
	}
	if n == 0 {
		return len(sOrig) - len(s) + n
	}
	if s[n-1] != '\\' {
		return len(sOrig) - len(s) + n
	}
	nOrig := n
	slashes := 0
	for n > 0 && s[n-1] == '\\' {
		slashes++
		n--
	}
	if slashes&1 == 0 {
		return len(sOrig) - len(s) + nOrig
	}
	s = s[nOrig+1:]
	goto again
}

func nextUnescapedCharForTagArray(s string, ch byte) int {
	// Fast path: just search for ch in s, since s has no escape chars.
	lbracket := strings.IndexByte(s, '[')
	if lbracket < 0 {
		return strings.IndexByte(s, ch)
	}

	rbracket := strings.IndexByte(s[lbracket:], ']')
	if rbracket < 0 {
		return strings.IndexByte(s, ch)
	}
	rbracket += lbracket

	index := strings.IndexByte(s, ch)

	// eg,tk1=value1,tk2=[value2,value22]
	// this time is tk1=value1
	if index < lbracket || index > rbracket {
		return index
	}

	if rbracket == len(s)-1 {
		// eg,tk1=value1,tk2=[value2,value22]
		// this time is tk2=[value2,value22]
		return -1
	}

	// eg, eg,tk1=value1,tk2=[value2,value22],tk3=value3
	// this time is tk2=[value2,value22]
	return rbracket + 1
}

func generateKeyWithTagArray1() []string {
	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=[value3,value33,value333]",
		"mn-1,tk1=value11,tk2=value2,tk3=value3",
		"mn-1,tk1=value11,tk2=value2,tk3=[value33]",
	}

	return keys
}

func generateKeyWithTagArray2() []string {
	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=value3",
		"mn-1,tk1=value1,tk2=value2,tk3=value33",
		"mn-1,tk1=value1,tk2=value2,tk3=value333",
		"mn-1,tk1=value11,tk2=value2,tk3=value3",
		"mn-1,tk1=value11,tk2=value2,tk3=value33",
	}

	return keys
}

func generateKeyWithTagArray3() []string {
	keys := []string{
		"mn-1,tk3=[]",
	}

	return keys
}

func generateKeyWithTagArray4() []string {
	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=[value3,value33,value333]",
		"mn-1,tk1=value11,tk2=[value2,value22,value222],tk3=[value3,value33,]",
	}

	return keys
}

func generateKeyWithTagArray5() []string {
	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=[value3,value33]",
		"mn-1,tk1=value1,tk2=value2,tk3=value3",
	}

	return keys
}

func generateKeyWithForProm() []string {
	keys := []string{
		"mn-1,tk1=value1_1,tk2=value2,tk3=value3",
		"mn-1,tk1=value1_2,tk2=value2,tk3=value3",
	}

	return keys
}

func CreateIndexByPts_TagArray(iBuilder *IndexBuilder, idx Index, genKeys func() []string) {
	keys := genKeys()
	pts := make([]influx.Row, 0, len(keys))
	var tagsPool []influx.Tag
	for _, key := range keys {
		tagsPool = tagsPool[:0]
		pt := influx.Row{}
		n := strings.IndexByte(key, ',')
		pt.Name = key[:n]
		key = key[n+1:]
		tagsPool, _ = unmarshalTags(tagsPool, key, true)

		pt.Tags = make(influx.PointTags, len(tagsPool))
		for i, tag := range tagsPool {
			pt.Tags[i].Key = tag.Key
			pt.Tags[i].Value = tag.Value
			pt.Tags[i].IsArray = false
			if strings.HasPrefix(pt.Tags[i].Value, "[") && strings.HasSuffix(pt.Tags[i].Value, "]") {
				pt.Tags[i].IsArray = true
			}
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
		if !strings.Contains(err.Error(), "error tag array") {
			panic(err)
		}
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

func CreateIndexByPts_TagArray_Error(iBuilder *IndexBuilder, idx Index, genKeys func() []string) {
	keys := genKeys()
	pts := make([]influx.Row, 0, len(keys))
	var tagsPool []influx.Tag
	for _, key := range keys {
		tagsPool = tagsPool[:0]
		pt := influx.Row{}
		n := strings.IndexByte(key, ',')
		pt.Name = key[:n]
		key = key[n+1:]
		tagsPool, _ = unmarshalTags(tagsPool, key, true)

		pt.Tags = make(influx.PointTags, len(tagsPool))
		for i, tag := range tagsPool {
			pt.Tags[i].Key = tag.Key
			pt.Tags[i].Value = tag.Value
			if strings.HasPrefix(pt.Tags[i].Value, "[") && strings.HasSuffix(pt.Tags[i].Value, "]") {
				pt.Tags[i].IsArray = true
			}
		}
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}

	mmPoints := &dictpool.Dict{}
	mmPoints.Set("mn-1", &pts)
	err := iBuilder.CreateIndexIfNotExists(mmPoints, true)

	if err == nil {
		panic("create index failed")
	}

	iBuilder.Close()
	iBuilder.Open()
}

func TestWriteTagArray_Success(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)
}

func TestWriteTagArray_Fail1(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray_Error(idxBuilder, idx, generateKeyWithTagArray3)
}

func TestSearchSeriesWithOpts_TagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		groups, _, err := idx.SearchSeriesWithOpts(nil, name, opt, resourceallocator.DefaultSeriesAllocateFunc, nil)
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

	t.Run("no_cond", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: nil,
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	t.Run("tag_and_field", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value11' AND field_float1>1.0`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})

		opt.Condition = MustParseExpr(`field_float1>1.0 AND tk3='value3'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
		})

		// Test condition with or field filter
		opt.Condition = MustParseExpr(`tk1='value1' OR field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})

		// Test condition from tag array
		opt.Condition = MustParseExpr(`tk3='value3' AND field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
		})
	})

	t.Run("tag_and_tag", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value1' AND tk3='value3'`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		})

		opt.Condition = MustParseExpr(`tk1='value1' OR tk3='value3'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
		})

		opt.Condition = MustParseExpr(`tk3!='value3''`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})
}

func TestSearchSeriesWithOpts_NoTagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray2)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		groups, _, err := idx.SearchSeriesWithOpts(nil, name, opt, resourceallocator.DefaultSeriesAllocateFunc, nil)
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

	t.Run("tag_and_field", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value11' AND field_float1>1.0`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})

		opt.Condition = MustParseExpr(`field_float1>1.0 AND tk1='value11'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})

		// Test condition with or field filter
		opt.Condition = MustParseExpr(`tk1='value1' OR field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})

		// Test condition from tag array
		opt.Condition = MustParseExpr(`tk3='value3' AND field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
		})
	})

	t.Run("tag_and_tag", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value1' AND tk3='value3'`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		})

		opt.Condition = MustParseExpr(`tk1='value1' OR tk3='value3'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
		})

		opt.Condition = MustParseExpr(`tk3!='value3''`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})
}

func TestUnmarshalCombineIndexKeysFail(t *testing.T) {
	src := []byte{1, 2}
	_, _, err := unmarshalCombineIndexKeys(nil, src)
	if err == nil {
		t.Fatal("expect unmarshalCombineIndexKeys fail, but success")
	}
}

func TestSearchSeriesWithTagArrayFail(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	mergeIndex, _ := idx.(*MergeSetIndex)

	src := []byte{1, 2}
	var isExpectSeries []bool
	_, _, _, err := mergeIndex.searchSeriesWithTagArray(1, nil, nil, src, isExpectSeries, nil, false)
	if err == nil {
		t.Fatal("expect searchSeriesWithTagArray fail, but success")
	}

	condition := MustParseExpr(`tk1='value11'`)
	src = []byte{0, 0, 0, 0, 0, 2, 0, 49, 0, 0, 0, 49, 0, 8, 99, 112, 117, 95, 48, 48, 48, 48, 0, 3, 0, 3, 116, 107, 49, 0, 6, 118, 97, 108, 117, 101, 49, 0, 3, 116, 107, 50, 0, 6, 118, 97, 108, 117, 101, 50, 0, 3, 116, 107, 51, 0, 6,
		0, 49, 0, 0, 0, 49, 0, 8, 99, 112, 117, 95, 48, 48, 48, 48, 0, 3, 0, 3, 116, 107, 49, 0, 6, 118, 97, 108, 117, 101, 49, 0, 3, 116, 107, 50, 0, 6, 118, 97, 108, 117, 101, 50, 0, 3, 116, 107, 51, 0, 6}
	_, _, _, err = mergeIndex.searchSeriesWithTagArray(1, nil, nil, src, isExpectSeries, condition, false)
	if err == nil {
		t.Fatal("expect searchSeriesWithTagArray fail, but success")
	}
}

func TestSeriesByExprIterator_TagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
	}

	f := func(name []byte, expr influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		index := idx.(*MergeSetIndex)
		is := index.getIndexSearch()
		defer index.putIndexSearch(is)

		var tsids *uint64set.Set
		iterator, err := is.seriesByExprIterator(name, expr, &tsids, false)
		if err != nil {
			t.Fatal(err)
		}

		ids := iterator.Ids().AppendTo(nil)

		keys := make([]string, 0, len(ids))
		var seriesKeys [][]byte
		var combineSeriesKey []byte
		var isExpectSeries []bool
		for _, id := range ids {
			seriesKeys, _, isExpectSeries, err := index.searchSeriesWithTagArray(id, seriesKeys, nil, combineSeriesKey, isExpectSeries, opt.Condition, false)
			if err != nil {
				t.Fatal(err)
			}
			for i := range seriesKeys {
				if !isExpectSeries[i] {
					continue
				}

				var seriesKey []byte
				seriesKey = append(seriesKey, seriesKeys[i]...)
				keys = append(keys, string(influx.Parse2SeriesKey(seriesKey, nil, true)))
			}

		}
		sort.Strings(keys)
		assert.Equal(t, len(keys), len(expectedSeriesKeys))

		for i := 0; i < len(keys); i++ {
			assert.Equal(t, keys[i], expectedSeriesKeys[i])
		}
	}

	// tag AND field
	opt.Condition = MustParseExpr(`tk1='value11' AND field_float1>1.0`)
	t.Run("tag AND field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// field AND tag
	opt.Condition = MustParseExpr(`field_float1>1.0 AND tk1='value11'`)
	t.Run("field AND tag", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// field AND field
	opt.Condition = MustParseExpr(`field_float1>1.0 AND field_float1>0'`)
	t.Run("field AND field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// tag AND parent field
	opt.Condition = MustParseExpr(`tk1='value11' AND ((field_float1))>1.0`)
	t.Run("tag AND parent field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// parent field AND tag
	opt.Condition = MustParseExpr(`((field_float1))>1.0 AND tk1='value11'`)
	t.Run("parent field AND tag", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// parent field AND parent field
	opt.Condition = MustParseExpr(`((field_float1>1.0)) AND ((field_float1>0))'`)
	t.Run("parent field AND parent field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// tag OR field
	opt.Condition = MustParseExpr(`tk1='value11' OR field_float1>1.0`)
	t.Run("tag OR field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// field OR tag
	opt.Condition = MustParseExpr(`field_float1>1.0 OR tk1='value11'`)
	t.Run("field OR tag", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// field OR field
	opt.Condition = MustParseExpr(`field_float1>1.0 OR field_float1<0.5`)
	t.Run("field OR field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// tag OR parent field
	opt.Condition = MustParseExpr(`tk3='value33' OR ((field_float1>1.0))`)
	t.Run("tag OR parent field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// parent field OR tag
	opt.Condition = MustParseExpr(`((field_float1>1.0)) OR tk3='value33'`)
	t.Run("parent field OR tag", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	// parent field OR parent field
	opt.Condition = MustParseExpr(`((field_float1>1.0)) OR ((field_float1<0.5))`)
	t.Run("parent field OR parent field", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value333",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value11\x00tk2\x00value2\x00tk3\x00value33",
		})
	})
}

func TestSearchSeries_With_TagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	f := func(name []byte, opts influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		dst := make([][]byte, 1)
		dst, err := idx.SearchSeries(dst[:0], name, opts, tr)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(dst, func(i, j int) bool {
			return string(dst[i]) < string(dst[j])
		})

		assert.Equal(t, len(dst), len(expectedSeriesKeys))
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
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})
	})

	t.Run("EQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3='value3'`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})
	})

	t.Run("NEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3!='value3'`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})
	})

	t.Run("AND", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value11') AND (tk3='value33')`), defaultTR, []string{
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})
	})

	t.Run("OR", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value11') OR (tk3='value33')`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})
	})

	t.Run("RegEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3=~/val.*3/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk3=~/val.*33/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk3=~/(val.*e3|val.*33)/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})
	})

	t.Run("RegNEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3!~/val.*33/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})
	})
}

func TestSearchTagValues_With_TagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	f := func(name []byte, tagKeys [][]byte, condition influxql.Expr, expectedTagValues [][]string) {
		tagValues, err := idx.SearchTagValues(name, tagKeys, condition)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, len(expectedTagValues), len(tagValues))

		for i := 0; i < len(tagValues); i++ {
			assert.Equal(t, len(expectedTagValues[i]), len(tagValues[i]))
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
		}})

		f([]byte("mn-1"), [][]byte{[]byte("tk3")}, nil, [][]string{{
			"value3",
			"value33",
			"value333",
		}})
	})

	t.Run("SingleKeyWithCond", func(t *testing.T) {
		f([]byte("mn-1"), [][]byte{[]byte("tk1")}, MustParseExpr(`tk3="value33"`), [][]string{{
			"value1",
			"value11",
		}})

		f([]byte("mn-1"), [][]byte{[]byte("tk2")}, MustParseExpr(`tk3="value33"`), [][]string{{
			"value2",
		}})

		f([]byte("mn-1"), [][]byte{[]byte("tk3")}, MustParseExpr(`tk1="value1"`), [][]string{{
			"value3",
			"value33",
			"value333",
		}})
	})

	t.Run("MultiKeysWithCond", func(t *testing.T) {
		f([]byte("mn-1"), [][]byte{[]byte("tk1"), []byte("tk2")}, MustParseExpr(`tk3="value33"`), [][]string{
			{
				"value1",
				"value11",
			},
			{
				"value2",
			},
		})

		f([]byte("mn-1"), [][]byte{[]byte("tk3"), []byte("tk2")}, MustParseExpr(`tk1="value1"`), [][]string{
			{
				"value3",
				"value33",
				"value333",
			},
			{
				"value2",
			},
		})
	})
}

func TestSeriesCardinality_With_TagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	f := func(name []byte, condition influxql.Expr, expectCardinality uint64) {
		count, err := idx.SeriesCardinality(name, condition, defaultTR)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, count, expectCardinality)
	}

	t.Run("cardinality from measurement", func(t *testing.T) {
		f([]byte("mn-1"), nil, 3)
	})

	t.Run("cardinality with condition", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr("tk3=value3"), 2)
	})
}

func TestSearchSeries_With_Multi_TagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray4)

	f := func(name []byte, opts influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		dst := make([][]byte, 1)
		dst, err := idx.SearchSeries(dst[:0], name, opts, tr)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(dst, func(i, j int) bool {
			return string(dst[i]) < string(dst[j])
		})

		assert.Equal(t, len(dst), len(expectedSeriesKeys))
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
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
			"mn-1,tk1=value11,tk2=value222",
		})
	})

	t.Run("EQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3='value3'`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})
	})

	t.Run("NEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3!='value3'`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("AND", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value11') AND (tk3='value33')`), defaultTR, []string{
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("OR", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`(tk1='value11') OR (tk3='value33')`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
			"mn-1,tk1=value11,tk2=value222",
		})
	})

	t.Run("RegEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3=~/val.*3/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk3=~/val.*33/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})

		f([]byte("mn-1"), MustParseExpr(`tk3=~/(val.*e3|val.*33)/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value22,tk3=value33",
		})
	})

	t.Run("RegNEQ", func(t *testing.T) {
		f([]byte("mn-1"), MustParseExpr(`tk3!~/val.*33/`), defaultTR, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})
	})
}

func TestSearchSeriesWithOrOpts(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray5)

	f := func(name []byte, opt *query.ProcessorOptions, tr TimeRange, expectedSeriesKeys []string, filtersIndex []int) {
		_, span := tracing.NewTrace("root")
		groups, _, err := idx.SearchSeriesWithOpts(span, name, opt, func(num int64) error {
			return nil
		}, nil)
		if err != nil {
			t.Fatal(err)
		}

		keys := make([]string, 0)
		filterCount := 0
		for _, group := range groups {
			for _, key := range group.SeriesKeys {
				keys = append(keys, string(key))
			}

			for _, filter := range group.Filters {
				if filter != nil {
					filterCount++
				}
			}
		}
		assert.Equal(t, filterCount, len(filtersIndex))

		sort.Strings(keys)
		sort.Strings(expectedSeriesKeys)
		assert.Equal(t, len(keys), len(expectedSeriesKeys))

		for i := 0; i < len(keys); i++ {
			assert.Equal(t, string(keys[i]), expectedSeriesKeys[i])
		}
	}

	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
		Condition: MustParseExpr(`tk2='k2'`),
	}
	t.Run("OR", func(t *testing.T) {
		opt.Condition = MustParseExpr(`tk3='value3' OR field_float1>1.0`)
		filterIndex := []int{2}
		f([]byte("mn-1"), opt, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
		}, filterIndex)

		opt.Condition = MustParseExpr(`tk3='value333' OR field_float1>1.0`)
		filterIndex = []int{0, 1, 2}
		f([]byte("mn-1"), opt, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
		}, filterIndex)

		opt.Condition = MustParseExpr(`field_float1>1.0 OR tk3='value3'`)
		filterIndex = []int{2}
		f([]byte("mn-1"), opt, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
		}, filterIndex)

		opt.Condition = MustParseExpr(`field_float1>1.0 OR tk3='value333'`)
		filterIndex = []int{0, 1, 2}
		f([]byte("mn-1"), opt, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
		}, filterIndex)

		opt.Condition = MustParseExpr(`field_float1>1.0 OR field_float1>2.0`)
		filterIndex = []int{0, 1, 2}
		f([]byte("mn-1"), opt, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
		}, filterIndex)

		opt.Condition = MustParseExpr(`tk3='value3' OR tk3='value333'`)
		filterIndex = nil
		f([]byte("mn-1"), opt, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value3",
		}, filterIndex)
	})
}

func BenchmarkWrite_With_TagArray(b *testing.B) {
	path := b.TempDir()
	_, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()

	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=[a,b,c,d,e,f,g]",
	}
	pts := make([]influx.Row, 0, len(keys))
	var tagsPool []influx.Tag
	for _, key := range keys {
		tagsPool = tagsPool[:0]
		pt := influx.Row{}
		n := strings.IndexByte(key, ',')
		pt.Name = key[:n]
		key = key[n+1:]
		tagsPool, _ = unmarshalTags(tagsPool, key, true)

		pt.Tags = make(influx.PointTags, len(tagsPool))
		for i, tag := range tagsPool {
			pt.Tags[i].Key = tag.Key
			pt.Tags[i].Value = tag.Value
			if strings.HasPrefix(pt.Tags[i].Value, "[") && strings.HasSuffix(pt.Tags[i].Value, "]") {
				pt.Tags[i].IsArray = true
			}
		}
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}

	mmPoints := &dictpool.Dict{}
	mmPoints.Set("mn-1", &pts)

	for i := 0; i < b.N; i++ {
		if err := idxBuilder.CreateIndexIfNotExists(mmPoints, true); err != nil {
			panic(err)
		}
	}
}

func BenchmarkWrite_With_NoTagArray(b *testing.B) {
	path := b.TempDir()
	_, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer func() {
		idxBuilder.Close()
	}()

	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=a",
		"mn-1,tk1=value1,tk2=value2,tk3=b",
		"mn-1,tk1=value1,tk2=value2,tk3=c",
		"mn-1,tk1=value1,tk2=value2,tk3=d",
		"mn-1,tk1=value1,tk2=value2,tk3=e",
		"mn-1,tk1=value1,tk2=value2,tk3=f",
		"mn-1,tk1=value1,tk2=value2,tk3=g",
	}
	pts := make([]influx.Row, 0, len(keys))
	var tagsPool []influx.Tag
	for _, key := range keys {
		tagsPool = tagsPool[:0]
		pt := influx.Row{}
		n := strings.IndexByte(key, ',')
		pt.Name = key[:n]
		key = key[n+1:]
		tagsPool, _ = unmarshalTags(tagsPool, key, true)

		pt.Tags = make(influx.PointTags, len(tagsPool))
		for i, tag := range tagsPool {
			pt.Tags[i].Key = tag.Key
			pt.Tags[i].Value = tag.Value
			if strings.HasPrefix(pt.Tags[i].Value, "[") && strings.HasSuffix(pt.Tags[i].Value, "]") {
				pt.Tags[i].IsArray = true
			}
		}
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}

	mmPoints := &dictpool.Dict{}
	mmPoints.Set("mn-1", &pts)

	for i := 0; i < b.N; i++ {
		if err := idxBuilder.CreateIndexIfNotExists(mmPoints, true); err != nil {
			panic(err)
		}
	}
}

func BenchmarkQuery_With_TagArray(b *testing.B) {
	path := b.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		_, _, err := idx.SearchSeriesWithOpts(nil, name, opt, nil, resourceallocator.DefaultSeriesAllocateFunc)
		if err != nil {
			b.Fatal(err)
		}
	}

	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
		Condition: MustParseExpr(`tk1='value1' AND tk3='value3'`),
	}

	for i := 0; i < b.N; i++ {
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
		})
	}
}

func BenchmarkQuery_With_NoTagArray(b *testing.B) {
	path := b.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	defer func() {
		idxBuilder.Close()
	}()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray2)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		_, _, err := idx.SearchSeriesWithOpts(nil, name, opt, nil, resourceallocator.DefaultSeriesAllocateFunc)
		if err != nil {
			b.Fatal(err)
		}
	}

	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
		Condition: MustParseExpr(`tk1='value1' AND tk3='value3'`),
	}

	for i := 0; i < b.N; i++ {
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
		})
	}
}

func searchSeriesWithTagArray1(idx *MergeSetIndex, series [][]byte, name []byte, condition influxql.Expr, tr TimeRange) ([][]byte, error) {
	tsids, err := idx.searchTSIDs(name, condition, tr)
	if err != nil {
		return series, err
	}
	if cap(series) >= len(tsids) {
		series = series[:len(tsids)]
	} else {
		series = series[:cap(series)]
		series = append(series, make([][]byte, len(tsids)-cap(series))...)
	}

	var combineSeriesKey []byte
	var combineKeys [][]byte
	var isExpectSeries []bool
	var index int
	for i := range tsids {
		combineKeys, _, isExpectSeries, err := idx.searchSeriesWithTagArray(tsids[i], combineKeys, nil, combineSeriesKey, isExpectSeries, condition, false)
		if err != nil {
			return nil, err
		}

		for j := range combineKeys {
			if !isExpectSeries[j] {
				continue
			}

			bufSeries := influx.GetBytesBuffer()
			bufSeries = influx.Parse2SeriesKey(combineKeys[j], bufSeries, true)
			if index >= len(tsids) {
				series = append(series, bufSeries)
			} else {
				series[index] = bufSeries
				index++
			}
		}
	}
	return series, nil
}

func BenchmarkShowSeries_With_TagArray1(b *testing.B) {
	path := b.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	series := make([][]byte, 1)
	for i := 0; i < b.N; i++ {
		searchSeriesWithTagArray1(idx.(*MergeSetIndex), series, []byte("mn-1"), nil, defaultTR)
	}
}

func searchSeriesWithTagArray2(idx *MergeSetIndex, series [][]byte, name []byte, condition influxql.Expr, tr TimeRange) ([][]byte, error) {
	tsids, err := idx.searchTSIDs(name, condition, tr)
	if err != nil {
		return series, err
	}

	var combineSeriesKey []byte
	var combineKeys [][]byte
	var isExpectSeries []bool
	for i := range tsids {
		combineKeys, _, isExpectSeries, err := idx.searchSeriesWithTagArray(tsids[i], combineKeys, nil, combineSeriesKey, isExpectSeries, condition, false)
		if err != nil {
			return nil, err
		}

		for j := range combineKeys {
			if !isExpectSeries[j] {
				continue
			}

			bufSeries := influx.GetBytesBuffer()
			bufSeries = influx.Parse2SeriesKey(combineKeys[j], bufSeries, true)
			series = append(series, bufSeries)
		}
	}
	return series, nil
}

func BenchmarkShowSeries_With_TagArray2(b *testing.B) {
	path := b.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	series := make([][]byte, 1)
	for i := 0; i < b.N; i++ {
		searchSeriesWithTagArray2(idx.(*MergeSetIndex), series, []byte("mn-1"), nil, defaultTR)
	}
}

func TestSearchSeries_With_TagArray_By_MultiTagAnd(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	opt := &query.ProcessorOptions{
		StartTime: DefaultTR.Min,
		EndTime:   DefaultTR.Max,
	}

	f := func(name []byte, expr influxql.Expr, tr TimeRange, expectedSeriesKeys []string) {
		index := idx.(*MergeSetIndex)
		is := index.getIndexSearch()
		defer index.putIndexSearch(is)

		var tsids *uint64set.Set
		iterator, err := is.seriesByExprIterator(name, expr, &tsids, false)
		if err != nil {
			t.Fatal(err)
		}

		ids := iterator.Ids().AppendTo(nil)

		keys := make([]string, 0, len(ids))
		var seriesKeys [][]byte
		var combineSeriesKey []byte
		var isExpectSeries []bool
		for _, id := range ids {
			seriesKeys, _, isExpectSeries, err := index.searchSeriesWithTagArray(id, seriesKeys, nil, combineSeriesKey, isExpectSeries, opt.Condition, false)
			if err != nil {
				t.Fatal(err)
			}
			for i := range seriesKeys {
				if !isExpectSeries[i] {
					continue
				}

				var seriesKey []byte
				seriesKey = append(seriesKey, seriesKeys[i]...)
				keys = append(keys, string(influx.Parse2SeriesKey(seriesKey, nil, true)))
			}

		}
		sort.Strings(keys)
		assert.Equal(t, len(keys), len(expectedSeriesKeys))

		for i := 0; i < len(keys); i++ {
			assert.Equal(t, keys[i], expectedSeriesKeys[i])
		}
	}

	// tag AND
	opt.Condition = MustParseExpr(`tk1='value1' AND tk3='value33'`)
	t.Run("tag AND 1", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
		})
	})

	preMaxIndexMetrics := maxIndexMetrics
	maxIndexMetrics = 0
	opt.Condition = MustParseExpr(`tk1='value1' AND tk3='value33'`)
	t.Run("tag AND 2", func(t *testing.T) {
		f([]byte("mn-1"), opt.Condition, defaultTR, []string{
			"mn-1,tk1\x00value1\x00tk2\x00value2\x00tk3\x00value33",
		})
	})
	maxIndexMetrics = preMaxIndexMetrics
}

func BenchmarkAnalyzeTagSets(b *testing.B) {
	var buildTags = func() []influx.Tag {
		return []influx.Tag{
			{Key: "foo0", Value: "aaa", IsArray: false},
			{Key: "cmpt", Value: "[126165,126395,126088,126156,126411,179380,126396,179381,125837,125526,224108,125528,126202,126203,125529,126094,126104,179379,125517,126420,325357]", IsArray: true},
			{Key: "component", Value: "[autorecovery-had,cps-haagent,upg-client,cascaded-nova-compute,wrap-server,platform-pkgs,network-client,om-commons,neutron-dvr-compute-agent,neutron-dhcp-agent,pecado-local-controller,neutron-openvswitch-agent,ceilometer-agent-hardware,ceilometer-agent-compute,neutron-metadata-agent,cps-monitor,cps-client,platform-commons,Block-Service,openstack-omm-agent,Server-VM]", IsArray: true},
			{Key: "service", Value: "[ECS,IaaSDeploy,IaaSUpgrade,ECS,IaaSDeploy,IaaSDeploy,IaaSDeploy,IaaSDeploy,VPC,VPC,VPC,VPC,IaaSCommonServiceComponents,IaaSCommonServiceComponents,VPC,IaaSDeploy,IaaSDeploy,IaaSDeploy,EVS,IaaSDeploy,Server]", IsArray: true},
			{Key: "foo3", Value: "aaa", IsArray: false},
		}
	}

	b.Run("new", func(b *testing.B) {
		dstTagSets := &tagSets{}
		tags := buildTags()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dstTagSets.reset()
			_ = AnalyzeTagSets(dstTagSets, tags)
		}
	})

	b.Run("old", func(b *testing.B) {
		dstTagSets := &tagSets{}
		tags := buildTags()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dstTagSets.reset()
			_ = analyzeTagSetsOld(dstTagSets, tags)
		}
	})
}

func TestAnalyzeTagSets(t *testing.T) {
	var buildTags = func() []influx.Tag {
		return []influx.Tag{
			{Key: "foo0", Value: "aaa", IsArray: false},
			{Key: "cmpt", Value: "[126165,126395,126088,126156,126411,179380,126396,179381,125837,125526,224108,125528,126202,126203,125529,126094,126104,179379,125517,126420,325357]", IsArray: true},
			{Key: "component", Value: "[autorecovery-had,cps-haagent,upg-client,cascaded-nova-compute,wrap-server,platform-pkgs,network-client,om-commons,neutron-dvr-compute-agent,neutron-dhcp-agent,pecado-local-controller,neutron-openvswitch-agent,ceilometer-agent-hardware,ceilometer-agent-compute,neutron-metadata-agent,cps-monitor,cps-client,platform-commons,Block-Service,openstack-omm-agent,Server-VM]", IsArray: true},
			{Key: "service", Value: "[ECS,IaaSDeploy,IaaSUpgrade,ECS,IaaSDeploy,IaaSDeploy,IaaSDeploy,IaaSDeploy,VPC,VPC,VPC,VPC,IaaSCommonServiceComponents,IaaSCommonServiceComponents,VPC,IaaSDeploy,IaaSDeploy,IaaSDeploy,EVS,IaaSDeploy,Server]", IsArray: true},
			{Key: "foo3", Value: "aaa", IsArray: false},
		}
	}

	dstTagSets1 := &tagSets{}
	dstTagSets2 := &tagSets{}

	require.NoError(t, AnalyzeTagSets(dstTagSets1, buildTags()))
	require.NoError(t, analyzeTagSetsOld(dstTagSets2, buildTags()))

	require.Equal(t, dstTagSets1, dstTagSets2)
}

func analyzeTagSetsOld(dstTagSets *tagSets, tags []influx.Tag) error {
	var arrayLen int
	for i := range tags {
		if tags[i].IsArray {
			tagCount := strings.Count(tags[i].Value, ",") + 1
			if arrayLen == 0 {
				arrayLen = tagCount
			}

			if arrayLen != tagCount {
				return errno.NewError(errno.ErrorTagArrayFormat)
			}
		}
	}

	dstTagSets.resize(arrayLen, len(tags))

	for cIndex := range tags {
		if tags[cIndex].IsArray {
			values := strings.Split(tags[cIndex].Value[1:len(tags[cIndex].Value)-1], ",")
			for rIndex := range values {
				dstTagSets.tagsArray[rIndex][cIndex].Key = tags[cIndex].Key
				dstTagSets.tagsArray[rIndex][cIndex].Value = values[rIndex]
			}
		} else {
			for rIndex := 0; rIndex < arrayLen; rIndex++ {
				dstTagSets.tagsArray[rIndex][cIndex].Key = tags[cIndex].Key
				dstTagSets.tagsArray[rIndex][cIndex].Value = tags[cIndex].Value
			}
		}
	}
	return nil
}

func TestSearchSeriesWithPromOpt(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path, config.TSSTORE)
	idxBuilder.EnableTagArray = true
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithForProm)

	f := func(name []byte, opt *query.ProcessorOptions, tr TimeRange, expectedSeriesKeys []string, filtersIndex []int) {
		_, span := tracing.NewTrace("root")
		groups, _, err := idx.SearchSeriesWithOpts(span, name, opt, func(num int64) error {
			return nil
		}, nil)
		if err != nil {
			t.Fatal(err)
		}

		keys := make([]string, 0)
		filterCount := 0
		for _, group := range groups {
			keys = append(keys, string(group.key))

			for _, filter := range group.Filters {
				if filter != nil {
					filterCount++
				}
			}
		}
		assert.Equal(t, filterCount, len(filtersIndex))

		sort.Strings(keys)
		sort.Strings(expectedSeriesKeys)
		assert.Equal(t, len(keys), len(expectedSeriesKeys))

		for i := 0; i < len(keys); i++ {
			assert.Equal(t, string(keys[i]), expectedSeriesKeys[i])
		}
	}

	opt1 := &query.ProcessorOptions{
		StartTime:      DefaultTR.Min,
		EndTime:        DefaultTR.Max,
		PromQuery:      true,
		GroupByAllDims: true,
	}
	opt2 := &query.ProcessorOptions{
		StartTime:  DefaultTR.Min,
		EndTime:    DefaultTR.Max,
		PromQuery:  true,
		Without:    true,
		Dimensions: []string{"tk1"},
	}
	opt3 := &query.ProcessorOptions{
		StartTime:  DefaultTR.Min,
		EndTime:    DefaultTR.Max,
		PromQuery:  true,
		Dimensions: []string{"tk1"},
	}
	t.Run("searchSeriesWithPromOpt", func(t *testing.T) {
		var filterIndex []int = nil
		f([]byte("mn-1"), opt1, defaultTR, []string{
			"tk1\x00value1_1\x00tk2\x00value2\x00tk3\x00value3",
			"tk1\x00value1_2\x00tk2\x00value2\x00tk3\x00value3",
		}, filterIndex)

		f([]byte("mn-1"), opt2, defaultTR, []string{
			"tk2\x00value2\x00tk3\x00value3",
		}, filterIndex)

		f([]byte("mn-1"), opt3, defaultTR, []string{
			"tk1\x00value1_1",
			"tk1\x00value1_2",
		}, filterIndex)
	})
}
