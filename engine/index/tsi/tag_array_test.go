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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func unmarshalTag(tag *influx.Tag, s string, noEscapeChars bool) error {
	tag.Reset()
	n := nextUnescapedChar(s, '=', noEscapeChars, false)
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
		n := nextUnescapedChar(s, ',', noEscapeChars, true)
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

func nextUnescapedChar(s string, ch byte, noEscapeChars, tagParse bool) int {
	if noEscapeChars {
		// eg,tk1=value1,tk2=[value2,value22],tk3=value3
		if config.EnableTagArray && tagParse {
			return nextUnescapedCharForTagArray(s, ch)
		}
		return strings.IndexByte(s, ch)
	}

	sOrig := s
again:
	// eg,tk1=value1,tk2=[value2,value22],tk3=value3
	var n int
	if config.EnableTagArray && tagParse {
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
		"mn-1,tk1=[value1,value11],tk2=[value2,value22],tk3=[value3,value33]",
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
			if strings.HasPrefix(pt.Tags[i].Value, "[") && strings.HasSuffix(pt.Tags[i].Value, "]") {
				pt.Tags[i].IsArray = true
				pt.HasTagArray = true
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
	if err := iBuilder.CreateIndexIfNotExists(mmPoints); err != nil {
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
				pt.HasTagArray = true
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
	err := iBuilder.CreateIndexIfNotExists(mmPoints)

	if err == nil {
		panic("create index failed")
	}

	iBuilder.Close()
	iBuilder.Open()
}

func TestWriteTagArray_Success(t *testing.T) {
	config.EnableTagArray = true
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)
}

func TestWriteTagArray_Fail1(t *testing.T) {
	config.EnableTagArray = true
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()
	CreateIndexByPts_TagArray_Error(idxBuilder, idx, generateKeyWithTagArray3)
}

func TestWriteTagArray_Fai2(t *testing.T) {
	config.EnableTagArray = true
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()
	CreateIndexByPts_TagArray_Error(idxBuilder, idx, generateKeyWithTagArray4)
}

func TestSearchSeriesWithOpts_TagArray(t *testing.T) {
	config.EnableTagArray = true
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		groups, err := idx.SearchSeriesWithOpts(nil, name, opt, nil)
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
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})
	})

	t.Run("tag_and_field", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value11' AND field_float1>1.0`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})

		opt.Condition = MustParseExpr(`field_float1>1.0 AND tk3='value3'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})

		// Test condition with or field filter
		opt.Condition = MustParseExpr(`tk1='value1' OR field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})

		// Test condition from tag array
		opt.Condition = MustParseExpr(`tk3='value3' AND field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})
	})

	t.Run("tag_and_tag", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value1' AND tk3='value3'`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
		})

		opt.Condition = MustParseExpr(`tk1='value1' OR tk3='value3'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})

		opt.Condition = MustParseExpr(`tk3!='value3''`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})
	})
}

func TestSearchSeriesWithOpts_NoTagArray(t *testing.T) {
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer idxBuilder.Close()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray2)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		groups, err := idx.SearchSeriesWithOpts(nil, name, opt, nil)
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
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})

		opt.Condition = MustParseExpr(`field_float1>1.0 AND tk1='value11'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})

		// Test condition with or field filter
		opt.Condition = MustParseExpr(`tk1='value1' OR field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
		})

		// Test condition from tag array
		opt.Condition = MustParseExpr(`tk3='value3' AND field_float1>1.0`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})
	})

	t.Run("tag_and_tag", func(t *testing.T) {
		opt := &query.ProcessorOptions{
			StartTime: DefaultTR.Min,
			EndTime:   DefaultTR.Max,
			Condition: MustParseExpr(`tk1='value1' AND tk3='value3'`),
		}
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
		})

		opt.Condition = MustParseExpr(`tk1='value1' OR tk3='value3'`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value3",
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value3",
		})

		opt.Condition = MustParseExpr(`tk3!='value3''`)
		f([]byte("mn-1"), opt, []string{
			"mn-1,tk1=value1,tk2=value2,tk3=value33",
			"mn-1,tk1=value1,tk2=value2,tk3=value333",
			"mn-1,tk1=value11,tk2=value2,tk3=value33",
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
	config.EnableTagArray = true
	path := t.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()
	mergeIndex, _ := idx.(*MergeSetIndex)

	src := []byte{1, 2}
	var isExpectSeries []bool
	_, _, err := mergeIndex.searchSeriesWithTagArray(1, nil, src, isExpectSeries, nil)
	if err == nil {
		t.Fatal("expect searchSeriesWithTagArray fail, but success")
	}

	condition := MustParseExpr(`tk1='value11'`)
	src = []byte{0, 0, 0, 0, 0, 2, 0, 49, 0, 0, 0, 49, 0, 8, 99, 112, 117, 95, 48, 48, 48, 48, 0, 3, 0, 3, 116, 107, 49, 0, 6, 118, 97, 108, 117, 101, 49, 0, 3, 116, 107, 50, 0, 6, 118, 97, 108, 117, 101, 50, 0, 3, 116, 107, 51, 0, 6,
		0, 49, 0, 0, 0, 49, 0, 8, 99, 112, 117, 95, 48, 48, 48, 48, 0, 3, 0, 3, 116, 107, 49, 0, 6, 118, 97, 108, 117, 101, 49, 0, 3, 116, 107, 50, 0, 6, 118, 97, 108, 117, 101, 50, 0, 3, 116, 107, 51, 0, 6}
	_, _, err = mergeIndex.searchSeriesWithTagArray(1, nil, src, isExpectSeries, condition)
	if err == nil {
		t.Fatal("expect searchSeriesWithTagArray fail, but success")
	}
}

func BenchmarkWrite_With_TagArray(b *testing.B) {
	config.EnableTagArray = true
	path := b.TempDir()
	_, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()

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
				pt.HasTagArray = true
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
		if err := idxBuilder.CreateIndexIfNotExists(mmPoints); err != nil {
			panic(err)
		}
	}
}

func BenchmarkWrite_With_NoTagArray(b *testing.B) {
	path := b.TempDir()
	_, idxBuilder := getTestIndexAndBuilder(path)
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
				pt.HasTagArray = true
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
		if err := idxBuilder.CreateIndexIfNotExists(mmPoints); err != nil {
			panic(err)
		}
	}
}

func BenchmarkQuery_With_TagArray(b *testing.B) {
	config.EnableTagArray = true
	path := b.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray1)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		_, err := idx.SearchSeriesWithOpts(nil, name, opt, nil)
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
	config.EnableTagArray = true
	path := b.TempDir()
	idx, idxBuilder := getTestIndexAndBuilder(path)
	defer func() {
		config.EnableTagArray = false
		idxBuilder.Close()
	}()
	CreateIndexByPts_TagArray(idxBuilder, idx, generateKeyWithTagArray2)

	f := func(name []byte, opt *query.ProcessorOptions, expectedSeriesKeys []string) {
		_, err := idx.SearchSeriesWithOpts(nil, name, opt, nil)
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
