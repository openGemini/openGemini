/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
package httpd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
	"github.com/stretchr/testify/assert"
)

func TestGetAnalysisResults(t *testing.T) {
	resp := &Response{
		Results: []*query.Result{
			&query.Result{
				Series: models.Rows{
					&models.Row{
						Name:    "mst",
						Tags:    nil,
						Columns: []string{"key", "value"},
						Values: [][]interface{}{
							{111, "author", "mao"},
						},
					},
				},
			},
		},
	}
	r := strings.NewReader("select count(*) from mst")
	p := influxql.NewParser(r)
	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	q, err := YyParser.GetQuery()
	if err != nil {
		t.Error(err)
	}

	re := GetAnalysisResults(resp, q)
	assert.Equal(t, 2, len(re))
	assert.Equal(t, 3, len(re[0][0]))
}

func TestGetAnalysisResultsByTime(t *testing.T) {
	resp := &Response{
		Results: []*query.Result{
			&query.Result{
				Series: models.Rows{
					&models.Row{
						Name:    "mst",
						Tags:    nil,
						Columns: []string{"time", "key", "value"},
						Values: [][]interface{}{
							{111, "author", "mao"},
						},
					},
				},
			},
		},
	}
	r := strings.NewReader("select count(*) from mst")
	p := influxql.NewParser(r)
	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	q, err := YyParser.GetQuery()
	if err != nil {
		t.Error(err)
	}

	re := GetAnalysisResults(resp, q)
	assert.Equal(t, 2, len(re))
	assert.Equal(t, 3, len(re[0][0]))
}

func TestGetKeys(t *testing.T) {
	keysMap := map[string]bool{
		"test2": true,
		"test4": true,
		"test1": true,
		"test3": true,
		"test5": true,
	}

	keys := getKeys(keysMap)
	for i, v := range keys {
		assert.Equal(t, fmt.Sprintf("%v%v", "test", i+1), v)
	}
	assert.Equal(t, len(keysMap), cap(keys))
}

func TestFieldScopesSlice2Map(t *testing.T) {
	mfs1 := marshalFieldScope{"test1", 1, 2}
	mfs2 := marshalFieldScope{"test2", 1, 2}
	mfs3 := marshalFieldScope{"test3", 1, 2}

	fieldScopes := []marshalFieldScope{mfs1, mfs2, mfs3}
	fieldScopesMap := fieldScopesSlice2Map(fieldScopes)
	assert.Equal(t, len(fieldScopes), len(fieldScopesMap))
}

func TestExtractFieldFragments(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}

	content := map[string]interface{}{}
	content["field1"] = "this is json"
	content["field2"] = "json filed"
	content["json"] = "this is filed"

	fieldSlice := []string{"field1", "field2", "json"}
	var fieldScopes []marshalFieldScope
	for _, key := range fieldSlice {
		fieldScopes = h.appendFieldScopes(fieldScopes, key, content[key])
	}
	b, err := json2.MarshalToString(content)
	assert.Equal(t, nil, err)

	contentTokenFinder := tokenizer.NewSimpleTokenFinder(tokenizer.CONTENT_SPLIT_TABLE)

	// Test only field1 has the highlighted words
	highlightWords := map[string]map[string][]int{"json": {"field1": {influxql.MATCHPHRASE}}}
	fragments := extractFieldFragments(b, highlightWords, contentTokenFinder, fieldScopes)
	for _, fragment := range fragments {
		assert.Equal(t, "json", b[fragment.Offset:fragment.Offset+fragment.Length])
	}
	assert.Equal(t, 1, len(fragments))

	// Test that all three fields have this highlighted word
	highlightWords = map[string]map[string][]int{"json": {
		"field1": {influxql.MATCHPHRASE},
		"field2": {influxql.MATCHPHRASE},
		"json":   {influxql.MATCHPHRASE},
	}}
	fragments = extractFieldFragments(b, highlightWords, contentTokenFinder, fieldScopes)
	for _, fragment := range fragments {
		assert.Equal(t, "json", b[fragment.Offset:fragment.Offset+fragment.Length])
	}
	assert.Equal(t, 2, len(fragments))

	// The full text of the test has this highlight word
	highlightWords = map[string]map[string][]int{"json": {logparser.DefaultFieldForFullText: {influxql.MATCHPHRASE}}}
	fragments = extractFieldFragments(b, highlightWords, contentTokenFinder, fieldScopes)
	for _, fragment := range fragments {
		assert.Equal(t, "json", b[fragment.Offset:fragment.Offset+fragment.Length])
	}
	assert.Equal(t, 3, len(fragments))
}

func TestIsHighlight(t *testing.T) {
	res := isHighlight(influxql.EQ, "12.2", "12.2")
	assert.Equal(t, true, res)

	res = isHighlight(influxql.LTE, "12.2", "12.2")
	assert.Equal(t, true, res)

	res = isHighlight(influxql.LT, "12.2", "12.2")
	assert.Equal(t, false, res)

	res = isHighlight(influxql.GT, "12.2", "12.1")
	assert.Equal(t, true, res)

	res = isHighlight(influxql.GTE, "12.2", "12.5")
	assert.Equal(t, false, res)
}

func TestExtractNumericalFragments(t *testing.T) {
	contentTokenFinder := tokenizer.NewSimpleTokenFinder(tokenizer.CONTENT_SPLIT_TABLE)

	// Tests the numeric types that match the conditions
	highlightWords := map[string][]int{"50": {influxql.GT}}
	fragments := extractFragments("85", highlightWords, contentTokenFinder)
	assert.Equal(t, 1, len(fragments))
	assert.Equal(t, 0, fragments[0].Offset)
	assert.Equal(t, len("85"), fragments[0].Length)

	// Tests for numeric types that do not match a condition
	highlightWords = map[string][]int{"50": {influxql.LT}}
	fragments = extractFragments("85", highlightWords, contentTokenFinder)
	assert.Equal(t, 0, len(fragments))

	// Test part of the condition meets the highlight
	highlightWords = map[string][]int{"50": {influxql.LT}, "85": {influxql.MATCHPHRASE}}
	fragments = extractFragments("85", highlightWords, contentTokenFinder)
	assert.Equal(t, 1, len(fragments))
	assert.Equal(t, 0, fragments[0].Offset)
	assert.Equal(t, len("85"), fragments[0].Length)

	// Test multi-condition matching highlighting
	highlightWords = map[string][]int{"50": {influxql.GT}, "85": {influxql.MATCHPHRASE}}
	fragments = extractFragments("85", highlightWords, contentTokenFinder)
	fragments = mergeFragments(fragments)
	assert.Equal(t, 1, len(fragments))
	assert.Equal(t, 0, fragments[0].Offset)
	assert.Equal(t, len("85"), fragments[0].Length)
}

func TestGetHighlightFragments(t *testing.T) {
	h := &Handler{
		Logger: logger.NewLogger(errno.ModuleLogStore),
	}

	rec := map[string]interface{}{}
	highlightWords := map[string]map[string][]int{}
	var fieldScopes []marshalFieldScope

	key := "content"
	val := "<val:abc>"
	content := map[string]interface{}{key: val}
	rec[Content] = content
	fieldScopes = h.appendFieldScopes(fieldScopes, key, val)
	highlight1 := h.getHighlightFragments(rec, highlightWords, fieldScopes)
	assert.Equal(t, `{"content":"<val:abc>"}`, highlight1[Content].([]HighlightFragment)[0].Fragment)
	assert.Equal(t, false, highlight1[Content].([]HighlightFragment)[0].Highlight)

	key = "content"
	val = "\"\",\n"
	content = map[string]interface{}{key: val}
	rec[Content] = content
	fieldScopes = fieldScopes[:0]
	fieldScopes = h.appendFieldScopes(fieldScopes, key, val)
	highlight2 := h.getHighlightFragments(rec, highlightWords, fieldScopes)
	assert.Equal(t, `{"content":"\"\",\n"}`, highlight2[Content].([]HighlightFragment)[0].Fragment)
	assert.Equal(t, false, highlight2[Content].([]HighlightFragment)[0].Highlight)
}
