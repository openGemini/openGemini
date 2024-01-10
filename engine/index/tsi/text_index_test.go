/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"os"
	"testing"

	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

var TEXT_PATH string = "/tmp/textindex"
var CLV_PATH string = TEXT_PATH + "/clv"
var LOCK_PATH string = CLV_PATH + "/clv.lock"
var MST_NAME string = "logmst"

func buildRow(seriesId uint64, timestamp int64, textFiledValue string) influx.Row {
	// construct tag
	tags := []influx.Tag{}
	tags = append(tags, influx.Tag{Key: "clientip", Value: "127.0.0.2"})

	// construct field
	var fields []influx.Field
	fields = append(fields, influx.Field{
		Key:      "request",
		StrValue: textFiledValue,
		Type:     influx.Field_Type_String,
	})

	// construct indexOptions
	indexOptions := influx.IndexOptions{{
		IndexList: []uint16{1},
		Oid:       uint32(Text),
	}}

	r := &influx.Row{}
	r.Timestamp = timestamp
	r.SeriesId = seriesId
	r.Name = MST_NAME
	r.Tags = tags
	r.Fields = fields
	r.IndexOptions = indexOptions

	return *r
}

func CreateTestRows() []influx.Row {
	rows := []influx.Row{}
	rows = append(rows, buildRow(101, 100000, "GET /french/index.html HTTP/1.0"))
	rows = append(rows, buildRow(101, 100001, "GET /french/competition/flashed_stage1.htm HTTP/1.0"))
	rows = append(rows, buildRow(102, 100002, "GET /french/nav_top_inet.html HTTP/1.0"))
	rows = append(rows, buildRow(102, 100003, "GET /images/logo_cfo.gif HTTP/1.1"))
	rows = append(rows, buildRow(103, 100004, "GET /english/images/nav_news_off.gif HTTP/1.1"))
	rows = append(rows, buildRow(103, 100005, "GET /english/images/hm_official.gif HTTP/1.1"))
	rows = append(rows, buildRow(104, 100006, "GET /french/images/comp_bg2_hm.gif HTTP/1.0"))
	rows = append(rows, buildRow(104, 100007, "GET /french/nav_inet.html HTTP/1.0"))
	rows = append(rows, buildRow(105, 100008, "GET /images/tck_pkit_fx_b.jpg HTTP/1.1"))
	rows = append(rows, buildRow(105, 100009, "GET /french/splash_inet.html HTTP/1.0"))
	return rows
}

func insertClvFullTextIndex(opts *Options) (*TextIndex, error) {
	rows := CreateTestRows()
	textIndex, err := NewTextIndex(opts)
	if err != nil {
		return nil, err
	}

	err = TextOpen(textIndex)
	if err != nil {
		return nil, fmt.Errorf("TextOpen Failed, err:%v", err)
	}

	for i := 0; i < len(rows); i++ {
		_, err = TextInsert(textIndex, nil, []byte(MST_NAME), &rows[i])
		if err != nil {
			return nil, fmt.Errorf("insert textindex Failed, rows:%d-%v, err:%v", i, rows[i], err)
		}
	}
	TextFlush(textIndex)
	err = TextClose(textIndex)
	if err != nil {
		return nil, fmt.Errorf("TextClose Failed, err:%v", err)
	}

	return textIndex, nil
}

func TestTextIndexInsert(t *testing.T) {
	os.RemoveAll(TEXT_PATH)
	defer func() {
		_ = os.RemoveAll(TEXT_PATH)
	}()
	opts := Options{
		path: CLV_PATH,
		lock: &LOCK_PATH,
	}
	_, err := insertClvFullTextIndex(&opts)
	if err != nil {
		t.Fatalf("insert fulltext index Failed, err:%v", err)
	}
}

func getSearchExpr() influxql.Expr {
	dExpr0 := &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}}
	dExpr1 := &influxql.BinaryExpr{Op: influxql.LT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 5000}}
	dExpr2 := &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}
	tExpr0 := &influxql.BinaryExpr{Op: influxql.MATCH, LHS: &influxql.VarRef{Val: "request"}, RHS: &influxql.StringLiteral{Val: "french"}}
	tExpr1 := &influxql.BinaryExpr{Op: influxql.MATCHPHRASE, LHS: &influxql.VarRef{Val: "request"}, RHS: &influxql.StringLiteral{Val: "flashed_stage1.htm HTTP"}}
	tExpr2 := &influxql.BinaryExpr{Op: influxql.LIKE, LHS: &influxql.VarRef{Val: "request"}, RHS: &influxql.StringLiteral{Val: "competit%"}}
	tExpr3 := &influxql.BinaryExpr{Op: influxql.LIKE, LHS: &influxql.VarRef{Val: "request"}, RHS: &influxql.VarRef{Val: "fren%"}}
	/*
	   condition:
	                                AND
	                              /     \
	                         (OR)        AND
	                        /   \      /      \
	                     AND  tExpr0 AND        OR
	                   /   \        /  \       /    \
	             dExpr0 dExpr1 tExpr1 tExpr3 tExpr2 dExpr2
	*/
	searchExpr := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.ParenExpr{
			Expr: &influxql.BinaryExpr{
				Op:  influxql.OR,
				LHS: &influxql.BinaryExpr{Op: influxql.AND, LHS: dExpr0, RHS: dExpr1},
				RHS: tExpr0}},
		RHS: &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: &influxql.BinaryExpr{Op: influxql.AND, LHS: tExpr1, RHS: tExpr3},
			RHS: &influxql.BinaryExpr{Op: influxql.OR, LHS: tExpr2, RHS: dExpr2}},
	}

	return searchExpr
}

func getTagsetForTextSearchTest() *TagSetInfo {
	tagSet := &TagSetInfo{
		ref:        0,
		key:        make([]byte, 0),
		IDs:        make([]uint64, 0),
		Filters:    make([]influxql.Expr, 0),
		RowFilters: clv.NewRowFilters(),
		SeriesKeys: make([][]byte, 0),
		TagsVec:    make([]influx.PointTags, 0),
	}

	tagSet.Append(101, []byte("clientip"), getSearchExpr(), nil, nil)
	tagSet.Append(102, []byte("clientip"), getSearchExpr(), nil, nil)
	tagSet.Append(103, []byte("clientip"), getSearchExpr(), nil, nil)
	tagSet.Append(104, []byte("clientip"), getSearchExpr(), nil, nil)
	tagSet.Append(105, []byte("clientip"), nil, nil, nil)
	tagSet.Append(106, []byte("clientip"), nil, nil, nil) // seriesId=106 not existed.

	return tagSet
}

func TestTextIndexSearch(t *testing.T) {
	os.RemoveAll(TEXT_PATH)
	defer func() {
		_ = os.RemoveAll(TEXT_PATH)
	}()
	opts := Options{
		path: CLV_PATH,
		lock: &LOCK_PATH,
	}
	textIndex, err := insertClvFullTextIndex(&opts)
	if err != nil {
		t.Fatalf("insert fulltext index Failed, err:%v", err)
	}

	err = TextOpen(textIndex)
	if err != nil {
		t.Fatalf("TextOpen Failed, err:%v", err)
	}

	expr := getSearchExpr()
	opt := &query.ProcessorOptions{Condition: expr}
	tagSet := getTagsetForTextSearchTest()
	group, _, err0 := TextScan(textIndex, nil, nil, []byte(MST_NAME), opt, nil, GroupSeries{tagSet})
	if err0 != nil {
		t.Fatalf("TextScan Failed, err:%v", err0)
	}
	fmt.Printf("result: %+v", group)
}
