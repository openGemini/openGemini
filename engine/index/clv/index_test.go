// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package clv

import (
	"os"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type searchTestTag struct {
	op       QueryType
	queryStr string
	sids     []uint64
	result   InvertIndex
}

var glogStrs = []Logs{
	{[]byte("GET /english/nav_top_inet.html HTTP/1.00"), 10, 1000},
	{[]byte("GET /english/nav_inet.html HTTP/1.0"), 11, 2000},
	{[]byte("GET /english/splash_inet.html HTTP/1.0"), 10, 3000},
	{[]byte("GET /english/news/1605corr.htm HTTP/1.0"), 11, 4000},
	{[]byte("GET /images/home_bg_stars.gif HTTP/1.1"), 10, 5000},
	{[]byte("GET /french/images/nav_news_off.gif HTTP/1.0"), 11, 6000},
	{[]byte("GET /french/images/nav_store_off.gif HTTP/1.0"), 12, 7000},
}

var analyzerStr = []string{
	"GET english",
	"GET french images",
	"english nav_top_inet.html",
	"HTTP 1.0",
}

var gtestOps = []searchTestTag{
	{Match, "nav_top_inet.html", []uint64{10, 11, 12}, InvertIndex{
		invertStates: map[uint64]*InvertStates{
			10: &InvertStates{
				invertState: []InvertState{{rowId: 1000, position: 2, filter: nil}},
				sid:         10},
		},
	}},
	{Match_Phrase, "GET /english/nav_top_inet.html", []uint64{10, 11, 12}, InvertIndex{
		invertStates: map[uint64]*InvertStates{
			10: &InvertStates{
				invertState: []InvertState{{rowId: 1000, position: 0, filter: nil}},
				sid:         10},
		},
	}},
	{Fuzzy, "nav_top_%.html", []uint64{10, 11, 12}, InvertIndex{
		invertStates: map[uint64]*InvertStates{
			10: &InvertStates{
				invertState: []InvertState{{rowId: 1000, position: 2, filter: nil}},
				sid:         10},
		},
	}},
}

func SearchTest(t *testing.T, index *TokenIndex, testOps []searchTestTag) {
	for i := 0; i < len(testOps); i++ {
		invert, err := index.Search(testOps[i].op, testOps[i].queryStr, testOps[i].sids)
		if err != nil {
			t.Fatalf("search document failed, err:%v", err)
		}
		if !reflect.DeepEqual(testOps[i].result.invertStates, invert.invertStates) {
			t.Fatalf("search [%d, %s] reuslt wrong, exp:%v get:%v",
				testOps[i].op, testOps[i].queryStr,
				testOps[i].result.invertStates[10], invert.invertStates[10])
		}
	}
}

var CLV_PATH string = "/tmp/clv/"
var CLV_INDEX_PATH string = CLV_PATH + "index/"
var CLV_ANALYZER_PATH string = CLV_PATH + "dictionary/"
var CLV_LOCK_PATH string = CLV_INDEX_PATH + "lock"

func AddDocumentForTest(index *TokenIndex, logData []Logs) error {
	for i := 0; i < len(logData); i++ {
		err := index.AddDocument(string(logData[i].log), logData[i].sid, logData[i].rowId)
		if err != nil {
			return err
		}
	}
	// flush
	index.Flush()

	return nil
}

func TestDefaultTokenizerToFullIndex(t *testing.T) {
	os.RemoveAll(CLV_PATH)
	defer func() {
		_ = os.RemoveAll(CLV_PATH)
	}()

	opt := &Options{
		Path:        CLV_INDEX_PATH,
		Measurement: "logMst",
		Field:       "request",
		Lock:        &CLV_LOCK_PATH,
	}
	tokenIndex, err := NewTokenIndex(opt)
	if err != nil || tokenIndex == nil {
		t.Fatalf("create token index failed, err:%v", err)
	}
	// add document
	err = AddDocumentForTest(tokenIndex, glogStrs)
	if err != nil {
		t.Fatalf("add document failed, err:%v", err)
	}

	SearchTest(t, tokenIndex, gtestOps)
	tokenIndex.Close()
}

func buildLearningAnalyzer() *Analyzer {
	a := newAnalyzer(CLV_ANALYZER_PATH, "logmst", "content", 0)

	for i := 0; i < len(analyzerStr); i++ {
		a.InsertToDictionary(analyzerStr[i])
	}
	err := a.AssignId()
	if err != nil {
		return nil
	}
	return a
}

func TestLearningTokenizerToFullIndex(t *testing.T) {
	os.RemoveAll(CLV_PATH)
	defer func() {
		_ = os.RemoveAll(CLV_PATH)
	}()

	opt := &Options{
		Path:        CLV_INDEX_PATH,
		Measurement: "logMst",
		Field:       "request",
		Lock:        &CLV_LOCK_PATH,
	}
	tokenIndex, err := NewTokenIndex(opt)
	if err != nil || tokenIndex == nil {
		t.Fatalf("create token index failed, err:%v", err)
	}
	analyzer := buildLearningAnalyzer()
	if analyzer == nil {
		t.Fatalf("create token index analyzer failed")
	}
	tokenIndex.analyzer = analyzer
	err = AddDocumentForTest(tokenIndex, glogStrs)
	if err != nil {
		t.Fatalf("add document failed, err:%v", err)
	}

	SearchTest(t, tokenIndex, gtestOps)
	tokenIndex.Close()
}

func TestInvertIndexRowFilter(t *testing.T) {
	invert := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}, {6, 8, nil}, {7, 3, nil}, {7, 5, nil}},
				sid:         2,
			},
		},
		ids: map[uint32]struct{}{
			3: struct{}{},
		},
		filter: nil,
	}
	filter := &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}}
	invert.SetFilter(filter)
	filter0 := invert.GetFilter()
	if !reflect.DeepEqual(filter, filter0) {
		t.Fatalf("get invert filter, exp:%v get:%v", filter, filter0)
	}

	rowFilter := []RowFilter{{6, nil}, {7, nil}}
	rowFilter0 := invert.GetRowFilterBySid(2)
	if !reflect.DeepEqual(rowFilter, rowFilter0) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", rowFilter, rowFilter0)
	}
}

func TestIntersectForSameSid(t *testing.T) {
	li := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}, {8, 3, nil}},
				sid:         2,
			},
		},
		filter: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{7, 3, nil}, {8, 7, nil}, {9, 7, nil}},
				sid:         2,
			},
		},
	}
	ll := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{
					{7, 0, &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}}},
					{8, 0, nil},
					{9, 0, &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}}}},
				sid: 2,
			},
		},
		ids: make(map[uint32]struct{}),
	}
	invert := IntersectInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}

func TestIntersectForDifferentSid(t *testing.T) {
	li := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}},
				sid:         2,
			},
		},
		filter: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			3: {
				invertState: []InvertState{{7, 3, nil}},
				sid:         3,
			},
		},
		filter: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}},
	}
	ll := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}}},
				sid:         2,
			},
			3: {
				invertState: []InvertState{{7, 3, &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}}}},
				sid:         3,
			},
		},
		ids: make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{Op: influxql.AND,
			LHS: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}},
	}
	invert := IntersectInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}

func TestIntersectForNullInvert(t *testing.T) {
	li := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}},
	}
	ll := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{Op: influxql.AND,
			LHS: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}},
	}
	invert := IntersectInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}

func TestIntersectForSingleNullInvert(t *testing.T) {
	li := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}},
				sid:         2,
			},
		},
		ids:    make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}},
	}
	ll := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{
					{6, 7, &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}}},
				sid: 2,
			},
		},
		ids: make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{Op: influxql.AND,
			LHS: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}},
	}
	invert := IntersectInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}

func TestUnionForSameSid(t *testing.T) {
	li := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{7, 3, nil}, {8, 7, nil}},
				sid:         2,
			},
		},
		filter: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}, {9, 3, nil}, {10, 3, nil}},
				sid:         2,
			},
		},
		filter: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}},
	}
	ll := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}, {7, 3, nil}, {8, 7, nil}, {9, 3, nil}, {10, 3, nil}},
				sid:         2,
			},
		},
		ids: make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{
			Op:  influxql.OR,
			LHS: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}},
	}
	invert := UnionInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}

func TestUnionForDifferentSid(t *testing.T) {
	li := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{7, 3, nil}, {8, 7, nil}},
				sid:         2,
			},
		},
		filter: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			3: {
				invertState: []InvertState{{6, 7, nil}, {9, 3, nil}, {10, 3, nil}},
				sid:         3,
			},
		},
		filter: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}},
	}
	ll := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{7, 3, nil}, {8, 7, nil}},
				sid:         2,
			},
			3: {
				invertState: []InvertState{{6, 7, nil}, {9, 3, nil}, {10, 3, nil}},
				sid:         3,
			},
		},
		ids: make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{
			Op:  influxql.OR,
			LHS: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}},
	}
	invert := UnionInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}

func TestUnionForNullSid(t *testing.T) {
	li := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}},
	}
	ll := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{Op: influxql.OR,
			LHS: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}},
	}
	invert := UnionInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}

func TestUnionForSingleSid(t *testing.T) {
	li := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}},
				sid:         2,
			},
		},
		ids:    make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
	}
	ri := &InvertIndex{
		invertStates: make(map[uint64]*InvertStates),
		ids:          make(map[uint32]struct{}),
		filter:       &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}},
	}
	ll := &InvertIndex{
		invertStates: map[uint64]*InvertStates{
			2: {
				invertState: []InvertState{{6, 7, nil}},
				sid:         2,
			},
		},
		ids: make(map[uint32]struct{}),
		filter: &influxql.BinaryExpr{Op: influxql.OR,
			LHS: &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "size"}, RHS: &influxql.IntegerLiteral{Val: 1000}},
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "status"}, RHS: &influxql.IntegerLiteral{Val: 200}}},
	}
	invert := UnionInvertIndexAndExpr(li, ri)
	if !reflect.DeepEqual(ll, invert) {
		t.Fatalf("get invert rowfilter, exp:%v get:%v", ll, invert)
	}
}
