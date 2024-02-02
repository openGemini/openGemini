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
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"sync"

	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	TextDirectory = "text"
)

type TextIndex struct {
	fieldTable     map[string]map[string]*clv.TokenIndex // (measurementName, fieldName) -> *TokenIndex table
	fieldTableLock sync.RWMutex
	path           string
	lock           *string
}

func NewTextIndex(opts *Options) (*TextIndex, error) {
	textIndex := &TextIndex{
		fieldTable: make(map[string]map[string]*clv.TokenIndex),
		path:       opts.path, // = data/db/pt/rp/index/indexid..
		lock:       opts.lock,
	}
	return textIndex, nil
}

func getAllDataInvert(expr influxql.Expr) (*clv.InvertIndex, error) {
	rexpr, err := expr2BinaryExpr(expr)
	if err != nil {
		return nil, err
	}
	invert := clv.NewInvertIndex()
	invert.SetFilter(rexpr)

	return &invert, nil
}

func haveTextFilter(expr influxql.Expr) bool {
	if expr == nil {
		return false
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			return haveTextFilter(expr.LHS) || haveTextFilter(expr.RHS)
		case influxql.MATCH, influxql.MATCHPHRASE, influxql.LIKE:
			return true
		default:
			return false
		}
	case *influxql.ParenExpr:
		return haveTextFilter(expr.Expr)
	default:
	}
	return false
}

func (idx *TextIndex) NewTokenIndex(idxPath, measurement, field string) error {
	txtIdxPath := path.Join(idxPath, TextDirectory)
	fieldName := make([]byte, len(field))
	copy(fieldName, field)
	mstName := make([]byte, len(measurement))
	copy(mstName, measurement)

	opts := clv.Options{
		Path:        txtIdxPath,
		Measurement: string(mstName),
		Field:       string(fieldName),
		Lock:        idx.lock,
	}
	tokenIndex, err := clv.NewTokenIndex(&opts)
	if err != nil {
		return err
	}
	if _, ok := idx.fieldTable[string(mstName)]; !ok {
		idx.fieldTable[string(mstName)] = make(map[string]*clv.TokenIndex)
	}

	idx.fieldTable[string(mstName)][string(fieldName)] = tokenIndex
	return nil
}

func (idx *TextIndex) Open() error {
	path := path.Join(idx.path, TextDirectory)
	mstDirs, err := fileops.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fileops.MkdirAll(path, 0750)
		}
		return err
	}

	for mstIdx := range mstDirs {
		if !mstDirs[mstIdx].IsDir() {
			continue
		}
		measurement := mstDirs[mstIdx].Name()
		tmpMstDir := path + "/" + measurement
		fieldDirs, err := fileops.ReadDir(tmpMstDir)
		if err != nil {
			continue
		}
		// fulltext/measuremnt/field
		for fieldIdx := range fieldDirs {
			if !fieldDirs[fieldIdx].IsDir() {
				continue
			}
			field := fieldDirs[fieldIdx].Name()
			idx.fieldTableLock.Lock()
			err := idx.NewTokenIndex(idx.path, measurement, field)
			idx.fieldTableLock.Unlock()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (idx *TextIndex) Close() error {
	idx.fieldTableLock.RLock()
	for _, tokenIndexMap := range idx.fieldTable {
		for _, tokenIndex := range tokenIndexMap {
			tokenIndex.Close()
		}
	}
	idx.fieldTableLock.RUnlock()

	return nil
}

func (idx *TextIndex) CreateIndexIfNotExists(primaryIndex PrimaryIndex, row *influx.Row) (uint64, error) {
	var field influx.Field
	tsid := row.SeriesId
	timestamp := row.Timestamp
	// Find the field need to be created index.
	for _, opt := range row.IndexOptions {
		if opt.Oid != uint32(index.Text) {
			continue
		}

		for i := 0; i < len(opt.IndexList); i++ {
			if int(opt.IndexList[i]) < len(row.Tags) {
				return 0, fmt.Errorf("cannot create text index for tag: %s", row.Tags[opt.IndexList[i]].Key)
			}

			field = row.Fields[int(opt.IndexList[i])-len(row.Tags)]
			if field.Type != influx.Field_Type_String {
				return 0, fmt.Errorf("field type must be string for TextIndex, field: %s", field.Key)
			}

			idx.fieldTableLock.RLock()
			tokenIndex, ok := idx.fieldTable[row.Name][field.Key]
			idx.fieldTableLock.RUnlock()
			if !ok {
				idx.fieldTableLock.Lock()
				if idx.fieldTable[row.Name][field.Key] == nil {
					err := idx.NewTokenIndex(idx.path, row.Name, field.Key)
					if err != nil {
						idx.fieldTableLock.Unlock()
						return 0, err
					}
				}
				idx.fieldTableLock.Unlock()
				idx.fieldTableLock.RLock()
				tokenIndex = idx.fieldTable[row.Name][field.Key]
				idx.fieldTableLock.RUnlock()
			}

			err := tokenIndex.AddDocument(field.StrValue, tsid, timestamp)
			if err != nil {
				return 0, err
			}
		}
	}

	return 0, nil
}

func (idx *TextIndex) SearchByTokenIndex(name string, sids []uint64, n *influxql.BinaryExpr) (*clv.InvertIndex, error) {
	key, ok := n.LHS.(*influxql.VarRef)
	if !ok {
		return nil, fmt.Errorf("the type of LHS value is wrong")
	}
	idx.fieldTableLock.RLock()
	tokenIndex, ok := idx.fieldTable[name][key.Val]
	idx.fieldTableLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("the field(%s) of measurement(%s) has no text index", key.Val, name)
	}
	value, ok := n.RHS.(*influxql.StringLiteral)
	if !ok {
		value, ok := n.RHS.(*influxql.VarRef)
		if n.Op == influxql.LIKE && ok {
			return tokenIndex.Search(clv.Fuzzy, value.Val, sids)
		}
		return nil, fmt.Errorf("the type of RHS value is wrong")
	}
	switch n.Op {
	case influxql.MATCH:
		return tokenIndex.Search(clv.Match, value.Val, sids)
	case influxql.MATCHPHRASE:
		return tokenIndex.Search(clv.Match_Phrase, value.Val, sids)
	case influxql.LIKE:
		return tokenIndex.Search(clv.Fuzzy, value.Val, sids)
	default:
	}
	return nil, nil
}

var ErrTextExpr = errors.New("no text expr")

func (idx *TextIndex) SearchTextIndexByExpr(name string, sids []uint64, expr influxql.Expr) (*clv.InvertIndex, error) {
	if expr == nil {
		return nil, nil
	}

	var err error
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			li, lerr := idx.SearchTextIndexByExpr(name, sids, expr.LHS)
			if lerr != nil && lerr != ErrTextExpr {
				return nil, lerr
			}
			ri, rerr := idx.SearchTextIndexByExpr(name, sids, expr.RHS)
			if rerr != nil && rerr != ErrTextExpr {
				return nil, rerr
			}

			if lerr == ErrTextExpr {
				li, err = getAllDataInvert(expr.LHS)
				if err != nil {
					return nil, err
				}
			}
			if rerr == ErrTextExpr {
				ri, err = getAllDataInvert(expr.RHS)
				if err != nil {
					return nil, err
				}
			}

			// Intersect invertIndex and expr
			if expr.Op == influxql.AND {
				return clv.IntersectInvertIndexAndExpr(li, ri), nil
			} else {
				return clv.UnionInvertIndexAndExpr(li, ri), nil
			}
		case influxql.MATCH, influxql.MATCHPHRASE, influxql.LIKE:
			return idx.SearchByTokenIndex(name, sids, expr)
		default:
			return nil, ErrTextExpr
		}
	case *influxql.ParenExpr:
		return idx.SearchTextIndexByExpr(name, sids, expr.Expr)
	default:
	}
	return nil, nil
}

// It is necessary to strictly confirm whether the expression is the same.
// If it cannot be confirmed, it will return notEequal, but this will reduce query efficiency.
func exprDeepEqual(expr0, expr1 influxql.Expr) bool {
	if expr0 == expr1 { // both nil
		return true
	}
	if expr0 == nil || expr1 == nil {
		return false
	}

	if !reflect.DeepEqual(expr0, expr1) {
		return false
	}

	return true
}

// The Filter for each ID of the srcSet is the same.
func (idx *TextIndex) SearchTextIndex(name []byte, srcSet *TagSetInfo) (*TagSetInfo, error) {
	if len(srcSet.IDs) == 0 || srcSet.Filters[0] == nil {
		return srcSet, nil
	}

	invert, err := idx.SearchTextIndexByExpr(string(name), srcSet.IDs, srcSet.Filters[0])
	if err != nil {
		if err == ErrTextExpr {
			return srcSet, nil
		}
		return nil, err
	}

	tagSet := new(TagSetInfo)
	tagSet.RowFilters = clv.NewRowFilters()
	for i, sid := range srcSet.IDs {
		filter := invert.GetFilter()
		rowFilter := invert.GetRowFilterBySid(sid)
		if filter == nil && rowFilter == nil {
			continue
		}
		tagSet.Append(sid, srcSet.SeriesKeys[i], filter, srcSet.TagsVec[i], rowFilter)
	}

	return tagSet, nil
}

func (idx *TextIndex) Search(primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, groups interface{}) (GroupSeries, error) {
	groupSeries, ok := groups.(GroupSeries)
	if !ok {
		return nil, fmt.Errorf("not a group series: %v", groups)
	}

	if !haveTextFilter(opt.Condition) {
		return groupSeries, nil
	}

	if _, ok := idx.fieldTable[string(name)]; !ok {
		return groupSeries, nil
	}

	var preFilter influxql.Expr
	sortedTagsSets := make(GroupSeries, 0, len(groupSeries))
	for _, group := range groupSeries {
		tagSet := new(TagSetInfo)
		for i := range group.IDs {
			if i != 0 && !exprDeepEqual(group.Filters[i], preFilter) {
				tmpTagSet, err := idx.SearchTextIndex(name, tagSet)
				if err != nil {
					return nil, err
				}
				sortedTagsSets = append(sortedTagsSets, tmpTagSet)
				tagSet = new(TagSetInfo)
			}
			tagSet.Append(group.IDs[i], group.SeriesKeys[i], group.Filters[i], group.TagsVec[i], nil)
			preFilter = group.Filters[i]
		}
		// the last one
		if len(tagSet.IDs) > 0 {
			tmpTagSet, err := idx.SearchTextIndex(name, tagSet)
			if err != nil {
				return nil, err
			}
			sortedTagsSets = append(sortedTagsSets, tmpTagSet)
		}
	}

	return sortedTagsSets, nil
}

func (idx *TextIndex) Delete(primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	sids, _ := primaryIndex.GetDeletePrimaryKeys(name, condition, tr)
	logger.GetLogger().Info(fmt.Sprintf("TextIndex Delete, %d", len(sids)))
	// TODO
	return nil
}

func (idx *TextIndex) Flush() {
	idx.fieldTableLock.RLock()
	for _, tokenIndexMap := range idx.fieldTable {
		for _, tokenIndex := range tokenIndexMap {
			tokenIndex.Flush()
		}
	}
	idx.fieldTableLock.RUnlock()
}

func TextIndexHandler(opt *Options, primaryIndex PrimaryIndex) (*IndexAmRoutine, error) {
	textIndex, err := NewTextIndex(opt)
	if err != nil {
		return nil, err
	}
	return &IndexAmRoutine{
		amKeyType:    index.Text,
		amOpen:       TextOpen,
		amBuild:      TextBuild,
		amInsert:     TextInsert,
		amDelete:     TextDelete,
		amScan:       TextScan,
		amClose:      TextClose,
		amFlush:      TextFlush,
		index:        textIndex,
		primaryIndex: primaryIndex,
	}, nil
}

func TextBuild(relation *IndexRelation) error {
	return nil
}

func TextOpen(index interface{}) error {
	textIndex := index.(*TextIndex)
	return textIndex.Open()
}

func TextInsert(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}) (uint64, error) {
	textIndex := index.(*TextIndex)
	insertRow := row.(*influx.Row)
	return textIndex.CreateIndexIfNotExists(primaryIndex, insertRow)
}

// upper function call should analyze result
func TextScan(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, groups interface{}) (interface{}, int64, error) {
	textIndex := index.(*TextIndex)
	result, err := textIndex.Search(primaryIndex, span, name, opt, groups)
	return result, 0, err
}

func TextDelete(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	textIndex := index.(*TextIndex)
	return textIndex.Delete(primaryIndex, name, condition, tr)
}

func TextClose(index interface{}) error {
	textIndex := index.(*TextIndex)
	return textIndex.Close()
}

func TextFlush(index interface{}) {
	textIndex, ok := index.(*TextIndex)
	if !ok {
		return
	}
	textIndex.Flush()
}
