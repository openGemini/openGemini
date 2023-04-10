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
	"os"
	"path"
	"sync"

	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

const (
	TextDirectory = "text"
)

type TextIndex struct {
	fieldTable     map[string]map[string]*clv.TokenIndex // (measurementName, fieldName) -> *TokenIndex table
	fieldTableLock sync.RWMutex
	path           string
}

func NewTextIndex(opts *Options) (*TextIndex, error) {
	textIndex := &TextIndex{
		fieldTable: make(map[string]map[string]*clv.TokenIndex),
		path:       opts.path, // = data/db/pt/rp/index/indexid..
	}
	return textIndex, nil
}

func (idx *TextIndex) NewTokenIndex(idxPath, measurement, field string) error {
	txtIdxPath := path.Join(idxPath, TextDirectory)
	opts := clv.Options{
		Path:        txtIdxPath,
		Measurement: measurement,
		Field:       field,
	}
	tokenIndex, err := clv.NewTokenIndex(&opts)
	if err != nil {
		return err
	}
	if _, ok := idx.fieldTable[measurement]; !ok {
		idx.fieldTable[measurement] = make(map[string]*clv.TokenIndex)
	}

	idx.fieldTable[measurement][field] = tokenIndex
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
			err := idx.NewTokenIndex(idx.path, measurement, field)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (idx *TextIndex) Close() error {
	for mst, tokenIndexMap := range idx.fieldTable {
		for field, tokenIndex := range tokenIndexMap {
			tokenIndex.Close()
			fmt.Println("TextIndex Close:", mst, field)
		}
	}

	return nil
}

func (idx *TextIndex) CreateIndexIfNotExists(primaryIndex PrimaryIndex, row *influx.Row) (uint64, error) {
	var field influx.Field
	tsid := row.SeriesId
	timestamp := row.Timestamp
	// Find the field need to be created index.
	for _, opt := range row.IndexOptions {
		if opt.Oid == uint32(Text) {
			if int(opt.IndexList[0]) < len(row.Tags) {
				return 0, fmt.Errorf("cannot create text index for tag: %s", row.Tags[opt.IndexList[0]].Key)
			}

			field = row.Fields[int(opt.IndexList[0])-len(row.Tags)]
			if field.Type != influx.Field_Type_String {
				return 0, fmt.Errorf("field type must be string for TextIndex")
			}

			tokenIndex, ok := idx.fieldTable[row.Name][field.Key]
			if !ok {
				idx.fieldTableLock.Lock()
				if idx.fieldTable[row.Name][field.Key] == nil {
					err := idx.NewTokenIndex(idx.path, row.Name, field.Key)
					if err != nil {
						return 0, err
					}
				}
				idx.fieldTableLock.Unlock()
				tokenIndex = idx.fieldTable[row.Name][field.Key]
			}
			err := tokenIndex.AddDocument(field.StrValue, tsid, timestamp)
			if err != nil {
				return 0, err
			}

		}
	}

	return 0, nil
}

func (idx *TextIndex) SearchByTokenIndex(name string, n *influxql.BinaryExpr) (*clv.InvertIndex, error) {
	key, ok := n.LHS.(*influxql.StringLiteral)
	if !ok {
		return nil, fmt.Errorf("The type of LHS value is wrong.")
	}
	value, ok := n.RHS.(*influxql.VarRef)
	if !ok {
		return nil, fmt.Errorf("The type of RHS value is wrong.")
	}

	tokenIndex, ok := idx.fieldTable[name][key.Val]
	if !ok {
		return nil, fmt.Errorf("The filed(%s) of measurement(%s) has no text index.", key.Val, name)
	}

	switch n.Op {
	case influxql.MATCH:
		return tokenIndex.Search(clv.Match, value.Val)
	case influxql.MATCH_PHRASE:
		return tokenIndex.Search(clv.Match_Phrase, value.Val)
	default:
	}
	return nil, nil
}

func (idx *TextIndex) SearchTextIndex(name string, expr influxql.Expr) (*clv.InvertIndex, error) {
	if expr == nil {
		return nil, nil
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			li, lerr := idx.SearchTextIndex(name, expr.LHS)
			if lerr != nil {
				return nil, lerr
			}
			ri, rerr := idx.SearchTextIndex(name, expr.RHS)
			if rerr != nil {
				return nil, rerr
			}

			if expr.Op == influxql.AND {
				return clv.IntersectInvertIndex(li, ri), nil
			} else {
				return clv.UnionInvertIndex(li, ri), nil
			}

		case influxql.MATCH, influxql.MATCH_PHRASE:
			return idx.SearchByTokenIndex(name, expr)
		default:
			return nil, nil
		}
	case *influxql.ParenExpr:
		return idx.SearchTextIndex(name, expr.Expr)
	default:
	}
	return nil, nil
}

func copyTagSetInfo(tagset *TagSetInfo, group *TagSetInfo, filterTime []int64, i int) {
	tagset.IDs = append(tagset.IDs, group.IDs[i])
	tagset.Filters = append(tagset.Filters, group.Filters[i])
	tagset.SeriesKeys = append(tagset.SeriesKeys, group.SeriesKeys[i])
	tagset.TagsVec = append(tagset.TagsVec, group.TagsVec[i])
	tagset.FilterTimes = append(tagset.FilterTimes, filterTime)
	// how to deal this?
	tagset.key = group.key
}

// something wrong about the relation of primary index and secondary index, eg.. where host="1.1.1.1" or/and match("masseage", "error")... or/and is different.
func (idx *TextIndex) Search(primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, groups interface{}) (GroupSeries, error) {
	groupSeries, ok := groups.(GroupSeries)
	if !ok {
		return nil, fmt.Errorf("not a group series: %v", groups)
	}

	if _, ok := idx.fieldTable[string(name)]; !ok {
		return groupSeries, nil
	}

	invert, err := idx.SearchTextIndex(string(name), opt.Condition)
	if err != nil {
		return nil, err
	}

	if invert == nil {
		return groupSeries, nil
	}

	sortedTagsSets := make(GroupSeries, 0, len(groupSeries))
	for _, group := range groupSeries {
		tagSet := new(TagSetInfo)
		for i, sid := range group.IDs {
			filterTime := invert.GetFilterTimeBySid(sid)
			if len(filterTime) == 0 {
				continue
			}
			copyTagSetInfo(tagSet, group, filterTime, i)
		}
		if len(tagSet.IDs) > 0 {
			sortedTagsSets = append(sortedTagsSets, tagSet)
		}
	}

	return sortedTagsSets, nil
}

func (idx *TextIndex) Delete(primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	sids, _ := primaryIndex.GetDeletePrimaryKeys(name, condition, tr)
	fmt.Println("TextIndex Delete", len(sids))
	// TODO
	return nil
}

func TextIndexHandler(opt *Options, primaryIndex PrimaryIndex) (*IndexAmRoutine, error) {
	index, err := NewTextIndex(opt)
	if err != nil {
		return nil, err
	}
	return &IndexAmRoutine{
		amKeyType:    Text,
		amOpen:       TextOpen,
		amBuild:      TextBuild,
		amInsert:     TextInsert,
		amDelete:     TextDelete,
		amScan:       TextScan,
		amClose:      TextClose,
		index:        index,
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
func TextScan(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, groups interface{}) (interface{}, error) {
	textIndex := index.(*TextIndex)
	return textIndex.Search(primaryIndex, span, name, opt, groups)
}

func TextDelete(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	textIndex := index.(*TextIndex)
	return textIndex.Delete(primaryIndex, name, condition, tr)
}

func TextClose(index interface{}) error {
	textIndex := index.(*TextIndex)
	return textIndex.Close()
}
