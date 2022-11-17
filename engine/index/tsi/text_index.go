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
	"time"

	"github.com/openGemini/openGemini/lib/clvIndex"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/gotils/strings"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type TextIndex struct {
	FieldKeys map[string][]string
	ClvIndex  *clvIndex.CLVIndex
}

func NewTextIndex(opts *Options) (*TextIndex, error) {
	textIndex := &TextIndex{
		FieldKeys: make(map[string][]string),
		ClvIndex:  clvIndex.NewCLVIndex(clvIndex.VGRAM),
	}
	str := make([]string, 1)
	str[0] = "logs"
	textIndex.FieldKeys["clvTable"] = str
	if err := textIndex.Open(); err != nil {
		return nil, err
	}
	return textIndex, nil
}

// Need to read the disk file
func (idx *TextIndex) Open() error {
	fmt.Println("TextIndex Open")
	// TODO
	return nil
}

func (idx *TextIndex) Close() error {
	fmt.Println("TextIndex Close")
	// TODO
	return nil
}

func (idx *TextIndex) CreateIndexIfNotExists(primaryIndex PrimaryIndex, row *influx.Row, version uint16) (uint64, error) {
	if fieldNames, ok := idx.FieldKeys[row.Name]; ok {
		for i := 0; i < len(fieldNames); i++ {
			rowFields := row.Fields
			for j := 0; j < len(rowFields); j++ {
				if rowFields[j].Key == fieldNames[i] {
					tsid := row.SeriesId
					timeStamp := row.Timestamp
					log := strings.Copy(row.Fields[i].StrValue)
					idx.ClvIndex.CreateCLVIndex(log, tsid, timeStamp, row.Name, fieldNames[i])
				}
			}
		}
	}
	return 0, nil
}
func And(s1, s2 []utils.SeriesId) []utils.SeriesId {
	smap := make(map[utils.SeriesId]struct{})
	for i := 0; i < len(s1); i++ {
		smap[s1[i]] = struct{}{}
	}
	result := make([]utils.SeriesId, 0)
	for i := 0; i < len(s2); i++ {
		_, ok := smap[s2[i]]
		if ok {
			result = append(result, s2[i])
		}
	}
	return result
}

func Or(s1, s2 []utils.SeriesId) []utils.SeriesId {
	smap := make(map[utils.SeriesId]struct{})
	for i := 0; i < len(s1); i++ {
		smap[s1[i]] = struct{}{}
	}
	for i := 0; i < len(s2); i++ {
		_, ok := smap[s2[i]]
		if !ok {
			s1 = append(s1, s2[i])
		}
	}
	return s1
}

func (idx *TextIndex) searchTSIDsByBinaryExpr(n *influxql.BinaryExpr, measurementName string) ([]utils.SeriesId, error) {
	key, _ := n.LHS.(*influxql.VarRef)
	value, _ := n.RHS.(*influxql.StringLiteral)
	if n.Op == influxql.MATCH {
		return idx.ClvIndex.CLVSearch(measurementName, key.Val, clvIndex.MATCHSEARCH, value.Val), nil
	} else if n.Op == influxql.FUZZY {
		return idx.ClvIndex.CLVSearch(measurementName, key.Val, clvIndex.FUZZYSEARCH, value.Val), nil
	} else if n.Op == influxql.REGEX {
		return idx.ClvIndex.CLVSearch(measurementName, key.Val, clvIndex.REGEXSEARCH, value.Val), nil
	} else {
		return make([]utils.SeriesId, 0), nil
	}
}

func (idx *TextIndex) searchTSIDsInternal(expr influxql.Expr, measurementName string) ([]utils.SeriesId, error) {
	if expr == nil {
		return nil, nil
	}
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			if expr.Op == influxql.AND {
				l, _ := idx.searchTSIDsInternal(expr.LHS, measurementName)
				r, _ := idx.searchTSIDsInternal(expr.RHS, measurementName)
				return And(l, r), nil
			} else {
				l, _ := idx.searchTSIDsInternal(expr.LHS, measurementName)
				r, _ := idx.searchTSIDsInternal(expr.RHS, measurementName)
				return Or(l, r), nil
			}
		default:
			return idx.searchTSIDsByBinaryExpr(expr, measurementName)

		}

	case *influxql.ParenExpr:
		return idx.searchTSIDsInternal(expr.Expr, measurementName)
	default:
		return nil, nil
	}

}

func (idx *TextIndex) Search(primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions) (GroupSeries, error) {
	start := time.Now().UnixMicro()
	measurementName := opt.Name
	start2 := time.Now().UnixMicro()
	clvSids, _ := idx.searchTSIDsInternal(opt.Condition, measurementName)
	end2 := time.Now().UnixMicro()
	start3 := time.Now().UnixMicro()
	mapClvSids := make(map[uint64][]int64)
	for i := 0; i < len(clvSids); i++ { //todo
		key := clvSids[i].Id
		val := clvSids[i].Time
		if _, ok := mapClvSids[key]; !ok {
			timeArr := []int64{}
			timeArr = append(timeArr, val)
			mapClvSids[key] = timeArr
		} else {
			mapClvSids[key] = append(mapClvSids[key], val)
		}
	}
	end3 := time.Now().UnixMicro()
	mergeSetIndex := primaryIndex.(*MergeSetIndex)
	var indexKeyBuf []byte // reused todo
	var err error

	groupSeries := make(GroupSeries, 1)
	for i := 0; i < 1; i++ {
		tagSetInfo := NewTagSetInfo()
		for id, timeArr := range mapClvSids {
			indexKeyBuf, err = mergeSetIndex.searchSeriesKey(indexKeyBuf, id)
			if err != nil {
				panic(err)
			}
			var tagsBuf influx.PointTags
			influx.IndexKeyToTags(indexKeyBuf, true, &tagsBuf)
			seriesKey := getSeriesKeyBuf()
			seriesKey = influx.Parse2SeriesKey(indexKeyBuf, seriesKey)
			//tagSetInfo.Append(id, seriesKey, nil, tagsBuf)

			tagSetInfo.IDs = append(tagSetInfo.IDs, id)
			tagSetInfo.SeriesKeys = append(tagSetInfo.SeriesKeys, seriesKey)
			tagSetInfo.TagsVec = append(tagSetInfo.TagsVec, tagsBuf)
			tagSetInfo.Filters = append(tagSetInfo.Filters, nil)
			var tmp = make([]int64, 0)
			tmp = append(tmp, timeArr...)
			tagSetInfo.Timestamps = append(tagSetInfo.Timestamps, tmp)
			indexKeyBuf = indexKeyBuf[:0]
		}
		groupSeries[i] = tagSetInfo
	}
	end := time.Now().UnixMicro()
	fmt.Println(len(clvSids))
	fmt.Println("clvSearch-time: ")
	fmt.Println(float64(end2-start2)/1000, "ms")
	fmt.Println("pro-time: ")
	fmt.Println(float64(end3-start3)/1000, "ms")
	fmt.Println("all-time: ")
	fmt.Println(float64(end-start)/1000, "ms")
	return groupSeries, nil
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

func TextInsert(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}, version uint16) (uint64, error) {
	textIndex := index.(*TextIndex)
	insertRow := row.(*influx.Row)
	return textIndex.CreateIndexIfNotExists(primaryIndex, insertRow, version)
}

// upper function call should analyze result
func TextScan(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions) (interface{}, error) {
	textIndex := index.(*TextIndex)
	return textIndex.Search(primaryIndex, span, name, opt)
}

func TextDelete(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error {
	textIndex := index.(*TextIndex)
	return textIndex.Delete(primaryIndex, name, condition, tr)
}

func TextClose(index interface{}) error {
	textIndex := index.(*TextIndex)
	return textIndex.Close()
}
