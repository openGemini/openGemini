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
package immutable

import (
	"path"
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/consume"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestGetCursorBy(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	mstName := "mst"
	err := writeData(testCompDir, mstName)
	if err != nil {
		t.Errorf(err.Error())
	}
	p := path.Join(testCompDir, mstName)
	path := sparseindex.NewOBSFilterPath("", p, nil)
	metaIndex, metaIndexId, _ := GetCursorsBy(path, util.TimeRange{Min: 0, Max: 2}, true)
	assert.Equal(t, -1, metaIndex)
	assert.Equal(t, uint64(0), metaIndexId)

	metaIndex, metaIndexId, _ = GetCursorsBy(path, util.TimeRange{Min: 0, Max: 2}, false)
	assert.Equal(t, -1, metaIndex)
	assert.Equal(t, uint64(0), metaIndexId)

	metaIndex, metaIndexId, _ = GetCursorsBy(path, util.TimeRange{Min: 0, Max: 1635732519000000000}, true)
	assert.Equal(t, 0, metaIndex)
	assert.Equal(t, uint64(0), metaIndexId)
	metaIndex, metaIndexId, _ = GetCursorsBy(path, util.TimeRange{Min: 0, Max: 1635732519000000000}, false)
	assert.Equal(t, 1, metaIndex)
	assert.Equal(t, uint64(384), metaIndexId)

	path = sparseindex.NewOBSFilterPath("", testCompDir, nil)
	m, _, err := GetCursorsBy(path, util.TimeRange{Min: 0, Max: 1935732519000000000}, true)
	if err != nil || m != -1 {
		t.Errorf("get wrong cursor")
	}

	consumeInfo := &consume.ConsumeInfo{
		Tr: util.TimeRange{Min: 0, Max: 1635732519000000000},
	}
	consumeInfo.FromCursor = &consume.ConsumeCursor{
		Time:           0,
		Reverse:        false,
		CursorID:       0,
		TaskNum:        1,
		CurrTotalPtNum: 1,
		Tasks:          make([]*consume.ConsumeSegmentTask, 0, 1),
	}
	consumeInfo.FromCursor.Tasks = make([]*consume.ConsumeSegmentTask, 1)
	consumeInfo.FromCursor.Tasks[0] = &consume.ConsumeSegmentTask{}
	consumeInfo.FromCursor.Tasks[0].CurrTask = &consume.ConsumeTask{
		MetaIndexId: 0,
		BlockID:     0,
		Timestamp:   0,
	}

	path1 := sparseindex.NewOBSFilterPath("", p, nil)
	var isEnd bool
	s, _ := NewSegmentSequenceReader(path1, 0, 10, consumeInfo, schema, nil)
	a, isEnd, _, _, _, err := s.ConsumeDateByShard()
	assert.Equal(t, false, isEnd)
	assert.Equal(t, 200, len(a))
	s.Close()
	consumeInfo.EndCursor = &consume.ConsumeCursor{
		Time:           0,
		Reverse:        false,
		CursorID:       0,
		TaskNum:        1,
		CurrTotalPtNum: 1,
		Tasks:          make([]*consume.ConsumeSegmentTask, 0, 1),
	}
	consumeInfo.EndCursor.Tasks = make([]*consume.ConsumeSegmentTask, 1)
	consumeInfo.EndCursor.Tasks[0] = &consume.ConsumeSegmentTask{}
	consumeInfo.EndCursor.Tasks[0].CurrTask = &consume.ConsumeTask{
		MetaIndexId: 0,
		BlockID:     6,
		Timestamp:   0,
	}
	s, _ = NewSegmentSequenceReader(path1, 0, 10, consumeInfo, schema, nil)
	a, isEnd, _, _, _, err = s.ConsumeDateByShard()
	assert.Equal(t, false, isEnd)
	assert.Equal(t, 200, len(a))
	s.Close()
	consumeInfo.Cond = &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: "primaryKey_string1", Type: influxql.String},
		RHS: &influxql.StringLiteral{Val: "hello"},
	}

	filterOption := &BaseFilterOptions{
		CondFunctions: &binaryfilterfunc.ConditionImpl{},
		FieldsIdx:     []int{0},
		RedIdxMap:     map[int]struct{}{},
		FiltersMap:    make(map[string]*influxql.FilterMapValue),
	}

	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	consumeInfo.FilterBitmap = bitmap.NewFilterBitmap(filterOption.CondFunctions.NumFilter())
	consumeInfo.Mst = &meta.MeasurementInfo{
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	consumeInfo.ProcessOption = &query.ProcessorOptions{
		Sources:     []influxql.Source{&influxql.Measurement{Name: mstName, EngineType: config.COLUMNSTORE, IndexRelation: &consumeInfo.Mst.IndexRelation}},
		ChunkSize:   1024,
		ChunkedSize: 10000,
		Dimensions:  make([]string, 0),
		Condition:   consumeInfo.Cond,
	}

	s, _ = NewSegmentSequenceReader(path1, 0, 10, consumeInfo, schema, NewFilterOpts(consumeInfo.Cond, filterOption, nil, nil))
	a, isEnd, _, _, _, err = s.ConsumeDateByShard()
	assert.Equal(t, false, isEnd)
	assert.Equal(t, 0, len(a))

	consumeInfo.EndCursor.Tasks[0].CurrTask.MetaIndexId = 1
	consumeInfo.FromCursor.Tasks[0].CurrTask.MetaIndexId = 1
	consumeInfo.FromCursor.Tasks[0].CurrTask.BlockID = 635
	s, _ = NewSegmentSequenceReader(path1, 0, 10, consumeInfo, schema, NewFilterOpts(consumeInfo.Cond, filterOption, nil, nil))
	a, isEnd, _, _, _, err = s.ConsumeDateByShard()
	assert.Equal(t, true, isEnd)
	assert.Equal(t, 0, len(a))
	s.Close()
}

func TestFilterNilBy(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	mstName := "mst"
	err := writeData(testCompDir, mstName)
	if err != nil {
		t.Errorf(err.Error())
	}
	p := path.Join(testCompDir, mstName)
	consumeInfo := &consume.ConsumeInfo{
		Tr: util.TimeRange{Min: 0, Max: 1635732519000000000},
	}
	consumeInfo.FromCursor = &consume.ConsumeCursor{
		Time:           0,
		Reverse:        false,
		CursorID:       0,
		TaskNum:        1,
		CurrTotalPtNum: 1,
		Tasks:          make([]*consume.ConsumeSegmentTask, 0, 1),
	}
	consumeInfo.FromCursor.Tasks = make([]*consume.ConsumeSegmentTask, 1)
	consumeInfo.FromCursor.Tasks[0] = &consume.ConsumeSegmentTask{}
	consumeInfo.FromCursor.Tasks[0].CurrTask = &consume.ConsumeTask{
		MetaIndexId: 0,
		BlockID:     0,
		Timestamp:   0,
	}

	path1 := sparseindex.NewOBSFilterPath("", p, nil)
	s, _ := NewSegmentSequenceReader(path1, 0, 10, consumeInfo, schema, nil)
	a, _, _, _, _, err := s.ConsumeDateByShard()
	assert.Equal(t, 200, len(a))
	s.Close()
	consumeInfo.EndCursor = &consume.ConsumeCursor{
		Time:           0,
		Reverse:        false,
		CursorID:       0,
		TaskNum:        1,
		CurrTotalPtNum: 1,
		Tasks:          make([]*consume.ConsumeSegmentTask, 0, 1),
	}
	consumeInfo.EndCursor.Tasks = make([]*consume.ConsumeSegmentTask, 1)
	consumeInfo.EndCursor.Tasks[0] = &consume.ConsumeSegmentTask{}
	consumeInfo.EndCursor.Tasks[0].CurrTask = &consume.ConsumeTask{
		MetaIndexId: 0,
		BlockID:     6,
		Timestamp:   0,
	}
	s, _ = NewSegmentSequenceReader(path1, 0, 10, consumeInfo, schema, nil)
	a, _, _, _, _, err = s.ConsumeDateByShard()
	assert.Equal(t, 200, len(a))

	consumeInfo.Cond = &influxql.BinaryExpr{
		Op:  influxql.LT,
		LHS: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 3},
	}

	filterOption := &BaseFilterOptions{
		CondFunctions: &binaryfilterfunc.ConditionImpl{},
		FieldsIdx:     []int{0},
		RedIdxMap:     map[int]struct{}{},
		FiltersMap:    make(map[string]*influxql.FilterMapValue),
	}

	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	consumeInfo.FilterBitmap = bitmap.NewFilterBitmap(filterOption.CondFunctions.NumFilter())
	consumeInfo.Mst = &meta.MeasurementInfo{
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	consumeInfo.ProcessOption = &query.ProcessorOptions{
		Sources:     []influxql.Source{&influxql.Measurement{Name: mstName, EngineType: config.COLUMNSTORE, IndexRelation: &consumeInfo.Mst.IndexRelation}},
		ChunkSize:   1024,
		ChunkedSize: 10000,
		Dimensions:  make([]string, 0),
		Condition:   consumeInfo.Cond,
	}

	s, _ = NewSegmentSequenceReader(path1, 0, 10, consumeInfo, schema, NewFilterOpts(consumeInfo.Cond, filterOption, nil, nil))
	a, _, _, _, _, err = s.ConsumeDateByShard()
	assert.Equal(t, 0, len(a))
	s.Close()
}
